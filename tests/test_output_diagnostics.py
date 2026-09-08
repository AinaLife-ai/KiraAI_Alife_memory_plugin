import json
import pytest
from test_memory import c, s, e


@pytest.mark.asyncio
async def test_compression_retry_reports_field_without_leaking_output(tmp_path):
    store = s.Store(tmp_path / "db")
    store.initialize()
    store.capture(
        "legacy:unscoped",
        "turn",
        [dict(role="user", content="完整原文", users=[], time=1.0) for _ in range(4)],
    )
    calls = []

    async def model(_, purpose, instruction, schema, payload):
        calls.append(instruction + c.dump(payload))
        if len(calls) == 1:
            return '{"summary":"私人内容不得进入诊断", "facts":"错误类型"}'
        assert "facts" in payload["output_feedback"]
        assert "list_type" in payload["output_feedback"]
        assert "私人内容不得进入诊断" not in payload["output_feedback"]
        return '{"summary":"压缩完成", "facts":[]}'

    cfg = c.Settings(threshold=4, batch_size=2, model_retries=1)
    await e.Engine(store, lambda: cfg, model, None, None).compress("legacy:unscoped")
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_audit_invalid_record_id_retries_before_store_write(tmp_path):
    store = s.Store(tmp_path / "db")
    store.initialize()
    rid = store.memorize("a:dm:u", "喜欢猫", ["a:u"], 1.0, 1.0)
    with store.connect() as db:
        fid = store._add_fact(
            db,
            "a:dm:u",
            dict(
                category="preference",
                subject="a:u",
                content="喜欢猫",
                reason="",
                scenario="",
                tags=[],
                relations=[],
                source_ids=[rid],
            ),
        )
    calls = []

    async def model(_, purpose, instruction, schema, payload):
        calls.append(instruction)
        return c.dump(
            {
                "actions": [
                    dict(
                        action="correct",
                        target_id=fid,
                        source_ids=[rid if len(calls) == 1 else fid],
                        content="喜欢猫，但不养猫",
                        reason="按原文核对",
                        relations=[],
                    )
                ]
            }
        )

    cfg = c.Settings(model_retries=1)
    await e.Engine(store, lambda: cfg, model, None, None).audit("a:dm:u")
    assert len(calls) == 2
    assert "unknown audit evidence" in calls[1]
    assert store.facts("a:dm:u")[0]["content"] == "喜欢猫，但不养猫"


def test_same_name_manual_reason_is_recorded(tmp_path):
    store = s.Store(tmp_path / "db")
    store.initialize()
    store.observe_name("qq:123", "小夏", observed=1.0)
    rev = store.entities()[0]["revision"]
    store.observe_name(
        "qq:123",
        "小夏",
        revision=rev,
        reason="核对本人确认无误",
        source="manual",
        observed=2.0,
    )
    assert store.entities()[0]["history"][0]["reason"] == "核对本人确认无误"


def test_search_can_exclude_seen_ids_before_pagination(tmp_path):
    store = s.Store(tmp_path / "db")
    store.initialize()
    ids = [
        store.memorize("a:dm:u", f"猫的经历{i}", ["a:u"], float(i), float(i))
        for i in range(5)
    ]
    result = store.search(scope="global", lexical="猫", limit=2, exclude_ids=ids[:2])
    assert result["total"] == 3 and [r["id"] for r in result["items"]] == ids[2:4]


def test_read_archive_projection_avoids_double_encoded_batch_and_pages_ids():
    from alife_test_plugin.retrieval import archive_view

    content = c.dump(
        [dict(id=f"legacy-{i}", role="user", content='原文"不丢失"') for i in range(70)]
    )
    row = dict(
        id="parent",
        sid="legacy:unscoped",
        role="assistant",
        level=1,
        start=1.0,
        end=2.0,
        summary="摘要",
        users=[],
        revision=1,
        permanent=0,
        content=content,
        children=[f"legacy-{i}" for i in range(70)],
    )
    view = archive_view(row)
    assert (
        "content" not in view
        and len(view["children"]) == 20
        and view["children_total"] == 70
    )
    assert view["next_child_offset"] == 20
    full = archive_view(row, 60, 20, True)
    assert len(full["children"]) == 10 and full["next_child_offset"] is None
    assert full["content"] == json.loads(content) and row["content"] == content


def test_migration_identity_labels_never_infer_platform(tmp_path):
    from alife_test_plugin.retrieval import identity_info

    assert identity_info("legacy:global")["lookup_id"] == ""
    assert identity_info("legacy:unscoped")["lookup_id"] == ""
    assert identity_info("legacy:user:12001")["lookup_id"] == ""
    assert identity_info("legacy:user:qq:12002")["lookup_id"] == "qq:12002"
    store = s.Store(tmp_path / "db")
    store.initialize()
    rid = store.memorize("legacy:user:qq:123", "旧记忆", ["qq:123"], 1.0, 1.0)
    original = store.get(rid)
    store.observe_name("qq:123", "小夏", observed=10.0)
    n = store.entities(ids=["legacy:user:qq:123"])[0]
    assert "小夏" in n["label"] and n["name_source_id"] == "qq:123"
    assert store.get(rid) == original


def test_diagnostics_do_not_echo_extra_keys_or_private_values():
    from alife_test_plugin.output_validation import diagnostic

    with pytest.raises(ValueError) as caught:
        c.parse_output(
            '{"summary":"正常", "facts":[], "PRIVATE_SECRET":"敏感正文"}', c.Compression
        )
    text = diagnostic(caught.value)
    assert (
        "extra_forbidden" in text
        and "PRIVATE_SECRET" not in text
        and "敏感正文" not in text
    )


def test_recall_window_is_bounded_and_has_no_cross_caller_state():
    from alife_test_plugin.retrieval import RecallWindow

    w = RecallWindow()
    w.remember(("group", "user1", "global"), "猫", ["one"])
    assert not w.get(("group", "user2", "global"))["ids"]
    assert not w.get(("group", "user1", "session"))["ids"]
    for n in range(300):
        w.remember(n, "主题", [str(i) for i in range(300)])
    assert len(w.entries) == 256 and len(w.get(299)["ids"]) == 300


@pytest.mark.asyncio
async def test_audit_bounds_complete_evidence_and_keeps_review_reason(tmp_path):
    store = s.Store(tmp_path / "db")
    store.initialize()
    for i in range(3):
        rid = store.memorize("a:dm:u", "完整证据" * 800, ["a:u"], float(i), float(i))
        with store.connect() as db:
            store._add_fact(
                db,
                "a:dm:u",
                dict(
                    category="fact",
                    subject="a:u",
                    content=f"独立事实{i}",
                    reason="",
                    scenario="",
                    tags=[],
                    relations=[],
                    source_ids=[rid],
                ),
            )

    async def model(_, purpose, instruction, schema, payload):
        assert len(payload["facts"]) == 1 and len(payload["evidence"]) == 1
        assert payload["evidence"][0]["content"] == "完整证据" * 800
        fid = payload["facts"][0]["id"]
        return c.dump(
            {
                "actions": [
                    dict(
                        action="keep",
                        target_id=fid,
                        source_ids=[fid],
                        content=payload["facts"][0]["content"],
                        reason="原文证据充分，保留",
                        relations=None,
                    )
                ]
            }
        )

    cfg = c.Settings(compress_input_chars=4000)
    await e.Engine(store, lambda: cfg, model, None, None).audit("a:dm:u")
    assert len(store.facts("a:dm:u")) == 3
    history = store.edit_history("fact", [f["id"] for f in store.facts("a:dm:u")])
    assert (
        len(history) == 1
        and next(iter(history.values()))[0]["reason"] == "原文证据充分，保留"
    )
