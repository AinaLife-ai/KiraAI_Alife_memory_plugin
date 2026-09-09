"""Context hygiene: tool payloads never flood the injected memory block."""

import asyncio
import importlib
import json
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_hygiene_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_hygiene_test", package)
s = importlib.import_module("alife_hygiene_test.storage")
r = importlib.import_module("alife_hygiene_test.retrieval")
e = importlib.import_module("alife_hygiene_test.engine")
c = importlib.import_module("alife_hygiene_test.contracts")

PREFIX = r.TOOL_RESULT_PREFIX
SID = "qq:gm:188395693"


def seed(store):
    echo = '{"ok":true,"total":52,"items":[],"next_page":null,"archives_in_context":3}'
    store.capture(
        SID,
        "echo",
        [{"role": "assistant", "content": PREFIX + "\n" + echo, "time": 1.0, "users": []}],
    )
    big = '{"weather":"' + "x" * 900 + '"}'
    store.capture(
        SID,
        "big",
        [{"role": "assistant", "content": PREFIX + "\n" + big, "time": 2.0, "users": []}],
    )
    calls = json.dumps(
        {
            "tool_calls": [
                {
                    "function": {
                        "name": "SearchMemoryArchive",
                        "arguments": '{"keyword":"COM3D2"}',
                    }
                }
            ]
        }
    )
    store.capture(
        SID,
        "calls",
        [{"role": "assistant", "content": "好的\n" + calls, "time": 3.0, "users": []}],
    )
    store.capture(
        SID,
        "plain",
        [{"role": "user", "content": "今天天气不错", "time": 4.0, "users": []}],
    )
    return big, calls


def test_capture_keeps_summary_and_content_separate(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    store.capture(
        SID,
        "ev",
        [
            {
                "role": "assistant",
                "content": "完整原文",
                "summary": "短摘要",
                "time": 1.0,
                "users": [],
            }
        ],
    )
    row = store.active(SID)[0]
    assert row["summary"] == "短摘要" and row["content"] == "完整原文"


def test_cleanup_removes_echoes_and_compacts_tool_summaries(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    big, calls = seed(store)
    report = store.cleanup_tool_records()
    assert report["removed"] == 1 and report["rewritten"] == 2
    assert report["freed_chars"] > 800

    echo = next(
        rec
        for rec in store.export()["records"]
        if rec["content"].startswith(PREFIX)
        and '"archives_in_context"' in rec["content"]
    )
    assert echo["deleted"] == 1

    big_row = next(rec for rec in store.export()["records"] if rec["content"] == PREFIX + "\n" + big)
    assert big_row["deleted"] == 0
    assert big_row["content"] == PREFIX + "\n" + big  # raw text untouched
    assert big_row["summary"] == PREFIX + r.tool_preview(big)
    assert len(big_row["summary"]) < 300

    calls_row = next(
        rec for rec in store.export()["records"] if rec["content"].endswith(calls)
    )
    assert calls_row["summary"] == '好的 [调用工具：SearchMemoryArchive({"keyword":"COM3D2"})]'
    assert calls_row["content"] == "好的\n" + calls

    plain = next(rec for rec in store.export()["records"] if rec["content"] == "今天天气不错")
    assert plain["summary"] == plain["content"] == "今天天气不错"


def test_cleanup_is_idempotent_and_drops_orphan_facts(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    seed(store)
    echo = next(
        rec
        for rec in store.export()["records"]
        if rec["content"].startswith(PREFIX) and '"archives_in_context"' in rec["content"]
    )
    with store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        store._add_fact(
            db,
            SID,
            {
                "category": "fact",
                "subject": "qq:1",
                "content": "来自回声的事实",
                "reason": "",
                "scenario": "",
                "tags": [],
                "relations": [],
                "source_ids": [echo["id"]],
            },
        )
    assert len(store.facts(SID, limit=10)) == 1
    store.cleanup_tool_records()
    assert store.facts(SID, limit=10) == []
    again = store.cleanup_tool_records()
    assert again["removed"] == 0 and again["rewritten"] == 0
    assert again["freed_chars"] == 0


def test_totals_and_top_users_respect_scope(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    for uid, count in (("qq:1", 3), ("qq:2", 1)):
        for i in range(count):
            store.capture(
                SID,
                f"{uid}-{i}",
                [
                    {
                        "role": "user",
                        "content": f"{uid} 的第 {i} 条",
                        "time": float(i),
                        "users": [uid],
                    }
                ],
            )
    store.capture(
        "qq:dm:9",
        "x",
        [{"role": "user", "content": "私聊消息", "time": 1.0, "users": ["qq:9"]}],
    )
    session = store.totals(SID, ["qq:1"], "session")
    assert session["records"] == 4 and session["users"] == 2
    assert session["sessions"] == 1 and session["groups"] == 1
    whole = store.totals(SID, ["qq:1"], "global")
    assert whole["records"] == 5 and whole["users"] == 3 and whole["sessions"] == 2


def test_search_hides_archived_until_explicit(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    for i in range(4):
        store.capture(
            SID,
            f"ev{i}",
            [
                {
                    "role": "user",
                    "content": f"绿岛酒吧 第{i}条经历",
                    "time": float(i),
                    "users": ["qq:1"],
                }
            ],
        )
    cfg = c.Settings(threshold=4, batch_size=2, model_retries=0)

    async def model(*args):
        return json.dumps(
            {"summary": "两条经历的合并摘要：绿岛酒吧", "facts": []}, ensure_ascii=False
        )

    asyncio.run(e.Engine(store, lambda: cfg, model, None, None).compress(SID))
    archived = [
        row
        for row in store.export()["records"]
        if row["level"] == 0 and row["active"] == 0
    ]
    assert len(archived) == 2

    live = store.search(keyword="绿岛酒吧", scope="global", limit=10, active=True)
    assert live["total"] == 3 and all(item["active"] == 1 for item in live["items"])
    everything = store.search(keyword="绿岛酒吧", scope="global", limit=10)
    assert everything["total"] == 5


def test_forget_cold_archives_and_id_read_still_works(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    record_id = store.memorize(SID, "主人喜欢乌龙茶", ["qq:1"], 1.0, 1.0)
    row = store.get(record_id)
    store.edit("record", record_id, row["revision"], {"active": False}, "forget")
    cold = store.get(record_id)
    assert cold["cold"] == 1 and cold["active"] == 0 and cold["archived_at"] > 0
    # Cold archives never show up in search, even with archived included.
    assert store.search(keyword="乌龙茶", scope="global")["total"] == 0
    assert store.search(keyword="乌龙茶", scope="global", cold_after_days=0)["total"] == 0
    assert store.get(record_id) is not None  # ...but reading by id still works
    store.edit("record", record_id, cold["revision"], {"active": True}, "restore")
    revived = store.get(record_id)
    assert revived["cold"] == 0 and revived["archived_at"] == 0
    assert store.search(keyword="乌龙茶", scope="global")["total"] == 1


def test_archived_fades_to_cold_after_days(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    for i in range(4):
        store.capture(
            SID,
            f"ev{i}",
            [
                {
                    "role": "user",
                    "content": f"绿岛酒吧 第{i}条经历",
                    "time": float(i),
                    "users": ["qq:1"],
                }
            ],
        )
    cfg = c.Settings(threshold=4, batch_size=2, model_retries=0)

    async def model(*args):
        return json.dumps({"summary": "绿岛酒吧 合并摘要", "facts": []}, ensure_ascii=False)

    asyncio.run(e.Engine(store, lambda: cfg, model, None, None).compress(SID))
    archived = [
        row
        for row in store.export()["records"]
        if row["level"] == 0 and row["active"] == 0
    ]
    assert archived and all(row["cold"] == 0 for row in archived)
    assert (
        store.search(keyword="绿岛酒吧", scope="global", cold_after_days=180)["total"]
        == 5
    )
    with store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute(
            "UPDATE records SET archived_at=? WHERE id=?", (1.0, archived[0]["id"])
        )
    assert (
        store.search(keyword="绿岛酒吧", scope="global", cold_after_days=180)["total"]
        == 4
    )
    assert (
        store.search(keyword="绿岛酒吧", scope="global", cold_after_days=0)["total"]
        == 5
    )
    assert store.get(archived[0]["id"]) is not None


def test_session_affinity_reorders_without_narrowing(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    for sid in ("qq:gm:com", "qq:gm:newbot"):
        for i in range(2):
            store.capture(
                sid,
                f"ev{i}",
                [
                    {
                        "role": "user",
                        "content": "COM3D2 群里的绿岛酒吧趣事",
                        "time": float(i),
                        "users": ["qq:1"],
                    }
                ],
            )
    plain = store.search(lexical="COM3D2 绿岛酒吧", scope="global", limit=4)
    assert {item["sid"] for item in plain["items"]} == {"qq:gm:com", "qq:gm:newbot"}
    preferred = store.search(
        lexical="COM3D2 绿岛酒吧",
        scope="global",
        limit=4,
        prefer_sid="qq:gm:com",
        prefer_users=["qq:1"],
    )
    assert [item["sid"] for item in preferred["items"]] == [
        "qq:gm:com",
        "qq:gm:com",
        "qq:gm:newbot",
        "qq:gm:newbot",
    ]
    assert preferred["total"] == plain["total"]


def test_audit_scheduler_picks_stalest_sessions(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    for sid, audited in (("a:dm:1", 0.0), ("a:dm:2", 100.0), ("a:dm:3", 200.0)):
        store.capture(
            sid,
            "e",
            [{"role": "user", "content": "x", "time": 1.0, "users": []}],
        )
        with store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            store._add_fact(
                db,
                sid,
                {
                    "category": "fact",
                    "subject": "s",
                    "content": "c",
                    "reason": "",
                    "scenario": "",
                    "tags": [],
                    "relations": [],
                    "source_ids": [],
                },
            )
            db.execute("UPDATE facts SET audited=? WHERE sid=?", (audited, sid))
    assert store.sessions_by_audit_age(2) == ["a:dm:1", "a:dm:2"]
