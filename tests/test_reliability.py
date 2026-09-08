"""Regression coverage for production-reported memory failures."""

import asyncio
import json
import sqlite3
from contextlib import closing
import pytest
from test_memory import c, s, e


@pytest.mark.asyncio
async def test_compression_recovers_from_transient_provider_timeout(tmp_path):
    store = s.Store(tmp_path / "memory.db")
    store.initialize()
    store.capture(
        "qq:gm:188395693",
        "turn",
        [
            {
                "role": "user",
                "content": "今晚一起看星星",
                "users": ["qq:123"],
                "time": float(i),
            }
            for i in range(4)
        ],
    )
    calls = []

    async def provider(*args):
        calls.append(args)
        if len(calls) == 1:
            raise asyncio.TimeoutError()
        return '{"summary":"约好今晚看星星", "facts":[]}'

    cfg = c.Settings(threshold=4, batch_size=2, model_retries=1)
    engine = e.Engine(store, lambda: cfg, provider, None, None)
    await engine.compress("qq:gm:188395693")
    assert len(calls) == 2
    assert any(r["level"] == 1 for r in store.active("qq:gm:188395693"))
    assert store.status()["records"] == 5


@pytest.mark.asyncio
async def test_timeout_shrinks_batch_without_losing_any_source(tmp_path):
    store = s.Store(tmp_path / "memory.db")
    store.initialize()
    sid = "test:gm:1"
    store.capture(
        sid,
        "turn",
        [
            {
                "role": "user",
                "content": f"完整原文{i}",
                "users": ["test:u"],
                "time": float(i),
            }
            for i in range(8)
        ],
    )
    originals = store.active(sid)
    sizes = []

    async def provider(model, purpose, instruction, schema, payload):
        sizes.append(len(payload["records"]))
        if sizes[-1] > 2:
            raise TimeoutError()
        return c.dump({"summary": "合并前两条原文", "facts": []})

    cfg = c.Settings(threshold=8, batch_size=6, model_retries=2)
    await e.Engine(store, lambda: cfg, provider, None, None).compress(sid)
    assert sizes == [6, 3, 2]
    assert all(store.get(r["id"])["content"] == r["content"] for r in originals)
    archive = next(r for r in store.active(sid) if r["level"] == 1)
    assert len(store.get(archive["id"])["children"]) == 2


@pytest.mark.asyncio
async def test_failed_worker_has_actionable_detail_and_cooldown(tmp_path):
    store = s.Store(tmp_path / "memory.db")
    store.initialize()
    store.capture(
        "legacy:unscoped",
        "x",
        [
            {"role": "user", "content": "原文", "users": [], "time": 1.0}
            for _ in range(4)
        ],
    )

    async def provider(*args):
        raise TimeoutError()

    cfg = c.Settings(
        threshold=4, batch_size=2, model_retries=0, probability=0.0, audit_enabled=False
    )
    engine = e.Engine(store, lambda: cfg, provider, None, None)
    await engine.enqueue("compress", "legacy:unscoped")
    task = asyncio.create_task(engine.worker(0))
    try:
        async with asyncio.timeout(2):
            while store.status()["jobs"][0]["state"] != "failed":
                await asyncio.sleep(0.01)
        assert "原始记忆未丢失" in store.status()["jobs"][0]["detail"]
        assert (
            await engine.enqueue("compress", "legacy:unscoped", automatic=True) is None
        )
        assert len(store.active("legacy:unscoped")) == 4
        assert await engine.enqueue("compress", "legacy:unscoped")
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def test_upgrade_backup_and_name_history_do_not_rewrite_memories(tmp_path):
    path = tmp_path / "memory.db"
    store = s.Store(path)
    store.initialize()
    id = store.memorize("test:dm:u", "小明喜欢猫", ["test:u"], 1.0, 1.0)
    original = store.export()["records"]
    with store.connect() as db:
        db.execute("PRAGMA user_version=0")
    store.initialize()
    backup = path.with_name("memory.pre-v3.sqlite3")
    assert backup.exists()
    with closing(sqlite3.connect(backup)) as db:
        assert (
            db.execute("SELECT content FROM records WHERE id=?", (id,)).fetchone()[0]
            == "小明喜欢猫"
        )
    assert store.export()["records"] == original
    store.observe_name("test:u", "小明", observed=10.0)
    store.observe_name("test:u", "小夏", observed=20.0)
    store.observe_name("test:u", "过期消息里的名字", observed=15.0)
    store.observe_name("test:other", "小夏", observed=20.0)
    assert len(store.entities(query="小夏")) == 2
    n = store.entities(query="小明")[0]
    assert n["name"] == "小夏" and [h["name"] for h in n["history"]] == ["小夏", "小明"]
    assert store.search(scope="global", keyword="小夏")["items"][0]["id"] == id
    with pytest.raises(s.Conflict):
        store.observe_name("test:u", "过期编辑", revision=1)
    before = backup.read_bytes()
    store.initialize()
    assert backup.read_bytes() == before and store.export()["records"] == original


def test_weak_legacy_relation_quarantined_and_audit_can_repair(tmp_path):
    store = s.Store(tmp_path / "memory.db")
    store.initialize()
    source = store.memorize("test:dm:u", "小明明确说阿澄是朋友", ["test:u"], 1.0, 1.0)
    bad = {"subject": "Bot", "predicate": "认为", "object": "阿澄"}
    with pytest.raises(ValueError):
        c.Relation.model_validate(bad)
    fact = {
        "category": "relationship",
        "subject": "test:u",
        "content": "阿澄是小明的朋友",
        "reason": "",
        "scenario": "",
        "tags": [],
        "relations": [bad],
        "source_ids": [source],
    }
    with store.connect() as db:
        id = store._add_fact(db, "test:dm:u", fact)
    rows = store.facts("test:dm:u")
    assert rows[0]["relations"] == [bad] and not rows[0]["verified_relations"]
    from alife_test_plugin.retrieval import safe_facts

    assert safe_facts(rows)[0]["relations"] == []
    good = {"subject": "test:u", "predicate": "朋友", "object": "阿澄"}
    output = c.Audit.model_validate(
        {
            "actions": [
                {
                    "action": "correct",
                    "target_id": id,
                    "source_ids": [id],
                    "content": fact["content"],
                    "reason": "依据原文修正关系主体",
                    "relations": [good],
                }
            ]
        }
    )
    store.audit(rows, output.model_dump())
    assert store.facts("test:dm:u")[0]["verified_relations"] == [good]
    assert json.loads(store.export()["versions"][0]["snapshot"])["relations"] == [bad]


def test_delete_removes_fact_when_its_last_live_source_is_deleted(tmp_path):
    store = s.Store(tmp_path / "memory.db")
    store.initialize()
    source = store.memorize("test:dm:u", "待删除记忆", ["test:u"], 1.0, 1.0)
    fact = {
        "category": "fact",
        "subject": "test:u",
        "content": "待删除记忆",
        "reason": "",
        "scenario": "",
        "tags": [],
        "relations": [],
        "source_ids": [source],
    }
    with store.connect() as db:
        store._add_fact(db, "test:dm:u", fact)
    store.edit("record", source, 1, {"deleted": True}, "删除验证")
    assert store.get(source) is None
    assert store.search(scope="global")["total"] == 0 and store.facts("test:dm:u") == []
    assert store.export()["records"][0]["content"] == "待删除记忆"


def test_linked_identity_scope_and_out_of_order_same_name_observations(tmp_path):
    store = s.Store(tmp_path / "memory.db")
    store.initialize()
    id = store.memorize(
        "test:gm:elsewhere", "两个人的约定", ["test:u", "test:v"], 1.0, 1.0
    )
    store.edit("record", id, 1, {"active": False}, "移出常驻但仍允许检索")
    store.observe_name("test:v", "新昵称", observed=10.0)
    store.observe_name("test:v", "新昵称", observed=30.0)
    store.observe_name("test:v", "迟到的旧昵称", observed=20.0)
    assert store.entities(ids=["test:v"])[0]["name"] == "新昵称"
    assert "test:v" in store.entity_ids("test:dm:u", ["test:u"], "linked")
    assert "test:v" not in store.entity_ids("test:dm:u", ["test:u"], "session")


def test_single_character_chinese_search_still_recalls(tmp_path):
    store = s.Store(tmp_path / "memory.db")
    store.initialize()
    id = store.memorize("test:gm:cats", "我喜欢猫", ["test:u"], 1.0, 1.0)
    assert (
        store.search("another:dm:v", scope="global", lexical="猫")["items"][0]["id"]
        == id
    )
    assert not store.search("another:dm:v", scope="global", lexical="狗")["items"]


@pytest.mark.asyncio
async def test_mixed_model_failures_share_one_retry_budget(tmp_path):
    store = s.Store(tmp_path / "memory.db")
    store.initialize()
    store.capture(
        "a:dm:u",
        "x",
        [
            {"role": "user", "content": "记录", "users": [], "time": 1.0}
            for _ in range(4)
        ],
    )
    calls = []

    async def provider(*args):
        calls.append(1)
        if len(calls) == 2:
            raise TimeoutError()
        return "``` invalid JSON ```"

    cfg = c.Settings(threshold=4, batch_size=2, model_retries=2)
    with pytest.raises(ValueError, match="structured_output_rejected"):
        await e.Engine(store, lambda: cfg, provider, None, None).compress("a:dm:u")
    assert len(calls) == 3 and len(store.active("a:dm:u")) == 4
