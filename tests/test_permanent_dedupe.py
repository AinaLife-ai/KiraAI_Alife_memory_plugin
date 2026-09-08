"""Similar permanent memories are folded by the audit model, newest wins."""

import asyncio
import importlib
import json
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_dedupe_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_dedupe_test", package)
s = importlib.import_module("alife_dedupe_test.storage")
e = importlib.import_module("alife_dedupe_test.engine")
c = importlib.import_module("alife_dedupe_test.contracts")
r = importlib.import_module("alife_dedupe_test.retrieval")

SID = "qq:gm:188395693"
TEXTS = [
    "AI数量是10+1个bot，以AI花名册为准（之前说的七姐妹已过时）",
    "AI总数是10+1个bot，其中+1就是喵梓，由萧洋和主人一起养",
    "AI花名册10+1个bot中，第十个常规bot是爱亲理（QQ 1845575735）",
]


def seed(store):
    return [
        store.memorize(SID, text, ["qq:769690776"], float(i), float(i))
        for i, text in enumerate(TEXTS)
    ]


def test_dedupe_runs_on_its_own_job_lane(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    store.enqueue("dedupe", SID)
    store.enqueue("compress", SID)
    assert store.claim(exclude=("dedupe",))["kind"] == "compress"
    assert store.claim(kind="dedupe")["kind"] == "dedupe"
    assert store.claim() is None


def test_similarity_and_clustering():
    assert r.similarity(TEXTS[0], TEXTS[1]) > 0.3
    assert r.similarity(TEXTS[0], "今天天气不错适合出门散步") == 0.0
    rows = [{"id": f"r{i}", "summary": t} for i, t in enumerate(TEXTS)]
    clusters = e.permanent_clusters(rows, 0.3)
    assert len(clusters) == 1 and len(clusters[0]) == 3
    assert e.permanent_clusters(rows + [{"id": "x", "summary": "完全无关的另一件事"}], 0.3)[0][0]["id"] == "r0"


def test_merge_records_keeps_newest_and_archives_rest(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    ids = seed(store)
    newest = ids[-1]
    result = store.merge_records(
        newest, ids, "AI花名册：10+1 个 bot（以主人最新确认为准）", "同一花名册的多次更正"
    )
    assert result == {"target": newest, "folded": 3}
    rows = {row["id"]: row for row in store.export()["records"]}
    assert rows[newest]["summary"].startswith("AI花名册")
    assert rows[newest]["active"] == 1
    assert rows[newest]["content"] == TEXTS[-1]  # archive text untouched
    for old in ids[:-1]:
        assert rows[old]["active"] == 0 and rows[old]["content"] in TEXTS
    assert [row["id"] for row in store.permanent_records(SID)] == [newest]
    history = store.edit_history("record", ids)
    assert all(history.get(rid) for rid in ids)


@pytest.mark.asyncio
async def test_consolidate_merges_only_when_model_says_so(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    ids = seed(store)

    async def keep_model(*args):
        return json.dumps(
            {"action": "keep", "content": "", "reason": "只是话题相近", "source_ids": ids[:2]},
            ensure_ascii=False,
        )

    cfg = c.Settings(dedupe_threshold=0.3, model_retries=0)
    engine = e.Engine(store, lambda: cfg, keep_model, None, None)
    await engine.consolidate(SID)
    assert len(store.permanent_records(SID)) == 3

    async def merge_model(*args):
        return json.dumps(
            {
                "action": "merge",
                "content": "AI花名册：10+1 个 bot，其中 +1 是喵梓；第十个是爱亲理",
                "reason": "同一花名册的更正",
                "source_ids": ids,
            },
            ensure_ascii=False,
        )

    engine = e.Engine(store, lambda: cfg, merge_model, None, None)
    await engine.consolidate(SID)
    remaining = store.permanent_records(SID)
    assert len(remaining) == 1 and remaining[0]["id"] == ids[-1]
    assert "喵梓" in remaining[0]["summary"]


@pytest.mark.asyncio
async def test_consolidate_rejects_invented_ids(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    seed(store)

    async def bad_model(*args):
        return json.dumps(
            {"action": "merge", "content": "合并", "reason": "x", "source_ids": ["not-a-real-id", "r2"]},
            ensure_ascii=False,
        )

    cfg = c.Settings(dedupe_threshold=0.3, model_retries=0)
    engine = e.Engine(store, lambda: cfg, bad_model, None, None)
    with pytest.raises(Exception):
        await engine.consolidate(SID)
    assert len(store.permanent_records(SID)) == 3


def test_dedupe_disabled_leaves_memories_alone(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    seed(store)

    async def merge_model(*args):
        raise AssertionError("model must not be called when dedupe is off")

    cfg = c.Settings(permanent_dedupe=False, model_retries=0)
    engine = e.Engine(store, lambda: cfg, merge_model, None, None)
    asyncio.run(engine.consolidate(SID))
    assert len(store.permanent_records(SID)) == 3
