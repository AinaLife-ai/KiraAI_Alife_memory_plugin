"""轮换槽位（v2.15.0）：只从过门槛的池子里取、池空留空、反馈驱动换批。"""

import asyncio
import importlib
import json
import os
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_rotate_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_rotate_test", package)
storage = importlib.import_module("alife_rotate_test.storage")
retrieval = importlib.import_module("alife_rotate_test.retrieval")


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def _store(tmp_path):
    store = storage.Store(tmp_path / "db.sqlite3")
    store.initialize()
    return store


def _record(store, rid, content="内容"):
    with store.connect() as db:
        db.execute(
            "INSERT INTO records(id,sid,role,level,start,end,summary,content,users,"
            "speaker,position,created,search_body) "
            "VALUES (?,?,'user',0,1,1,?,?,'[]','',0,1,'')",
            (rid, "qq:gm:1", content, content),
        )


class RotationPickTests(unittest.TestCase):
    """挑选顺序就是"学习"：没展示过的优先，用过比例高的次之。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = _store(Path(self.tmp.name))
        for rid in ("never", "used", "ignored"):
            _record(self.store, rid)

    def tearDown(self):
        self.tmp.cleanup()

    def test_unseen_first_then_hit_rate(self):
        self.store.mark_rotation(["used"], ["used"])       # 展示1 用1
        self.store.mark_rotation(["ignored"], [])          # 展示1 用0
        picked = self.store.rotation_pick(["used", "ignored", "never"], 2)
        self.assertEqual(picked, ["never", "used"])

    def test_empty_pool_returns_nothing(self):
        self.assertEqual(self.store.rotation_pick([], 3), [])


def test_overlap_detection_for_feedback():
    """反馈信号：独特数字串命中 1 个即算；普通词元要够 min_hits。"""
    assert retrieval.overlap_hit("香菇QQ3322046929 尾巴会摇", "香菇的QQ是3322046929", 2)
    assert retrieval.overlap_hit("香菇自称尾巴会摇", "香菇说尾巴会摇", 2)
    assert not retrieval.overlap_hit("香菇自称尾巴会摇", "今天天气不错", 2)


@pytest.mark.asyncio
async def test_rotation_slot_behaviour(tmp_path):
    """池空留空 / 留批 / 用到就换 / 留满也换 / seen 与冷却过滤。"""
    if not os.environ.get("KIRA_CORE"):
        pytest.skip("set KIRA_CORE for host integration")
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_helpers_plugin import build_plugin

    plugin, store = await build_plugin(tmp_path)
    try:
        for rid, text in (("a", "香菇尾巴会摇"), ("b", "老汤圆被踢三次"), ("c", "紫小贱捏脸")):
            _record(store, rid, text)
        cfg = plugin.settings
        cfg.rotate_enabled = True
        cfg.rotate_count = 3
        cfg.rotate_keep_rounds = 3
        cfg.rotate_cooldown_rounds = 10
        pool = [
            {"id": "a", "content": "香菇尾巴会摇"},
            {"id": "b", "content": "老汤圆被踢三次"},
            {"id": "c", "content": "紫小贱捏脸"},
        ]
        key = ("qq:gm:1", ("qq:1",), "global")
        first = await plugin.rotation_extras("qq:gm:1", cfg, pool, key, lambda r: r["content"])
        assert [r["id"] for r in first] == ["a", "b", "c"], first
        # 没被用到 → 同一批继续留（不再重新挑）
        again = await plugin.rotation_extras("qq:gm:1", cfg, pool, key, lambda r: r["content"])
        assert [r["id"] for r in again] == ["a", "b", "c"]
        # seen 已经记住它们 → 换批时池子里没有"新的"了 → 留空
        store.mark_rotation([], ["a"])
        plugin.rotation["qq:gm:1"]["next"] = True
        empty = await plugin.rotation_extras("qq:gm:1", cfg, pool, key, lambda r: r["content"])
        assert empty == []
        # 新候选进来 → 冷却中的 a 不会再被挑
        _record(store, "d", "新的候选")
        pool2 = pool + [{"id": "d", "content": "新的候选"}]
        picked = await plugin.rotation_extras("qq:gm:1", cfg, pool2, key, lambda r: r["content"])
        assert [r["id"] for r in picked] == ["d"], picked
    finally:
        await plugin.terminate()


@pytest.mark.asyncio
async def test_feedback_flips_to_next_batch(tmp_path):
    """回复里用到了 → 下轮换批；一直没用 → 留满 keep_rounds 也换。"""
    if not os.environ.get("KIRA_CORE"):
        pytest.skip("set KIRA_CORE for host integration")
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_helpers_plugin import build_plugin

    plugin, store = await build_plugin(tmp_path)
    try:
        cfg = plugin.settings
        cfg.rotate_keep_rounds = 3
        cfg.rotate_min_hits = 2
        _record(store, "a", "香菇尾巴会摇")
        plugin.rotation["qq:gm:1"] = {
            "rows": [{"id": "a"}], "texts": {"a": "香菇尾巴会摇"},
            "ids": ["a"], "rounds": 0, "next": False, "cooldown": {}, "touch": 0,
        }
        await plugin.rotation_feedback("qq:gm:1", "今天风挺大")
        assert plugin.rotation["qq:gm:1"]["next"] is False  # 没用上 → 继续留
        assert plugin.rotation["qq:gm:1"]["rounds"] == 1
        await plugin.rotation_feedback("qq:gm:1", "香菇的尾巴会摇")
        assert plugin.rotation["qq:gm:1"]["next"] is True  # 用上了 → 换批
        plugin.rotation["qq:gm:1"]["next"] = False
        plugin.rotation["qq:gm:1"]["rounds"] = 3
        await plugin.rotation_feedback("qq:gm:1", "今天风挺大")
        assert plugin.rotation["qq:gm:1"]["next"] is True, "留满 keep_rounds 也要换批"
    finally:
        await plugin.terminate()


def test_pure_order_matches_sql_pick(tmp_path):
    """A 方案的安全阀：纯内存排序必须与 storage.rotation_pick 结果完全一致。

    两者同一套顺序；谁只改了其中一处，这条立刻红灯。
    """
    store = _store(tmp_path)
    ids = ["a", "b", "c", "d", "e"]
    for rid in ids:
        _record(store, rid)
    store.mark_rotation(["b"], ["b"])
    store.mark_rotation(["c"], [])
    store.mark_rotation(["d", "d", "d"], [])
    store.mark_rotation(["e"], ["e"])
    store.mark_rotation(["e"], ["e"])
    snapshot = store.rotation_stats()
    shown = {rid: stat[0] for rid, stat in snapshot.items()}
    used = {rid: stat[1] for rid, stat in snapshot.items()}
    for limit in (1, 3, 5):
        assert retrieval.rotation_order(ids, shown, used, limit) == store.rotation_pick(
            ids, limit
        ), limit
    assert retrieval.rotation_order(["x", "a"], {}, {}, 2) == ["x", "a"]
