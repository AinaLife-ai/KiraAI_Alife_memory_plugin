"""跨类别去重（v2.9.0 追加）：同主体分档阈值 + 三动作无 keep + 证据开关。"""

import asyncio
import importlib
import json
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_cross_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_cross_test", package)
s = importlib.import_module("alife_cross_test.storage")
e = importlib.import_module("alife_cross_test.engine")
c = importlib.import_module("alife_cross_test.contracts")


def seed(store, sid="qq:gm:1"):
    ids = []
    rows = [
        ("preference", "星月最喜欢吃草莓蛋糕，发草莓蛋糕就能安抚她"),
        ("event", "星月最喜欢吃草莓蛋糕，发草莓蛋糕可以安抚她"),
        ("event", "完全不相关的一条：昨天群主改了群公告"),
    ]
    store.capture(sid, "turn", [
        {"role": "user", "content": text, "users": ["qq:9"], "time": float(i)}
        for i, (_, text) in enumerate(rows)
    ])
    records = store.active(sid)
    with store.connect() as db:
        for index, (category, text) in enumerate(rows):
            ids.append(store._add_fact(db, sid, {
                "category": category, "subject": "qq:9", "content": text,
                "reason": "", "scenario": "", "tags": [], "relations": [],
                "source_ids": [records[index]["id"]], "importance": 5,
            }))
    return ids


def test_cross_category_pairs_only_with_higher_threshold(tmp_path):
    store = s.Store(tmp_path / "db")
    store.initialize()
    a, b, unrelated = seed(store)
    # 同主体、跨类别：0.4 才进来
    assert set(store.flag_similar_pairs([a, b], 0.25, 0.4)) == {a, b}
    # 关掉跨类别（cross_threshold=0）就只认同类别的
    assert store.flag_similar_pairs([a, b], 0.25, 0.0) == []
    # 阈值高于实际相似度时也不标记
    assert store.flag_similar_pairs([a, b], 0.25, 0.99) == []
    # 不相关的不会被牵连
    assert unrelated not in store.flag_similar_pairs([a, b, unrelated], 0.25, 0.4)


def test_clusters_allow_cross_category_but_never_cross_subject():
    rows = [
        {"id": "1", "subject": "qq:9", "category": "preference", "content": "星月最喜欢吃草莓蛋糕"},
        {"id": "2", "subject": "qq:9", "category": "event", "content": "星月最喜欢吃草莓蛋糕"},
        {"id": "3", "subject": "qq:8", "category": "preference", "content": "星月最喜欢吃草莓蛋糕"},
    ]
    groups = e.Engine._fact_clusters(rows, 0.25, 0.4)
    assert [sorted(g["id"] for g in group) for group in groups] == [["1", "2"]]
    # 关掉跨类别 → 不再成组
    assert e.Engine._fact_clusters(rows, 0.25, 0.0) == []
