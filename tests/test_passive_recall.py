"""v2.6.0 被动召回：事实内容匹配 + 归档参与检索。

两个改动的共同目标：让「被动召回」更全，但仍尊重软删、冷归档与超龄淡出。
"""

import importlib
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_recall_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_recall_test", package)
c = importlib.import_module("alife_recall_test.contracts")
s = importlib.import_module("alife_recall_test.storage")

SID = "qq:gm:1"
OTHER = "qq:gm:2"
USERS = ["qq:1001"]


def new_store(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    store.capture(
        SID, "turn", [{"role": "user", "content": "开场白", "time": 1.0, "users": USERS}]
    )
    return store


def add_fact(store, category, subject, content, importance=6):
    with store.connect() as db:
        store._add_fact(
            db,
            SID,
            {
                "category": category,
                "subject": subject,
                "content": content,
                "reason": "测试",
                "scenario": "",
                "relations": [],
                "importance": importance,
                "tags": [],
                "source_ids": [store.active(SID)[0]["id"]],
            },
        )


def archive_other_session(store, text, summary=None):
    store.capture(
        OTHER, "turn", [{"role": "user", "content": text, "time": 2.0, "users": USERS}]
    )
    ids = [r["id"] for r in store.active(OTHER)]
    if summary:
        store.compress(
            OTHER, [store.get(i) for i in ids], 0, {"summary": summary, "facts": []}
        )
    return ids


def recall(store, query, min_score=2):
    return store.facts(
        SID,
        global_scope=True,
        users=USERS,
        lexical=query,
        limit=5,
        importance_first=True,
        min_score=min_score,
    )


def test_fact_content_matching_works_without_entity_name(tmp_path):
    """「你师傅是谁」→ 事实「我师傅是星月」（主体和名字都没出现）也要召回。"""
    store = new_store(tmp_path)
    add_fact(store, "relationship", "qq:1001", "我师傅是星月")
    query = "你师傅是谁"

    # 类别通道与实体通道都够不着它
    assert store.facts(SID, category="profile", limit=10) == []
    assert store.entity_ids_for_query(query, SID, USERS, "global") == []

    assert [f["content"] for f in recall(store, query)] == ["我师傅是星月"]


def test_min_score_filters_weak_matches(tmp_path):
    """门槛生效：只命中一个双字片段（2 分）的，在门槛 4 时被挡掉。"""
    store = new_store(tmp_path)
    add_fact(store, "event", "qq:1001", "师傅上周来工作室看了作品")
    query = "你师傅是谁"

    assert [f["content"] for f in recall(store, query, min_score=2)] == [
        "师傅上周来工作室看了作品"
    ]
    assert recall(store, query, min_score=4) == []
    assert len(recall(store, query, min_score=0)) == 1


def test_soft_deleted_facts_never_recalled(tmp_path):
    """审计撤回 / 合并（软删）的事实不参与被动召回。"""
    store = new_store(tmp_path)
    add_fact(store, "relationship", "qq:1001", "我师傅是星月")
    fact = store.facts(SID, global_scope=True, users=USERS)[0]
    store.edit("fact", fact["id"], fact["revision"], {"deleted": True}, "审计撤回")
    assert recall(store, "你师傅是谁") == []


def test_archived_records_join_search_when_configured(tmp_path):
    """归档也参与检索；只搜常驻时不该出现离开上下文的原文。"""
    store = new_store(tmp_path)
    archive_other_session(store, "我师傅是星月，他跟了三年", summary="聊到师傅与星月的近况")
    query = "我师傅是星月"

    def run(active):
        return store.search(
            SID,
            lexical=query,
            scope="global",
            users=USERS,
            limit=10,
            exclude_sid=SID,
            active=active,
            cold_after_days=180,
        )["items"]

    assert any(not i["active"] for i in run(False)), "应带回离开上下文的原文"
    assert all(i["active"] for i in run(True)), "只搜常驻时不该有归档"


def test_search_prefers_active_on_equal_score(tmp_path):
    """同分时常驻优先：归档原文不会挤掉还在上下文里的存档。"""
    store = new_store(tmp_path)
    archive_other_session(store, "我师傅是星月", summary="我师傅是星月")  # 已离开上下文
    store.capture(
        OTHER, "turn", [{"role": "user", "content": "我师傅是星月", "time": 9.0, "users": USERS}]
    )
    items = store.search(
        SID,
        lexical="我师傅是星月",
        scope="global",
        users=USERS,
        limit=10,
        exclude_sid=SID,
        active=False,
        cold_after_days=180,
    )["items"]
    assert len(items) >= 2
    assert items[0]["active"] == 1


def search_all(store, query):
    return store.search(
        SID,
        lexical=query,
        scope="global",
        users=USERS,
        limit=10,
        exclude_sid=SID,
        active=False,
        cold_after_days=180,
    )["items"]


def test_cold_archives_stay_out(tmp_path):
    """冷归档（被合并的永久记忆）即使开关打开也搜不到。"""
    store = new_store(tmp_path)
    folded = store.memorize(OTHER, "我师傅是星月", USERS, 5.0, 6.0)
    kept = store.memorize(OTHER, "今天天气不错", USERS, 7.0, 8.0)
    store.merge_records(kept, [folded, kept], "今天天气不错", "合并相似永久记忆")
    assert store.get(folded)["cold"] == 1, "被并入的永久记忆应转为冷归档"
    assert search_all(store, "我师傅是星月") == []


def test_soft_deleted_archives_stay_out(tmp_path):
    """软删的存档不参与检索。"""
    store = new_store(tmp_path)
    ids = archive_other_session(store, "我师傅是星月")
    row = store.get(ids[0])
    store.edit("record", ids[0], row["revision"], {"deleted": True}, "删除")
    assert search_all(store, "我师傅是星月") == []


def test_defaults_match_agreed_behaviour():
    """默认值 = 这次拍板的结果。"""
    cfg = c.Settings()
    assert cfg.fact_recall_min_score == 2
    assert cfg.search_active_only is False
