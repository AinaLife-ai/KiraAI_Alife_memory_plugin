"""Profile aggregation, importance ordering and entity resolution from text."""

import importlib
import sys
import tempfile
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_profile_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_profile_test", package)
s = importlib.import_module("alife_profile_test.storage")


class ProfileCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = s.Store(Path(self.temp.name) / "m.db")
        self.store.initialize()

    def tearDown(self):
        self.temp.cleanup()

    def add_fact(self, sid, subject, content, category="fact", importance=5, relations=None):
        self.store.capture(
            sid, "t", [{"role": "user", "content": content, "users": [subject], "time": 1.0}]
        )
        record = self.store.active(sid)[-1]
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            return self.store._add_fact(
                db,
                sid,
                {
                    "category": category,
                    "subject": subject,
                    "content": content,
                    "reason": "",
                    "scenario": "",
                    "tags": [],
                    "relations": relations or [],
                    "source_ids": [record["id"]],
                    "importance": importance,
                },
            )

    def test_profile_groups_facts_by_category(self):
        self.add_fact("qq:gm:1", "qq:9", "是群里的老成员", "profile", 9)
        self.add_fact("qq:gm:1", "qq:9", "对花生过敏", "preference", 8)
        self.add_fact("qq:gm:2", "qq:9", "喜欢甜口蛋糕", "preference", 7)
        self.store.observe_name("qq:9", "萤火", source="admin")
        profile = self.store.profile("qq:9", 3)
        self.assertEqual(profile["entity"]["name"], "萤火")
        self.assertEqual(sorted(profile["categories"]), ["preference", "profile"])
        self.assertEqual(profile["stats"]["facts"], 3)
        self.assertEqual(profile["stats"]["sessions"], 2)
        self.assertEqual(profile["summary"][0], "是群里的老成员")

    def test_summary_uses_importance_then_recency(self):
        self.add_fact("qq:gm:1", "qq:9", "低重要度", "fact", 2)
        self.add_fact("qq:gm:1", "qq:9", "高重要度", "fact", 9)
        self.add_fact("qq:gm:1", "qq:9", "中重要度", "fact", 5)
        profile = self.store.profile("qq:9", 2)
        self.assertEqual(profile["summary"], ["高重要度", "中重要度"])

    def test_profile_collects_relations(self):
        self.add_fact(
            "qq:gm:1", "qq:9", "和阿远是朋友", "relationship", 6,
            relations=[{"subject": "qq:9", "predicate": "朋友", "object": "qq:1"}],
        )
        profile = self.store.profile("qq:9", 3)
        self.assertEqual(len(profile["relations"]), 1)
        self.assertEqual(profile["relations"][0]["object"], "qq:1")
        self.assertEqual(profile["stats"]["relations"], 1)

    def test_session_scope_limits_facts(self):
        self.add_fact("qq:gm:1", "qq:9", "本会话事实")
        self.add_fact("qq:gm:2", "qq:9", "别的会话事实")
        scoped = self.store.profile("qq:9", 3, sid="qq:gm:1", global_scope=False)
        self.assertEqual([f["content"] for f in scoped["categories"]["fact"]], ["本会话事实"])

    def test_entity_ids_for_query_matches_alias(self):
        self.add_fact("qq:gm:1", "qq:9", "事实")
        self.store.observe_name("qq:9", "萤火", source="admin")
        self.store.observe_name("qq:9", "小萤", source="admin")
        ids = self.store.entity_ids_for_query("小萤最近怎么样", "qq:gm:1", ["qq:9"], "global")
        self.assertEqual(ids, ["qq:9"])
        self.assertEqual(
            self.store.entity_ids_for_query("无关的一句话", "qq:gm:1", ["qq:9"], "global"),
            [],
        )

    def test_entity_list_carries_summary(self):
        self.add_fact("qq:gm:1", "qq:9", "对花生过敏", "preference", 9)
        self.store.observe_name("qq:9", "萤火", source="admin")
        rows = self.store.entities(ids=["qq:9"], summaries=3)
        self.assertEqual(rows[0]["stats"]["facts"], 1)
        self.assertEqual(rows[0]["stats"]["summary"], ["对花生过敏"])

    def test_facts_importance_ordering(self):
        self.add_fact("qq:gm:1", "qq:9", "低重要度", "fact", 2)
        self.add_fact("qq:gm:1", "qq:9", "高重要度", "fact", 9)
        self.add_fact("qq:gm:1", "qq:9", "中重要度", "fact", 5)
        ordered = self.store.facts("qq:gm:1", importance_first=True)
        self.assertEqual(
            [f["content"] for f in ordered], ["高重要度", "中重要度", "低重要度"]
        )

    def test_facts_subject_preference_beats_importance(self):
        self.add_fact("qq:gm:1", "qq:9", "提到的人的低重要度事实", "fact", 1)
        self.add_fact("qq:gm:1", "qq:8", "别人的高重要度事实", "fact", 10)
        ordered = self.store.facts(
            "qq:gm:1", importance_first=True, prefer_subjects=("qq:9",)
        )
        self.assertEqual(ordered[0]["content"], "提到的人的低重要度事实")


if __name__ == "__main__":
    unittest.main()
