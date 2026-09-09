"""Version snapshots can be restored for facts and records."""

import importlib
import sys
import tempfile
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_restore_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_restore_test", package)
s = importlib.import_module("alife_restore_test.storage")


class RestoreCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = s.Store(Path(self.temp.name) / "m.db")
        self.store.initialize()

    def tearDown(self):
        self.temp.cleanup()

    def add_fact(self, content="原始内容"):
        self.store.capture(
            "qq:gm:1", "t", [{"role": "user", "content": content, "users": ["qq:9"], "time": 1.0}]
        )
        record = self.store.active("qq:gm:1")[-1]
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            return self.store._add_fact(
                db,
                "qq:gm:1",
                {
                    "category": "fact",
                    "subject": "qq:9",
                    "content": content,
                    "reason": "",
                    "scenario": "",
                    "tags": ["tag"],
                    "relations": [],
                    "source_ids": [record["id"]],
                },
            )

    def version_of(self, kind, target):
        with self.store.connect() as db:
            return db.execute(
                "SELECT id FROM versions WHERE kind=? AND target=? ORDER BY id LIMIT 1",
                (kind, target),
            ).fetchone()[0]

    def test_restore_fact_snapshot(self):
        fact_id = self.add_fact()
        fact = self.store.facts("qq:gm:1")[0]
        self.store.edit(
            "fact", fact_id, fact["revision"], {"content": "被改过的内容"}, "人工修正"
        )
        version_id = self.version_of("fact", fact_id)
        current = self.store.facts("qq:gm:1")[0]
        self.store.restore("fact", fact_id, version_id, current["revision"])
        restored = self.store.facts("qq:gm:1")[0]
        self.assertEqual(restored["content"], "原始内容")
        self.assertGreater(restored["revision"], current["revision"])
        with self.store.connect() as db:
            reasons = [
                row[0]
                for row in db.execute(
                    "SELECT reason FROM versions WHERE target=? ORDER BY id", (fact_id,)
                )
            ]
        self.assertIn("恢复前存档", reasons)

    def test_restore_revives_merged_fact(self):
        first = self.add_fact("萤火对花生过敏")
        second = self.add_fact("萤火对花生严重过敏")
        self.store.merge_facts(first, [first, second], "萤火对花生严重过敏", "合并")
        self.assertEqual(len(self.store.facts("qq:gm:1")), 1)
        version_id = self.version_of("fact", second)
        target = self.store.facts("qq:gm:1")[0]
        with self.store.connect() as db:
            revision = db.execute(
                "SELECT revision FROM facts WHERE id=?", (second,)
            ).fetchone()[0]
        self.store.restore("fact", second, version_id, revision)
        rows = self.store.facts("qq:gm:1")
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            sorted(f["id"] for f in rows), sorted([first, second])
        )
        self.assertEqual(target["merge_pending"], 0)

    def test_restore_record_snapshot(self):
        record_id = self.store.memorize("qq:gm:1", "记住这件事", ["qq:9"], 1.0, 1.0)
        record = self.store.get(record_id)
        self.store.edit(
            "record", record_id, record["revision"], {"summary": "改过的摘要"}, "人工修正"
        )
        version_id = self.version_of("record", record_id)
        current = self.store.get(record_id)
        self.store.restore("record", record_id, version_id, current["revision"])
        self.assertEqual(self.store.get(record_id)["summary"], "记住这件事")

    def test_restore_rejects_stale_revision(self):
        fact_id = self.add_fact()
        fact = self.store.facts("qq:gm:1")[0]
        self.store.edit(
            "fact", fact_id, fact["revision"], {"content": "改过"}, "人工修正"
        )
        version_id = self.version_of("fact", fact_id)
        with self.assertRaises(s.Conflict):
            self.store.restore("fact", fact_id, version_id, 1)

    def test_restore_rejects_unknown_version(self):
        fact_id = self.add_fact()
        fact = self.store.facts("qq:gm:1")[0]
        with self.assertRaises(ValueError):
            self.store.restore("fact", fact_id, 9999, fact["revision"])


if __name__ == "__main__":
    unittest.main()
