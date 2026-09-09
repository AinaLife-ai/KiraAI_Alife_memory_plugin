"""Audit pacing: cooldown, priority for new facts, daily fuse."""

import asyncio
import importlib
import json
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_pacing_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_pacing_test", package)
c = importlib.import_module("alife_pacing_test.contracts")
s = importlib.import_module("alife_pacing_test.storage")
e = importlib.import_module("alife_pacing_test.engine")

DAY = 86400


class PacingCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = s.Store(Path(self.temp.name) / "m.db")
        self.store.initialize()

    def tearDown(self):
        self.temp.cleanup()

    def add_fact(self, sid="qq:gm:1", content="事实", subject="qq:9"):
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
                    "category": "fact",
                    "subject": subject,
                    "content": content,
                    "reason": "",
                    "scenario": "",
                    "tags": [],
                    "relations": [],
                    "source_ids": [record["id"]],
                },
            )

    def stamp(self, fact_id, when):
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE facts SET audited=? WHERE id=?", (when, fact_id))

    def test_recently_audited_facts_are_skipped_by_cooldown(self):
        fact_id = self.add_fact()
        self.stamp(fact_id, time.time())
        self.assertEqual(
            len(self.store.audit_candidates("qq:gm:1", 20, 7 * DAY)), 0
        )
        self.assertEqual(self.store.sessions_by_audit_age(2, 7 * DAY), [])

    def test_never_audited_fact_is_always_eligible(self):
        self.add_fact()
        self.assertEqual(len(self.store.audit_candidates("qq:gm:1", 20, 7 * DAY)), 1)
        self.assertEqual(self.store.sessions_by_audit_age(2, 7 * DAY), ["qq:gm:1"])

    def test_new_fact_outranks_stale_one(self):
        old = self.add_fact(content="旧事实")
        self.stamp(old, time.time())
        self.add_fact(content="新事实")
        rows = self.store.audit_candidates("qq:gm:1", 20, 7 * DAY)
        self.assertEqual([r["content"] for r in rows], ["新事实"])

    def test_expired_cooldown_makes_fact_eligible_again(self):
        fact_id = self.add_fact()
        self.stamp(fact_id, 1.0)
        self.assertEqual(len(self.store.audit_candidates("qq:gm:1", 20, 7 * DAY)), 1)

    def test_pending_facts_are_not_audited(self):
        fact_id = self.add_fact()
        self.store.mark_merge_pending([fact_id])
        self.assertEqual(len(self.store.audit_candidates("qq:gm:1", 20, 0)), 0)
        self.assertEqual(self.store.sessions_by_audit_age(2, 0), [])

    def test_daily_call_fuse(self):
        cfg = c.Settings(audit_daily_calls=2)
        engine = e.Engine(self.store, lambda: cfg, None, None, None)
        self.assertTrue(engine.audit_budget_ok(cfg))
        engine.audit_calls = 2
        self.assertFalse(engine.audit_budget_ok(cfg))
        engine.audit_day = "1970-01-01"
        self.assertTrue(engine.audit_budget_ok(cfg))
        unlimited = c.Settings(audit_daily_calls=0)
        engine.audit_calls = 999
        self.assertTrue(engine.audit_budget_ok(unlimited))

    def test_audit_uses_cooldown_candidates(self):
        fact_id = self.add_fact()
        self.stamp(fact_id, time.time())
        cfg = c.Settings()
        calls = []

        async def model(*args):
            calls.append(args)
            return '{"actions":[]}'

        engine = e.Engine(self.store, lambda: cfg, model, None, None)
        asyncio.run(engine.audit("qq:gm:1"))
        self.assertEqual(calls, [])

    def test_audit_can_correct_importance(self):
        fact_id = self.add_fact()
        cfg = c.Settings()
        calls = []

        async def model(*args):
            calls.append(args)
            payload = args[-1]
            return json.dumps(
                {
                    "actions": [
                        {
                            "action": "keep",
                            "target_id": payload["facts"][0]["id"],
                            "source_ids": [payload["facts"][0]["id"]],
                            "content": payload["facts"][0]["content"],
                            "reason": "证据一致",
                            "relations": None,
                            "importance": 9,
                        }
                    ]
                },
                ensure_ascii=False,
            )

        engine = e.Engine(self.store, lambda: cfg, model, None, None)
        asyncio.run(engine.audit("qq:gm:1"))
        self.assertEqual(len(calls), 1)
        rows = self.store.facts("qq:gm:1")
        self.assertEqual(rows[0]["importance"], 9)

    def test_audit_rejects_extra_fields(self):
        self.add_fact()
        cfg = c.Settings(model_retries=0)

        async def model(*args):
            payload = args[-1]
            return json.dumps(
                {
                    "actions": [
                        {
                            "action": "keep",
                            "target_id": payload["facts"][0]["id"],
                            "source_ids": [payload["facts"][0]["id"]],
                            "content": "x",
                            "reason": "y",
                            "relations": None,
                            "unknown_field": 1,
                        }
                    ]
                },
                ensure_ascii=False,
            )

        engine = e.Engine(self.store, lambda: cfg, model, None, None)
        with self.assertRaises(Exception):
            asyncio.run(engine.audit("qq:gm:1"))


if __name__ == "__main__":
    unittest.main()
