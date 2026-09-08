import asyncio
import importlib
import sys
import tempfile
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_test_plugin")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_test_plugin", package)
c = importlib.import_module("alife_test_plugin.contracts")
s = importlib.import_module("alife_test_plugin.storage")
e = importlib.import_module("alife_test_plugin.engine")


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = s.Store(Path(self.temp.name) / "memory.db")
        self.store.initialize()
        self.cfg = c.Settings(threshold=4, batch_size=2, probability=1.0)

    def tearDown(self):
        self.temp.cleanup()

    def capture(self, sid="a:dm:u", count=4, event="turn"):
        self.store.capture(
            sid,
            event,
            [
                {
                    "role": "user" if i % 2 == 0 else "assistant",
                    "content": f"原文 {i} 喜欢猫",
                    "time": float(i + 1),
                    "users": ["a:u"],
                }
                for i in range(count)
            ],
        )

    def fact(self, ids, content="用户喜欢猫"):
        return {
            "category": "preference",
            "subject": "a:u",
            "content": content,
            "reason": "陪伴",
            "scenario": "日常",
            "tags": ["猫"],
            "relations": [],
            "source_ids": ids,
        }

    def test_cascade_retains_raw_and_nested_archives(self):
        self.capture(count=12)
        originals = self.store.active("a:dm:u")
        while plan := e.compression_plan(self.store.active("a:dm:u"), self.cfg):
            rows, level = plan
            archive = self.store.compress(
                "a:dm:u",
                rows,
                level,
                {"summary": "归档摘要", "facts": [self.fact([r["id"] for r in rows])]},
            )
            self.assertEqual(
                self.store.get(archive)["children"], [r["id"] for r in rows]
            )
        active = self.store.active("a:dm:u")
        self.assertTrue(any(r["level"] == 2 for r in active))
        for row in originals:
            self.assertEqual(self.store.get(row["id"])["content"], row["content"])
        self.assertEqual(len(self.store.facts("a:dm:u")), 1)
        self.assertGreater(len(self.store.facts("a:dm:u")[0]["sources"]), 2)

    def test_reversed_regions_never_forge_levels(self):
        self.capture()
        rows = self.store.active("a:dm:u")
        cases = [[0, 0, 0, 2], [0, 2], [0, 3, 0, 2], [2, 0, 0]]
        for levels in cases:
            fixture = [
                dict(rows[i % 4], id=str(i), level=level)
                for i, level in enumerate(levels)
            ]
            self.assertIsNone(e.compression_plan(fixture, self.cfg))
            self.assertEqual([r["level"] for r in fixture], levels)
        fixture = [
            dict(rows[i % 4], id=str(i), level=level)
            for i, level in enumerate([0, 2, 0, 0, 0])
        ]
        group, level = e.compression_plan(fixture, self.cfg)
        self.assertEqual(level, 1)
        self.assertTrue(all(r["level"] == 0 for r in group))

    def test_concurrent_editor_wins_without_partial_archive(self):
        self.capture()
        rows, level = e.compression_plan(self.store.active("a:dm:u"), self.cfg)
        self.store.edit(
            "record",
            rows[0]["id"],
            rows[0]["revision"],
            {"summary": "已由用户修正"},
            "correction",
        )
        with self.assertRaises(s.Conflict):
            self.store.compress(
                "a:dm:u", rows, level, {"summary": "过期模型输出", "facts": []}
            )
        self.assertEqual(self.store.status()["records"], 4)
        self.assertTrue(all(r["active"] for r in self.store.active("a:dm:u")))
        self.assertEqual(self.store.get(rows[0]["id"])["content"], rows[0]["content"])

    def test_idempotence_and_same_timestamp_no_overwrite(self):
        self.capture()
        self.capture()
        self.assertEqual(self.store.status()["records"], 4)
        first = self.store.memorize("a:dm:u", "第一条", ["a:u"], 1.0, 1.0)
        second = self.store.memorize("a:dm:u", "第二条", ["a:u"], 1.0, 1.0)
        self.assertNotEqual(first, second)

    def test_forget_retains_readable_archive(self):
        record = self.store.memorize("a:dm:u", "珍贵的经历", ["a:u"], 1.0, 2.0)
        self.store.edit("record", record, 1, {"active": False}, "forget")
        self.assertFalse(self.store.active("a:dm:u"))
        self.assertEqual(self.store.get(record)["content"], "珍贵的经历")

    def test_search_time_level_unicode_paging_scope(self):
        self.capture()
        self.capture(sid="a:dm:other", event="other")
        result = self.store.search(
            "a:dm:u", keyword="喜欢猫", level=0, start=2.0, end=3.0, limit=1
        )
        self.assertEqual(result["total"], 2)
        self.assertEqual(len(result["items"]), 1)
        self.assertEqual(self.store.search("empty")["total"], 0)
        self.assertEqual(
            self.store.search("empty", scope="linked", users=["a:u"])["total"], 8
        )
        self.assertEqual(self.store.search("a:dm:u", keyword="' OR 1=1 --")["total"], 0)

    def test_embedding_model_mismatch_is_not_ranked(self):
        self.capture()
        rows = self.store.active("a:dm:u")
        self.store.set_vector(rows[2]["id"], "provider:model", 1, [1.0, 0.0])
        result = self.store.search("a:dm:u", vector=[1.0, 0.0], model="provider:model")
        self.assertEqual(result["items"][0]["id"], rows[2]["id"])
        result = self.store.search("a:dm:u", vector=[1.0, 0.0], model="other:model")
        self.assertTrue(all(r["score"] == -1 for r in result["items"]))

    def test_unknown_sources_roll_back(self):
        self.capture()
        rows, level = e.compression_plan(self.store.active("a:dm:u"), self.cfg)
        with self.assertRaises(ValueError):
            self.store.compress(
                "a:dm:u",
                rows,
                level,
                {"summary": "错误摘要", "facts": [self.fact(["fake"])]},
            )
        self.assertEqual(self.store.status()["records"], 4)

    def test_jobs_single_claim_restart(self):
        first = self.store.enqueue("compress", "a:dm:u")
        self.assertEqual(self.store.enqueue("compress", "a:dm:u"), first)
        self.assertEqual(self.store.claim()["id"], first)
        self.assertIsNone(self.store.claim())
        self.store.initialize()
        self.assertEqual(self.store.claim()["id"], first)

    def test_merge_keeps_provenance_and_edit_history(self):
        self.capture()
        with self.store.connect() as db:
            a = self.store._add_fact(db, "a:dm:u", self.fact(["one"]))
            b = self.store._add_fact(db, "a:dm:u", self.fact(["two"], "很喜欢猫"))
        candidates = self.store.facts("a:dm:u")
        self.store.audit(
            candidates,
            {
                "actions": [
                    {
                        "action": "merge",
                        "target_id": a,
                        "source_ids": [b],
                        "content": "用户很喜欢猫",
                        "reason": "两条证据表达同一偏好",
                    }
                ]
            },
        )
        facts = self.store.facts("a:dm:u")
        self.assertEqual(len(facts), 1)
        self.assertEqual(facts[0]["sources"], ["one", "two"])
        self.assertEqual(len(self.store.export()["versions"]), 2)

    def test_cross_subject_merge_rejected(self):
        with self.store.connect() as db:
            a = self.store._add_fact(db, "a:dm:u", self.fact(["one"]))
            fact = self.fact(["two"])
            fact["subject"] = "a:v"
            b = self.store._add_fact(db, "a:dm:u", fact)
        with self.assertRaises(ValueError):
            self.store.audit(
                self.store.facts("a:dm:u"),
                {
                    "actions": [
                        {
                            "action": "merge",
                            "target_id": a,
                            "source_ids": [b],
                            "content": "合并",
                            "reason": "测试",
                        }
                    ]
                },
            )
        self.assertEqual(len(self.store.facts("a:dm:u")), 2)


class ContractTests(unittest.TestCase):
    def test_drift_rejected(self):
        bad = [
            '```json\n{"summary":"x","facts":[]}\n```',
            '说明 {"summary":"x","facts":[]}',
            '{"summary":"x","summary":"y","facts":[]}',
            '{"summary":1,"facts":[]}',
            '{"summary":"x","facts":[],"extra":1}',
            '{"summary":"x"}',
            '{"summary":"x","facts":NaN}',
        ]
        for value in bad:
            with self.subTest(value=value), self.assertRaises(ValueError):
                c.parse_output(value, c.Compression)

    def test_config_bounds_and_coercion(self):
        for data in [
            {"enabled": "true"},
            {"threshold": 4, "batch_size": 4},
            {"max_level": 100},
            {"probability": float("nan")},
            {"unexpected": 1},
        ]:
            with self.assertRaises(ValueError):
                c.Settings.model_validate(data)


class EngineTests(unittest.IsolatedAsyncioTestCase):
    async def test_database_thread_is_joined_before_cancellation_returns(self):
        import threading

        with tempfile.TemporaryDirectory() as directory:
            store = s.Store(Path(directory) / "db")
            store.initialize()
            entered, release = threading.Event(), threading.Event()

            def blocking():
                with store.connect() as db:
                    db.execute("SELECT count(*) FROM records")
                    entered.set()
                    release.wait(5)

            store.blocking = blocking
            task = asyncio.create_task(store.call("blocking"))
            await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_schema_retry_then_success(self):
        calls = []

        async def model(*args):
            calls.append(args)
            return "bad" if len(calls) == 1 else '{"summary":"有效摘要","facts":[]}'

        engine = e.Engine(None, lambda: c.Settings(), model, None, None)
        result = await engine.structured(
            c.Compression, "compress", {"records": []}, c.Settings()
        )
        self.assertEqual(len(calls), 2)
        self.assertEqual(result["summary"], "有效摘要")

    async def test_inflight_config_change_does_not_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            store = s.Store(Path(directory) / "db")
            store.initialize()
            store.capture(
                "s",
                "e",
                [{"role": "user", "content": "original", "time": 1.0, "users": []}] * 4,
            )
            settings = [c.Settings(threshold=4, batch_size=2)]

            async def model(*args):
                settings[0] = settings[0].model_copy(
                    update={"compress_model": "changed"}
                )
                return '{"summary":"obsolete","facts":[]}'

            engine = e.Engine(store, lambda: settings[0], model, None, None)
            await engine.compress("s")
            self.assertEqual(store.status()["records"], 4)

    async def test_cancel_requeues_and_stops_workers(self):
        with tempfile.TemporaryDirectory() as directory:
            store = s.Store(Path(directory) / "db")
            store.initialize()
            store.capture(
                "s",
                "e",
                [{"role": "user", "content": "original", "time": 1.0, "users": []}] * 4,
            )
            entered = asyncio.Event()

            async def model(*args):
                entered.set()
                await asyncio.Event().wait()

            engine = e.Engine(
                store, lambda: c.Settings(threshold=4, batch_size=2), model, None, None
            )
            await engine.enqueue("compress", "s")
            await engine.start()
            await asyncio.wait_for(entered.wait(), 5)
            await engine.stop()
            self.assertEqual(store.status()["jobs"][0]["state"], "queued")
            self.assertFalse(engine.tasks)


if __name__ == "__main__":
    unittest.main()
