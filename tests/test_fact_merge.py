"""Write-time fact merging: local detection, forced merge, visibility window."""

import asyncio
import importlib
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_merge_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_merge_test", package)
c = importlib.import_module("alife_merge_test.contracts")
s = importlib.import_module("alife_merge_test.storage")
e = importlib.import_module("alife_merge_test.engine")


def run(coro):
    return asyncio.run(coro)


class MergeCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = s.Store(Path(self.temp.name) / "m.db")
        self.store.initialize()
        self.calls = []

    def tearDown(self):
        self.temp.cleanup()

    def seed(self, sid="qq:gm:1", texts=None, subject="qq:9", category="preference"):
        texts = texts or ["萤火对花生过敏", "萤火对花生严重过敏"]
        messages = [
            {"role": "user", "content": text, "users": [subject], "time": float(i)}
            for i, text in enumerate(texts)
        ]
        self.store.capture(sid, "turn", messages)
        records = self.store.active(sid)
        ids = []
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for index, text in enumerate(texts):
                ids.append(
                    self.store._add_fact(
                        db,
                        sid,
                        {
                            "category": category,
                            "subject": subject,
                            "content": text,
                            "reason": "",
                            "scenario": "",
                            "tags": [],
                            "relations": [],
                            "source_ids": [records[index]["id"]],
                            "importance": 5 + index,
                        },
                    )
                )
        return ids

    def engine(self, cfg, model):
        return e.Engine(self.store, lambda: cfg, model, None, None)

    @staticmethod
    def merge_reply(payload):
        group = payload["groups"][0]
        return json.dumps(
            {
                "groups": [
                    {
                        "target_id": group["facts"][0]["id"],
                        "source_ids": [f["id"] for f in group["facts"]],
                        "content": "萤火对花生严重过敏",
                        "reason": "合并重复",
                    }
                ]
            },
            ensure_ascii=False,
        )

    def test_trigger_and_merge_keeps_newest_and_max_importance(self):
        ids = self.seed()
        cfg = c.Settings()

        async def model(*args):
            self.calls.append(args)
            return self.merge_reply(args[-1])

        engine = self.engine(cfg, model)
        flagged = run(engine.queue_fact_merges("qq:gm:1", 0))
        self.assertEqual(flagged, 2)
        merged = run(engine.merge_facts("qq:gm:1"))
        self.assertEqual(merged, 1)
        rows = self.store.facts("qq:gm:1")
        self.assertEqual([r["content"] for r in rows], ["萤火对花生严重过敏"])
        self.assertEqual(rows[0]["importance"], 6)
        self.assertEqual(self.calls[0][1], "fact_merge")
        with self.store.connect() as db:
            versions = db.execute(
                "SELECT count(*) FROM versions WHERE kind='fact'"
            ).fetchone()[0]
        self.assertGreaterEqual(versions, 2)
        self.assertNotEqual(rows[0]["id"], ids[0])

    def test_short_facts_are_detected(self):
        ids = self.seed(texts=["她喜欢猫", "她喜欢猫咪"], subject="qq:7")
        candidates = self.store.similar_facts(
            "qq:gm:1", "qq:7", "preference", "她喜欢猫",
            min_score=0.25, exclude_ids=[ids[0]],
        )
        self.assertEqual(len(candidates), 1)
        self.assertGreaterEqual(candidates[0][0], 0.9)

    def test_different_subject_is_never_merged(self):
        self.seed(texts=["他喜欢猫"], subject="qq:1")
        self.seed(texts=["他喜欢狗"], subject="qq:2")
        cfg = c.Settings()
        engine = self.engine(cfg, None)
        self.assertEqual(run(engine.queue_fact_merges("qq:gm:1", 0)), 0)

    def test_cross_session_only_for_identity_categories(self):
        self.seed(sid="qq:gm:1", texts=["萤火喜欢甜蛋糕"], category="preference")
        self.seed(sid="qq:gm:2", texts=["萤火喜欢甜口蛋糕"], category="preference")
        cross = self.store.similar_facts(
            "qq:gm:2", "qq:9", "preference", "萤火喜欢甜口蛋糕",
            cross_session=True,
        )
        self.assertTrue(cross)
        self.seed(sid="qq:gm:3", texts=["周六三点见面"], category="event")
        last = self.seed(sid="qq:gm:4", texts=["周六三点见面"], category="event")
        same_session_only = self.store.similar_facts(
            "qq:gm:4", "qq:9", "event", "周六三点见面",
            cross_session=False, exclude_ids=last,
        )
        self.assertFalse(same_session_only)

    def test_pending_facts_are_hidden_until_merged(self):
        self.seed()
        cfg = c.Settings()
        engine = self.engine(cfg, None)
        run(engine.queue_fact_merges("qq:gm:1", 0))
        hidden = self.store.facts("qq:gm:1", hide_pending=cfg.merge_pending_hide)
        self.assertEqual(hidden, [])
        self.assertEqual(len(self.store.facts("qq:gm:1")), 2)

    def test_long_model_output_falls_back_to_union(self):
        self.seed()
        cfg = c.Settings(model_retries=0)

        async def too_long(*args):
            group = args[-1]["groups"][0]
            return json.dumps(
                {
                    "groups": [
                        {
                            "target_id": group["facts"][0]["id"],
                            "source_ids": [f["id"] for f in group["facts"]],
                            "content": "长" * 500,
                            "reason": "x",
                        }
                    ]
                },
                ensure_ascii=False,
            )

        engine = self.engine(cfg, too_long)
        run(engine.queue_fact_merges("qq:gm:1", 0))
        self.assertEqual(run(engine.merge_facts("qq:gm:1")), 1)
        rows = self.store.facts("qq:gm:1")
        self.assertEqual(len(rows), 1)
        self.assertIn("萤火对花生严重过敏", rows[0]["content"])
        self.assertIn("萤火对花生过敏", rows[0]["content"])
        self.assertLessEqual(len(rows[0]["content"]), cfg.fact_merge_max_chars)
        self.assertEqual(rows[0]["merge_pending"], 0)

    def test_soft_limits_are_rendered_into_prompt(self):
        cfg = c.Settings(fact_merge_soft_chars=77, fact_merge_soft_reason_chars=11)
        instruction = e.build_instruction("fact_merge", cfg)
        self.assertIn("77", instruction)
        self.assertIn("11", instruction)
        self.assertIn("merge", instruction)

    def test_compression_output_is_scanned_for_duplicates(self):
        self.seed(texts=["萤火对花生过敏"], subject="qq:9")
        self.store.capture(
            "qq:gm:1",
            "turn2",
            [
                {"role": "user", "content": f"记录 {i}", "users": ["qq:9"], "time": 10.0 + i}
                for i in range(4)
            ],
        )
        rows = [r for r in self.store.active("qq:gm:1") if r["level"] == 0][-4:]
        cfg = c.Settings(threshold=4, batch_size=2, probability=1.0)

        async def model(*args):
            if args[1] == "compress":
                payload = args[-1]
                return json.dumps(
                    {
                        "summary": "这段时间聊到了过敏",
                        "facts": [
                            {
                                "category": "preference",
                                "subject": "qq:9",
                                "content": "萤火对花生严重过敏",
                                "reason": "",
                                "scenario": "",
                                "tags": [],
                                "relations": [],
                                "source_ids": [payload["records"][0]["id"]],
                                "importance": 7,
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            return self.merge_reply(args[-1])

        engine = self.engine(cfg, model)
        asyncio.run(engine.compress("qq:gm:1"))
        pending = self.store.facts_for_merge(sid="qq:gm:1", pending_only=True)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["content"], "萤火对花生严重过敏")
        self.assertEqual(pending[0]["importance"], 7)

    def test_cross_session_merge_lands_in_global_scope(self):
        self.seed(sid="qq:gm:1", texts=["萤火喜欢甜蛋糕"], category="preference")
        self.seed(sid="qq:gm:2", texts=["萤火喜欢甜口蛋糕"], category="preference")
        cfg = c.Settings()

        async def model(*args):
            group = args[-1]["groups"][0]
            return json.dumps(
                {
                    "groups": [
                        {
                            "target_id": group["facts"][0]["id"],
                            "source_ids": [f["id"] for f in group["facts"]],
                            "content": "萤火喜欢甜口蛋糕",
                            "reason": "跨会话合并",
                        }
                    ]
                },
                ensure_ascii=False,
            )

        engine = self.engine(cfg, model)
        asyncio.run(engine.queue_fact_merges("qq:gm:2", 0))
        asyncio.run(engine.merge_facts("qq:gm:2"))
        global_facts = self.store.facts("qq:gm:3", include_shared=True)
        self.assertEqual([f["content"] for f in global_facts], ["萤火喜欢甜口蛋糕"])
        self.assertEqual(global_facts[0]["sid"], "global")

    def test_fact_merge_lane_processes_queued_job(self):
        self.seed()
        cfg = c.Settings()

        async def model(*args):
            self.calls.append(args)
            return self.merge_reply(args[-1])

        engine = self.engine(cfg, model)

        async def run():
            # Only the merge lane: keeps the test independent of the other lanes.
            task = asyncio.create_task(engine.fact_merge_worker())
            try:
                await engine.queue_fact_merges("qq:gm:1", 0)
                for _ in range(200):
                    await asyncio.sleep(0.05)
                    with self.store.connect() as db:
                        row = db.execute(
                            "SELECT state FROM jobs WHERE kind='fact_merge'"
                        ).fetchone()
                    if row and row[0] in ("completed", "failed"):
                        break
            finally:
                engine.stopping = True
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

        asyncio.run(run())
        with self.store.connect() as db:
            rows = db.execute(
                "SELECT state,detail FROM jobs WHERE kind='fact_merge' ORDER BY created DESC"
            ).fetchall()
        self.assertEqual(len(self.store.facts("qq:gm:1")), 1, rows)
        self.assertTrue(rows, "fact_merge 任务应存在")
        self.assertEqual(rows[0][0], "completed", rows)

    def test_startup_requeues_pending_facts(self):
        self.seed()
        cfg = c.Settings()

        async def model(*args):
            return self.merge_reply(args[-1])

        rows = self.store.facts("qq:gm:1")
        self.store.mark_merge_pending([rows[0]["id"]])
        engine = self.engine(cfg, model)

        async def run():
            await engine.start()
            try:
                with self.store.connect() as db:
                    job = db.execute(
                        "SELECT sid,state FROM jobs WHERE kind='fact_merge'"
                    ).fetchone()
                self.assertIsNotNone(job, "启动时应把待合并事实重新入队")
                self.assertEqual(job[0], "qq:gm:1")
                for _ in range(200):
                    await asyncio.sleep(0.05)
                    if len(self.store.facts("qq:gm:1")) == 1:
                        break
            finally:
                await engine.stop()

        asyncio.run(run())
        self.assertEqual(len(self.store.facts("qq:gm:1")), 1)


if __name__ == "__main__":
    unittest.main()
