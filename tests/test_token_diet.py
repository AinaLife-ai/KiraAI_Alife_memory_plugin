"""v2.5.3 token 瘦身：注入视图、审计 payload、压缩别名、retract、配置同步。"""

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
package = types.ModuleType("alife_diet_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_diet_test", package)
c = importlib.import_module("alife_diet_test.contracts")
s = importlib.import_module("alife_diet_test.storage")
e = importlib.import_module("alife_diet_test.engine")
r = importlib.import_module("alife_diet_test.retrieval")


def fact(**overrides):
    row = {
        "id": "f-1",
        "sid": "qq:dm:u",
        "category": "preference",
        "subject": "qq:u",
        "content": "喜欢猫",
        "reason": "用户自己说的",
        "scenario": "日常闲聊",
        "tags": ["猫"],
        "relations": [],
        "sources": ["rec-1"],
        "fingerprint": "fp-1",
        "deleted": 0,
        "revision": 0,
        "audited": 0,
        "importance": 6,
        "merge_pending": 0,
        "created": 1700000000.0,
        "relationship_status": "evidence_required",
        "verified_relations": [],
        "relation_warnings": [],
    }
    row.update(overrides)
    return row


class BotFactsTests(unittest.TestCase):
    def test_keeps_only_decision_and_provenance_fields(self):
        view = r.bot_facts([fact()], "qq:dm:u")[0]
        self.assertEqual(
            set(view), {"category", "subject", "content", "relations", "importance", "src", "t"}
        )
        self.assertEqual(view["src"], "rec-1")
        self.assertEqual(view["t"], time.strftime("%Y-%m-%d", time.gmtime(1700000000.0)))
        # 本会话事实不再重复 sid/by
        self.assertNotIn("sid", view)

    def test_cross_session_fact_keeps_source_and_speaker(self):
        view = r.bot_facts(
            [fact(sid="global", src_user="qq:other")], "qq:dm:u"
        )[0]
        self.assertEqual(view["sid"], "global")
        self.assertEqual(view["by"], "qq:other")

    def test_needs_review_only_when_flagged(self):
        self.assertNotIn("needs_review", r.bot_facts([fact()], "qq:dm:u")[0])
        flagged = r.bot_facts(
            [fact(relationship_status="needs_review")], "qq:dm:u"
        )[0]
        self.assertTrue(flagged["needs_review"])


class PayloadTests(unittest.TestCase):
    def test_compress_records_use_short_aliases_and_single_timestamp(self):
        rows = [
            {"id": "a" * 32, "role": "user", "level": 0, "summary": "内容", "users": ["u"],
             "start": 100.0, "end": 100.0},
            {"id": "b" * 32, "role": "assistant", "level": 0, "summary": "回复", "users": ["u"],
             "start": 101.0, "end": 101.0},
        ]
        aliases = {"r1": "a" * 32, "r2": "b" * 32}
        records = e.compress_records(rows, aliases)
        self.assertEqual([rec["id"] for rec in records], ["r1", "r2"])
        self.assertNotIn("level", records[0])
        self.assertEqual(records[0]["t"], 100.0)
        self.assertNotIn("start", records[0])

    def test_compress_records_keep_range_for_archives(self):
        rows = [
            {"id": "c" * 32, "role": "assistant", "level": 1, "summary": "摘要", "users": [],
             "start": 10.0, "end": 20.0},
        ]
        record = e.compress_records(rows, {"r1": "c" * 32})[0]
        self.assertEqual((record["start"], record["end"]), (10.0, 20.0))
        self.assertNotIn("t", record)

    def test_restore_maps_aliases_and_rejects_unknown(self):
        aliases = {"r1": "real-1"}
        out = e.restore_compress_ids(
            {"summary": "s", "facts": [{"source_ids": ["r1"]}]}, aliases
        )
        self.assertEqual(out["facts"][0]["source_ids"], ["real-1"])
        with self.assertRaises(ValueError):
            e.restore_compress_ids(
                {"summary": "s", "facts": [{"source_ids": ["r9"]}]}, aliases
            )

    def test_schema_titles_are_stripped(self):
        ov = importlib.import_module("alife_diet_test.output_validation")
        stripped = ov.strip_schema_titles(c.Compression.model_json_schema())
        self.assertNotIn("title", json.dumps(stripped))
        self.assertIn("properties", stripped)


class ConfigSyncTests(unittest.TestCase):
    def test_new_switches_exist_in_all_three_places(self):
        schema = json.load(open(ROOT / "schema.json", encoding="utf-8"))["alife"]["fields"]
        help_text = importlib.import_module("alife_diet_test.setting_help").HELP
        for key in ("compress_persona", "audit_persona", "inject_mode"):
            self.assertIn(key, schema)
            self.assertIn(key, help_text)
            self.assertIn(key, c.Settings.model_fields)
        defaults = c.Settings()
        self.assertTrue(defaults.compress_persona)
        self.assertFalse(defaults.audit_persona)
        self.assertEqual(defaults.inject_mode, "situational")


class RetractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = s.Store(Path(self.temp.name) / "db.sqlite3")
        self.store.initialize()
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for index, fid in enumerate(("f-1", "f-2")):
                db.execute(
                    """INSERT INTO facts(id,sid,category,subject,content,reason,scenario,tags,
                       relations,sources,fingerprint,deleted,revision,audited,importance,
                       merge_pending,created) VALUES (?,?,?,?,?,?,?,?,?,?,?,0,0,0,?,0,?)""",
                    (fid, "qq:dm:u", "event", "qq:u", "内容", "理由", "", "[]", "[]",
                     "[]", "fp-" + fid, 5, 1000.0),
                )

    def tearDown(self):
        self.temp.cleanup()

    def test_retract_soft_deletes_and_keeps_version(self):
        candidates = self.store.audit_candidates("qq:dm:u", limit=5)
        self.assertEqual(len(candidates), 2)
        target = candidates[0]
        self.store.audit(
            candidates,
            {
                "actions": [
                    {
                        "action": "retract",
                        "target_id": target["id"],
                        "source_ids": [target["id"]],
                        "content": target["content"],
                        "reason": "与证据矛盾",
                    }
                ]
            },
        )
        rows = {row["id"]: row for row in self.store.facts("qq:dm:u", "", "", 50, 0, True)}
        self.assertNotIn(target["id"], rows)          # 已退出注入与检索
        self.assertEqual(rows[candidates[1]["id"]]["deleted"], 0)
        with self.store.connect() as db:
            snapshot = db.execute(
                "SELECT snapshot FROM versions WHERE kind='fact' AND target=?",
                (target["id"],),
            ).fetchone()
        self.assertIsNotNone(snapshot)                # 原文留档，可恢复


class AuditPayloadTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = s.Store(Path(self.temp.name) / "db.sqlite3")
        self.store.initialize()
        self.seen = []

        async def model_call(model, purpose, instruction, schema, payload):
            self.seen.append({"purpose": purpose, "payload": payload, "schema": schema})
            return json.dumps({"actions": []}, ensure_ascii=False)

        self.engine = e.Engine(
            self.store,
            lambda: c.Settings(audit_persona=False),
            model_call,
            lambda text, cfg: asyncio.sleep(0, result=(None, "")),
            lambda sid: asyncio.sleep(0, result=None),
        )

    def tearDown(self):
        self.temp.cleanup()

    def test_audit_payload_drops_internal_fields(self):
        self.store.capture(
            "qq:dm:u",
            "turn",
            [{"role": "user", "content": "我喜欢猫", "time": 1000.0, "users": ["qq:u"]}],
        )
        record_id = self.store.active("qq:dm:u")[0]["id"]
        with self.store.connect() as db:
            db.execute(
                """INSERT INTO facts(id,sid,category,subject,content,reason,scenario,tags,
                   relations,sources,fingerprint,deleted,revision,audited,importance,
                   merge_pending,created) VALUES (?,?,?,?,?,?,?,?,?,?,?,0,0,0,?,0,?)""",
                ("f-1", "qq:dm:u", "preference", "qq:u", "喜欢猫", "用户自己说的",
                 "日常", '["猫"]', "[]", json.dumps([record_id]), "fp", 5, 1000.0),
            )
        asyncio.run(self.engine.audit("qq:dm:u"))
        payload = self.seen[-1]["payload"]
        self.assertEqual(
            set(payload["facts"][0]),
            {"id", "sid", "subject", "category", "content", "reason", "relations",
             "importance", "sources"},
        )
        self.assertEqual(set(payload["evidence"][0]), {"content", "start", "end"})
        self.assertNotIn("title", json.dumps(self.seen[-1]["schema"]))


if __name__ == "__main__":
    unittest.main()
