"""Versioned config migration: upgrade untouched defaults, never user choices."""

import importlib
import sys
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_config_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_config_test", package)
c = importlib.import_module("alife_config_test.contracts")
m = importlib.import_module("alife_config_test.config_migrate")


class MigrationCase(unittest.TestCase):
    def test_old_default_is_upgraded(self):
        changed, updated = m.migrate({"alife": {"audit_interval": 1800}})
        self.assertEqual(changed, ["audit_interval"])
        self.assertEqual(updated["alife"]["audit_interval"], 7200)
        self.assertEqual(updated["alife_meta"]["config_version"], m.CURRENT_VERSION)

    def test_customised_value_is_kept(self):
        changed, updated = m.migrate({"alife": {"audit_interval": 3600}})
        self.assertEqual(changed, [])
        self.assertEqual(updated["alife"]["audit_interval"], 3600)
        self.assertEqual(updated["alife_meta"]["config_version"], m.CURRENT_VERSION)

    def test_missing_key_is_left_to_host_defaults(self):
        changed, updated = m.migrate({"alife": {}})
        self.assertEqual(changed, [])
        self.assertNotIn("audit_interval", updated["alife"])

    def test_runs_once(self):
        _, first = m.migrate({"alife": {"audit_interval": 1800}})
        changed, second = m.migrate(first)
        self.assertEqual(changed, [])
        self.assertEqual(second["alife"]["audit_interval"], 7200)

    def test_empty_or_broken_config_is_safe(self):
        for value in ({}, None, {"alife": None}, {"alife_meta": {"config_version": "x"}}):
            changed, updated = m.migrate(value)
            self.assertEqual(changed, [])
            self.assertEqual(
                updated["alife_meta"]["config_version"], m.CURRENT_VERSION
            )

    def test_prompt_defaults_match_contracts(self):
        defaults = m.prompt_defaults()
        self.assertEqual(defaults["fact_merge_prompt"], c.FACT_MERGE_PROMPT)
        self.assertEqual(defaults["record_merge_prompt"], c.RECORD_MERGE_PROMPT)
        self.assertEqual(c.Settings().fact_merge_prompt, c.FACT_MERGE_PROMPT)
        self.assertEqual(c.Settings().record_merge_prompt, c.RECORD_MERGE_PROMPT)

    def test_soft_limit_cannot_exceed_hard_limit(self):
        with self.assertRaises(Exception):
            c.Settings(fact_merge_soft_chars=200, fact_merge_max_chars=150)
        with self.assertRaises(Exception):
            c.Settings(record_merge_soft_reason_chars=90, record_merge_reason_chars=60)


if __name__ == "__main__":
    unittest.main()
