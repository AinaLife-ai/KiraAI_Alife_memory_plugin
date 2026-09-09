"""存档页按会话筛选：严格模式只返回该会话，含跨会话模式保持原召回语义。"""

import importlib
import sys
import tempfile
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_search_scope")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_search_scope", package)
s = importlib.import_module("alife_search_scope.storage")


class SearchScopeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = s.Store(Path(self.temp.name) / "memory.db")
        self.store.initialize()
        for sid in ("a:dm:u", "b:dm:u"):
            self.store.capture(
                sid,
                "turn-" + sid,
                [
                    {
                        "role": "user",
                        "content": f"{sid} 的记录",
                        "time": 1.0,
                        "users": [sid.split(":")[0] + ":u"],
                    }
                ],
            )
        with self.store.connect() as db:
            # 旧插件迁移来的全局记忆
            db.execute(
                """INSERT INTO records
                   (id,sid,role,level,start,end,summary,content,users,position,created,visibility)
                   VALUES ('migrated-global','global','assistant',1,1,1,'旧全局记忆','旧全局记忆','[]',0,1,'global')"""
            )
            # a 会话里一条跨会话可见的记录
            db.execute(
                """INSERT INTO records
                   (id,sid,role,level,start,end,summary,content,users,position,created,visibility)
                   VALUES ('shared-a','a:dm:u','assistant',1,1,1,'a 的跨会话记录','a 的跨会话记录','[]',0,1,'global')"""
            )

    def tearDown(self):
        self.temp.cleanup()

    def ids(self, **kwargs):
        out = self.store.search(include_cold=True, limit=50, **kwargs)
        return sorted(r["id"] for r in out["items"])

    def test_strict_session_returns_only_that_session(self):
        strict_a = self.ids(sid="a:dm:u", scope="session", strict_session=True)
        self.assertIn("shared-a", strict_a)          # 属于 a 会话，保留
        self.assertNotIn("migrated-global", strict_a)  # 全局记忆不再混入
        strict_b = self.ids(sid="b:dm:u", scope="session", strict_session=True)
        self.assertEqual(len(strict_b), 1, strict_b)   # 只有 b 自己那条
        self.assertNotIn("shared-a", strict_b)         # a 的跨会话记录不再泄漏

    def test_default_scope_still_includes_global_memories(self):
        rows = self.ids(sid="b:dm:u", scope="session")
        self.assertIn("shared-a", rows)          # 别的会话共享过来的
        self.assertIn("migrated-global", rows)   # 旧插件的全局记忆

    def test_global_browse_is_unchanged(self):
        self.assertEqual(len(self.ids(scope="global")), 4)


if __name__ == "__main__":
    unittest.main()
