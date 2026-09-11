"""降级拼接事实的二次整理（v2.13.0）。

不变量：
1. 绝不丢信息——来源还原失败就整体放弃，不做半截
2. 幂等——重复触发不重复还原、不重复加次数
3. 次数上限——自动重试最多 3 次；手动排队不受限
4. 只碰带标记的行——重做前被人改过（revision 不符）绝不动
"""

import asyncio
import importlib
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_rewrite_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_rewrite_test", package)
c = importlib.import_module("alife_rewrite_test.contracts")
s = importlib.import_module("alife_rewrite_test.storage")
e = importlib.import_module("alife_rewrite_test.engine")


def run(coro):
    return asyncio.run(coro)


class RewriteCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = s.Store(Path(self.temp.name) / "r.db")
        self.store.initialize()

    def tearDown(self):
        self.temp.cleanup()

    def seed(self, texts=None, sid="qq:gm:1", subject="qq:9"):
        texts = texts or ["星月喜欢草莓蛋糕", "星月最爱草莓蛋糕，不吃巧克力的"]
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
                            "category": "preference",
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

    def concat_merge(self, ids):
        """模拟降级：模型不可用 → 按时间拼接，并打上待重做标记。"""
        texts = [self.target_row(fid)["content"] for fid in ids]
        self.store.merge_facts(
            ids[0],
            ids,
            "；".join(dict.fromkeys(texts)),
            "模型输出不可用，按时间拼接",
        )
        self.store.mark_rewrite_pending([ids[0]], 1)
        return ids[0]

    def target_row(self, fact_id):
        with self.store.connect() as db:
            return self.store.row(
                db.execute("SELECT * FROM facts WHERE id=?", (fact_id,)).fetchone()
            )

    # ---- 1. 还原：来源取回、目标回退、重新排队 -------------------------------

    def test_unmerge_restores_sources_and_target(self):
        ids = self.seed()
        texts = [self.target_row(fid)["content"] for fid in ids]
        target = self.concat_merge(ids)
        before = self.target_row(target)

        assert self.store.unmerge_fact(target) is True
        row = self.target_row(target)
        # 目标回退到合并前（保留自己的原文），并且重新排队等合并
        assert row["content"] == texts[0]
        assert row["merge_pending"] == 1
        assert row["rewrite_pending"] == 0
        assert row["rewrite_attempts"] == 1
        assert row["revision"] == before["revision"] + 1
        with self.store.connect() as db:
            others = {
                r["id"]: r["deleted"]
                for r in db.execute("SELECT id,deleted FROM facts").fetchall()
            }
        assert others[ids[1]] == 0, "来源必须从回收站取回"
        assert self.store.rewrite_backlog() == 0
        history = self.store.edit_history("fact", [target])[target]
        assert history[0]["reason"].startswith("重做合并"), history

    def test_facts_payload_exposes_rewrite_flags(self):
        """前端据此渲染「待整理 N/3」——字段必须在事实 API 的行里。"""
        ids = self.seed()
        self.concat_merge(ids)
        row = self.store.facts("qq:gm:1", "", "", 100, 0, False)[0]
        assert row["rewrite_pending"] == 1
        assert row["rewrite_attempts"] == 0

    # ---- 2. 幂等 ------------------------------------------------------------

    def test_unmerge_is_idempotent(self):
        ids = self.seed()
        target = self.concat_merge(ids)
        assert self.store.unmerge_fact(target) is True
        assert self.store.unmerge_fact(target) is False  # 标记已清 → 再点无效
        row = self.target_row(target)
        assert row["rewrite_attempts"] == 1, "重复触发不能重复加次数"

    # ---- 3. 只碰带标记的行 --------------------------------------------------

    def test_unmerge_refuses_after_manual_edit(self):
        ids = self.seed()
        target = self.concat_merge(ids)
        self.store.edit(
            "fact", target, self.target_row(target)["revision"], {"content": "人工改过"}, "手工修改"
        )
        assert self.store.unmerge_fact(target) is False
        assert self.target_row(target)["content"] == "人工改过"

    def test_unmerge_refuses_when_source_missing(self):
        ids = self.seed()
        target = self.concat_merge(ids)
        with self.store.connect() as db:  # 来源被彻底删掉
            db.execute("UPDATE versions SET snapshot='{}' WHERE target=?", (ids[1],))
        assert self.store.unmerge_fact(target) is False
        assert self.target_row(target)["rewrite_pending"] == 1, "放弃重做时标记要留着"

    # ---- 4. 次数上限 --------------------------------------------------------

    def test_pending_rewrites_respects_attempt_cap(self):
        ids = self.seed()
        target = self.concat_merge(ids)
        with self.store.connect() as db:
            db.execute("UPDATE facts SET rewrite_attempts=3 WHERE id=?", (target,))
        assert self.store.pending_rewrites(3, 3) == []
        assert [r["id"] for r in self.store.pending_rewrites(3, 10**6)] == [target]
        assert self.store.rewrite_backlog() == 1


class RewriteEngineCase(RewriteCase):
    """引擎侧：降级要打标记，重做要走完整流水线。"""

    def test_fallback_marks_rewrite_pending(self):
        ids = self.seed()

        async def boom(*args, **kwargs):
            raise ValueError("model output unusable")

        engine = e.Engine(self.store, lambda: c.Settings(), boom, None, None)
        engine.structured = boom
        run(engine.queue_fact_merges("qq:gm:1", 0))
        run(engine.merge_facts("qq:gm:1", None))
        with self.store.connect() as db:
            rows = [dict(r) for r in db.execute(
                "SELECT * FROM facts WHERE deleted=0").fetchall()]
        assert len(rows) == 1, "降级仍然要把重复合并掉"
        assert rows[0]["rewrite_pending"] == 1
        assert "；" in rows[0]["content"], rows[0]["content"]
        assert self.store.rewrite_backlog() == 1

    @staticmethod
    def merged_reply(content):
        async def model(*args, **kwargs):
            group = args[-1]["groups"][0]
            return json.dumps(
                {
                    "groups": [
                        {
                            "target_id": group["facts"][0]["id"],
                            "source_ids": [f["id"] for f in group["facts"]],
                            "content": content,
                            "reason": "合并重复",
                            "action": "merge",
                        }
                    ]
                }
            )
        return model

    def test_redo_merges_properly_and_clears_flag(self):
        ids = self.seed()
        target = self.concat_merge(ids)
        engine = e.Engine(
            self.store, lambda: c.Settings(), self.merged_reply("星月最爱草莓蛋糕，不吃巧克力"), None, None
        )
        assert run(engine.redo_pending_rewrites(3)) == 1
        with self.store.connect() as db:
            alive = [dict(r) for r in db.execute("SELECT * FROM facts WHERE deleted=0").fetchall()]
        assert len(alive) == 1, "重做后应当重新合并成一条"
        row = alive[0]
        # 合并可以保留组里任意一条作为目标（这里是模型选的），只要内容是真的合并结果
        assert row["content"] == "星月最爱草莓蛋糕，不吃巧克力"
        assert "；" not in row["content"], "不能还是拼接的那句"
        assert row["rewrite_pending"] == 0
        assert self.store.rewrite_backlog() == 0

    def test_redo_skips_when_attempts_exhausted_but_manual_forces(self):
        ids = self.seed()
        target = self.concat_merge(ids)
        with self.store.connect() as db:
            db.execute("UPDATE facts SET rewrite_attempts=3 WHERE id=?", (target,))
        engine = e.Engine(
            self.store, lambda: c.Settings(), self.merged_reply("星月最爱草莓蛋糕"), None, None
        )
        assert run(engine.redo_pending_rewrites(3)) == 0, "自动重试到上限就该停"
        assert run(engine.redo_pending_rewrites(10**6, "job")) == 1, "手动应当强制再来一次"


if __name__ == "__main__":
    unittest.main()
