"""后台任务明细：压缩/审计处理了哪几条，能否回溯与编辑。"""

import asyncio
import importlib
import sys
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_jobitem_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_jobitem_test", package)
c = importlib.import_module("alife_jobitem_test.contracts")
s = importlib.import_module("alife_jobitem_test.storage")
e = importlib.import_module("alife_jobitem_test.engine")


def store(tmp_path):
    st = s.Store(tmp_path / "m.db")
    st.initialize()
    return st


def add_fact(st, sid, content, subject="qq:9", category="preference"):
    st.capture(
        sid, "t", [{"role": "user", "content": content, "users": [subject], "time": 1.0}]
    )
    record = st.active(sid)[-1]
    with st.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        return st._add_fact(
            db,
            sid,
            {
                "category": category,
                "subject": subject,
                "content": content,
                "reason": "",
                "scenario": "",
                "tags": [],
                "relations": [],
                "source_ids": [record["id"]],
            },
        )


def test_audit_with_job_id_records_items(tmp_path):
    st = store(tmp_path)
    a = add_fact(st, "qq:gm:1", "萤火喜欢猫")
    b = add_fact(st, "qq:gm:1", "萤火喜欢猫粮")
    candidates = st.facts("qq:gm:1")
    output = {
        "actions": [
            {
                "action": "keep",
                "target_id": a,
                "source_ids": [a],
                "content": "萤火喜欢猫",
                "reason": "证据一致",
            },
            {
                "action": "retract",
                "target_id": b,
                "source_ids": [b],
                "content": "萤火喜欢猫粮",
                "reason": "与上一条重复",
            },
        ]
    }
    st.audit(candidates, output, "job-1")
    items = st.job_items("job-1")
    assert [(i["kind"], i["target"], i["action"]) for i in items] == [
        ("fact", a, "keep"),
        ("fact", b, "retract"),
    ]
    assert items[1]["note"] == "与上一条重复"
    # 没有 job_id 时保持原行为（不写明细）
    st.audit(st.facts("qq:gm:1")[:1], {"actions": []}, "")
    assert st.job_items("") == []
    # 明细里带改前内容；被合并的另有一条「并入」记录
    items = st.job_items("job-1")
    retract = [i for i in items if i["action"] == "retract"][0]
    assert retract["before"] == "萤火喜欢猫粮"


def test_compress_with_job_id_records_archive_and_sources(tmp_path):
    st = store(tmp_path)
    cfg = c.Settings(threshold=4, batch_size=2, probability=1.0)
    st.capture(
        "qq:gm:1",
        "turn",
        [
            {"role": "user", "content": f"第{i}条原文", "users": ["qq:9"], "time": float(i)}
            for i in range(4)
        ],
    )
    calls = []

    async def model(*args):
        calls.append(args)
        payload = args[-1]
        ids = [r["id"] for r in payload["records"]]
        return c.dump(
            {
                "summary": "归档摘要",
                "facts": [
                    {
                        "category": "event",
                        "subject": "qq:9",
                        "content": "发生了一件事",
                        "reason": "用户说的",
                        "scenario": "",
                        "tags": [],
                        "relations": [],
                        "source_ids": ids,
                        "importance": 5,
                    }
                ],
            }
        )

    async def embed(text, cfg_):
        return None, ""

    engine = e.Engine(st, lambda: cfg, model, embed, lambda sid: asyncio.sleep(0))
    asyncio.run(engine.compress("qq:gm:1", "job-2"))
    items = st.job_items("job-2")
    archives = [i for i in items if i["action"] == "archive"]
    sources = [i for i in items if i["action"] == "compressed"]
    assert len(archives) == 1 and archives[0]["target"].startswith("1-")
    assert archives[0]["note"].startswith("压缩 2 条 → L1")
    assert len(sources) == 2
    assert sources[0]["note"] == "并入 " + archives[0]["target"]
    # 存档确实带上了这两条原文
    row = st.get(archives[0]["target"])
    assert sorted(row["children"]) == sorted(i["target"] for i in sources)


def test_records_by_ids_is_bulk_and_skips_unknown(tmp_path):
    st = store(tmp_path)
    st.capture(
        "qq:gm:1",
        "t",
        [{"role": "user", "content": "一", "users": ["qq:9"], "time": 1.0}],
    )
    record = st.active("qq:gm:1")[0]
    rows = st.records_by_ids([record["id"], "不存在"])
    assert [r["id"] for r in rows] == [record["id"]]
    assert st.records_by_ids([]) == []
    assert st.records_by_ids([record["id"], record["id"]]) != []


class MergeDirectionTests(unittest.TestCase):
    def setUp(self):
        import tempfile

        self.temp = tempfile.TemporaryDirectory()
        self.store = s.Store(Path(self.temp.name) / "m.db")
        self.store.initialize()

    def tearDown(self):
        self.temp.cleanup()

    def fact(self, content):
        self.store.capture(
            "qq:gm:1",
            "t",
            [{"role": "user", "content": content, "users": ["qq:9"], "time": 1.0}],
        )
        record = self.store.active("qq:gm:1")[-1]
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            return self.store._add_fact(
                db,
                "qq:gm:1",
                {
                    "category": "preference",
                    "subject": "qq:9",
                    "content": content,
                    "reason": "",
                    "scenario": "",
                    "tags": [],
                    "relations": [],
                    "source_ids": [record["id"]],
                },
            )

    def test_merge_items_keep_direction_and_before(self):
        st = self.store
        a = self.fact("萤火喜欢猫")
        b = self.fact("萤火喜欢猫粮")
        candidates = st.facts("qq:gm:1")
        output = {
            "actions": [
                {
                    "action": "merge",
                    "target_id": a,
                    "source_ids": [a, b],
                    "content": "萤火喜欢猫与猫粮",
                    "reason": "重复",
                }
            ]
        }
        st.audit(candidates, output, "job-merge")
        items = st.job_items("job-merge")
        assert [i["action"] for i in items] == ["merge", "merged"]
        assert items[0]["before"] == "萤火喜欢猫"
        assert items[1]["note"] == a and items[1]["before"] == "萤火喜欢猫粮"
        # 被合并掉的事实默认读不到，但明细需要能读（include_deleted）
        assert st.facts_by_ids([b]) == []
        kept = st.facts_by_ids([b], include_deleted=True)[0]
        assert kept["content"] == "萤火喜欢猫粮"


class TrashTests(unittest.TestCase):
    def setUp(self):
        import tempfile

        self.temp = tempfile.TemporaryDirectory()
        self.store = s.Store(Path(self.temp.name) / "m.db")
        self.store.initialize()

    def tearDown(self):
        self.temp.cleanup()

    def add(self, content, sid="qq:gm:1", subject="qq:9"):
        self.store.capture(
            sid,
            content,
            [{"role": "user", "content": content, "users": [subject], "time": 1.0}],
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
                    "reason": "用户说的",
                    "scenario": "",
                    "tags": [],
                    "relations": [],
                    "source_ids": [record["id"]],
                },
            )

    def test_trash_lists_and_restores_deleted_fact(self):
        fact_id = self.add("萤火喜欢猫")
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE facts SET deleted=1 WHERE id=?", (fact_id,))
        page = self.store.trash("facts")
        assert page["total"] == 1 and page["items"][0]["id"] == fact_id
        assert page["items"][0]["removed_at"] == 0  # 没有版本记录时退回 0
        assert self.store.trash("facts", keyword="猫")["total"] == 1
        assert self.store.trash("facts", keyword="狗")["total"] == 0
        assert self.store.undelete("fact", fact_id) is True
        assert self.store.trash("facts")["total"] == 0
        assert [f["id"] for f in self.store.facts("qq:gm:1")] == [fact_id]
        # 还原动作本身也留一条版本，可追溯
        reasons = [v["reason"] for v in self.store.versions_of("fact", fact_id)]
        assert "从回收站还原" in reasons

    def test_trash_separates_deleted_records_and_cold_archives(self):
        self.store.capture(
            "qq:gm:1",
            "t",
            [{"role": "user", "content": "周五去看展", "users": ["qq:9"], "time": 2.0}],
        )
        record = self.store.active("qq:gm:1")[-1]
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE records SET deleted=1 WHERE id=?", (record["id"],))
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE records SET archived_at=? WHERE id=?", (123.0, record["id"])
            )
        assert [r["id"] for r in self.store.trash("records")["items"]] == [record["id"]]
        assert self.store.trash("cold")["items"] == []
        # 冷归档：未删除但已移出上下文
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE records SET deleted=0,active=0,cold=1 WHERE id=?",
                (record["id"],),
            )
        cold = self.store.trash("cold")
        assert [r["id"] for r in cold["items"]] == [record["id"]]
        assert self.store.trash("records")["items"] == []
        # 按 ID 单取放行软删（编辑器要能打开回收站里的条目）
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE records SET deleted=1 WHERE id=?", (record["id"],))
        assert self.store.get(record["id"]) is None
        assert self.store.get(record["id"], include_deleted=True)["summary"] == "周五去看展"


    def test_reactivate_brings_cold_archive_back(self):
        self.store.capture(
            "qq:gm:1",
            "t",
            [{"role": "user", "content": "周五去看展", "users": ["qq:9"], "time": 2.0}],
        )
        record = self.store.active("qq:gm:1")[-1]
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE records SET active=0,cold=1,archived_at=99 WHERE id=?",
                (record["id"],),
            )
        assert self.store.trash("cold")["total"] == 1
        assert self.store.reactivate(record["id"]) is True
        row = self.store.get(record["id"])
        assert row["cold"] == 0 and row["active"] == 1 and row["archived_at"] == 0
        assert self.store.trash("cold")["total"] == 0
        reasons = [v["reason"] for v in self.store.versions_of("record", record["id"])]
        assert "从冷归档取回" in reasons

    def test_purge_removes_row_versions_and_links(self):
        fact_id = self.add("萤火喜欢猫")
        self.store.capture(
            "qq:gm:1",
            "t2",
            [{"role": "user", "content": "周五去看展", "users": ["qq:9"], "time": 2.0}],
        )
        record = self.store.active("qq:gm:1")[-1]
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE facts SET deleted=1 WHERE id=?", (fact_id,))
            db.execute(
                "INSERT INTO versions(kind,target,snapshot,reason,created)"
                " VALUES ('fact',?,?,'测试',1.0)",
                (fact_id, "{}"),
            )
            # 外键引用的迁移项必须先断开，否则删不掉
            db.execute(
                "INSERT INTO migration_items(source,source_key,digest,record_id,reason,"
                "file_hash,metadata,created) VALUES ('old','k','d',?,'r','h','{}',1.0)",
                (record["id"],),
            )
        assert self.store.purge("fact", fact_id) is True
        assert self.store.trash("facts")["total"] == 0
        assert self.store.versions_of("fact", fact_id) == []
        assert self.store.purge("record", record["id"]) is True
        assert self.store.get(record["id"], include_deleted=True) is None
        with self.store.connect() as db:
            assert db.execute(
                "SELECT record_id FROM migration_items WHERE source_key='k'"
            ).fetchone()[0] is None
