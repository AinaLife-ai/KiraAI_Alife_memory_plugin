"""后台任务明细：压缩/审计处理了哪几条，能否回溯与编辑。"""

import asyncio
import importlib
import sys
import types
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
