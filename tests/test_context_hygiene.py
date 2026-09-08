"""Context hygiene: tool payloads never flood the injected memory block."""

import importlib
import json
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_hygiene_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_hygiene_test", package)
s = importlib.import_module("alife_hygiene_test.storage")
r = importlib.import_module("alife_hygiene_test.retrieval")

PREFIX = r.TOOL_RESULT_PREFIX
SID = "qq:gm:188395693"


def seed(store):
    echo = '{"ok":true,"total":52,"items":[],"next_page":null,"archives_in_context":3}'
    store.capture(
        SID,
        "echo",
        [{"role": "assistant", "content": PREFIX + "\n" + echo, "time": 1.0, "users": []}],
    )
    big = '{"weather":"' + "x" * 900 + '"}'
    store.capture(
        SID,
        "big",
        [{"role": "assistant", "content": PREFIX + "\n" + big, "time": 2.0, "users": []}],
    )
    calls = json.dumps(
        {
            "tool_calls": [
                {
                    "function": {
                        "name": "SearchMemoryArchive",
                        "arguments": '{"keyword":"COM3D2"}',
                    }
                }
            ]
        }
    )
    store.capture(
        SID,
        "calls",
        [{"role": "assistant", "content": "好的\n" + calls, "time": 3.0, "users": []}],
    )
    store.capture(
        SID,
        "plain",
        [{"role": "user", "content": "今天天气不错", "time": 4.0, "users": []}],
    )
    return big, calls


def test_capture_keeps_summary_and_content_separate(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    store.capture(
        SID,
        "ev",
        [
            {
                "role": "assistant",
                "content": "完整原文",
                "summary": "短摘要",
                "time": 1.0,
                "users": [],
            }
        ],
    )
    row = store.active(SID)[0]
    assert row["summary"] == "短摘要" and row["content"] == "完整原文"


def test_cleanup_removes_echoes_and_compacts_tool_summaries(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    big, calls = seed(store)
    report = store.cleanup_tool_records()
    assert report["removed"] == 1 and report["rewritten"] == 2
    assert report["freed_chars"] > 800

    echo = next(
        rec
        for rec in store.export()["records"]
        if rec["content"].startswith(PREFIX)
        and '"archives_in_context"' in rec["content"]
    )
    assert echo["deleted"] == 1

    big_row = next(rec for rec in store.export()["records"] if rec["content"] == PREFIX + "\n" + big)
    assert big_row["deleted"] == 0
    assert big_row["content"] == PREFIX + "\n" + big  # raw text untouched
    assert big_row["summary"] == PREFIX + r.tool_preview(big)
    assert len(big_row["summary"]) < 300

    calls_row = next(
        rec for rec in store.export()["records"] if rec["content"].endswith(calls)
    )
    assert calls_row["summary"] == '好的 [调用工具：SearchMemoryArchive({"keyword":"COM3D2"})]'
    assert calls_row["content"] == "好的\n" + calls

    plain = next(rec for rec in store.export()["records"] if rec["content"] == "今天天气不错")
    assert plain["summary"] == plain["content"] == "今天天气不错"


def test_cleanup_is_idempotent_and_drops_orphan_facts(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    seed(store)
    echo = next(
        rec
        for rec in store.export()["records"]
        if rec["content"].startswith(PREFIX) and '"archives_in_context"' in rec["content"]
    )
    with store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        store._add_fact(
            db,
            SID,
            {
                "category": "fact",
                "subject": "qq:1",
                "content": "来自回声的事实",
                "reason": "",
                "scenario": "",
                "tags": [],
                "relations": [],
                "source_ids": [echo["id"]],
            },
        )
    assert len(store.facts(SID, limit=10)) == 1
    store.cleanup_tool_records()
    assert store.facts(SID, limit=10) == []
    again = store.cleanup_tool_records()
    assert again["removed"] == 0 and again["rewritten"] == 0
    assert again["freed_chars"] == 0


def test_totals_and_top_users_respect_scope(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    for uid, count in (("qq:1", 3), ("qq:2", 1)):
        for i in range(count):
            store.capture(
                SID,
                f"{uid}-{i}",
                [
                    {
                        "role": "user",
                        "content": f"{uid} 的第 {i} 条",
                        "time": float(i),
                        "users": [uid],
                    }
                ],
            )
    store.capture(
        "qq:dm:9",
        "x",
        [{"role": "user", "content": "私聊消息", "time": 1.0, "users": ["qq:9"]}],
    )
    session = store.totals(SID, ["qq:1"], "session")
    assert session["records"] == 4 and session["users"] == 2
    assert session["sessions"] == 1 and session["groups"] == 1
    whole = store.totals(SID, ["qq:1"], "global")
    assert whole["records"] == 5 and whole["users"] == 3 and whole["sessions"] == 2
    top = store.top_users(SID, ["qq:1"], "global", limit=10)
    assert top["total"] == 3 and top["items"][0]["user_id"] == "qq:1"
    capped = store.top_users(SID, ["qq:1"], "global", limit=1)
    assert len(capped["items"]) == 1 and capped["total"] == 3


def test_session_affinity_reorders_without_narrowing(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    for sid in ("qq:gm:com", "qq:gm:newbot"):
        for i in range(2):
            store.capture(
                sid,
                f"ev{i}",
                [
                    {
                        "role": "user",
                        "content": "COM3D2 群里的绿岛酒吧趣事",
                        "time": float(i),
                        "users": ["qq:1"],
                    }
                ],
            )
    plain = store.search(lexical="COM3D2 绿岛酒吧", scope="global", limit=4)
    assert {item["sid"] for item in plain["items"]} == {"qq:gm:com", "qq:gm:newbot"}
    preferred = store.search(
        lexical="COM3D2 绿岛酒吧",
        scope="global",
        limit=4,
        prefer_sid="qq:gm:com",
        prefer_users=["qq:1"],
    )
    assert [item["sid"] for item in preferred["items"]] == [
        "qq:gm:com",
        "qq:gm:com",
        "qq:gm:newbot",
        "qq:gm:newbot",
    ]
    assert preferred["total"] == plain["total"]
