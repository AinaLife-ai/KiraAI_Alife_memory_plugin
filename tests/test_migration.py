import hashlib
import importlib
import json
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_migration_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_migration_test", package)
m = importlib.import_module("alife_migration_test.migration")
s = importlib.import_module("alife_migration_test.storage")
c = importlib.import_module("alife_migration_test.contracts")


def write(root, relative, text):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def fixture(root):
    write(
        root,
        "core.txt",
        "共同约定每周日散步\n" + "默认记忆保留长句" * 20 + "\n暂无信息\n",
    )
    write(
        root,
        "entities/user_test%3Au/facts/cat.toml",
        """id = "cat"
type = "preference"
text = '喜欢"橘猫"，不喜欢噪音'
tags = ["偏好"]
[source]
session = "test:pm:u"
time = 2026-09-01T08:00:00+08:00
""",
    )
    write(
        root,
        "entities/user_test%3Au/facts/copy.toml",
        """id = "cat-copy"
type = "preference"
text = '喜欢"橘猫"，不喜欢噪音'
""",
    )
    for size in (120, 121):
        write(
            root,
            f"entities/user_test%3Au/facts/length{size}.toml",
            "text = "
            + json.dumps(
                "有效记忆" * 30 + ("尾" if size % 2 else ""), ensure_ascii=False
            ),
        )
    write(
        root, "entities/user_other%3Au/facts/private.toml", 'text = "另一个平台的秘密"'
    )
    write(
        root,
        "entities/user_unknown/facts/unspecified.toml",
        'text = "归属尚待确认的旧事实"',
    )
    write(root, "entities/group_test%3Ag/facts/meet.toml", 'text = "小组周五开会"')
    write(
        root,
        "entities/user_test%3Au/profile.json",
        json.dumps(
            {
                "entity_id": "test:u",
                "entity_type": "user",
                "name": "小明",
                "preferences": {"drink": "不喝咖啡"},
                "relationships": {"test:v": "朋友"},
                "description": "长篇画像" * 40,
                "facts": ["养了一只猫"],
            },
            ensure_ascii=False,
        ),
    )
    write(root, "global/self/facts/identity.toml", 'text = "机器人自述喜欢星空"')
    write(root, "global/facts/junk.toml", 'text = "```json xxx```"')


@pytest.fixture
def store(tmp_path):
    store = s.Store(tmp_path / "new" / "alife.sqlite3")
    store.initialize()
    return store


def test_source_immutability_limits_dedupe_reachability_and_provenance(tmp_path, store):
    root = tmp_path / "memory"
    fixture(root)
    before = {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }
    for pid in m.SOURCES:
        snap = m.snapshot(root, pid, 120)
        assert not snap["errors"]
        store.import_legacy(snap)
    exported = store.export()
    assert len(exported["migration_items"]) > len(exported["records"])
    assert all(r["level"] == 0 for r in exported["records"])
    assert any(len(r["summary"]) == 120 for r in exported["records"])
    assert not any(len(r["summary"]) == 121 for r in exported["records"])
    assert not any("长篇画像" in r["summary"] for r in exported["records"])
    assert any(
        len(r["summary"]) > 120 and "默认" in r["summary"] for r in exported["records"]
    )
    assert {r["reason"] for r in exported["migration_items"]} >= {
        "too_long",
        "placeholder",
        "markup_or_serialized_output",
    }
    context = store.context("test:dm:u", ["test:u"])
    content = str(context)
    assert "不喜欢噪音" in content and "每周日散步" in content and "星空" in content
    assert (
        "另一个平台的秘密" not in content
        and "归属尚待确认" not in content
        and "周五开会" not in content
    )
    other = str(store.context("test:dm:v", ["test:v"]))
    assert "不喜欢噪音" not in other and "每周日散步" in other
    assert "周五开会" in str(store.context("test:gm:g", []))
    found = store.search(sid="test:dm:u", keyword="橘猫", users=["test:u"])
    assert found["total"] == 1
    record = store.get(found["items"][0]["id"])
    assert len(record["legacy_sources"]) == 2
    assert record["start"] == 1788220800.0
    facts = store.facts("test:dm:u", users=["test:u"], include_shared=True)
    assert any(
        f["relations"]
        == [{"subject": "test:u", "predicate": "朋友", "object": "test:v"}]
        for f in facts
    )
    for pid in m.SOURCES:
        store.import_legacy(m.snapshot(root, pid, 120))
    assert len(store.export()["records"]) == len(exported["records"])
    assert before == {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }


def test_rerun_respects_edits_deletions_and_changed_filter(tmp_path, store):
    root = tmp_path / "memory"
    fixture(root)
    store.import_legacy(m.snapshot(root, m.KIRAOS, 120))
    row = store.search(sid="test:dm:u", keyword="橘猫", users=["test:u"])["items"][0]
    store.edit("record", row["id"], 1, {"deleted": True}, "human deletion")
    store.import_legacy(m.snapshot(root, m.KIRAOS, 160))
    assert store.get(row["id"]) is None
    assert any(len(r["summary"]) == 121 for r in store.export()["records"])
    write(root, "entities/user_test%3Au/facts/cat.toml", 'text = "现在喜欢安静的狗"')
    store.import_legacy(m.snapshot(root, m.KIRAOS, 160))
    assert (
        store.search(sid="test:dm:u", keyword="安静的狗", users=["test:u"])["total"]
        == 1
    )


def test_atomic_receipt_and_import_rollback(tmp_path, store, monkeypatch):
    root = tmp_path / "memory"
    write(root, "core.txt", "一个可用事实\n另一个可用事实")

    def fail(*args):
        raise RuntimeError("injected failure")

    monkeypatch.setattr(store, "_add_fact", fail)
    with pytest.raises(RuntimeError):
        store.import_legacy(m.snapshot(root, m.SIMPLE, 120))
    assert store.export()["records"] == []
    assert store.export()["migration_items"] == []


def test_strict_toml_error_keeps_other_sources_readable(tmp_path, store):
    root = tmp_path / "memory"
    write(root, "entities/user_test%3Au/facts/bad.toml", 'text = "unclosed')
    write(root, "entities/user_test%3Au/profile.json", '{"name":"one","name":"two"}')
    write(
        root,
        "entities/user_test%3Au/facts/good.toml",
        '''text = """第一行\n第二行，保持原意"""''',
    )
    snap = m.snapshot(root, m.KIRAOS, 120)
    assert len(snap["errors"]) == 2
    store.import_legacy(snap)
    assert "第一行\n第二行" in store.context("test:dm:u", ["test:u"])[0]["summary"]


def test_shared_visibility_survives_compression(tmp_path, store):
    root = tmp_path / "memory"
    write(root, "core.txt", "喜欢散步\n不喝咖啡\n喜欢安静\n周日休息")
    store.import_legacy(m.snapshot(root, m.SIMPLE, 120))
    rows = store.active("legacy:global")[:2]
    key = store.compress(
        "legacy:global", rows, 1, {"summary": "喜欢散步，不喝咖啡", "facts": []}
    )
    assert key in {r["id"] for r in store.context("other:dm:v", ["other:v"])}


def test_no_vector_local_search_does_not_return_unrelated_rows(store):
    store.capture(
        "s",
        "event",
        [
            {"role": "user", "content": text, "users": [], "time": 1.0}
            for text in ["小明喜欢橘猫", "周五开会", "cat sleeps"]
        ],
    )
    assert not c.Settings().semantic_enabled
    assert store.search(sid="s", lexical="记得橘猫吗")["total"] == 1
    assert store.search(sid="s", lexical="cat")["items"][0]["summary"] == "cat sleeps"
    assert store.search(sid="s", lexical="不存在")["total"] == 0
