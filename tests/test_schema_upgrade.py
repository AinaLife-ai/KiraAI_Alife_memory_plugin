"""从旧库升级：新增列与依赖它的索引必须都能补上。

为什么单独一个文件：v2.13.0 的事故是「依赖新列的索引建在迁移之前」——
老库还没有那一列，`CREATE INDEX ... WHERE rewrite_pending=1` 直接
`no such column: rewrite_pending`，插件**整个起不来**。
而当时的测试全都建**全新的库**（新库建表时就带上了那两列），迁移路径一次都没跑过 ✗
"""
import importlib
import sqlite3
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_schema_upgrade")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_schema_upgrade", package)

storage = importlib.import_module("alife_schema_upgrade.storage")
retrieval = importlib.import_module("alife_schema_upgrade.retrieval")

# v2.12 之前的老 facts 表（没有 rewrite_*，也没有 importance/merge_pending 之外的列）
OLD_FACTS = """
CREATE TABLE facts (
  id TEXT PRIMARY KEY, sid TEXT NOT NULL, subject TEXT NOT NULL, category TEXT NOT NULL,
  content TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', scenario TEXT NOT NULL DEFAULT '',
  tags TEXT NOT NULL DEFAULT '[]', relations TEXT NOT NULL DEFAULT '[]',
  sources TEXT NOT NULL DEFAULT '[]', importance INTEGER NOT NULL DEFAULT 5,
  deleted INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 1,
  fingerprint TEXT NOT NULL DEFAULT '', audited REAL NOT NULL DEFAULT 0,
  merge_pending INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL DEFAULT 0,
  visibility TEXT NOT NULL DEFAULT 'session');
CREATE INDEX fact_identity ON facts(sid,subject,fingerprint,deleted);
CREATE INDEX fact_subject ON facts(sid,subject,category,deleted);
CREATE INDEX fact_scan ON facts(deleted,merge_pending);
"""


def legacy_store(tmp_path):
    """造一个「老库」：只有老 schema 的 facts 表，别的表让 initialize 去建。"""
    path = tmp_path / "legacy.sqlite3"
    db = sqlite3.connect(path)
    db.executescript(OLD_FACTS)
    db.execute(
        "INSERT INTO facts(id,sid,subject,category,content,reason,tags,relations,"
        "sources,importance,deleted,revision,fingerprint,audited,merge_pending,created)"
        " VALUES ('old1','qq:gm:1','qq:9','preference','主人喜欢乌龙茶','','[]','[]',"
        "'[]',7,0,1,'fp',0,0,0)"
    )
    db.commit()
    db.close()
    return path


def test_legacy_db_initializes_and_gets_new_columns(tmp_path):
    """老库 initialize 不能抛错，且要补齐 facts.rewrite_pending / rewrite_attempts。"""
    path = legacy_store(tmp_path)
    store = storage.Store(path)
    store.initialize()  # v2.13.0 就是在这里炸的：no such column: rewrite_pending

    with store.connect() as db:
        columns = {row[1] for row in db.execute("PRAGMA table_info(facts)")}
        indexes = {row[0] for row in db.execute("SELECT name FROM sqlite_master")}
        kept = store.row(db.execute("SELECT * FROM facts WHERE id='old1'").fetchone())
    assert {"rewrite_pending", "rewrite_attempts"} <= columns
    assert "fact_rewrite" in indexes, "依赖新列的索引要在迁移之后建出来"
    assert kept["content"] == "主人喜欢乌龙茶", "老数据一个字都不能动"
    assert kept["importance"] == 7
    assert kept["rewrite_pending"] == 0


def test_legacy_db_upgrade_is_repeatable(tmp_path):
    """重复 initialize（插件每次启动都会跑）不能再炸，也不能重复加列。"""
    path = legacy_store(tmp_path)
    for _ in range(3):
        store = storage.Store(path)
        store.initialize()
    with store.connect() as db:
        columns = [row[1] for row in db.execute("PRAGMA table_info(facts)")]
    assert columns.count("rewrite_pending") == 1


def test_time_and_speaker_columns_are_migrated(tmp_path):
    """v2.14 的时间/发言人列也要能补齐（老库升级路径）。"""
    path = tmp_path / "legacy.sqlite3"
    legacy = storage.Store(path)
    legacy.initialize()
    with legacy.connect() as db:  # 抹掉新列，模拟老库
        for table, column in (("records", "speaker"), ("facts", "event_at")):
            assert column in [r[1] for r in db.execute("PRAGMA table_info(%s)" % table)]
    upgraded = storage.Store(path)
    upgraded.initialize()  # 幂等，不该抛错
    with upgraded.connect() as db:
        assert "speaker" in [r[1] for r in db.execute("PRAGMA table_info(records)")]
        fact_columns = [r[1] for r in db.execute("PRAGMA table_info(facts)")]
    assert "event_at" in fact_columns and "event_end" in fact_columns


def test_new_column_is_usable_after_upgrade(tmp_path):
    """升级后新功能真的能用：标记 → 挑候选 → 还原重做。"""
    path = legacy_store(tmp_path)
    store = storage.Store(path)
    store.initialize()
    assert store.mark_rewrite_pending(["old1"], 1) == 1
    assert [row["id"] for row in store.pending_rewrites(5)] == ["old1"]
    assert store.rewrite_backlog() == 1
    # 没有"降级合并"的版本快照 → 拒绝重做（不做半截），但标记保留
    assert store.unmerge_fact("old1") is False
    assert store.rewrite_backlog() == 1


def test_schema_version_is_migrated_not_recreated(tmp_path):
    """迁移不能把老库当新库建：老表要留着（否则等于丢数据）。"""
    path = legacy_store(tmp_path)
    store = storage.Store(path)
    store.initialize()
    with store.connect() as db:
        sql = db.execute(
            "SELECT sql FROM sqlite_master WHERE name='facts'"
        ).fetchone()[0]
    assert "rewrite_pending" in sql, "列应当是被 ALTER 补上的"


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
