"""检索索引的健康度：回填必须结束、写入必须同步、失败必须显式降级。

线上问题（v2.11.0 后）：界面一直停在「索引 回填中」。
根因：摘要为空 / 只有标点的记录永远算不出索引体（bigram 为空），
而 search_body='' 正是「还没进索引」的判据 → 回填每批都把同一批行捞出来，
循环永不结束，状态卡在 building。
"""

import asyncio
import importlib
import os
import sqlite3
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_fts_health")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_fts_health", package)

storage = importlib.import_module("alife_fts_health.storage")
retrieval = importlib.import_module("alife_fts_health.retrieval")


def open_store(path):
    store = storage.Store(path)
    store.initialize()
    if store.search_index_state() == "unavailable":
        pytest.skip("本机 SQLite 无 FTS5")
    return store


def raw_record(store, rid, sid, summary, search_body=""):
    """绕过本模块直接写库：模拟旧版 / 第三方工具（这类行必须照样能召回）。"""
    with store.connect() as db:
        db.execute(
            "INSERT INTO records(id,sid,role,level,start,end,summary,content,users,"
            "position,created,search_body) VALUES (?,?,'user',0,0.0,0.0,?,?,'[]',0,0.0,?)",
            (rid, sid, summary, summary, search_body),
        )


def test_backfill_finishes_when_a_summary_has_no_text(tmp_path):
    store = open_store(tmp_path / "db")
    store.capture(
        "qq:gm:A",
        "ev0",
        [{"role": "user", "content": "星月喜欢草莓蛋糕", "users": ["u:1"], "time": 1.0}],
    )
    raw_record(store, "empty", "qq:gm:A", "")
    raw_record(store, "punct", "qq:gm:A", "。。。！")
    raw_record(store, "wide", "qq:gm:A", "   ")

    # 模拟重启：启动探测到「未进索引」的行 → building，然后后台分批回填
    reopened = open_store(tmp_path / "db")
    assert reopened.search_index_state() == "building"

    rounds = 0
    while not reopened.index_backfill():
        rounds += 1
        assert rounds < 50, "回填没有结束（线上表现就是界面一直「索引 回填中」）"

    assert reopened.search_index_state() == "ready"
    with reopened.connect() as db:
        bodies = dict(db.execute("SELECT id,search_body FROM records").fetchall())
    # 没有文字的摘要写占位符：非空 → 不会再被当成「未进索引」捞出来
    for rid in ("empty", "punct", "wide"):
        assert bodies[rid] == storage.NO_TOKENS, rid
    assert "" not in bodies.values(), "仍有行被留在「未进索引」状态"


def test_new_records_enter_the_index_immediately(tmp_path):
    """写入时就该算好索引体：否则新记录要等下次启动回填才进索引。"""
    store = open_store(tmp_path / "db")
    assert store.search_index_state() == "ready"

    store.capture(
        "qq:gm:A",
        "ev0",
        [{"role": "user", "content": "星月喜欢草莓蛋糕", "users": ["u:1"], "time": 1.0}],
    )
    record_id = store.memorize(
        "qq:gm:A", "主人喜欢乌龙茶", ["u:1"], 2.0, 2.0, importance=7, category="偏好"
    )
    # 新写入的行自带索引体 → 不需要等回填
    assert store.search_index_state() == "ready"

    hits = store._fts_hits(retrieval.query_tokens("草莓蛋糕"))
    assert hits, "capture 写入的记录没有进 FTS 索引"
    hits = store._fts_hits(retrieval.query_tokens("乌龙茶"))
    assert hits, "永久记忆写入时没有进 FTS 索引"

    with store.connect() as db:
        body = db.execute(
            "SELECT search_body FROM records WHERE id=?", (record_id,)
        ).fetchone()[0]
    assert body == storage.search_body_of("主人喜欢乌龙茶")


def test_editing_a_summary_refreshes_the_index(tmp_path):
    """摘要被编辑后，索引体必须一起更新，否则该行会静默离开 FTS 候选集。"""
    store = open_store(tmp_path / "db")
    record_id = store.memorize(
        "qq:gm:A", "主人喜欢乌龙茶", ["u:1"], 1.0, 1.0, importance=7, category="偏好"
    )
    store.edit(
        "record", record_id, 1, {"summary": "主人改喝普洱茶了"}, "改了口径"
    )
    with store.connect() as db:
        body = db.execute(
            "SELECT search_body FROM records WHERE id=?", (record_id,)
        ).fetchone()[0]
    assert body == storage.search_body_of("主人改喝普洱茶了")
    hits = store._fts_hits(retrieval.query_tokens("普洱茶"))
    assert hits, "编辑后的摘要没有进索引 → 这次编辑会被静默漏召回"
    assert not store._fts_hits(retrieval.query_tokens("乌龙茶"))
    # 新文字照样能被检索到（走索引路径，结果与全表一致）
    store._fts_state = "ready"
    found = store.search(
        "qq:gm:A", lexical="普洱茶", scope="global", users=["u:1"], limit=10, exclude_sid=""
    )
    assert [i["id"] for i in found["items"]] == [record_id]


@pytest.mark.parametrize(
    "text",
    [
        "星月喜欢草莓蛋糕",
        "上次说 iPhone 15 的事",
        "cat sleeps",
        "翅 膀被人处刑了",
    ],
)
def test_every_substring_query_still_hits_the_index(tmp_path, text):
    """索引候选集必须是打分结果的超集：任何子串查询都不能漏。

    实测教训：v2.11.0 的索引体只有双字滑窗，单字查询（「猫」）与跨词边界的
    子串查询（「iph」⊂「iphone」）都查不到索引 → 候选集把该命中的行排除掉。
    """
    store = open_store(tmp_path / "db")
    store.capture(
        "qq:gm:A",
        "ev0",
        [{"role": "user", "content": text, "users": ["u:1"], "time": 1.0}],
    )
    assert store.search_index_state() == "ready"

    queries = {
        text[i:j]
        for i in range(len(text))
        for j in range(i + 1, min(len(text), i + 4) + 1)
    }
    for query in sorted(queries):
        tokens = retrieval.query_tokens(query)
        if not tokens:
            continue
        # 打分口径：任一词元（已 squeeze/casefold）是摘要的子串即命中
        expected = any(token in text.casefold() for token in tokens)
        if expected:
            assert store._fts_hits(tokens), "索引漏掉了子串查询：%r" % query
        indexed = store.search(
            "qq:gm:A", lexical=query, scope="global", users=["u:1"], limit=10, exclude_sid=""
        )
        store._fts_state = "building"
        scanned = store.search(
            "qq:gm:A", lexical=query, scope="global", users=["u:1"], limit=10, exclude_sid=""
        )
        store._fts_state = "ready"
        assert [i["id"] for i in indexed["items"]] == [
            i["id"] for i in scanned["items"]
        ], query
        if expected:
            assert indexed["items"], "打分能命中、索引路径却漏了：%r" % query


def test_failed_backfill_degrades_to_full_scan(tmp_path):
    """回填失败必须显式降级：留在 building 会让界面永远显示「回填中」。"""
    store = open_store(tmp_path / "db")
    store.capture(
        "qq:gm:A",
        "ev0",
        [{"role": "user", "content": "星月喜欢草莓蛋糕", "users": ["u:1"], "time": 1.0}],
    )
    store.abandon_search_index()
    assert store.search_index_state() == "unavailable"
    assert store._fts_hits(retrieval.query_tokens("草莓蛋糕")) is None
    # 索引退出后，全表路径必须照样能召回
    found = store.search(
        "qq:gm:A", lexical="草莓蛋糕", scope="global", users=["u:1"], limit=10, exclude_sid=""
    )
    assert found["items"], "降级后召回不能变少"


def test_scheme_upgrade_rebuilds_old_index_bodies(tmp_path):
    """v2.11.0 的旧索引体（只有双字滑窗）必须被识别为过期并重算。

    不重算的话，索引看着是「已启用」，但单字查询（「猫」）与跨词边界子串
    （「iph」⊂「iphone」）会静默查不到 → 漏召回。
    """
    import re

    def legacy_body(text):
        squeezed = retrieval.squeeze(text)
        out = []
        for chunk in re.findall(r"[a-z0-9_]+|[\u3400-\u9fff]+", squeezed.casefold()):
            if len(chunk) >= 2 and re.match(r"[\u3400-\u9fff]", chunk):
                out.extend(chunk[i : i + 2] for i in range(len(chunk) - 1))
            else:
                out.append(chunk)
        return " ".join(out)

    store = open_store(tmp_path / "db")
    store.capture(
        "qq:gm:A",
        "ev0",
        [{"role": "user", "content": "我喜欢猫", "users": ["u:1"], "time": 1.0}],
    )
    with store.connect() as db:
        # 模拟 v2.11.0 的库：旧口径索引体 + 没有口径标记
        db.execute("UPDATE records SET search_body=?", (legacy_body("我喜欢猫"),))
        db.execute("DELETE FROM meta WHERE key='search_index_scheme'")

    reopened = open_store(tmp_path / "db")
    assert reopened.search_index_state() == "building"
    assert reopened.prepare_search_index() is True
    rounds = 0
    while not reopened.index_backfill():
        rounds += 1
        assert rounds < 50
    assert reopened.search_index_state() == "ready"
    assert reopened._fts_hits(retrieval.query_tokens("猫")), "口径升级后单字查询仍漏"


@pytest.mark.asyncio
async def test_startup_backfill_finishes_on_a_legacy_db(tmp_path):
    """线上场景：旧库（全部还没进索引 + 有无文字的摘要）重启后必须真的走到 ready。

    症状就是「界面一直显示 索引 回填中」——回填循环永远结束不了。
    """
    if not os.environ.get("KIRA_CORE"):
        pytest.skip("set KIRA_CORE for host integration")
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_helpers_plugin import build_plugin

    plugin, store = await build_plugin(tmp_path)
    try:
        store.capture(
            "qq:gm:A",
            "ev0",
            [{"role": "user", "content": "星月喜欢草莓蛋糕", "users": ["u:1"], "time": 1.0}],
        )
        raw_record(store, "empty", "qq:gm:A", "")  # 没有可检索文字的摘要
        path = store.path
    finally:
        await plugin.terminate()

    # 打回 v2.11.0 的样子：索引体全空、没有口径标记
    legacy = storage.Store(path)
    with legacy.connect() as db:
        db.execute("UPDATE records SET search_body=''")
        db.execute("DELETE FROM meta WHERE key='search_index_scheme'")

    reopened, store2 = await build_plugin(tmp_path)  # 用户升级后重启插件
    try:
        for _ in range(250):
            if store2.search_index_state() != "building":
                break
            await asyncio.sleep(0.02)
        assert store2.search_index_state() == "ready", "回填没有结束（界面会一直停在「回填中」）"
        # 回填的工作量与「有无可检索文字」都要被记下来：启动日志就是照这些数字说的
        # （「检索索引已就绪：本次补齐 N 条（其中 M 条没有任何可检索文字）」）
        stats = store2.search_index_stats()
        assert stats["filled"] >= 2 and stats["plain"] >= 1, stats
    finally:
        await reopened.terminate()


@pytest.mark.asyncio
async def test_backfill_failure_is_reported_not_left_building(tmp_path):
    """插件层：回填抛错 → 状态降级为 unavailable（而不是留在 building）。"""
    if not os.environ.get("KIRA_CORE"):
        pytest.skip("set KIRA_CORE for host integration")
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_helpers_plugin import build_plugin

    plugin, store = await build_plugin(tmp_path)
    try:
        real_call = store.call

        async def failing_call(method, *args, **kwargs):
            if method == "index_backfill":
                raise sqlite3.OperationalError("disk I/O error")
            return await real_call(method, *args, **kwargs)

        store.call = failing_call
        await plugin.build_search_index()
        assert store.search_index_state() == "unavailable"
    finally:
        await plugin.terminate()
