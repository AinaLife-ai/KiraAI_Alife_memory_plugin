"""v2.8.0：模型侧入参瘦身（短码 / 可读时间 / 名字随行 / 省略默认值）。"""

import importlib
import json
import sys
import time
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_diet280")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_diet280", package)
r = importlib.import_module("alife_diet280.retrieval")
e = importlib.import_module("alife_diet280.engine")


def test_named_pair_keeps_id_first():
    """`ID(名字)` 里 ID 在前：模型照抄 subject 时拿到的是稳定 ID。"""
    assert r.named_pair("qq:769690776", "周武") == "qq:769690776(周武)"
    assert r.named_pair("qq:769690776", "") == "qq:769690776"
    assert r.named_pair("qq:769690776", "qq:769690776") == "qq:769690776"
    assert r.bare_id("qq:769690776(周武)") == "qq:769690776"
    assert r.bare_id("qq:769690776") == "qq:769690776"


def test_full_time_is_readable_and_keeps_year():
    stamp = time.mktime(time.strptime("2026-03-08 02:36", "%Y-%m-%d %H:%M"))
    assert r.full_time(stamp) == "2026-03-08 02:36"
    assert r.full_time(0) == ""


def test_category_codes_cover_every_category():
    from alife_diet280.contracts import Settings

    schema_categories = {
        "event", "fact", "preference", "commitment",
        "relationship", "profile", "resource", "self",
    }
    assert set(r.CATEGORY_CODES) == schema_categories
    assert all(len(code) == 2 for code in r.CATEGORY_CODES.values())
    assert Settings  # 契约可导入（类别集合来自它）


def test_compress_records_are_compact_and_named():
    rows = [
        {"id": "a" * 32, "role": "user", "level": 0, "users": ["qq:769690776"],
         "summary": "<msg><text>翅 膀被人打了</text></msg>", "start": 1772908601.0,
         "end": 1772908601.0},
        {"id": "b" * 32, "role": "assistant", "level": 1, "users": [],
         "summary": "摘要", "start": 1772908601.0, "end": 1772908700.0},
    ]
    records = e.compress_records(rows, None, {"qq:769690776": "周武"}, ())
    first, second = records
    assert first["s"] == "翅膀被人打了"          # 剥包裹 + 折行空格合并
    assert "role" not in first and "bot" not in first
    assert second["bot"] == 1                    # assistant 用一位标记
    assert first["u"] == ["qq:769690776"]  # 只写 ID（名字在顶层 names 表，省 token）
    assert first["t"] == "2026-03-08 02:36"      # 可读时间（本地）
    assert second["t2"] and second["t2"] != ""   # 多层存档保留起止两点
    assert "summary" not in first and "start" not in first


def test_restore_audit_ids_accepts_alias_and_real_id():
    aliases = {"f1": "real-1", "f2": "real-2"}
    out = e.restore_audit_ids(
        {"actions": [{"target_id": "f1", "source_ids": ["f2", "real-2"]}]}, aliases
    )
    assert out["actions"][0]["target_id"] == "real-1"
    assert out["actions"][0]["source_ids"] == ["real-2", "real-2"]
    try:
        e.restore_audit_ids({"actions": [{"target_id": "zz", "source_ids": []}]}, aliases)
    except ValueError as exc:
        assert "unknown audit target" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("未知 id 必须抛错，交给重试路径")


def test_restore_group_ids_for_merges():
    aliases = {"g1-1": "real-1", "g1-2": "real-2"}
    out = e.restore_group_ids(
        {"groups": [{"target_id": "g1-1", "source_ids": ["g1-1", "g1-2"]}]}, aliases
    )
    assert out["groups"][0]["target_id"] == "real-1"
    assert out["groups"][0]["source_ids"] == ["real-1", "real-2"]
    single = e.restore_group_ids({"source_ids": ["d1", "d2"]}, {"d1": "r1", "d2": "r2"})
    assert single["source_ids"] == ["r1", "r2"]


def test_compress_payload_carries_names_map():
    """名字只在顶层给一次（ID→名字），记录里只用 ID —— 少重复、不丢信息。"""
    src = (Path(__file__).resolve().parents[1] / "engine.py").read_text(encoding="utf-8")
    assert '"names": {' in src, "压缩 payload 顶层要带 names 表"
    assert "names 表（ID→名字）" in src or "names 表" in src, "COMMON 要说明 u 是 ID、名字看 names"


def test_instructions_carry_the_new_limits():
    text = e.COMMON_INSTRUCTION + e.build_instruction("compress", _settings())
    assert "summary 不超过 300 字" in text
    assert "scenario 不超过 20 字" in text
    assert "content 不超过 60 字" in text
    assert "records[].s 是这段对话的原文" in text
    assert "records[].u 是实体 ID 列表" in text, "要说明 u 是 ID、名字在 names 表"
    audit = e.AUDIT_INSTRUCTION
    assert "facts[].sources" not in audit, "sources 已不入参，指令不该再提它"


def test_fact_view_uses_codes_and_drops_defaults():
    fact = {
        "id": "x", "category": "relationship", "subject": "qq:1", "content": "是师傅",
        "importance": 5, "src": "rec-1", "created": 1772908601.0, "relations": [],
    }
    view = r.bot_facts([fact], "sess", short=lambda value: "ab12cd")[0]
    assert view == {"c": "re", "u": "qq:1", "x": "是师傅", "src": "ab12cd", "t": "03-08"}
    # 短码映射里没有的值退回原值，绝不输出 null
    other = r.bot_facts([fact], "sess", short=lambda value: None)[0]
    assert other["src"] == "rec-1"
    # 非默认重要度才输出 imp
    heavy = dict(fact, importance=9)
    assert r.bot_facts([heavy], "sess", short=lambda v: "x")[0]["imp"] == 9


def _settings():
    c = importlib.import_module("alife_diet280.contracts")
    return c.Settings()


@pytest.mark.asyncio
async def test_model_payloads_carry_no_raw_ids_or_float_times(tmp_path):
    """完整审计：发给模型的每一处入参，都不该出现原始长 id 与浮点时间戳。"""
    s = importlib.import_module("alife_diet280.storage")
    c = importlib.import_module("alife_diet280.contracts")
    store = s.Store(tmp_path / "db")
    store.initialize()
    for i in range(4):
        store.capture(
            "qq:gm:1", f"turn{i}",
            [{"role": "user" if i % 2 == 0 else "assistant",
              "content": f"<msg><text>第{i}句 话里有折行</text></msg>",
              "time": 1772908600.0 + i, "users": ["qq:769690776"]}],
        )
    store.observe_name("qq:769690776", "周武", observed=1.0)
    seen = []

    async def model(_, purpose, instruction, schema, payload):
        seen.append((purpose, payload))
        return c.dump({"summary": "摘要", "facts": []})

    cfg = c.Settings(probability=1.0, threshold=4, batch_size=2, model_retries=0)
    await e.Engine(store, lambda: cfg, model, None, None).compress("qq:gm:1")
    assert seen, "压缩必须调用模型"
    for purpose, payload in seen:
        blob = json.dumps(payload, ensure_ascii=False)
        assert "<msg" not in blob, f"{purpose} 入参仍有 XML 包裹"
        # 真实记录 id 是 32 位以上；入参里只应有 r1..rN 这类短别名
        ids = [r["id"] for r in payload.get("records", [])]
        assert all(len(value) <= 8 for value in ids), f"{purpose} 入参用了真实 id"
        # 时间必须是可读字符串，不能再是 epoch 浮点
        for record in payload.get("records", []):
            for key in ("t", "t2"):
                if key in record:
                    assert isinstance(record[key], str) and "-" in record[key]
