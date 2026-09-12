"""成本优化（v2.16.0）：紧凑 schema 与规则块精简后的**一致性**兜底。

改这些模板时最怕两件事：漏掉必填字段（模型输出被拒 ✗）或悄悄长回去（钱白花 ✗），
所以都要有测试钉住。
"""

import importlib
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_compact_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_compact_test", package)
engine = importlib.import_module("alife_compact_test.engine")
contracts = importlib.import_module("alife_compact_test.contracts")

PURPOSES = {
    "compress": (contracts.Compression, "Compression"),
    "fact_merge": (contracts.FactMerge, "FactMerge"),
    "audit": (contracts.Audit, "Audit"),
}


def _required_fields(model, prefix=""):
    """契约里所有必填字段名（含 $defs 里的嵌套模型）。"""
    schema = model.model_json_schema()
    out = set()

    def walk(node, path=""):
        if not isinstance(node, dict):
            return
        for name in node.get("required", []):
            out.add(name)
            out.add(path + name)
        for name, child in (node.get("properties") or {}).items():
            walk(child, path + name + ".")
        for name, child in (node.get("$defs") or {}).items():
            walk(child, name + ".")
        if "items" in node:
            walk(node["items"], path)

    walk(schema, prefix)
    return out


def test_compact_schema_covers_every_required_field():
    """紧凑声明必须覆盖契约的全部必填字段名 —— 漏一个就会开始被拒 ✗。"""
    for purpose, (model, _) in PURPOSES.items():
        text = engine.COMPACT_SCHEMAS[purpose]
        missing = [
            name
            for name in _required_fields(model)
            if "." not in name and name not in text
        ]
        assert not missing, "%s 的紧凑声明漏了：%s" % (purpose, missing)


def test_compact_schema_is_much_smaller_than_auto_schema():
    """瘦身必须真的瘦（否则白改 ✗）；同时不能瘦到没有结构说明。"""
    for purpose, (model, _) in PURPOSES.items():
        import json

        auto = len(
            json.dumps(engine.strip_schema_titles(model.model_json_schema()), ensure_ascii=False)
        )
        compact = len(engine.COMPACT_SCHEMAS[purpose])
        assert compact < auto * 0.7, (purpose, auto, compact)
        assert "必填" in engine.COMPACT_SCHEMAS[purpose]
        assert "常见错误" in engine.COMPACT_SCHEMAS[purpose]


def test_compact_schema_falls_back_to_full_schema():
    """连续被拒要有兜底（否则省钱的代价是白跑 ✗）。"""
    source = (ROOT / "engine.py").read_text(encoding="utf-8")
    assert "attempt >= 2 and isinstance(schema, str)" in source
    assert "strip_schema_titles(contract.model_json_schema())" in source
    # 提示词组装要支持字符串形式的 schema
    main = (ROOT / "main.py").read_text(encoding="utf-8")
    assert "isinstance(schema, str)" in main


def test_memory_rules_keeps_the_essential_tokens():
    """规则块精简后，这些关键 token 一个都不能少（否则模型会误读 payload ✗）。"""
    main_text = (ROOT / "main.py").read_text(encoding="utf-8")
    start = main_text.find("MEMORY_RULES = (")
    block = main_text[start : main_text.find(")\n", start)]
    for token in (
        "next_batch",
        "needs_review",
        "SearchMemoryArchive",
        "GetProfile",
        "Memorize",
        "CorrectMemory",
        "names",
        "sp",
        "rec",
        "t2",
        "src",
    ):
        assert token in block, token
    assert len(block) < 900, "规则块每轮全价发送，涨回去就是白花钱 ✗"
