"""v2.7.0：给模型看的东西要短、要稳、要不会抄错。

覆盖：短码（证据编码）、文本清洗（只剥外层容器）、嵌套长文本截断、
检索词空白归一、工具返回的紧凑形态与 who 表。
"""

import importlib
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_compact_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_compact_test", package)
s = importlib.import_module("alife_compact_test.storage")
r = importlib.import_module("alife_compact_test.retrieval")


def test_short_id_is_stable_short_and_reversible(tmp_path):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    real = "legacy-" + "a" * 64  # 迁移进来的历史 id 有 71 字符
    short = store.short_id(real)
    assert len(short) <= 8 and short != real
    assert store.short_id(real) == short        # 幂等
    assert store.real_id(short) == real         # 短码 → 真实 id
    assert store.real_id(real) == real          # 全 id 也认
    assert store.real_id("不存在") == "不存在"  # 未知值原样返回
    store.initialize()                          # 重启后映射不变
    assert store.short_id(real) == short


def test_clean_text_strips_only_outer_container():
    assert r.clean_text(
        '<msg message_id="1">\n    <text>行，主人想听我就多说点</text>\n</msg>'
    ) == "行，主人想听我就多说点"
    # 正文里真的写了标签、写了小于号，必须原样保留
    assert r.clean_text("<msg><text>他说 3<5 且提到 <text> 这个词</text></msg>") == (
        "他说 3<5 且提到 <text> 这个词"
    )
    # 多段消息保留边界
    assert r.clean_text("<msg><text>第一条</text></msg><msg><text>第二条</text></msg>") == (
        "第一条\n第二条"
    )
    # 折行残留是「孤立的一个空格」，合并掉（日志里 33 处实例全是这种）
    assert r.clean_text("<msg><text>翅 膀被他处刑</text></msg>") == "翅膀被他处刑"
    assert r.clean_text("群友并发布鬼图，末冬 时也在") == "群友并发布鬼图，末冬时也在"
    # 连续被空白隔开的写法（三个以上）视为刻意强调，原样保留
    assert r.clean_text("翅 膀 被 打 了") == "翅 膀 被 打 了"
    assert r.clean_text("<reply>123</reply>他说 3<5") == "↩123他说 3<5"


def test_spaced_text_is_not_over_normalized():
    """真实的空格要保住：逐字强调、以及名字里真的带空白的昵称。"""
    # 刻意的逐字强调（三个以上汉字被空白隔开）
    assert r.clean_text("很 重 要") == "很 重 要"
    assert r.clean_text("不 要 这 样") == "不 要 这 样"
    # 名字带空格：调用方（main.py）会把实体表里这类名字作为 keep 传进来
    assert r.clean_text("今天星 月来找我", keep=["星 月"]) == "今天星 月来找我"
    assert r.clean_text("今天星 月来找我") == "今天星月来找我"  # 不传保护名单时才会合并
    # 保护名单里的名字不会被截断/剥离，也不受折行合并影响
    assert r.clean_text("<msg><text>并发 布鬼图，星 月也在</text></msg>", keep=["星 月"]) == (
        "并发布鬼图，星 月也在"
    )


def test_trim_nested_clips_but_keeps_conversation():
    long_desc = "这是一段很长的图片描述" * 20
    out = r.trim_nested(f"[Sticker {long_desc}]")
    assert out.startswith("[Sticker 这是一段很长的图片描述")
    assert out.endswith("…]") and len(out) < 130
    out = r.trim_nested("[Reply ID: 42 content: " + "被引用的原文很长" * 20 + "]")
    assert out.startswith("[Reply 42: ") and out.endswith("…]") and len(out) < 60
    # 正常短文本不受影响
    assert r.trim_nested("普通摘要，没有嵌套") == "普通摘要，没有嵌套"


def test_relevance_ignores_inserted_spaces():
    """检索词里被插了空格也要能命中（日志里出现过 keyword='翅 膀'）。"""
    assert r.relevance("翅 膀", "翅膀被他处刑好几次") > 0
    assert r.relevance("翅膀", "翅 膀被他处刑好几次") > 0
    assert r.relevance("黑 天鹅", "黑天鹅解释天使被关的原因") > 0


def _main_source():
    # 直接读源码，避免在没装核心时要 import main
    return (ROOT / "main.py").read_text(encoding="utf-8")


def test_tool_items_use_compact_keys_and_who_table():
    source = _main_source()
    # 工具返回改用短键 + who 映射（完整账号整批只出现一次）
    assert '"i": shorts.get(r["id"], r["id"])' in source
    assert 'result["who"] = who' in source
    # 旧的长字段不该再出现在工具返回里
    for legacy in ('"permanent": r["permanent"]', '"revision": r["revision"]'):
        assert legacy not in source
    # 短码要能被回传：检索排除项、读取、遗忘、修正都要先还原
    assert source.count('self.store.call("real_id", ') >= 4


def test_keyword_is_normalized_before_search():
    source = _main_source()
    assert "keyword, prompt = squeeze(keyword), squeeze(prompt)" in source
