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
    # 2026-09-18（用户）：媒体描述预算 100 → **30** ✓；Reply 去掉 msgid 且压平嵌套 ✓
    out = r.trim_nested(f"[Sticker {long_desc}]")
    assert out.startswith("[Sticker 这是一段很长的图片描述")
    assert out.endswith("…]") and len(out) < 60
    out = r.trim_nested("[Reply ID: 42 content: " + "被引用的原文很长" * 20 + "]")
    assert out.startswith("[Reply: ") and out.endswith("]") and len(out) < 60
    assert "42" not in out, "无意义的 msgid 不许出现 ✗（模型没有按它查的能力 ✓）"
    assert "需要保留 msgid" not in out
    # 引用里是**媒体** ⇒ 按更小预算裁（30 ✓）
    out = r.trim_nested("[Reply ID: 7 content: [Image " + "猫在窗台上" * 30 + "]]")
    assert out.startswith("[Reply: ") and len(out) < 60, out
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


def test_negative_reply_id_is_normalized_too():
    """★ 2026-09-19 用户真机日志：archive 槽注入 `[Reply ID: -19 content: …]` 原样穿透 ✗
    原因：旧正则只认 `\d+` ▶ 负数 id 匹配不上 ⇒ 永远走不到新格式 ✓"""
    out = r.trim_nested(r.clean_text("[Reply ID: -19 content: [你好呀]]", ()))
    assert out == "[Reply: 你好呀]", out
    assert "Reply ID" not in out and "-19" not in out, "负 id 也必须被吃掉：%r" % out
    # 引用里是媒体（真机同样出现过 -43）
    out = r.trim_nested(r.clean_text("[Reply ID: -43 content: [Image 一只猫在窗台]] 看这个", ()))
    assert out.startswith("[Reply: Image ") and "看这个" in out, out


def test_at_ids_never_reach_the_model():
    """★ 用户日志：`[At 3991867505]` 这种数字 id 原样进注入 ✗（模型拿它什么都做不了）"""
    assert r.strip_at_ids("[At 3991867505(nickname: 爱奈丽)] 说话").strip() == "@爱奈丽 说话"
    assert r.strip_at_ids("[At 3991867505] 说话").strip() == "说话"
    assert r.strip_at_ids("[At -123] 嗯").strip() == "嗯"      # 负 id 同样处理 ✓
    assert r.strip_at_ids("普通文本，没有 At ✓") == "普通文本，没有 At ✓"
    assert r.strip_at_ids("") == ""


def test_at_only_messages_never_reach_the_model_without_names():
    """★ 2026-09-19（用户）：「像其他链路那样，不召回只有 at 的」
    要点：**结构化 `[At …]` 壳不依赖名单表** ✓（与 [Reply]/[CQ:at]/<at> 同档 ✓）
          ⇒ 空名单也必须判得出来 ✓（否则用户改名后就会漏 ✓）
    """
    for only in (
        "[At 3991867505]",
        "[At 3991867505(nickname: 爱奈丽)]",
        "[At -19]",
        "[At 123] [At 456]",
    ):
        assert r.media_only(only, ()) is True, "只有 at 的消息必须不召回：%r" % only
    # 有真内容的一律保留 ✓（绝不能被 at 壳连累 ✗）
    for keep in (
        "[At 123] 你好呀",
        "[At 3991867505(nickname: 爱奈丽)] 明天见",
        "@他就好了",
    ):
        assert r.media_only(keep, ()) is False, "有内容的不能吃掉：%r" % keep
    # 裸文本 `@` 才需要名单 ✓ 且**改名后是 fail-safe**：宁可多留，绝不吃真话 ✓
    assert r.media_only("@爱奈丽", ()) is False
    assert r.media_only("@爱奈丽", ("爱奈丽",)) is True


def test_quoted_reply_drops_leading_timestamp():
    """★ 2026-09-19（用户真机日志）：引用内容开头是时间戳 ✗
    `[Reply: 2026-09-19 08:09:37] 爱奈丽：…` ⇒ 21 个字符白占 40 字预算的一半 ✓
    时间戳对模型毫无用处（created/event_at 另有字段）⇒ 剥掉 ✓"""
    raw = ("[Reply ID: -1026278658 content: [2026-09-19 08:09:37] 爱奈丽：行吧，"
           "那我还歪打正着预言了一波]所以预言家跳了对吧？")
    out = r.trim_nested(r.clean_text(raw, ()))
    assert out.startswith("[Reply: 爱奈丽："), out
    assert "2026-09-19" not in out, "时间戳不许进注入：%r" % out
    # 无外层方括号的形态也要认 ✓（.strip("[]") 会先把 [ 剥掉）
    assert r._TS_LEAD.sub("", "2026-09-19 08:09:37 你好呀").strip() == "你好呀"
    # 别误伤"像日期的正文"✓
    assert r._TS_LEAD.sub("", "2026年8月28日深夜小怪兽") == "2026年8月28日深夜小怪兽"
