"""中文检索质量：词级打分（★ 2026-09-19，来自用户真机日志）

现象（用户）：
    问「doro 的 bot 是谁」⇒ 主动搜 `doro` **命中 761 条** ⇒ 结果全是"doro 的插件"
    ⇒ 真正那条（`doro 的 bot 叫 X`）**进不了前 5** ⇒ 最后靠**被动轮换槽**侥幸救场 ✗

根因：
    打分口径是"按字切分 + 子串命中"⇒ 查询 `doro 的 bot 是谁` 会切出 `的`/`是谁` ✗
    而中文里「的/是谁/是」**到处都是** ⇒ 无关行白拿 3 分 ⇒ 与答案的差距只剩 1 分 ✗
    ⇒ 几百条命中里大量并列 ⇒ 排序退化为"数据库顺序" ⇒ 答案被埋 ✓

修法（本文件守着）：
    打分改用 `score_tokens`（有 jieba 按**词**，无 jieba 回退按字 ✓）
    ⚠️ 只改打分 ✗ SQL 预筛与 FTS 索引**不动** ⇒ 无需重建索引 ✓
"""

import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_pkg = types.ModuleType("z_search_quality")
_pkg.__path__ = [str(ROOT)]
sys.modules["z_search_quality"] = _pkg
r = __import__("importlib").import_module("z_search_quality.retrieval")

ANSWER = "周武：doro 的 bot 叫 艾芙萝丝，是她自己写的那个"
NOISE = [
    "这个插件是谁写的？doro 说是她自己搞的",
    "这个是 doro 的插件吗，作者是谁",
    "doro 的东西是谁做的我忘了",
    "doro 说的是谁，我记不清了",
    "谁的 doro 插件，是不是她的",
    "这是 doro 的，那是谁的",
]


def _rank(tokens):
    rows = []
    for t in NOISE + [ANSWER]:
        low = r.squeeze(t).casefold()
        rows.append((sum(len(x) * (x in low) for x in tokens), t))
    rows.sort(key=lambda kv: -kv[0])
    return rows


def test_virtual_words_no_longer_score():
    """虚词不该进打分 ✓（`的`/`是谁` 在中文里到处都有 ✗）"""
    toks = r.score_tokens("doro 的 bot 是谁")
    assert "doro" in toks and "bot" in toks
    for junk in ("的", "是谁", "是"):
        assert junk not in toks, "虚词不该计分：%r in %r" % (junk, toks)


def test_answer_outranks_noise_with_a_clear_margin():
    """★ 判据：答案必须**明显**领先噪声 ✓ 而不是只领先 1 分（1 分=几十条并列 ✗）

    改前（按字）：答案 8 / 噪声 7 ⇒ **只差 1** ✗ —— 这就是用户那条为什么进不了前 5 ✓
    改后（词级）：答案 7 / 噪声 4 ⇒ **差 3** ✓ ⇒ 761 条里也压得住 ✓
    """
    rows = _rank(r.score_tokens("doro 的 bot 是谁"))
    assert rows[0][1] == ANSWER, "答案必须排第一 ✓ 实际：%r" % (rows[0][1],)
    top, second = rows[0][0], rows[1][0]
    assert top - second >= 3, "与噪声的差距必须 ≥3 分（改前只有 1 分 ✗）：%d vs %d" % (top, second)


def test_old_tokenizer_would_have_tied_more():
    """反向对照 ✓：**旧口径**下答案只领先 1 分 ⇒ 说明这个判据确实在守东西 ✓"""
    rows = _rank(r.query_tokens("doro 的 bot 是谁"))
    assert rows[0][1] == ANSWER
    assert rows[0][0] - rows[1][0] <= 1, "旧口径应当只领先 ≤1 分（复现用户的坑 ✓）"


def test_fallback_keeps_working_without_jieba(monkeypatch=None):
    """没有 jieba 时**绝不能崩** ✓ 且回退到今天的行为 ✓"""
    saved = r._JIEBA_AVAILABLE
    try:
        r._JIEBA_AVAILABLE = False
        assert r.score_tokens("doro 的 bot 是谁") == r.query_tokens("doro 的 bot 是谁")
        assert r.score_tokens("") == r.query_tokens("")
    finally:
        r._JIEBA_AVAILABLE = saved
