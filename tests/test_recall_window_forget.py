"""压缩感知的「已给过」解除（★ 2026-09-19，用户提问引出）

背景：
    `RecallWindow` 记录"这个会话已经给过哪些记忆" ✓（30 分钟窗口 ✓）
    但 seen 只记得"我给过" ✗ **不知道上下文里那些结果还在不在** ✗
    一旦发生压缩（我们自己的分层压缩 ✓）⇒ 那批结果很可能已被压掉/出窗 ✗
    而 seen 仍在压制 ⇒ 模型最长 30 分钟拿不回来（只能靠 allow_seen 自救 ✓）

修法：
    压缩完成 ⇒ `forget_sid(sid)` **只解除压制**（允许重发）✓ **不动任何数据** ✓
    ⇒ 最坏结果只是多花几个 token ✓ 绝无数据风险 ✓
"""

import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_pkg = types.ModuleType("z_recall_window")
_pkg.__path__ = [str(ROOT)]
sys.modules["z_recall_window"] = _pkg
r = __import__("importlib").import_module("z_recall_window.retrieval")


def _key(sid, users=("u1",), scope="session"):
    return (sid, users, scope)


def test_forget_sid_only_clears_that_session():
    w = r.RecallWindow()
    w.remember(_key("s1"), "q", ["a", "b"])
    w.remember(_key("s2"), "q", ["c"])
    assert w.get(_key("s1"))["ids"] == ["a", "b"]
    n = w.forget_sid("s1")
    assert n == 1, "应当只清掉 s1 的 1 条记录，实际 %d" % n
    assert w.get(_key("s1"))["ids"] == [], "s1 应被放行（可重发 ✓）"
    assert w.get(_key("s2"))["ids"] == ["c"], "别的会话**绝不能受影响** ✗"


def test_forget_sid_is_safe_on_empty_and_weird_inputs():
    w = r.RecallWindow()
    assert w.forget_sid("") == 0
    assert w.forget_sid(None) == 0
    w.remember("not-a-tuple", "q", ["x"])     # 非三元组 key 也不能炸 ✓
    assert w.forget_sid("s1") == 0


def test_after_forget_the_same_memory_can_be_returned_again():
    """核心意图：压缩后**允许重发** ✓（而不是继续压制 ✗）"""
    w = r.RecallWindow()
    k = _key("s1")
    w.remember(k, "doro", ["r1"])
    assert w.get(k)["ids"] == ["r1"]          # 第一次给过 ✓
    w.forget_sid("s1")                        # 压缩发生 ✓
    assert w.get(k)["ids"] == [], "解除后应当能再给一次 ✓"


def test_engine_callback_is_optional_and_never_breaks_compress():
    """引擎侧必须是**可选回调 + 全包异常** ✓（绝不能让压缩因它失败 ✗）"""
    src = (ROOT / "engine.py").read_text(encoding="utf-8")
    assert 'getattr(self, "on_compressed", None)' in src, "必须是可选回调 ✓"
    assert 'logger.exception("[记忆·Z] on_compressed 回调失败' in src, "必须包异常 ✓"
    main = (ROOT / "main.py").read_text(encoding="utf-8")
    assert "self.engine.on_compressed = self._on_compressed" in main, "插件必须挂上 ✓"
    assert "def _on_compressed(self, sid):" in main
