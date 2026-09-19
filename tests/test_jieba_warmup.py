"""分词预热（★ 2026-09-19，用户实测）

现象：日志里 `Loading model cost 1.340 seconds` ✓ 但它发生在**对话进行中** ✗
      ⇒ 那 1.3 秒砸在一次真实回复的链路上 ✓
修法：在插件 **initialize()**（加载阶段）用**后台线程**预热 ✓
      · 与 KiraOS 的 initialize() 约定一致 ✓
      · 后台线程 ⇒ **不阻塞启动** ✓
      · 失败绝不影响加载 ✓（预热只是优化 ✓）
"""

import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_pkg = types.ModuleType("z_warmup")
_pkg.__path__ = [str(ROOT)]
sys.modules["z_warmup"] = _pkg
r = __import__("importlib").import_module("z_warmup.retrieval")
MAIN = (ROOT / "main.py").read_text(encoding="utf-8")


def test_warm_function_exists_and_is_safe():
    assert callable(r.warm_jieba), "必须有 warm_jieba ✓"
    assert isinstance(r.warm_jieba(), bool), "必须返回 bool，且**不许抛异常** ✓"


def test_initialize_warms_in_background_thread():
    """★ 必须在 initialize() 里预热 ✓ 且必须用**后台线程**（不阻塞启动 ✓）"""
    i = MAIN.index("async def initialize(self):")
    body = MAIN[i : i + 2000]
    assert "warm_jieba" in body, "initialize() 里必须预热 ✓"
    assert "threading.Thread" in body, "必须用后台线程 ⇒ 不阻塞启动 ✓"
    assert "daemon=True" in body, "守护线程 ⇒ 插件退出不挂住进程 ✓"


def test_prewarm_failure_never_blocks_loading():
    body = MAIN[MAIN.index("async def initialize(self):") :][:2000]
    block = body[body.index("预热") : body.index("预热") + 700] if "预热" in body else body
    assert "except Exception" in block, "预热整段必须包异常 ✓（失败绝不影响加载 ✓）"
    assert "不影响功能" in block, "注释里要写清：失败不影响功能 ✓"


def test_jieba_log_is_silenced():
    """jieba 建词典会打 4 行 DEBUG ✗ 对用户是噪声 ⇒ 必须静音 ✓"""
    src = (ROOT / "retrieval.py").read_text(encoding="utf-8")
    assert "setLogLevel" in src, "必须静音 jieba 的 DEBUG 输出 ✓"


def test_fallback_still_works_without_jieba():
    """没装 jieba ⇒ warm_jieba 返回 False ✓ 绝不抛 ✓"""
    saved = r._JIEBA_AVAILABLE
    try:
        r._JIEBA_AVAILABLE = False
        assert r.warm_jieba() is False
    finally:
        r._JIEBA_AVAILABLE = saved
