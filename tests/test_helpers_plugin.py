"""测试用的小工具：搭一个真插件实例（需要 KIRA_CORE）。"""

import importlib
import os
import sys
import types
from pathlib import Path

CORE = os.environ.get("KIRA_CORE")
if CORE:
    sys.path.insert(0, str(Path(CORE).resolve()))

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


async def build_plugin(tmp_path):
    """真插件 + 独立数据目录；调用方负责 terminate()。"""
    package = types.ModuleType("alife_tidy_helper")
    package.__path__ = [str(ROOT)]
    sys.modules.setdefault("alife_tidy_helper", package)
    module = importlib.import_module("alife_tidy_helper.main")
    module.get_data_path = lambda: tmp_path
    ctx = types.SimpleNamespace(
        get_plugin_data_dir=lambda: tmp_path,
        plugin_mgr=types.SimpleNamespace(plugin_configs={}),
    )
    plugin = module.AlifeMemoryPlugin(
        ctx, {"alife": {"probability": 0.0, "audit_enabled": False}}
    )
    await plugin.initialize()
    return plugin, plugin.store
