"""Bootstrap guard: never seed a session from a merged/compressed history."""

import importlib
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_SAVED = {}


def _fake(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    _SAVED.setdefault(name, sys.modules.get(name))
    sys.modules[name] = module
    return module


class _Decorators:
    def __getattr__(self, _name):
        def decorator(*_args, **_kwargs):
            def wrap(func):
                return func

            return wrap

        return decorator


class _BasePlugin:
    def __init__(self, ctx, cfg):
        self.ctx = ctx
        self.plugin_cfg = cfg


class _Priority:
    LOW = -50


class _PageMenu:
    def __init__(self, **_kwargs):
        pass


class _PluginPage:
    @staticmethod
    def from_folder(*_args, **_kwargs):
        return None


_fake("fastapi", HTTPException=type("HTTPException", (Exception,), {}), Request=object)
_fake(
    "openai",
    APITimeoutError=type("APITimeoutError", (Exception,), {}),
    APIConnectionError=type("APIConnectionError", (Exception,), {}),
)
_fake(
    "core.plugin",
    BasePlugin=_BasePlugin,
    PageMenu=_PageMenu,
    PluginPage=_PluginPage,
    Priority=_Priority,
    on=_Decorators(),
    register=_Decorators(),
)
_fake("core.provider", LLMRequest=type("LLMRequest", (), {}))
_fake("core.agent.message", OpenAIMessage=type("OpenAIMessage", (), {}))
_fake("core.prompt_manager", Prompt=type("Prompt", (), {}))
_fake("core.chat", MessageChain=list)
_fake("core.chat.message_elements", Text=type("Text", (), {}))
_fake(
    "core.utils.path_utils",
    get_config_path=lambda: Path("/tmp"),
    get_data_path=lambda: Path("/tmp"),
)
_fake("core.logging_manager", get_logger=lambda *a, **k: types.SimpleNamespace(
    info=lambda *a, **k: None,
    warning=lambda *a, **k: None,
    error=lambda *a, **k: None,
))
for _pkg in ("core", "core.agent", "core.utils", "core.chat"):
    _SAVED.setdefault(_pkg, sys.modules.get(_pkg))
    sys.modules.setdefault(_pkg, types.ModuleType(_pkg))

package = types.ModuleType("alife_bootstrap_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_bootstrap_test", package)
module = importlib.import_module("alife_bootstrap_test.main")

for _name, _previous in _SAVED.items():
    if _previous is None:
        sys.modules.pop(_name, None)
    else:
        sys.modules[_name] = _previous


class Manager:
    def __init__(self, states=None, instances=None):
        self.states = states or {}
        self.plugin_instances = instances or {}
        self.plugin_configs = {}

    def has_plugin(self, pid):
        return pid in self.states or pid in self.plugin_instances

    def is_plugin_enabled(self, pid):
        return self.states.get(pid, False)


def build(states=None, instances=None, seed="auto"):
    ctx = types.SimpleNamespace(plugin_mgr=Manager(states, instances))
    return module.AlifeMemoryPlugin(ctx, {"alife": {"bootstrap_seed": seed}})


def test_auto_seeds_when_nothing_rewrites_history():
    assert build().bootstrap_allowed() is True
    assert build({"kira_session_merger": False}).bootstrap_allowed() is True


def test_auto_skips_when_merge_or_compression_plugin_enabled():
    assert build({"kira_session_merger": True}).bootstrap_allowed() is False
    assert build({"auto_delete_session": True}).bootstrap_allowed() is False
    assert (
        build({"KiraAI-ContextCondensation": True}).bootstrap_allowed() is False
    )
    # 目录名可能带后缀，按模糊匹配识别
    assert (
        build(instances={"KiraAI-ContextCondensation-main": None}).bootstrap_allowed()
        is True
    )
    assert (
        build(
            {"KiraAI-ContextCondensation-main": True},
            {"KiraAI-ContextCondensation-main": None},
        ).bootstrap_allowed()
        is False
    )


def test_always_and_off_override_detection():
    assert build({"kira_session_merger": True}, seed="always").bootstrap_allowed()
    assert build(seed="off").bootstrap_allowed() is False
    assert build({"kira_session_merger": True}, seed="off").bootstrap_allowed() is False


def test_merge_plugin_active_reports_the_blocker():
    plugin = build({"auto_delete_session": True})
    assert plugin.merge_plugin_active() == "auto_delete_session"
    assert build().merge_plugin_active() == ""


def _store(tmp_path):
    storage = importlib.import_module("alife_bootstrap_test.storage")
    store = storage.Store(tmp_path / "bootstrap.sqlite3")
    store.initialize()
    return store


def test_clean_marker_excludes_records_from_review(tmp_path):
    store = _store(tmp_path)
    seed = [{"role": "user", "content": "旧对话", "time": 1.0, "users": ["u"]}]
    store.capture("s1", "bootstrap", seed)
    assert store.bootstrap_review() == ["s1"]
    store.mark_bootstrap("s1", clean=True)
    assert store.bootstrap_review() == []


def test_legacy_records_need_review_until_purged(tmp_path):
    store = _store(tmp_path)
    seed = [{"role": "user", "content": "旧对话", "time": 1.0, "users": ["u"]}]
    store.capture("s2", "bootstrap", seed)
    store.mark_bootstrap("s2")  # 老版本：没有 clean 标记
    assert store.bootstrap_review() == ["s2"]
    assert store.bootstrap_reviewed() is False
    store.mark_bootstrap_reviewed()
    assert store.bootstrap_reviewed() is True
    assert store.purge_bootstrap()["removed"] == 1
    assert store.bootstrap_review() == []
