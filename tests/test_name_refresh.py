"""Name refresh must never overwrite a name the user already filled."""

import asyncio
import importlib
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


_SAVED_MODULES = {}


def _install_fake_core():
    def module(name, **attrs):
        mod = types.ModuleType(name)
        for key, value in attrs.items():
            setattr(mod, key, value)
        _SAVED_MODULES.setdefault(name, sys.modules.get(name))
        sys.modules[name] = mod
        return mod

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
        LOW = 0

    class _PageMenu:
        def __init__(self, **_kwargs):
            pass

    class _Logger:
        def __getattr__(self, _name):
            return lambda *a, **k: None

    module("fastapi", HTTPException=type("HTTPException", (Exception,), {}), Request=object)
    module(
        "openai",
        APITimeoutError=type("APITimeoutError", (Exception,), {}),
        APIConnectionError=type("APIConnectionError", (Exception,), {}),
    )
    module(
        "core.plugin",
        BasePlugin=_BasePlugin,
        PageMenu=_PageMenu,
        PluginPage=type("PluginPage", (), {}),
        Priority=_Priority,
        on=_Decorators(),
        register=_Decorators(),
    )
    module("core.provider", LLMRequest=type("LLMRequest", (), {}))
    module("core.agent.message", OpenAIMessage=type("OpenAIMessage", (), {}))
    module("core.prompt_manager", Prompt=type("Prompt", (), {}))
    module("core.chat", MessageChain=type("MessageChain", (), {}))
    module("core.chat.message_elements", Text=type("Text", (), {}))
    module(
        "core.utils.path_utils",
        get_config_path=lambda: Path("/tmp"),
        get_data_path=lambda: Path("/tmp"),
    )
    module("core.logging_manager", get_logger=lambda *a, **k: _Logger())
    for name in ("core", "core.agent", "core.utils", "core.chat"):
        _SAVED_MODULES.setdefault(name, sys.modules.get(name))
        sys.modules.setdefault(name, types.ModuleType(name))


def _restore_modules():
    """Drop the fakes again so other test files still see the real host."""
    for name, previous in _SAVED_MODULES.items():
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous


_install_fake_core()
package = types.ModuleType("alife_name_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_name_test", package)
module = importlib.import_module("alife_name_test.main")
s = importlib.import_module("alife_name_test.storage")
_restore_modules()


class _Bot:
    def __init__(self):
        self.calls = []

    async def get_user_info(self, user_id):
        self.calls.append(user_id)
        return {"data": {"nickname": "平台昵称"}}


class _Adapter:
    def __init__(self):
        self.bot = _Bot()


class _Manager:
    def __init__(self, adapters):
        self._adapters = adapters

    def get_adapter(self, name):
        return self._adapters.get(name)

    def get_adapters(self):
        return self._adapters


def _plugin(tmp_path, adapters=("qq",)):
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    ctx = types.SimpleNamespace(
        adapter_mgr=_Manager({name: _Adapter() for name in adapters}),
        plugin_mgr=types.SimpleNamespace(plugin_configs={}),
    )
    plugin = module.AlifeMemoryPlugin(ctx, {"alife": {}})
    plugin.store = store
    plugin.name_refresh_lock = asyncio.Lock()
    return plugin, store


def _placeholder(store, entity_id, kind="user"):
    with store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute(
            "INSERT OR IGNORE INTO entities(id,kind,name,updated) VALUES (?,?, '',0)",
            (entity_id, kind),
        )


@pytest.mark.asyncio
async def test_batch_never_overwrites_filled_name(tmp_path):
    plugin, store = _plugin(tmp_path)
    store.observe_name("qq:123", "手工名", kind="user", observed=100.0)
    _placeholder(store, "unresolved:user:123")
    assert await store.call("refreshable_names", 50) == ["unresolved:user:123"]

    entity, wrote = await plugin.refresh_name_detail(
        "unresolved:user:123", reason="批量确认当前QQ昵称", skip_named=True
    )
    assert wrote is False
    assert entity["name"] == "手工名"
    assert store.entities(ids=["qq:123"])[0]["name"] == "手工名"
    # The placeholder is still bound and merged, just without touching the name.
    assert not store.entities(ids=["unresolved:user:123"])


@pytest.mark.asyncio
async def test_manual_refresh_still_updates(tmp_path):
    plugin, store = _plugin(tmp_path)
    store.observe_name("qq:123", "手工名", kind="user", observed=100.0)
    entity = await plugin.refresh_name("qq:123", reason="人工确认")
    assert entity["name"] == "平台昵称"
    history = store.entities(ids=["qq:123"])[0]["history"]
    assert history[0]["name"] == "平台昵称"


@pytest.mark.asyncio
async def test_batch_skips_named_entity_without_lookup(tmp_path):
    plugin, store = _plugin(tmp_path)
    store.observe_name("qq:123", "手工名", kind="user", observed=100.0)
    entity, wrote = await plugin.refresh_name_detail("qq:123", skip_named=True)
    assert wrote is False and entity["name"] == "手工名"
    assert plugin.ctx.adapter_mgr.get_adapter("qq").bot.calls == []


@pytest.mark.asyncio
async def test_batch_fills_empty_name(tmp_path):
    plugin, store = _plugin(tmp_path)
    _placeholder(store, "qq:123")
    entity, wrote = await plugin.refresh_name_detail(
        "qq:123", reason="批量确认当前QQ昵称", skip_named=True
    )
    assert wrote is True and entity["name"] == "平台昵称"
    assert entity["history"][0]["reason"] == "批量确认当前QQ昵称"
