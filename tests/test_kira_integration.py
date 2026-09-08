"""Run with KIRA_CORE pointing at a real checkout; no fake core modules."""

import importlib
import os
import sys
import types
from pathlib import Path
import pytest

CORE = os.environ.get("KIRA_CORE")
if not CORE:
    pytest.skip("set KIRA_CORE for actual host integration", allow_module_level=True)
sys.path.insert(0, str(Path(CORE).resolve()))
ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_host_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_host_test", package)
module = importlib.import_module("alife_host_test.main")
from core.provider import LLMRequest, LLMResponse
from core.agent.message import OpenAIMessage
from core.prompt_manager import Prompt
from core.chat import MessageChain
from core.chat.message_elements import Text
from core.chat.message_utils import KiraIMMessage, KiraMessageBatchEvent
from core.chat.session import User, Session


def make_event():
    session = Session(adapter_name="test", session_type="dm", session_id="u")
    msg = KiraIMMessage(
        message_id="one",
        self_id="bot",
        chain=MessageChain([Text("我喜欢猫")]),
        timestamp=100,
        sender=User(user_id="u", nickname="小明"),
    )
    msg.message_str = "[小明] 我喜欢猫"
    return KiraMessageBatchEvent(
        messages=[msg], session=session, timestamp=100, message_types=[]
    )


def test_host_schema_matches_every_validated_setting():
    import json
    from core.config.config_field import create_field_from_schema

    schema = json.loads((ROOT / "schema.json").read_text(encoding="utf-8"))
    fields = schema["alife"]["fields"]
    assert set(fields) == set(module.Settings.model_fields)
    for key, spec in fields.items():
        field = create_field_from_schema(key, spec)
        assert field.default == module.Settings().model_dump()[key]


@pytest.mark.asyncio
async def test_real_core_capture_inject_edit_reload(tmp_path, monkeypatch):
    monkeypatch.setattr(module, "get_config_path", lambda: tmp_path / "config")
    ctx = types.SimpleNamespace(
        get_plugin_data_dir=lambda: tmp_path,
        plugin_mgr=types.SimpleNamespace(plugin_configs={}),
    )
    plugin = module.AlifeMemoryPlugin(
        ctx, {"alife": {"probability": 0.0, "audit_enabled": False}}
    )
    await plugin.initialize()
    try:
        event = make_event()
        await plugin.on_response(
            event, LLMResponse(text_response="我记住啦", agent_step_index=0)
        )
        await plugin.on_response(
            event, LLMResponse(text_response="我记住啦", agent_step_index=0)
        )
        assert plugin.store.status()["records"] == 2
        req = LLMRequest(
            messages=[OpenAIMessage(role="assistant", content="core history")]
        )
        req.user_prompt = [Prompt("新一轮", name="message")]
        await plugin.on_request(event, req)
        assert any("我喜欢猫" in str(m.content) for m in req.messages)
        injected = [p for p in req.user_prompt if p.name == "alife_memory"]
        assert len(injected) == 1 and not injected[0].persist
        req.assemble_prompt()
        assert req.messages[-1].role == "user"
        result = await plugin.memorize(event, "一起看流星的约定")
        import json

        record_id = json.loads(result)["id"]
        assert json.loads(await plugin.forget(event, record_id))["ok"]
        assert json.loads(await plugin.read_archive(event, record_id))["ok"]
        other = make_event()
        other.session.session_id = "other"
        assert not json.loads(await plugin.read_archive(other, record_id))["ok"]
    finally:
        await plugin.terminate()
    plugin2 = module.AlifeMemoryPlugin(
        ctx, {"alife": {"probability": 0.0, "audit_enabled": False}}
    )
    await plugin2.initialize()
    try:
        assert plugin2.store.status()["records"] == 3
    finally:
        await plugin2.terminate()


@pytest.mark.asyncio
async def test_api_validation_conflict_and_atomic_config(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from httpx import AsyncClient, ASGITransport

    monkeypatch.setattr(module, "get_config_path", lambda: tmp_path / "config")
    ctx = types.SimpleNamespace(
        get_plugin_data_dir=lambda: tmp_path,
        plugin_mgr=types.SimpleNamespace(plugin_configs={}),
    )
    plugin = module.AlifeMemoryPlugin(
        ctx, {"alife": {"probability": 0.0, "audit_enabled": False}}
    )
    await plugin.initialize()
    app = FastAPI()
    app.post("/config")(plugin.api_save_config)
    app.post("/memory")(plugin.api_new)
    app.post("/edit")(plugin.api_edit)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            config = await plugin.api_config()
            config["settings"]["compress_model"] = "provider:compress"
            response = await client.post(
                "/config",
                json={"revision": config["revision"], "settings": config["settings"]},
            )
            assert response.status_code == 200
            assert plugin.settings.compress_model == "provider:compress"
            assert (tmp_path / "config/plugins/alife_memory_z.json").exists()
            response = await client.post(
                "/config",
                json={"revision": config["revision"], "settings": config["settings"]},
            )
            assert response.status_code == 409
            response = await client.post(
                "/memory", json={"sid": "test:dm:u", "content": "人工记忆"}
            )
            record_id = response.json()["id"]
            edit = {
                "kind": "record",
                "target": record_id,
                "revision": 1,
                "patch": {"summary": "修改后的记忆"},
                "reason": "test",
            }
            assert (await client.post("/edit", json=edit)).status_code == 200
            assert (await client.post("/edit", json=edit)).status_code == 409
            edit["revision"] = 2
            edit["patch"] = {"summary": 22}
            assert (await client.post("/edit", json=edit)).status_code == 422
    finally:
        await plugin.terminate()
