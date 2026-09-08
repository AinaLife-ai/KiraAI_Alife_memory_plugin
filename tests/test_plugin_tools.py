"""Our own tool outputs must never become new memories."""

import json
import types

import pytest
from test_name_refresh import _plugin


class _Engine:
    async def enqueue(self, *args, **kwargs):
        return None


class _Result:
    def __init__(self, text):
        self.text = text

    async def assemble_result(self):
        return self.text


def _event():
    return types.SimpleNamespace(
        sid="qq:gm:188395693",
        event_id="evt",
        messages=[],
        session=types.SimpleNamespace(adapter_name="qq"),
    )


@pytest.mark.asyncio
async def test_own_tool_output_is_not_stored(tmp_path):
    plugin, store = _plugin(tmp_path)
    plugin.engine = _Engine()
    event = _event()
    text = await plugin.memorize(event, "主人喜欢乌龙茶")
    assert "100-" in text  # the permanent memory itself was created
    before = len(store.export()["records"])
    await plugin.on_tool_result(event, _Result(text))
    assert len(store.export()["records"]) == before

    # A genuinely external tool result is still captured.
    await plugin.on_tool_result(event, _Result('{"weather":"晴"}'))
    rows = store.export()["records"]
    assert len(rows) == before + 1
    assert rows[-1]["summary"].startswith("工具感知结果：")


@pytest.mark.asyncio
async def test_memorize_skips_duplicates_and_revives_forgotten(tmp_path):
    import json

    plugin, store = _plugin(tmp_path)
    plugin.engine = _Engine()
    event = _event()
    first = json.loads(await plugin.memorize(event, "主人喜欢乌龙茶"))
    assert first.get("existing") is not True
    before = len(store.export()["records"])

    again = json.loads(await plugin.memorize(event, "主人喜欢乌龙茶。"))
    assert again == {"ok": True, "id": first["id"], "existing": True}
    assert len(store.export()["records"]) == before

    await plugin.forget(event, first["id"])
    assert store.get(first["id"])["active"] == 0
    revived = json.loads(await plugin.memorize(event, "主人喜欢乌龙茶"))
    assert revived["existing"] is True
    assert store.get(first["id"])["active"] == 1


@pytest.mark.asyncio
async def test_search_returns_only_new_memories(tmp_path):
    plugin, store = _plugin(tmp_path)
    plugin.engine = _Engine()
    event = _event()
    for i in range(3):
        store.capture(
            event.sid,
            f"e{i}",
            [
                {
                    "role": "user",
                    "content": f"关于喵梓的记忆{i}",
                    "time": float(i),
                    "users": [],
                }
            ],
        )
    first = json.loads(await plugin.search_archive(event, keyword="喵梓"))
    assert len(first["items"]) == 3 and first["already_seen"] == 0

    second = json.loads(await plugin.search_archive(event, keyword="喵梓"))
    assert second["items"] == [] and second["already_seen"] == 3
    assert "ReadMemoryArchive" in second["hint"]

    again = json.loads(
        await plugin.search_archive(event, keyword="喵梓", allow_seen=True)
    )
    assert len(again["items"]) == 3
