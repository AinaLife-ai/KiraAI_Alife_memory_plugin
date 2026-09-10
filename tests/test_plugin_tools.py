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
    # v2.7.0 起对外只给短码（真实 id 是 31~71 字符，模型容易抄错）
    memorized = json.loads(text)
    assert memorized["ok"] and len(memorized["id"]) <= 8
    real = store.real_id(memorized["id"])
    assert store.get(real)["permanent"] == 1
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

    await plugin.forget(event, first["id"])  # 用短码回传也要认得
    assert store.get(store.real_id(first["id"]))["active"] == 0
    revived = json.loads(await plugin.memorize(event, "主人喜欢乌龙茶"))
    assert revived["existing"] is True and revived["id"] == first["id"]
    assert store.get(store.real_id(first["id"]))["active"] == 1


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


class _Sender:
    def __init__(self, uid, nickname):
        self.user_id = uid
        self.nickname = nickname


class _Message:
    def __init__(self, uid, nickname, notice=False, timestamp=100.0):
        self.sender = _Sender(uid, nickname)
        self.is_notice = notice
        self.timestamp = timestamp


def _notice_event():
    return types.SimpleNamespace(
        sid="qq:dm:1835996851",
        event_id="reminder",
        timestamp=200.0,
        messages=[
            _Message("1835996851", "提醒任务所有者", notice=True, timestamp=200.0)
        ],
        session=types.SimpleNamespace(
            adapter_name="qq", session_title="提醒任务所有者"
        ),
    )


@pytest.mark.asyncio
async def test_notice_messages_do_not_overwrite_nicknames(tmp_path):
    plugin, store = _plugin(tmp_path)
    plugin.engine = _Engine()
    real = types.SimpleNamespace(
        sid="qq:dm:1835996851",
        event_id="real",
        timestamp=100.0,
        messages=[_Message("1835996851", "萤火", timestamp=100.0)],
        session=types.SimpleNamespace(adapter_name="qq", session_title="萤火"),
    )
    await plugin.observe_event_names(real)
    assert store.entities(ids=["qq:1835996851"])[0]["name"] == "萤火"
    assert store.entities(ids=["qq:dm:1835996851"])[0]["name"] == "萤火"

    await plugin.observe_event_names(_notice_event())
    assert store.entities(ids=["qq:1835996851"])[0]["name"] == "萤火"
    assert store.entities(ids=["qq:dm:1835996851"])[0]["name"] == "萤火"


@pytest.mark.asyncio
async def test_memory_names_returns_lean_entities(tmp_path):
    plugin, store = _plugin(tmp_path)
    plugin.engine = _Engine()
    store.observe_name("qq:1", "小明", kind="user", observed=1.0)
    store.observe_name("qq:1", "明哥", kind="user", observed=2.0)
    out = json.loads(await plugin.memory_names(_event(), query="小明"))
    entity = out["entities"][0]
    assert set(entity) <= {"id", "kind", "name", "revision", "aliases", "lookup_id"}
    assert entity["name"] == "明哥" and "小明" in entity["aliases"]
    assert "identity_note" not in entity and "label" not in entity
