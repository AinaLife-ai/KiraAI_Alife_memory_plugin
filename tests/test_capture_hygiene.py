"""落库净化：协议外壳、思考块、空消息，以及新增默认值的不变式。

线上背景：L0 里混进了 `<msg/>` 这类空外壳和 `<reasoning>` 思考块——
它们没有信息量，却会计入压缩阈值、进压缩输入，思考块还会被写进摘要与事实。
"""

import importlib
import json
import os
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_capture_hygiene")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_capture_hygiene", package)
retrieval = importlib.import_module("alife_capture_hygiene.retrieval")
storage = importlib.import_module("alife_capture_hygiene.storage")
contracts = importlib.import_module("alife_capture_hygiene.contracts")

EMPTY_SHELLS = [
    "<msg/>",
    "<msg />",
    "<msg  />",
    "<msg>",
    "<msg >",
    "</msg>",
    "</msg >",
    "<msg></msg>",
    "<msg> </msg>",
    '<msg message_id="556201337"/>',
    "<msg message_id='556201337' />",
    "<MSG/>",
    "<Msg></Msg>",
    "<text/>",
    "<text />",
    "<text></text>",
    "<Text></Text>",
    "<msg/>\n<msg/>",
    " <msg/> ",
    "\n<msg />\n</msg>\n",
    "<forward/>",
    "<quote></quote>",
    # 空文本块不在行首时也必须是空（模糊测试抓到的漏网：<msg /><text/> 曾留下 '<text/>'）
    "<msg /><text/>",
    "<msg /><text />",
    "<msg></msg><text></text>",
    "<text/><text />",
    "<msg a='1'/><text/>",
]


@pytest.mark.parametrize("raw", EMPTY_SHELLS)
def test_empty_shell_variants_clean_to_nothing(raw):
    """只写了外壳 = 没有说话，清洗后必须是空（这样才不会建记录）。"""
    assert retrieval.clean_text(raw).strip() == ""


def test_cleaning_never_eats_text_outside_tags():
    """内容安全的底线：只吃标签，标签以外的字符一个都不能少。"""
    cases = {
        "我教你 <text> 标签是这样用的": "我教你 <text> 标签是这样用的",
        "3 < 5 是对的": "3 < 5 是对的",
        "字段叫 <msg_id> 或者 <msg_type>": "字段叫 <msg_id> 或者 <msg_type>",
        "孤零零的 <msg 没有闭合": "孤零零的 <msg 没有闭合",
        "苹果<香蕉>梨": "苹果<香蕉>梨",
        # 标签本身被吃掉（不可避免，见上条注释），汉字一个不少；
        # 空白按既有归一口径处理（CJK 之间的空格会被合并）
        "正文里写到 <msg> 这个词": "正文里写到这个词",
    }
    for raw, expected in cases.items():
        assert retrieval.clean_text(raw) == expected, raw


def test_semantic_tags_become_short_markers():
    raw = '<msg message_id="1777654688">\n<reply>729771603</reply>\n<text>黑天鹅啊</text>\n<sticker>8</sticker>\n</msg>'
    assert retrieval.clean_text(raw) == "↩729771603\n黑天鹅啊\n[表情8]"


def test_unclosed_inline_tag_does_not_eat_later_messages():
    """未闭合的 <sticker> 不能一路吃到后面那条消息的 </sticker>（内容安全）。"""
    raw = "a <sticker>8</sticker> b\n</sticker> c"
    assert retrieval.clean_text(raw) == "a [表情8] b\nc"


REASONING_CASES = [
    ("<reasoning>想一下</reasoning><msg><text>你好</text></msg>", "你好"),
    ("<reasoning>\n多行\n思考\n</reasoning>\n<msg />", ""),
    ("<reasoning>没闭合\n<msg><text>正文</text></msg>", "正文"),
    ('<REASONING a="1">x</REASONING>正文', "正文"),
    ("<reasoning/>正文", "正文"),
    ("<reasoning>  </reasoning>", ""),
]


@pytest.mark.parametrize("raw,expected", REASONING_CASES)
def test_reasoning_is_stripped_with_its_content(raw, expected):
    assert retrieval.clean_text(retrieval.strip_reasoning(raw)).strip() == expected


def test_unclosed_reasoning_keeps_text_when_no_message_follows():
    """未闭合、后面也没有 <msg>：宁可留下思考，也不丢正文。"""
    raw = "<reasoning>没闭合也没有消息标签，正文在这里"
    out = retrieval.clean_text(retrieval.strip_reasoning(raw)).strip()
    assert out == "没闭合也没有消息标签，正文在这里"


def test_capture_text_helper_shapes():
    main = _main_module()
    assert main.capture_text("<msg/>") == ""
    assert main.capture_text("<msg/>", is_bot=True) == ""
    assert main.capture_text("普通消息") == "普通消息"
    assert main.capture_text("<msg><text>好的</text></msg>") == "好的"
    # 思考块只对 Bot 的输出剥——用户消息里的字面标签是"他真说过的话"
    assert main.capture_text("<reasoning>x</reasoning>你好", is_bot=True) == "你好"
    assert main.capture_text("<reasoning>x</reasoning>你好") == "<reasoning>x</reasoning>你好"


def _main_module():
    if not os.environ.get("KIRA_CORE"):
        pytest.skip("set KIRA_CORE for host integration")
    core = Path(os.environ["KIRA_CORE"]).resolve()
    if str(core) not in sys.path:
        sys.path.insert(0, str(core))
    return importlib.import_module("alife_capture_hygiene.main")


def test_compression_defaults_and_invariants():
    """默认值：50 条触发、每批 40、概率 0.8；并钉住两条不变式。"""
    settings = contracts.Settings()
    assert (settings.threshold, settings.batch_size) == (50, 40)
    assert settings.probability == pytest.approx(0.8)
    # 尾巴 = 阈值 − 每批条数，必须为正（否则整理完一条原文都不剩）
    assert settings.batch_size < settings.threshold
    assert 0.0 <= settings.probability <= 1.0
    # 配置页（schema）与代码里的默认值必须一致——两处都要改，别只改一处
    fields = json.loads((ROOT / "schema.json").read_text(encoding="utf-8"))["alife"]["fields"]
    assert fields["threshold"]["default"] == settings.threshold
    assert fields["batch_size"]["default"] == settings.batch_size
    assert fields["probability"]["default"] == pytest.approx(settings.probability)


def test_scrub_cleans_existing_rows_once(tmp_path):
    """存量清洗：老记录里的外壳与思考块要被清掉，索引体跟着重算，且可重复执行。"""
    store = _store(tmp_path)
    store.capture(
        "qq:gm:A",
        "ev0",
        [
            {
                "role": "user",
                "content": '<msg message_id="1">\n<text>草莓蛋糕</text>\n</msg>',
                "users": ["u:1"],
                "time": 1.0,
            },
            {
                "role": "assistant",
                "content": "<reasoning>想了想</reasoning><msg><text>好的</text></msg>",
                "users": ["u:1"],
                "time": 2.0,
            },
        ],
    )
    assert store.prepare_capture_scrub() is True
    while not store.scrub_capture_text():             # 与后台任务同样的循环
        pass
    assert store.scrub_stats()["changed"] == 2
    rows = _rows(store, "qq:gm:A")
    assert [row["content"] for row in rows] == ["草莓蛋糕", "好的"]
    assert [row["summary"] for row in rows] == ["草莓蛋糕", "好的"]
    # 索引体要跟着重算，否则搜到旧文字、搜不到新文字
    with store.connect() as db:
        bodies = dict(db.execute("SELECT summary, search_body FROM records").fetchall())
    assert all("msg" not in body for body in bodies.values())
    # 幂等 + 只跑一次
    store.finish_capture_scrub()
    assert store.prepare_capture_scrub() is False
    store._scrub_cursor = 0
    while not store.scrub_capture_text():
        pass
    assert store.scrub_stats()["changed"] == 2         # 没有新增改动


def test_scrub_keeps_archive_prose_intact(tmp_path):
    """L1+ 是模型写的散文摘要：只剥思考块，不碰其它尖括号内容。"""
    store = _store(tmp_path)
    with store.connect() as db:
        db.execute(
            "INSERT INTO records(id,sid,role,level,start,end,summary,content,users,"
            "position,created,search_body) VALUES ('a','qq:gm:A','assistant',1,0.0,0.0,?,?,"
            "'[]',0,0.0,?)",
            (
                "<reasoning>旧思考</reasoning>聊到了 <msg_id> 这个字段",
                "<reasoning>旧思考</reasoning>聊到了 <msg_id> 这个字段",
                "聊到了 <msg_id> 这个字段",
            ),
        )
    store.scrub_capture_text()
    with store.connect() as db:
        row = db.execute("SELECT summary FROM records WHERE id='a'").fetchone()
    assert row["summary"] == "聊到了 <msg_id> 这个字段"


def _store(tmp_path):
    store = storage.Store(Path(tmp_path) / "db")
    store.initialize()
    if store.search_index_state() == "unavailable":
        pytest.skip("本机 SQLite 无 FTS5")
    return store


def _rows(store, sid):
    with store.connect() as db:
        return db.execute(
            "SELECT role, summary, content FROM records WHERE sid=? ORDER BY position",
            (sid,),
        ).fetchall()


@pytest.mark.asyncio
async def test_capture_path_drops_empty_and_strips_reasoning(tmp_path):
    """端到端：`<msg/>` 不建记录；思考块剥掉；渲染为空的消息不占 L0 席位。"""
    if not os.environ.get("KIRA_CORE"):
        pytest.skip("set KIRA_CORE for host integration")
    sys.path.insert(0, str(Path(os.environ["KIRA_CORE"]).resolve()))
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_helpers_plugin import build_plugin

    from core.adapter.adapter_info import AdapterInfo
    from core.chat import MessageChain
    from core.chat.message_elements import Text
    from core.chat.message_utils import KiraIMMessage, KiraMessageBatchEvent
    from core.chat.session import Session, User
    from core.provider import LLMResponse

    plugin, store = await build_plugin(tmp_path)
    try:
        session = Session(adapter_name="test", session_type="dm", session_id="u")
        messages = [
            KiraIMMessage(
                message_id="m1",
                self_id="bot",
                chain=MessageChain([Text("草莓蛋糕")]),
                timestamp=100,
                sender=User(user_id="u", nickname="小明"),
            ),
            KiraIMMessage(
                message_id="m2",
                self_id="bot",
                chain=MessageChain([]),
                timestamp=101,
                sender=User(user_id="u", nickname="小明"),
            ),
        ]
        messages[0].message_str = '<msg message_id="1">\n<text>草莓蛋糕</text>\n</msg>'
        messages[1].message_str = "<msg/>"  # 渲染不出内容的消息
        event = KiraMessageBatchEvent(
            message_types=[],
            timestamp=100,
            messages=messages,
            adapter=AdapterInfo(enabled=True, adapter_id="test", name="test", platform="QQ"),
        )
        event.session = session

        # ① 只输出空外壳 = 这次没说话 → 不建记录（但事件本身照处理）
        await plugin.on_response(event, LLMResponse(text_response="<msg/>", agent_step_index=1))
        rows = await store.call("active", session.sid)
        assert [row["role"] for row in rows] == ["user"], rows
        # 用户侧：外壳剥掉、渲染为空的那条不落库
        assert [row["content"] for row in rows] == ["草莓蛋糕"]

        # ② 真的说了话：思考块 + 外壳都剥掉，落库的是人能看到的那句话
        await plugin.on_response(
            event,
            LLMResponse(
                text_response="<reasoning>\n主人叫收声\n</reasoning>\n<msg>\n<text>知道了，安静</text>\n</msg>",
                agent_step_index=2,
            ),
        )
        rows = await store.call("active", session.sid)
        bot = [row for row in rows if row["role"] == "assistant"]
        assert [row["content"] for row in bot] == ["知道了，安静"], rows
        assert all("reasoning" not in row["content"] for row in rows)
    finally:
        await plugin.terminate()


def test_one_line_protocol_is_unwrapped_too():
    """模型常把整条回复写在一行里：标记后面的 <text> 也要当外壳剥。"""
    raw = "<msg><reply>7</reply><text>好</text><sticker>8</sticker></msg>"
    assert retrieval.clean_text(raw) == "↩7好[表情8]"
    # 跨行写法同样正确（换行是消息边界，要保留）
    raw2 = '<msg message_id="1">\n<text>知道了，安静</text>\n<sticker>8</sticker>\n</msg>'
    assert retrieval.clean_text(raw2) == "知道了，安静\n[表情8]"


def test_two_text_blocks_never_merge_into_one_message():
    """同一个 <msg> 里连着两个 <text>（协议上不合法）：内容一个都不能丢。

    这种输入下第二块的标签可能剥不掉（它前面已经是正文了），但**两块的内容
    必须都在**——非贪婪匹配跨过后一个 </text> 就会把两条消息合成一条（实测踩过）。
    """
    out = retrieval.clean_text("<msg><text>A</text><text>B</text></msg>")
    assert "A" in out and "B" in out
    assert out.count("A") == 1 and out.count("B") == 1
    # 也不能把中间那半截标签弄丢成 `<text>B` 这种残骸
    assert out != "A</text><text>B"


def test_model_facing_text_drops_reasoning_but_stored_text_keeps_it():
    """给模型的出口比落库多剥一层思考块——用户可能真的引用过它。

    落库要保原文（他确实打过那些字），但发给模型的内容不该带内部推理；
    这样即使存量清理没跑到，模型也不会被思考块污染。
    """
    quoted = "<reasoning>内部独白</reasoning><msg><text>正文</text></msg>"
    assert retrieval.model_text(quoted) == "正文"
    assert "内部独白" in retrieval.clean_text(quoted)


def test_model_facing_text_also_gets_the_new_shell_rules():
    """外壳变体、一行式、字面标签这三类，给模型的出口同样适用。"""
    assert retrieval.model_text("<msg /><text/>") == ""
    assert retrieval.model_text("他说的是 <text>这个</text>") == "他说的是 <text>这个</text>"
    assert (
        retrieval.model_text(
            "<msg><reply>7</reply><text>好</text><sticker>8</sticker></msg>"
        )
        == "↩7好[表情8]"
    )
