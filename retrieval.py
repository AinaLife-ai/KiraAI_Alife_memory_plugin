"""Local relevance and safe factual projections; no embedding provider required."""

import re
import json
import time
from collections import OrderedDict

# Our own read tools return a distinctive JSON envelope. Those results are
# self-recall echoes: storing them as memories only bloats the next context.
_MEMORY_PAYLOAD_MARKERS = (
    '"archives_in_context"',
    '"children_total"',
    '"next_page"',
    '"subjects"',
    '"entities"',
    '"omitted_ids"',
    '"related_archives"',
)
TOOL_RESULT_PREFIX = "工具感知结果："


def looks_like_memory_payload(text):
    head = (text or "")[:4000]
    if head.startswith(TOOL_RESULT_PREFIX):
        head = head[len(TOOL_RESULT_PREFIX) :].lstrip()
    # Tolerate different serializers (with or without spaces after ':').
    return head[:200].replace(" ", "").startswith('{"ok":true') and any(
        marker in head for marker in _MEMORY_PAYLOAD_MARKERS
    )


def tool_preview(text, limit=240):
    """Short, single-line preview kept as the injected summary."""
    flat = " ".join((text or "").split())
    return flat[:limit] + ("…" if len(flat) > limit else "")


def tool_call_summary(tool_calls, limit=60):
    """Readable replacement for the raw tool_calls JSON blob."""
    parts = []
    for call in tool_calls or []:
        function = call.get("function", {}) if isinstance(call, dict) else {}
        name = function.get("name") or call.get("name") or "工具"
        arguments = function.get("arguments") or ""
        parts.append(f"{name}({arguments[:limit]})" if arguments else name)
    return "[调用工具：" + "、".join(parts) + "]" if parts else ""


# Names written by third-party plugins for their own synthetic messages; they
# must never become a user's remembered nickname.
SYNTHETIC_NAMES = frozenset(
    {
        "提醒任务所有者",
        "Kira",
        "system",
        "system:reminder_plugin",
        "Web UI 用户",
        "Web UI 管理员",
        "自主意图循环",
        "未知",
    }
)

_NOISE = re.compile(r"[\s，。、；：！？,.!?;:'\"“”‘’()（）\[\]【】<>《》\-—~～/\\]+")


def normalize_text(text):
    """Case- and punctuation-insensitive form used for duplicate detection."""
    return _NOISE.sub("", (text or "").casefold())


def _bigrams(text):
    flat = normalize_text(text)
    if len(flat) < 2:
        return {flat} if flat else set()
    return {flat[i : i + 2] for i in range(len(flat) - 1)}


def similarity(left, right, min_overlap=4):
    """Bigram containment: "does one memory largely cover the other".

    Jaccard over long texts is too diluted for near-duplicate detection, so we
    score the overlap against the smaller side and require a real overlap.
    Short facts need a lower overlap floor; callers can pass ``min_overlap``.
    """
    a, b = _bigrams(left), _bigrams(right)
    if not a or not b:
        return 0.0
    overlap = len(a & b)
    if overlap < min_overlap:
        return 0.0
    return overlap / min(len(a), len(b))


def identity_info(entity_id):
    """Display labels; synthetic ids are pending/uncategorised, never a fake entity."""
    from . import identity

    if entity_id in (identity.GLOBAL, identity.GLOBAL_ID):
        return {
            "label": "全局记忆",
            "identity_note": "跨会话共享的全局记忆，不是群聊。",
            "lookup_id": "",
        }
    if entity_id in (identity.SELF, identity.SELF_ID):
        return {
            "label": "机器人自身",
            "identity_note": "机器人自己的认知与经历。",
            "lookup_id": "",
        }
    if entity_id in (identity.UNSCOPED, identity.UNSCOPED_ID):
        return {
            "label": "未分类 · 来源会话未确定",
            "identity_note": "旧数据没有可靠会话标识；保留待核对，不猜群名或归属。",
            "lookup_id": "",
        }
    if entity_id.startswith(identity.PENDING):
        shape = identity.pending_shape(entity_id)
        kind = "群" if shape and shape[0] == "group" else "人物"
        number = shape[2] if shape else ""
        return {
            "label": f"待绑定 · {kind} {number}",
            "identity_note": "已按号码登记；出现同号码账号或在线适配器后会自动合并。",
            "lookup_id": "",
        }
    if entity_id.startswith(identity.LEGACY):
        shape = identity.legacy_shape(entity_id)
        if not shape:
            return {
                "label": "未分类",
                "identity_note": "身份尚未确认。",
                "lookup_id": "",
            }
        kind, adapter, number = shape
        if kind == "user":
            lookup = f"{adapter}:{number}" if adapter else ""
        elif kind == "group":
            lookup = f"{adapter}:gm:{number}" if adapter else ""
        else:
            lookup = ""
        return {
            "label": f"待绑定 · 号码 {number}" if number else "未分类",
            "identity_note": "同号码账号出现后自动合并，不会单独保留为旧档案。",
            "lookup_id": lookup,
        }
    return {
        "label": "名称待补全",
        "identity_note": "使用稳定账号区分身份，昵称相同不会合并。",
        "lookup_id": entity_id,
    }


def archive_view(row, child_offset=0, child_count=20, include_content=False):
    result = {
        k: row[k]
        for k in (
            "id",
            "sid",
            "role",
            "level",
            "start",
            "end",
            "summary",
            "users",
            "revision",
            "permanent",
        )
    }
    children = row.get("children", [])
    result.update(
        children=children[child_offset : child_offset + child_count],
        children_total=len(children),
        child_offset=child_offset,
        next_child_offset=child_offset + child_count
        if child_offset + child_count < len(children)
        else None,
    )
    result["parents"] = row.get("parents", [])
    if include_content:
        result["versions"] = row.get("versions", [])
        result["legacy_sources"] = row.get("legacy_sources", [])
    result["content_included"] = not children or include_content
    if result["content_included"]:
        # Decode only the plugin's own complete archive/message envelope, not arbitrary prose.
        content = row["content"]
        try:
            parsed = json.loads(content)
            if isinstance(parsed, list) and all(
                isinstance(r, dict) and {"id", "role", "content"} <= r.keys()
                for r in parsed
            ):
                content = parsed
            elif (
                isinstance(parsed, dict)
                and parsed.get("role") in {"user", "assistant", "tool"}
                and "content" in parsed
            ):
                content = parsed
        except (ValueError, TypeError):
            pass
        result["content"] = content
    return result


class RecallWindow:
    """Delivered-result history: what this conversation has already been shown.

    It accumulates for the lifetime of the conversation (30 minutes), so the
    model never receives the same memory twice unless it explicitly asks for it.
    """

    def __init__(self):
        self.entries = OrderedDict()

    def get(self, key):
        now = time.monotonic()
        for k in list(self.entries):
            if now - self.entries[k]["updated"] > 1800:
                del self.entries[k]
        return self.entries.get(key, {"query": "", "ids": [], "facts": []})

    def remember(self, key, query, ids, facts=()):
        old = self.get(key)
        self.entries[key] = dict(
            # An empty query never overwrites the last search topic.
            query=query or old["query"],
            ids=list(dict.fromkeys([*old["ids"], *ids]))[-300:],
            facts=list(dict.fromkeys([*old["facts"], *facts]))[-300:],
            updated=time.monotonic(),
        )
        self.entries.move_to_end(key)
        while len(self.entries) > 256:
            self.entries.popitem(last=False)


STOP = {
    "记得",
    "之前",
    "上次",
    "曾经",
    "什么",
    "那个",
    "这个",
    "我们",
    "你们",
    "他们",
    "怎么",
    "是不是",
    "the",
    "and",
    "that",
    "with",
}


import re

# 纯包裹层：成对出现才剥
WRAPPER_TAGS = ("msg", "text", "forward", "quote", "message")
_WRAPPER_RE = {
    tag: (
        re.compile(rf"<{tag}(?:\s[^>]*)?>", re.I),
        re.compile(rf"</{tag}\s*>", re.I),
    )
    for tag in WRAPPER_TAGS
}

# 带语义的标签：压缩成短记号，但保留信息
_INLINE_RE = [
    (re.compile(r"<reply>(.*?)</reply>", re.S | re.I), lambda m: "↩" + m.group(1).strip()),
    (re.compile(r"<at>(.*?)</at>", re.S | re.I), lambda m: "@" + m.group(1).strip()),
    (re.compile(r"<sticker>(.*?)</sticker>", re.S | re.I), lambda m: "[表情" + m.group(1).strip() + "]"),
    (re.compile(r"<image>(.*?)</image>", re.S | re.I), lambda m: "[图片" + m.group(1).strip() + "]"),
]

_CJK = r"\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff"
_SPACE_BETWEEN_CJK = re.compile(rf"(?<=[{_CJK}])[ \t]+(?=[{_CJK}])")
# 逐字拉开写（「很 重 要」「请 注 意」）是刻意的强调：至少三个汉字被空白隔开。
# 折行残留只会插入「一个」空格，不可能连成这种形态，所以用它区分。
_SPACED_OUT = re.compile(rf"[{_CJK}](?:[ \t]+[{_CJK}]){{2,}}")
_MULTI_SPACE = re.compile(r"[ \t]{2,}")
_BLANK_LINES = re.compile(r"\n\s*\n+")
_LEADING_INDENT = re.compile(r"\n[ \t]+")
_BAD_CHARS = re.compile(r"[\u0000-\u0008\u000b\u000c\u000e-\u001f\ufffd]")


_MSG_OPEN = re.compile(r"<msg(?:\s[^>]*)?>", re.I)
_MSG_CLOSE = re.compile(r"</msg\s*>", re.I)
_OTHER_WRAPPERS = re.compile(r"</?(?:forward|quote|message)(?:\s[^>]*)?>", re.I)
# 一个消息块：以 <text> 开头、</text> 收尾（允许前面有气泡/换行）。
# 用「锚定 + 非贪婪」而不是数配对，这样正文里真的写了 <text> 也不会被吃掉。
_BLOCK = re.compile(
    r"(?:^|\n)[ \t]*<text(?:\s[^>]*)?>(.*?)</text\s*>(?=[ \t]*(?:\n|$))",
    re.S | re.I,
)


def _strip_wrappers(text):
    """剥掉 message_str 的最外层容器，保留消息之间的边界（换行）。"""
    out = _MSG_OPEN.sub("", text)
    out = _MSG_CLOSE.sub("\n", out)
    out = _OTHER_WRAPPERS.sub("", out)
    for _ in range(3):  # 少数情况会套两层
        # 保留消息边界：匹配时吃掉了前导换行，这里补回来
        new_out = _BLOCK.sub(lambda m: "\n" + m.group(1), out)
        if new_out == out:
            break
        out = new_out
    return out


def clean_text(text, keep=()):
    """剥掉最外层包裹 + 归一空白。孤立的 ``<``、正文里的字面标签都原样保留。

    ``keep`` 是「不能被空白归一碰」的片段——昵称真的可能带空格（``星 月``），
    渲染前先从实体表查出这类名字传进来，正文里的它们原样保留。
    """
    if not text:
        return ""
    out = str(text)
    holders = {}
    # 刻意拉开写的强调句先原样保出来
    for match in _SPACED_OUT.finditer(out):
        token = "\ue000%d\ue001" % len(holders)
        while token in out:
            token += "\ue000"
        holders[token] = match.group(0)
        out = out.replace(match.group(0), token, 1)
    for value in keep or ():
        value = str(value or "")
        # 只保护真正出现、且确实含空白的名字；占位符用私用区字符，不会被其它规则碰到
        if value and value in out and any(c.isspace() for c in value):
            token = "\ue000%d\ue001" % len(holders)
            while token in out:
                token += "\ue000"
            holders[token] = value
            out = out.replace(value, token)
    out = _BAD_CHARS.sub("", out)
    out = _strip_wrappers(out)
    for pattern, repl in _INLINE_RE:
        out = pattern.sub(repl, out)
    out = _LEADING_INDENT.sub("\n", out)
    out = _BLANK_LINES.sub("\n", out)
    out = _SPACE_BETWEEN_CJK.sub("", out)
    out = _MULTI_SPACE.sub(" ", out)
    out = "\n".join(line.strip() for line in out.split("\n")).strip()
    for token, value in holders.items():
        out = out.replace(token, value)
    return out


def trim_nested(text, reply_chars=40, desc_chars=100):
    """压缩嵌套的长文本：引用里的原文、表情/图片的视觉描述。

    只截断「嵌套段落」，正文摘要不动；括号用配对扫描，不会被内容里的 ``]`` 提前截断。
    日志实测：表情包的视觉描述平均 276 字符，是摘要里最大的单块开销。
    """
    if not text:
        return ""
    out, i = [], 0
    while i < len(text):
        reply = _REPLY_HEAD.match(text, i)
        if reply:
            end = _scan_bracket(text, i)
            if end < 0:
                out.append(text[i:])
                break
            inner = text[reply.end() : end]
            out.append(f"[Reply {reply.group(1)}: {_clip(inner, reply_chars)}]")
            i = end + 1
            continue
        media = _MEDIA_HEAD.match(text, i)
        if media:
            end = _scan_bracket(text, i)
            if end < 0:
                out.append(text[i:])
                break
            body = text[media.end() : end].rstrip()
            tail = ""
            split = re.match(r"(.*?)(,\s*file_path:.*)$", body, re.S)
            if split:
                body, tail = split.group(1), split.group(2)
            out.append(f"[{media.group(1)} {_clip(body, desc_chars)}{tail}]")
            i = end + 1
            continue
        out.append(text[i])
        i += 1
    return "".join(out)


def model_text(text, keep=(), reply_chars=40, desc_chars=100):
    """统一入口：剥包裹 + 压空白 + 截断嵌套长描述（只影响模型看到的样子）。"""
    return trim_nested(clean_text(text, keep), reply_chars, desc_chars)


def _scan_bracket(text, start):
    """从 ``text[start]`` 的 ``[`` 起找到配对的 ``]``（考虑嵌套），返回下标或 -1。"""
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "[":
            depth += 1
        elif text[i] == "]":
            depth -= 1
            if depth == 0:
                return i
    return -1


_REPLY_HEAD = re.compile(r"\[Reply ID:\s*(\d+)\s*content:\s*")
_MEDIA_HEAD = re.compile(r"\[(Sticker|Image)\s+")


def _clip(text, limit):
    text = text.strip()
    if limit and len(text) > limit:
        return text[:limit].rstrip() + "…"
    return text


def short_time(value):
    """epoch → ``MM-DD HH:MM``（本地时区）。浮点秒对模型毫无意义，还占 25 字符。"""
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return ""
    if ts <= 0:
        return ""
    return time.strftime("%m-%d %H:%M", time.localtime(ts))


def full_time(value):
    """epoch → ``YYYY-MM-DD HH:MM``（本地时区）。

    模型对浮点秒没有任何直觉（1772908601.6758957 是几号？），可读时间反而让它
    判断时间线更准；年份必须带上，跨年批次才不会有歧义。
    """
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return ""
    if ts <= 0:
        return ""
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))


# 给模型看的类别短码：两字母，配合静态规则块里的一行图例（规则块走缓存，不额外花每轮 token）
CATEGORY_CODES = {
    "event": "ev",
    "fact": "fa",
    "preference": "pr",
    "commitment": "co",
    "relationship": "re",
    "profile": "pf",
    "resource": "rs",
    "self": "sf",
}
# 记忆类别优先级：越小越"必须留在上下文里"（用于注入排序与裁剪）
CATEGORY_RANK = {
    "rule": 0,
    "commitment": 1,
    "preference": 2,
    "profile": 3,
    "relationship": 4,
    "resource": 5,
    "event": 6,
    "fact": 7,
    "note": 8,
}

CATEGORY_LEGEND = "事实短码：" + " ".join(
    f"{code}={name}" for name, code in CATEGORY_CODES.items()
)


def named_pair(entity_id, name=None):
    """``qq:769690776(周武)`` —— 稳定 ID 在前，名字在括号里。

    模型照着抄 subject 时拿到的是 ID；同时它知道这个人叫什么，写摘要就能用名字。
    """
    entity_id = str(entity_id or "")
    name = str(name or "").strip()
    if not entity_id:
        return name
    if not name or name == entity_id:
        return entity_id
    return f"{entity_id}({name})"


def bare_id(value):
    """从 ``qq:769690776(周武)`` 里取回 ``qq:769690776``。"""
    value = str(value or "").strip()
    head, sep, _ = value.partition("(")
    return head.strip() if sep else value


def squeeze(text):
    """轻量归一：去控制/替换字符、压缩空白、去掉 CJK 之间的空格。

    检索两侧都过一遍，于是「翅 膀」这种被插入空格的写法依然能命中「翅膀」。
    """
    if not text:
        return ""
    out = _BAD_CHARS.sub("", str(text))
    out = _SPACE_BETWEEN_CJK.sub("", out)
    return _MULTI_SPACE.sub(" ", out).strip()


def bigram_body(text):
    """把文本切成「与 query_tokens 完全一致」的词元串，用于 FTS5 索引。

    汉字按 2 字滑窗（中文双字词是常态，而 trigram 分词器要求 ≥3 字符、命中不了），
    字母数字整段保留（否则 "iphone" 会被切成 "ip ph ho …"，查询侧就对不上 ✗）。
    索引前走同一个 squeeze()，保证与打分侧口径一致。
    """
    squeezed = squeeze(text or "")
    out = []
    for chunk in re.findall(r"[a-z0-9_]+|[\u3400-\u9fff]+", squeezed.casefold()):
        if len(chunk) >= 2 and re.match(r"[\u3400-\u9fff]", chunk):
            out.extend(chunk[i : i + 2] for i in range(len(chunk) - 1))
        else:
            out.append(chunk)
    return " ".join(out)


def fts_match_query(tokens):
    """把词元拼成安全的 FTS5 MATCH 表达式（一律当短语、内部引号双写）。"""
    quoted = []
    for token in tokens:
        value = str(token or "").strip()
        if not value:
            continue
        quoted.append('"%s"' % value.replace('"', '""'))
    return " OR ".join(quoted)


def query_tokens(query):
    """查询侧词元（与 relevance 口径完全一致），供 SQL 粗筛复用。"""
    chunks = re.findall(r"[a-z0-9_]+|[\u3400-\u9fff]+", squeeze(query).casefold())
    return sorted(
        {
            t
            for chunk in chunks
            for t in (
                [chunk]
                if len(chunk) < 2 or not re.match(r"[\u3400-\u9fff]", chunk)
                else [chunk[i : i + 2] for i in range(len(chunk) - 1)]
            )
        }
        - STOP
    )


def relevance(query, text):
    tokens = query_tokens(query)
    lowered = squeeze(text).casefold()
    return sum(len(t) * (t in lowered) for t in tokens)


def bot_facts(facts, current_sid="", short=None):
    """给 Bot 看的精简事实视图：只留判断与追溯必需的字段。

    内部簿记（fingerprint/deleted/revision/merge_pending/audited）、
    分类装饰（scenario/tags）、以及只给审计用的大段 reason 都不进上下文。

    ``short`` 可传入「真实 id → 短码」的转换函数；空字段（如空的 relations）
    直接省略——它们在每轮注入里是纯开销。
    """
    view = []
    for fact in facts:
        # 短键 + 类别短码 + 默认值省略：每轮都发的东西，信封比内容还贵
        item = {
            "c": CATEGORY_CODES.get(fact.get("category", ""), fact.get("category", "")),
            "u": fact.get("subject", ""),  # 稳定实体 ID 不变，工具要用它
            "x": fact.get("content", ""),
        }
        importance = fact.get("importance", 5)
        if importance != 5:
            item["imp"] = importance
        relations = fact.get("verified_relations", fact.get("relations", []))
        if relations:
            item["rel"] = relations
        if fact.get("sid") and fact["sid"] != current_sid:
            item["sid"] = fact["sid"]
            if fact.get("src_user"):
                item["by"] = fact["src_user"]
        source = fact.get("src") or (fact.get("sources") or [None])[-1]
        if source:
            # 短码映射里没有的（例如 sources 兜底值）就用原值，绝不输出 null
            item["src"] = (short(source) or source) if short else source
        created = fact.get("created")
        if isinstance(created, (int, float)) and created > 0:
            item["t"] = time.strftime("%m-%d", time.localtime(created))
        if fact.get("relationship_status") == "needs_review" or fact.get(
            "relation_warnings"
        ):
            item["rev"] = True
        view.append(item)
    return view

