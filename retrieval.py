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


def relevance(query, text):
    chunks = re.findall(r"[a-z0-9_]+|[\u3400-\u9fff]+", query.casefold())
    tokens = {
        t
        for c in chunks
        for t in (
            [c]
            if len(c) < 2 or not re.match(r"[\u3400-\u9fff]", c)
            else [c[i : i + 2] for i in range(len(c) - 1)]
        )
    } - STOP
    lowered = text.casefold()
    return sum(len(t) * (t in lowered) for t in tokens)


def bot_facts(facts, current_sid=""):
    """给 Bot 看的精简事实视图：只留判断与追溯必需的字段。

    内部簿记（fingerprint/deleted/revision/merge_pending/audited）、
    分类装饰（scenario/tags）、以及只给审计用的大段 reason 都不进上下文。
    """
    view = []
    for fact in facts:
        item = {
            "category": fact.get("category", ""),
            "subject": fact.get("subject", ""),
            "content": fact.get("content", ""),
            "relations": fact.get("verified_relations", fact.get("relations", [])),
            "importance": fact.get("importance", 5),
        }
        if fact.get("sid") and fact["sid"] != current_sid:
            item["sid"] = fact["sid"]
            if fact.get("src_user"):
                item["by"] = fact["src_user"]
        source = fact.get("src") or (fact.get("sources") or [None])[-1]
        if source:
            item["src"] = source
        created = fact.get("created")
        if isinstance(created, (int, float)) and created > 0:
            item["t"] = time.strftime("%Y-%m-%d", time.gmtime(created))
        if fact.get("relationship_status") == "needs_review" or fact.get(
            "relation_warnings"
        ):
            item["needs_review"] = True
        view.append(item)
    return view

