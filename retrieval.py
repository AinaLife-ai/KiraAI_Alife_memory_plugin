"""Local relevance and safe factual projections; no embedding provider required."""

import re
import json
import time
from collections import OrderedDict


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
    """Ephemeral delivered-result history, bounded and isolated by caller and scope."""

    def __init__(self):
        self.entries = OrderedDict()

    def get(self, key):
        now = time.monotonic()
        for k in list(self.entries):
            if now - self.entries[k]["updated"] > 1800:
                del self.entries[k]
        return self.entries.get(key, {"query": "", "ids": [], "facts": []})

    def remember(self, key, query, ids, facts=(), continuation=False):
        old = self.get(key) if continuation else {"ids": [], "facts": []}
        self.entries[key] = dict(
            query=query,
            ids=list(dict.fromkeys([*old["ids"], *ids]))[-200:],
            facts=list(dict.fromkeys([*old["facts"], *facts]))[-200:],
            updated=time.monotonic(),
        )
        self.entries.move_to_end(key)
        while len(self.entries) > 256:
            self.entries.popitem(last=False)


def asks_for_more(text):
    return bool(
        re.fullmatch(
            r"[\s，,。.!！?？]*(还有别的(?:吗|么)?|还有呢|还有吗|还有么|再说点|继续回忆|再想想|别的呢)[\s，,。.!！?？]*",
            text,
        )
    )


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


def safe_facts(facts):
    return [
        {
            **{
                k: v
                for k, v in f.items()
                if k not in {"verified_relations", "relation_warnings"}
            },
            "relations": f.get("verified_relations", f["relations"]),
            "relationship_status": "needs_review"
            if f.get("relation_warnings")
            else "evidence_required",
        }
        for f in facts
    ]
