"""Strict validation with bounded, content-free repair feedback."""

import json
from pydantic import ValidationError


class OutputRejected(ValueError):
    def __init__(self, diagnostic):
        super().__init__("structured_output_rejected")
        self.diagnostic = diagnostic


def diagnostic(exc):
    if isinstance(exc, ValidationError):
        # Never echo invalid values, model-generated extra keys or validator context.
        fields = {
            "summary",
            "facts",
            "category",
            "subject",
            "content",
            "reason",
            "scenario",
            "tags",
            "relations",
            "source_ids",
            "predicate",
            "object",
            "actions",
            "action",
            "target_id",
        }
        return "; ".join(
            ".".join(
                str(p) if isinstance(p, int) or p in fields else "<field>"
                for p in err["loc"]
            )
            + ": "
            + err["type"]
            for err in exc.errors(include_input=False, include_context=False)[:5]
        )[:500]
    if isinstance(exc, json.JSONDecodeError):
        return f"JSON语法错误 line={exc.lineno} column={exc.colno}；仅返回完整JSON对象"
    known = {
        "duplicate JSON key",
        "non-finite JSON number",
        "model output too large",
        "unknown source",
        "unknown audit evidence",
        "overlapping audit actions",
        "invalid merge",
        "cross-scope or cross-category merge is forbidden",
        "unexpected_tool_call",
        "invalid audit source group",
    }
    return str(exc) if str(exc) in known else "输出不是契约要求的JSON对象或类型"


def validate_audit(candidates, output):
    by_id = {r["id"]: r for r in candidates}
    touched = set()
    for action in output["actions"]:
        target = by_id.get(action["target_id"])
        if not target or not set(action["source_ids"]) <= by_id.keys():
            raise ValueError("unknown audit evidence")
        if action["target_id"] in touched:
            raise ValueError("overlapping audit actions")
        group = {action["target_id"], *action["source_ids"]}
        if action["action"] != "merge" and group != {action["target_id"]}:
            raise ValueError("invalid audit source group")
        if action["action"] == "merge":
            if len(group) < 2 or touched & group:
                raise ValueError("invalid merge")
            if any(
                (by_id[k]["sid"], by_id[k]["subject"], by_id[k]["category"])
                != (target["sid"], target["subject"], target["category"])
                for k in group
            ):
                raise ValueError("cross-scope or cross-category merge is forbidden")
        touched.update(group)
