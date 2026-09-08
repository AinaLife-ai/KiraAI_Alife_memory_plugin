"""Local relevance and safe factual projections; no embedding provider required."""

import re

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
