"""Shared strict contracts for model output, tools and configuration."""

from __future__ import annotations
import json
from typing import Annotated, Literal
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

Text = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=16000)
]
Short = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)
]
Category = Literal[
    "event",
    "fact",
    "preference",
    "commitment",
    "relationship",
    "profile",
    "resource",
    "self",
]


def relation_issue(relation):
    """A speech act alone does not establish a relationship between two entities."""
    if relation["predicate"].strip().casefold() in {
        "认为",
        "觉得",
        "说",
        "提到",
        "表示",
        "评价",
        "是",
        "相关",
        "关系",
        "thinks",
        "says",
        "is",
        "mentions",
    }:
        return "谓词没有表达具体关系；请按原文补全，无法确定时删除这条连线"
    if relation["subject"] == relation["object"]:
        return "关系两端相同，需核对身份"
    return ""


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class Relation(Strict):
    subject: Short
    predicate: Short
    object: Short

    @model_validator(mode="after")
    def meaningful(self):
        if issue := relation_issue(self.model_dump()):
            raise ValueError(issue)
        return self


class Fact(Strict):
    category: Category
    subject: Short
    content: Text
    reason: str = Field(max_length=2000)
    scenario: str = Field(max_length=2000)
    tags: list[Short] = Field(max_length=12)
    relations: list[Relation] = Field(max_length=20)
    source_ids: list[Short] = Field(min_length=1, max_length=200)


class Compression(Strict):
    summary: Text
    facts: list[Fact] = Field(max_length=100)


class AuditAction(Strict):
    action: Literal["keep", "correct", "merge"]
    target_id: Short
    source_ids: list[Short] = Field(min_length=1, max_length=100)
    content: Text
    reason: Short
    relations: list[Relation] | None = Field(default=None, max_length=20)


class Audit(Strict):
    actions: list[AuditAction] = Field(max_length=50)


class RecordMerge(Strict):
    """Verdict for a cluster of similar permanent memories."""

    action: Literal["keep", "merge"]
    content: str = Field(default="", max_length=16000)
    reason: Short
    source_ids: list[Short] = Field(min_length=2, max_length=10)

    @model_validator(mode="after")
    def valid(self):
        if self.action == "merge" and not self.content.strip():
            raise ValueError("merged content required")
        return self


class Settings(Strict):
    enabled: bool = True
    capture_enabled: bool = True
    auto_inject: bool = True
    threshold: int = Field(default=100, ge=4, le=10000)
    batch_size: int = Field(default=70, ge=2, le=9999)
    probability: float = Field(default=0.4, ge=0, le=1)
    max_level: int = Field(default=8, ge=1, le=32)
    compress_model: str = ""
    audit_model: str = ""
    embedding_model: str = ""
    semantic_enabled: bool = False
    audit_enabled: bool = True
    audit_interval: int = Field(default=1800, ge=30, le=604800)
    audit_batch: int = Field(default=20, ge=1, le=50)
    model_timeout: int = Field(default=120, ge=5, le=600)
    model_retries: int = Field(default=2, ge=0, le=4)
    worker_count: int = Field(default=2, ge=1, le=4)
    context_chars: int = Field(default=24000, ge=2000, le=500000)
    token_warning: int = Field(default=120000, ge=1000, le=2000000)
    recall_keywords: list[Short] = ["记得", "之前", "上次", "曾经"]
    recall_scope: Literal["session", "linked", "global"] = "global"
    top_k: int = Field(default=5, ge=1, le=30)
    proactive_enabled: bool = False
    proactive_interval: int = Field(default=3600, ge=60, le=604800)
    proactive_sessions: list[Short] = Field(default_factory=list, max_length=100)
    compress_instruction: str = Field(
        default="以自身视角保留事件、感情、人物、关键事实和生活轨迹。精简但不要按珍贵程度丢弃线索；只依据输入，保留时间、否定、条件和不确定性。",
        max_length=4000,
    )
    auto_migrate: bool = True
    mutual_exclusion: bool = True
    migration_max_chars: int = Field(default=120, ge=1, le=16000)
    compress_input_chars: int = Field(default=48000, ge=4000, le=500000)
    boot_enabled: bool = True
    boot_replay_seconds: int = Field(default=90, ge=0, le=86400)
    session_affinity: bool = False
    permanent_dedupe: bool = True
    dedupe_threshold: float = Field(default=0.25, ge=0.1, le=0.95)

    @model_validator(mode="after")
    def valid_batch(self):
        if self.batch_size >= self.threshold:
            raise ValueError("batch_size must be smaller than threshold")
        for sid in self.proactive_sessions:
            parts = sid.split(":", 2)
            if len(parts) != 3 or parts[1] not in ("dm", "gm") or not all(parts):
                raise ValueError("invalid proactive session")
        return self


def parse_output(text: str, contract: type[Strict]):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def invalid(value):
        raise ValueError("non-finite JSON number")

    if len(text) > 250000:
        raise ValueError("model output too large")
    return contract.model_validate(
        json.loads(text, object_pairs_hook=unique, parse_constant=invalid)
    )


def dump(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


class Search(Strict):
    sid: str = ""
    keyword: str = Field(default="", max_length=500)
    prompt: str = Field(default="", max_length=2000)
    level: int | None = Field(default=None, ge=0, le=100)
    start: float | None = None
    end: float | None = None
    offset: int = Field(default=0, ge=0, le=1000000)
    limit: int = Field(default=30, ge=1, le=100)
    subject: str = ""


class Edit(Strict):
    kind: Literal["record", "fact"]
    target: Short
    revision: int = Field(ge=1)
    patch: dict
    reason: Short

    @model_validator(mode="after")
    def validate_patch(self):
        if self.kind == "record":
            if not set(self.patch) <= {"summary", "active", "deleted"}:
                raise ValueError("invalid fields")
            if "summary" in self.patch and (
                not isinstance(self.patch["summary"], str)
                or not self.patch["summary"].strip()
                or len(self.patch["summary"]) > 16000
            ):
                raise ValueError("invalid summary")
        else:
            allowed = set(Fact.model_fields) - {"source_ids"} | {"deleted"}
            if not set(self.patch) <= allowed:
                raise ValueError("invalid fields")
        for k in ("active", "deleted"):
            if k in self.patch and type(self.patch[k]) is not bool:
                raise ValueError("boolean required")
        if not self.patch:
            raise ValueError("empty patch")
        return self


class NewMemory(Strict):
    sid: Short
    content: Text
    users: list[Short] = Field(default_factory=list, max_length=100)
    start: float | None = None
    end: float | None = None


class Job(Strict):
    kind: Literal["compress", "audit", "reindex", "dedupe"]
    sid: Short


class ConfigEdit(Strict):
    revision: Short
    settings: Settings


class NameEdit(Strict):
    entity_id: Short
    name: Short
    revision: int = Field(ge=1)
    reason: Short

    @model_validator(mode="after")
    def valid_name(self):
        if any(ord(c) < 32 for c in self.name):
            raise ValueError("invalid name")
        return self


class EntityRefresh(Strict):
    entity_id: Short


class NameBatch(Strict):
    ids: list[Short] = Field(default_factory=list, max_length=200)
    reason: Short = "批量确认当前QQ昵称"
