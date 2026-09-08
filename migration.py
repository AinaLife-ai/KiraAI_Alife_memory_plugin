"""Read-only legacy adapters; transactional, source-addressed imports.

KiraOS TOML is the authority, never its rebuildable SQLite index. No provider
calls, source rewrites, inferred compression depth, or invented entity IDs.
"""

from __future__ import annotations
import hashlib
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote

try:
    import tomllib
except ImportError:
    import tomli as tomllib

from .contracts import Fact, dump

SIMPLE = "kira_plugin_simple_memory"
KIRAOS = "kira_plugin_kiraos"
SOURCES = (SIMPLE, KIRAOS)


class Rejected(ValueError):
    pass


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def clean(value, limit):
    if not isinstance(value, str):
        raise Rejected("invalid_text")
    # Check the original decoded content before normalization, never truncate.
    if len(value) > limit:
        raise Rejected("too_long")
    value = value.strip()
    if not value or not any(c.isalnum() for c in value):
        raise Rejected("empty")
    if any(ord(c) < 32 and c not in "\n\t\r" for c in value):
        raise Rejected("control_characters")
    if value.startswith(("```", "{", "[", "<")) or re.search(r"</?\w+[^>]*>", value):
        raise Rejected("markup_or_serialized_output")
    if value in {
        "暂无",
        "暂无信息",
        "暂无画像信息",
        "无",
        "未知",
        "无有效信息",
        "None",
        "null",
    }:
        raise Rejected("placeholder")
    if len(value) > 12 and len(set(value)) < 3:
        raise Rejected("repetition")
    return value


def timestamp(value, fallback):
    try:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if isinstance(value, datetime):
            # Naive source times are explicitly interpreted as UTC, not host-local.
            return value.replace(
                tzinfo=value.tzinfo or timezone.utc
            ).timestamp(), "source.time"
        if type(value) in (int, float) and 0 < value < 32503680000:
            return float(value), "source.timestamp"
    except (ValueError, OverflowError):
        pass
    return fallback, "file_mtime_estimate"


def location(relative, source):
    parts = relative.parts
    if parts[0] == "global":
        return (
            "legacy:global",
            "global",
            "legacy:self" if "self" in parts else "legacy:global",
            [],
        )
    raw_sid = source.get("session", "")
    sid = "legacy:unscoped"
    if isinstance(raw_sid, str):
        split = raw_sid.split(":", 2)
        if len(split) == 3 and all(split) and split[1] in ("dm", "gm", "pm"):
            sid = f"{split[0]}:{'dm' if split[1] == 'pm' else split[1]}:{split[2]}"
    entity_type, _, entity = parts[1].partition("_") if len(parts) > 1 else ("", "", "")
    entity = unquote(entity)
    # Only adapter-qualified entity IDs can be matched to live users.
    if entity_type == "user" and re.fullmatch(r"[^:\s]+:[^\s]+", entity):
        return "legacy:user:" + entity, "user", entity, [entity]
    if entity_type == "group" and re.fullmatch(r"[^:\s]+:[^\s]+", entity):
        adapter, identifier = entity.split(":", 1)
        sid = f"{adapter}:gm:{identifier}"
    return sid, "session", f"legacy:{entity_type}:{entity}" if entity else sid, []


def profile_entries(data):
    for key in ("name", "nickname", "description"):
        if data.get(key):
            yield (
                key,
                f"{key}: {data[key]}" if isinstance(data[key], str) else data[key],
                "profile",
                [],
            )
    for key, category in (
        ("traits", "profile"),
        ("aliases", "profile"),
        ("facts", "fact"),
    ):
        values = data.get(key, [])
        if not isinstance(values, list):
            raise Rejected("invalid_profile")
        for index, value in enumerate(values):
            yield f"{key}/{index}", value, category, []
    for key, category in (
        ("preferences", "preference"),
        ("relationships", "relationship"),
    ):
        values = data.get(key, {})
        if not isinstance(values, dict):
            raise Rejected("invalid_profile")
        for name, value in values.items():
            if not isinstance(value, str):
                yield f"{key}/{name}", value, category, []
            else:
                yield (
                    f"{key}/{name}",
                    f"{name}: {value}",
                    category,
                    [(name, value)] if key == "relationships" else [],
                )


def snapshot(root: Path, plugin_id: str, limit: int):
    """Read only named memory sources; each rejected item has an auditable reason."""
    root = root.resolve()
    paths = (
        [root / "core.txt"]
        if plugin_id == SIMPLE
        else sorted(
            [
                p
                for folder in ("entities", "global")
                for p in (root / folder).rglob("*")
                if p.suffix == ".toml" or p.name == "profile.json"
            ]
        )
    )
    items, files, errors = [], {}, []
    for path in paths:
        if not path.exists():
            continue
        relative = path.relative_to(root)
        if "skills" in relative.parts:
            continue
        key = relative.as_posix()
        try:
            if not path.resolve().is_relative_to(root) or path.is_symlink():
                raise ValueError("unsafe_source_path")
            if path.stat().st_size > 2_000_000:
                raise ValueError("source_too_large")
            raw = path.read_bytes()
            files[key] = hashlib.sha256(raw).hexdigest()
            mtime = path.stat().st_mtime
            text = raw.decode("utf-8-sig")
            if plugin_id == SIMPLE:
                entries = [
                    (str(i), line, "fact", [])
                    for i, line in enumerate(text.splitlines(), 1)
                ]
                sid, visibility, subject, users = (
                    "legacy:global",
                    "global",
                    "legacy:global",
                    [],
                )
                ts, time_basis, tags, metadata = mtime, "file_mtime_estimate", [], {}
            else:
                data = (
                    json.loads(text, object_pairs_hook=unique_object)
                    if path.name == "profile.json"
                    else tomllib.loads(text)
                )
                if not isinstance(data, dict):
                    raise ValueError("invalid_source_document")
                source = data.get("source", {})
                if not isinstance(source, dict):
                    raise ValueError("invalid_source_metadata")
                sid, visibility, subject, users = location(relative, source)
                ts, time_basis = timestamp(
                    source.get("time", data.get("last_interaction")), mtime
                )
                tags = data.get("tags", [])
                if not isinstance(tags, list) or any(
                    not isinstance(t, str) for t in tags
                ):
                    raise ValueError("invalid_tags")
                tags = list(dict.fromkeys(t.strip() for t in tags if t.strip()))[:11]
                metadata = {
                    "session": source.get("session", ""),
                    "importance": data.get("importance"),
                    "type": data.get("type"),
                }
                if path.name == "profile.json":
                    _, _, folder_id = relative.parts[1].partition("_")
                    if data.get("entity_id") and data["entity_id"] != unquote(
                        folder_id
                    ):
                        raise ValueError("profile_entity_mismatch")
                    entries = list(profile_entries(data))
                else:
                    category = {
                        "entity": "profile",
                        "task": "commitment",
                        "source": "resource",
                        "reflection": "self" if subject == "legacy:self" else "profile",
                    }.get(data.get("type"), data.get("type", "fact"))
                    if category not in (
                        "event",
                        "fact",
                        "preference",
                        "commitment",
                        "relationship",
                        "profile",
                        "resource",
                        "self",
                    ):
                        category = "fact"
                    entries = [
                        (str(data.get("id", "text")), data.get("text"), category, [])
                    ]
            for entry_key, value, category, relationships in entries:
                item = {
                    "key": key + "#" + entry_key,
                    "file": key,
                    "file_hash": files[key],
                }
                item["hash"] = hashlib.sha256(
                    dump(
                        [value, sid, subject, category, tags, relationships, metadata]
                    ).encode()
                ).hexdigest()
                try:
                    content = clean(value, 16000 if plugin_id == SIMPLE else limit)
                    relations = [
                        {"subject": subject, "predicate": relation, "object": target}
                        for target, relation in relationships
                    ]
                    fact = Fact(
                        category=category,
                        subject=subject,
                        content=content,
                        reason="",
                        scenario="",
                        tags=[*tags, "legacy_import"],
                        relations=relations,
                        source_ids=["pending"],
                    ).model_dump()
                    item.update(
                        sid=sid,
                        visibility=visibility,
                        subject=subject,
                        users=users,
                        content=content,
                        fact=fact,
                        time=ts,
                        time_basis=time_basis,
                        metadata=metadata,
                        reason="",
                    )
                except (ValueError, TypeError) as exc:
                    item["reason"] = (
                        str(exc) if isinstance(exc, Rejected) else "invalid_fields"
                    )
                items.append(item)
        except (OSError, UnicodeError, ValueError, TypeError) as exc:
            errors.append({"file": key, "reason": type(exc).__name__})
    return {"source": plugin_id, "items": items, "files": files, "errors": errors}


def import_snapshot(store, snapshot):
    """Records, facts and the receipt commit together; retries never revive deletions."""
    counts = {"imported": 0, "duplicate": 0, "skipped": 0, "unscoped": 0}
    source = snapshot["source"]
    with store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        for item in snapshot["items"]:
            previous = db.execute(
                "SELECT record_id FROM migration_items WHERE source=? AND source_key=? AND digest=?",
                (source, item["key"], item["hash"]),
            ).fetchone()
            if previous and (previous[0] or item["reason"]):
                counts["duplicate" if previous[0] else "skipped"] += 1
                continue
            if previous:
                db.execute(
                    "DELETE FROM migration_items WHERE source=? AND source_key=? AND digest=?",
                    (source, item["key"], item["hash"]),
                )
            record_id = None
            if item["reason"]:
                counts["skipped"] += 1
            else:
                fingerprint = hashlib.sha256(
                    dump(
                        [
                            item["sid"],
                            item["subject"],
                            item["content"],
                            item["fact"]["category"],
                            item["fact"]["relations"],
                        ]
                    ).encode()
                ).hexdigest()
                record_id = "legacy-" + fingerprint
                store._ensure_entities(db, item["sid"], item["users"])
                existing = db.execute(
                    "SELECT id FROM records WHERE id=?", (record_id,)
                ).fetchone()
                if not existing:
                    position = db.execute(
                        "SELECT coalesce(max(position),0)+1 FROM records WHERE sid=?",
                        (item["sid"],),
                    ).fetchone()[0]
                    evidence = dump(
                        {
                            "legacy_text": item["content"],
                            "source": source,
                            "file": item["file"],
                            "digest": item["file_hash"],
                            "time_basis": item["time_basis"],
                            "metadata": item["metadata"],
                        }
                    )
                    db.execute(
                        """INSERT INTO records (id,sid,role,level,start,end,summary,content,users,position,created,visibility)
                        VALUES (?,?,'user',0,?,?,?,?,?,?,?,?)""",
                        (
                            record_id,
                            item["sid"],
                            item["time"],
                            item["time"],
                            item["content"],
                            evidence,
                            dump(item["users"]),
                            position,
                            time.time(),
                            item["visibility"],
                        ),
                    )
                    item["fact"]["source_ids"] = [record_id]
                    store._add_fact(db, item["sid"], item["fact"])
                    counts["imported"] += 1
                else:
                    counts["duplicate"] += 1
                if item["sid"] == "legacy:unscoped":
                    counts["unscoped"] += 1
            db.execute(
                "INSERT INTO migration_items VALUES (?,?,?,?,?,?,?,?)",
                (
                    source,
                    item["key"],
                    item["hash"],
                    record_id,
                    item["reason"],
                    item["file_hash"],
                    dump(item.get("metadata", {})),
                    time.time(),
                ),
            )
        counts["unscoped"] = db.execute(
            """SELECT count(DISTINCT records.id) FROM records JOIN migration_items
            ON migration_items.record_id=records.id WHERE migration_items.source=? AND records.sid='legacy:unscoped'
            AND records.deleted=0""",
            (source,),
        ).fetchone()[0]
        report = {
            "source": source,
            **counts,
            "errors": snapshot["errors"],
            "files": len(snapshot["files"]),
            "updated": time.time(),
        }
        db.execute(
            "INSERT OR REPLACE INTO migration_reports VALUES (?,?)",
            (source, dump(report)),
        )
        store.bump(db)
    return report
