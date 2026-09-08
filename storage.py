"""Archive tree ported from Alife MemoryManager/MemoryStorage (AGPL-3.0).

All raw records survive compression; transactions protect asynchronous edits.
"""

from __future__ import annotations
import asyncio
import hashlib
import json
import math
import sqlite3
import time
import uuid
from contextlib import contextmanager, closing
from pathlib import Path
from .contracts import dump, relation_issue
from .output_validation import validate_audit
from .retrieval import identity_info


class Conflict(ValueError):
    pass


def uid():
    return uuid.uuid4().hex


class Store:
    def __init__(self, path: Path):
        self.path = path

    async def call(self, method, *args, **kwargs):
        operation = asyncio.create_task(
            asyncio.to_thread(getattr(self, method), *args, **kwargs)
        )
        try:
            return await asyncio.shield(operation)
        except asyncio.CancelledError:
            # Cancelling a coroutine does not stop its SQLite worker thread.
            # Join it before reload may open the same database with a new engine.
            await asyncio.gather(operation, return_exceptions=True)
            raise

    def requeue_running(self):
        with self.connect() as db:
            db.execute(
                "UPDATE jobs SET state='queued',detail='paused during reload' WHERE state='running'"
            )

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=20)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            with db:
                yield db
        finally:
            db.close()

    def initialize(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            with closing(sqlite3.connect(self.path)) as source:
                if source.execute("PRAGMA user_version").fetchone()[0] < 3:
                    backup = self.path.with_name(self.path.stem + ".pre-v3.sqlite3")
                    if not backup.exists():
                        temporary = backup.with_suffix(".tmp")
                        with closing(sqlite3.connect(temporary)) as target:
                            source.backup(target)
                        temporary.replace(backup)
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
            CREATE TABLE IF NOT EXISTS records (
              id TEXT PRIMARY KEY, sid TEXT NOT NULL, role TEXT NOT NULL,
              level INTEGER NOT NULL, start REAL NOT NULL, end REAL NOT NULL,
              summary TEXT NOT NULL, content TEXT NOT NULL, users TEXT NOT NULL,
              active INTEGER NOT NULL DEFAULT 1, deleted INTEGER NOT NULL DEFAULT 0,
              revision INTEGER NOT NULL DEFAULT 1, permanent INTEGER NOT NULL DEFAULT 0,
              position INTEGER NOT NULL, event_key TEXT UNIQUE, created REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS record_context ON records(sid,active,deleted,level DESC,position);
            CREATE INDEX IF NOT EXISTS record_time ON records(level,start,end);
            CREATE TABLE IF NOT EXISTS edges (
              parent TEXT NOT NULL REFERENCES records(id), child TEXT NOT NULL REFERENCES records(id),
              ordinal INTEGER NOT NULL, PRIMARY KEY(parent,child));
            CREATE TABLE IF NOT EXISTS facts (
              id TEXT PRIMARY KEY, sid TEXT NOT NULL, category TEXT NOT NULL, subject TEXT NOT NULL,
              content TEXT NOT NULL, reason TEXT NOT NULL, scenario TEXT NOT NULL, tags TEXT NOT NULL,
              relations TEXT NOT NULL, sources TEXT NOT NULL, fingerprint TEXT NOT NULL,
              deleted INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 1,
              audited REAL NOT NULL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS fact_identity ON facts(sid,subject,fingerprint,deleted);
            CREATE INDEX IF NOT EXISTS fact_subject ON facts(sid,subject,category,deleted);
            CREATE TABLE IF NOT EXISTS versions (
              id INTEGER PRIMARY KEY, kind TEXT NOT NULL, target TEXT NOT NULL,
              snapshot TEXT NOT NULL, reason TEXT NOT NULL, created REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS jobs (
              id TEXT PRIMARY KEY, kind TEXT NOT NULL, sid TEXT NOT NULL, state TEXT NOT NULL,
              detail TEXT NOT NULL DEFAULT '', created REAL NOT NULL, updated REAL NOT NULL);
            CREATE UNIQUE INDEX IF NOT EXISTS job_unique ON jobs(kind,sid) WHERE state IN ('queued','running');
            CREATE TABLE IF NOT EXISTS vectors (
              id TEXT PRIMARY KEY REFERENCES records(id), model TEXT NOT NULL,
              revision INTEGER NOT NULL, vector TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS migration_items (
              source TEXT NOT NULL, source_key TEXT NOT NULL, digest TEXT NOT NULL,
              record_id TEXT REFERENCES records(id), reason TEXT NOT NULL,
              file_hash TEXT NOT NULL, metadata TEXT NOT NULL, created REAL NOT NULL,
              PRIMARY KEY(source,source_key,digest));
            CREATE TABLE IF NOT EXISTS migration_reports (source TEXT PRIMARY KEY, report TEXT NOT NULL);
            INSERT OR IGNORE INTO meta VALUES ('revision',0);
            CREATE TABLE IF NOT EXISTS entities (
              id TEXT PRIMARY KEY, kind TEXT NOT NULL, name TEXT NOT NULL,
              revision INTEGER NOT NULL DEFAULT 1, updated REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS entity_names (
              id INTEGER PRIMARY KEY AUTOINCREMENT, entity_id TEXT NOT NULL REFERENCES entities(id),
              name TEXT NOT NULL, source TEXT NOT NULL, context TEXT NOT NULL,
              observed REAL NOT NULL, reason TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS name_entity ON entity_names(entity_id,observed DESC);
            """)
            if "visibility" not in {
                r[1] for r in db.execute("PRAGMA table_info(records)")
            }:
                db.execute(
                    "ALTER TABLE records ADD COLUMN visibility TEXT NOT NULL DEFAULT 'session'"
                )
            db.execute(
                "UPDATE jobs SET state='queued',detail='resumed after restart' WHERE state='running'"
            )
            db.execute("PRAGMA user_version=4")
            db.execute(
                "INSERT OR IGNORE INTO entities(id,kind,name,updated) SELECT DISTINCT value,'user','',0 FROM records,json_each(records.users)"
            )
            db.execute(
                "INSERT OR IGNORE INTO entities(id,kind,name,updated) SELECT DISTINCT sid,'session','',0 FROM records"
            )
            db.execute(
                "INSERT OR IGNORE INTO entities(id,kind,name,updated) SELECT DISTINCT subject,'user','',0 FROM facts WHERE subject LIKE 'legacy:%'"
            )

    @staticmethod
    def bump(db):
        db.execute("UPDATE meta SET value=value+1 WHERE key='revision'")

    @staticmethod
    def row(row):
        if row is None:
            return None
        result = dict(row)
        for key in ("users", "tags", "relations", "sources"):
            if key in result:
                result[key] = json.loads(result[key])
        if "relations" in result:
            result["relation_warnings"] = [
                {"relation": r, "reason": relation_issue(r)}
                for r in result["relations"]
                if relation_issue(r)
            ]
            result["verified_relations"] = [
                r for r in result["relations"] if not relation_issue(r)
            ]
        return result

    def observe_name(
        self,
        entity_id,
        name,
        kind="user",
        source="adapter",
        context="",
        observed=None,
        revision=None,
        reason="",
    ):
        name = str(name or "").strip()
        if not name or len(name) > 500 or any(ord(c) < 32 for c in name):
            return False
        observed = time.time() if observed is None else observed
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute(
                "SELECT * FROM entities WHERE id=?", (entity_id,)
            ).fetchone()
            if revision is not None and (not old or old["revision"] != revision):
                raise Conflict("name changed")
            if old and old["name"] == name and revision is None:
                if observed > old["updated"]:
                    db.execute(
                        "UPDATE entities SET updated=? WHERE id=?",
                        (observed, entity_id),
                    )
                return False
            if old and observed < old["updated"]:
                return False
            db.execute(
                """INSERT INTO entities(id,kind,name,updated) VALUES (?,?,?,?)
              ON CONFLICT(id) DO UPDATE SET name=excluded.name,revision=entities.revision+1,updated=excluded.updated""",
                (entity_id, kind, name, observed),
            )
            db.execute(
                "INSERT INTO entity_names(entity_id,name,source,context,observed,reason) VALUES (?,?,?,?,?,?)",
                (entity_id, name, source, context, observed, reason),
            )
            self.bump(db)
            return True

    def entities(self, query="", ids=None, limit=100, offset=0):
        clauses, args = [], []
        if ids is not None:
            clauses.append("e.id IN (SELECT value FROM json_each(?))")
            args.append(dump(list(ids)))
        if query:
            clauses.append(
                "(instr(lower(e.id),lower(?))>0 OR EXISTS (SELECT 1 FROM entity_names n WHERE n.entity_id=e.id AND instr(lower(n.name),lower(?))>0))"
            )
            args.extend([query, query])
        with self.connect() as db:
            rows = [
                dict(r)
                for r in db.execute(
                    "SELECT e.* FROM entities e"
                    + (" WHERE " + " AND ".join(clauses) if clauses else "")
                    + " ORDER BY e.updated DESC,e.id LIMIT ? OFFSET ?",
                    [*args, limit, offset],
                )
            ]
            for r in rows:
                r.update(identity_info(r["id"]))
                r["history"] = [
                    dict(n)
                    for n in db.execute(
                        "SELECT name,source,context,observed,reason FROM entity_names WHERE entity_id=? ORDER BY observed DESC,id DESC LIMIT 50",
                        (r["id"],),
                    )
                ]
                if not r["name"] and r["lookup_id"] and r["lookup_id"] != r["id"]:
                    linked = db.execute(
                        "SELECT name FROM entities WHERE id=?", (r["lookup_id"],)
                    ).fetchone()
                    if linked and linked[0]:
                        r["label"] = linked[0] + " · 同号码档案"
                        r["name_source_id"] = r["lookup_id"]
            return rows

    def entity_ids(self, sid, users=(), scope="session"):
        # Resolve identities from the same scope predicate, independently of pagination.
        with self.connect() as db:
            if scope == "global":
                return [r[0] for r in db.execute("SELECT id FROM entities")]
            rows = [
                self.row(r)
                for r in db.execute(
                    """SELECT sid,users FROM records WHERE deleted=0
              AND (sid=? OR visibility='global' OR ((visibility='user' OR ?='linked') AND EXISTS
                (SELECT 1 FROM json_each(records.users) WHERE value IN (SELECT value FROM json_each(?)))))""",
                    (sid, scope, dump(list(users))),
                )
            ]
            ids = {sid, *users}
            for row in rows:
                ids.update(row["users"])
                ids.add(row["sid"])
            return sorted(ids)

    def can_schedule(self, kind, sid):
        with self.connect() as db:
            row = db.execute(
                "SELECT state,updated FROM jobs WHERE kind=? AND sid=? ORDER BY created DESC LIMIT 1",
                (kind, sid),
            ).fetchone()
            return (
                not row
                or row["state"] != "failed"
                or time.time() - row["updated"] >= 300
            )

    def capture(self, sid, event_key, messages):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._ensure_entities(db, sid, {u for m in messages for u in m["users"]})
            pos = db.execute(
                "SELECT coalesce(max(position),0) FROM records WHERE sid=?", (sid,)
            ).fetchone()[0]
            for index, msg in enumerate(messages):
                pos += 1
                db.execute(
                    """INSERT OR IGNORE INTO records
                  (id,sid,role,level,start,end,summary,content,users,position,event_key,created)
                  VALUES (?,?,?,0,?,?,?,?,?,?,?,?)""",
                    (
                        uid(),
                        sid,
                        msg["role"],
                        msg["time"],
                        msg["time"],
                        # The summary is what reaches the model; content keeps the full text.
                        msg.get("summary") or msg["content"],
                        msg["content"],
                        dump(msg["users"]),
                        pos,
                        f"{sid}:{event_key}:{index}",
                        time.time(),
                    ),
                )
            self.bump(db)

    @staticmethod
    def _ensure_entities(db, sid, users):
        db.execute(
            "INSERT OR IGNORE INTO entities(id,kind,name,updated) VALUES (?,'session','',0)",
            (sid,),
        )
        db.executemany(
            "INSERT OR IGNORE INTO entities(id,kind,name,updated) VALUES (?,'user','',0)",
            [(u,) for u in users],
        )

    def active(self, sid):
        with self.connect() as db:
            return [
                self.row(r)
                for r in db.execute(
                    """SELECT * FROM records WHERE sid=? AND active=1 AND deleted=0
              ORDER BY permanent DESC,level DESC,position,id""",
                    (sid,),
                )
            ]

    def context(self, sid, users=(), scope="session"):
        with self.connect() as db:
            return [
                self.row(r)
                for r in db.execute(
                    """SELECT * FROM records WHERE active=1 AND deleted=0 AND
                (sid=? OR visibility='global' OR (?='global' AND permanent=1) OR ((visibility='user' OR ?='linked') AND EXISTS
                  (SELECT 1 FROM json_each(records.users) WHERE value IN (SELECT value FROM json_each(?)))))
                ORDER BY permanent DESC,level DESC,position,id""",
                    (sid, scope, scope, dump(list(users))),
                )
            ]

    def import_legacy(self, snapshot):
        from .migration import import_snapshot

        return import_snapshot(self, snapshot)

    def scan_legacy(self, root, plugin_id, limit, adapters=()):
        from .migration import snapshot
        from .identity import Resolver

        with self.connect() as db:
            entities = [
                (r["id"], r["kind"]) for r in db.execute("SELECT id,kind FROM entities")
            ]
        return snapshot(root, plugin_id, limit, Resolver(entities, adapters))

    def synthetic_identity(self):
        """True while any row still carries a synthetic migration identifier."""
        with self.connect() as db:
            for table, column in (
                ("entities", "id"),
                ("records", "sid"),
                ("facts", "sid"),
                ("facts", "subject"),
            ):
                if db.execute(
                    f"SELECT 1 FROM {table} WHERE {column} LIKE 'legacy:%' "
                    f"OR {column} LIKE 'unresolved:%' LIMIT 1"
                ).fetchone():
                    return True
            return False

    def canonicalize_identity(self, adapters=(), bindings=None):
        """Merge synthetic migration identifiers onto the live same-number entity.

        Idempotent: rows that already carry a live identifier are untouched.
        ``bindings`` forces a mapping, e.g. after an adapter lookup proved which
        platform an ``unresolved:user:123`` placeholder belongs to.
        """
        from . import identity

        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            entities = [
                (r["id"], r["kind"])
                for r in db.execute("SELECT id,kind FROM entities")
            ]
            resolver = identity.Resolver(entities, adapters)
            mapping = {}

            def note(value):
                if identity.synthetic(value):
                    mapping[value] = resolver.resolve(value)

            for entity_id, _ in entities:
                note(entity_id)
            records = [dict(r) for r in db.execute("SELECT id,sid,users FROM records")]
            for record in records:
                note(record["sid"])
                for user in json.loads(record["users"]):
                    note(user)
            facts = [
                dict(r)
                for r in db.execute(
                    "SELECT id,sid,subject,category,content,reason,scenario,"
                    "tags,relations,sources,fingerprint FROM facts WHERE deleted=0"
                )
            ]
            for fact in facts:
                note(fact["sid"])
                note(fact["subject"])
                for relation in json.loads(fact["relations"]):
                    note(relation.get("subject"))
                    note(relation.get("object"))
            if bindings:
                for old, value in bindings.items():
                    mapping[old] = value
            record_sid = {record["id"]: record["sid"] for record in records}
            overrides = {}
            for row in db.execute(
                "SELECT record_id,source_key FROM migration_items "
                "WHERE record_id IS NOT NULL"
            ):
                shape = identity.source_shape(row["source_key"])
                if not shape:
                    continue
                kind, adapter, number = shape
                hint = ""
                sid = record_sid.get(row["record_id"], "")
                if sid and not identity.synthetic(sid) and sid != identity.UNSCOPED:
                    name, _number, session = identity.split_adapter(sid)
                    if name and session:
                        hint = name
                if kind == "global":
                    overrides[row["record_id"]] = (
                        identity.GLOBAL,
                        identity.GLOBAL,
                        "global",
                    )
                elif kind == "self":
                    overrides[row["record_id"]] = (
                        identity.SELF,
                        identity.SELF,
                        "self",
                    )
                elif kind == "user":
                    overrides[row["record_id"]] = resolver.user(
                        adapter or hint, number
                    )
                else:
                    overrides[row["record_id"]] = resolver.group(
                        adapter or hint, number
                    )
            report = {
                "records": 0,
                "facts": 0,
                "entities": 0,
                "merged_facts": 0,
                "pending": [],
                "ambiguous": [],
            }
            record_new_sid = {}
            for record in records:
                sid, users = record["sid"], json.loads(record["users"])
                new_sid = mapping.get(sid, (sid, sid, ""))[1] or sid
                new_users = [mapping.get(u, (u, "", ""))[0] for u in users]
                override = overrides.get(record["id"])
                if override:
                    entity, entity_sid, kind = override
                    if kind == "user":
                        if entity_sid and entity_sid != identity.UNSCOPED:
                            new_sid = entity_sid
                        if entity and entity not in new_users:
                            new_users.append(entity)
                    else:
                        new_sid = entity_sid
                new_users = sorted(
                    {
                        user
                        for user in new_users
                        if user and not user.startswith(identity.PENDING)
                    }
                )
                record_new_sid[record["id"]] = new_sid
                if new_sid != sid or new_users != users:
                    db.execute(
                        "UPDATE records SET sid=?,users=? WHERE id=?",
                        (new_sid, dump(new_users), record["id"]),
                    )
                    report["records"] += 1
            for fact in facts:
                sid, subject = fact["sid"], fact["subject"]
                new_sid = mapping.get(sid, (sid, sid, ""))[1] or sid
                if identity.synthetic(sid) or sid == identity.UNSCOPED:
                    for source in json.loads(fact["sources"]):
                        candidate = record_new_sid.get(source)
                        if candidate and candidate != identity.UNSCOPED:
                            new_sid = candidate
                            break
                new_subject = mapping.get(subject, (subject, "", ""))[0]
                relations = json.loads(fact["relations"])
                new_relations = [
                    {
                        **relation,
                        "subject": mapping.get(
                            relation.get("subject"),
                            (relation.get("subject"), "", ""),
                        )[0],
                        "object": mapping.get(
                            relation.get("object"), (relation.get("object"), "", "")
                        )[0],
                    }
                    for relation in relations
                ]
                fingerprint = hashlib.sha256(
                    dump(
                        [
                            fact["category"],
                            new_subject,
                            fact["content"],
                            fact["reason"],
                            fact["scenario"],
                            new_relations,
                        ]
                    ).encode()
                ).hexdigest()
                if (
                    new_sid != sid
                    or new_subject != subject
                    or new_relations != relations
                    or fingerprint != fact["fingerprint"]
                ):
                    db.execute(
                        "UPDATE facts SET sid=?,subject=?,relations=?,fingerprint=?,"
                        "revision=revision+1 WHERE id=?",
                        (
                            new_sid,
                            new_subject,
                            dump(new_relations),
                            fingerprint,
                            fact["id"],
                        ),
                    )
                    report["facts"] += 1
            for old, (entity, _sid, kind) in mapping.items():
                if old == entity:
                    continue
                db.execute(
                    "INSERT OR IGNORE INTO entities(id,kind,name,updated) "
                    "VALUES (?,?, '',0)",
                    (entity, kind or "user"),
                )
                if kind:
                    db.execute(
                        "UPDATE entities SET kind=? WHERE id=?", (kind, entity)
                    )
                db.execute(
                    "UPDATE entity_names SET entity_id=? WHERE entity_id=?",
                    (entity, old),
                )
                db.execute("DELETE FROM entities WHERE id=?", (old,))
                latest = db.execute(
                    "SELECT name FROM entity_names WHERE entity_id=? "
                    "ORDER BY observed DESC,id DESC LIMIT 1",
                    (entity,),
                ).fetchone()
                if latest and latest[0]:
                    db.execute(
                        "UPDATE entities SET name=? WHERE id=? "
                        "AND (name IS NULL OR name='')",
                        (latest[0], entity),
                    )
                report["entities"] += 1
            seen = {}
            for row in db.execute(
                "SELECT id,sid,subject,fingerprint,sources,tags FROM facts "
                "WHERE deleted=0 ORDER BY rowid"
            ):
                key = (row["sid"], row["subject"], row["fingerprint"])
                kept = seen.get(key)
                if kept is None:
                    seen[key] = {
                        "id": row["id"],
                        "sources": row["sources"],
                        "tags": row["tags"],
                    }
                    continue
                sources = sorted(
                    set(json.loads(kept["sources"]) + json.loads(row["sources"]))
                )
                tags = sorted(set(json.loads(kept["tags"]) + json.loads(row["tags"])))
                db.execute(
                    "UPDATE facts SET sources=?,tags=?,revision=revision+1 WHERE id=?",
                    (dump(sources), dump(tags), kept["id"]),
                )
                db.execute(
                    "INSERT INTO versions(kind,target,snapshot,reason,created) "
                    "VALUES ('fact',?,?,?,?)",
                    (
                        row["id"],
                        dump(dict(row)),
                        "identity merge: identical fact",
                        time.time(),
                    ),
                )
                db.execute(
                    "UPDATE facts SET deleted=1,revision=revision+1 WHERE id=?",
                    (row["id"],),
                )
                kept["sources"], kept["tags"] = dump(sources), dump(tags)
                report["merged_facts"] += 1
            pending = sorted(
                {
                    entity
                    for _old, (entity, _sid, _kind) in mapping.items()
                    if entity.startswith(identity.PENDING)
                }
            )
            ambiguous = []
            for old, (entity, _sid, _kind) in mapping.items():
                if not entity.startswith(identity.PENDING):
                    continue
                shape = identity.legacy_shape(old) or identity.pending_shape(old)
                if not shape:
                    continue
                pool = resolver.users if shape[0] == "user" else resolver.groups
                if len(pool.get(shape[2], ())) > 1:
                    ambiguous.append(entity)
            report["pending"] = pending
            report["ambiguous"] = sorted(set(ambiguous))
            if any(
                report[key] for key in ("records", "facts", "entities", "merged_facts")
            ):
                self.bump(db)
            return report

    def refreshable_names(self, limit=200):
        """Entities that have a number but no current name, newest first."""
        from . import identity

        result = []
        with self.connect() as db:
            rows = db.execute(
                "SELECT id FROM entities WHERE (name IS NULL OR name='') "
                "ORDER BY updated DESC,id LIMIT ?",
                (max(limit * 2, 400),),
            )
            for (entity_id,) in rows:
                if entity_id in (identity.GLOBAL, identity.SELF, identity.UNSCOPED):
                    continue
                _adapter, number, _session = identity.split_adapter(entity_id)
                if not number:
                    shape = identity.pending_shape(entity_id) or identity.legacy_shape(
                        entity_id
                    )
                    number = shape[2] if shape else ""
                if number:
                    result.append(entity_id)
                if len(result) >= limit:
                    break
        return result

    @staticmethod
    def _scope_clause(alias, scope, sid, users):
        """Visibility predicate shared by totals and top-user ranking."""
        if scope == "global":
            return "1=1", []
        if scope == "linked":
            return (
                f"({alias}.sid=? OR {alias}.visibility='global' OR EXISTS "
                f"(SELECT 1 FROM json_each({alias}.users) WHERE value IN "
                "(SELECT value FROM json_each(?))))",
                [sid, dump(list(users))],
            )
        return (
            f"({alias}.sid=? OR {alias}.visibility='global' OR "
            f"({alias}.visibility='user' AND EXISTS "
            f"(SELECT 1 FROM json_each({alias}.users) WHERE value IN "
            "(SELECT value FROM json_each(?)))))",
            [sid, dump(list(users))],
        )

    def totals(self, sid="", users=(), scope="session"):
        clause, args = self._scope_clause("records", scope, sid, users)
        with self.connect() as db:
            row = db.execute(
                "SELECT count(*) AS records, coalesce(sum(permanent),0) AS permanent, "
                f"count(DISTINCT sid) AS sessions FROM records WHERE deleted=0 AND {clause}",
                args,
            ).fetchone()
            people = db.execute(
                "SELECT count(DISTINCT value) FROM records, json_each(records.users) "
                f"WHERE records.deleted=0 AND {clause}",
                args,
            ).fetchone()[0]
            groups = db.execute(
                f"SELECT count(DISTINCT sid) FROM records WHERE deleted=0 AND {clause} "
                "AND instr(sid,':gm:')>0",
                args,
            ).fetchone()[0]
            source_clause, source_args = self._scope_clause("r", scope, sid, users)
            facts = db.execute(
                f"""SELECT count(*) FROM facts WHERE deleted=0 AND (
                  sid IN (SELECT sid FROM records WHERE deleted=0 AND {clause})
                  OR EXISTS(SELECT 1 FROM json_each(facts.sources) v JOIN records r
                            ON r.id=v.value WHERE r.deleted=0 AND {source_clause}))""",
                [*args, *source_args],
            ).fetchone()[0]
        return {
            "records": row["records"],
            "facts": facts,
            "users": people,
            "sessions": row["sessions"],
            "groups": groups,
            "permanent": row["permanent"],
        }

    def needs_tool_cleanup(self):
        with self.connect() as db:
            return not db.execute(
                "SELECT 1 FROM meta WHERE key='tools_cleanup'"
            ).fetchone()

    def cleanup_tool_records(self):
        """One-time repair for legacy tool payloads that flood the context.

        Self-recall echoes are soft-deleted (raw text stays in the database);
        other tool records keep their content but gain a readable summary.
        Idempotent: after the first pass nothing matches any more.
        """
        from .retrieval import (
            TOOL_RESULT_PREFIX,
            looks_like_memory_payload,
            tool_call_summary,
            tool_preview,
        )

        report = {"scanned": 0, "removed": 0, "rewritten": 0, "freed_chars": 0}
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute(
                "SELECT id,sid,role,summary,content FROM records WHERE deleted=0 AND "
                "(summary LIKE ? OR summary LIKE ?)",
                (TOOL_RESULT_PREFIX + "%", '%{"tool_calls"%'),
            ).fetchall()
            for row in rows:
                report["scanned"] += 1
                summary, content = row["summary"], row["content"]
                if looks_like_memory_payload(
                    summary
                ) or looks_like_memory_payload(content):
                    db.execute(
                        "INSERT INTO versions(kind,target,snapshot,reason,created) "
                        "VALUES ('record',?,?,?,?)",
                        (
                            row["id"],
                            dump(dict(row)),
                            "清理自身记忆读取回声",
                            time.time(),
                        ),
                    )
                    db.execute(
                        "UPDATE records SET deleted=1,revision=revision+1 WHERE id=?",
                        (row["id"],),
                    )
                    self._orphan_facts(db, row["id"])
                    report["removed"] += 1
                    report["freed_chars"] += len(summary)
                    continue
                if summary.startswith(TOOL_RESULT_PREFIX):
                    if len(summary) <= 400:
                        continue
                    new_summary = TOOL_RESULT_PREFIX + tool_preview(
                        summary[len(TOOL_RESULT_PREFIX) :]
                    )
                else:
                    text_part, _, blob = summary.partition("\n{")
                    label = ""
                    try:
                        label = tool_call_summary(
                            json.loads("{" + blob).get("tool_calls", [])
                        )
                    except (ValueError, TypeError, AttributeError):
                        label = ""
                    new_summary = (
                        (text_part.strip() + " ") if text_part.strip() else ""
                    ) + (label or "[调用工具]")
                if new_summary == summary:
                    continue
                db.execute(
                    "INSERT INTO versions(kind,target,snapshot,reason,created) "
                    "VALUES ('record',?,?,?,?)",
                    (row["id"], dump(dict(row)), "工具记录摘要压缩", time.time()),
                )
                db.execute(
                    "UPDATE records SET summary=?,revision=revision+1 WHERE id=?",
                    (new_summary, row["id"]),
                )
                report["rewritten"] += 1
                report["freed_chars"] += max(0, len(summary) - len(new_summary))
            db.execute("INSERT OR REPLACE INTO meta VALUES ('tools_cleanup',1)")
            self.bump(db)
        return report

    def migration_status(self):
        with self.connect() as db:
            return {
                "reports": [
                    json.loads(r[0])
                    for r in db.execute(
                        "SELECT report FROM migration_reports ORDER BY source"
                    )
                ],
                "skips": [
                    dict(r)
                    for r in db.execute(
                        "SELECT source,reason,count(*) AS count FROM migration_items WHERE reason<>'' GROUP BY source,reason"
                    )
                ],
                "total_imported": db.execute(
                    "SELECT count(DISTINCT record_id) FROM migration_items"
                ).fetchone()[0],
            }

    def sessions(self):
        with self.connect() as db:
            return [
                r[0]
                for r in db.execute(
                    "SELECT DISTINCT sid FROM records WHERE active=1 AND deleted=0"
                )
            ]

    def get(self, record_id):
        with self.connect() as db:
            result = self.row(
                db.execute(
                    "SELECT * FROM records WHERE id=? AND deleted=0", (record_id,)
                ).fetchone()
            )
            if result:
                result["legacy_sources"] = [
                    dict(r)
                    for r in db.execute(
                        "SELECT source,source_key,digest,file_hash,metadata FROM migration_items WHERE record_id=?",
                        (record_id,),
                    )
                ]
                result["children"] = [
                    r[0]
                    for r in db.execute(
                        "SELECT child FROM edges WHERE parent=? ORDER BY ordinal",
                        (record_id,),
                    )
                ]
                result["parents"] = [
                    r[0]
                    for r in db.execute(
                        "SELECT parent FROM edges WHERE child=?", (record_id,)
                    )
                ]
                result["versions"] = [
                    dict(r)
                    for r in db.execute(
                        "SELECT * FROM versions WHERE kind='record' AND target=? ORDER BY id DESC",
                        (record_id,),
                    )
                ]
            return result

    def _add_fact(self, db, sid, fact):
        fingerprint = hashlib.sha256(
            dump(
                [
                    fact[k]
                    for k in (
                        "category",
                        "subject",
                        "content",
                        "reason",
                        "scenario",
                        "relations",
                    )
                ]
            ).encode()
        ).hexdigest()
        old = db.execute(
            "SELECT * FROM facts WHERE sid=? AND subject=? AND fingerprint=? AND deleted=0",
            (sid, fact["subject"], fingerprint),
        ).fetchone()
        if old:
            sources = sorted(set(json.loads(old["sources"]) + fact["source_ids"]))
            tags = sorted(set(json.loads(old["tags"]) + fact["tags"]))
            db.execute(
                "UPDATE facts SET sources=?,tags=?,revision=revision+1 WHERE id=?",
                (dump(sources), dump(tags), old["id"]),
            )
            return old["id"]
        new_id = uid()
        db.execute(
            """INSERT INTO facts(id,sid,category,subject,content,reason,scenario,tags,relations,sources,fingerprint)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                new_id,
                sid,
                fact["category"],
                fact["subject"],
                fact["content"],
                fact["reason"],
                fact["scenario"],
                dump(fact["tags"]),
                dump(fact["relations"]),
                dump(fact["source_ids"]),
                fingerprint,
            ),
        )
        return new_id

    def compress(self, sid, candidates, level, output):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for row in candidates:
                current = db.execute(
                    "SELECT revision,active,deleted FROM records WHERE id=? AND sid=?",
                    (row["id"], sid),
                ).fetchone()
                if not current or tuple(current) != (row["revision"], 1, 0):
                    raise Conflict("source changed during compression")
            ids = {r["id"] for r in candidates}
            if any(not set(f["source_ids"]) <= ids for f in output["facts"]):
                raise ValueError("unknown source id")
            start, end = (
                min(r["start"] for r in candidates),
                max(r["end"] for r in candidates),
            )
            archive_id = f"{level}-{int(start * 1000)}-{int(end * 1000)}-{uid()[:12]}"
            content = dump(
                [
                    {
                        "id": r["id"],
                        "role": r["role"],
                        "level": r["level"],
                        "content": r["summary"],
                    }
                    for r in candidates
                ]
            )
            users = sorted({u for r in candidates for u in r["users"]})
            db.execute(
                """INSERT INTO records(id,sid,role,level,start,end,summary,content,users,position,created)
               VALUES (?,?,'assistant',?,?,?,?,?,?,?,?)""",
                (
                    archive_id,
                    sid,
                    level,
                    start,
                    end,
                    output["summary"],
                    content,
                    dump(users),
                    min(r["position"] for r in candidates),
                    time.time(),
                ),
            )
            visibility = {r.get("visibility", "session") for r in candidates}
            if len(visibility) != 1:
                raise ValueError("mixed visibility cannot be compressed")
            db.execute(
                "UPDATE records SET visibility=? WHERE id=?",
                (visibility.pop(), archive_id),
            )
            for index, row in enumerate(candidates):
                db.execute(
                    "INSERT INTO edges VALUES (?,?,?)", (archive_id, row["id"], index)
                )
                db.execute(
                    "UPDATE records SET active=0,revision=revision+1 WHERE id=?",
                    (row["id"],),
                )
                db.execute(
                    "UPDATE vectors SET revision=revision+1 WHERE id=?", (row["id"],)
                )
            for fact in output["facts"]:
                self._add_fact(db, sid, fact)
            self.bump(db)
            return archive_id

    def memorize(self, sid, content, users, start, end):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._ensure_entities(db, sid, users)
            record_id = f"100-{int(start * 1000)}-{int(end * 1000)}-{uid()[:12]}"
            db.execute(
                """INSERT INTO records(id,sid,role,level,start,end,summary,content,users,permanent,position,created)
              VALUES (?,?,'assistant',100,?,?,?,?,?,1,0,?)""",
                (
                    record_id,
                    sid,
                    start,
                    end,
                    content,
                    content,
                    dump(users),
                    time.time(),
                ),
            )
            self.bump(db)
            return record_id

    def find_permanent(self, sid, content):
        """Exact (normalised) duplicate among this session's permanent memories."""
        from .retrieval import normalize_text

        target = normalize_text(content)
        with self.connect() as db:
            for row in db.execute(
                "SELECT * FROM records WHERE sid=? AND permanent=1 AND deleted=0 "
                "ORDER BY end DESC,id",
                (sid,),
            ):
                if normalize_text(row["content"]) == target:
                    return self.row(row)
        return None

    def permanent_records(self, sid):
        """Active permanent memories of one session, newest first."""
        with self.connect() as db:
            return [
                self.row(row)
                for row in db.execute(
                    "SELECT * FROM records WHERE sid=? AND permanent=1 AND deleted=0 "
                    "AND active=1 ORDER BY end DESC,id",
                    (sid,),
                )
            ]

    def sessions_with_permanents(self):
        with self.connect() as db:
            return [
                row[0]
                for row in db.execute(
                    "SELECT sid FROM records WHERE permanent=1 AND deleted=0 "
                    "AND active=1 GROUP BY sid HAVING count(*)>1"
                )
            ]

    def merge_records(self, target_id, source_ids, content, reason):
        """Fold similar permanent memories into the newest one; originals stay."""
        ids = list(dict.fromkeys(source_ids))
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = {}
            for record_id in ids:
                row = db.execute(
                    "SELECT * FROM records WHERE id=? AND deleted=0", (record_id,)
                ).fetchone()
                if not row or not row["permanent"]:
                    raise ValueError("invalid merge source")
                rows[record_id] = row
            if target_id not in rows:
                raise ValueError("invalid merge target")
            sid = rows[target_id]["sid"]
            if any(row["sid"] != sid for row in rows.values()):
                raise ValueError("cross-session merge is forbidden")
            for record_id, row in rows.items():
                db.execute(
                    "INSERT INTO versions(kind,target,snapshot,reason,created) "
                    "VALUES ('record',?,?,?,?)",
                    (record_id, dump(dict(row)), reason, time.time()),
                )
            # Summary is what the model sees; content keeps the archive text.
            db.execute(
                "UPDATE records SET summary=?,revision=revision+1 WHERE id=?",
                (content, target_id),
            )
            for record_id in ids:
                if record_id == target_id:
                    continue
                db.execute(
                    "UPDATE records SET active=0,revision=revision+1 WHERE id=?",
                    (record_id,),
                )
                db.execute("DELETE FROM vectors WHERE id=?", (record_id,))
            self.bump(db)
        return {"target": target_id, "folded": len(ids)}

    def edit(self, kind, target, revision, patch, reason):
        table = "records" if kind == "record" else "facts"
        allowed = (
            {"summary", "active", "deleted"}
            if kind == "record"
            else {
                "content",
                "category",
                "subject",
                "reason",
                "scenario",
                "tags",
                "relations",
                "deleted",
            }
        )
        if not patch or not set(patch) <= allowed:
            raise ValueError("invalid editable fields")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute(
                f"SELECT * FROM {table} WHERE id=? AND deleted=0", (target,)
            ).fetchone()
            if not old or old["revision"] != revision:
                raise Conflict("record changed; reload before saving")
            if kind == "record" and "active" in patch and not old["permanent"]:
                raise ValueError(
                    "only permanent memories can leave/rejoin context manually"
                )
            db.execute(
                "INSERT INTO versions(kind,target,snapshot,reason,created) VALUES (?,?,?,?,?)",
                (kind, target, dump(dict(old)), reason, time.time()),
            )
            values = [dump(v) if isinstance(v, list) else v for v in patch.values()]
            db.execute(
                f"UPDATE {table} SET {','.join(k + '=?' for k in patch)},revision=revision+1 WHERE id=?",
                (*values, target),
            )
            if kind == "fact":
                db.execute("UPDATE facts SET fingerprint=? WHERE id=?", (uid(), target))
            else:
                db.execute("DELETE FROM vectors WHERE id=?", (target,))
                if patch.get("deleted"):
                    self._orphan_facts(db, target)
            self.bump(db)

    @staticmethod
    def _orphan_facts(db, record_id):
        """Derived facts with no remaining live evidence must not be recalled."""
        for fact in db.execute(
            """SELECT * FROM facts WHERE deleted=0
              AND EXISTS(SELECT 1 FROM json_each(facts.sources) WHERE value=?)
              AND NOT EXISTS(SELECT 1 FROM records,json_each(facts.sources) src
                             WHERE records.id=src.value AND records.deleted=0)""",
            (record_id,),
        ).fetchall():
            db.execute(
                "INSERT INTO versions(kind,target,snapshot,reason,created) "
                "VALUES ('fact',?,?,?,?)",
                (fact["id"], dump(dict(fact)), "source deleted", time.time()),
            )
            db.execute(
                "UPDATE facts SET deleted=1,revision=revision+1 WHERE id=?",
                (fact["id"],),
            )

    def search(
        self,
        sid="",
        keyword="",
        level=None,
        start=None,
        end=None,
        offset=0,
        limit=30,
        subject="",
        scope="session",
        users=(),
        active=False,
        vector=None,
        model="",
        lexical="",
        exclude_sid="",
        exclude_ids=(),
        prefer_sid="",
        prefer_users=(),
    ):
        clauses, args = ["deleted=0"], []
        if exclude_ids:
            clauses.append("id NOT IN (SELECT value FROM json_each(?))")
            args.append(dump(list(exclude_ids)))
        if exclude_sid:
            clauses.append("sid<>?")
            args.append(exclude_sid)
        if scope == "session":
            clauses.append(
                "(sid=? OR visibility='global' OR (visibility='user' AND EXISTS (SELECT 1 FROM json_each(records.users) WHERE value IN (SELECT value FROM json_each(?)))) )"
            )
            args.extend([sid, dump(list(users))])
        elif scope == "linked":
            clauses.append(
                "(sid=? OR visibility='global' OR EXISTS (SELECT 1 FROM json_each(records.users) WHERE value IN (SELECT value FROM json_each(?))))"
            )
            args.extend([sid, dump(list(users))])
        if keyword:
            clauses.append("""(instr(lower(summary),lower(?))>0 OR EXISTS
              (SELECT 1 FROM entity_names n WHERE instr(lower(n.name),lower(?))>0
              AND (n.entity_id=records.sid OR n.entity_id IN (SELECT value FROM json_each(records.users)))))""")
            args.extend([keyword, keyword])
        if level is not None:
            clauses.append("level=?")
            args.append(level)
        if start is not None:
            clauses.append("end>=?")
            args.append(start)
        if end is not None:
            clauses.append("start<=?")
            args.append(end)
        if active:
            clauses.append("active=1")
        if subject:
            clauses.append(
                "EXISTS (SELECT 1 FROM json_each(records.users) WHERE value=?)"
            )
            args.append(subject)
        tier_sql, tier_args = "", []
        if prefer_sid or prefer_users:
            # Session affinity only reorders; the recall scope stays untouched.
            tier_sql = (
                "CASE WHEN sid=? THEN 0 WHEN EXISTS(SELECT 1 FROM json_each(records.users) "
                "WHERE value IN (SELECT value FROM json_each(?))) THEN 1 "
                "WHEN visibility='global' THEN 2 ELSE 3 END, "
            )
            tier_args = [prefer_sid, dump(list(prefer_users))]
        with self.connect() as db:
            if lexical and not vector:
                from .retrieval import relevance

                db.create_function(
                    "lexical_score",
                    1,
                    lambda text: relevance(lexical, text),
                )
                clauses.append("lexical_score(summary)>0")
            where = " AND ".join(clauses)
            total = db.execute(
                "SELECT count(*) FROM records WHERE " + where, args
            ).fetchone()[0]
            if vector:
                norm = math.sqrt(sum(x * x for x in vector))

                def cosine(raw):
                    other = json.loads(raw)
                    if len(other) != len(vector):
                        return -1.0
                    denom = norm * math.sqrt(sum(x * x for x in other))
                    return (
                        sum(x * y for x, y in zip(vector, other)) / denom
                        if denom
                        else -1.0
                    )

                db.create_function("similarity", 1, cosine)
                sql = f"""SELECT records.*,coalesce((SELECT similarity(vector) FROM vectors WHERE vectors.id=records.id
                  AND vectors.model=? AND vectors.revision=records.revision),-1) AS score FROM records WHERE {where}
                  ORDER BY {tier_sql}score DESC,end,id LIMIT ? OFFSET ?"""
                rows = db.execute(sql, [model, *args, *tier_args, limit, offset])
            else:
                rows = db.execute(
                    "SELECT * FROM records WHERE "
                    + where
                    + (
                        f" ORDER BY {tier_sql}lexical_score(summary) DESC,end,id LIMIT ? OFFSET ?"
                        if lexical
                        else f" ORDER BY {tier_sql}end,id LIMIT ? OFFSET ?"
                    ),
                    [*args, *tier_args, limit, offset],
                )
            return {"total": total, "items": [self.row(r) for r in rows]}

    def facts(
        self,
        sid="",
        subject="",
        category="",
        limit=100,
        offset=0,
        global_scope=False,
        users=(),
        include_shared=False,
        lexical="",
        exclude_ids=(),
        prefer_sid="",
        prefer_users=(),
    ):
        where, args = ["deleted=0"], []
        if exclude_ids:
            where.append("id NOT IN (SELECT value FROM json_each(?))")
            args.append(dump(list(exclude_ids)))
        if not global_scope:
            if include_shared:
                where.append("""(sid=? OR EXISTS (SELECT 1 FROM records,json_each(facts.sources) AS src WHERE records.id=src.value
                  AND records.deleted=0 AND (records.visibility='global' OR (records.visibility='user' AND EXISTS
                  (SELECT 1 FROM json_each(records.users) WHERE value IN (SELECT value FROM json_each(?)))))))""")
                args.extend([sid, dump(list(users))])
            else:
                where.append("sid=?")
                args.append(sid)
        for key, value in [("subject", subject), ("category", category)]:
            if value:
                where.append(key + "=?")
                args.append(value)
        tier_sql, tier_args = "", []
        if prefer_sid or prefer_users:
            # Same session first, then facts about the current participants.
            tier_sql = (
                "CASE WHEN sid=? THEN 0 WHEN subject IN "
                "(SELECT value FROM json_each(?)) THEN 1 ELSE 2 END, "
            )
            tier_args = [prefer_sid, dump(list(prefer_users))]
        with self.connect() as db:
            if lexical:
                from .retrieval import relevance

                db.create_function(
                    "fact_score", 1, lambda text: relevance(lexical, text)
                )
                where.append("fact_score(content)>0")
            return [
                self.row(r)
                for r in db.execute(
                    "SELECT * FROM facts WHERE "
                    + " AND ".join(where)
                    + (
                        f" ORDER BY {tier_sql}fact_score(content) DESC,audited,id LIMIT ? OFFSET ?"
                        if lexical
                        else f" ORDER BY {tier_sql}audited,id LIMIT ? OFFSET ?"
                    ),
                    [*args, *tier_args, limit, offset],
                )
            ]

    def edit_history(self, kind, targets):
        with self.connect() as db:
            rows = db.execute(
                """SELECT target,reason,created FROM
              (SELECT target,reason,created,id,row_number() OVER (PARTITION BY target ORDER BY id DESC) AS n
               FROM versions WHERE kind=? AND target IN (SELECT value FROM json_each(?)))
              WHERE n<=5 ORDER BY id DESC""",
                (kind, dump(list(targets))),
            )
            result = {}
            for row in rows:
                result.setdefault(row["target"], []).append(dict(row))
            return result

    def classify(self, row, output):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            current = db.execute(
                "SELECT revision,deleted FROM records WHERE id=?", (row["id"],)
            ).fetchone()
            if not current or tuple(current) != (row["revision"], 0):
                raise Conflict("classification source changed")
            for fact in output["facts"]:
                if set(fact["source_ids"]) != {row["id"]}:
                    raise ValueError("unknown classification source")
                self._add_fact(db, row["sid"], fact)
            self.bump(db)

    def audit(self, candidates, output):
        by_id = {r["id"]: r for r in candidates}
        validate_audit(candidates, output)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for old in candidates:
                cur = db.execute(
                    "SELECT revision,deleted FROM facts WHERE id=?", (old["id"],)
                ).fetchone()
                if not cur or tuple(cur) != (old["revision"], 0):
                    raise Conflict("audit evidence changed")
            for a in output["actions"]:
                old = by_id[a["target_id"]]
                group = {a["target_id"], *a["source_ids"]}
                db.execute(
                    "INSERT INTO versions(kind,target,snapshot,reason,created) VALUES ('fact',?,?,?,?)",
                    (old["id"], dump(old), a["reason"], time.time()),
                )
                if a["action"] == "keep":
                    continue
                sources = sorted({s for k in group for s in by_id[k]["sources"]})
                relations = {
                    dump(rel): rel for k in group for rel in by_id[k]["relations"]
                }
                if a.get("relations") is not None:
                    relations = {dump(rel): rel for rel in a["relations"]}
                tags = sorted({tag for k in group for tag in by_id[k]["tags"]})
                db.execute(
                    "UPDATE facts SET content=?,sources=?,relations=?,tags=?,fingerprint=?,revision=revision+1 WHERE id=?",
                    (
                        a["content"],
                        dump(sources),
                        dump(list(relations.values())),
                        dump(tags),
                        uid(),
                        old["id"],
                    ),
                )
                if a["action"] == "merge":
                    for k in group - {old["id"]}:
                        db.execute(
                            "INSERT INTO versions(kind,target,snapshot,reason,created) VALUES ('fact',?,?,?,?)",
                            (k, dump(by_id[k]), a["reason"], time.time()),
                        )
                        db.execute(
                            "UPDATE facts SET deleted=1,revision=revision+1 WHERE id=?",
                            (k,),
                        )
            for old in candidates:
                db.execute(
                    "UPDATE facts SET audited=? WHERE id=?", (time.time(), old["id"])
                )
            self.bump(db)

    def set_vector(self, record_id, model, revision, vector):
        if not vector or any(
            not isinstance(x, (float, int)) or not math.isfinite(x) for x in vector
        ):
            raise ValueError("invalid embedding")
        with self.connect() as db:
            db.execute(
                "INSERT OR REPLACE INTO vectors VALUES (?,?,?,?)",
                (record_id, model, revision, dump(vector)),
            )

    def enqueue(self, kind, sid):
        with self.connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO jobs(id,kind,sid,state,created,updated) VALUES (?,?,?,'queued',?,?)",
                (uid(), kind, sid, time.time(), time.time()),
            )
            return db.execute(
                "SELECT id FROM jobs WHERE kind=? AND sid=? AND state IN ('queued','running')",
                (kind, sid),
            ).fetchone()[0]

    def claim(self):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM jobs WHERE state='queued' ORDER BY created LIMIT 1"
            ).fetchone()
            if row:
                db.execute(
                    "UPDATE jobs SET state='running',updated=? WHERE id=?",
                    (time.time(), row["id"]),
                )
            return dict(row) if row else None

    def finish(self, job, state, detail=""):
        with self.connect() as db:
            db.execute(
                "UPDATE jobs SET state=?,detail=?,updated=? WHERE id=?",
                (state, detail, time.time(), job),
            )
            self.bump(db)

    def status(self):
        with self.connect() as db:
            return {
                "revision": db.execute(
                    "SELECT value FROM meta WHERE key='revision'"
                ).fetchone()[0],
                "levels": [
                    dict(r)
                    for r in db.execute(
                        "SELECT level,count(*) AS count,sum(active) AS active FROM records WHERE deleted=0 GROUP BY level ORDER BY level"
                    )
                ],
                "records": db.execute(
                    "SELECT count(*) FROM records WHERE deleted=0"
                ).fetchone()[0],
                "facts": db.execute(
                    "SELECT count(*) FROM facts WHERE deleted=0"
                ).fetchone()[0],
                "users": db.execute(
                    "SELECT count(DISTINCT subject) FROM facts WHERE deleted=0"
                ).fetchone()[0],
                "jobs": [
                    dict(r)
                    for r in db.execute(
                        "SELECT * FROM jobs ORDER BY created DESC LIMIT 50"
                    )
                ],
            }

    def export(self):
        with self.connect() as db:
            return {
                t: [dict(r) for r in db.execute("SELECT * FROM " + t)]
                for t in (
                    "records",
                    "edges",
                    "facts",
                    "versions",
                    "migration_items",
                    "migration_reports",
                    "entities",
                    "entity_names",
                )
            }
