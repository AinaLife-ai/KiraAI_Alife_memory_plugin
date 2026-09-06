"""SQLite-backed hierarchical memory storage."""
from __future__ import annotations

import asyncio
import json
import math
import re
import sqlite3
import time
import uuid
import hashlib
from pathlib import Path
from typing import Any


_WORD_RE = re.compile(r"[\w\u4e00-\u9fff]+", re.UNICODE)


def tokenize(text: str) -> list[str]:
    return [x.lower() for x in _WORD_RE.findall(text or "") if x.strip()]


def estimate_tokens(text: str) -> int:
    text = text or ""
    return max(1, int(len(text) / 2.2)) if text else 0


def normalized_fact_key(summary: str, content: str) -> str:
    value = re.sub(r"[^\w\u4e00-\u9fff]+", "", f"{summary} {content}".lower())
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def cosine(a: list[float] | None, b: list[float] | None) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


class MemoryStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    sid TEXT NOT NULL,
                    user_id TEXT NOT NULL DEFAULT '',
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    ts REAL NOT NULL,
                    token_est INTEGER NOT NULL DEFAULT 0,
                    compressed INTEGER NOT NULL DEFAULT 0,
                    archive_id TEXT,
                    turn_key TEXT,
                    turn_no INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_messages_sid_state
                    ON messages(sid, compressed, ts);
                CREATE INDEX IF NOT EXISTS idx_messages_turn ON messages(turn_key);
                CREATE TABLE IF NOT EXISTS memories (
                    id TEXT PRIMARY KEY,
                    sid TEXT NOT NULL,
                    user_id TEXT NOT NULL DEFAULT '',
                    level INTEGER NOT NULL,
                    summary TEXT NOT NULL,
                    content TEXT NOT NULL,
                    start_ts REAL NOT NULL,
                    end_ts REAL NOT NULL,
                    importance REAL NOT NULL DEFAULT 0.5,
                    source_ids TEXT NOT NULL DEFAULT '[]',
                    embedding TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'active',
                    confidence REAL NOT NULL DEFAULT 0.65,
                    supersedes TEXT,
                    correction_note TEXT NOT NULL DEFAULT '',
                    source_fingerprint TEXT NOT NULL DEFAULT '',
                    dedupe_key TEXT NOT NULL DEFAULT '',
                    source_refs TEXT NOT NULL DEFAULT '[]'
                );
                CREATE INDEX IF NOT EXISTS idx_memories_sid_level_time
                    ON memories(sid, level, end_ts DESC);
                CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
                    summary, content, sid UNINDEXED, level UNINDEXED,
                    memory_id UNINDEXED
                );
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    sid TEXT,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_updated ON tasks(updated_at DESC);
                """
            )
            columns = {row[1] for row in conn.execute("PRAGMA table_info(memories)").fetchall()}
            message_columns = {row[1] for row in conn.execute("PRAGMA table_info(messages)").fetchall()}
            if "user_id" not in message_columns:
                conn.execute("ALTER TABLE messages ADD COLUMN user_id TEXT NOT NULL DEFAULT ''")
            if "turn_no" not in message_columns:
                conn.execute("ALTER TABLE messages ADD COLUMN turn_no INTEGER NOT NULL DEFAULT 0")
            if "user_id" not in columns:
                conn.execute("ALTER TABLE memories ADD COLUMN user_id TEXT NOT NULL DEFAULT ''")
            migrations = {
                "status": "ALTER TABLE memories ADD COLUMN status TEXT NOT NULL DEFAULT 'active'",
                "confidence": "ALTER TABLE memories ADD COLUMN confidence REAL NOT NULL DEFAULT 0.65",
                "supersedes": "ALTER TABLE memories ADD COLUMN supersedes TEXT",
                "correction_note": "ALTER TABLE memories ADD COLUMN correction_note TEXT NOT NULL DEFAULT ''",
                "source_fingerprint": "ALTER TABLE memories ADD COLUMN source_fingerprint TEXT NOT NULL DEFAULT ''",
                "dedupe_key": "ALTER TABLE memories ADD COLUMN dedupe_key TEXT NOT NULL DEFAULT ''",
                "source_refs": "ALTER TABLE memories ADD COLUMN source_refs TEXT NOT NULL DEFAULT '[]'",
            }
            for name, statement in migrations.items():
                if name not in columns:
                    conn.execute(statement)
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_memories_source_fingerprint ON memories(source_fingerprint) WHERE source_fingerprint <> ''")
            conn.commit()
        finally:
            conn.close()

    async def add_turn(self, sid: str, user_text: str, assistant_text: str,
                       ts: float | None = None, turn_key: str | None = None,
                       user_id: str = "") -> int:
        return await asyncio.to_thread(self._add_turn, sid, user_text, assistant_text, ts, turn_key, user_id)

    def _add_turn(self, sid, user_text, assistant_text, ts, turn_key, user_id):
        now = ts or time.time()
        key = turn_key or uuid.uuid4().hex
        conn = self._connect()
        try:
            exists = conn.execute("SELECT 1 FROM messages WHERE turn_key=? LIMIT 1", (key,)).fetchone()
            if exists:
                return 0
            rows = [(uuid.uuid4().hex, sid, "user", user_text.strip(), now),
                    (uuid.uuid4().hex, sid, "assistant", assistant_text.strip(), now)]
            conn.executemany(
                "INSERT INTO messages(id,sid,user_id,role,content,ts,token_est,turn_key,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                [(i, s, str(user_id or ""), r, c, t, estimate_tokens(c), key, now) for i, s, r, c, t in rows if c]
            )
            conn.commit()
            return len(rows)
        finally:
            conn.close()

    async def add_batch(self, sid: str, messages: list[dict[str, Any]], assistant_text: str,
                        ts: float | None = None, turn_key: str | None = None) -> int:
        return await asyncio.to_thread(self._add_batch, sid, messages, assistant_text, ts, turn_key)

    def _add_batch(self, sid, messages, assistant_text, ts, turn_key):
        now = ts or time.time()
        key = turn_key or uuid.uuid4().hex
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute("SELECT 1 FROM messages WHERE turn_key=? LIMIT 1", (key,)).fetchone():
                conn.rollback()
                return 0
            rows = []
            for index, item in enumerate(messages):
                content = str(item.get("content", "")).strip()
                if content:
                    rows.append((uuid.uuid4().hex, sid, str(item.get("user_id", "")), "user", content,
                                 float(item.get("ts") or now), index))
            if assistant_text.strip():
                rows.append((uuid.uuid4().hex, sid, "", "assistant", assistant_text.strip(), now, len(rows)))
            conn.executemany(
                "INSERT INTO messages(id,sid,user_id,role,content,ts,token_est,turn_key,turn_no,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                [(i, s, u, r, c, t, estimate_tokens(c), key, no, now) for i, s, u, r, c, t, no in rows]
            )
            conn.commit()
            return len(rows)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    async def pending_messages(self, sid: str, limit: int) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._pending_messages, sid, limit)

    def _pending_messages(self, sid, limit):
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM messages WHERE sid=? AND compressed=0 ORDER BY ts,id LIMIT ?",
                (sid, limit)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    async def mark_compressed(self, ids: list[str], archive_id: str) -> None:
        if ids:
            await asyncio.to_thread(self._mark_compressed, ids, archive_id)

    def _mark_compressed(self, ids, archive_id):
        conn = self._connect()
        try:
            conn.executemany("UPDATE messages SET compressed=1, archive_id=? WHERE id=?",
                             [(archive_id, x) for x in ids])
            conn.commit()
        finally:
            conn.close()

    async def increment_attempts(self, ids: list[str]) -> None:
        if ids:
            await asyncio.to_thread(self._increment_attempts, ids)

    def _increment_attempts(self, ids):
        conn = self._connect()
        try:
            conn.executemany("UPDATE messages SET attempts=attempts+1 WHERE id=?", [(x,) for x in ids])
            conn.commit()
        finally:
            conn.close()

    async def mark_skipped(self, ids: list[str], reason: str) -> None:
        if ids:
            await asyncio.to_thread(self._mark_skipped, ids, reason)

    def _mark_skipped(self, ids, reason):
        conn = self._connect()
        try:
            conn.executemany("UPDATE messages SET compressed=1, skip_reason=? WHERE id=?",
                             [(reason, x) for x in ids])
            conn.commit()
        finally:
            conn.close()

    async def add_memory(self, sid: str, level: int, summary: str, content: str,
                         start_ts: float, end_ts: float, source_ids: list[str],
                         importance: float = 0.5, embedding: list[float] | None = None,
                         memory_id: str | None = None, confidence: float = 0.65,
                         supersedes: str | None = None, correction_note: str = "",
                         user_id: str = "", source_fingerprint: str = "",
                         source_refs: list[dict[str, Any]] | None = None) -> str:
        return await asyncio.to_thread(self._add_memory, sid, level, summary, content,
                                       start_ts, end_ts, source_ids, importance, embedding,
                                       memory_id, confidence, supersedes, correction_note, user_id,
                                       source_fingerprint, source_refs)

    def _add_memory(self, sid, level, summary, content, start_ts, end_ts, source_ids,
                    importance, embedding, memory_id, confidence, supersedes, correction_note, user_id,
                    source_fingerprint, source_refs):
        mid = memory_id or f"L{level}-{int(start_ts)}-{int(end_ts)}-{uuid.uuid4().hex[:8]}"
        fact_key = normalized_fact_key(summary, content)
        now = time.time()
        conn = self._connect()
        try:
            if source_fingerprint:
                existing = conn.execute("SELECT id FROM memories WHERE source_fingerprint=?", (source_fingerprint,)).fetchone()
                if existing:
                    existing_id = str(existing[0])
                    # 合并新的 source_refs（跨来源关联）
                    incoming_refs = source_refs or [{'sid': sid, 'user_id': str(user_id or ''), 'start_ts': start_ts, 'end_ts': end_ts}]
                    if incoming_refs:
                        old_row = conn.execute("SELECT source_refs FROM memories WHERE id=?", (existing_id,)).fetchone()
                        if old_row:
                            try:
                                old_refs = json.loads(old_row['source_refs'] or '[]')
                            except (TypeError, json.JSONDecodeError):
                                old_refs = []
                            merged = old_refs + [ref for ref in incoming_refs if ref not in old_refs]
                            conn.execute("UPDATE memories SET source_refs=?, updated_at=? WHERE id=?",
                                         (json.dumps(merged, ensure_ascii=False), time.time(), existing_id))
                            conn.commit()
                    return existing_id
            duplicate = conn.execute("SELECT * FROM memories WHERE dedupe_key=? AND status='active' AND deleted=0 LIMIT 1", (fact_key,)).fetchone()
            if duplicate and int(duplicate['level']) == int(level):
                try:
                    existing_refs = json.loads(duplicate['source_refs'] or '[]')
                except (TypeError, json.JSONDecodeError):
                    existing_refs = []
                incoming_refs = source_refs or [{'sid': sid, 'user_id': str(user_id or ''), 'start_ts': start_ts, 'end_ts': end_ts}]
                merged_refs = existing_refs + [ref for ref in incoming_refs if ref not in existing_refs]
                try:
                    existing_ids = json.loads(duplicate['source_ids'] or '[]')
                except (TypeError, json.JSONDecodeError):
                    existing_ids = []
                merged_ids = list(dict.fromkeys(existing_ids + list(source_ids)))
                conn.execute("UPDATE memories SET source_ids=?, source_refs=?, importance=?, confidence=?, end_ts=?, updated_at=? WHERE id=?",
                             (json.dumps(merged_ids, ensure_ascii=False), json.dumps(merged_refs, ensure_ascii=False),
                              max(float(duplicate['importance']), float(importance)), max(float(duplicate['confidence']), float(confidence)),
                              max(float(duplicate['end_ts']), float(end_ts)), now, duplicate['id']))
                conn.commit()
                return str(duplicate['id'])
            refs = source_refs or [{'sid': sid, 'user_id': str(user_id or ''), 'start_ts': start_ts, 'end_ts': end_ts}]
            conn.execute(
                "INSERT OR REPLACE INTO memories(id,sid,user_id,level,summary,content,start_ts,end_ts,importance,source_ids,embedding,created_at,updated_at,status,confidence,supersedes,correction_note,source_fingerprint,dedupe_key,source_refs) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (mid, sid, str(user_id or ""), level, summary.strip(), content, start_ts, end_ts, max(0.0, min(1.0, importance)),
                 json.dumps(source_ids, ensure_ascii=False), json.dumps(embedding) if embedding else None, now, now,
                 "active", max(0.0, min(1.0, confidence)), supersedes, correction_note or "", source_fingerprint or "", fact_key,
                 json.dumps(refs, ensure_ascii=False))
            )
            conn.execute("DELETE FROM memories_fts WHERE memory_id=?", (mid,))
            conn.execute("INSERT INTO memories_fts(summary,content,sid,level,memory_id) VALUES(?,?,?,?,?)",
                         (summary, content, sid, level, mid))
            conn.commit()
            return mid
        finally:
            conn.close()

    async def search(self, sid: str, query: str, limit: int = 6,
                     levels: list[int] | None = None,
                     query_embedding: list[float] | None = None,
                     recency_half_life_days: float = 45.0,
                     scope: str = "session", user_id: str = "") -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._search, sid, query, limit, levels, query_embedding, recency_half_life_days, scope, user_id)

    def _search(self, sid, query, limit, levels, query_embedding, half_life, scope, user_id):
        conn = self._connect()
        try:
            terms = tokenize(query)
            match = " OR ".join('"' + t.replace('"', ' ') + '"' for t in terms[:12])
            level_sql = "" if not levels else " AND m.level IN (%s)" % ",".join("?" * len(levels))
            if scope in ("global", "linked"):
                owner_sql, owner_params = "1=1", []
            elif scope == "user" and user_id:
                owner_sql, owner_params = "m.user_id=?", [str(user_id)]
            else:
                owner_sql, owner_params = "m.sid=?", [sid]
            if match:
                rows = conn.execute(
                    f"""SELECT m.*, bm25(memories_fts) AS bm25_score
                        FROM memories_fts f JOIN memories m ON m.id=f.memory_id
                        WHERE {owner_sql} AND memories_fts MATCH ? AND m.deleted=0 AND m.status IN ('active','archived') {level_sql}
                        ORDER BY bm25_score LIMIT 80""", [*owner_params, match, *([] if not levels else levels)]
                ).fetchall()
                if not rows:
                    like_terms = [f"%{term}%" for term in terms[:8]]
                    like_sql = " OR ".join("(m.summary LIKE ? OR m.content LIKE ?)" for _ in like_terms)
                    rows = conn.execute(
                        f"""SELECT m.*, 0.0 AS bm25_score FROM memories m
                            WHERE {owner_sql} AND m.deleted=0 AND m.status IN ('active','archived')
                              AND ({like_sql}) {level_sql}
                            ORDER BY m.importance DESC, m.end_ts DESC LIMIT 80""",
                        [*owner_params, *sum(([x, x] for x in like_terms), []), *([] if not levels else levels)]
                    ).fetchall()
            else:
                rows = conn.execute(
                    f"""SELECT m.*, 0.0 AS bm25_score FROM memories m
                        WHERE {owner_sql} AND m.deleted=0 AND m.status='active' {level_sql}
                        ORDER BY m.importance DESC, m.end_ts DESC LIMIT 80""", [*owner_params, *([] if not levels else levels)]
                ).fetchall()
            now = time.time()
            scored = []
            for row in rows:
                item = dict(row)
                lex = 1.0 / (1.0 + max(0.0, float(item.get("bm25_score") or 0.0)))
                semantic = cosine(query_embedding, json.loads(item["embedding"])) if query_embedding and item.get("embedding") else 0.0
                age_days = max(0.0, (now - float(item["end_ts"])) / 86400.0)
                decay = 0.5 ** (age_days / max(1.0, half_life))
                level_bonus = min(0.18, int(item["level"]) * 0.045)
                scope_bonus = 0.0
                if scope == "linked":
                    if item.get("sid") == sid:
                        scope_bonus += 0.12
                    if user_id and item.get("user_id") == str(user_id):
                        scope_bonus += 0.08
                item["score"] = 0.48 * lex + 0.34 * semantic + 0.10 * decay + 0.08 * float(item.get("importance") or 0.5) + level_bonus + scope_bonus
                item["source_ids"] = json.loads(item.get("source_ids") or "[]")
                try:
                    item["source_refs"] = json.loads(item.get("source_refs") or "[]")
                except (TypeError, json.JSONDecodeError):
                    item["source_refs"] = []
                item.pop("embedding", None)
                scored.append(item)
            scored.sort(key=lambda x: (x["score"], x["importance"]), reverse=True)
            return scored[:max(1, limit)]
        finally:
            conn.close()

    async def recent(self, sid: str, limit: int = 20) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._recent, sid, limit)

    def _recent(self, sid, limit):
        conn = self._connect()
        try:
            return [dict(x) for x in conn.execute(
                "SELECT * FROM messages WHERE sid=? ORDER BY ts DESC,id DESC LIMIT ?", (sid, limit)
            ).fetchall()]
        finally:
            conn.close()

    async def list_memories(self, sid: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._list_memories, sid, limit)

    def _list_memories(self, sid, limit):
        conn = self._connect()
        try:
            if sid:
                rows = conn.execute("SELECT * FROM memories WHERE sid=? AND deleted=0 ORDER BY end_ts DESC LIMIT ?", (sid, limit)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM memories WHERE deleted=0 ORDER BY end_ts DESC LIMIT ?", (limit,)).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                try:
                    item["source_ids"] = json.loads(item.get("source_ids") or "[]")
                    item["source_refs"] = json.loads(item.get("source_refs") or "[]")
                except (TypeError, json.JSONDecodeError):
                    item["source_ids"], item["source_refs"] = [], []
                result.append(item)
            return result
        finally:
            conn.close()

    async def get_memory(self, memory_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_memory, memory_id)

    def _get_memory(self, memory_id):
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM memories WHERE id=?", (memory_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    async def correct_memory(self, memory_id: str, summary: str, content: str,
                             note: str, confidence: float = 0.95,
                             embedding: list[float] | None = None) -> str | None:
        return await asyncio.to_thread(self._correct_memory, memory_id, summary, content, note, confidence, embedding)

    def _correct_memory(self, memory_id, summary, content, note, confidence, embedding):
        conn = self._connect()
        try:
            old = conn.execute("SELECT * FROM memories WHERE id=? AND deleted=0", (memory_id,)).fetchone()
            if not old:
                return None
            new_id = f"corr-{int(time.time())}-{uuid.uuid4().hex[:8]}"
            now = time.time()
            conn.execute("UPDATE memories SET status='superseded', updated_at=?, correction_note=? WHERE id=?",
                         (now, note, memory_id))
            conn.execute(
                "INSERT INTO memories(id,sid,user_id,level,summary,content,start_ts,end_ts,importance,source_ids,embedding,created_at,updated_at,status,confidence,supersedes,correction_note,source_fingerprint,dedupe_key,source_refs) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (new_id, old['sid'], old['user_id'], old['level'], summary.strip(), content, old['start_ts'], old['end_ts'],
                 old['importance'], old['source_ids'], json.dumps(embedding) if embedding else old['embedding'],
                 now, now, 'active', max(0.0, min(1.0, confidence)), memory_id, note or '', '',
                 normalized_fact_key(summary, content), old['source_refs']))
            conn.execute("DELETE FROM memories_fts WHERE memory_id IN (?,?)", (memory_id, new_id))
            conn.execute("INSERT INTO memories_fts(summary,content,sid,level,memory_id) VALUES(?,?,?,?,?)",
                         (summary, content, old['sid'], old['level'], new_id))
            conn.commit()
            return new_id
        finally:
            conn.close()

    async def mark_stale(self, memory_id: str, note: str) -> bool:
        return await asyncio.to_thread(self._mark_stale, memory_id, note)

    def _mark_stale(self, memory_id, note):
        conn = self._connect()
        try:
            cur = conn.execute("UPDATE memories SET status='stale', updated_at=?, correction_note=? WHERE id=? AND deleted=0",
                               (time.time(), note or '', memory_id))
            conn.execute("DELETE FROM memories_fts WHERE memory_id=?", (memory_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    async def delete_memory(self, memory_id: str) -> bool:
        return await asyncio.to_thread(self._delete_memory, memory_id)

    def _delete_memory(self, memory_id):
        conn = self._connect()
        try:
            cur = conn.execute("UPDATE memories SET deleted=1, updated_at=? WHERE id=? AND deleted=0", (time.time(), memory_id))
            conn.execute("DELETE FROM memories_fts WHERE memory_id=?", (memory_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    async def stats(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._stats)

    def _stats(self):
        conn = self._connect()
        try:
            def n(sql): return int(conn.execute(sql).fetchone()[0])
            return {"messages": n("SELECT COUNT(*) FROM messages"), "pending": n("SELECT COUNT(*) FROM messages WHERE compressed=0"),
                    "memories": n("SELECT COUNT(*) FROM memories WHERE deleted=0"), "sessions": n("SELECT COUNT(DISTINCT sid) FROM messages"),
                    "db_bytes": self.path.stat().st_size if self.path.exists() else 0}
        finally:
            conn.close()

    async def create_task(self, sid: str, kind: str, message: str = "") -> str:
        return await asyncio.to_thread(self._create_task, sid, kind, message)

    def _create_task(self, sid, kind, message):
        task_id = uuid.uuid4().hex[:12]
        now = time.time()
        conn = self._connect()
        try:
            conn.execute("INSERT INTO tasks VALUES(?,?,?,?,?,?,?)", (task_id, sid, kind, "running", message, now, now))
            conn.commit()
            return task_id
        finally:
            conn.close()

    async def update_task(self, task_id: str, status: str, message: str = "") -> None:
        await asyncio.to_thread(self._update_task, task_id, status, message)

    def _update_task(self, task_id, status, message):
        conn = self._connect()
        try:
            conn.execute("UPDATE tasks SET status=?,message=?,updated_at=? WHERE id=?", (status, message, time.time(), task_id))
            conn.commit()
        finally:
            conn.close()

    async def tasks(self, limit: int = 50) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._tasks, limit)

    def _tasks(self, limit):
        conn = self._connect()
        try:
            return [dict(x) for x in conn.execute("SELECT * FROM tasks ORDER BY updated_at DESC LIMIT ?", (limit,)).fetchall()]
        finally:
            conn.close()

    async def pending_stats(self, sid: str) -> dict:
        return await asyncio.to_thread(self._pending_stats, sid)

    def _pending_stats(self, sid):
        """Return {message_count, token_sum, round_count} for pending messages."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT COUNT(*) AS msg_count, COALESCE(SUM(token_est), 0) AS token_sum, "
                "COUNT(DISTINCT turn_key) AS round_count FROM messages WHERE sid=? AND compressed=0",
                (sid,)
            ).fetchone()
            return {"message_count": row["msg_count"], "token_sum": row["token_sum"],
                    "round_count": max(0, row["round_count"])}
        finally:
            conn.close()

    async def pending_messages_all(self) -> dict:
        return await asyncio.to_thread(self._pending_messages_all)

    def _pending_messages_all(self):
        """Return aggregate {message_count, token_sum, round_count} across all sessions."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT COUNT(*) AS msg_count, COALESCE(SUM(token_est), 0) AS token_sum, "
                "COUNT(DISTINCT turn_key) AS round_count FROM messages WHERE compressed=0"
            ).fetchone()
            return {"message_count": row["msg_count"], "token_sum": row["token_sum"],
                    "round_count": max(0, row["round_count"])}
        finally:
            conn.close()

    async def count_memories_by_user(self, user_id: str, limit: int = 20) -> list[dict]:
        return await asyncio.to_thread(self._count_memories_by_user, user_id, limit)

    def _count_memories_by_user(self, user_id, limit):
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM memories WHERE user_id=? AND deleted=0 AND status='active' ORDER BY end_ts DESC LIMIT ?",
                (user_id, limit)
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                try:
                    item["source_ids"] = json.loads(item.get("source_ids") or "[]")
                    item["source_refs"] = json.loads(item.get("source_refs") or "[]")
                except (TypeError, json.JSONDecodeError):
                    item["source_ids"], item["source_refs"] = [], []
                result.append(item)
            return result
        finally:
            conn.close()

    async def top_level_memories(self, sid: str, min_level: int = 1, limit: int = 5) -> list[dict]:
        return await asyncio.to_thread(self._top_level_memories, sid, min_level, limit)

    def _top_level_memories(self, sid, min_level, limit):
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT id, level, summary, content, end_ts, sid, user_id, status FROM memories "
                "WHERE sid=? AND deleted=0 AND status IN ('active','archived') ORDER BY level DESC, end_ts DESC LIMIT ?",
                (sid, limit)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    async def close(self):
        return None

    async def archive_inactive(self, days: int, level_min: int = 1) -> int:
        return await asyncio.to_thread(self._archive_inactive, days, level_min)

    def _archive_inactive(self, days, level_min):
        if days <= 0:
            return 0
        conn = self._connect()
        try:
            cutoff = time.time() - days * 86400
            # 归档 level < level_min 的旧记忆，level_min 及以上不受影响
            cur = conn.execute(
                "UPDATE memories SET status='archived', updated_at=? WHERE status='active' AND end_ts < ? AND level < ?",
                (time.time(), cutoff, level_min))
            count = cur.rowcount
            conn.commit()
            return count
        finally:
            conn.close()

    async def cleanup(self, message_retention_days: int = 7, stale_retention_days: int = 30) -> dict:
        return await asyncio.to_thread(self._cleanup, message_retention_days, stale_retention_days)

    def _cleanup(self, message_retention_days, stale_retention_days):
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cutoff = time.time() - message_retention_days * 86400
            # 删除已压缩的旧消息（compressed=1 且超过保留期）
            deleted_messages = conn.execute(
                "DELETE FROM messages WHERE compressed=1 AND created_at < ?", (cutoff,)
            ).rowcount
            # 删除 superseded 和 stale 版本的记忆（超过保留期）
            stale_cutoff = time.time() - stale_retention_days * 86400
            stale_ids = conn.execute(
                "SELECT id FROM memories WHERE status IN ('stale','superseded') AND updated_at < ?", (stale_cutoff,)
            ).fetchall()
            for (sid,) in stale_ids:
                conn.execute("DELETE FROM memories_fts WHERE memory_id=?", (sid,))
            deleted_stale = conn.execute(
                "DELETE FROM memories WHERE status IN ('stale','superseded') AND updated_at < ?", (stale_cutoff,)
            ).rowcount
            conn.commit()
            return {"deleted_messages": deleted_messages, "deleted_stale_memories": deleted_stale}
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
