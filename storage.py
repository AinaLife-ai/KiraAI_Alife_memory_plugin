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

# jieba 中文分词（可选依赖，缺失则降级为字符级 fallback）
try:
    import jieba  # type: ignore
    _JIEBA_AVAILABLE = True
except Exception:  # pragma: no cover
    _JIEBA_AVAILABLE = False


def _fallback_tokenize(text: str) -> list[str]:
    """无 jieba 时的兜底分词：英文按词、中文按字符切分（比整句当一个 token 强）。"""
    if not text:
        return []
    tokens: list[str] = []
    buf = ""
    for ch in text:
        if ch.isascii() and (ch.isalnum() or ch == "_"):
            buf += ch
        else:
            if buf:
                tokens.append(buf)
                buf = ""
            if ch.strip():
                tokens.append(ch)
    if buf:
        tokens.append(buf)
    return tokens


def segment_for_fts(text: str) -> str:
    """把文本用 jieba（可选）分词后用空格连接，使 FTS5 unicode61 能正确检索中文。
    无 jieba 时降级为字符级 fallback。"""
    if not text:
        return ""
    if _JIEBA_AVAILABLE:
        toks = [t for t in jieba.lcut(text) if t.strip()]
    else:
        toks = [t for t in _fallback_tokenize(text) if t.strip()]
    return " ".join(toks)


def build_fts_query(text: str) -> str:
    """构造 FTS5 查询：jieba 分词后过滤单字中文（保留英文单字符），OR 连接各 token。"""
    if not text or not text.strip():
        return ""
    cleaned = text.strip()
    for ch in ['"', "'", "(", ")", "*", "+", "-", ":", "^", "{", "}", "~",
               "[", "]", "@", "<", ">", "/", "\\", "|", "!", "?", "#", "&",
               "=", ";", ",", "."]:
        cleaned = cleaned.replace(ch, " ")
    cleaned = cleaned.strip()
    if not cleaned:
        return ""
    if _JIEBA_AVAILABLE:
        toks = [t.strip() for t in jieba.lcut(cleaned) if t.strip()]
    else:
        toks = [t.strip() for t in _fallback_tokenize(cleaned) if t.strip()]
    # 过滤单字符中文（停用词），保留英文单字符
    toks = [t for t in toks if len(t) > 1 or t.isascii()]
    if not toks:
        return cleaned
    if len(toks) == 1:
        return toks[0]
    return " OR ".join(toks)


def tokenize(text: str) -> list[str]:
    """返回分词 token 列表（jieba 优先，字符级兜底）。"""
    if not text:
        return []
    if _JIEBA_AVAILABLE:
        return [t for t in jieba.lcut(text) if t.strip()]
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


def _normalize_item(item: dict) -> dict:
    """解析并清理从 memories 表返回的行：反序列化 source_ids/source_refs/tags，
    去掉 embedding 大数组（对 LLM/前端无意义，避免浪费），保留 embed_model/结构化字段。"""
    try:
        item["source_ids"] = json.loads(item.get("source_ids") or "[]")
    except (TypeError, json.JSONDecodeError):
        item["source_ids"] = []
    try:
        item["source_refs"] = json.loads(item.get("source_refs") or "[]")
    except (TypeError, json.JSONDecodeError):
        item["source_refs"] = []
    try:
        item["tags"] = json.loads(item.get("tags") or "[]")
    except (TypeError, json.JSONDecodeError):
        item["tags"] = []
    item.pop("embedding", None)
    return item


def _write_tags(conn, mid: str, tags: list[str] | None) -> None:
    """把标签写入 memories_tags 索引表（结构化检索用）。"""
    if not tags:
        return
    for tag in tags:
        tag = str(tag).strip()
        if tag:
            conn.execute("INSERT OR IGNORE INTO memories_tags(mid,tag) VALUES(?,?)", (mid, tag))


def _table_has_column(conn, table: str, column: str) -> bool:
    """检查表是否有某列。"""
    try:
        cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        return column in cols
    except Exception:
        return False


import re as _PREFIX_RE
_PREFIX_RE_PATTERN = _PREFIX_RE.compile(r'^\[(标签:|关于我的|我喜欢的|正在发生的|去哪查|日常的|关于实体|偏好风格|约定任务|关系网络|溯源查询|日常事件|原因:|何时想起:)')
def _re_match_prefix(s: str) -> bool:
    """判断字符串是否以已知前缀开头（用于迁移判断）。"""
    return bool(_PREFIX_RE_PATTERN.match(s or ""))


def _parse_legacy_prefix(summary: str, content: str) -> tuple[str, list[str], str, str]:
    """反解析历史字符串前缀 [关于我的]/[我喜欢的]/[标签:...]/[原因:...]/[何时想起:...]。
    返回 (clean_summary, clean_content, memory_type, reason, scenario)。
    只移除已知前缀，解析不出则原样保留（幂等、不丢数据）。"""
    import re as _re
    memory_type = ""
    reason = ""
    scenario = ""
    leg_map = {"关于我的": "关于实体", "我喜欢的": "偏好风格", "正在发生的": "约定任务",
               "去哪查": "溯源查询", "日常的": "日常事件"}
    # 抽取类型前缀
    m = _re.match(r'^\[(关于我的|我喜欢的|正在发生的|去哪查|日常的|关于实体|偏好风格|约定任务|关系网络|溯源查询|日常事件)\]', summary)
    if m:
        memory_type = leg_map.get(m.group(1), m.group(1))
        summary = summary[m.end():].strip()
        content = _re.sub(r'^\[(关于我的|我喜欢的|正在发生的|去哪查|日常的|关于实体|偏好风格|约定任务|关系网络|溯源查询|日常事件)\]\s*', '', content)
    # 抽取标签前缀（summary 和 content 开头都可能带，都尝试）
    tags = []
    tm = _re.match(r'^\[标签:([^\]]+)\]\s*', summary)
    if tm:
        tags = [t.strip() for t in tm.group(1).split(",") if t.strip()]
        summary = summary[tm.end():].strip()
        content = _re.sub(r'^\[标签:[^\]]+\]\s*', '', content)
    else:
        tm2 = _re.match(r'^\[标签:([^\]]+)\]\s*', content)
        if tm2:
            tags = [t.strip() for t in tm2.group(1).split(",") if t.strip()]
            content = content[tm2.end():].strip()
    # 抽取原因
    rm = _re.search(r'\[原因:([^\]]*)\]', content)
    if rm:
        reason = rm.group(1)
        content = content.replace(rm.group(0), "").strip()
    # 抽取何时想起
    sm = _re.search(r'\[何时想起:([^\]]*)\]', content)
    if sm:
        scenario = sm.group(1)
        content = content.replace(sm.group(0), "").strip()
    return summary, content, memory_type, tags, reason, scenario


def _migrate_legacy_prefixes(conn) -> int:
    """把历史 [关于我的]/[标签:...] 等前缀记忆反解析成结构化字段。幂等。返回处理条数。"""
    if not _table_has_column(conn, "memories", "memory_type"):
        return 0
    rows = conn.execute("SELECT id,sid,level,summary,content,memory_type,tags,reason,scenario FROM memories").fetchall()
    n = 0
    for r in rows:
        need = False
        clean_sum = r["summary"]; clean_con = r["content"]; mtype = r["memory_type"]; tags = r["tags"]; reason = r["reason"]; scenario = r["scenario"]
        # 只处理仍带旧前缀的
        if clean_sum.startswith("[") and "[标签:" in clean_sum or _re_match_prefix(clean_sum) or _re_match_prefix(clean_con):
            clean_sum, clean_con, new_type, new_tags, new_reason, new_scenario = _parse_legacy_prefix(clean_sum, clean_con)
            if new_type:
                mtype = _norm_memory_type(new_type); need = True
            if new_tags:
                try:
                    tags = json.loads(tags or "[]")
                except (TypeError, json.JSONDecodeError):
                    tags = []
                tags = list(dict.fromkeys(tags + new_tags))
                need = True
            if new_reason:
                reason = new_reason; need = True
            if new_scenario:
                scenario = new_scenario; need = True
            if clean_sum != r["summary"] or clean_con != r["content"]:
                need = True
            if not need:
                continue
            conn.execute("UPDATE memories SET summary=?, content=?, memory_type=?, tags=?, reason=?, scenario=? WHERE id=?",
                         (clean_sum, clean_con, mtype, json.dumps(tags, ensure_ascii=False), reason, scenario, r["id"]))
            if new_tags:
                _write_tags(conn, r["id"], new_tags)
            # 重建 FTS
            conn.execute("DELETE FROM memories_fts WHERE memory_id=?", (r["id"],))
            conn.execute("INSERT INTO memories_fts(summary,content,sid,level,memory_id) VALUES(?,?,?,?,?)",
                         (segment_for_fts(clean_sum), segment_for_fts(clean_con), r["sid"], r["level"], r["id"]))
            n += 1
    return n


def _norm_memory_type(mt: str) -> str:
    """规范化记忆类型到标准 6 类。非法值回退为「日常事件」。兼容旧 5 类中文标签和英文枚举。"""
    valid = {"关于实体", "偏好风格", "约定任务", "关系网络", "溯源查询", "日常事件"}
    mt = (mt or "").strip()
    if mt in valid:
        return mt
    # 兼容旧 5 类标签
    legacy = {"关于我的": "关于实体", "我喜欢的": "偏好风格", "正在发生的": "约定任务",
              "去哪查": "溯源查询", "日常的": "日常事件"}
    if mt in legacy:
        return legacy[mt]
    # 兼容英文枚举（后端模型可能输出）
    en = {"entity": "关于实体", "preference": "偏好风格", "task": "约定任务",
          "relation": "关系网络", "source": "溯源查询", "daily": "日常事件", "fact": "日常事件"}
    return en.get(mt, "日常事件")


def uid_or_empty(user_id: str) -> str:
    """实体关联字段：优先记忆显式声明的 entity_id，缺省时退化到 user_id。"""
    return str(user_id or "")


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
                    source_refs TEXT NOT NULL DEFAULT '[]',
                    embed_model TEXT DEFAULT ''
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
                "embed_model": "ALTER TABLE memories ADD COLUMN embed_model TEXT DEFAULT ''",
                "memory_type": "ALTER TABLE memories ADD COLUMN memory_type TEXT NOT NULL DEFAULT 'fact'",
                "tags": "ALTER TABLE memories ADD COLUMN tags TEXT NOT NULL DEFAULT '[]'",
                "reason": "ALTER TABLE memories ADD COLUMN reason TEXT NOT NULL DEFAULT ''",
                "scenario": "ALTER TABLE memories ADD COLUMN scenario TEXT NOT NULL DEFAULT ''",
                "entity_id": "ALTER TABLE memories ADD COLUMN entity_id TEXT NOT NULL DEFAULT ''",
                "reflect_count": "ALTER TABLE memories ADD COLUMN reflect_count INTEGER NOT NULL DEFAULT 0",
                "reflect_last_ts": "ALTER TABLE memories ADD COLUMN reflect_last_ts REAL NOT NULL DEFAULT 0",
            }
            for name, statement in migrations.items():
                if name not in columns:
                    conn.execute(statement)
            # 标签索引表（结构化标签，替代字符串解析）
            conn.execute("CREATE TABLE IF NOT EXISTS memories_tags(mid TEXT NOT NULL, tag TEXT NOT NULL, UNIQUE(mid, tag))")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_memories_tags_tag ON memories_tags(tag)")
            # 用户/实体画像表（entity_id 统一，user/group/bot 归一张表，entity_type 区分）
            conn.execute("""CREATE TABLE IF NOT EXISTS user_profiles(
                entity_id TEXT PRIMARY KEY,
                entity_type TEXT NOT NULL DEFAULT 'user',
                name TEXT DEFAULT '', nickname TEXT DEFAULT '',
                description TEXT DEFAULT '',
                platform TEXT DEFAULT '',
                traits TEXT DEFAULT '[]',
                preferences TEXT DEFAULT '{}',
                relationships TEXT DEFAULT '[]',
                facts TEXT DEFAULT '[]',
                aliases TEXT DEFAULT '[]',
                interaction_count INTEGER DEFAULT 0,
                last_interaction REAL DEFAULT 0,
                updated_at REAL DEFAULT 0,
                generated_from TEXT DEFAULT '',
                memory_type TEXT DEFAULT ''
            )""")
            # 关系网络表：实体间三元组（谁是谁的什么人）
            conn.execute("""CREATE TABLE IF NOT EXISTS relationships(
                id TEXT PRIMARY KEY,
                entity_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                relation TEXT NOT NULL,
                direction TEXT NOT NULL DEFAULT 'direct',
                source_mid TEXT,
                confidence REAL DEFAULT 0.7,
                created_at REAL, updated_at REAL
            )""")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_relationships_entity ON relationships(entity_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_relationships_target ON relationships(target_id)")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_memories_source_fingerprint ON memories(source_fingerprint) WHERE source_fingerprint <> ''")
            # FTS 中文检索迁移：统一重建为 unicode61（jieba 分词后空格连接入库，能正确检索中文）
            try:
                fts_sql_row = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='memories_fts'"
                ).fetchone()
                # 旧表可能是 trigram（存原文）或旧 unicode61（无分词）；统一重建为 jieba 分词入库
                if fts_sql_row and ("trigram" in (fts_sql_row[0] or "").lower() or "tokenize" not in (fts_sql_row[0] or "").lower()):
                    logger = __import__("logging").getLogger("alife_memory_storage")
                    logger.info("[alife_memory_storage] 重建 memories_fts 为 jieba 分词索引（中文检索）")
                    # 读取旧 FTS 里的原文本（从 memories 表，避免占用旧 FTS）
                    old_rows = conn.execute("SELECT id,sid,level,summary,content FROM memories WHERE deleted=0").fetchall()
                    conn.execute("DROP TABLE memories_fts")
                    conn.execute("""
                        CREATE VIRTUAL TABLE memories_fts USING fts5(
                            summary, content, sid UNINDEXED, level UNINDEXED, memory_id UNINDEXED
                        )
                    """)
                    for r in old_rows:
                        seg_sum = segment_for_fts(r["summary"])
                        seg_con = segment_for_fts(r["content"])
                        conn.execute(
                            "INSERT INTO memories_fts(summary,content,sid,level,memory_id) VALUES(?,?,?,?,?)",
                            (seg_sum, seg_con, r["sid"], r["level"], r["id"])
                        )
            except Exception:
                pass
            # 反解析历史 [关于我的]/[标签:...] 前缀 → 结构化字段（幂等迁移）
            try:
                mig_n = _migrate_legacy_prefixes(conn)
                if mig_n:
                    logger = __import__("logging").getLogger("alife_memory_storage")
                    logger.info("[alife_memory_storage] 反解析历史标签前缀: %d 条记忆 → 结构化字段", mig_n)
            except Exception:
                pass
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
                         source_refs: list[dict[str, Any]] | None = None,
                         embed_model: str = "", memory_type: str = "日常事件",
                         tags: list[str] | None = None, reason: str = "",
                         scenario: str = "", entity_id: str = "") -> str:
        return await asyncio.to_thread(self._add_memory, sid, level, summary, content,
                                       start_ts, end_ts, source_ids, importance, embedding,
                                       memory_id, confidence, supersedes, correction_note, user_id,
                                       source_fingerprint, source_refs, embed_model,
                                       memory_type, tags, reason, scenario, entity_id)

    def _add_memory(self, sid, level, summary, content, start_ts, end_ts, source_ids,
                    importance, embedding, memory_id, confidence, supersedes, correction_note, user_id,
                    source_fingerprint, source_refs, embed_model="",
                    memory_type="日常事件", tags=None, reason="", scenario="", entity_id=""):
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
            _mt = _norm_memory_type(memory_type)
            _tags = [t for t in (tags or []) if str(t).strip()]
            conn.execute(
                "INSERT OR REPLACE INTO memories(id,sid,user_id,level,summary,content,start_ts,end_ts,importance,source_ids,embedding,created_at,updated_at,status,confidence,supersedes,correction_note,source_fingerprint,dedupe_key,source_refs,embed_model,memory_type,tags,reason,scenario,entity_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (mid, sid, str(user_id or ""), level, summary.strip(), content, start_ts, end_ts, max(0.0, min(1.0, importance)),
                 json.dumps(source_ids, ensure_ascii=False), json.dumps(embedding) if embedding else None, now, now,
                 "active", max(0.0, min(1.0, confidence)), supersedes, correction_note or "", source_fingerprint or "", fact_key,
                 json.dumps(refs, ensure_ascii=False), embed_model or "", _mt,
                 json.dumps(_tags, ensure_ascii=False), reason or "", scenario or "",
                 str(entity_id or uid_or_empty(user_id))))
            _write_tags(conn, mid, _tags)
            conn.execute("DELETE FROM memories_fts WHERE memory_id=?", (mid,))
            conn.execute("INSERT INTO memories_fts(summary,content,sid,level,memory_id) VALUES(?,?,?,?,?)",
                         (segment_for_fts(summary), segment_for_fts(content), sid, level, mid))
            conn.commit()
            return mid
        finally:
            conn.close()

    async def add_memories_batch(self, rows: list[dict]) -> int:
        """批量写记忆（迁移专用）：单连接单事务处理多条，显著快于逐条 add_memory。
        保留 source_fingerprint 去重，避免重复迁移。返回新增条数。"""
        if not rows:
            return 0
        return await asyncio.to_thread(self._add_memories_batch, rows)

    def _add_memories_batch(self, rows):
        conn = self._connect()
        added = 0
        try:
            for it in rows:
                sid = it["sid"]; level = it["level"]; summary = it["summary"]
                content = it["content"]; start_ts = it["start_ts"]; end_ts = it["end_ts"]
                importance = it.get("importance", 0.5); embedding = it.get("embedding")
                embed_model = it.get("embed_model", "")
                source_ids = it.get("source_ids", []); source_refs = it.get("source_refs", [])
                user_id = it.get("user_id", ""); confidence = it.get("confidence", 0.65)
                source_fingerprint = it.get("source_fingerprint", ""); memory_id = it.get("memory_id")
                memory_type = it.get("memory_type", "日常事件"); tags = it.get("tags") or []
                reason = it.get("reason", ""); scenario = it.get("scenario", ""); entity_id = it.get("entity_id", "")
                fact_key = normalized_fact_key(summary, content)
                now = time.time()
                # 去重：同指纹已存在 → 跳过（迁移幂等）
                if source_fingerprint:
                    existing = conn.execute("SELECT id FROM memories WHERE source_fingerprint=? LIMIT 1", (source_fingerprint,)).fetchone()
                    if existing:
                        continue
                # dedupe_key 一级去重（同层级同事实）
                dup = conn.execute("SELECT id FROM memories WHERE dedupe_key=? AND status='active' AND deleted=0 AND level=? LIMIT 1", (fact_key, level)).fetchone()
                if dup:
                    continue
                mid = memory_id or f"L{level}-{int(start_ts)}-{int(end_ts)}-{uuid.uuid4().hex[:8]}"
                refs = source_refs or [{"sid": sid, "user_id": str(user_id or ""), "start_ts": start_ts, "end_ts": end_ts}]
                _mt = _norm_memory_type(memory_type)
                _tags = [t for t in (tags or []) if str(t).strip()]
                conn.execute(
                    "INSERT OR REPLACE INTO memories(id,sid,user_id,level,summary,content,start_ts,end_ts,importance,source_ids,embedding,created_at,updated_at,status,confidence,supersedes,correction_note,source_fingerprint,dedupe_key,source_refs,embed_model,memory_type,tags,reason,scenario,entity_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (mid, sid, str(user_id or ""), level, summary.strip(), content, start_ts, end_ts,
                     max(0.0, min(1.0, importance)), json.dumps(source_ids, ensure_ascii=False),
                     json.dumps(embedding) if embedding else None, now, now, "active",
                     max(0.0, min(1.0, confidence)), None, "", source_fingerprint or "", fact_key,
                     json.dumps(refs, ensure_ascii=False), embed_model or "", _mt,
                     json.dumps(_tags, ensure_ascii=False), reason or "", scenario or "",
                     str(entity_id or uid_or_empty(user_id))))
                _write_tags(conn, mid, _tags)
                conn.execute("DELETE FROM memories_fts WHERE memory_id=?", (mid,))
                conn.execute("INSERT INTO memories_fts(summary,content,sid,level,memory_id) VALUES(?,?,?,?,?)",
                             (segment_for_fts(summary), segment_for_fts(content), sid, level, mid))
                added += 1
            conn.commit()
            return added
        finally:
            conn.close()

    async def search(self, sid: str, query: str, limit: int = 6,
                     levels: list[int] | None = None,
                     query_embedding: list[float] | None = None,
                     recency_half_life_days: float = 45.0,
                     scope: str = "session", user_id: str = "",
                     embed_model: str = "") -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._search, sid, query, limit, levels, query_embedding, recency_half_life_days, scope, user_id, embed_model)

    def _search(self, sid, query, limit, levels, query_embedding, half_life, scope, user_id, embed_model):
        conn = self._connect()
        try:
            terms = tokenize(query)
            level_sql = "" if not levels else " AND m.level IN (%s)" % ",".join("?" * len(levels))
            if scope in ("global", "linked"):
                owner_sql, owner_params = "1=1", []
            elif scope == "user" and user_id:
                owner_sql, owner_params = "m.user_id=?", [str(user_id)]
            else:
                owner_sql, owner_params = "m.sid=?", [sid]
            rows = []
            seen_ids: set[str] = set()
            # jieba 分词后的 FTS 查询（unicode61 能正确检索中文）；无有效 token 时跳过
            fts_query = build_fts_query(query)
            fts_bm25 = {}
            if fts_query:
                fts_rows = conn.execute(
                    f"""SELECT m.*, bm25(memories_fts) AS bm25_score
                        FROM memories_fts f JOIN memories m ON m.id=f.memory_id
                        WHERE {owner_sql} AND memories_fts MATCH ? AND m.deleted=0 AND m.status IN ('active','archived') {level_sql}
                        ORDER BY bm25_score LIMIT 80""",
                    [*owner_params, fts_query, *([] if not levels else levels)]
                ).fetchall()
                for r in fts_rows:
                    if r["id"] not in seen_ids:
                        rows.append(r)
                        seen_ids.add(r["id"])
                        fts_bm25[r["id"]] = float(r["bm25_score"] or 0.0)
            # LIKE 兜底：仅当 FTS 命中不足时才启用，避免全表扫和单字误召回刷屏。
            # 若 FTS 已命中足够结果（>= limit*2），直接跳过（省性能、防误召回）。
            target = max(limit * 2, limit)
            if len(seen_ids) < target:
                like_terms = list(dict.fromkeys(terms[:8]))  # 分词 token 去重
                # 补充中文字符（去重，过滤空白），覆盖单字书名/人名等 jieba 未登录词
                for ch in str(query):
                    if ch.strip() and ('\u4e00' <= ch <= '\u9fff'):
                        like_terms.append(ch)
                like_terms_all = [f"%{term}%" for term in like_terms[:12]]
                if like_terms_all:
                    like_sql = " OR ".join("(m.summary LIKE ? OR m.content LIKE ?)" for _ in like_terms_all)
                    like_rows = conn.execute(
                        f"""SELECT m.*, 0.0 AS bm25_score FROM memories m
                            WHERE {owner_sql} AND m.deleted=0 AND m.status IN ('active','archived')
                              AND ({like_sql}) {level_sql}
                            ORDER BY m.importance DESC, m.end_ts DESC LIMIT 80""",
                        [*owner_params, *sum(([x, x] for x in like_terms_all), []), *([] if not levels else levels)]
                    ).fetchall()
                    for r in like_rows:
                        if r["id"] not in seen_ids:
                            rows.append(r)
                            seen_ids.add(r["id"])
                            fts_bm25[r["id"]] = 0.0
            if not rows:
                rows = conn.execute(
                    f"""SELECT m.*, 0.0 AS bm25_score FROM memories m
                        WHERE {owner_sql} AND m.deleted=0 AND m.status='active' {level_sql}
                        ORDER BY m.importance DESC, m.end_ts DESC LIMIT 80""", [*owner_params, *([] if not levels else levels)]
                ).fetchall()
            now = time.time()
            scored = []
            for row in rows:
                item = dict(row)
                # 从 fts_bm25 取该记忆的 bm25 分（LIKE 兜底行未命中 FTS 则为 0）
                bm25_val = fts_bm25.get(item["id"], 0.0)
                lex = abs(float(bm25_val))
                # FTS5 bm25 负值越小越相关，归一化到 [0,1]：相关→趋近1，不相关→趋近0
                lex = lex / (1.0 + lex)
                # 语义项：仅当当前配置的 embedding 模型与记忆向量一致时才用（避免跨模型错误匹配）
                item_emb_model = str(item.get("embed_model") or "")
                if query_embedding and item.get("embedding") and (not embed_model or item_emb_model == embed_model):
                    try:
                        semantic = cosine(query_embedding, json.loads(item["embedding"]))
                    except (TypeError, ValueError, json.JSONDecodeError):
                        semantic = 0.0
                else:
                    semantic = 0.0
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
                result.append(_normalize_item(dict(row)))
            return result
        finally:
            conn.close()

    async def list_memories_filtered(self, sid: str | None = None, level: int | None = None,
                                     memory_type: str | None = None, tag: str | None = None,
                                     user_id: str | None = None, limit: int = 100) -> list[dict]:
        """按维度筛选记忆（WebUI 五维查看用）。支持 sid/level/memory_type/tag/user_id 组合。"""
        return await asyncio.to_thread(self._list_memories_filtered, sid, level, memory_type, tag, user_id, limit)

    def _list_memories_filtered(self, sid, level, memory_type, tag, user_id, limit):
        conn = self._connect()
        try:
            where = ["deleted=0"]
            params: list = []
            if sid:
                where.append("m.sid=?")
                params.append(sid)
            if level:
                where.append("m.level=?")
                params.append(level)
            if memory_type:
                where.append("m.memory_type=?")
                params.append(memory_type)
            if user_id:
                where.append("(m.user_id=? OR m.entity_id=?)")
                params.extend([user_id, user_id])
            if tag:
                # 通过 memories_tags 索引表筛标签
                tag_sql = ("SELECT mid FROM memories_tags WHERE tag=?")
                rows = conn.execute(
                    f"SELECT m.* FROM memories m WHERE {' AND '.join(where)} AND m.id IN ({tag_sql}) "
                    "ORDER BY m.end_ts DESC LIMIT ?",
                    [*params, tag, limit]).fetchall()
            else:
                rows = conn.execute(
                    f"SELECT m.* FROM memories m WHERE {' AND '.join(where)} ORDER BY m.end_ts DESC LIMIT ?",
                    [*params, limit]).fetchall()
            result = []
            for row in rows:
                result.append(_normalize_item(dict(row)))
            return result
        finally:
            conn.close()

    async def get_memory(self, memory_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_memory, memory_id)

    def _get_memory(self, memory_id):
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM memories WHERE id=?", (memory_id,)).fetchone()
            return _normalize_item(dict(row)) if row else None
        finally:
            conn.close()

    async def correct_memory(self, memory_id: str, summary: str, content: str,
                             note: str, confidence: float = 0.95,
                             embedding: list[float] | None = None,
                             memory_type: str = "", tags: list[str] | None = None,
                             reason: str = "", scenario: str = "",
                             entity_id: str = "") -> str | None:
        return await asyncio.to_thread(self._correct_memory, memory_id, summary, content, note, confidence, embedding,
                                       memory_type, tags, reason, scenario, entity_id)

    def _correct_memory(self, memory_id, summary, content, note, confidence, embedding,
                        memory_type="", tags=None, reason="", scenario="", entity_id=""):
        conn = self._connect()
        try:
            old = conn.execute("SELECT * FROM memories WHERE id=? AND deleted=0", (memory_id,)).fetchone()
            if not old:
                return None
            new_id = f"corr-{int(time.time())}-{uuid.uuid4().hex[:8]}"
            now = time.time()
            conn.execute("UPDATE memories SET status='superseded', updated_at=?, correction_note=? WHERE id=?",
                         (now, note, memory_id))
            _mt = _norm_memory_type(memory_type or old['memory_type'] if 'memory_type' in old.keys() and old['memory_type'] else memory_type or "日常事件")
            _tags = [t for t in (tags or (json.loads(old['tags']) if 'tags' in old.keys() and old['tags'] else [])) if str(t).strip()]
            conn.execute(
                "INSERT INTO memories(id,sid,user_id,level,summary,content,start_ts,end_ts,importance,source_ids,embedding,created_at,updated_at,status,confidence,supersedes,correction_note,source_fingerprint,dedupe_key,source_refs,embed_model,memory_type,tags,reason,scenario,entity_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (new_id, old['sid'], old['user_id'], old['level'], summary.strip(), content, old['start_ts'], old['end_ts'],
                 old['importance'], old['source_ids'], json.dumps(embedding) if embedding else old['embedding'],
                 now, now, 'active', max(0.0, min(1.0, confidence)), memory_id, note or '', '',
                 normalized_fact_key(summary, content), old['source_refs'],
                 str(old['embed_model']) if 'embed_model' in old.keys() else '',
                 _mt, json.dumps(_tags, ensure_ascii=False), reason or '', scenario or '',
                 str(entity_id or (old['entity_id'] if 'entity_id' in old.keys() else ''))))
            _write_tags(conn, new_id, _tags)
            conn.execute("DELETE FROM memories_fts WHERE memory_id IN (?,?)", (memory_id, new_id))
            conn.execute("INSERT INTO memories_fts(summary,content,sid,level,memory_id) VALUES(?,?,?,?,?)",
                         (segment_for_fts(summary), segment_for_fts(content), old['sid'], old['level'], new_id))
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

    async def mark_superseded(self, memory_id: str, note: str) -> bool:
        """标记记忆为 superseded（被合并/修正吸收），保留审计链。"""
        return await asyncio.to_thread(self._mark_superseded, memory_id, note)

    def _mark_superseded(self, memory_id, note):
        conn = self._connect()
        try:
            cur = conn.execute("UPDATE memories SET status='superseded', updated_at=?, correction_note=? WHERE id=? AND deleted=0",
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
                    "users": n("SELECT COUNT(DISTINCT user_id) FROM memories WHERE deleted=0 AND user_id != ''"),
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

    async def count_memories_by_users(self, user_ids: list[str], limit: int = 20) -> list[dict]:
        """批量查询多个用户的跨会话记忆，一次 SQL 代替逐个查询。"""
        return await asyncio.to_thread(self._count_memories_by_users, user_ids, limit)

    def _count_memories_by_users(self, user_ids, limit):
        if not user_ids:
            return []
        conn = self._connect()
        try:
            placeholders = ",".join("?" * len(user_ids))
            rows = conn.execute(
                f"SELECT * FROM memories WHERE user_id IN ({placeholders}) AND deleted=0 AND status='active' "
                "ORDER BY end_ts DESC LIMIT ?",
                [*user_ids, limit * len(user_ids)]
            ).fetchall()
            result = []
            for row in rows:
                result.append(_normalize_item(dict(row)))
            return result
        finally:
            conn.close()

    async def count_users(self) -> int:
        """真实用户总数（去重，排除空）。"""
        return await asyncio.to_thread(self._count_users)

    def _count_users(self):
        conn = self._connect()
        try:
            return int(conn.execute(
                "SELECT COUNT(DISTINCT user_id) FROM memories WHERE deleted=0 AND user_id != ''"
            ).fetchone()[0])
        finally:
            conn.close()

    async def count_sessions(self) -> int:
        """真实会话总数（去重）。"""
        return await asyncio.to_thread(self._count_sessions)

    def _count_sessions(self):
        conn = self._connect()
        try:
            return int(conn.execute(
                "SELECT COUNT(DISTINCT sid) FROM memories WHERE deleted=0 AND sid != ''"
            ).fetchone()[0])
        finally:
            conn.close()

    # ---- 用户/实体画像 ----

    async def get_profile(self, entity_id: str) -> dict | None:
        """读取单个实体画像。"""
        return await asyncio.to_thread(self._get_profile, entity_id)

    def _get_profile(self, entity_id: str):
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM user_profiles WHERE entity_id=?", (entity_id,)).fetchone()
            if not row:
                return None
            return self._normalize_profile(dict(row))
        finally:
            conn.close()

    @staticmethod
    def _normalize_profile(p: dict) -> dict:
        for k in ("traits", "relationships", "facts", "aliases"):
            try:
                p[k] = json.loads(p.get(k) or "[]")
            except (TypeError, json.JSONDecodeError):
                p[k] = []
        try:
            p["preferences"] = json.loads(p.get("preferences") or "{}")
        except (TypeError, json.JSONDecodeError):
            p["preferences"] = {}
        return p

    async def upsert_profile(self, entity_id: str, entity_type: str = "user", **fields) -> bool:
        """写入/更新实体画像（upsert by entity_id）。"""
        return await asyncio.to_thread(self._upsert_profile, entity_id, entity_type, fields)

    def _upsert_profile(self, entity_id, entity_type, fields):
        conn = self._connect()
        try:
            now = time.time()
            existing = conn.execute("SELECT * FROM user_profiles WHERE entity_id=?", (entity_id,)).fetchone()
            if existing:
                conn.execute(
                    "UPDATE user_profiles SET entity_type=?, name=?, nickname=?, description=?, platform=?, "
                    "traits=?, preferences=?, relationships=?, facts=?, aliases=?, interaction_count=?, "
                    "last_interaction=?, updated_at=?, generated_from=?, memory_type=? WHERE entity_id=?",
                    (fields.get("entity_type", existing["entity_type"]), fields.get("name", existing["name"]),
                     fields.get("nickname", existing["nickname"]), fields.get("description", existing["description"]),
                     fields.get("platform", existing["platform"]),
                     json.dumps(fields.get("traits", json.loads(existing["traits"] or "[]")), ensure_ascii=False),
                     json.dumps(fields.get("preferences", json.loads(existing["preferences"] or "{}")), ensure_ascii=False),
                     json.dumps(fields.get("relationships", json.loads(existing["relationships"] or "[]")), ensure_ascii=False),
                     json.dumps(fields.get("facts", json.loads(existing["facts"] or "[]")), ensure_ascii=False),
                     json.dumps(fields.get("aliases", json.loads(existing["aliases"] or "[]")), ensure_ascii=False),
                     int(fields.get("interaction_count", existing["interaction_count"])),
                     float(fields.get("last_interaction", existing["last_interaction"])),
                     now, fields.get("generated_from", existing["generated_from"]),
                     fields.get("memory_type", existing["memory_type"]) or "", entity_id))
            else:
                conn.execute(
                    "INSERT OR REPLACE INTO user_profiles(entity_id,entity_type,name,nickname,description,platform,traits,preferences,relationships,facts,aliases,interaction_count,last_interaction,updated_at,generated_from,memory_type) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (entity_id, fields.get("entity_type", entity_type), fields.get("name", ""),
                     fields.get("nickname", ""), fields.get("description", ""), fields.get("platform", ""),
                     json.dumps(fields.get("traits", []), ensure_ascii=False),
                     json.dumps(fields.get("preferences", {}), ensure_ascii=False),
                     json.dumps(fields.get("relationships", []), ensure_ascii=False),
                     json.dumps(fields.get("facts", []), ensure_ascii=False),
                     json.dumps(fields.get("aliases", []), ensure_ascii=False),
                     int(fields.get("interaction_count", 0)), float(fields.get("last_interaction", now)),
                     now, fields.get("generated_from", ""), fields.get("memory_type", "")))
            conn.commit()
            return True
        finally:
            conn.close()

    async def list_profiles(self, limit: int = 100, entity_type: str | None = None) -> list[dict]:
        """列出所有画像（默认 user；可过滤 group/bot）。"""
        return await asyncio.to_thread(self._list_profiles, limit, entity_type)

    def _list_profiles(self, limit, entity_type):
        conn = self._connect()
        try:
            if entity_type:
                rows = conn.execute("SELECT * FROM user_profiles WHERE entity_type=? ORDER BY last_interaction DESC LIMIT ?",
                                    (entity_type, limit)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM user_profiles ORDER BY last_interaction DESC LIMIT ?", (limit,)).fetchall()
            return [self._normalize_profile(dict(r)) for r in rows]
        finally:
            conn.close()

    async def search_profiles(self, keyword: str, limit: int = 20) -> list[dict]:
        """按实体 id/name/nickname 模糊搜画像。"""
        return await asyncio.to_thread(self._search_profiles, keyword, limit)

    def _search_profiles(self, keyword, limit):
        conn = self._connect()
        try:
            like = f"%{keyword}%"
            rows = conn.execute(
                "SELECT * FROM user_profiles WHERE entity_id LIKE ? OR name LIKE ? OR nickname LIKE ? LIMIT ?",
                (like, like, like, limit)).fetchall()
            return [self._normalize_profile(dict(r)) for r in rows]
        finally:
            conn.close()

    # ---- 关系网络 ----

    async def upsert_relationship(self, entity_id: str, target_id: str, relation: str,
                                  direction: str = "direct", source_mid: str = "",
                                  confidence: float = 0.7) -> str:
        """写入/更新一条关系（去重：同 entity+target+relation）。返回 relation id。"""
        return await asyncio.to_thread(self._upsert_relationship, entity_id, target_id, relation,
                                       direction, source_mid, confidence)

    def _upsert_relationship(self, entity_id, target_id, relation, direction, source_mid, confidence):
        conn = self._connect()
        try:
            now = time.time()
            existing = conn.execute(
                "SELECT id, source_mid, confidence FROM relationships WHERE entity_id=? AND target_id=? AND relation=?",
                (entity_id, target_id, relation)).fetchone()
            if existing:
                rid = existing["id"]
                # 合并来源 + 取更高置信度
                merged_src = (existing["source_mid"] or "")
                if source_mid and source_mid not in merged_src:
                    merged_src = (merged_src + "," + source_mid).strip(",")
                merged_conf = max(float(existing["confidence"]), float(confidence))
                conn.execute(
                    "UPDATE relationships SET source_mid=?, confidence=?, updated_at=? WHERE id=?",
                    (merged_src, merged_conf, now, rid))
            else:
                rid = f"rel-{int(now)}-{uuid.uuid4().hex[:8]}"
                conn.execute(
                    "INSERT OR REPLACE INTO relationships(id,entity_id,target_id,relation,direction,source_mid,confidence,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (rid, entity_id, target_id, relation, direction or "direct", source_mid,
                     max(0.0, min(1.0, confidence)), now, now))
            conn.commit()
            return rid
        finally:
            conn.close()

    async def list_relationships(self, entity_id: str | None = None, target_id: str | None = None) -> list[dict]:
        """列出关系网络。可按主体或对象过滤。"""
        return await asyncio.to_thread(self._list_relationships, entity_id, target_id)

    def _list_relationships(self, entity_id, target_id):
        conn = self._connect()
        try:
            if entity_id and target_id:
                rows = conn.execute("SELECT * FROM relationships WHERE entity_id=? AND target_id=? ORDER BY confidence DESC", (entity_id, target_id)).fetchall()
            elif entity_id:
                rows = conn.execute("SELECT * FROM relationships WHERE entity_id=? ORDER BY confidence DESC", (entity_id,)).fetchall()
            elif target_id:
                rows = conn.execute("SELECT * FROM relationships WHERE target_id=? ORDER BY confidence DESC", (target_id,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM relationships ORDER BY confidence DESC LIMIT 500").fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    async def list_users(self, limit: int = 50) -> list[dict]:
        """列出所有被记住的用户及其记忆概况。"""
        return await asyncio.to_thread(self._list_users, limit)

    def _list_users(self, limit):
        conn = self._connect()
        try:
            now = time.time()
            rows = conn.execute(
                "SELECT user_id, COUNT(*) AS cnt, MAX(end_ts) AS last_ts, MAX(level) AS max_level, "
                "MAX(importance) AS max_importance "
                "FROM memories WHERE deleted=0 AND user_id != '' AND status='active' "
                "GROUP BY user_id "
                "ORDER BY (MAX(importance)*0.5 + (1.0/(1.0+MAX(0.0,(?-MAX(end_ts))/86400.0)))*0.3 "
                "         + (COUNT(*)*1.0/(COUNT(*)+10.0))*0.2) DESC LIMIT ?",
                (now, limit)
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                uid = item["user_id"]
                # 最近几条（按时间倒序，默认3条）
                recent = conn.execute(
                    "SELECT summary, end_ts FROM memories WHERE user_id=? AND deleted=0 AND status='active' "
                    "ORDER BY end_ts DESC LIMIT 3",
                    (uid,)
                ).fetchall()
                item["recent_summaries"] = [r["summary"] for r in recent]
                item["recent_summary"] = item["recent_summaries"][0] if item["recent_summaries"] else ""
                # 最高权重×层级 那条
                top = conn.execute(
                    "SELECT summary, level, importance, end_ts FROM memories WHERE user_id=? AND deleted=0 AND status='active' "
                    "ORDER BY (importance * 0.6 + level * 0.4) DESC, end_ts DESC LIMIT 1",
                    (uid,)
                ).fetchone()
                if top:
                    item["top_summary"] = top["summary"]
                    item["top_level"] = top["level"]
                    item["top_importance"] = top["importance"]
                else:
                    item["top_summary"] = ""
                    item["top_level"] = item.get("max_level", 1)
                    item["top_importance"] = 0.0
                result.append(item)
            return result
        finally:
            conn.close()

    async def count_memories_by_user(self, user_id: str) -> int:
        """某用户的真实记忆总数（去删除）。"""
        return await asyncio.to_thread(self._count_memories_by_user, user_id)

    def _count_memories_by_user(self, user_id):
        conn = self._connect()
        try:
            return int(conn.execute(
                "SELECT COUNT(*) FROM memories WHERE user_id=? AND deleted=0",
                (user_id,)
            ).fetchone()[0])
        finally:
            conn.close()

    async def count_memories_by_sid(self, sid: str) -> int:
        """某会话的真实记忆总数（去删除）。"""
        return await asyncio.to_thread(self._count_memories_by_sid, sid)

    def _count_memories_by_sid(self, sid):
        conn = self._connect()
        try:
            return int(conn.execute(
                "SELECT COUNT(*) FROM memories WHERE sid=? AND deleted=0",
                (sid,)
            ).fetchone()[0])
        finally:
            conn.close()

    async def list_memories_by_user(self, user_id: str, limit: int = 20, include_archived: bool = False) -> list[dict]:
        """按用户精确列出其全部记忆（跨会话）。按 高权重×高层级 优先，兼顾最近。"""
        return await asyncio.to_thread(self._list_memories_by_user, user_id, limit, include_archived)

    def _list_memories_by_user(self, user_id, limit, include_archived):
        conn = self._connect()
        try:
            status_sql = "IN ('active','archived')" if include_archived else "='active'"
            rows = conn.execute(
                f"SELECT * FROM memories WHERE user_id=? AND deleted=0 AND status{status_sql} "
                "ORDER BY (importance * 0.6 + level * 0.4) DESC, end_ts DESC LIMIT ?",
                (user_id, limit)
            ).fetchall()
            result = []
            for row in rows:
                result.append(_normalize_item(dict(row)))
            return result
        finally:
            conn.close()

    async def list_memories_by_entity(self, entity_id: str, limit: int = 200) -> list[dict]:
        """按实体检索记忆：entity_id 列 或 user_id 列 命中（画像聚合用）。跨会话跨用户。"""
        return await asyncio.to_thread(self._list_memories_by_entity, entity_id, limit)

    def _list_memories_by_entity(self, entity_id, limit):
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM memories WHERE deleted=0 AND status='active' "
                "AND (entity_id=? OR user_id=?) "
                "ORDER BY (importance * 0.6 + level * 0.4) DESC, end_ts DESC LIMIT ?",
                (entity_id, entity_id, limit)).fetchall()
            result = []
            for row in rows:
                result.append(_normalize_item(dict(row)))
            return result
        finally:
            conn.close()

    # ---- 后台审计调度（轮转）----
    async def pick_reflection_candidates(self, limit: int = 20, period_days: float = 3.0) -> list[dict]:
        """全局池挑选"应审计"的记忆：从未审过 或 距上次审已超周期。
        排序：reflect_count 升序(审得越少越优先) → importance 降序 → 时间降序。
        返回的项带 reflect_count / reflect_last_ts，供审计流程记账。"""
        return await asyncio.to_thread(self._pick_reflection_candidates, limit, period_days)

    def _pick_reflection_candidates(self, limit, period_days):
        conn = self._connect()
        try:
            cutoff = time.time() - float(period_days) * 86400.0
            rows = conn.execute(
                "SELECT * FROM memories WHERE deleted=0 AND status='active' "
                "AND (reflect_count=0 OR reflect_last_ts=0 OR reflect_last_ts < ?) "
                "ORDER BY reflect_count ASC, importance DESC, end_ts DESC LIMIT ?",
                (cutoff, limit)).fetchall()
            result = []
            for row in rows:
                result.append(_normalize_item(dict(row)))
            return result
        finally:
            conn.close()

    async def mark_reflected(self, memory_id: str, reflect_count: int, last_ts: float) -> None:
        """审计记账：更新某记忆的审计次数和上次审计时间。"""
        return await asyncio.to_thread(self._mark_reflected, memory_id, reflect_count, last_ts)

    def _mark_reflected(self, memory_id, reflect_count, last_ts):
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE memories SET reflect_count=?, reflect_last_ts=? WHERE id=?",
                (int(reflect_count), float(last_ts), memory_id))
            conn.commit()
        finally:
            conn.close()

    async def find_similar_memories(self, summary: str, content: str, entity_id: str = "",
                                    limit: int = 3, exclude_id: str = "") -> list[dict]:
        """FTS5 语义召回同实体的相似记忆（去重/审计用，纯本地，不依赖向量）。"""
        return await asyncio.to_thread(self._find_similar_memories, summary, content, entity_id, limit, exclude_id)

    def _find_similar_memories(self, summary, content, entity_id, limit, exclude_id):
        conn = self._connect()
        try:
            # 基础过滤：同实体 + active + 非自身
            where = ["m.deleted=0 AND m.status='active'"]
            params = []
            if entity_id:
                where.append("(m.entity_id=? OR m.user_id=?)")
                params.extend([entity_id, entity_id])
            if exclude_id:
                where.append("m.id<>?")
                params.append(exclude_id)
            base_where = " AND ".join(where)
            query = (summary + " " + content)[:200]
            # 分词 + 拆字，得到候选 term（用于 LIKE 兜底）
            import re as _re
            terms = tokenize(query) or []
            like_terms = list(dict.fromkeys(terms[:8]))
            for ch in str(query):
                if ch.strip() and ('\u4e00' <= ch <= '\u9fff') and ch not in like_terms:
                    like_terms.append(ch)
            like_terms = like_terms[:12]
            # 1) FTS5 主召回
            ftss = []
            fts_query = build_fts_query(query)
            if fts_query:
                try:
                    rows = conn.execute(
                        f"SELECT m.*, bm25(memories_fts) AS bm25_score FROM memories_fts f "
                        f"JOIN memories m ON m.id=f.memory_id "
                        f"WHERE memories_fts MATCH ? AND {base_where} ORDER BY bm25_score LIMIT {int(limit)}",
                        [fts_query, *params]).fetchall()
                    ftss = [dict(r) for r in rows]
                except Exception:
                    ftss = []
            # 2) LIKE 兜底：FTS 命中不足时补（短文本 FTS 常召回不到，靠 LIKE 提高召回）
            if len(ftss) < limit and like_terms:
                like_terms_all = [f"%{t}%" for t in like_terms]
                like_sql = " OR ".join("(m.summary LIKE ? OR m.content LIKE ?)" for _ in like_terms_all)
                like_rows = conn.execute(
                    f"SELECT m.*, 0.0 AS bm25_score FROM memories m WHERE {base_where} AND ({like_sql}) "
                    f"ORDER BY m.importance DESC, m.end_ts DESC LIMIT {int(limit)}",
                    [*params, *sum(([x, x] for x in like_terms_all), [])]).fetchall()
                ftss.extend(dict(r) for r in like_rows)
            # 3) 去重 + 用 token 重叠算 similarity（不依赖 bm25，短文本更可靠）
            seen = set()
            result = []
            for row in ftss:
                item = _normalize_item(row)
                mid = item["id"]
                if mid in seen:
                    continue
                seen.add(mid)
                # token 重叠相似度：query 的 term 里有多少出现在候选的 summary/content
                q_terms = set(t for t in tokenize(query) if t)
                cand_text = (item.get("summary", "") + " " + item.get("content", ""))
                if q_terms:
                    overlap = sum(1 for t in q_terms if t in cand_text)
                    item["similarity"] = round(overlap / len(q_terms), 3)
                else:
                    item["similarity"] = 0.0
                result.append(item)
            # 按 similarity 降序 + importance 降序
            result.sort(key=lambda x: (x.get("similarity", 0), x.get("importance", 0)), reverse=True)
            return result[:limit]
        finally:
            conn.close()

    async def list_memories_by_sid(self, sid: str, limit: int = 20, include_archived: bool = False) -> list[dict]:
        """按会话（群聊）精确列出其记忆。按 高权重×高层级 优先，兼顾最近。"""
        return await asyncio.to_thread(self._list_memories_by_sid, sid, limit, include_archived)

    def _list_memories_by_sid(self, sid, limit, include_archived):
        conn = self._connect()
        try:
            status_sql = "IN ('active','archived')" if include_archived else "='active'"
            rows = conn.execute(
                f"SELECT * FROM memories WHERE sid=? AND deleted=0 AND status{status_sql} "
                "ORDER BY (importance * 0.6 + level * 0.4) DESC, end_ts DESC LIMIT ?",
                (sid, limit)
            ).fetchall()
            result = []
            for row in rows:
                result.append(_normalize_item(dict(row)))
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
