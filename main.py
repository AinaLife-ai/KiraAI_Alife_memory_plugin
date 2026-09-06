"""Alife Memory for KiraAI — 异步分层记忆 + 三重触发 + 被动召回 + 事件还原."""
from __future__ import annotations

import asyncio
import hashlib
import json
import random
import re
import time
from pathlib import Path
from typing import Any

from fastapi import Request

from core.plugin import BasePlugin, PageMenu, PluginPage, Priority, logger, on, register
from core.chat import MessageChain
from core.chat.message_elements import Text
from core.chat.message_utils import KiraMessageBatchEvent, KiraMessageEvent
from core.prompt_manager import Prompt
from core.provider import LLMRequest
from core.agent.message import OpenAIMessage
from core.utils.path_utils import get_config_path

from .storage import MemoryStore, estimate_tokens


PLUGIN_ID = "alife_memory_z"


def _num(value, default, low, high, integer=False):
    try:
        value = int(value) if integer else float(value)
    except (TypeError, ValueError):
        value = default
    return max(low, min(high, value))


def _event_user_id(message: Any) -> str:
    sender = getattr(message, "sender", None)
    if sender is None and isinstance(message, dict):
        sender = message.get("sender")
    value = getattr(sender, "user_id", None) if sender is not None else None
    if value is None and isinstance(sender, dict):
        value = sender.get("user_id") or sender.get("user_id")
    return str(value or "")


def _text_from_message(message: Any) -> str:
    if message is None:
        return ""
    chain = getattr(message, "chain", None)
    if chain is not None:
        out = []
        for element in chain:
            if isinstance(element, Text):
                out.append(element.text)
            elif getattr(element, "text", None):
                out.append(str(element.text))
        if out:
            return "".join(out).strip()
    for attr in ("content", "text", "raw_message"):
        value = getattr(message, attr, None)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _json_object(text: str) -> dict[str, Any] | None:
    text = (text or "").strip()
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.I | re.S).strip()
    try:
        value = json.loads(text)
        return value if isinstance(value, dict) else None
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.S)
        if not match:
            return None
        try:
            value = json.loads(match.group(0))
            return value if isinstance(value, dict) else None
        except json.JSONDecodeError:
            return None


class AlifeMemoryPlugin(BasePlugin):
    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)
        self.cfg = cfg
        self._tasks: set[asyncio.Task] = set()
        self._queues: dict[str, asyncio.Queue] = {}
        self._workers: dict[str, asyncio.Task] = {}
        self._reflect_task: asyncio.Task | None = None
        self._cleanup_task: asyncio.Task | None = None
        self._closed = False
        self._seen_user_ids: set[str] = set()
        self._load_settings()
        # 数据目录迁移：旧 alife_memory → alife_memory_z
        data_dir = Path(ctx.get_plugin_data_dir())
        old_data_dir = data_dir.parent / "alife_memory"
        if old_data_dir.exists() and not data_dir.exists():
            logger.info("[alife_memory] 检测到旧数据目录 %s，迁移到 %s", old_data_dir, data_dir)
            data_dir.parent.mkdir(parents=True, exist_ok=True)
            import shutil
            shutil.move(str(old_data_dir), str(data_dir))
        self.store = MemoryStore(data_dir / "memory.sqlite3")
        self._config_path = get_config_path() / "plugins" / f"{PLUGIN_ID}.json"

    def _load_settings(self):
        basic = self.cfg.get("section_basic", {}) or {}
        compression = self.cfg.get("section_compression", {}) or {}
        trig = self.cfg.get("section_trigger", {}) or {}
        retrieval = self.cfg.get("section_retrieval", {}) or {}
        reflection = self.cfg.get("section_reflection", {}) or {}

        self.enabled = bool(basic.get("enabled", True))
        self.capture_enabled = bool(basic.get("capture_enabled", True))
        self.auto_inject = bool(basic.get("auto_inject", True))
        self.passive_recall = bool(basic.get("passive_recall", True))
        self.max_injected_tokens = _num(basic.get("max_injected_tokens", 1500), 1500, 100, 8000, True)
        self.max_injected_items = _num(basic.get("max_injected_items", 8), 8, 1, 30, True)
        self.max_injected_chars = _num(basic.get("max_injected_chars", 3000), 3000, 200, 20000, True)
        self.passive_recall_boost_threshold = _num(basic.get("passive_recall_boost_threshold", 0.4), 0.4, 0.0, 1.0)

        self.compress_model = str(compression.get("compress_model", "") or "").strip()
        self.reflect_model = str(reflection.get("reflect_model", "") or "").strip()
        self.compression_batch = _num(compression.get("batch_size", 10), 10, 2, 80, True)
        self.max_level = _num(compression.get("max_level", 5), 5, 1, 12, True)
        self.level_group_size = _num(compression.get("level_group_size", 4), 4, 2, 12, True)
        self.compress_min_chars = _num(compression.get("min_chars", 80), 80, 20, 2000, True)

        self.trigger_mode = str(trig.get("trigger_mode", "rounds") or "rounds").lower()
        self.round_threshold = _num(trig.get("round_threshold", 10), 10, 1, 200, True)
        self.token_threshold = _num(trig.get("token_threshold", 10000), 10000, 100, 100000, True)
        self.message_threshold = _num(trig.get("message_threshold", 50), 50, 2, 500, True)

        self.reflect_enabled = bool(reflection.get("enabled", True))
        self.reflect_interval = _num(reflection.get("interval_seconds", 1800), 1800, 300, 86400, True)
        self.reflect_max_items = _num(reflection.get("max_items", 4), 4, 1, 20, True)

        self.message_retention_days = _num(basic.get("message_retention_days", 7), 7, 1, 365, True)
        self.stale_retention_days = _num(basic.get("stale_retention_days", 30), 30, 1, 730, True)

        self.retrieval_top_k = _num(retrieval.get("top_k", 5), 5, 1, 12, True)
        self.half_life = _num(retrieval.get("recency_half_life_days", 45), 45, 1, 3650)
        self.semantic_enabled = bool(retrieval.get("semantic_enabled", True))
        self.search_scope = str(retrieval.get("search_scope", "linked") or "linked").lower()
        if self.search_scope not in ("session", "linked", "global"):
            self.search_scope = "linked"
        self.cross_user_enabled = bool(retrieval.get("cross_user_enabled", True))
        self.inject_level_max = _num(retrieval.get("inject_level_max", 12), 12, 0, 12, True)

        self.compress_prompt = str(compression.get("prompt", "") or "").strip() or self._default_compress_prompt()
        self.reflect_prompt = str(reflection.get("prompt", "") or "").strip() or self._default_reflect_prompt()
        self.compress_probability = _num(compression.get("compress_probability", 0.8), 0.8, 0.0, 1.0)
        self.inject_context_marker = bool(basic.get("inject_context_marker", True))
        self.recall_hint_keywords = list(basic.get("recall_hint_keywords", ["记得", "回忆", "以前", "上次", "忘记"])) if isinstance(basic.get("recall_hint_keywords"), list) else []
        self.max_injected_lines = _num(basic.get("max_injected_lines", 30), 30, 5, 100, True)
        self.auto_archive_days = _num(basic.get("auto_archive_days", 0), 0, 0, 3650, True)
        # archive_level_min 默认 = max_level（最高层保护，其余层级可归档）
        # 必须在 max_level 已赋值后才设置
        self.archive_level_min = _num(basic.get("archive_level_min", self.max_level), 1, 1, 12, True)

    @staticmethod
    def _default_compress_prompt():
        return ("你是长期记忆整理器。把{range}中提取成可验证、简洁、无重复的记忆。\n"
                "只输出 JSON，不要 Markdown：{\"summary\":\"一句话概述\",\"content\":\"事实、偏好、决定和关系变化，分行列出\",\"importance\":0.0}\n"
                "不要臆测，不要把闲聊或临时情绪写成长期事实；保留时间、主体和限定条件。\n待整理内容：\n{content}")

    @staticmethod
    def _default_reflect_prompt():
        return ("你是记忆审校器。比较旧记忆和新证据，只处理有明确矛盾的事实。\n"
                "只输出 JSON：{\"action\":\"none|correct|stale\",\"summary\":\"修正后的概述\",\"content\":\"修正后的事实\",\"confidence\":0.0,\"reason\":\"证据依据\"}\n"
                "若只是措辞不同、证据不足或可能是临时状态，输出 none。不得凭空补全。\n旧记忆：\n{memory}\n新证据：\n{evidence}")

    @staticmethod
    def _convert_relative_dates(text: str) -> str:
        """把描述性日期转换为绝对日期"""
        import re as _re
        now = time.localtime()
        today = time.strftime("%Y-%m-%d", now)
        # "昨天" → 日期
        text = _re.sub(r"昨天", time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400)), text)
        text = _re.sub(r"前天", time.strftime("%Y-%m-%d", time.localtime(time.time() - 172800)), text)
        text = _re.sub(r"今天", today, text)
        text = _re.sub(r"明天", time.strftime("%Y-%m-%d", time.localtime(time.time() + 86400)), text)
        text = _re.sub(r"后天", time.strftime("%Y-%m-%d", time.localtime(time.time() + 172800)), text)
        now_wday = (now.tm_wday + 1) % 7  # 0=周一
        # 单字匹配（"上周五"、"这周三"）
        for prefix, offset in [("这周|这个", 0), ("下周|下个", 7), ("上周|上个", -7)]:
            for ch, idx in [("一",0),("二",1),("三",2),("四",3),("五",4),("六",5),("日",6)]:
                text = _re.sub(f"(?:{prefix}){ch}",
                               time.strftime("%Y-%m-%d", time.localtime(time.time() + (idx - now_wday + offset) * 86400)), text)
        # 双字匹配（"周五"、"星期一"，单字匹配之后的双字可能已被部分替换，但安全的）
        for prefix, offset in [("这周|这个", 0), ("下周|下个", 7), ("上周|上个", -7)]:
            for wk in ["周一","周二","周三","周四","周五","周六","周日",
                       "星期一","星期二","星期三","星期四","星期五","星期六","星期日"]:
                idx = {"星期一":0,"星期二":1,"星期三":2,"星期四":3,"星期五":4,"星期六":5,"星期日":6,
                        "周一":0,"周二":1,"周三":2,"周四":3,"周五":4,"周六":5,"周日":6}[wk]
                text = _re.sub(f"(?:{prefix}){_re.escape(wk)}",
                               time.strftime("%Y-%m-%d", time.localtime(time.time() + (idx - now_wday + offset) * 86400)), text)
        # "X天后" "X天后" 等
        text = _re.sub(r"(\d+)\s*天(?:后|之后|以后)",
                       lambda m: time.strftime("%Y-%m-%d", time.localtime(time.time() + int(m.group(1)) * 86400)), text)
        text = _re.sub(r"(\d+)\s*天(?:前|之前|以前)",
                       lambda m: time.strftime("%Y-%m-%d", time.localtime(time.time() - int(m.group(1)) * 86400)), text)
        text = _re.sub(r"(?:下|这个)\s*(?:个)?\s*(?:月|月(?:底|初))",
                       lambda m: time.strftime("%Y-%m", time.localtime(time.time())), text)
        # "X月X号" "X月X日" → 加年份
        text = _re.sub(r"(\d{1,2})月(\d{1,2})(?:号|日)",
                       lambda m: f"{time.strftime('%Y', now)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}", text)
        # 季度
        text = _re.sub(r"本季度|这个季度",
                       lambda m: f"Q{(now.tm_mon - 1) // 3 + 1} {time.strftime('%Y', now)}", text)
        return text

    async def initialize(self):
        self._closed = False
        # 互斥检测+迁移放入后台任务，避免阻塞初始化进程导致 WebUI 安装超时
        self._tasks.add(asyncio.create_task(self._mutual_exclusion()))
        if self.reflect_enabled:
            self._reflect_task = asyncio.create_task(self._reflection_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("[alife_memory] initialized: triple-trigger compression, passive recall, linked retrieval")

    async def _mutual_exclusion(self):
        """检测冲突的记忆插件并自动关闭，同时发起迁移"""
        migrated = False
        conflict_plugins = {
            "kira_plugin_simple_memory": {"name": "Simple Memory（内置）", "data_file": "memory/core.txt"},
            "kira_plugin_kiraos": {"name": "KiraOS / 海马体记忆", "data_file": None},
        }
        try:
            ctx = self.ctx
            pm = getattr(ctx, "plugin_mgr", None)
            if not pm:
                return  # 无法获取插件管理器时跳过
            for plugin_id, info in conflict_plugins.items():
                plugin_inst = pm.get_plugin_inst(plugin_id)
                if plugin_inst is None:
                    continue  # 未安装，跳过
                is_enabled = pm.is_plugin_enabled(plugin_id) if hasattr(pm, "is_plugin_enabled") else True
                if not is_enabled:
                    continue  # 已禁用，跳过
                logger.info("[alife_memory] 检测到冲突插件 %s (%s)，正在自动禁用并迁移其记忆数据……", plugin_id, info["name"])
                # 记忆迁移（禁用前读取数据）
                migrated |= await self._migrate_from(plugin_id, info, pm, plugin_inst)
                # 自动禁用
                if hasattr(pm, "set_plugin_enabled"):
                    await pm.set_plugin_enabled(plugin_id, False)
                    logger.info("[alife_memory] 已自动禁用冲突插件 %s (%s)", plugin_id, info["name"])
        except Exception as exc:
            logger.warning("[alife_memory] 互斥检测/禁用失败: %s", exc)

    async def _migrate_from(self, plugin_id: str, info: dict, pm, plugin_inst) -> bool:
        """从指定插件迁移记忆。返回是否有数据导入。"""
        migrated = False
        try:
            # 确定数据目录
            from core.utils.path_utils import get_data_path
            data_root = Path(get_data_path())

            if plugin_id == "kira_plugin_simple_memory":
                # Simple Memory: data/memory/core.txt，单文件逐行
                core_txt = data_root / "memory" / "core.txt"
                if core_txt.exists():
                    logger.info("[alife_memory] 发现 Simple Memory 数据文件: %s，正在迁移……", core_txt)
                    raw_text = core_txt.read_text(encoding="utf-8", errors="replace")
                    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
                    if lines:
                        count = 0
                        for line in lines:
                            content_stripped = line
                            vector = await self._embed(content_stripped)
                            fp = hashlib.sha256(("simple_memory_import|" + content_stripped).encode()).hexdigest()[:32]
                            try:
                                await self.store.add_memory(
                                    sid="system", level=3, summary=content_stripped[:100],
                                    content=content_stripped, start_ts=time.time(),
                                    end_ts=time.time(), source_ids=[], importance=0.5,
                                    embedding=vector, user_id="",
                                    source_fingerprint=fp,
                                    source_refs=[{"sid": "simple_memory_import",
                                                  "user_id": "", "ts": time.time(),
                                                  "action": "从Simple Memory自动迁移，原始文件保留未清理"}])
                                count += 1
                            except Exception:
                                pass
                        if count:
                            migrated = True
                            logger.info("[alife_memory] 已从 Simple Memory 迁移 %d 条记忆（原始文件未删除）", count)

            elif plugin_id == "kira_plugin_kiraos":
                # KiraOS: data/memory/entities/ 目录下的 TOML 文件
                kiraos_entities = data_root / "memory" / "entities"
                if await asyncio.to_thread(kiraos_entities.exists):
                    toml_files = await asyncio.to_thread(lambda: list(kiraos_entities.rglob("*.toml")))
                    if toml_files:
                        count = 0
                        for tf in toml_files:
                            try:
                                text = await asyncio.to_thread(lambda: tf.read_text(encoding="utf-8", errors="replace"))
                                import re as _re
                                t_id = _re.search(r'id\s*=\s*"([^"]*)"', text)
                                t_text_match = _re.search(r'text\s*=\s*"([^"]*)"', text)
                                t_type = _re.search(r'type\s*=\s*"([^"]*)"', text)
                                t_imp = _re.search(r'importance\s*=\s*(\d+)', text)
                                t_sid = _re.search(r'session\s*=\s*"([^"]*)"', text)
                                t_ts = _re.search(r'time\s*=\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', text)
                                if not t_text_match:
                                    continue
                                content = t_text_match.group(1)
                                summary = content[:100]
                                importance = float(t_imp.group(1)) / 10.0 if t_imp else 0.5
                                source_sid = t_sid.group(1) if t_sid else "kiraos_import"
                                vector = await self._embed(content)
                                from datetime import datetime as _dt
                                try:
                                    ts_val = _dt.fromisoformat(t_ts.group(1)).timestamp() if t_ts else time.time()
                                except Exception:
                                    ts_val = time.time()
                                fp = hashlib.sha256(("kiraos_import|" + content).encode()).hexdigest()[:32]
                                try:
                                    await self.store.add_memory(
                                        sid=source_sid, level=3, summary=summary,
                                        content=content, start_ts=ts_val, end_ts=ts_val,
                                        source_ids=[], importance=importance,
                                        embedding=vector, user_id="",
                                        source_fingerprint=fp,
                                        source_refs=[{"sid": source_sid,
                                                      "user_id": "", "ts": ts_val,
                                                      "action": "从KiraOS记忆自动迁移，原始文件保留未清理"}])
                                    count += 1
                                except Exception:
                                    pass
                            except Exception:
                                continue
                        if count:
                            migrated = True
                            logger.info("[alife_memory] 已从 KiraOS 记忆迁移 %d 条记忆（原始文件未删除）", count)
        except Exception as exc:
            logger.warning("[alife_memory] 从 %s 迁移失败: %s", plugin_id, exc)
        return migrated

    async def terminate(self):
        self._closed = True
        tasks = list(self._workers.values()) + ([self._reflect_task] if self._reflect_task else []) + list(self._tasks)
        if self._cleanup_task: tasks.append(self._cleanup_task)
        for task in tasks:
            if task and not task.done():
                task.cancel()
        for task in tasks:
            if task:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.exception("[alife_memory] background task shutdown failed")
        self._workers.clear()
        self._reflect_task = None
        await self.store.close()

    def _track(self, coro):
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def _client(self, model: str):
        try:
            return self.ctx.get_llm_client(model_uuid=model) if model else self.ctx.get_default_llm_client()
        except Exception:
            return None

    async def _llm_json(self, prompt: str, model: str) -> dict[str, Any] | None:
        client = self._client(model)
        if not client:
            return None
        try:
            response = await client.chat(LLMRequest(messages=[OpenAIMessage(role="user", content=prompt)]))
            return _json_object(getattr(response, "text_response", "") or "")
        except Exception as exc:
            logger.warning("[alife_memory] model request failed: %s", exc)
            return None

    async def _embed(self, text: str) -> list[float] | None:
        if not self.semantic_enabled:
            return None
        try:
            client = self.ctx.get_default_embedding_client()
            if not client:
                return None
            result = await client.embed([text])
            vector = result[0] if isinstance(result, list) and result else result
            return [float(x) for x in vector] if vector else None
        except Exception as exc:
            logger.debug("[alife_memory] embedding unavailable: %s", exc)
            return None

    async def _remember_turn(self, sid: str, user_messages: list[dict[str, Any]], assistant_text: str,
                             ts: float, turn_key: str = ""):
        if self._closed or not self.capture_enabled or not user_messages or not assistant_text.strip():
            return
        if await self.store.add_batch(sid, user_messages, assistant_text, ts, turn_key=turn_key):
            self._enqueue(sid)

    def _enqueue(self, sid: str, priority: bool = False):
        queue = self._queues.setdefault(sid, asyncio.Queue(maxsize=2))
        try:
            queue.put_nowait(priority)
        except asyncio.QueueFull:
            pass
        if sid not in self._workers or self._workers[sid].done():
            self._workers[sid] = asyncio.create_task(self._compression_worker(sid))

    async def _compression_worker(self, sid: str):
        queue = self._queues[sid]
        try:
            while not self._closed:
                priority = await queue.get()
                await self._compress_available(sid, bool(priority))
                if queue.empty():
                    break
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[alife_memory] compression worker failed for %s", sid)
        finally:
            if self._workers.get(sid) is asyncio.current_task():
                self._workers.pop(sid, None)

    async def _compress_available(self, sid: str, priority: bool = False):
        pending = await self.store.pending_messages(sid, self.compression_batch)
        if not pending:
            return
        stats = await self.store.pending_stats(sid)
        should = priority
        if not should:
            if self.trigger_mode in ("rounds", "either") and stats["round_count"] >= self.round_threshold:
                should = True
            if self.trigger_mode in ("tokens", "either") and stats["token_sum"] >= self.token_threshold:
                should = True
            if self.trigger_mode in ("messages", "either") and stats["message_count"] >= self.message_threshold:
                should = True
        if not should:
            return
        while len(pending) >= self.compression_batch or (priority and pending):
            batch = pending[:self.compression_batch]
            content = "\n".join(f"[{x['role']} user={x.get('user_id', '') or 'unknown'} turn={x.get('turn_no', 0)}] {x['content']}" for x in batch)
            if len(content) < self.compress_min_chars:
                return
            # 概率扰动：避免每次触发都压缩
            if self.compress_probability < 1.0 and random.random() > self.compress_probability:
                return
            # 构建 range 描述
            batch_start = time.strftime('%Y-%m-%d %H:%M', time.localtime(batch[0]['ts']))
            batch_end = time.strftime('%Y-%m-%d %H:%M', time.localtime(batch[-1]['ts']))
            range_desc = f"从 {batch_start} 到 {batch_end} 期间的对话"
            task_id = await self.store.create_task(sid, "compress", "压缩对话为长期记忆")
            try:
                result = await self._llm_json(self.compress_prompt.replace("{range}", range_desc).replace("{content}", content), self.compress_model or "fast")
                summary = str((result or {}).get("summary", "")).strip()
                detail = str((result or {}).get("content", "")).strip()
                if not summary or not detail:
                    await self.store.update_task(task_id, "failed", "模型未返回有效结构化记忆")
                    return
                importance = _num((result or {}).get("importance", 0.55), 0.55, 0.0, 1.0)
                vector = await self._embed(summary + "\n" + detail)
                fingerprint = hashlib.sha256((sid + "|" + "|".join(x["id"] for x in batch)).encode()).hexdigest()
                refs = [{"sid": sid, "user_id": str(x.get("user_id", "")), "message_id": x["id"], "turn_no": x.get("turn_no", 0), "ts": x["ts"]} for x in batch]
                archive_id = await self.store.add_memory(
                    sid, 1, summary, detail, batch[0]["ts"], batch[-1]["ts"],
                    [x["id"] for x in batch], importance, vector,
                    user_id=str(batch[0].get("user_id", "")) if len({x.get("user_id", "") for x in batch}) == 1 else "",
                    source_fingerprint=fingerprint, source_refs=refs)
                await self.store.mark_compressed([x["id"] for x in batch], archive_id)
                await self.store.update_task(task_id, "completed", archive_id)
            except Exception as exc:
                await self.store.update_task(task_id, "failed", str(exc)[:500])
                logger.exception("[alife_memory] compression failed")
                return
            pending = await self.store.pending_messages(sid, self.compression_batch)
        await self._compact_levels(sid)

    async def _compact_levels(self, sid: str):
        for level in range(1, self.max_level):
            memories = [x for x in await self.store.list_memories(sid, 1000) if x["level"] == level and x.get("status") == "active"]
            memories.sort(key=lambda x: (x["start_ts"], x["end_ts"], x["id"]))
            if len(memories) < self.level_group_size:
                continue
            group = memories[:self.level_group_size]
            if group[-1]["start_ts"] < group[0]["start_ts"]:
                continue
            content = "\n".join(f"[{x['summary']}] {x['content']}" for x in group)
            result = await self._llm_json(self.compress_prompt.replace("{content}", content), self.compress_model or "fast")
            if not result:
                continue
            summary = str(result.get("summary", "")).strip()
            detail = str(result.get("content", "")).strip()
            if not summary or not detail:
                continue
            vector = await self._embed(summary + "\n" + detail)
            users = sorted({str(x.get("user_id", "")) for x in group if x.get("user_id")})
            refs = []
            for item in group:
                raw_refs = item.get("source_refs") or []
                if isinstance(raw_refs, str):
                    try:
                        raw_refs = json.loads(raw_refs)
                    except json.JSONDecodeError:
                        raw_refs = []
                if isinstance(raw_refs, list):
                    refs.extend(raw_refs)
                else:
                    refs.append({"sid": item.get("sid", sid), "user_id": item.get("user_id", ""), "start_ts": item["start_ts"], "end_ts": item["end_ts"]})
            new_id = await self.store.add_memory(
                sid, level + 1, summary, detail, group[0]["start_ts"], group[-1]["end_ts"],
                [x["id"] for x in group], _num(result.get("importance", 0.65), 0.65, 0.0, 1.0), vector,
                user_id=users[0] if len(users) == 1 else "", source_refs=refs)
            for old in group:
                await self.store.mark_stale(old["id"], f"merged into {new_id}")

    async def _reflection_loop(self):
        try:
            while not self._closed:
                await asyncio.sleep(self.reflect_interval)
                sessions = set()
                for sid_item in await self.store.list_memories(None, 1000):
                    sessions.add(sid_item["sid"])
                for sid in sessions:
                    try:
                        await self._reflect_session(sid)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("[alife_memory] reflection failed for %s", sid)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[alife_memory] reflection loop failed")

    async def _cleanup_loop(self):
        try:
            while not self._closed:
                await asyncio.sleep(21600)  # 每 6 小时
                if self._closed:
                    break
                try:
                    # 1. 常规清理（删除过期消息和 stale 版本）
                    result = await self.store.cleanup(self.message_retention_days, self.stale_retention_days)
                    # 2. 自动归档（默认关闭）
                    if self.auto_archive_days > 0:
                        archived = await self.store.archive_inactive(self.auto_archive_days, self.archive_level_min)
                        if archived:
                            logger.info("[alife_memory] auto-archived %d inactive memories", archived)
                            result["archived"] = archived
                    if result.get("deleted_messages") or result.get("deleted_stale_memories") or result.get("archived"):
                        logger.info("[alife_memory] cleanup: %s", result)
                except Exception as exc:
                    logger.warning("[alife_memory] cleanup failed: %s", exc)
        except asyncio.CancelledError:
            pass

    async def _reflect_session(self, sid: str):
        memories = [x for x in await self.store.list_memories(sid, self.reflect_max_items) if x.get("status") == "active"]
        recent = await self.store.recent(sid, 10)
        evidence = "\n".join(f"[{x['role']}] {x['content']}" for x in recent)
        for memory in memories:
            prompt = self.reflect_prompt.replace("{memory}", memory["summary"] + "\n" + memory["content"]).replace("{evidence}", evidence)
            result = await self._llm_json(prompt, self.reflect_model or self.compress_model)
            if not result or str(result.get("action", "none")) == "none":
                continue
            confidence = _num(result.get("confidence", 0), 0, 0, 1)
            if confidence < 0.86:
                continue
            note = str(result.get("reason", "后台审校发现新证据"))[:500]
            if result.get("action") == "stale":
                await self.store.mark_stale(memory["id"], note)
            elif result.get("action") == "correct":
                summary = str(result.get("summary", "")).strip()
                content = str(result.get("content", "")).strip()
                if summary and content:
                    vector = await self._embed(summary + "\n" + content)
                    await self.store.correct_memory(memory["id"], summary, content, note, confidence, vector)

    async def _inject(self, sid: str, req: LLMRequest, user_id: str = "", recall_user_ids: list[str] | None = None):
        if not self.auto_inject:
            return
        query = ""
        for message in reversed(getattr(req, "messages", []) or []):
            role = getattr(message, "role", None) or (message.get("role") if isinstance(message, dict) else "")
            if role == "user":
                query = getattr(message, "content", None) or (message.get("content") if isinstance(message, dict) else "") or ""
                break
        if not query:
            return
        user_id = user_id or _event_user_id(getattr(req, "messages", [])[-1] if getattr(req, "messages", []) else None)
        all_results: list[dict] = []
        seen_ids: set[str] = set()
        scope = self.search_scope if self.cross_user_enabled else ("user" if user_id else "session")

        # 第一轮：按用户查询内容检索
        content_results = await self.store.search(sid, str(query), self.retrieval_top_k, list(range(self.inject_level_max + 1)), await self._embed(str(query)), self.half_life, scope, user_id)
        # 只注入 active 记忆，archived 的不参与注入
        content_results = [r for r in content_results if r.get("status", "active") == "active"]
        best_score = 0.0
        for r in content_results:
            if r["id"] not in seen_ids:
                all_results.append(r)
                seen_ids.add(r["id"])
                best_score = max(best_score, r.get("score", 0.0))

        # 被动召回加强：当主检索结果不够强时，自动做宽召回
        if self.passive_recall:
            # 1) 首次见到的用户额外召回他们的记忆
            if recall_user_ids:
                for uid in recall_user_ids:
                    if uid and uid == user_id:
                        continue
                    for r in await self.store.count_memories_by_user(uid, 5):
                        if r["id"] not in seen_ids:
                            r["score"] = 0.3
                            all_results.append(r)
                            seen_ids.add(r["id"])

            # 2) 检索结果弱时，以主题关键词做全局宽召回
            if best_score < self.passive_recall_boost_threshold:
                terms = [t.strip() for t in re.split(r'[，。！？、\s,;:?]+', str(query)[:80]) if len(t.strip()) >= 2][:5]
                for term in terms:
                    if len(seen_ids) >= self.retrieval_top_k * 2:
                        break
                    wide_results = await self.store.search(sid, term, 3, list(range(self.inject_level_max + 1)), await self._embed(term), self.half_life, "linked", user_id)
                    for r in wide_results:
                        if r["id"] not in seen_ids:
                            r["score"] = r.get("score", 0.0) * 0.85
                            all_results.append(r)
                            seen_ids.add(r["id"])

        if not all_results:
            return
        all_results.sort(key=lambda x: (x.get("score", 0.0), x.get("importance", 0.0)), reverse=True)
        lines = []
        token_budget = self.max_injected_tokens
        item_budget = self.max_injected_items
        char_budget = self.max_injected_chars
        for item in all_results:
            if item_budget <= 0:
                break
            source = f"来源:{item.get('sid', 'unknown')} / 用户:{item.get('user_id', '') or 'unknown'} / 时间:{time.strftime('%Y-%m-%d', time.localtime(item['end_ts']))}"
            text = f"[{item['summary']}] {item['content']}（{source}）"
            cost = estimate_tokens(text)
            char_cost = len(text)
            if cost > token_budget or char_cost > char_budget:
                if not lines:
                    snippet = text[:max(token_budget, char_budget)]
                    lines.append(f"- {snippet}")
                break
            lines.append(f"- {text}")
            token_budget -= cost
            char_budget -= char_cost
            item_budget -= 1
        if not lines:
            return
        # 行数预算 — 双截断兜底
        line_budget = self.max_injected_lines
        if len(lines) > line_budget:
            lines = lines[:line_budget]
            lines.append("（注：记忆索引太长，只展示了部分）")
        # 注入到 messages 数组首条系统消息（不破坏 system_prompt 前缀缓存）
        block = ("一些关于过去的事情，供你参考。如果新了解到的情况和下面不一致，以最新的为准：\n"
                 + "\n".join(lines)
                 + "\n"
                 + '（提醒：记录代码行号、文件路径、git 历史这些会过时的信息没有意义，'
                   '它们应该通过查代码或日志来确认。'
                   '如果对方跟你聊起过去的事，自然地用这些记忆回应就好，不必刻意提起\u201c我记得你说过\u201d。）')
        system_msg = OpenAIMessage(role="system", content=block)
        req.messages.insert(0, system_msg)

    async def _inject_context_marker(self, req: LLMRequest, sid: str):
        """P0: 在上下文中保留最高层记忆标记，让 AI 感知自己有关联的记忆"""
        if not self.inject_context_marker or not sid:
            return
        top = await self.store.top_level_memories(sid, self.max_level, 10)
        # 只展示 active 的记忆
        top = [t for t in top if t.get("status", "active") == "active"][:5]
        if not top:
            return
        lines = []
        for item in top:
            source = f"来源:{item.get('sid', 'unknown')} / 时间:{time.strftime('%Y-%m-%d', time.localtime(item['end_ts']))}"
            text = f"[{item['summary']}]（{source}）"
            lines.append(f"- {text}")
        if not lines:
            return
        block = ("你过去经历过的一些事：\n"
                 + "\n".join(lines)
                 + '\n（完整的记忆细节可通过 search_long_term_memory 工具检索）')
        system_msg = OpenAIMessage(role="system", content=block)
        req.messages.insert(0, system_msg)

    @on.im_batch_message(priority=Priority.LOW)
    async def on_batch(self, event: KiraMessageBatchEvent, *_):
        if not self.enabled or not self.capture_enabled:
            return

    @on.llm_response(priority=Priority.LOW)
    async def on_response(self, event, resp, *_):
        if not self.enabled or not self.capture_enabled or getattr(resp, "tool_calls", None):
            return
        text = (getattr(resp, "text_response", "") or "").strip()
        if not text:
            return
        messages = getattr(event, "messages", []) or []
        user_messages = []
        for index, item in enumerate(messages):
            role = getattr(item, "role", None) or (item.get("role") if isinstance(item, dict) else "") or "user"
            if role == "user":
                content = _text_from_message(item)
                if content:
                    user_messages.append({"content": content, "user_id": _event_user_id(item), "ts": time.time(), "turn_no": index})
        sid = getattr(event, "sid", "")
        if user_messages and sid:
            turn_key = str(getattr(event, "event_id", "") or "")
            self._track(self._remember_turn(sid, user_messages, text, time.time(), turn_key))

    @on.llm_request(priority=Priority.LOW)
    async def on_request(self, event, req: LLMRequest, tag_set, *_):
        if not self.enabled:
            return
        sid = getattr(event, "sid", "") or getattr(getattr(event, "session", None), "sid", "")
        messages = getattr(event, "messages", []) or []
        user_id = _event_user_id(messages[-1] if messages else None)
        recall_user_ids = []
        if self.passive_recall:
            for msg in messages:
                uid = _event_user_id(msg)
                if uid and uid not in self._seen_user_ids:
                    self._seen_user_ids.add(uid)
                    recall_user_ids.append(uid)
        if not sid:
            return
        # P0: 上下文记忆标记
        if self.inject_context_marker:
            try:
                await self._inject_context_marker(req, sid)
            except Exception:
                logger.exception("[alife_memory] context marker failed")
        try:
            # 回忆关键词提醒
            if self.recall_hint_keywords:
                query = ""
                for message in reversed(getattr(req, "messages", []) or []):
                    role = getattr(message, "role", None) or (message.get("role") if isinstance(message, dict) else "")
                    if role == "user":
                        query = getattr(message, "content", None) or (message.get("content") if isinstance(message, dict) else "") or ""
                        break
                if query and any(kw in query for kw in self.recall_hint_keywords):
                    hint = "（提示：用户的话可能涉及过往记忆，你可通过 search_long_term_memory 工具检索相关记忆）"
                    req.messages.insert(0, OpenAIMessage(role="system", content=hint))
            # 记忆注入
            if self.auto_inject:
                await self._inject(sid, req, user_id, recall_user_ids)
        except Exception:
            logger.exception("[alife_memory] memory injection failed")

    @register.tool(
        name="remember_fact",
        description="把值得记住的事情写下来。就像是你的日记本——记下关于这个人的事、Ta的习惯、正在忙什么、去哪查什么。来源会话和时间会自动记上，你专注写内容就好。",
        params={"type": "object", "properties": {
            "content": {"type": "string", "description": "要记住的事情本身。尽量写完整，就像你跟朋友提起时那样自然。"},
            "summary": {"type": "string", "description": "一句话概括，留空的话系统会自动取 content 的开头"},
            "memory_type": {"type": "string", "description": "这是哪类事？\n- '关于我的' → 关于这个人是谁：职业、背景、性格、知识水平\n- '我喜欢的' → 这个人的习惯和偏好：喜欢什么、不喜欢什么、做事风格\n- '正在发生的' → 当前在忙的事：项目进展、决定、截止日期\n- '去哪查' → 外部信息源：去哪找什么资料、哪个系统管什么\n- '日常的' → 其他值得记住的日常事情", "default": "日常的"},
            "tags": {"type": "string", "description": "标签，逗号分隔，方便以后翻找。比如：工作,爱好,约定", "default": ""},
            "importance": {"type": "number", "description": "这件事多重要？0-1。0.9是很重要的事，0.5是普通日常，0.3是随手一记", "default": 0.5},
            "reason": {"type": "string", "description": "（偏好和项目类必填）为什么是这样？背后有什么故事或原因？比如Ta踩过什么坑才会这么要求", "default": ""},
            "when_it_matters": {"type": "string", "description": "（偏好和项目类必填）什么时候该想起这条？什么场景下适用？", "default": ""}},
            "required": ["content"]})
    async def remember_fact(self, event: KiraMessageBatchEvent, content: str, summary: str = "",
                            memory_type: str = "日常的", tags: str = "",
                            importance: float = 0.5, reason: str = "", when_it_matters: str = "") -> str:
        sid = getattr(event, "sid", "")
        if not sid or not content.strip():
            return "还不知道是在哪个会话说的，先聊起来再记吧"
        # 校验类型
        valid_types = {"关于我的", "我喜欢的", "正在发生的", "去哪查", "日常的"}
        memory_type = memory_type.strip()
        if memory_type not in valid_types:
            memory_type = "日常的"
        # 偏好和项目类强制要求原因和适用场景
        if memory_type in ("我喜欢的", "正在发生的"):
            if not reason.strip() or not when_it_matters.strip():
                return f"关于「{memory_type}」类的事情，最好也告诉我为什么是这样、什么时候该想起它，这样以后用起来才不迷糊。"
        summary = summary.strip() or content.strip().splitlines()[0][:100]
        user_id = _event_user_id((getattr(event, "messages", []) or [None])[-1])
        now = time.time()

        # 相对日期 → 绝对日期转换
        processed_content = self._convert_relative_dates(content)
        processed_reason = self._convert_relative_dates(reason) if reason else ""
        processed_when = self._convert_relative_dates(when_it_matters) if when_it_matters else ""

        # 组装摘要
        type_tag = f"[{memory_type}]"
        extra = ""
        if reason:
            extra += f" [原因:{processed_reason[:200]}]"
        if when_it_matters:
            extra += f" [何时想起:{processed_when[:200]}]"
        content_with_meta = f"{type_tag} {processed_content.strip()}{extra}"
        summary_with_meta = f"{type_tag} {summary}"

        # 标签追加到内容
        tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
        if tag_list:
            tag_text = f"[标签:{','.join(tag_list)}] "
            content_with_meta = tag_text + content_with_meta
            summary_with_meta = tag_text + summary_with_meta

        vector = await self._embed(summary_with_meta + "\n" + content_with_meta)
        mid = await self.store.add_memory(
            sid, self.max_level, summary_with_meta, content_with_meta,
            now, now, [], importance, vector, user_id=user_id,
            source_refs=[{"sid": sid, "user_id": user_id, "ts": now,
                          "action": "主动记录", "type": memory_type}])
        return f"已经记下了：{summary_with_meta[:80]}……（ID: {mid}，来源会话和保存时间已自动记录）"

    @register.tool(
        name="search_long_term_memory",
        description="检索长期记忆。涉及过去事实、偏好、约定或时间线时调用。返回结果包含记忆 ID、概要、内容和关联信息。结果最相关在前。",
        params={"type": "object", "properties": {"query": {"type": "string", "description": "检索问题，越精确越好"}, "top_k": {"type": "integer", "description": "结果数，最多 12 条", "default": 5}, "scope": {"type": "string", "description": "检索范围：session 当前会话、linked 所有来源交叉关联、global 全部记忆", "default": ""}}, "required": ["query"]})
    async def search_long_term_memory(self, event: KiraMessageBatchEvent, query: str, top_k: int = 5, scope: str = "") -> str:
        sid = getattr(event, "sid", "")
        scope = (scope or self.search_scope).lower()
        if scope not in ("session", "linked", "global"):
            scope = self.search_scope
        user_id = _event_user_id((getattr(event, "messages", []) or [None])[-1])
        results = await self.store.search(sid, query, _num(top_k, 5, 1, 12, True), list(range(self.max_level + 1)), await self._embed(query), self.half_life, scope, user_id)
        if not results:
            return "没有找到相关长期记忆"
        out = [f"共找到 {len(results)} 条相关记忆："]
        for i, x in enumerate(results, 1):
            out.append(f"\n--- {i}. ID: {x['id']} ---")
            out.append(f"概要: {x['summary']}")
            out.append(f"内容: {x['content']}")
            out.append(f"相关度: {x['score']:.2f} | 来源会话: {x.get('sid', 'unknown')} | 用户: {x.get('user_id', '') or 'unknown'} | 时间: {time.strftime('%Y-%m-%d %H:%M', time.localtime(x['end_ts']))}")
        return "\n".join(out)

    @register.tool(
        name="correct_long_term_memory",
        description="修正一条长期记忆。当用户指出记忆错误或出现新证据时使用。旧版本自动保留审计链。修正时间由系统自动记录。",
        params={"type": "object", "properties": {"memory_id": {"type": "string", "description": "要修正的记忆 ID"}, "summary": {"type": "string", "description": "修正后的概述"}, "content": {"type": "string", "description": "修正后的事实，应包含完整修正后的信息"}, "reason": {"type": "string", "description": "修正依据，如‘用户指出记忆有误’或‘新证据表明...’"}}, "required": ["memory_id", "summary", "content", "reason"]})
    async def correct_long_term_memory(self, event: KiraMessageBatchEvent, memory_id: str, summary: str, content: str, reason: str) -> str:
        vector = await self._embed(summary + "\n" + content)
        new_id = await self.store.correct_memory(memory_id, summary, content, reason, 1.0, vector)
        return f"已修正记忆，新版本为 {new_id}" if new_id else "未找到可修正的记忆"

    @register.tool(
        name="forget_long_term_memory",
        description="删除一条长期记忆。只有用户明确要求遗忘时使用。删除不可恢复。",
        params={"type": "object", "properties": {"memory_id": {"type": "string", "description": "要删除的记忆 ID，可通过 list_all_memories 或 search_long_term_memory 获取"}}, "required": ["memory_id"]})
    async def forget_long_term_memory(self, event: KiraMessageBatchEvent, memory_id: str) -> str:
        return "已删除" if await self.store.delete_memory(memory_id) else "未找到该记忆"

    @register.tool(
        name="list_all_memories",
        description="列出当前会话的全部长期记忆。用于概览你记得什么，或找到特定记忆的 ID 以便修正/删除。",
        params={"type": "object", "properties": {"limit": {"type": "integer", "description": "最多返回多少条，最多 50", "default": 20}, "level": {"type": "integer", "description": "按层级过滤，留空返回全部层级", "default": 0}}, "required": []})
    async def list_all_memories(self, event: KiraMessageBatchEvent, limit: int = 20, level: int = 0) -> str:
        sid = getattr(event, "sid", "")
        if not sid:
            return "未确定当前会话"
        memories = await self.store.list_memories(sid, _num(limit, 20, 1, 50, True))
        if not memories:
            return "当前会话没有长期记忆"
        out = [f"当前会话共有 {len(memories)} 条长期记忆："]
        for m in memories:
            if level > 0 and m["level"] != level:
                continue
            t_str = time.strftime('%Y-%m-%d %H:%M', time.localtime(m["start_ts"]))
            out.append(f"\n- ID: {m['id']} | 层级 L{m['level']} | {t_str}")
            out.append(f"  概要: {m['summary']}")
            out.append(f"  状态: {m['status']} | 置信度: {m.get('confidence', 0.65)*100:.0f}%")
        return "\n".join(out)

    @register.tool(
        name="get_memory_detail",
        description="获取一条记忆的完整详细信息，包括原始内容、时间范围、来源引用链等。",
        params={"type": "object", "properties": {"memory_id": {"type": "string", "description": "记忆 ID，可通过 list_all_memories 获取"}}, "required": ["memory_id"]})
    async def get_memory_detail(self, event: KiraMessageBatchEvent, memory_id: str) -> str:
        memory = await self.store.get_memory(memory_id)
        if not memory:
            return f"未找到 ID 为 {memory_id} 的记忆"
        t_start = time.strftime('%Y-%m-%d %H:%M', time.localtime(memory["start_ts"]))
        t_end = time.strftime('%Y-%m-%d %H:%M', time.localtime(memory["end_ts"]))
        refs = memory.get("source_refs", [])
        if isinstance(refs, str):
            try:
                refs = json.loads(refs)
            except json.JSONDecodeError:
                refs = []
        source_info = ""
        if refs:
            seen_sids = set()
            for ref in refs:
                s = ref.get("sid", "")
                if s and s not in seen_sids:
                    seen_sids.add(s)
                    source_info += f"\n  来源会话: {s} | 用户: {ref.get('user_id', '') or 'unknown'} | 时间: {time.strftime('%Y-%m-%d %H:%M', time.localtime(ref.get('ts', 0)))}"
        tags = ""
        if memory["summary"].startswith("[标签:"):
            tags = memory["summary"].split("]")[0].strip("[]")
        parts = [
            f"记忆 ID: {memory['id']}",
            f"层级: L{memory['level']}",
            f"状态: {memory.get('status', 'active')}",
            f"置信度: {memory.get('confidence', 0.65)*100:.0f}%",
            f"时间范围: {t_start} → {t_end}",
            f"概要: {memory['summary']}",
            f"详细内容: {memory['content']}",
        ]
        if tags:
            parts.insert(0, f"标签: {tags}")
        if source_info:
            parts.append(f"来源引用:{source_info}")
        if memory.get("correction_note"):
            parts.append(f"修正备注: {memory['correction_note']}")
        if memory.get("supersedes"):
            parts.append(f"旧版本被取代: {memory['supersedes']}")
        return "\n".join(parts)

    @register.page("/index", menu=PageMenu(label={"zh": "长期记忆·Z", "en": "Memory·Z"}, icon="Brain", order=90))
    def page(self):
        return PluginPage.from_folder("./web")

    # ---- API ----

    @register.api(method="GET", path="/status", auth=True, summary="Memory status")
    async def api_status(self):
        result = await self.store.stats()
        result.update({"enabled": self.enabled, "capture_enabled": self.capture_enabled, "auto_inject": self.auto_inject,
                       "passive_recall": self.passive_recall, "workers": len(self._workers),
                       "reflection": self.reflect_enabled, "trigger_mode": self.trigger_mode,
                       "round_threshold": self.round_threshold, "token_threshold": self.token_threshold,
                       "message_threshold": self.message_threshold,
                       "max_level": self.max_level, "auto_archive_days": self.auto_archive_days,
                       "archive_level_min": self.archive_level_min})
        return result

    @register.api(method="GET", path="/pending", auth=True, summary="Pending compression stats")
    async def api_pending(self):
        pending = await self.store.pending_messages_all()
        return {"round_count": pending.get("round_count", 0), "token_sum": pending.get("token_sum", 0),
                "message_count": pending.get("message_count", 0)}

    @register.api(method="GET", path="/memories", auth=True, summary="List memories")
    async def api_memories(self, sid: str | None = None, limit: int = 100):
        return await self.store.list_memories(sid or None, _num(limit, 100, 1, 500, True))

    @register.api(method="GET", path="/memory/{memory_id}", auth=True, summary="Get memory")
    async def api_memory(self, memory_id: str):
        return await self.store.get_memory(memory_id) or {"error": "not found"}

    @register.api(method="POST", path="/search", auth=True, summary="Search memory")
    async def api_search(self, request: Request):
        body = await request.json()
        query = str(body.get("query", "")).strip()
        if not query:
            return {"error": "query required"}
        sid = str(body.get("sid", ""))
        scope = str(body.get("scope", self.search_scope) or self.search_scope).lower()
        if scope not in ("session", "linked", "global"):
            scope = self.search_scope
        user_id = str(body.get("user_id", ""))
        result = await self.store.search(sid, query, _num(body.get("top_k", self.retrieval_top_k), self.retrieval_top_k, 1, 20, True), None, await self._embed(query), self.half_life, scope, user_id)
        return {"query": query, "scope": scope, "results": result}

    @register.api(method="POST", path="/memory/{memory_id}/correct", auth=True, summary="Correct memory")
    async def api_correct(self, memory_id: str, request: Request):
        body = await request.json()
        vector = await self._embed(str(body.get("summary", "")) + "\n" + str(body.get("content", "")))
        new_id = await self.store.correct_memory(memory_id, str(body.get("summary", "")), str(body.get("content", "")), str(body.get("reason", "WebUI correction")), 1.0, vector)
        return {"ok": bool(new_id), "new_id": new_id}

    @register.api(method="DELETE", path="/memory/{memory_id}", auth=True, summary="Delete memory")
    async def api_delete(self, memory_id: str):
        return {"ok": await self.store.delete_memory(memory_id)}

    @register.api(method="POST", path="/reflect", auth=True, summary="Run reflection")
    async def api_reflect(self, request: Request):
        body = await request.json()
        sid = str(body.get("sid", "")).strip()
        if sid:
            self._track(self._reflect_session(sid))
        else:
            for item in await self.store.list_memories(None, 1000):
                self._track(self._reflect_session(item["sid"]))
        return {"ok": True, "message": "后台审校任务已排队"}

    @register.api(method="GET", path="/tasks", auth=True, summary="List memory tasks")
    async def api_tasks(self, limit: int = 50):
        return await self.store.tasks(_num(limit, 50, 1, 200, True))

    @register.api(method="POST", path="/cleanup", auth=True, summary="Trigger manual cleanup")
    async def api_cleanup(self):
        result = await self.store.cleanup(self.message_retention_days, self.stale_retention_days)
        return {"ok": True, "deleted_messages": result.get("deleted_messages", 0),
                "deleted_stale_memories": result.get("deleted_stale_memories", 0)}

    @register.api(method="GET", path="/config", auth=True, summary="Get current config")
    async def api_config_get(self):
        return self.cfg

    @register.api(method="GET", path="/models", auth=True, summary="List available LLM models")
    async def api_models(self):
        models = []
        try:
            ctx = self.ctx
            pm = getattr(ctx, "provider_mgr", None)
            if pm and hasattr(pm, "kira_config"):
                providers_config = pm.kira_config.get("providers", {}) or {}
                for pid, pcfg in providers_config.items():
                    if not isinstance(pcfg, dict):
                        continue
                    model_config = (pcfg or {}).get("model_config", {}) or {}
                    llm_models = model_config.get("llm", {}) or {}
                    pname = (pcfg.get('name', '') or pid)
                    for mid in llm_models:
                        models.append({"id": f"{pid}:{mid}", "name": f"{mid} ({pname})"})
        except Exception as exc:
            logger.warning("[alife_memory] failed to list models: %s", exc)
        return {"models": models}

    @register.api(method="POST", path="/config", auth=True, summary="Hot update memory config")
    async def api_config_post(self, request: Request):
        body = await request.json()
        if not isinstance(body, dict):
            return {"error": "object required"}
        for key, value in body.items():
            if isinstance(value, dict) and isinstance(self.cfg.get(key), dict):
                self.cfg[key].update(value)
            else:
                self.cfg[key] = value
        return {"ok": True}
          