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

from core.plugin import BasePlugin, PageMenu, PluginPage, Priority, on, register
from core.logging_manager import get_logger

logger = get_logger("alife_memory", "light_purple")
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


_MEMORY_TYPES = ("关于实体", "偏好风格", "约定任务", "关系网络", "溯源查询", "日常事件")
_LEGACY_MT = {"关于我的": "关于实体", "我喜欢的": "偏好风格", "正在发生的": "约定任务",
              "去哪查": "溯源查询", "日常的": "日常事件"}


def _norm_mt(mt: str) -> str:
    """规范化 memory_type 到 6 类。非法值回退「日常事件」。"""
    mt = (mt or "").strip()
    if mt in _MEMORY_TYPES:
        return mt
    if mt in _LEGACY_MT:
        return _LEGACY_MT[mt]
    en = {"entity": "关于实体", "preference": "偏好风格", "task": "约定任务",
          "relation": "关系网络", "source": "溯源查询", "daily": "日常事件", "fact": "日常事件"}
    return en.get(mt.lower(), "日常事件")


def _norm_clamp(v, lo: float, hi: float, default: float) -> float:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, f))


def _cos_structured_json(text: str, schema: str = "items") -> list[dict[str, Any]]:
    """解析 LLM 输出的结构化记忆/审计 JSON，做 schema 校验 + 缺失字段兜底。
    schema='items' 期望 {"items":[{summary,content,memory_type,tags,...,confidence,importance,...}]}。
    支持顶层直接是数组，或 {"items":[...]}。解析失败返回 []（调用方降级）。"""
    raw = _json_object(text) if isinstance(text, str) else text
    if not isinstance(raw, dict):
        return []
    items = raw.get("items")
    if items is None and schema == "items":
        # 兼容旧的单条输出 {summary,content,...} 包装成 items
        items = [raw] if raw.get("summary") else []
    if not isinstance(items, list):
        return []
    out: list[dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        item = dict(it)
        # 摘要/内容兜底
        item["summary"] = str(item.get("summary", "") or "").strip()
        item["content"] = str(item.get("content", "") or "").strip()
        # memory_type 规范化
        item["memory_type"] = _norm_mt(str(item.get("memory_type", "") or ""))
        # tags 数组化
        raw_tags = item.get("tags", [])
        if isinstance(raw_tags, str):
            raw_tags = [t.strip() for t in raw_tags.replace("，", ",").split(",") if t.strip()]
        if isinstance(raw_tags, list):
            raw_tags = [str(t).strip() for t in raw_tags if str(t).strip()]
        else:
            raw_tags = []
        item["tags"] = raw_tags[:8]
        # importance/confidence 钳制（缺省也补默认值，保证后续 _num/落库有值）
        item["importance"] = _norm_clamp(item.get("importance"), 0.0, 1.0, 0.55)
        item["confidence"] = _norm_clamp(item.get("confidence"), 0.0, 1.0, 0.65)
        item["reason"] = str(item.get("reason", "") or "").strip()
        item["scenario"] = str(item.get("scenario", "") or "").strip()
        item["entity_id"] = str(item.get("entity_id", "") or "").strip()
        # 偏好/约定类缺 reason/scenario 时补默认说明（不让空字段进库）
        if item["memory_type"] in ("偏好风格", "约定任务"):
            if not item["reason"]:
                item["reason"] = "用户明确表达/正在进行的约定"
            if not item["scenario"]:
                item["scenario"] = "聊到该话题或相关情境时"
        out.append(item)
    return out


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
        self._seen_recall: dict[str, set[str]] = {}  # sid → 该会话已召回过记忆的用户集合（per-session 去重）
        self._recall_round_counter: dict[str, int] = {}
        # rerank 探测缓存：首次调用时探测一次，没配置就记住不再每次尝试，插件重载/热更新时重置
        self._rerank_checked = False
        self._rerank_client = None
        # embedding 探测缓存：与 rerank 对称，首次探测后缓存，避免每次注入都调 get_default_embedding_client
        self._embed_checked = False
        self._embed_client = None
        self._ctx_injected_ids: set[str] = set()  # context marker 已注入的记忆 id，避免 _inject 重复注入
        self._load_settings()
        self._llm_semaphore = asyncio.Semaphore(max(1, self.llm_concurrency_limit))
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
        self.passive_recall_update_rounds = _num(basic.get("passive_recall_update_rounds", 10), 10, 1, 200, True)
        self.passive_recall_keyword_limit = _num(basic.get("passive_recall_keyword_limit", 8), 8, 1, 30, True)
        self.llm_concurrency_limit = _num(basic.get("llm_concurrency_limit", 4), 4, 1, 20, True)
        # 列表展示默认值（可在 WebUI/侧边栏热配置）
        self.user_list_limit = _num(basic.get("user_list_limit", 50), 50, 1, 200, True)
        self.memory_list_limit = _num(basic.get("memory_list_limit", 20), 20, 1, 100, True)
        self.recent_summaries_count = _num(basic.get("recent_summaries_count", 3), 3, 1, 10, True)
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
        self.embedding_model = str(retrieval.get("embedding_model", "") or "").strip()
        self.rerank_model = str(retrieval.get("rerank_model", "") or "").strip()
        self.search_scope = str(retrieval.get("search_scope", "linked") or "linked").lower()
        if self.search_scope not in ("session", "linked", "global"):
            self.search_scope = "linked"
        self.cross_user_enabled = bool(retrieval.get("cross_user_enabled", True))
        self.inject_level_max = _num(retrieval.get("inject_level_max", 12), 12, 0, 12, True)

        self.compress_prompt = str(compression.get("prompt", "") or "").strip() or self._default_compress_prompt()
        self.reflect_prompt = str(reflection.get("prompt", "") or "").strip() or self._default_reflect_prompt()
        # 若用户未自定义压缩/审校提示词，把默认词回填进 cfg，让 WebUI 可展示、可编辑（用户改过后不再覆盖）
        self._sync_default_prompts(compression, reflection)
        self.compress_probability = _num(compression.get("compress_probability", 0.8), 0.8, 0.0, 1.0)
        self.compress_timeout = _num(compression.get("compress_timeout", 30), 30, 5, 120, True)
        self.max_compress_retry = _num(compression.get("max_compress_retry", 2), 2, 0, 10, True)
        self.inject_context_marker = bool(basic.get("inject_context_marker", True))
        self.recall_hint_keywords = list(basic.get("recall_hint_keywords", ["记得", "回忆", "以前", "上次", "忘记"])) if isinstance(basic.get("recall_hint_keywords"), list) else []
        self.max_injected_lines = _num(basic.get("max_injected_lines", 30), 30, 5, 100, True)
        self.auto_archive_days = _num(basic.get("auto_archive_days", 0), 0, 0, 3650, True)
        # archive_level_min 默认 = max_level（最高层保护，其余层级可归档）
        # 必须在 max_level 已赋值后才设置
        self.archive_level_min = _num(basic.get("archive_level_min", self.max_level), 1, 1, 12, True)
        # 用户画像（批次 B）：是否启用画像生成/注入
        self.profile_enabled = bool(basic.get("profile_enabled", True))
        self.profile_inject = bool(basic.get("profile_inject", True))
        self.profile_inject_max = _num(basic.get("profile_inject_max", 3), 3, 0, 6, True)
        self.profile_generate_model = str(basic.get("profile_generate_model", "") or "").strip()
        self.profile_gen_trigger = _num(basic.get("profile_gen_trigger", 5), 5, 1, 50, True)  # 某实体累计多少条记忆触发一次画像生成

    @staticmethod
    def _default_compress_prompt():
        return ("你是长期记忆整理器。把{range}中提取成可验证、简洁、无重复的记忆。\n"
                "只输出 JSON，不要 Markdown：{\"items\":[{\"summary\":\"一句话概述\",\"content\":\"事实、偏好、决定和关系变化，分行列出\",\"memory_type\":\"关于实体|偏好风格|约定任务|关系网络|溯源查询|日常事件\",\"tags\":[\"标签1\",\"标签2\"],\"importance\":0.0,\"confidence\":0.0,\"reason\":\"为什么记（偏好/约定类必填）\",\"scenario\":\"什么时候该想起（偏好/约定类必填）\",\"entity_id\":\"关联到谁（用户/群/机器人标识，可空）\"}]}\n"
                "memory_type 只能从 6 类里选：关于实体(这个人/群是谁、身份背景) / 偏好风格(喜欢什么、习惯、禁止项) / 约定任务(正在做的事、项目、约定、截止) / 关系网络(谁是谁的什么人、成员列表、角色设定) / 溯源查询(去哪查资料、哪个系统管什么) / 日常事件(其他值得记住的)。\n"
                "偏好风格和约定任务两类必须填 reason(为什么记这件事)和 scenario(什么时候该想起它)。\n"
                "tags 是 2-4 个简短中文标签，方便以后按标签翻找，不要超过 6 个。\n"
                "不要臆测，不要把闲聊或临时情绪写成长期事实；保留时间、主体和限定条件。\n待整理内容：\n{content}")

    @staticmethod
    def _default_reflect_prompt():
        return ("你是记忆审校器。比较旧记忆和新证据，只处理有明确矛盾的事实。\n"
                "只输出 JSON：{\"action\":\"none|correct|stale\",\"items\":[{\"memory_id\":\"\",\"summary\":\"修正后的概述\",\"content\":\"修正后的事实\",\"memory_type\":\"关于实体|偏好风格|约定任务|关系网络|溯源查询|日常事件\",\"tags\":[\"标签\"],\"confidence\":0.0,\"reason\":\"证据依据\",\"scenario\":\"何时想起\",\"entity_id\":\"\"}]}\n"
                "action=correct 时 items 里给出修正后的完整结构化字段（含 memory_type/tags/reason/scenario）；action=stale 时 items 可只给 memory_id。\n"
                "若只是措辞不同、证据不足或可能是临时状态，输出 none。不得凭空补全。\n旧记忆：\n{memory}\n新证据：\n{evidence}")

    def _sync_default_prompts(self, compression: dict, reflection: dict):
        """当用户未自定义提示词时，把最新默认词回填进 cfg（供 WebUI 展示/编辑）。
        用户一旦改过（cfg 里是非空且非默认的内容），就不再覆盖。"""
        default_comp = self._default_compress_prompt()
        default_refl = self._default_reflect_prompt()
        comp_val = str(compression.get("prompt", "") or "").strip()
        refl_val = str(reflection.get("prompt", "") or "").strip()
        # 旧的非结构化默认词（用户从未自定义、仍是内置默认时也升级到新结构化词）
        old_comp_defaults = [
            '你是长期记忆整理器。把{range}中提取成可验证、简洁、无重复的记忆。\n只输出 JSON，不要 Markdown：{"summary":"一句话概述","content":"事实、偏好、决定和关系变化，分行列出","importance":0.0}\n不要臆测，不要把闲聊或临时情绪写成长期事实；保留时间、主体和限定条件。\n待整理内容：\n{content}']
        old_refl_defaults = [
            '你是记忆审校器。比较旧记忆和新证据，只处理有明确矛盾的事实。\n只输出 JSON：{"action":"none|correct|stale","summary":"修正后的概述","content":"修正后的事实","confidence":0.0,"reason":"证据依据"}\n若只是措辞不同、证据不足或可能是临时状态，输出 none。不得凭空补全。\n旧记忆：\n{memory}\n新证据：\n{evidence}']
        changed = False
        # 空 或 等于旧默认词（即从未真正自定义）→ 用最新结构化默认词
        if not comp_val or any(comp_val == o.strip() for o in old_comp_defaults if o.strip()):
            compression["prompt"] = default_comp
            changed = True
        if not refl_val or any(refl_val == o.strip() for o in old_refl_defaults if o.strip()):
            reflection["prompt"] = default_refl
            changed = True
        if changed:
            # 回写到模块级 cfg，让 WebUI /config 能拿到
            self.cfg.setdefault("section_compression", {})["prompt"] = compression.get("prompt", default_comp)
            self.cfg.setdefault("section_reflection", {})["prompt"] = reflection.get("prompt", default_refl)

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
        """检测冲突的记忆插件，先禁用再迁移，日志统计待迁移条数"""
        migrated = False
        conflict_plugins = {
            "kira_plugin_simple_memory": {"name": "Simple Memory（内置）", "data_file": "memory/core.txt"},
            "kira_plugin_kiraos": {"name": "KiraOS / 海马体记忆", "data_file": None},
        }
        try:
            ctx = self.ctx
            pm = getattr(ctx, "plugin_mgr", None)
            if not pm:
                return
            for plugin_id, info in conflict_plugins.items():
                plugin_inst = pm.get_plugin_inst(plugin_id)
                if plugin_inst is None:
                    continue
                is_enabled = pm.is_plugin_enabled(plugin_id) if hasattr(pm, "is_plugin_enabled") else True
                if not is_enabled:
                    continue
                logger.info("[alife_memory] 检测到冲突插件 %s (%s)，先禁用……", plugin_id, info["name"])
                # 先禁用，再迁移
                if hasattr(pm, "set_plugin_enabled"):
                    await pm.set_plugin_enabled(plugin_id, False)
                    logger.info("[alife_memory] 已自动禁用 %s", plugin_id)
                # 统计待迁移条数
                pre_count = await self._count_migratable(plugin_id)
                if pre_count > 0:
                    logger.info("[alife_memory] 检测到 %s 有 %d 条记忆待迁移", info["name"], pre_count)
                # 迁移数据
                migrated |= await self._migrate_from(plugin_id, info, pm, plugin_inst)
        except Exception as exc:
            logger.warning("[alife_memory] 互斥检测/禁用失败: %s", exc)

    @staticmethod
    def _parse_kiraos_toml(tf: Path):
        """解析单个 KiraOS TOML 记忆文件。
        返回 (content, summary, importance, sid, ts, tags, memory_type) 或 None。
        同时解析 KiraOS 的 tags 数组（之前丢失），供结构化落库。"""
        try:
            text = tf.read_text(encoding="utf-8", errors="replace")
            import re as _re
            t_text_match = _re.search(r'text\s*=\s*"([^"]*)"', text)
            if not t_text_match:
                return None
            content = t_text_match.group(1)
            # 跳过超长内容（KiraOS 海马体的长篇概况通常质量低、污染大）
            if len(content) > 120:
                return None
            t_imp = _re.search(r'importance\s*=\s*(\d+)', text)
            t_sid = _re.search(r'session\s*=\s*"([^"]*)"', text)
            t_ts = _re.search(r'time\s*=\s*"([^"]*)"', text)
            # 解析 KiraOS tags 数组（多行 ['tag1','tag2']），之前完全丢失
            t_tags = _re.findall(r'"([^"]+)"', _re.search(r'tags\s*=\s*\[(.*?)\]', text, _re.S).group(1) if _re.search(r'tags\s*=\s*\[(.*?)\]', text, _re.S) else '')
            tags = [t.strip() for t in t_tags if t.strip()]
            # KiraOS type: fact / relationship / preference 等
            t_type = _re.search(r'type\s*=\s*"([^"]*)"', text)
            kira_type = t_type.group(1) if t_type else "fact"
            type_map = {"fact": "日常事件", "relationship": "关系网络", "preference": "偏好风格",
                        "entity": "关于实体", "task": "约定任务", "source": "溯源查询"}
            memory_type = type_map.get(kira_type, "日常事件")
            importance = float(t_imp.group(1)) / 10.0 if t_imp else 0.5
            source_sid = t_sid.group(1) if t_sid else "kiraos_import"
            from datetime import datetime as _dt
            try:
                ts_val = _dt.fromisoformat(t_ts.group(1)).timestamp() if t_ts else time.time()
            except Exception:
                ts_val = time.time()
            return (content, content[:100], importance, source_sid, ts_val, tags, memory_type)
        except Exception:
            return None

    async def _count_migratable(self, plugin_id: str) -> int:
        """统计冲突插件的待迁移记忆条数"""
        try:
            from core.utils.path_utils import get_data_path
            data_root = Path(get_data_path())
            if plugin_id == "kira_plugin_simple_memory":
                core_txt = data_root / "memory" / "core.txt"
                exists = await asyncio.to_thread(core_txt.exists)
                if not exists:
                    return 0
                text = await asyncio.to_thread(lambda: core_txt.read_text(encoding="utf-8", errors="replace"))
                return len([l for l in text.splitlines() if l.strip()])
            elif plugin_id == "kira_plugin_kiraos":
                kiraos_dir = data_root / "memory" / "entities"
                exists = await asyncio.to_thread(kiraos_dir.exists)
                if not exists:
                    return 0
                files = await asyncio.to_thread(lambda: list(kiraos_dir.rglob("*.toml")))
                return len(files)
        except Exception:
            return 0
        return 0

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
                    raw_text = await asyncio.to_thread(lambda: core_txt.read_text(encoding="utf-8", errors="replace"))
                    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
                    if lines:
                        now = time.time()
                        # 迁移只导入文本 + 元数据，不计算向量（避免卡顿/白忙/依赖向量模型）
                        rows = []
                        for line in lines:
                            if len(line) > 120:
                                continue
                            content_stripped = line
                            rows.append({
                                "sid": "system", "level": 3, "summary": content_stripped[:100],
                                "content": content_stripped, "start_ts": now, "end_ts": now,
                                "source_ids": [], "importance": 0.5, "embedding": None,
                                "user_id": "", "source_fingerprint": hashlib.sha256(("simple_memory_import|" + content_stripped).encode()).hexdigest()[:32],
                                "source_refs": [{"sid": "simple_memory_import", "user_id": "", "ts": now,
                                                 "action": "从Simple Memory自动迁移，原始文件保留未清理"}],
                            })
                        count = await self.store.add_memories_batch(rows)
                        if count:
                            migrated = True
                            logger.info("[alife_memory] 已从 Simple Memory 迁移 %d 条记忆（原始文件未删除）", count)

            elif plugin_id == "kira_plugin_kiraos":
                # KiraOS: data/memory/entities/ 目录下的 TOML 文件
                kiraos_entities = data_root / "memory" / "entities"
                if await asyncio.to_thread(kiraos_entities.exists):
                    toml_files = await asyncio.to_thread(lambda: list(kiraos_entities.rglob("*.toml")))
                    if toml_files:
                        # 1) 并行读取所有文件并解析（替代逐条串行 read_text）
                        from datetime import datetime as _dt
                        parsed = await asyncio.gather(*[asyncio.to_thread(self._parse_kiraos_toml, tf) for tf in toml_files])
                        valid = [p for p in parsed if p]  # [(content, summary, importance, sid, ts, tags, memory_type)]
                        if valid:
                            # 迁移只导入文本 + 元数据，不计算向量（避免卡顿/白忙/依赖向量模型）
                            rows = []
                            now = time.time()
                            for (content, summary, importance, source_sid, ts_val, ktags, ktype) in valid:
                                rows.append({
                                    "sid": source_sid, "level": 3, "summary": summary,
                                    "content": content, "start_ts": ts_val, "end_ts": ts_val,
                                    "source_ids": [], "importance": importance,
                                    "embedding": None, "user_id": "",
                                    "memory_type": ktype, "tags": ktags,
                                    "source_fingerprint": hashlib.sha256(("kiraos_import|" + content).encode()).hexdigest()[:32],
                                    "source_refs": [{"sid": source_sid, "user_id": "", "ts": ts_val,
                                                     "action": "从KiraOS记忆自动迁移，原始文件保留未清理"}],
                                })
                            count = await self.store.add_memories_batch(rows)
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
            async with self._llm_semaphore:
                response = await asyncio.wait_for(
                    client.chat(LLMRequest(messages=[OpenAIMessage(role="user", content=prompt)])),
                    timeout=self.compress_timeout if hasattr(self, 'compress_timeout') and self.compress_timeout else 30
                )
            return _json_object(getattr(response, "text_response", "") or "")
        except asyncio.TimeoutError:
            logger.warning("[alife_memory] model request timed out after %ds",
                           getattr(self, 'compress_timeout', 30))
            return None
        except Exception as exc:
            logger.warning("[alife_memory] model request failed: %s", exc)
            return None

    async def _embed(self, text: str) -> list[float] | None:
        if not self.semantic_enabled:
            return None
        # 首次探测一次并缓存 client：若不可用则记住，之后直接跳过，避免每轮重复探测
        if not self._embed_checked:
            try:
                if self.embedding_model:
                    client = self.ctx.get_embedding_client(model_uuid=self.embedding_model)
                else:
                    client = self.ctx.get_default_embedding_client()
            except Exception:
                client = None
            self._embed_client = client if (client and hasattr(client, "embed")) else None
            self._embed_checked = True
        client = self._embed_client
        if not client:
            return None
        try:
            result = await client.embed([text])
            vector = result[0] if isinstance(result, list) and result else result
            return [float(x) for x in vector] if vector else None
        except Exception as exc:
            logger.debug("[alife_memory] embedding unavailable: %s", exc)
            return None

    async def _embed_batch(self, texts: list[str], concurrency: int = 16) -> dict[str, list[float] | None]:
        """并发批量嵌入，返回 {text: embedding}。用信号量限制并发，避免一次性大量请求压垮 embedding API。
        比逐条串行快得多，但对大规模（如迁移 1000 条）也能安全并发。"""
        if not texts:
            return {}
        sem = asyncio.Semaphore(max(1, concurrency))

        async def _one(t):
            async with sem:
                return await self._embed(t)

        results = await asyncio.gather(*(_one(t) for t in texts))
        return {t: r for t, r in zip(texts, results)}

    async def _rerank(self, query: str, items: list[dict]) -> list[dict]:
        """Rerank search results using configured rerank model.

        首次调用时探测一次：若未配置可用 rerank，缓存标记后之后直接跳过，
        避免每轮注入都重复尝试 get_default_rerank() 造成阻塞。
        """
        if not items:
            return items
        # 已探测过且确认不可用 → 直接跳过
        if self._rerank_checked and self._rerank_client is None:
            return items
        try:
            # 首次探测：解析 rerank client 并缓存
            if not self._rerank_checked:
                pm = getattr(self.ctx, "provider_mgr", None)
                if not pm:
                    self._rerank_checked = True
                    self._rerank_client = None
                    return items
                if self.rerank_model:
                    parts = self.rerank_model.split(":")
                    client = pm.get_model_client(parts[0], ":".join(parts[1:]))
                else:
                    client = pm.get_default_rerank()
                if not client or not hasattr(client, "rerank"):
                    self._rerank_checked = True
                    self._rerank_client = None
                    return items
                self._rerank_client = client
                self._rerank_checked = True
            client = self._rerank_client
            texts = [f"{x.get('summary', '')} {x.get('content', '')}" for x in items]
            scores = await client.rerank(query, texts)
            if scores and len(scores) == len(items):
                for i, x in enumerate(items):
                    x["score"] = x.get("score", 0.0) * 0.5 + float(scores[i]) * 0.5
                items.sort(key=lambda x: x["score"], reverse=True)
        except Exception:
            logger.warning("[alife_memory] rerank unavailable, skipped")
        return items

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
            # 检查整批中是否有超过最大重试次数的
            max_attempts = max(x.get("attempts", 0) or 0 for x in batch)
            if max_attempts >= self.max_compress_retry:
                batch_ids = [x["id"] for x in batch]
                await self.store.mark_skipped(batch_ids, f"超时{max_attempts}次，跳过整批")
                logger.info("[alife_memory] 压缩跳过整批 %d 条（超时 %d 次）", len(batch_ids), max_attempts)
                pending = await self.store.pending_messages(sid, self.compression_batch)
                continue
            content = "\n".join(f"[{x['role']} user={x.get('user_id', '') or 'unknown'} turn={x.get('turn_no', 0)}] {x['content']}" for x in batch)
            if len(content) < self.compress_min_chars:
                return
            if self.compress_probability < 1.0 and random.random() > self.compress_probability:
                return
            batch_start = time.strftime('%Y-%m-%d %H:%M', time.localtime(batch[0]['ts']))
            batch_end = time.strftime('%Y-%m-%d %H:%M', time.localtime(batch[-1]['ts']))
            range_desc = f"从 {batch_start} 到 {batch_end} 期间的对话"
            task_id = await self.store.create_task(sid, "compress", "压缩对话为长期记忆")
            try:
                result = await self._llm_json(self.compress_prompt.replace("{range}", range_desc).replace("{content}", content), self.compress_model or "fast")
                if result is None:
                    # 超时或失败，记录重试次数
                    batch_ids = [x["id"] for x in batch]
                    await self.store.increment_attempts(batch_ids)
                    new_attempts = max_attempts + 1
                    await self.store.update_task(task_id, "failed", f"超时/失败，重试 {new_attempts}/{self.max_compress_retry}")
                    logger.info("[alife_memory] 压缩超时/失败，重试 %d/%d: %d 条", new_attempts, self.max_compress_retry, len(batch_ids))
                    pending = await self.store.pending_messages(sid, self.compression_batch)
                    continue
                # 结构化解析：支持 {"items":[...]}，每条带 memory_type/tags/reason/scenario/entity_id
                items = _cos_structured_json(result)
                if not items:
                    await self.store.update_task(task_id, "failed", "模型未返回有效结构化记忆")
                    batch_ids = [x["id"] for x in batch]
                    await self.store.increment_attempts(batch_ids)
                    pending = await self.store.pending_messages(sid, self.compression_batch)
                    continue
                fingerprint = hashlib.sha256((sid + "|" + "|".join(x["id"] for x in batch)).encode()).hexdigest()
                refs = [{"sid": sid, "user_id": str(x.get("user_id", "")), "message_id": x["id"], "turn_no": x.get("turn_no", 0), "ts": x["ts"]} for x in batch]
                archived: list[str] = []
                for item in items:
                    if not item.get("summary") or not item.get("content"):
                        continue
                    vector = await self._embed(item["summary"] + "\n" + item["content"])
                    one_refs = refs + [{"sid": item["entity_id"] or "", "user_id": str(item.get("entity_id", "") or ""), "note": "关联实体", "ts": batch[-1]["ts"]}] if item.get("entity_id") else refs
                    aid = await self.store.add_memory(
                        sid, 1, item["summary"], item["content"], batch[0]["ts"], batch[-1]["ts"],
                        [x["id"] for x in batch], _num(item.get("importance", 0.55), 0.55, 0.0, 1.0), vector,
                        user_id=str(batch[0].get("user_id", "")) if len({x.get("user_id", "") for x in batch}) == 1 else "",
                        source_fingerprint=(fingerprint + "|" + item["summary"][:40]), source_refs=one_refs,
                        embed_model=self.embedding_model,
                        memory_type=item.get("memory_type", "日常事件"),
                        tags=item.get("tags") or [],
                        reason=item.get("reason", ""), scenario=item.get("scenario", ""),
                        entity_id=item.get("entity_id", ""))
                    archived.append(aid)
                if not archived:
                    await self.store.update_task(task_id, "failed", "结构化输出无有效记忆")
                    batch_ids = [x["id"] for x in batch]
                    await self.store.increment_attempts(batch_ids)
                    pending = await self.store.pending_messages(sid, self.compression_batch)
                    continue
                await self.store.mark_compressed([x["id"] for x in batch], archived[0])
                await self.store.update_task(task_id, "completed", archived[0])
                logger.debug("[alife_memory] 压缩成功: %d 条 → %s", len(batch), archived[0])
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
            items = _cos_structured_json(result)
            if not items or not items[0].get("summary"):
                continue
            merge_item = items[0]
            summary = merge_item["summary"]; detail = merge_item["content"]
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
                [x["id"] for x in group], _num(merge_item.get("importance", 0.65), 0.65, 0.0, 1.0), vector,
                user_id=users[0] if len(users) == 1 else "", source_refs=refs,
                embed_model=self.embedding_model,
                memory_type=merge_item.get("memory_type", "日常事件"),
                tags=merge_item.get("tags") or [],
                reason=merge_item.get("reason", ""), scenario=merge_item.get("scenario", ""),
                entity_id=merge_item.get("entity_id", ""))
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
            result = await self._llm_json(prompt, self.reflect_model)
            if not result:
                continue
            action = str(result.get("action", "none")).lower()
            # 结构化审计：items 里给出修正条目（含 memory_type/tags/reason/scenario 修正）
            corr_items = _cos_structured_json(result)
            if action == "none" and not corr_items:
                continue
            if action == "none":
                action = "correct"
            # 置信度：整体或单条
            base_confidence = _num(result.get("confidence", 0), 0, 0, 1)
            if action == "stale":
                await self.store.mark_stale(memory["id"], "后台审校发现旧/过期")
                continue
            if action == "correct" and corr_items:
                ci = corr_items[0]
                confidence = _num(ci.get("confidence", base_confidence), 0, 0, 1)
                if confidence < 0.86:
                    continue
                note = str(ci.get("reason") or result.get("reason") or "后台审校发现新证据")[:500]
                summary = str(ci.get("summary", "")).strip()
                content = str(ci.get("content", "")).strip()
                if summary and content:
                    vector = await self._embed(summary + "\n" + content)
                    await self.store.correct_memory(
                        memory["id"], summary, content, note, confidence, vector,
                        memory_type=ci.get("memory_type", memory.get("memory_type", "")),
                        tags=ci.get("tags") or memory.get("tags") or [],
                        reason=ci.get("reason", memory.get("reason", "")),
                        scenario=ci.get("scenario", memory.get("scenario", "")),
                        entity_id=ci.get("entity_id", memory.get("entity_id", "")))

    # ---- 画像（批次 B）----
    async def _ensure_profile(self, entity_id: str, entity_type: str = "user") -> None:
        """确保某实体有画像。若不存在，用其记忆聚合并由 LLM 生成画像。"""
        if not getattr(self, "profile_enabled", True) or not entity_id:
            return
        profile = await self.store.get_profile(entity_id)
        if profile:
            return
        try:
            await self._generate_profile(entity_id, entity_type)
        except Exception as exc:
            logger.warning("[alife_memory] 生成画像失败 %s: %s", entity_id, exc)

    async def _generate_profile(self, entity_id: str, entity_type: str = "user") -> dict | None:
        """从该实体的记忆聚合并由 LLM 生成结构化画像，落库。"""
        memories = await self.store.list_memories_by_entity(entity_id, 200)
        if not memories:
            return None
        cand = [m for m in memories if m.get("status") == "active"][:60]
        if not cand:
            return None
        snippet = "\n".join(
            f"- [{m.get('memory_type','日常事件')}] {m['summary']} | 来源:{m.get('sid','')} 时间:{time.strftime('%Y-%m-%d', time.localtime(m['end_ts']))}"
            for m in cand)
        prompt = (
            f"你是用户画像提炼师。根据下面的记忆片段，提炼出实体「{entity_id}」（类型:{entity_type}）的结构化画像。\n"
            "只输出 JSON，不要 Markdown：{\"name\":\"\",\"nickname\":\"\",\"description\":\"一句话画像概述\","
            "\"traits\":[\"特质，如：关系_莎娜: 莎娜的主人\"],\"preferences\":{\"偏好键\":\"值\"},"
            "\"relationships\":[{\"target_id\":\"对象\",\"relation\":\"关系\",\"confidence\":0.9}],"
            "\"facts\":[\"关键事实\"],\"aliases\":[\"别名\"]}\n"
            "traits 是稳定特质/角色；preferences 是喜好/习惯/禁止项；relationships 是与其他实体（人或AI）的关系；facts 是关键事实。\n"
            "只基于给定记忆提炼，不要臆测。每条画像信息应能在记忆里找到依据。\n记忆片段：\n{snippet}"
        ).replace("{snippet}", snippet)
        result = await self._llm_json(prompt, self.profile_generate_model or "")
        if not isinstance(result, dict):
            return None
        profile = {
            "entity_id": entity_id, "entity_type": entity_type,
            "name": str(result.get("name", "") or ""),
            "nickname": str(result.get("nickname", "") or ""),
            "description": str(result.get("description", "") or ""),
            "traits": result.get("traits") if isinstance(result.get("traits"), list) else [],
            "preferences": result.get("preferences") if isinstance(result.get("preferences"), dict) else {},
            "relationships": result.get("relationships") if isinstance(result.get("relationships"), list) else [],
            "facts": result.get("facts") if isinstance(result.get("facts"), list) else [],
            "aliases": result.get("aliases") if isinstance(result.get("aliases"), list) else [],
            "platform": str(result.get("platform", "") or ""),
            "generated_from": ",".join(m["id"] for m in cand[:40]),
            "memory_type": entity_type,
        }
        await self.store.upsert_profile(entity_id, entity_type, **profile)
        for rel in profile.get("relationships", []):
            if isinstance(rel, dict) and rel.get("target_id") and rel.get("relation"):
                await self.store.upsert_relationship(entity_id, str(rel["target_id"]), str(rel["relation"]),
                                                     source_mid=profile.get("generated_from", ""),
                                                     confidence=float(rel.get("confidence", 0.7)))
        logger.info("[alife_memory] 已生成画像 %s: %d traits / %d relations", entity_id,
                    len(profile.get("traits", [])), len(profile.get("relationships", [])))
        return profile

    async def _format_profile_block(self, entity_id: str, scoped: str = "画像") -> str | None:
        """格式化某实体画像为注入块文本。"""
        p = await self.store.get_profile(entity_id)
        if not p or not (p.get("description") or p.get("traits") or p.get("preferences") or p.get("facts")):
            return None
        lines = [f"[{scoped}:{entity_id}] {p.get('description', '')}".strip()]
        if p.get("traits"):
            lines.append("  特质：" + "；".join(str(t) for t in p["traits"][:8]))
        if p.get("preferences"):
            prefs = p["preferences"]
            if isinstance(prefs, dict):
                lines.append("  偏好：" + "；".join(f"{k}→{v}" for k, v in list(prefs.items())[:10]))
        if p.get("facts"):
            lines.append("  事实：" + "；".join(str(f)[:60] for f in p["facts"][:8]))
        return "\n".join(lines)

    async def _inject(self, sid: str, req: LLMRequest, user_id: str = "", recall_user_ids: list[str] | None = None,
                     do_passive_update: bool = False):
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
        # context marker 已注入的记忆不再重复注入，避免上下文冗余
        seen_ids.update(getattr(self, "_ctx_injected_ids", ()) or ())
        scope = self.search_scope if self.cross_user_enabled else ("user" if user_id else "session")
        query_embed = await self._embed(str(query))

        # 1) 主注入：整句语义搜索
        content_results = await self.store.search(sid, str(query), self.retrieval_top_k + 2,
            list(range(self.inject_level_max + 1)), query_embed, self.half_life, scope, user_id,
            self.embedding_model)
        content_results = [r for r in content_results if r.get("status", "active") == "active"]
        best_score = 0.0
        for r in content_results:
            if r["id"] not in seen_ids:
                all_results.append(r)
                seen_ids.add(r["id"])
                best_score = max(best_score, r.get("score", 0.0))
        logger.debug("[alife_memory] 主注入: query='%s' → %d 条, 最高分 %.2f", query[:40], len(content_results), best_score)

        # 2) 关键词被动召回池：把关键词召回 / 被动更新 / 宽召回统一收集 term，
        #    一次并发 embed + 一次并发 search 全部完成，避免重复调用。
        if self.passive_recall and self.passive_recall_keyword_limit > 0:
            # 收集本轮需要检索的关键词（去重）
            recall_terms: list[tuple[str, float]] = []  # (term, weight)
            seen_terms: set[str] = set()
            # 基础关键词（每轮）
            for kw in re.split(r'[，。！？、：；\s,;:?()（）\n\t]+', str(query)[:200]):
                kw = kw.strip()
                if len(kw) >= 2 and not kw.isdigit() and kw not in seen_terms:
                    seen_terms.add(kw)
                    recall_terms.append((kw, 0.4))
            # 被动更新（每 N 轮）：权重更高，补充新记忆
            if do_passive_update:
                for t in re.split(r'[，。！？、：；\s,;:?()（）\n\t]+', str(query)[:120]):
                    t = t.strip()
                    if len(t) >= 2 and t not in seen_terms:
                        seen_terms.add(t)
                        recall_terms.append((t, 0.5))
            # 宽召回（弱结果时）：降低权重，扩大召回面
            if best_score < self.passive_recall_boost_threshold and best_score > 0:
                for t in re.split(r'[，。！？、\s,;:?]+', str(query)[:80]):
                    t = t.strip()
                    if len(t) >= 2 and t not in seen_terms:
                        seen_terms.add(t)
                        recall_terms.append((t, 0.85))
            recall_terms = recall_terms[:self.passive_recall_keyword_limit * 2]
            if recall_terms:
                # 一次并发 embed 所有 term
                terms = [t for t, _ in recall_terms]
                embeds = await self._embed_batch(terms)
                # 一次并发 search 所有 term
                search_tasks = [
                    self.store.search(sid, term, 3, list(range(self.inject_level_max + 1)),
                        embeds.get(term), self.half_life, "linked", user_id,
                        self.embedding_model)
                    for term in terms
                ]
                search_results = await asyncio.gather(*search_tasks)
                for (term, weight), results in zip(recall_terms, search_results):
                    for r in results:
                        if r["id"] not in seen_ids and r.get("status", "active") == "active":
                            r["score"] = r.get("score", 0.0) * weight
                            all_results.append(r)
                            seen_ids.add(r["id"])
                logger.debug("[alife_memory] 关键词召回池: %d 词 → +%d 条", len(recall_terms), len(seen_ids))

        # 3) 首次见到用户召回（per-session：仅当该会话还没召回过这个用户的记忆）
        if self.passive_recall and recall_user_ids:
            new_uids = [u for u in recall_user_ids if u and u != user_id]
            if new_uids:
                # 一次批量查询所有新用户的跨会话历史记忆，避免逐用户串行 SQL
                for r in await self.store.count_memories_by_users(new_uids, 5):
                    if r["id"] not in seen_ids and r.get("status", "active") == "active":
                        r["score"] = 0.3
                        all_results.append(r)
                        seen_ids.add(r["id"])
                logger.debug("[alife_memory] 被动召回: %d 个新用户 → 补 %d 条跨会话记忆", len(new_uids), len(seen_ids))

        if not all_results:
            return

        # 排序后重排序（如有配置）
        all_results.sort(key=lambda x: (x.get("score", 0.0), x.get("importance", 0.0)), reverse=True)
        reranked = await self._rerank(str(query), all_results[:self.retrieval_top_k * 3])
        if reranked is not all_results[:self.retrieval_top_k * 3]:
            all_results[:self.retrieval_top_k * 3] = reranked

        # 预算截断注入
        lines = []
        token_budget = self.max_injected_tokens
        item_budget = self.max_injected_items
        char_budget = self.max_injected_chars
        line_cnt = 0
        for item in all_results:
            if item_budget <= 0:
                break
            source = f"来源:{item.get('sid', 'unknown')} / 时间:{time.strftime('%Y-%m-%d', time.localtime(item['end_ts']))}"
            text = f"- [{item['summary']}]（{source}）"
            cost = estimate_tokens(text)
            char_cost = len(text)
            if cost > token_budget or char_cost > char_budget:
                if not lines:
                    lines.append(f"- {item['summary'][:200]}")
                break
            lines.append(text)
            line_cnt += 1
            if line_cnt >= self.max_injected_lines:
                break
            item_budget -= 1
            token_budget -= cost
            char_budget -= char_cost
        if not lines:
            return
        # 画像注入（批次 B）：当前用户画像 + 提及的关联实体（上限 profile_inject_max）
        profile_blocks: list[str] = []
        if getattr(self, "profile_inject", True) and getattr(self, "profile_enabled", True):
            # 当前实体（user_id 或按会话取第一个用户）
            cur_entity = user_id or _event_user_id(getattr(req, "messages", [])[-1] if getattr(req, "messages", []) else None)
            if cur_entity:
                # 首次见 → 记录并确保画像
                self._seen_profiles = getattr(self, "_seen_profiles", {})
                if cur_entity not in self._seen_profiles:
                    await self._ensure_profile(cur_entity, "user")
                    self._seen_profiles[cur_entity] = True
                pb = await self._format_profile_block(cur_entity, "画像")
                if pb:
                    profile_blocks.append(pb)
            # 扫码提取提及的关联实体（从 query 中出现的已知实体 id/名字/昵称）
            pmax = getattr(self, "profile_inject_max", 3)
            if pmax and pmax > 0:
                known = await self.store.list_profiles(pmax + 8)
                seen_entities = set()
                for kp in known:
                    eid = kp.get("entity_id", "")
                    if not eid or eid == cur_entity or eid in seen_entities:
                        continue
                    # 匹配实体 id 或其名字/昵称是否出现在 query 中
                    match_keys = [eid] + [str(kp.get("name", "")) for _k in (1,)] + [str(kp.get("nickname", ""))]
                    match_keys = [k for k in match_keys if k]
                    if any(k and k != cur_entity and k in str(query) for k in match_keys):
                        seen_entities.add(eid)
                        pb = await self._format_profile_block(eid, "画像:关联")
                        if pb:
                            profile_blocks.append(pb)
                        if len(profile_blocks) >= pmax + 1:
                            break
        block = ("你曾经的一些记忆：\n" + "\n".join(lines) +
                 '\n（完整的记忆细节可通过 search_long_term_memory 工具检索）')
        if profile_blocks:
            block += "\n\n" + "\n\n".join(profile_blocks) + "\n（以上是相关人物/实体的画像，帮助你记住这些人的身份和关系）"
        system_msg = OpenAIMessage(role="system", content=block)
        req.messages.insert(0, system_msg)
        logger.debug("[alife_memory] 注入完成: %d 条记忆 + %d 画像块, 约 %d tokens", len(lines), len(profile_blocks), self.max_injected_tokens - token_budget)


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
        self._ctx_injected_ids.clear()  # 每次重新记录本轮回注入的 id
        for item in top:
            source = f"来源:{item.get('sid', 'unknown')} / 时间:{time.strftime('%Y-%m-%d', time.localtime(item['end_ts']))}"
            text = f"[{item['summary']}]（{source}）"
            lines.append(f"- {text}")
            self._ctx_injected_ids.add(item["id"])
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
        if not sid:
            return
        messages = getattr(event, "messages", []) or []
        user_id = _event_user_id(messages[-1] if messages else None)
        # 轮次计数
        if self.passive_recall:
            self._recall_round_counter[sid] = self._recall_round_counter.get(sid, 0) + 1
            do_passive_update = self._recall_round_counter[sid] >= self.passive_recall_update_rounds
            if do_passive_update:
                self._recall_round_counter[sid] = 0
                logger.info("[alife_memory] 被动召回更新到达 %d 轮", self.passive_recall_update_rounds)
            recall_user_ids = []
            for msg in messages:
                uid = _event_user_id(msg)
                # per-session 去重：同一会话某用户只在首次出现时召回一次，
                # 换新会话（新 sid）则重新召回——只要该会话上下文还没它的记忆
                if uid:
                    sid_seen = self._seen_recall.setdefault(sid, set())
                    if uid not in sid_seen:
                        sid_seen.add(uid)
                        recall_user_ids.append(uid)
                        logger.debug("[alife_memory] 会话 %s 首次见到用户 %s，准备被动召回其记忆", sid, uid)
        else:
            do_passive_update = False
            recall_user_ids = []
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
                for m in reversed(getattr(req, "messages", []) or []):
                    role = getattr(m, "role", None) or (m.get("role") if isinstance(m, dict) else "")
                    if role == "user":
                        query = getattr(m, "content", None) or (m.get("content") if isinstance(m, dict) else "") or ""
                        break
                if query and any(kw in query for kw in self.recall_hint_keywords):
                    hint = "（提示：用户的话可能涉及过往记忆，你可通过 search_long_term_memory 工具检索相关记忆）"
                    req.messages.insert(0, OpenAIMessage(role="system", content=hint))
                    logger.debug("[alife_memory] 触发回忆关键词提醒: %s", query[:50])
            # 记忆注入
            if self.auto_inject:
                await self._inject(sid, req, user_id, recall_user_ids, do_passive_update)
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
                            memory_type: str = "日常事件", tags: str = "",
                            importance: float = 0.5, reason: str = "", when_it_matters: str = "") -> str:
        sid = getattr(event, "sid", "")
        if not sid or not content.strip():
            return "还不知道是在哪个会话说的，先聊起来再记吧"
        # 校验类型（新 6 类 + 兼容旧 5 类）
        valid_types = {"关于实体", "偏好风格", "约定任务", "关系网络", "溯源查询", "日常事件"}
        legacy_types = {"关于我的": "关于实体", "我喜欢的": "偏好风格", "正在发生的": "约定任务",
                        "去哪查": "溯源查询", "日常的": "日常事件"}
        memory_type = memory_type.strip()
        if memory_type in legacy_types:
            memory_type = legacy_types[memory_type]
        if memory_type not in valid_types:
            memory_type = "日常事件"
        # 偏好和约定类强制要求原因和适用场景
        if memory_type in ("偏好风格", "约定任务"):
            if not reason.strip() or not when_it_matters.strip():
                return f"关于「{memory_type}」类的事情，最好也告诉我为什么是这样、什么时候该想起它，这样以后用起来才不迷糊。"
        summary = summary.strip() or content.strip().splitlines()[0][:100]
        user_id = _event_user_id((getattr(event, "messages", []) or [None])[-1])
        now = time.time()

        # 相对日期 → 绝对日期转换
        processed_content = self._convert_relative_dates(content)
        processed_reason = self._convert_relative_dates(reason) if reason else ""
        processed_when = self._convert_relative_dates(when_it_matters) if when_it_matters else ""

        # 标签解析（逗号分隔字符串 → 数组）
        tag_list = [t.strip() for t in tags.replace("，", ",").split(",") if t.strip()] if tags else []

        # 结构化落库：不塞字符串前缀，直接存字段
        vector = await self._embed(summary + "\n" + processed_content)
        mid = await self.store.add_memory(
            sid, self.max_level, summary, processed_content,
            now, now, [], importance, vector, user_id=user_id,
            memory_type=memory_type, tags=tag_list,
            reason=processed_reason, scenario=processed_when,
            entity_id=user_id,
            source_refs=[{"sid": sid, "user_id": user_id, "ts": now,
                          "action": "主动记录", "type": memory_type}],
            embed_model=self.embedding_model)
        return f"已经记下了：{summary[:80]}……（ID: {mid}，来源会话和保存时间已自动记录）"

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
        results = await self.store.search(sid, query, _num(top_k, 5, 1, 12, True), list(range(self.max_level + 1)), await self._embed(query), self.half_life, scope, user_id, self.embedding_model)
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
        tags = memory.get("tags") or []
        mt = memory.get("memory_type") or "日常事件"
        parts = [
            f"记忆 ID: {memory['id']}",
            f"层级: L{memory['level']} | 类型: {mt}",
            f"状态: {memory.get('status', 'active')}",
            f"置信度: {memory.get('confidence', 0.65)*100:.0f}%",
            f"时间范围: {t_start} → {t_end}",
            f"概要: {memory['summary']}",
            f"详细内容: {memory['content']}",
        ]
        if tags:
            parts.insert(0, f"标签: {'、'.join(tags)}")
        if memory.get("reason"):
            parts.append(f"为什么记: {memory['reason']}")
        if memory.get("scenario"):
            parts.append(f"何时想起: {memory['scenario']}")
        if memory.get("entity_id"):
            parts.append(f"关联实体: {memory['entity_id']}")
        if source_info:
            parts.append(f"来源引用:{source_info}")
        if memory.get("correction_note"):
            parts.append(f"修正备注: {memory['correction_note']}")
        if memory.get("supersedes"):
            parts.append(f"旧版本被取代: {memory['supersedes']}")
        return "\n".join(parts)

    @register.tool(
        name="list_users",
        description="列出我记住的所有用户，以及每个用户的记忆数量和最近记忆概要。当用户问‘你记得哪些人/多少用户’时使用。特别适合找回某个用户，但人数很多时请加大 limit。",
        params={"type": "object", "properties": {"limit": {"type": "integer", "description": "本次列出多少个用户；留空则用配置默认值（默认50）。总数始终显示。"}}, "required": []})
    async def list_users(self, event: KiraMessageBatchEvent, limit: int | None = None) -> str:
        total = await self.store.count_users()
        if total == 0:
            return "我目前还没有记住任何用户。"
        shown = _num(limit, self.user_list_limit, 1, 500, True) if limit is not None else self.user_list_limit
        users = await self.store.list_users(shown)
        out = [f"我记得一共 {total} 个用户（显示前 {len(users)} 个）："]
        for u in users:
            ts = time.strftime('%Y-%m-%d', time.localtime(u["last_ts"])) if u["last_ts"] else "未知"
            max_lv = u.get("max_level") or 1
            out.append(f"\n- {u['user_id']} | {u['cnt']} 条记忆 | 最高层 L{max_lv} | 最近 {ts}")
            # 最高权重×层级的那条
            if u.get("top_summary"):
                out.append(f"  ★ 高权重: L{u.get('top_level',1)} 重要{u.get('top_importance',0.0):.2f} → {u['top_summary'][:90]}")
            # 最近记得的几条（数量用配置）
            recents = u.get("recent_summaries") or []
            rec_cnt = self.recent_summaries_count
            if recents:
                out.append(f"  最近记得:")
                for rs in recents[:rec_cnt]:
                    out.append(f"    · {rs[:80]}")
        if total > len(users):
            out.append(f"\n… 还有 {total - len(users)} 个用户未显示（可增大 limit 查看）")
        return "\n".join(out)

    @register.tool(
        name="list_user_memories",
        description="精确列出某个用户（指定 user_id）的全部记忆，跨会话。按重要性×层级优先，兼顾最近。当你要回忆某个具体用户的相关事情时使用，比笼统检索更准。",
        params={"type": "object", "properties": {"user_id": {"type": "string", "description": "要查询的用户标识，可通过 list_users 获取"}, "limit": {"type": "integer", "description": "本次列出多少条；留空则用配置默认值（默认20）。该用户总数始终显示。"}}, "required": ["user_id"]})
    async def list_user_memories(self, event: KiraMessageBatchEvent, user_id: str, limit: int | None = None) -> str:
        total = await self.store.count_memories_by_user(user_id)
        if total == 0:
            return f"没有找到用户 {user_id} 的记忆。"
        shown = _num(limit, self.memory_list_limit, 1, 200, True) if limit is not None else self.memory_list_limit
        memories = await self.store.list_memories_by_user(user_id, shown)
        out = [f"用户 {user_id} 共有 {total} 条记忆（显示前 {len(memories)} 条，按重要性×层级排序）："]
        for m in memories:
            t = time.strftime('%Y-%m-%d', time.localtime(m["end_ts"]))
            conf = m.get("confidence", 0.65)
            out.append(f"\n- L{m['level']} | 重要{m.get('importance', 0.5):.2f} | 置信{conf*100:.0f}% | {t} | {m['summary']}")
        if total > len(memories):
            out.append(f"\n… 还有 {total - len(memories)} 条未显示（可增大 limit 查看）")
        return "\n".join(out)

    @register.tool(
        name="list_session_memories",
        description="按会话（群聊/群组）列出该会话的全部记忆，适合群聊归因会话的记忆。按重要性×层级优先，兼顾最近。当用户问‘这个群里聊过什么/我记得这个会话的什么事’时使用。",
        params={"type": "object", "properties": {"sid": {"type": "string", "description": "要查询的会话标识（如 adapter:dm|gm:id），可通过实际对话上下文获取"}, "limit": {"type": "integer", "description": "本次列出多少条；留空则用配置默认值（默认20）。该会话总数始终显示。"}}, "required": ["sid"]})
    async def list_session_memories(self, event: KiraMessageBatchEvent, sid: str, limit: int | None = None) -> str:
        total = await self.store.count_memories_by_sid(sid)
        if total == 0:
            return f"没有找到会话 {sid} 的记忆。"
        shown = _num(limit, self.memory_list_limit, 1, 200, True) if limit is not None else self.memory_list_limit
        memories = await self.store.list_memories_by_sid(sid, shown)
        out = [f"会话 {sid} 共有 {total} 条记忆（显示前 {len(memories)} 条，按重要性×层级排序）："]
        for m in memories:
            t = time.strftime('%Y-%m-%d', time.localtime(m["end_ts"]))
            out.append(f"\n- L{m['level']} | 重要{m.get('importance', 0.5):.2f} | {t} | {m['summary']} | 用户 {m.get('user_id','') or '—'}")
        if total > len(memories):
            out.append(f"\n… 还有 {total - len(memories)} 条未显示（可增大 limit 查看）")
        return "\n".join(out)

    @register.page("/index", menu=PageMenu(label={"zh": "长期记忆·Z", "en": "Memory·Z"}, icon="Brain", order=90))
    def page(self):
        return PluginPage.from_folder("./web")

    # ---- API ----

    @register.api(method="GET", path="/status", auth=True, summary="Memory status")
    async def api_status(self):
        result = await self.store.stats()
        # 画像/关系统计（批次 B）
        profiles = await self.store.list_profiles(10000)
        rels = await asyncio.to_thread(self.store._list_relationships, None, None)
        result.update({"enabled": self.enabled, "capture_enabled": self.capture_enabled, "auto_inject": self.auto_inject,
                       "passive_recall": self.passive_recall, "workers": len(self._workers),
                       "reflection": self.reflect_enabled, "trigger_mode": self.trigger_mode,
                       "round_threshold": self.round_threshold, "token_threshold": self.token_threshold,
                       "message_threshold": self.message_threshold,
                       "max_level": self.max_level, "auto_archive_days": self.auto_archive_days,
                       "archive_level_min": self.archive_level_min,
                       "profiles": len(profiles), "relationships": len(rels) if isinstance(rels, list) else 0})
        return result

    @register.api(method="GET", path="/pending", auth=True, summary="Pending compression stats")
    async def api_pending(self):
        pending = await self.store.pending_messages_all()
        return {"round_count": pending.get("round_count", 0), "token_sum": pending.get("token_sum", 0),
                "message_count": pending.get("message_count", 0)}

    @register.api(method="GET", path="/memories", auth=True, summary="List memories")
    async def api_memories(self, sid: str | None = None, limit: int = 100,
                           level: int | None = None, memory_type: str | None = None,
                           tag: str | None = None, user_id: str | None = None):
        return await self.store.list_memories_filtered(
            sid or None, level, memory_type, tag, user_id,
            _num(limit, 100, 1, 500, True))

    @register.api(method="GET", path="/profiles", auth=True, summary="List entity profiles")
    async def api_profiles(self, limit: int = 100, entity_type: str | None = None):
        return await self.store.list_profiles(_num(limit, 100, 1, 500, True), entity_type or None)

    @register.api(method="GET", path="/profile/{entity_id}", auth=True, summary="Get one profile")
    async def api_profile(self, entity_id: str):
        return await self.store.get_profile(entity_id) or {"error": "not found"}

    @register.api(method="GET", path="/relationships", auth=True, summary="List relationship graph")
    async def api_relationships(self, entity_id: str | None = None, target_id: str | None = None):
        return await self.store.list_relationships(entity_id or None, target_id or None)

    @register.api(method="POST", path="/profile/{entity_id}/regenerate", auth=True, summary="Regenerate a profile")
    async def api_profile_regenerate(self, entity_id: str):
        p = await self._generate_profile(entity_id, "user")
        return {"ok": bool(p)}

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
        result = await self.store.search(sid, query, _num(body.get("top_k", self.retrieval_top_k), self.retrieval_top_k, 1, 20, True), None, await self._embed(query), self.half_life, scope, user_id, self.embedding_model)
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
        # 持久化：原子写回配置文件，避免热保存后重启丢失
        try:
            self._persist_config()
        except Exception:
            logger.exception("[alife_memory] persist config failed")
        # 热更新后重新读取设置，并重置 rerank 探测缓存（让新配置立即生效）
        try:
            self._load_settings()
        except Exception:
            logger.exception("[alife_memory] reload settings after config update failed")
        self._rerank_checked = False
        self._rerank_client = None
        # embedding 探测缓存同样重置，让新配的向量模型立即生效
        self._embed_checked = False
        self._embed_client = None
        return {"ok": True}

    def _persist_config(self):
        """把当前 cfg 原子写回配置文件（tmp + os.replace），并同步框架内存缓存，避免写坏/不一致。"""
        cfg_path = Path(getattr(self, "_config_path", "")) or (get_config_path() / "plugins" / f"{PLUGIN_ID}.json")
        cfg_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = cfg_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.cfg, ensure_ascii=False, indent=4), encoding="utf-8")
        tmp.replace(cfg_path)
        # 同步框架内存 plugin_configs（如果可达），确保框架侧读到最新配置
        try:
            pm = getattr(self.ctx, "plugin_mgr", None)
            if pm is not None and hasattr(pm, "plugin_configs"):
                pm.plugin_configs[PLUGIN_ID] = self.cfg
        except Exception:
            logger.debug("[alife_memory] sync plugin_configs cache skipped")
          