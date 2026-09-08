# 2.2.3：冷归档（只能按 ID 读）

## 为什么需要第三层

只有"常驻 / 归档"两档时，归档会无限累积，检索迟早又会被塞满。冷归档是第三层：**不进上下文、任何检索都搜不到、只能按存档 ID 读取**。

## 什么时候进入冷归档

| 触发 | 时机 | 说明 |
| --- | --- | --- |
| **Forget**（人工或 bot 移出常驻） | 立即 | 明确不要了，直接冷 |
| **永久记忆合并中落选的那几条** | 立即 | 模型已判定"重复或已被更正" |
| **归档满 `cold_after_days` 天** | 自动（计算得出） | 默认 180 天；0=不按时间冷 |

其余"看起来像错的"一概不动——没有自动"判错"这回事。

## 数据与状态

`records` 新增两列（`user_version` 5）：

- `cold INTEGER DEFAULT 0`：显式冷归档；
- `archived_at REAL DEFAULT 0`：进入归档的时刻，用于计算 180 天。

```
常驻(active=1) --压缩/合并/Forget--> 归档(active=0, archived_at=now)
归档 --满 N 天--> 冷（计算得出，不写库）
归档 --Forget/合并落选--> 冷（cold=1，立即）
冷 --「恢复到常驻上下文」--> 常驻（cold=0, archived_at=0）
```

升级旧库时，已有归档记录的 `archived_at` 会回填为**升级时刻**，不会让老数据立刻变冷。

## 检索与读取

| 路径 | 行为 |
| --- | --- |
| 默认检索 | 只搜常驻 |
| `include_archived=true` | 常驻 + 归档（**冷归档仍然不出现**） |
| `ReadMemoryArchive(id)` | 任何记录都能读，冷归档也不例外 |
| WebUI「记忆存档」页 | 全部可见，冷归档卡片显示「冷归档 · 仅按ID可读」，可一键恢复常驻 |

按 ID 读取仍然能拿到完整内容与子存档清单，所以"父存档 → children → 原文"的回溯链不会断。

## 配置

- `cold_after_days`（默认 180，0=不按时间冷归档），设置页热更改。

## 验证

- `tests/test_context_hygiene.py`：Forget 后冷归档且检索为 0、按 ID 仍可读、恢复后重新可搜；归档满 180 天自动移出检索、`cold_after_days=0` 时恢复可见。
- `tests/test_permanent_dedupe.py`：合并落选的记录 `cold=1`。
- 旧库迁移实测：v3 库打开后自动加列、`user_version=5`、旧归档 `archived_at` 已回填。
- 工具级端到端：Forget 后默认 0 / 含归档 0 / 按 ID 读得到 / 恢复后 1。
- 全量 `pytest tests/`：**73 passed, 1 skipped, 7 subtests passed**。
