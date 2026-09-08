# 2.2.3：检索范围——默认只搜常驻，归档需显式

## 背景

压缩会把原文收进上层摘要（原文 `active=0`，仍保留在库中）。原来的检索不过滤 `active`，于是**已归档的旧记录会被搜到，而且按时间排序还排在父摘要前面**，既重复又挤占名额。

## 对齐原版

Alife 原版没有"常驻 / 归档"这一对概念：压缩后原文直接从聊天历史移除，检索只在档案库里按**指定层级**进行（默认 L3），所以它天然不会翻出旧原文。本移植为了审计、编辑、版本回溯与迁移恢复，保留了全部记录，因此需要显式区分。

## 规则

| 调用 | 行为 |
| --- | --- |
| `SearchMemoryArchive` 默认 | 只搜**常驻**（`active=1`）：近期原文、活跃摘要、永久记忆 |
| `SearchMemoryArchive(include_archived=true)` | 常驻 + 归档，每条带 `archived: true/false` |
| 被动跨会话召回 | 只取常驻，结果同样带 `archived` 标记 |
| `ReadMemoryArchive(id)` | 任何存档都能读（逐层回溯原文的正路，不受限制） |
| WebUI「记忆存档」页 | 不变，人翻档案仍可搜全部记录 |

配置项 `search_active_only`（设置页热更改，**默认开启**）：

- 开启：上面的默认行为；
- 关闭：默认连归档一起搜（保留旧行为）。

## 验证

- `tests/test_context_hygiene.py` 新增：压缩出归档记录后，默认检索只返回常驻（`total=3`），去掉过滤返回全部（`total=5`）。
- 工具级端到端：默认 3 条全部 `archived:false`；`include_archived=true` 返回 5 条并正确标记；关闭配置后默认返回 5 条。
- 全量 `pytest tests/`：**71 passed, 1 skipped, 7 subtests passed**。
