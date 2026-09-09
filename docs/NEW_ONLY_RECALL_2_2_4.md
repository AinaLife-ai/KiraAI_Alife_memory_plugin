# 2.2.4：同一会话只给新情报

## 目标

同一轮对话里，模型不该反复拿到同一批记忆。搜索默认只返回**本次会话还没给过**的内容。

## 机制

每个「会话 + 参与者 + 访问范围」维护一个**已送达集合**（内存态，30 分钟过期，最多 300 条 ID，最多 256 组）：

- 搜索返回的存档；
- 每轮注入的存档与事实（它们本来就在模型眼前）；
- 按 ID 读过的存档。

`SearchMemoryArchive` 默认排除这些 ID，只给新情报：

```json
{ "ok": true, "total": 0, "already_seen": 3,
  "hint": "本轮没有新内容：相关记忆此前已经给过。如需重看，用 ReadMemoryArchive(id)，或传 allow_seen=true 重搜。" }
```

- **`allow_seen=true`**：显式要求重搜（跳过排除）；
- **`ReadMemoryArchive(id)`**：永远能重读指定存档；
- `MemoryOverview` 的**事实**同样去重：已给过的事实不再返回，`already_seen` 报告跳过了多少。每次最多返回 **50 条**新事实——不足 50 条时一次给完，之后再调用只会报告 `already_seen`；库里超过 50 条时才会分多次取下一批。

## 顺带删除：写死的「还有别的吗」启发式

原来 `asks_for_more()` 用 `re.fullmatch` 匹配 8 个固定短语（还有别的吗 / 还有呢 / 再想想…），整句不完全相等就不触发，换种说法就失效。现在**直接删除**：

- 模型驱动为主：`MEMORY_RULES` 已说明"用户追问还有别的时用 `SearchMemoryArchive(next_batch=true)` 找新证据"；
- 2.2.4 之后任何一次搜索都只给新内容，启发式的边际价值只剩"省一次工具调用"；
- 少一条隐式行为，`RecallWindow` 的语义也变干净：只负责"记下已送达的 ID + 上次搜索主题"。

## 边界

- 每轮的**常驻记忆块不受影响**——它是模型的上下文，不是"搜索结果"；去重只作用于"再看一遍"的动作。
- 窗口是内存态：重启、换会话、超过 30 分钟自动重置，等于新对话重新开始。
- 冷归档仍然任何检索都搜不到；归档需 `include_archived=true`。

## 验证

- `tests/test_plugin_tools.py`：同一关键词搜两次，第二次 `items=[]`、`already_seen=3`、提示可用 `ReadMemoryArchive`/`allow_seen`；`allow_seen=true` 重新返回 3 条。
- 按 ID 读过的记忆也会被后续搜索排除（实测 `already_seen=1`）。
- `tests/test_output_diagnostics.py` 窗口上限断言更新为 300。
- 全量 `pytest tests/`：**74 passed, 1 skipped, 7 subtests passed**。
