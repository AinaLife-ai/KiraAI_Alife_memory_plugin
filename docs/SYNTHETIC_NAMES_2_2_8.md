# 2.2.8：不再被第三方插件的合成昵称污染

## 问题

[KiraAI-Reminder-Plugin](https://github.com/xgrbhc/KiraAI-Reminder-Plugin) 触发提醒时会发布一条**合成消息**，把发送者设成被提醒的用户、昵称写成固定占位符：

```python
sender_name = "提醒任务所有者"
KiraIMMessage(sender=User(user_id=sender_id, nickname=sender_name), is_notice=True, ...)
```

本插件每轮都会从消息里记录昵称（`observe_event_names`），于是真实昵称被 `提醒任务所有者` 覆盖——`observed` 是提醒触发时间，比真实昵称更新，所以 `observe_name` 照单全收。

## 改动

**1. 不再记录合成昵称**
`observe_event_names` 跳过：

- `is_notice=True` 的消息（第三方插件的合成事件都是通知型）；
- 昵称命中合成占位集合的（`提醒任务所有者`、`Kira`、`system`、`Web UI 用户`、`Web UI 管理员`、`自主意图循环`、`未知`）；
- 全部消息都是通知时，也不再用 `session_title` 更新会话名（DM 的合成事件会把会话名也写成占位符）。

**2. 自动修复已被污染的数据**
启动时执行 `repair_synthetic_names()`：当前名是合成占位符、且历史里存在更早的正常称呼时，**恢复最近一个正常称呼**，并写入一条 `source=repair` 的历史记录（理由「忽略第三方插件的合成昵称」）。幂等，只处理确实被污染且能恢复的实体。

后台任务页新增「**修复被改写的昵称**」按钮可手动重跑。

**3. 顺手瘦身 `MemoryNames` 返回**
工具不再回传 `label` / `identity_note` / 完整 `history`（每实体最多 50 条），只给 `id / kind / name / revision / aliases`（+ 必要时 `lookup_id`）。`revision` 保留，供 `CorrectMemoryName` 使用。

## 验证

- 真实消息记名「萤火」→ 收到提醒通知后仍是「萤火」（用户实体与会话实体都不变）；
- 模拟已被污染的数据（名=提醒任务所有者）→ 修复后恢复「萤火」，历史新增一条 repair 记录，二次运行返回空（幂等）；
- `MemoryNames` 返回字段集合被断言收窄；
- 全量 `pytest tests/`：**80 passed, 1 skipped, 7 subtests passed**。
