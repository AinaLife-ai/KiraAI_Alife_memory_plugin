# 2.2.1：压缩可见性修复、批量昵称与星图交互

## 分层压缩失败修复

迁移并入私聊会话的用户档案带 `visibility=user`，与实时消息的 `visibility=session` 在同一层混排时，`compression_plan` 会把两者放进同一批，`Store.compress` 因“一个存档只能有一种可见性”抛出 `ValueError: mixed visibility cannot be compressed`。由于每轮都选同一批最早的记录，该会话的整理会持续失败。

- `compression_plan` 现在按 `(level, visibility)` 分组，同一可见性内部达到阈值才压缩；混合会话各自独立推进。
- 任务失败详情改为可读原因（`failure_detail`），并把 `mixed visibility cannot be compressed`、`source changed during compression` 等纳入已知错误白名单，后台任务页可直接看到原因。
- 修复后卡住的会话在下次自动整理时即可完成，也可在「后台任务」页手动点「压缩存档」。

## 一键批量拉取昵称

首次进入「人物与群名」页时，若存在“有号码但没有当前称呼”的实体，会弹窗询问是否从聊天平台一次性查询。

- 只更新显示名；稳定 ID 不变，同名不合并；**已有名字的跳过**。
- 之后每条新消息仍会记录昵称与群名，随时可在本页人工修改。
- 新增 `POST /names/refresh-batch`：逐个查询（群 `get_group_info`、人 `get_user_info`），间隔 150ms，单次最多 200 个，返回成功/失败/剩余数量。审计理由统一写入 `批量确认当前QQ昵称`。
- 「不再提示」记录在浏览器本地。

## 星图交互与动效

- 点击节点：下方出现该实体的联结面板（谓词 → 对方 + 「编辑依据」），事实卡片区同时筛选为相关事实；星图本身保持完整，不因筛选而丢失其他节点。
- **淡化连线不可点击**：被淡化的连线 `pointer-events:none`、`tabindex=-1`，点击处理再兜一层判断，避免误开到别的证据；只有高亮连线可打开编辑器。
- 切换栏目时缩放回到 100%；同页重绘保持。
- 节点名字按实体 ID 取色（8 色，明暗主题各一套），节点有轻微随机漂浮与名字呼吸，整张星图缓慢漂移，鼠标悬停时暂停漂移。
- 新增 ✨ 动效开关：默认跟随系统；系统开启“减少动态效果”时可用它强制开启，并在首次进入时提示一次。
- 若浏览器/系统开启“减少动态效果”且未强制开启，所有动画与过渡按无障碍设置停用。

## 重新迁移的保证

`migration_items(source, source_key, digest)` 幂等；迁移摘要按原始标识计算，跨版本一致。重复迁移只计 `duplicate`：不新增、不覆盖人工修改、不复活已删记录、原文件只读。已加入回归测试 `test_rerun_migration_keeps_edits_and_deletions`。

## 加载与依赖

`initialize()` 中的身份规范化与迁移全部包裹异常处理，任何失败只记录日志，不会让插件加载失败；`requirements.txt` 补充 `openai`（`main.py` 顶层导入）。

## 验证

- 全量 `pytest tests/`：**53 passed, 1 skipped, 7 subtests passed**（新增混合可见性压缩、重迁移幂等两项）。
- 浏览器以真实 `app.js` + 模拟接口验证：节点点击面板与卡片筛选、淡化线不可点、缩放切页重置、动效开关、批量昵称弹窗端到端。
