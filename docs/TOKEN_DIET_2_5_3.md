# v2.5.3 上下文瘦身（2026-09-10）

目标：在不牺牲「记忆感」的前提下，把长期记忆的 token 开销压到最低。
所有数字来自真实代码路径的实测（脚本见文末），不是估算。

## 一、瘦身前后（单会话、80 轮/天、人设 8000 字符）

| 调用 | 改前 | 改后 | 变化 |
| --- | --- | --- | --- |
| 每轮注入 | 16,227 字符 ≈ 4,660 token | **1,993 字符 ≈ 627 token** | **-88%** |
| 压缩请求 | 15,158 字符 ≈ 4,872 token | **10,637 字符 ≈ 3,750 token** | -23% |
| 审计请求 | 14,196 字符 ≈ 4,571 token | **9,162 字符 ≈ 3,278 token** | -28% |
| 审计人设（默认关） | 每次 +8,000 字符 ≈ 5,700 token | 0 | — |
| 合计 / 天 | ≈ 501k token | **≈ 103k token** | **-79%** |

## 二、注入改了什么

**A. 情境化注入（`inject_mode=situational`，默认）**

| 档位 | 内容 | 触发 |
| --- | --- | --- |
| 常驻 | 参与者、名字表、约定/偏好/画像类事实（每类 ≤ `top_k`）、最近 2 条压缩存档 | 每轮 |
| 触发 | 提到的人的事实（`subject` 精确匹配，含跨会话/跨用户）；命中 `recall_keywords` 时按重要度放宽一档 | 相关时 |
| 按需 | `ReadMemoryArchive` / `SearchMemoryArchive` | 模型判断 |

`inject_mode=full` 保留旧行为，用于回滚与对照。

**B. 事实字段裁剪**（`retrieval.bot_facts`）

| 处置 | 字段 |
| --- | --- |
| 删 | `id` `fingerprint` `deleted` `revision` `merge_pending` `audited` `created` `scenario` `tags` `reason` |
| 改 | `sources` → `src`（最新一条来源指针，由 `storage.attach_evidence` 填）；`relationship_status` → 仅 `needs_review` 时出现；时间戳 → `t` 日期 |
| 条件加 | 跨会话/共享事实才带 `sid` + `by`（最新来源的说话人） |

依据：`scenario`/`tags` 在 bot 侧无任何消费点（`SearchMemoryArchive` 不支持 tag 过滤），
`fingerprint`/`revision`/`deleted`/`merge_pending`/`audited` 是纯内部簿记，
`relationship_status` 的常量值 `evidence_required` 占了注入块 11.9%。

**C. 存档视图**：删 `role`，本会话存档不再重复 `sid`/`users`，时间戳改日期。

**D. `reason` 只给审计与网页端**：注入、`SearchMemoryArchive`、`GetProfile`/`MemoryOverview`
都不再返回 reason；数据库、审计 payload、网页端 `/profile` 保留。
压缩指令加「reason ≤ 40 字」（原契约上限 2000 字，没人约束）。

## 三、模型请求改了什么

- **人设开关**：`compress_persona`（默认开）、`audit_persona`（默认关）。
  人设动辄数千字，是每次调用里最大的单块。
- **压缩 payload**：短别名 `r1..rN` 代替 UUID（解析后映射回真实 ID，再走原有的
  `source_ids ⊆ records[].id` 校验；未知别名走既有重试路径）、删恒定的 `level`、
  L0 的 `start`/`end` 合并为 `t`（实测 70/70 条相同）。
- **审计 payload**：只发 `id sid subject category content reason relations importance sources`；
  evidence 去掉 `id`（`validate_audit` 不使用）。
- **JSON Schema**：剥掉 Pydantic 生成的 `title`（占 schema 17–19% 的纯噪音）。

## 四、审计新增 `retract`

- `AuditAction.action` 增加 `retract`：被证据推翻或纯属冗余的事实**软删**（`deleted=1`），
  立即退出注入与检索，原文与版本都保留，网页端可一键恢复。
- 指令里写明用途，但不加「不许删重要东西」之类的限制——由模型按证据判断。

## 五、顺带修复的真 bug

后台 worker 在 `finish(completed)` 的 await 中被取消时，`except asyncio.CancelledError`
会无条件把任务状态改回 `queued`（`paused during reload`），导致**已完成的任务被重复执行**。
现在取消路径使用 `only_running=True`，只回滚仍在运行的任务。
这是合并车道测试偶发失败的根因（基线分支 8 次跑 3 次失败，修复后 8/8 通过）。

## 六、复现与验证

- 注入：`/tmp/measure_inject.py`（真实宿主 + 成熟库：6 个 L1 存档 / 40 条事实）
- 压缩：`/tmp/measure_calls.py`（真实装配路径，70 条 L0）
- 审计：`/tmp/measure_audit.py`（20 条事实 + 20 条证据）
- 测试：`tests/test_token_diet.py`（10 项）+ `tests/test_kira_integration.py`（4 项新增）
  → 默认套件 146 passed，`KIRA_CORE` 套件 174 passed。
