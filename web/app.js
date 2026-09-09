"use strict";
const $ = (s) => document.querySelector(s),
  $$ = (s) => [...document.querySelectorAll(s)];
const labels = {
  event: "日常经历",
  fact: "关键事实",
  preference: "偏好习惯",
  commitment: "约定任务",
  relationship: "人物关系",
  profile: "人物画像",
  resource: "资源线索",
  self: "自我认知",
};
const fields = {
  compress_input_chars: "每批压缩输入预算（字符）",
  auto_migrate: "自动安全迁移旧记忆",
  mutual_exclusion: "迁移成功后互斥旧插件",
  migration_max_chars: "KiraOS 迁移字符上限",
  enabled: "启用记忆系统",
  bootstrap_seed: "旧历史播种",
  inject_recent_raw: "注入最近原文",
  capture_enabled: "记录对话与感知",
  auto_inject: "持续上下文与感知注入",
  threshold: "首层压缩阈值",
  batch_size: "首层每批条数",
  probability: "自动压缩概率",
  max_level: "最大压缩层级",
  compress_model: "压缩与合并模型",
  audit_model: "审计模型",
  embedding_model: "可选向量模型（启用后可能计费）",
  semantic_enabled: "启用向量检索（默认关闭）",
  audit_enabled: "后台审计",
  audit_interval: "审计间隔（秒）",
  audit_batch: "每批审计事实数",
  audit_recheck_days: "审计冷却（天）",
  audit_daily_calls: "审计每日调用上限",
  model_timeout: "模型超时（秒）",
  model_retries: "模型失败重试次数",
  worker_count: "后台并发数",
  context_chars: "上下文字符预算",
  token_warning: "估算 Token 警告线",
  recall_keywords: "回忆提示词（每行一个）",
  recall_scope: "Bot可访问范围",
  top_k: "自动召回条数",
  proactive_enabled: "定时主动感知",
  proactive_interval: "主动感知间隔（秒）",
  proactive_sessions: "主动感知会话（每行一个）",
  compress_instruction: "压缩补充要求",
  fact_merge_enabled: "写入时自动合并事实",
  fact_merge_threshold: "事实合并触发阈值",
  fact_merge_soft_chars: "事实合并字数（提示词）",
  fact_merge_max_chars: "事实合并字数（硬上限）",
  fact_merge_soft_reason_chars: "事实合并理由（提示词）",
  fact_merge_reason_chars: "事实合并理由（硬上限）",
  fact_merge_batch_clusters: "事实合并每批簇数",
  fact_merge_prompt: "事实合并提示词",
  cross_session_merge: "跨会话合并（身份类）",
  merge_pending_hide: "合并前不参与检索",
  boot_enabled: "打开面板时播放载入动画",
  boot_replay_seconds: "载入动画重播冷却（秒）",
  session_affinity: "全局召回优先当前会话",
  permanent_dedupe: "自动合并相似永久记忆",
  dedupe_force_merge: "检测到相似就强制合并",
  dedupe_threshold: "永久记忆相似度阈值",
  record_merge_soft_chars: "永久记忆合并字数（提示词）",
  record_merge_max_chars: "永久记忆合并字数（硬上限）",
  record_merge_soft_reason_chars: "永久记忆合并理由（提示词）",
  record_merge_reason_chars: "永久记忆合并理由（硬上限）",
  record_merge_prompt: "永久记忆合并提示词",
  profile_summary_count: "画像摘要条数",
  search_active_only: "检索默认只搜常驻",
  cold_after_days: "归档转入冷归档天数",
};
let ctx = null,
  tab = "home",
  status = null,
  asset = "",
  config = null,
  configDirty = false,
  offset = 0,
  total = 0,
  factOffset = 0,
  current = null,
  autoRefresh = localStorage.getItem("alife-auto-refresh") !== "off",
  refreshTimer = null,
  models = [];
const esc = (s) =>
  String(s ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const date = (t) =>
  new Date(t * 1000).toLocaleString("zh-CN", { hour12: false });
function toast(t) {
  $("#toast").textContent = t;
  $("#toast").classList.remove("hide");
  setTimeout(() => $("#toast").classList.add("hide"), 3500);
}
function fail(e) {
  const dialog = $("dialog[open]");
  if (dialog) {
    let box = dialog.querySelector(".dialog-error");
    if (!box) {
      box = document.createElement("p");
      box.className = "dialog-error";
      box.setAttribute("role", "alert");
      dialog.prepend(box);
    }
    box.textContent = e.message;
  }
  toast(e.message);
  $("#error").textContent = e.message;
  $("#error").classList.remove("hide");
}
async function api(path, body) {
  const token = window.PluginPageContext?.getToken?.();
  const r = await fetch(
    "/api/plugin/" +
      encodeURIComponent(ctx?.pluginId || "alife_memory_z") +
      path,
    {
      method: body === undefined ? "GET" : "POST",
      credentials: "same-origin",
      cache: "no-store",
      headers: {
        "Content-Type": "application/json",
        ...(token ? { Authorization: "Bearer " + token } : {}),
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    },
  );
  if (!r.ok) {
    if (r.status === 409)
      throw Error(
        "内容已被其他操作修改。当前草稿已保留，请重新打开最新版本后再保存。",
      );
    const detail = await r.json().catch(() => ({}));
    throw Error(
      r.status === 422
        ? "输入不符合要求：请检查必填项、范围，以及关系是否表达具体含义。" +
            (typeof detail.detail === "string" &&
            /[\u3400-\u9fff]/.test(detail.detail)
              ? detail.detail
              : "")
        : "操作失败（" + r.status + "），请检查连接或登录状态。",
    );
  }
  return r.json();
}
function empty(t) {
  return (
    '<div class="empty">◇<strong>' +
    esc(t) +
    "</strong>开始对话，或添加一段珍贵的永久记忆。</div>"
  );
}
function saveDraft() {
  try {
    sessionStorage.setItem(
      "alife-draft",
      JSON.stringify({
        tab,
        configDirty,
        config: configDirty ? readConfig() : null,
        configRevision: config?.revision,
        nameDraft: $("#nameEditor").open
          ? {
              row: selectedName,
              name: $("#nameValue").value,
              reason: $("#nameReason").value,
            }
          : null,
        editor: $("#editor").open
          ? {
              current,
              text: $("#editText").value,
              factPatch: factPatch(),
              newSid: $("#newSid")?.value,
            }
          : null,
      }),
    );
  } catch {}
}
function factPatch() {
  const out = {};
  $$("#factFields [data-field]").forEach((e) => {
    out[e.dataset.field] = e.value;
  });
  return out;
}
function renderMigration(m) {
  if (!m) return;
  const names = {
    kira_plugin_simple_memory: "默认记忆",
    kira_plugin_kiraos: "KiraOS",
  };
  const reasons = {
    too_long: "超过字符上限",
    empty: "空内容",
    placeholder: "占位内容",
    invalid_text: "文本类型错误",
    invalid_fields: "字段不规范",
    markup_or_serialized_output: "代码/标记/序列化输出",
    control_characters: "控制字符",
    repetition: "重复乱码",
  };
  $("#migrationReport").innerHTML =
    "<p>" +
    esc(
      m.conflicts?.length
        ? "检测到已启用的旧插件；互斥开启时 Alife 暂停，避免争抢记忆。"
        : m.note || "自动迁移未运行",
    ) +
    "</p><p>累计接续 <strong>" +
    m.total_imported +
    "</strong> 条唯一记忆</p>" +
    m.reports
      .map(
        (r) =>
          '<div class="task"><strong>' +
          esc(names[r.source] || r.source) +
          "</strong><span>本次新增 " +
          r.imported +
          " · 已处理 " +
          r.duplicate +
          " · 跳过 " +
          r.skipped +
          " · 待确认归属 " +
          r.unscoped +
          " · 读取错误 " +
          r.errors.length +
          "</span></div>" +
          r.errors
            .map(
              (e) =>
                '<p class="danger">' +
                esc(e.file) +
                " · " +
                esc(e.reason) +
                "</p>",
            )
            .join(""),
      )
      .join("") +
    m.skips
      .map(
        (r) =>
          "<small>" +
          esc(names[r.source] || r.source) +
          "：" +
          esc(reasons[r.reason] || r.reason) +
          " " +
          r.count +
          " 条</small><br>",
      )
      .join("");
}
function tasksHtml(jobs) {
  return jobs.length
    ? jobs
        .map(
          (j) =>
            '<div class="task"><div><strong>' +
            esc(
              {
                compress: "分层压缩",
                audit: "事实审计",
                reindex: "语义索引",
                classify: "记忆归类",
                dedupe: "永久记忆合并",
                proactive: "主动感知",
              }[j.kind] || j.kind,
            ) +
            '</strong><div class="muted">' +
            esc(j.sid) +
            "</div><small>" +
            esc(
              j.detail === "TimeoutError"
                ? "旧任务超时：可减小批次、提高超时或更换模型后重新排队。"
                : j.detail || date(j.created),
            ) +
            '</small></div><span class="state-' +
            esc(j.state) +
            '">' +
            esc(
              {
                queued: "排队中",
                running: "处理中",
                completed: "已完成",
                failed: "失败",
              }[j.state] || j.state,
            ) +
            "</span></div>",
        )
        .join("")
    : empty("暂时没有后台任务");
}
async function poll() {
  try {
    const next = await api("/status");
    $("#connection").textContent = next.enabled
      ? autoRefresh
        ? "已连接 · 实时同步"
        : "已连接 · 手动同步"
      : "已连接 · 已暂停";
    if (asset && asset !== next.assets) {
      saveDraft();
      location.reload();
      return;
    }
    asset = next.assets;
    const changed = !status || next.revision !== status.revision;
    status = next;
    if (next.boot) {
      bootConfig = {
        enabled: next.boot.enabled !== false,
        replay_seconds: Number(next.boot.replay_seconds) || 0,
      };
      localStorage.setItem("alife-boot", JSON.stringify(bootConfig));
      if (!bootConfig.enabled && bootPlaying) endBoot();
    }
    renderMigration(next.migration);
    renderBootstrapNotice(next.bootstrap_review);
    $$('[data-job="reindex"]').forEach((e) => {
      e.disabled = !next.semantic_enabled;
      e.title = next.semantic_enabled
        ? "将调用所选向量模型，可能计费"
        : "向量检索默认关闭；本地检索仍可使用";
    });
    $("#stats").innerHTML = [
      ["所有存档", next.records, "含完整原始记录"],
      ["归类事实", next.facts, "每条均有来源"],
      ["已知实体", next.users, "画像与关系的起点"],
      [
        "后台运行",
        next.jobs.filter((j) => j.state === "running").length,
        "压缩 / 审计 / 索引",
      ],
    ]
      .map(
        (x) =>
          '<div class="stat"><span class="muted">' +
          x[0] +
          "</span><b>" +
          x[1] +
          "</b><small>" +
          x[2] +
          "</small></div>",
      )
      .join("");
    const max = Math.max(1, ...next.levels.map((l) => l.count));
    $("#levels").innerHTML = next.levels.length
      ? next.levels
          .map(
            (l) =>
              '<div class="level"><strong>' +
              (l.level === 100 ? "永久" : "L" + l.level) +
              '</strong><div class="bar"><i style="width:' +
              Math.max(3, (100 * l.count) / max) +
              '%"></i></div><small>' +
              l.active +
              " 常驻 / " +
              l.count +
              "</small></div>",
          )
          .join("")
      : empty("记忆空间已准备好");
    $("#recent").innerHTML = tasksHtml(next.jobs.slice(0, 3));
    $("#jobs").innerHTML = tasksHtml(next.jobs);
    for (const id of ["session", "jobSession"]) {
      const e = $("#" + id),
        value = e.value;
      e.innerHTML =
        (id === "session" ? '<option value="">全部会话</option>' : "") +
        next.sessions
          .map(
            (s) =>
              '<option value="' +
              esc(s) +
              '">' +
              esc(
                next.session_names?.[s] ? next.session_names[s] + " · " + s : s,
              ) +
              "</option>",
          )
          .join("");
      if ([...e.options].some((o) => o.value === value)) e.value = value;
    }
    if (changed && !$("#editor").open) {
      if (tab === "archives") await loadArchives();
      if (tab === "profiles") await loadFacts();
      if (tab === "names" && !$("#nameEditor").open) await loadNames();
    }
    if (config && next.config_revision !== config.revision && !configDirty)
      await loadConfig();
  } catch (e) {
    $("#connection").textContent = "连接中断 · 自动重试";
    fail(e);
  }
}
async function selectTab(name) {
  tab = name;
  if (name !== "profiles") {
    resetGraphZoom(document.querySelector("#relations"));
    clearGraphFocus();
  }
  $$(".view").forEach((e) => e.classList.toggle("hide", e.id !== name));
  $$("[data-tab]").forEach((e) =>
    e.setAttribute("aria-current", e.dataset.tab === name ? "page" : "false"),
  );
  const titles = {
    home: ["记忆，在这里生长", "保留经历的细节，也记住彼此的联结。"],
    archives: ["每段经历，都有来处", "检索、编辑，或沿着存档逐层回到最初。"],
    profiles: ["记住你，也认识自己", "关系、偏好、约定，汇成有依据的画像。"],
    tasks: ["让记忆，慢慢沉淀", "在后台压缩、审计与合并，不打断当下的对话。"],
    names: ["名字会改变，彼此仍相识", "以稳定ID连接现在与曾经的称呼。"],
    settings: ["按你的节奏，整理记忆", "每一项配置，保存即生效。"],
  };
  $("#title").textContent = titles[name][0];
  $("#subtitle").textContent = titles[name][1];
  if (name === "names") {
    await loadNames();
    await maybeAskNameBatch();
  }
  if (name === "archives") await loadArchives();
  if (name === "profiles") await loadFacts();
  if (name === "settings" && !configDirty) await loadConfig();
  saveDraft();
}
async function loadArchives() {
  const q = {
    sid: $("#session").value,
    keyword: $("#keyword").value,
    prompt: $("#semantic").value,
    offset,
    limit: 20,
  };
  if ($("#level").value !== "") q.level = Number($("#level").value);
  if ($("#after").value) q.start = new Date($("#after").value).getTime() / 1000;
  if ($("#before").value) q.end = new Date($("#before").value).getTime() / 1000;
  const data = await api("/search", q);
  total = data.total;
  Object.assign(displayNames, data.names || {});
  $("#archiveCards").innerHTML = data.items.length
    ? data.items
        .map(
          (r) =>
            '<article class="card"><div class="row"><span class="tag">' +
            (r.permanent ? "永久记忆" : "L" + r.level) +
            "</span><small>" +
            date(r.end) +
            "</small></div><p>" +
            esc(r.summary) +
            '</p><div class="meta">' +
            esc(displayLabel(r.sid)) +
            "<br>" +
            esc(r.users.map(displayLabel).join(" · ")) +
            "</div><footer><small>" +
            (r.cold
              ? "冷归档 · 仅按ID可读"
              : r.active
                ? "常驻上下文"
                : "历史存档") +
            '</small><button data-open="' +
            esc(r.id) +
            '">查看与编辑 ↗</button></footer></article>',
        )
        .join("")
    : empty("没有找到这段记忆");
  $$("[data-open]").forEach(
    (e) => (e.onclick = () => guard(() => openRecord(e.dataset.open))),
  );
  $("#pageInfo").textContent =
    (total ? offset + 1 : 0) +
    "–" +
    Math.min(offset + 20, total) +
    " / " +
    total;
  $("#prev").disabled = offset === 0;
  $("#next").disabled = offset + 20 >= total;
}
async function loadFacts() {
  const q = new URLSearchParams({
    subject: $("#subject").value,
    category: $("#category").value,
    offset: factOffset,
  });
  shownFacts = await api("/facts?" + q);
  for (const f of shownFacts) Object.assign(displayNames, f.names || {});
  renderFactCards(shownFacts);
  renderGraph();
  $("#factPage").textContent = "第 " + (factOffset / 100 + 1) + " 页";
  $("#factPrev").disabled = factOffset === 0;
  $("#factNext").disabled = shownFacts.length < 100;
}
function renderFactCards(list) {
  $("#factCards").innerHTML = list.length
    ? list
        .map(
          (f, i) =>
            `<article class="card"><div class="row"><span class="tag">${esc(labels[f.category])}</span><strong>${esc(displayLabel(f.subject))}</strong></div><p>${esc(f.content)}</p><small>${esc(f.tags.join(" · "))}</small>${f.relation_warnings?.length ? '<div class="notice">待审校：' + esc(f.relation_warnings.map((w) => w.reason).join("；")) + "。该连线未用于关系召回。</div>" : ""}<p class="muted">${esc(f.reason ? "事实依据：" + f.reason : "")}${esc(f.scenario ? " · 场景：" + f.scenario : "")}</p>${f.edit_history?.length ? '<p class="muted">最近审校：' + esc(f.edit_history[0].reason) + " · " + date(f.edit_history[0].created) + "</p>" : ""}<footer><small>${f.sources.length} 个来源</small><button data-fact="${i}">编辑事实</button></footer></article>`,
        )
        .join("")
    : empty("画像还在形成");
  $$("[data-fact]").forEach(
    (e) =>
      (e.onclick = () => guard(() => openFact(list[Number(e.dataset.fact)]))),
  );
}
function showEditor() {
  $$(".dialog-error").forEach((e) => e.remove());
  $("#deleteConfirm").classList.add("hide");
  if (!$("#editor").open) $("#editor").showModal();
  $("#editText").focus();
}
async function openRecord(id) {
  current = {
    kind: "record",
    row: await api("/memory/" + encodeURIComponent(id)),
  };
  renderRecord();
  showEditor();
}
function renderRecord() {
  const r = current.row;
  $("#editorTitle").textContent =
    "记忆详情 · " + (r.permanent ? "永久" : "L" + r.level);
  $("#editorMeta").textContent =
    displayLabel(r.sid) +
    " · " +
    r.users.map(displayLabel).join(" / ") +
    " · " +
    date(r.start) +
    " — " +
    date(r.end);
  $("#editLabel").textContent = "可编辑摘要（原始内容始终保留）";
  $("#editText").value = r.summary;
  $("#factFields").classList.add("hide");
  $("#sources").classList.remove("hide");
  $("#sourceText").textContent =
    r.content +
    "\n\n旧插件来源\n" +
    JSON.stringify(r.legacy_sources || [], null, 2);
  renderVersions("record", r.id, r.revision, r.versions || []);
  $("#sourceLinks").innerHTML = r.children
    .map(
      (id) =>
        '<button data-child="' +
        esc(id) +
        '">读取子存档 ' +
        esc(id.slice(0, 14)) +
        "…</button>",
    )
    .join("");
  $$("[data-child]").forEach(
    (e) => (e.onclick = () => guard(() => openRecord(e.dataset.child))),
  );
  $("#forget").classList.toggle("hide", !r.permanent);
  $("#forget").textContent = r.active ? "移出常驻上下文" : "恢复到常驻上下文";
  $("#delete").classList.remove("hide");
}
async function openFact(row) {
  const detail = row.versions
    ? row
    : await api("/fact/" + encodeURIComponent(row.id));
  row = detail;
  current = { kind: "fact", row };
  $("#editorTitle").textContent = "编辑画像事实";
  $("#editorMeta").textContent = row.subject + " · " + row.sid;
  $("#editLabel").textContent = "事实内容";
  $("#editText").value = row.content;
  $("#factFields").classList.remove("hide");
  $("#factFields").innerHTML = [
    "subject",
    "category",
    "reason",
    "scenario",
    "tags",
    "relations",
  ]
    .map(
      (k) =>
        '<label class="field">' +
        esc(
          {
            subject: "实体",
            category: "分类",
            reason: "原因",
            scenario: "适用场景",
            tags: "标签（每行一个）",
            relations: "关系（三元组 JSON）",
          }[k],
        ) +
        (k === "category"
          ? '<select data-field="category">' +
            Object.entries(labels)
              .map(
                ([v, n]) =>
                  '<option value="' +
                  v +
                  '" ' +
                  (row.category === v ? "selected" : "") +
                  ">" +
                  n +
                  "</option>",
              )
              .join("") +
            "</select>"
          : '<textarea data-field="' +
            k +
            '">' +
            esc(
              k === "relations"
                ? JSON.stringify(row[k], null, 2)
                : Array.isArray(row[k])
                  ? row[k].join("\n")
                  : row[k],
            ) +
            "</textarea>") +
        "</label>",
    )
    .join("");
  $("#factFields").insertAdjacentHTML(
    "beforeend",
    '<small class="wide">关系示例：[{"subject":"qq:123","predicate":"朋友","object":"qq:456"}]。只填有证据的完整关系；无法确认时填 []。不要用“认为”代替关系。</small>',
  );
  $("#sources").classList.remove("hide");
  $("#sourceText").textContent =
    "支持来源：" +
    row.sources.join("\n") +
    "\n\n最近修改 / 模型审校：\n" +
    (row.edit_history || [])
      .map((h) => date(h.created) + " · " + h.reason)
      .join("\n");
  $("#sourceLinks").innerHTML = row.sources
    .map(
      (id) =>
        '<button data-child="' +
        esc(id) +
        '">读取证据 ' +
        esc(id.slice(0, 12)) +
        "</button>",
    )
    .join("");
  $$("[data-child]").forEach(
    (e) => (e.onclick = () => guard(() => openRecord(e.dataset.child))),
  );
  renderVersions("fact", row.id, row.revision, row.versions || []);
  $("#forget").classList.add("hide");
  $("#delete").classList.remove("hide");
  showEditor();
}
function newMemory() {
  current = { kind: "new" };
  $("#editorTitle").textContent = "新增永久记忆";
  $("#editorMeta").innerHTML =
    '<label>所属会话 <input id="newSid" placeholder="adapter:dm:user 或 adapter:gm:group" value="' +
    esc($("#session").value || status?.sessions?.[0] || "") +
    '"></label>';
  $("#editLabel").textContent = "值得长久记住的事";
  $("#editText").value = "";
  $("#factFields").classList.add("hide");
  $("#sources").classList.add("hide");
  $("#forget").classList.add("hide");
  $("#delete").classList.add("hide");
  showEditor();
}
async function saveEdit(extra) {
  if (current.kind === "new") {
    await api("/memory", {
      sid: $("#newSid").value,
      content: $("#editText").value,
    });
  } else {
    let patch =
      extra ||
      (current.kind === "record"
        ? { summary: $("#editText").value }
        : { content: $("#editText").value, ...factPatch() });
    if (current.kind === "fact" && !extra) {
      patch.tags = patch.tags.split("\n").filter(Boolean);
      try {
        patch.relations = JSON.parse(patch.relations);
      } catch {
        throw Error("关系必须是 JSON 三元组数组");
      }
    }
    await api("/edit", {
      kind: current.kind,
      target: current.row.id,
      revision: current.row.revision,
      patch,
      reason: "WebUI 人工编辑",
    });
  }
  $("#editor").close();
  current = null;
  saveDraft();
  toast("已保存，即时生效");
  await poll();
  if (tab === "archives") await loadArchives();
  if (tab === "profiles") await loadFacts();
}
function readConfig() {
  const value = { ...config.settings };
  $$("#configForm [data-key]").forEach((e) => {
    const key = e.dataset.key,
      kind = e.dataset.kind;
    value[key] =
      kind === "boolean"
        ? e.checked
        : kind === "array"
          ? e.value.split("\n").filter(Boolean)
          : ["integer", "number"].includes(kind)
            ? Number(e.value)
            : e.value;
  });
  return value;
}
async function loadConfig() {
  config = await api("/config");
  models = (await api("/models")).models;
  renderConfig(config.settings);
  configDirty = false;
  $("#dirty").textContent = "配置已同步";
}
function vectorControls() {
  const toggle = document.querySelector("[data-key=semantic_enabled]"),
    select = document.querySelector("[data-key=embedding_model]");
  if (toggle && select) select.disabled = !toggle.checked;
}
function renderConfig(values) {
  $("#configForm").innerHTML = Object.entries(config.schema.properties)
    .map(([key, p]) => {
      const type = p.type,
        value = values[key];
      let input = "";
      if (type === "boolean")
        input = '<input type="checkbox" ' + (value ? "checked" : "") + ">";
      else if (key.endsWith("_model"))
        input =
          '<select><option value="">使用 KiraAI 默认' +
          (key === "compress_model"
            ? "快速"
            : key === "audit_model"
              ? "主"
              : "向量") +
          "模型</option>" +
          models
            .filter(
              (m) =>
                m.kind === (key === "embedding_model" ? "embedding" : "llm"),
            )
            .map(
              (m) =>
                '<option value="' +
                esc(m.id) +
                '" ' +
                (value === m.id ? "selected" : "") +
                ">" +
                esc(m.id) +
                "</option>",
            )
            .join("") +
          (value && !models.some((m) => m.id === value)
            ? '<option selected value="' +
              esc(value) +
              '">' +
              esc(value) +
              "（当前不可用）</option>"
            : "") +
          "</select>";
      else if (p.enum)
        input =
          "<select>" +
          p.enum
            .map(
              (v) =>
                '<option value="' +
                esc(v) +
                '" ' +
                (value === v ? "selected" : "") +
                ">" +
                esc(
                  {
                    global: "全局 · 跨用户跨会话",
                    linked: "同参与者 · 关联会话",
                    session: "当前会话",
                  }[v] || v,
                ) +
                "</option>",
            )
            .join("") +
          "</select>";
      else if (
        type === "array" ||
        key === "compress_instruction" ||
        key.endsWith("_prompt")
      )
        input =
          "<textarea>" +
          esc(Array.isArray(value) ? value.join("\n") : value) +
          "</textarea>";
      else
        input =
          '<input type="' +
          (["number", "integer"].includes(type) ? "number" : "text") +
          '" value="' +
          esc(value) +
          '" ' +
          (p.minimum !== undefined ? 'min="' + p.minimum + '"' : "") +
          " " +
          (p.maximum !== undefined ? 'max="' + p.maximum + '"' : "") +
          ' step="' +
          (type === "number" ? "any" : "1") +
          '">';
      input = input.replace(
        /<(input|select|textarea)/,
        '<$1 data-key="' + key + '" data-kind="' + type + '"',
      );
      return (
        '<label class="field ' +
        (key === "compress_instruction" || key.endsWith("_prompt")
          ? "wide"
          : "") +
        '"><span>' +
        esc(fields[key] || key) +
        "</span>" +
        input +
        "<small>" +
        esc(config.help?.[key] || "") +
        "</small></label>"
      );
    })
    .join("");
  $$("#configForm [data-key]").forEach(
    (e) =>
      (e.oninput = () => {
        vectorControls();
        configDirty = true;
        $("#dirty").textContent = "有未保存的修改";
        saveDraft();
      }),
  );
  vectorControls();
}
async function guard(fn) {
  try {
    $("#error").classList.add("hide");
    await fn();
  } catch (e) {
    fail(e);
  }
}
$("#retryMigration").onclick = () =>
  guard(async () => {
    const b = $("#retryMigration");
    b.disabled = true;
    try {
      await api("/migrate", {});
      await poll();
      toast("迁移检查完成，请查看报告");
    } finally {
      b.disabled = false;
    }
  });
$$("[data-tab]").forEach(
  (e) => (e.onclick = () => guard(() => selectTab(e.dataset.tab))),
);
$("#refresh").onclick = () =>
  guard(async () => {
    await refreshNow();
    autoRefresh = !autoRefresh;
    localStorage.setItem("alife-auto-refresh", autoRefresh ? "on" : "off");
    applyRefreshMode();
  });
async function refreshNow() {
  await poll();
  if (tab === "archives") await loadArchives();
  if (tab === "profiles") await loadFacts();
  if (tab === "names") await loadNames();
}
function applyRefreshMode() {
  const button = $("#refresh");
  button.classList.toggle("auto", autoRefresh);
  button.setAttribute("aria-pressed", String(autoRefresh));
  $("#refreshLabel").textContent = autoRefresh ? "自动刷新" : "手动刷新";
  button.title = autoRefresh
    ? "自动刷新中 · 点击切换为手动"
    : "手动刷新 · 点击刷新并恢复自动";
  if (refreshTimer) clearInterval(refreshTimer);
  refreshTimer = autoRefresh
    ? setInterval(() => {
        if (!document.hidden) poll();
      }, 2500)
    : null;
}
$("#theme").onclick = () =>
  (document.documentElement.dataset.theme =
    document.documentElement.dataset.theme === "dark" ? "light" : "dark");
let forceMotion = localStorage.getItem("alife-motion") === "force";
function applyMotion() {
  if (forceMotion) document.documentElement.dataset.motion = "force";
  else delete document.documentElement.dataset.motion;
  const button = $("#motion");
  button.setAttribute("aria-pressed", String(forceMotion));
  button.classList.toggle("on", forceMotion);
  button.title = forceMotion
    ? "动效已强制开启 · 点击跟随系统"
    : "动效跟随系统 · 点击强制开启";
}
$("#motion").onclick = () => {
  forceMotion = !forceMotion;
  localStorage.setItem("alife-motion", forceMotion ? "force" : "auto");
  applyMotion();
};
applyMotion();
const BOOT_QUOTES = [
  "和谁的记忆，我都不想忘记",
  "每段记忆，都有来处",
  "和你的记忆，我不想再忘记",
  "想念，念想",
  "记得，是最长情的陪伴",
  "把时间，收进可以回去的地方",
  "走过的路，都在这里",
  "你忘掉的，我替你记着",
  "往事有光，来日有信",
  "每一次相遇，都值得被留存",
  "我们把日子，过成了故事",
  "记忆不是负担，是归处",
];
let bootConfig = { enabled: true, replay_seconds: 90 };
try {
  Object.assign(bootConfig, JSON.parse(localStorage.getItem("alife-boot") || "{}"));
} catch {}
let bootPlaying = true,
  bootTimer = null,
  bootHiddenAt = 0;
function bootQuote() {
  const quote = $("#bootQuote");
  if (!quote) return;
  const text = BOOT_QUOTES[Math.floor(Math.random() * BOOT_QUOTES.length)];
  quote.replaceChildren(
    ...[...text].map((ch, index) => {
      const span = document.createElement("span");
      span.textContent = ch;
      span.style.setProperty("--i", index);
      return span;
    }),
  );
}
function endBoot() {
  const boot = $("#boot");
  if (!boot || !bootPlaying) return;
  boot.classList.add("skip");
  clearTimeout(bootTimer);
  bootTimer = setTimeout(() => {
    boot.hidden = true;
    boot.classList.remove("skip");
    bootPlaying = false;
  }, 320);
}
function startBootTimer() {
  clearTimeout(bootTimer);
  bootTimer = setTimeout(endBoot, 3900);
}
function playBoot(replay) {
  const boot = $("#boot");
  if (!boot) return;
  if (!bootConfig.enabled) {
    boot.hidden = true;
    return;
  }
  if (replay) {
    if (bootPlaying) return;
    // Replacing the node restarts every CSS animation, including pseudo-elements.
    const fresh = boot.cloneNode(true);
    boot.replaceWith(fresh);
  }
  const current = $("#boot");
  current.hidden = false;
  current.classList.remove("skip");
  bootPlaying = true;
  bootQuote();
  startBootTimer();
}
// The static markup is already animating; just fill the quote and arm the timer.
playBoot(false);
document.addEventListener("click", (event) => {
  if (bootPlaying && event.target.closest("#boot")) endBoot();
});
document.addEventListener("keydown", (event) => {
  if (bootPlaying && ["Escape", "Enter", " "].includes(event.key)) endBoot();
});
function bootReplay() {
  if (!bootHiddenAt) return;
  const gap = (Date.now() - bootHiddenAt) / 1000;
  bootHiddenAt = 0;
  const cooldown = Number(bootConfig.replay_seconds);
  if (cooldown > 0 && gap >= cooldown) playBoot(true);
}
new IntersectionObserver(
  (entries) => {
    for (const entry of entries) {
      if (entry.isIntersecting) bootReplay();
      else if (!bootHiddenAt) bootHiddenAt = Date.now();
    }
  },
  { threshold: 0 },
).observe(document.documentElement);
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") bootReplay();
  else if (!bootHiddenAt) bootHiddenAt = Date.now();
});
$("#new").onclick = newMemory;
$("#closeEditor").onclick = () => {
  $("#editor").close();
  saveDraft();
};
$("#editText").oninput = saveDraft;
$("#search").onclick = () =>
  guard(() => {
    offset = 0;
    return loadArchives();
  });
$("#keyword").onkeydown = (e) => {
  if (e.key === "Enter") $("#search").click();
};
for (const id of ["#session", "#level"])
  $(id).onchange = () =>
    guard(() => {
      offset = 0;
      return loadArchives();
    });
$("#category").onchange = () =>
  guard(() => {
    factOffset = 0;
    clearGraphFocus();
    return loadFacts();
  });
$("#subject").onkeydown = (e) => {
  if (e.key === "Enter") $("#filterFacts").click();
};
$("#nameQuery").onkeydown = (e) => {
  if (e.key === "Enter") $("#searchNames").click();
};
$("#prev").onclick = () =>
  guard(() => {
    offset = Math.max(0, offset - 20);
    return loadArchives();
  });
$("#next").onclick = () =>
  guard(() => {
    offset += 20;
    return loadArchives();
  });
$("#filterFacts").onclick = () =>
  guard(() => {
    factOffset = 0;
    clearGraphFocus();
    return loadFacts();
  });
$("#factPrev").onclick = () =>
  guard(() => {
    factOffset = Math.max(0, factOffset - 100);
    return loadFacts();
  });
$("#factNext").onclick = () =>
  guard(() => {
    factOffset += 100;
    return loadFacts();
  });
$("#saveEdit").onclick = () =>
  guard(async () => {
    const b = $("#saveEdit");
    b.disabled = true;
    try {
      await saveEdit();
    } finally {
      b.disabled = false;
    }
  });
$("#forget").onclick = () =>
  guard(() => saveEdit({ active: !current.row.active }));
$("#delete").onclick = () => $("#deleteConfirm").classList.remove("hide");
$("#cancelDelete").onclick = () => $("#deleteConfirm").classList.add("hide");
$("#confirmDelete").onclick = () =>
  guard(async () => {
    const b = $("#confirmDelete");
    b.disabled = true;
    try {
      await saveEdit({ deleted: true });
    } finally {
      b.disabled = false;
    }
  });
$("#saveConfig").onclick = () =>
  guard(async () => {
    const result = await api("/config", {
      revision: config.revision,
      settings: readConfig(),
    });
    config.revision = result.revision;
    config.settings = readConfig();
    configDirty = false;
    $("#dirty").textContent = "已保存，配置立即生效";
    saveDraft();
    toast("配置已生效");
  });
$$("[data-job]").forEach(
  (e) =>
    (e.onclick = () =>
      guard(async () => {
        if (!$("#jobSession").value) throw Error("请先选择一个已有会话");
        await api("/jobs", {
          sid: $("#jobSession").value,
          kind: e.dataset.job,
        });
        toast("任务已进入后台队列");
        await poll();
      })),
);
$("#export").onclick = () =>
  guard(async () => {
    const data = await api("/export");
    $("#exportText").value = JSON.stringify(data, null, 2);
    $("#downloadExport").classList.toggle("hide", window.parent !== window);
    $("#exportHint").textContent = "";
    $("#exportDialog").showModal();
  });
$("#closeExport").onclick = () => $("#exportDialog").close();
$("#copyExport").onclick = async () => {
  const field = $("#exportText");
  field.focus();
  field.select();
  try {
    await navigator.clipboard.writeText(field.value);
    $("#exportHint").textContent = "已复制完整内容";
  } catch {
    $("#exportHint").textContent = document.execCommand("copy")
      ? "已复制完整内容"
      : "内容已全选，请按 Ctrl+C（Mac为 ⌘C）复制";
  }
};
$("#downloadExport").onclick = () => {
  const blob = new Blob([$("#exportText").value], { type: "application/json" }),
    a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "alife-memory-export.json";
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 5000);
};
for (let i = 1; i <= 32; i++)
  $("#level").add(new Option("L" + i + " · " + i + " 次压缩", i));
Object.entries(labels).forEach(([v, n]) =>
  $("#category").add(new Option(n, v)),
);
async function boot() {
  if (window.PluginPageContext && window.parent !== window)
    ctx = await window.PluginPageContext.ready();
  await poll();
  let draft;
  try {
    draft = JSON.parse(sessionStorage.getItem("alife-draft"));
  } catch {}
  if (draft?.configDirty) {
    await loadConfig();
    renderConfig({ ...config.settings, ...draft.config });
    config.revision = draft.configRevision;
    configDirty = true;
    $("#dirty").textContent = "已恢复未保存草稿；保存时将检查版本冲突";
  }
  if (draft?.tab) await selectTab(draft.tab);
  if (draft?.nameDraft) {
    openName(draft.nameDraft.row);
    $("#nameValue").value = draft.nameDraft.name;
    $("#nameReason").value = draft.nameDraft.reason;
  }
  if (draft?.editor?.current) {
    current = draft.editor.current;
    if (current.kind === "new") {
      newMemory();
      $("#newSid").value = draft.editor.newSid || "";
    } else if (current.kind === "record") {
      renderRecord();
      showEditor();
    } else openFact(current.row);
    $("#editText").value = draft.editor.text;
    Object.entries(draft.editor.factPatch || {}).forEach(([k, v]) => {
      const e = $('#factFields [data-field="' + k + '"]');
      if (e) e.value = v;
    });
  }
  applyRefreshMode();
  if (
    window.matchMedia("(prefers-reduced-motion: reduce)").matches &&
    !forceMotion &&
    !localStorage.getItem("alife-motion-hint")
  ) {
    localStorage.setItem("alife-motion-hint", "1");
    toast("系统开启了“减少动态效果”，动效已关闭；点 ✨ 可强制开启");
  }
}
let shownFacts = [],
  graphMode = "relations",
  nameOffset = 0,
  selectedName = null;
const displayNames = {};
const fixedLabels = {
  global: "全局记忆",
  self: "机器人自身",
  unscoped: "未分类 · 来源会话未确定",
};
const kindLabel = (kind) =>
  kind === "user"
    ? "人物"
    : kind === "self"
      ? "机器人自身"
      : kind === "global"
        ? "全局记忆"
        : "会话 / 群";
const displayLabel = (id) => {
  const name = displayNames[id] || fixedLabels[id];
  return name ? name + " · " + id : id;
};
function renderGraph() {
  drawMemoryGraph(
    $("#relations"),
    shownFacts,
    graphMode,
    displayNames,
    (f) => openFact(f),
    focusGraphEntity,
    graphFilter,
  );
}
let graphFilter = null;
function clearGraphFocus() {
  if (!graphFilter) {
    $("#graphFocus").classList.add("hide");
    return;
  }
  graphFilter = null;
  $("#graphFocus").classList.add("hide");
  renderFactCards(shownFacts);
}
function focusGraphEntity(id) {
  const panel = $("#graphFocus");
  if (!id) {
    graphFilter = null;
    panel.classList.add("hide");
    renderFactCards(shownFacts);
    renderGraph();
    return;
  }
  graphFilter = id;
  const related = shownFacts.filter(
    (fact) =>
      fact.subject === id ||
      (fact.verified_relations || []).some(
        (relation) => relation.subject === id || relation.object === id,
      ),
  );
  renderFactCards(related);
  renderGraphFocus(id, related);
  renderGraph();
}
function renderGraphFocus(id, facts) {
  const rows = [];
  for (const fact of facts)
    for (const relation of fact.verified_relations || []) {
      if (relation.subject !== id && relation.object !== id) continue;
      const other = relation.subject === id ? relation.object : relation.subject;
      rows.push({ label: relation.predicate + " → " + displayLabel(other), fact });
    }
  const panel = $("#graphFocus");
  panel.innerHTML =
    '<div class="row"><strong>' +
    esc(displayLabel(id)) +
    '</strong><small class="muted">' +
    rows.length +
    ' 条联结 · 已筛选下方事实卡片</small><button id="graphClear">清除筛选</button></div>' +
    (rows.length
      ? '<div class="graph-focus-list">' +
        rows
          .map(
            (row, index) =>
              '<div class="graph-focus-row"><span>' +
              esc(row.label) +
              '</span><button data-focus-fact="' +
              index +
              '">编辑依据</button></div>',
          )
          .join("") +
        "</div>"
      : '<p class="muted">该实体在当前筛选页没有可展示的联结。</p>');
  panel.classList.remove("hide");
  $("#graphClear").onclick = () => focusGraphEntity(null);
  $$("[data-focus-fact]").forEach(
    (button) =>
      (button.onclick = () =>
        guard(() => openFact(rows[Number(button.dataset.focusFact)].fact))),
  );
}
$("#graphRelations").onclick = () => {
  graphMode = "relations";
  $("#graphRelations").setAttribute("aria-pressed", "true");
  $("#graphDimensions").setAttribute("aria-pressed", "false");
  renderGraph();
};
$("#graphDimensions").onclick = () => {
  graphMode = "dimensions";
  $("#graphRelations").setAttribute("aria-pressed", "false");
  $("#graphDimensions").setAttribute("aria-pressed", "true");
  renderGraph();
};
async function loadNames() {
  const rows = await api(
    "/names?" +
      new URLSearchParams({ query: $("#nameQuery").value, offset: nameOffset }),
  );
  rows.forEach((n) => {
    if (n.name) displayNames[n.id] = n.name;
  });
  $("#nameCards").innerHTML = rows.length
    ? rows
        .map((n, i) => {
          const stats = n.stats || {};
          const summary = (stats.summary || []).join(" · ") || n.identity_note || "";
          const aliases =
            [...new Set(n.history.map((h) => h.name))]
              .filter((x) => x !== n.name)
              .join("、") || "暂无";
          return `<article class="card"><span class="tag">${esc(kindLabel(n.kind))}</span><h3>${esc(n.name || n.label || "名称待补全")}</h3><small>${esc(n.id)}</small><p class="muted">${esc(summary)}</p><p class="muted">事实 ${stats.facts ?? 0} · 关系 ${stats.relations ?? 0} · 曾用名：${esc(aliases)}</p><footer><small>${n.updated ? date(n.updated) : "等待新消息或手动更新"}</small><button data-profile="${i}">查看画像</button><button data-name="${i}">查看与审校</button></footer></article>`;
        })
        .join("")
    : empty("未找到名称");
  $$("[data-name]").forEach(
    (b) => (b.onclick = () => openName(rows[Number(b.dataset.name)])),
  );
  $$("[data-profile]").forEach(
    (b) =>
      (b.onclick = () =>
        guard(() => openProfile(rows[Number(b.dataset.profile)].id))),
  );
  $("#namePage").textContent = "第 " + (nameOffset / 100 + 1) + " 页";
  $("#namePrev").disabled = nameOffset === 0;
  $("#nameNext").disabled = rows.length < 100;
}
function openName(n) {
  $$(".dialog-error").forEach((e) => e.remove());
  selectedName = n;
  $("#nameIdentity").textContent = n.id + " · " + (n.identity_note || "");
  $("#lookupName").disabled = !n.lookup_id;
  $("#lookupName").title = n.lookup_id
    ? "按稳定账号查询平台名称"
    : "迁移归档区没有可确认的平台账号，请手动补充称呼";
  $("#nameValue").value = n.name;
  $("#nameReason").value = "";
  $("#nameHistory").innerHTML =
    n.history
      .map(
        (h) =>
          `<div class="task"><strong>${esc(h.name)}</strong><small>${date(h.observed)} · ${esc(h.source)}<br>${esc(h.context)} ${esc(h.reason)}</small>${h.name === n.name ? "" : `<button data-setname="${esc(h.name)}">设为当前名</button>`}</div>`,
      )
      .join("") || '<p class="muted">尚无名称记录</p>';
  $$("#nameHistory [data-setname]").forEach(
    (b) =>
      (b.onclick = () =>
        guard(async () => {
          await api("/names", {
            entity_id: n.id,
            name: b.dataset.setname,
            revision: n.revision,
            reason: "恢复到曾用名",
          });
          toast("已切换当前称呼");
          $("#nameEditor").close();
          await loadNames();
        })),
  );
  if (!$("#nameEditor").open) $("#nameEditor").showModal();
}
$("#searchNames").onclick = () =>
  guard(() => {
    nameOffset = 0;
    return loadNames();
  });
$("#repairNames").onclick = () =>
  guard(async () => {
    const button = $("#repairNames");
    button.disabled = true;
    try {
      const report = await api("/maintenance/names", {});
      $("#repairNamesHint").textContent = report.repaired.length
        ? "已修复 " +
          report.repaired.length +
          " 个昵称：" +
          report.repaired.map((item) => item.name).join("、")
        : "没有发现被第三方插件改写的昵称";
      await loadNames();
    } finally {
      button.disabled = false;
    }
  });
function renderBootstrapNotice(info) {
  const box = $("#bootstrapNotice");
  if (!box) return;
  const active = info && info.merge_plugin && info.count && !info.reviewed;
  box.classList.toggle("hide", !active);
  if (!active) return;
  $("#bootstrapNoticeText").textContent =
    "检测到 " +
    info.merge_plugin +
    " 会改写会话上下文：库中有 " +
    info.count +
    " 个会话的历史播种记录无法确认来源，可能混入了其他会话。" +
    "如果你是先装记忆插件、后装合并插件，这些记录通常是正常的，可以保留。";
}
$("#bootstrapGo").onclick = () => selectTab("tasks");
$("#bootstrapKeep").onclick = () =>
  guard(async () => {
    await api("/maintenance/bootstrap/review", {});
    await poll();
  });
$("#purgeBootstrap").onclick = () =>
  guard(async () => {
    const button = $("#purgeBootstrap");
    button.disabled = true;
    try {
      const report = await api("/maintenance/bootstrap", {});
      $("#purgeBootstrapHint").textContent = report.removed
        ? "已清理 " +
          report.removed +
          " 条（涉及 " +
          report.sessions +
          " 个会话）· 原文保留可恢复"
        : "没有发现历史播种记录";
      await poll();
    } finally {
      button.disabled = false;
    }
  });
$("#cleanupTools").onclick = () =>
  guard(async () => {
    const button = $("#cleanupTools");
    button.disabled = true;
    try {
      const report = await api("/maintenance/tools", {});
      $("#cleanupToolsHint").textContent =
        "已清理：移除 " +
        report.removed +
        " 条 · 重写 " +
        report.rewritten +
        " 条 · 约省 " +
        report.freed_chars +
        " 字符（原文保留）";
      await poll();
    } finally {
      button.disabled = false;
    }
  });
async function maybeAskNameBatch() {
  if (localStorage.getItem("alife-name-batch") === "off") return;
  if (sessionStorage.getItem("alife-name-batch-shown") === "1") return;
  let rows = [];
  try {
    rows = await api("/names?offset=0");
  } catch {
    return;
  }
  const pending = rows.filter(
    (row) =>
      !row.name &&
      row.kind !== "global" &&
      row.kind !== "self" &&
      row.id !== "unscoped" &&
      /\d/.test(String(row.id).split(":").pop() || ""),
  );
  if (!pending.length) return;
  sessionStorage.setItem("alife-name-batch-shown", "1");
  $("#nameBatchIntro").textContent =
    "记忆里有 " + pending.length + " 个号码还没有当前称呼，要从聊天平台一次性查询吗？";
  $("#nameBatchStatus").textContent = "";
  $("#nameBatchGo").disabled = false;
  $("#nameBatch").showModal();
}
$("#nameBatchLater").onclick = () => $("#nameBatch").close();
$("#nameBatchNever").onclick = () => {
  localStorage.setItem("alife-name-batch", "off");
  $("#nameBatch").close();
};
let nameBatchRunning = false;
$("#nameBatchGo").onclick = () =>
  guard(async () => {
    const go = $("#nameBatchGo"),
      stop = $("#nameBatchStop");
    go.disabled = true;
    $("#nameBatchLater").disabled = true;
    $("#nameBatchNever").disabled = true;
    stop.classList.remove("hide");
    nameBatchRunning = true;
    let done = 0,
      updated = 0,
      skipped = 0,
      failed = 0,
      remaining = 0;
    try {
      const pending = await api("/names/pending");
      const ids = pending.ids || [];
      if (!ids.length) {
        $("#nameBatchStatus").textContent = "没有需要补全的号码。";
        return;
      }
      for (let i = 0; i < ids.length && nameBatchRunning; i += 20) {
        $("#nameBatchStatus").textContent =
          "正在查询… 已处理 " + done + " / " + ids.length;
        const result = await api("/names/refresh-batch", {
          ids: ids.slice(i, i + 20),
          reason: "批量确认当前QQ昵称",
        });
        done += Math.min(20, ids.length - i);
        updated += result.updated.length;
        skipped += (result.skipped || []).length;
        failed += result.failed.length;
        remaining = result.remaining;
      }
      $("#nameBatchStatus").textContent =
        "完成：成功 " +
        updated +
        " · 跳过（已有名字）" +
        skipped +
        " · 失败 " +
        failed +
        " · 还有 " +
        remaining +
        " 个未填";
      await loadNames();
      if (tab === "profiles") await loadFacts();
      toast("已更新 " + updated + " 个昵称");
    } finally {
      nameBatchRunning = false;
      go.disabled = false;
      $("#nameBatchLater").disabled = false;
      $("#nameBatchNever").disabled = false;
      stop.classList.add("hide");
    }
  });
$("#nameBatchStop").onclick = () => {
  nameBatchRunning = false;
  $("#nameBatchStatus").textContent = "已停止；再次点击「一键拉取」可继续。";
};
$("#namePrev").onclick = () =>
  guard(() => {
    nameOffset = Math.max(0, nameOffset - 100);
    return loadNames();
  });
$("#nameNext").onclick = () =>
  guard(() => {
    nameOffset += 100;
    return loadNames();
  });
$("#closeName").onclick = () => {
  $("#nameEditor").close();
  saveDraft();
};
$("#nameValue").oninput = saveDraft;
$("#nameReason").oninput = saveDraft;
$("#saveName").onclick = () =>
  guard(async () => {
    await api("/names", {
      entity_id: selectedName.id,
      name: $("#nameValue").value,
      revision: selectedName.revision,
      reason: $("#nameReason").value,
    });
    $("#nameEditor").close();
    saveDraft();
    await loadNames();
    toast("称呼已更新，曾用名已保留");
  });
$("#lookupName").onclick = () =>
  guard(async () => {
    const b = $("#lookupName");
    b.disabled = true;
    try {
      openName(
        await api("/names/refresh", {
          entity_id: selectedName.lookup_id || selectedName.id,
        }),
      );
      await loadNames();
      toast("已从聊天平台更新");
    } finally {
      b.disabled = !selectedName.lookup_id;
    }
  });
guard(boot);

function renderVersions(kind, target, revision, versions) {
  const box = $("#versionList");
  if (!box) return;
  box.innerHTML = versions.length
    ? versions
        .map(
          (v) =>
            `<div class="task"><strong>${esc(v.reason || "修改")}</strong><small>${date(v.created)}</small><button data-version="${v.id}" data-vtarget="${esc(target)}" data-vrevision="${revision}" data-vkind="${kind}">恢复此版本</button></div>`,
        )
        .join("")
    : '<p class="muted">还没有历史版本</p>';
  $$("#versionList [data-version]").forEach(
    (b) =>
      (b.onclick = () =>
        guard(async () => {
          await api("/restore", {
            kind: b.dataset.vkind,
            target: b.dataset.vtarget,
            version_id: Number(b.dataset.version),
            revision: Number(b.dataset.vrevision),
          });
          toast("已恢复该版本");
          if (b.dataset.vkind === "record") await openRecord(b.dataset.vtarget);
          else await openFact({ id: b.dataset.vtarget });
        })),
  );
}

let profileData = null;

async function openProfile(entityId) {
  const p = await api("/profile?entity_id=" + encodeURIComponent(entityId));
  profileData = p;
  const stats = p.stats || {};
  $("#profileTitle").textContent =
    (p.entity.name || p.entity.label || "实体") + " 的画像";
  $("#profileMeta").textContent =
    p.entity.id +
    " · " +
    kindLabel(p.entity.kind) +
    " · 事实 " + (stats.facts || 0) +
    " · 关系 " + (stats.relations || 0) +
    " · 会话 " + (stats.sessions || 0) +
    (stats.last_active ? " · 最近活跃 " + date(stats.last_active) : "");
  $("#profileSummary").textContent =
    (p.summary || []).join(" · ") || "还没有画像要点";
  const groups = Object.entries(p.categories || {})
    .map(
      ([category, facts]) =>
        "<h3>" + esc(labels[category] || category) + "</h3>" +
        facts
          .map(
            (f) =>
              `<div class="task"><strong>${esc(f.content)}</strong><small>重要度 ${esc(String(f.importance))} · ${esc(f.sid)}${f.reason ? " · " + esc(f.reason) : ""}</small><button data-pf="${esc(f.id)}">编辑 / 恢复</button></div>`,
          )
          .join(""),
    )
    .join("");
  const relations = (p.relations || [])
    .map(
      (r) =>
        `<div class="task"><strong>${esc(r.subject)} —${esc(r.predicate)}→ ${esc(r.object)}</strong></div>`,
    )
    .join("");
  const names = (p.entity.history || [])
    .map(
      (h) =>
        `<div class="task"><strong>${esc(h.name)}</strong><small>${date(h.observed)} · ${esc(h.source)}</small>${h.name === p.entity.name ? "" : `<button data-pn="${esc(h.name)}">设为当前名</button>`}</div>`,
    )
    .join("");
  $("#profileBody").innerHTML =
    groups +
    "<h3>关系</h3>" +
    (relations || '<p class="muted">暂无关系连线</p>') +
    "<h3>名字历史</h3>" +
    (names || '<p class="muted">暂无名字记录</p>');
  $$("#profileBody [data-pf]").forEach(
    (b) =>
      (b.onclick = () =>
        guard(async () => {
          const fact = Object.values(p.categories || {})
            .flat()
            .find((f) => f.id === b.dataset.pf);
          $("#profileDialog").close();
          if (fact) await openFact(fact);
        })),
  );
  $$("#profileBody [data-pn]").forEach(
    (b) =>
      (b.onclick = () =>
        guard(async () => {
          await api("/names", {
            entity_id: p.entity.id,
            name: b.dataset.pn,
            revision: p.entity.revision,
            reason: "恢复到曾用名",
          });
          toast("已切换当前称呼");
          await openProfile(p.entity.id);
        })),
  );
  if (!$("#profileDialog").open) $("#profileDialog").showModal();
}
$("#closeProfile").onclick = () => $("#profileDialog").close();
$("#profileEditName").onclick = () =>
  guard(() => {
    if (!profileData) return;
    $("#profileDialog").close();
    openName({ ...profileData.entity, identity_note: "" });
  });
