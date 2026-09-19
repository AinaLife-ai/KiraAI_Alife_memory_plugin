// 前端运行时冒烟：在 node 里用最小 DOM 桩**真跑一遍 app.js**，并模拟一次 /status 响应
// 目的：抓 2.17.4 那类「语法/引用坏掉 → poll() 抛错 → 按钮全死」（node --check 抓不到的那类）
import fs from "fs";
import path from "path";
import vm from "vm";

const root = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const src = fs.readFileSync(path.join(root, "web", "app.js"), "utf8");

const created = [];
const memo = {};
function makeEl(tag = "div") {
  const el = {
    tagName: tag, textContent: "", value: "", className: "", title: "", id: "",
    children: [], dataset: {},
    style: { setProperty() {}, removeProperty() {}, getPropertyValue: () => "" },
    classList: { add() {}, remove() {}, toggle() {}, contains: () => false },
    appendChild(c) { el.children.push(c); return c; },
    insertBefore(c) { el.children.push(c); return c; },
    add() {}, remove: () => {}, removeChild() {}, setAttribute() {}, getAttribute: () => null,
    addEventListener() {}, removeEventListener() {}, focus() {}, click() {},
    replaceChildren() {}, cloneNode() { return makeEl(); }, contains: () => false,
    matches: () => false, animate: () => ({ finished: Promise.resolve() }),
    insertAdjacentHTML() {}, scrollTo() {}, scrollIntoView() {},
    getContext: () => null, closest: () => null,
    querySelector: () => makeEl(), querySelectorAll: () => [],
    getBoundingClientRect: () => ({ width: 0, height: 0, top: 0, left: 0 }),
  };
  created.push(el);
  return el;
}

const document = {
  body: makeEl("body"), head: makeEl("head"),
  documentElement: makeEl("html"),
  createElement: (t) => makeEl(t),
  createTextNode: (t) => ({ textContent: t }),
  querySelector: (sel) => (memo[sel] = memo[sel] || makeEl()),
  querySelectorAll: () => [],
  getElementById: (id) => (memo["#" + id] = memo["#" + id] || makeEl("span")),
  addEventListener() {}, removeEventListener() {},
  readyState: "complete",
};

const statusFixture = {
  enabled: true, revision: 7, sessions: ["qq:gm:1"], sessions_total: 1,
  capacity: { levels: { "0": 42 }, years: { "2026": 42 }, db_bytes: 12_500_000, fts_rows: 42 },
  recall_usage: { total_calls: 3, total_chars: 5200, sessions: 1 },
  audit_usage: { rounds: 2, round_sessions: 1, last_round_at: 1, calls_today: 1 },
  search_index: "ready",
};

const timers = [];
const sandbox = {
  MutationObserver: class { observe() {} disconnect() {} takeRecords() { return []; } },
  Option: class { constructor(text, value) { this.text = text; this.value = value; } },
  Event: class { constructor(type) { this.type = type; } },
  CustomEvent: class { constructor(type) { this.type = type; } },
  IntersectionObserver: class { observe() {} disconnect() {} },
  ResizeObserver: class { observe() {} disconnect() {} },
  requestAnimationFrame: (fn) => { timers.push(fn); return timers.length; },
  cancelAnimationFrame() {},
  getComputedStyle: () => ({ getPropertyValue: () => "" }),
  matchMedia: () => ({ matches: false, addEventListener() {}, addListener() {} }),
  crypto: { randomUUID: () => "00000000-0000-0000-0000-000000000000" },
  clearInterval() {}, clearTimeout() {},
  document, window: { addEventListener() {}, location: { href: "" }, matchMedia: () => ({ matches: false, addEventListener() {} }) },
  navigator: { userAgent: "node", clipboard: { writeText: async () => {} }, language: "zh-CN" },
  localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
  fetch: async () => ({ ok: true, status: 200, json: async () => statusFixture, text: async () => "" }),
  setTimeout: (fn) => { timers.push(fn); return timers.length; },
  setInterval: (fn) => { timers.push(fn); return 0; },
  console, JSON, Math, Date, Object, Array, String, Number, Boolean, Promise, Error, Map, Set, RegExp,
};
sandbox.globalThis = sandbox;

let failure = "";
try {
  vm.createContext(sandbox);
  vm.runInContext(src, sandbox, { filename: "app.js" });
  // 真跑一遍定时器回调（含状态刷新 poll()）——只跑一轮，避免自排重入 ✗
  const queued = timers.splice(0, timers.length);
  for (const fn of queued.slice(0, 6)) {
    fn();
  }
  const ran = queued.length;
  if (ran > 0) console.log("JS-SMOKE-OK timers=" + ran);
  // 真跑一遍状态刷新（app.js 通常暴露 refresh/poll 或自己启动；这里把常见入口都试一遍）
  // （旧的"按名字找入口"逻辑已由上面的定时器队列取代 ✗ 避免重复声明）
} catch (e) {
  failure = String((e && e.stack) || e);
}

if (failure) {
  console.error("JS-SMOKE-FAIL " + failure.split("\n").slice(0, 12).join("\n   "));
  process.exit(1);
}
console.log("JS-SMOKE-OK loaded");

// ── 体检页行为检查（★ 2026-09-18，用户反馈后升级）────────────────────
// 事故 1：loadHealth() 从没定义 ⇒ 页面停在"正在加载…"
// 事故 2（用户实测）：一次渲染**全部 300 条** ⇒ 每次点击后重建 ⇒ 浏览器卡死
// 事故 3（用户实测）：卡片信息太少、只能删除 ⇒ 不好判断
//   ⇒ 本检查：分页 50 ✓ 点卡片打开编辑器 ✓ ＋发对 id ✓ meta 更新 ✓
const healthCalls = [];
const manyRows = Array.from({ length: 300 }, (_, i) => ({
  id: "f" + i, sid: "s1", subject: "u-zhou", content: "第" + i + "条事实的内容",
  category: "fact", importance: 3, rotate_used: i % 5, age_days: 40 - (i % 30),
  score: 5 + (i % 20), sunk: i % 10 === 0, never_sink: false,
  tags: ["猫", "日常"], reason: "用户原话", scenario: "聊天",
  sources: [{ id: "r1" }, { id: "r2" }], rewrite_pending: false, relation_warnings: [], edit_history: [],
}));
sandbox.fetch = async (url, opts) => {
  healthCalls.push(String(url));
  if (String(url).includes("fact_health")) {
    const m = String(url).match(/offset=(\d+)/);
    const off = m ? Number(m[1]) : 0;
    return { ok: true, status: 200, json: async () => ({ threshold: 12, count: 300, offset: off, now: 0, rows: manyRows.slice(off, off + 50) }) };
  }
  return { ok: true, status: 200, json: async () => ({ id: "f0", subject: "u-zhou", sid: "s1", versions: [], fields: {} }), text: async () => "" };
};
{
  const results = [];
  const ok = (name, cond, extra) =>
    results.push((cond ? "HEALTH-SMOKE-OK   " : "HEALTH-SMOKE-FAIL ") + name + (extra ? " | " + extra : ""));
  ok("loadHealth 已定义", typeof sandbox.loadHealth === "function");
  try { await sandbox.loadHealth(); ok("调用不抛错", true); }
  catch (e) { ok("调用不抛错", false, String((e && e.message) || e)); }
  ok("请求了 /fact_health", healthCalls.some((u) => u.includes("fact_health")));
  {
    const body = src.slice(src.indexOf("async function loadHealth("));
    const iNames = body.indexOf("ensureNames()");
    const iFetch = body.indexOf("/fact_health");
    const iTimeout = body.indexOf("1500");   // ★ 超时上限也必须在（否则会卡住"永远正在加载"）
    ok("★ 先补昵称表（且带超时）再取数据",
      iNames > -1 && iFetch > -1 && iNames < iFetch && iTimeout > -1,
      "ensureNames@" + iNames + " / timeout@" + iTimeout + " / fact_health@" + iFetch);
  }
  ok("★ 服务端分页：请求带 limit=50&offset=0（不再一次拉 300 行）",
    healthCalls.some((u) => u.includes("limit=50") && u.includes("offset=0")), healthCalls[0]);
  const metaText = String((memo["#healthMeta"] || {}).textContent || "");
  ok("meta 已更新（不再停在加载中）", metaText.includes("300") && !metaText.includes("正在加载"), metaText.slice(0, 46));
  const html = String((memo["#healthList"] || {}).innerHTML || "");
  const cards = (html.match(/<article class="card"/g) || []).length;
  ok("★ 分页：300 条只渲染 50 张（防卡死）", cards === 50, "渲染 " + cards + " 张");
  ok("★ 有翻页控件（总数来自 count=300 ⇒ 6 页）", html.includes("data-hpage=") && html.includes("/ 6"), (html.match(/>\d+ \/ \d+</) || [""])[0]);
  // 点「下一页」⇒ 应该用 offset=50 再拉一次（而不是本地切片）
  const nxt = { dataset: { hpage: "next" }, onclick: null };
  sandbox.document.querySelectorAll = (sel) => (String(sel).includes("data-hpage") ? [nxt] : []);
  sandbox.paintHealth();
  const before = healthCalls.length;
  try { nxt.onclick && nxt.onclick(); } catch (e) {}
  await new Promise((r) => setTimeout(r, 20));
  ok("★ 翻页真的走服务端（请求 offset=50）",
    healthCalls.slice(before).some((u) => u.includes("offset=50")), healthCalls.slice(before).join(","));
  ok("★ 对齐正常卡片的四要素（tag/strong/p/small）",
    html.includes('class="tag"') && html.includes("<strong>") && html.includes("<p>") && html.includes("<small>重要度"));
  ok("卡片带 data-hidx（可点开编辑器）", html.includes("data-hidx="));
  // ★ 用户要求：与「事实卡片」显示的东西**一样都不能少** ✓
  const must = [
    ['类别标签', 'class="tag"'],
    ['显示名', "<strong>"],
    ['内容段', "<p>"],
    ['标签行', "<small>猫 · 日常</small>"],
    ['事实依据', "事实依据：用户原话"],
    ['场景', "场景：聊天"],
    ['来源数', "2 个来源"],
    ['明确的「编辑事实」按钮', ">编辑事实</button>"],
    ['重要度 ±1 快捷按钮', 'data-delta="-1"'],
    ['删除按钮', "data-del="],
    ['本页特有：分数', "分数 "],
    ['本页特有：被用次数', "被用 "],
    ['本页特有：年龄', "天前"],
  ];
  const missing = must.filter(([, needle]) => !html.includes(needle)).map(([name]) => name);
  ok("★ 与事实卡片逐样对齐（缺一即红）", missing.length === 0, missing.length ? "缺：" + missing.join("、") : "13 样齐");
  // 点卡片 ⇒ 打开编辑器（openFact 会去拉 /fact/<id>）
  await sandbox.loadHealth(0);   // ★ 回到第 1 页，免得上面翻页检查把状态留在第 2 页（测试隔离 ✓）
  const n0 = healthCalls.length;
  const cardEl = { dataset: { hidx: "0" }, onclick: null, closest: () => null };
  sandbox.document.querySelectorAll = (sel) => (String(sel).includes("data-hidx") ? [cardEl] : []);
  try { sandbox.paintHealth(); cardEl.onclick && cardEl.onclick({ target: { closest: () => null } }); }
  catch (e) { ok("点卡片不抛错", false, String(e && e.message)); }
  await new Promise((r) => setTimeout(r, 10));
  ok("★ 点卡片 = 打开编辑器（请求 /fact/<id>）",
    healthCalls.slice(n0).some((u) => u.includes("/fact/")), healthCalls.slice(n0).join(","));
  for (const line of results) console.log(line);
  if (results.some((l) => l.includes("FAIL"))) process.exitCode = 1;
}
