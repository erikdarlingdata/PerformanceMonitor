/* #7571: runs the server page's Blocking tab (Waiting Tasks and Blocked Sessions charts) and Wait Stats tab (Waiting Tasks grid)
   from wwwroot/js/pages/server-tabs.js with the shipped panels.js, charts.js and util.js against a scripted /api/read answer, and
   prints as one line of JSON what was drawn: each chart's bars (count, segments per time, widths), its legend labels, and the grid's
   headers. WaitingTasksBarsBehaviourTests starts it as
       node waiting-tasks-bars-harness.mjs <path to wwwroot/js> <scenario>
   The whole js tree is copied into a scratch folder first, so a module another change adds needs no edit here. Only the DOM and
   fetch are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

class FakeNode {
  constructor(tag) {
    this.style = {};
    this.classList = {
      add: (c) => { this.className = (this.className + " " + c).trim(); },
      remove: (c) => { this.className = this.className.split(" ").filter((x) => x !== c).join(" "); },
      toggle() {},
      contains: () => false,
    };
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.listeners = {};
    this.parent = null;
    this._text = "";
    this.className = "";
  }
  get parentNode() { return this.parent; }
  getBoundingClientRect() { return { left: 0, top: 0, width: 1000, height: 300 }; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    this.children.push(c);
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, ...e }); }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = {
  body: new FakeNode("body"),
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeText(t),
};
globalThis.location = { hash: "#/server/a/queries" };

/* Two wait types at 01:11 (the case the line drew straight down through), one wait type at each other time. Blocked sessions: two
   databases at 01:11. */
/* Times are hours back from now, so the 24 hour window the tab asks for always holds them. */
const ago = (h) => new Date(Math.floor(Date.now() / 60000) * 60000 - h * 3600000).toISOString().replace("Z", "0000");
const WAITS = [
  { collection_time: ago(6), wait_type: "PREEMPTIVE_OS_CREATEFILE", total_wait_ms: 4 },
  { collection_time: ago(4), wait_type: "PREEMPTIVE_OS_CREATEFILE", total_wait_ms: 43 },
  { collection_time: ago(3), wait_type: "CXCONSUMER", total_wait_ms: 164 },
  { collection_time: ago(3), wait_type: "LATCH_EX", total_wait_ms: 4 },
];
const BLOCKED = [
  { collection_time: ago(3), database_name: "Sales", blocked_count: 3 },
  { collection_time: ago(3), database_name: "Orders", blocked_count: 2 },
  { collection_time: ago(1), database_name: "Sales", blocked_count: 1 },
];
const TASKS = [
  { collection_time: ago(3), session_id: 61, wait_type: "CXCONSUMER", wait_duration_ms: 82, thread_count: 2, blocking_session_id: 0, database_name: "Sales" },
];
/* Two collections a minute apart and one far from them: the tightest gap is a hairline in a 24 hour window. */
const closeWaits = [
  { collection_time: ago(6), wait_type: "CXCONSUMER", total_wait_ms: 40 },
  { collection_time: ago(1), wait_type: "CXCONSUMER", total_wait_ms: 30 },
  { collection_time: new Date(Date.parse(ago(1) + "Z") + 60000).toISOString().replace("Z", "0000"), wait_type: "CXCONSUMER", total_wait_ms: 20 },
];
/* One collection in a 24 hour window: the tiling width is the whole plot, so only the cap keeps it from being a slab. */
const loneWaits = [{ collection_time: ago(3), wait_type: "CXCONSUMER", total_wait_ms: 40 }];
const manyWaits = Array.from({ length: 10 }, (_, i) => ({ collection_time: ago(3), wait_type: "W" + i, total_wait_ms: 100 - i }));

/* Databases named like the chart's own fields: one named for the time column and one that is the prototype's key. Each is a segment of
   its own at 01:11 (2, 3 and 1 sessions) and the time column survives (a second bar, later, is Sales alone). */
const nameBlocked = [
  { collection_time: ago(3), database_name: "collection_time", blocked_count: 2 },
  { collection_time: ago(3), database_name: "__proto__", blocked_count: 3 },
  { collection_time: ago(3), database_name: "Sales", blocked_count: 1 },
  { collection_time: ago(1), database_name: "Sales", blocked_count: 4 },
];

const answerFor = (name) => {
  if (name === "get_current_waits_trend") {
    return { server: "a", waiting_tasks: scenario === "many" ? manyWaits : scenario === "close" ? closeWaits : scenario === "lone" ? loneWaits : WAITS, blocked_sessions: scenario === "names" ? nameBlocked : BLOCKED };
  }
  if (name === "get_waiting_tasks") return { server: "a", tasks: TASKS };
  return {};
};
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const body = answerFor(u.pathname.replace("/api/read/", ""));
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "waiting-tasks-bars-"));
let modules;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = { util: await load("util.js"), tabs: await load(path.join("pages", "server-tabs.js")) };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const flush = async () => { for (let i = 0; i < 25; i++) await new Promise((r) => setTimeout(r, 0)); };
const draw = async (tabId) => {
  const tab = modules.tabs.SERVER_TABS.find((t) => t.id === tabId);
  const root = new FakeNode("main");
  modules.util.mount(root, tab.build("a", { hours: 24, label: "last 24 hours" }));
  await flush();
  return root;
};
const panelOf = (root, title) =>
  all(root).find((n) => n.className && /\bpanel\b/.test(n.className) && all(n).some((c) => c.tag === "h3" && c.textContent.startsWith(title))) || null;
const chartReport = (panel) => {
  const nodes = panel ? all(panel) : [];
  const bars = nodes.filter((n) => /\bseries-bar\b/.test(n.attrs.class || ""));
  const byX = new Map();
  for (const b of bars) byX.set(b.attrs.x, (byX.get(b.attrs.x) || 0) + 1);
  return {
    present: panel !== null,
    lines: nodes.filter((n) => /\bseries-line\b/.test(n.attrs.class || "")).length,
    bars: bars.length,
    barColumns: byX.size,
    segmentsPerColumn: [...byX.values()].sort((a, b) => b - a),
    widths: [...new Set(bars.map((b) => Math.round(Number(b.attrs.width))))],
    text: nodes.filter((n) => n.tag === "text" || n.tag === "span" || n.tag === "button").map((n) => n.textContent).filter((t) => t),
  };
};

const out = {};
if (scenario === "default" || scenario === "many" || scenario === "close" || scenario === "lone" || scenario === "names") {
  const root = await draw("blocking");
  out.waiting = chartReport(panelOf(root, "Waiting Tasks"));
  out.blocked = chartReport(panelOf(root, "Blocked Sessions"));
  if (scenario === "names") {
    /* Point at the first bar (the one at 01:11) and read what the hover tooltip names: its time and a row per segment. */
    const panel = panelOf(root, "Blocked Sessions");
    const bars = all(panel).filter((n) => /\bseries-bar\b/.test(n.attrs.class || ""));
    const firstX = Math.min(...bars.map((b) => Number(b.attrs.x)));
    const first = bars.filter((b) => Number(b.attrs.x) === firstX);
    out.blocked.heights = first.map((b) => Number(b.attrs.height)).sort((a, b) => a - b);
    const overlay = all(panel).find((n) => n.listeners.mousemove);
    overlay.fire("mousemove", { clientX: firstX + Number(first[0].attrs.width) / 2 });
    const tip = all(panel).find((n) => /\bchart-tooltip\b/.test(n.className));
    out.tooltipTime = all(tip).find((n) => /\bt-time\b/.test(n.className)).textContent;
    out.expectedTime = new Date(Date.parse(ago(3) + "Z")).toLocaleString();
    out.tooltipRows = all(tip).filter((n) => /\bt-row\b/.test(n.className)).map((r) => r.children.slice(1).map((c) => c.textContent));
  }
} else if (scenario === "grid") {
  const root = await draw("waits");
  const panel = panelOf(root, "Waiting Tasks");
  out.headers = panel ? all(panel).filter((n) => n.tag === "th").map((n) => n.textContent) : [];
  out.cells = panel ? all(panel).filter((n) => n.tag === "td").map((n) => n.textContent) : [];
}
console.log(JSON.stringify(out));
