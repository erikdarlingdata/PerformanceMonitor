/* #7655: runs two display details of the server page with the shipped panels.js, charts.js and util.js against a scripted /api/read
   answer, and prints as one line of JSON what a person reads:
     - blocking: the Blocking tab's Waiting Tasks and Lock Waits charts (wwwroot/js/pages/server-tabs.js), their y-axis tick labels and
       unit captions;
     - tiles: the Severity tile of Freeze Headroom, Autovacuum Backlog and Replication Slots (shared.js PG_*_STATS through panels.js).
   WebDetails7655BehaviourTests starts it as
       node web-details-7655-harness.mjs <path to wwwroot/js> <scenario>
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
  setAttribute(k, v) { this.attrs[k] = String(v); if (k === "class") this.className = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    this.children.push(c);
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = {
  body: new FakeNode("body"),
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeText(t),
};
globalThis.location = { hash: "#/server/a/blocking" };

/* Times are hours back from now, so the 24 hour window the tab asks for always holds them. One clock read anchors every row. */
const NOW_MIN = Math.floor(Date.now() / 60000) * 60000;
const ago = (h) => new Date(NOW_MIN - h * 3600000).toISOString().replace("Z", "0000");
/* Waiting Tasks: the busiest collection waited 46,800 ms in all, the figure the issue names. Lock Waits: up to 3,000 ms of lock wait a second. */
const WAITS = [
  { collection_time: ago(6), wait_type: "LCK_M_X", total_wait_ms: 4100 },
  { collection_time: ago(4), wait_type: "LCK_M_X", total_wait_ms: 26800 },
  { collection_time: ago(4), wait_type: "CXCONSUMER", total_wait_ms: 20000 },
  { collection_time: ago(2), wait_type: "LATCH_EX", total_wait_ms: 900 },
];
const BLOCKED = [{ collection_time: ago(4), database_name: "Sales", blocked_count: 3 }];
const LOCKS = [
  { collection_time: ago(6), wait_time_ms_per_second: 120.5 },
  { collection_time: ago(4), wait_time_ms_per_second: 3000 },
  { collection_time: ago(2), wait_time_ms_per_second: 40 },
];
const answerFor = (name) => {
  if (name === "get_current_waits_trend") return { server: "a", waiting_tasks: WAITS, blocked_sessions: BLOCKED };
  if (name === "get_lock_wait_trend") return { server: "a", trend: LOCKS, wait_types: ["LCK_M_X"] };
  return {};
};
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const body = answerFor(u.pathname.replace("/api/read/", ""));
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "web-details-7655-"));
let modules;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = { util: await load("util.js"), panels: await load("panels.js"), tabs: await load(path.join("pages", "server-tabs.js")), shared: await load(path.join("pages", "server-tabs", "shared.js")) };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const hasClass = (n, c) => new RegExp("\\b" + c + "\\b").test(n.className || n.attrs.class || "");
const flush = async () => { for (let i = 0; i < 25; i++) await new Promise((r) => setTimeout(r, 0)); };
const draw = async (registry, tabId) => {
  const tab = registry.find((t) => t.id === tabId);
  const root = new FakeNode("main");
  modules.util.mount(root, tab.build("a", { hours: 24, label: "last 24 hours" }));
  await flush();
  return root;
};
const panelOf = (root, title) =>
  all(root).find((n) => hasClass(n, "panel") && all(n).some((c) => c.tag === "h3" && c.textContent.startsWith(title))) || null;
/* The y-axis as the chart-axis harness reads it: the text nodes anchored at the end 8 px left of the plot's left edge, and the unit caption. */
const axisOf = (panel) => {
  if (!panel) return { present: false };
  const nodes = all(panel);
  const grid = nodes.filter((n) => n.tag === "line" && hasClass(n, "grid-line") && n.attrs.x2 !== n.attrs.x1);
  const left = grid.length ? Number(grid[0].attrs.x1) : NaN;
  const texts = nodes.filter((n) => n.tag === "text");
  return {
    present: true,
    labels: texts.filter((n) => !hasClass(n, "axis-unit") && n.attrs["text-anchor"] === "end" && Number(n.attrs.x) === left - 8).map((n) => n.textContent),
    caption: texts.filter((n) => hasClass(n, "axis-unit")).map((n) => n.textContent),
    legend: nodes.filter((n) => n.tag === "span" || n.tag === "button").map((n) => n.textContent).filter((t) => t),
  };
};
const tiles = (root) => Object.fromEntries(all(root).filter((n) => hasClass(n, "stat")).map((n) => [n.children[1].textContent, n.children[0].textContent]));

const out = {};
if (scenario === "blocking") {
  const root = await draw(modules.tabs.SERVER_TABS, "blocking");
  out.waitingTasks = axisOf(panelOf(root, "Waiting Tasks"));
  out.lockWaits = axisOf(panelOf(root, "Lock Waits"));
} else if (scenario === "tiles") {
  const { panels, shared } = modules;
  const severity = (stats, data) => tiles(panels.VIZ.stat(data, { stats }))["Severity"];
  out.wraparoundOk = severity(shared.PG_WRAPAROUND_STATS, { worst_database: "sales", worst_severity: "ok", worst_pct_toward_wraparound: 12.5 });
  out.wraparoundCritical = severity(shared.PG_WRAPAROUND_STATS, { worst_database: "sales", worst_severity: "critical_failsafe_range", worst_pct_toward_wraparound: 97.2 });
  out.autovacuumWarning = severity(shared.PG_AUTOVACUUM_STATS, { past_threshold_count: 2, growing_count: 1, autovacuum_disabled_count: 0, worst_table: "public.orders", worst_severity: "warning_past_threshold_and_growing" });
  out.autovacuumCritical = severity(shared.PG_AUTOVACUUM_STATS, { past_threshold_count: 2, growing_count: 1, autovacuum_disabled_count: 0, worst_table: "public.orders", worst_severity: "critical_far_past_threshold" });
  out.slotsWarning = severity(shared.PG_SLOT_STATS, { slot_count: 2, inactive_count: 1, total_retained_wal_gb: 1.2, worst_slot: "sub_a", worst_severity: "warning_inactive_and_growing" });
  out.slotsPlainWarning = severity(shared.PG_SLOT_STATS, { slot_count: 2, inactive_count: 1, total_retained_wal_gb: 1.2, worst_slot: "sub_a", worst_severity: "warning" });
  out.slotsMissing = severity(shared.PG_SLOT_STATS, { slot_count: 0, inactive_count: 0, total_retained_wal_gb: 0, worst_slot: null, worst_severity: null });
} else {
  throw new Error("unknown scenario " + scenario);
}
console.log(JSON.stringify(out));
