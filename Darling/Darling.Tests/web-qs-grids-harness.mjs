/* #4843: runs the server page's Queries tab (SERVER_TABS "queries" in wwwroot/js/pages/server-tabs.js, with the shipped
   panels.js and util.js) against a scripted /api/read answer and prints what its Query Store Regressions, Long Query
   Completions and Plan Corrections grids drew, as one line of JSON. WebQueryStoreGridGroupsBehaviourTests starts it as
       node web-qs-grids-harness.mjs <path to wwwroot/js> <scenario>
   Run it under TZ=America/New_York to see the browser-local rendering of a UTC instant. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

class FakeNode {
  constructor(tag) { this.style = {}; this.classList = { add: (c) => { this.className = (this.className + " " + c).trim(); }, remove: (c) => { this.className = this.className.split(" ").filter((x) => x !== c).join(" "); }, toggle() {}, contains: () => false }; this.tag = tag; this.children = []; this.attrs = {}; this.dataset = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; }
  get parentNode() { return this.parent; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  click() { this.fire("click", { target: this }); }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, ...e }); }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { body: new FakeNode("body"), createElement: (t) => new FakeNode(t), createElementNS: (ns, t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/a/queries" };

const fetches = [];
let answers = {};
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const name = u.pathname.replace("/api/read/", "");
  fetches.push(name);
  const body = answers[name] ?? {};
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "qsgrids-"));
let modules;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = { util: await load("util.js"), tabs: await load(path.join("pages", "server-tabs.js")) };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const UTC_STAMP = "2026-01-15T17:30:00.0000000";
const regressions = [{ severity: "High", database_name: "d1", query_text: "select 1", query_id: 7, additional_duration_ms: 5, baseline_cpu_ms: 11, recent_cpu_ms: 12, baseline_reads: 13, recent_reads: 14, baseline_exec_count: 15, recent_exec_count: 16, baseline_plan_count: 1, recent_plan_count: 2, last_execution_time: UTC_STAMP }];
const completions = [{ event_time: UTC_STAMP, statement: "select 2", event_type: "rpc", duration_ms: 9, logical_reads: 21, physical_reads: 22, writes: 23, row_count: 24, session_id: 55, client_app_name: "AppX", server_principal_name: "loginx", database_name: "d1" }];
const recommendations = [{ collection_time: UTC_STAMP, query_text: "select 3", database_name: "d1", recommendation_state: "Active", score: 90, query_id: 7, regressed_plan_id: 101, last_good_plan_id: 100, last_good_plan_forcing_type: "AUTO", regressed_plan_execution_count: 31, valid_since: UTC_STAMP, last_refresh: UTC_STAMP, execute_action_initiated_by: "System", execute_action_initiated_time: UTC_STAMP }];
answers = {
  get_query_store_regressions: { regressions },
  get_long_query_completions: { completions },
  get_plan_corrections: { recommendations, automatic_tuning: [] },
};

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const flush = async () => { for (let i = 0; i < 200; i++) await new Promise((r) => setTimeout(r, 0)); };
const draw = async () => {
  const tab = modules.tabs.SERVER_TABS.find((t) => t.id === "queries");
  const root = new FakeNode("main");
  modules.util.mount(root, tab.build("a", { hours: 24, label: "last 24 hours" }));
  await flush();
  return root;
};
const gridOf = (root, title) => {
  const panel = all(root).find((n) => n.className && /\bpanel\b/.test(n.className) && all(n).some((c) => c.tag === "h3" && c.textContent.startsWith(title))) || null;
  return panel;
};
const heads = (panel) => {
  const table = all(panel).find((n) => n.tag === "table");
  if (!table) return null;
  const tr = all(table).find((n) => n.tag === "tr");
  return tr.children.filter((t) => t.style.display !== "none").map((t) => t.textContent.replace(/[▲▼ ]/g, ""));
};
const rowCells = (panel) => {
  const table = all(panel).find((n) => n.tag === "table");
  const tbody = all(table).find((n) => n.tag === "tbody");
  return tbody.children[0].children.filter((t) => t.style.display !== "none").map((t) => t.textContent);
};
const picker = (panel) => all(panel).find((n) => n.className === "col-picker") || null;
const toggle = (panel, label) => picker(panel).children.find((c) => c.textContent === label);
const TITLES = { regressions: "Query Store Regressions", completions: "Long Query Completions", corrections: "Plan Corrections" };

const out = { fetches };
const scenarios = {
  async defaults() {
    const root = await draw();
    for (const [k, title] of Object.entries(TITLES)) {
      const p = gridOf(root, title);
      out[k] = { found: !!p, heads: p ? heads(p) : null, toggles: p && picker(p) ? picker(p).children.map((c) => c.textContent) : null };
    }
  },
  async toggling() {
    const root = await draw();
    const p = gridOf(root, TITLES.corrections);
    toggle(p, "Lifecycle").click();
    out.lifecycleOn = heads(p);
    toggle(p, "Plans").click();
    out.plansOn = heads(p);
    toggle(p, "Lifecycle").click();
    toggle(p, "Plans").click();
    out.allOff = heads(p);
    const q = gridOf(root, TITLES.regressions);
    toggle(q, "Executions and plans").click();
    out.regressionsExecs = heads(q);
    const l = gridOf(root, TITLES.completions);
    toggle(l, "Session").click();
    out.completionsSession = heads(l);
  },
  async repaint() {
    let root = await draw();
    toggle(gridOf(root, TITLES.corrections), "Lifecycle").click();
    toggle(gridOf(root, TITLES.completions), "I/O and rows").click();
    root = await draw();
    out.corrections = heads(gridOf(root, TITLES.corrections));
    out.completions = heads(gridOf(root, TITLES.completions));
    out.regressions = heads(gridOf(root, TITLES.regressions));
  },
  async localTime() {
    const root = await draw();
    const p = gridOf(root, TITLES.corrections);
    toggle(p, "Lifecycle").click();
    out.cells = rowCells(p);
    out.expected = new Date(UTC_STAMP + "Z").toLocaleString();
    const q = gridOf(root, TITLES.regressions);
    out.regressionCells = rowCells(q);
    const l = gridOf(root, TITLES.completions);
    out.completionCells = rowCells(l);
  },
};
await scenarios[scenario]();
console.log(JSON.stringify(out));
