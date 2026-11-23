/* Runs the Query Store clutter panels on the web viewer (#8929): pages/server-tabs.js (fanout), util.js (windowFloorStrip) and
   plain-text.js, with a stand-in DOM and a stub fetch that answers get_query_store_clutter with the window block the service writes and the
   scenario's per-arm spans. It prints the yellow notice lines of every panel of the Queries tab.
   WebClutterWindowNoteBehaviourTests starts it as
       node web-clutter-window-note-harness.mjs <path to wwwroot/js> <plan_churn_effective_hours_back, or "none"> <wait_effective_hours_back, or "none">
   The whole js tree is copied into a scratch folder first, then charts.js (the SVG renderer needs a real browser) is replaced. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.listeners = {};
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() {
    return this.children[0] || null;
  }
  appendChild(child) {
    child.parentNode = this;
    this.children.push(child);
    return child;
  }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    return child;
  }
  setAttribute(name, value) {
    this.attrs[name] = String(value);
  }
  getAttribute(name) {
    return name in this.attrs ? this.attrs[name] : null;
  }
  addEventListener(type, listener) {
    (this.listeners[type] = this.listeners[type] || []).push(listener);
  }
  set textContent(value) {
    this.children = [];
    this.text = String(value);
  }
  get textContent() {
    return (this.text || "") + this.children.map((c) => c.textContent).join("");
  }
}

globalThis.Node = FakeNode;
globalThis.document = {
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
};
globalThis.location = { hash: "#/server/SRV1" };

const rejections = [];
process.on("unhandledRejection", (e) => rejections.push(String(e && e.stack ? e.stack : e)));


const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "clutter-window-note-"));
let util;
let panels;
let tabs;
try {
  /* The whole js tree (js/, js/pages/ and every subdirectory) is copied rather than a hand-kept list (#5279); every stand-in
     below is written AFTER the copy, so it still replaces the real file. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    "export const SERIES_COLORS = ['#111', '#222', '#333', '#444', '#555', '#666'];\n" +
      "export const CATEGORICAL_COLORS = SERIES_COLORS;\n" +
      "export function normalizeColor(color) { return color; }\n" +
      "export function renderLineChart() { return document.createElement('div'); }\n" +
      "export function zoomableLineChart(spec) { (globalThis.__charts = globalThis.__charts || []).push({ title: spec.title, labels: (spec.series || []).map((x) => x.label), unit: spec.unit }); return document.createElement('div'); }\n" +
      "export function chartZoomScope() { return ''; }\n"
  );
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  util = await load("util.js");
  panels = await load("panels.js");
  tabs = await load(path.join("pages", "server-tabs.js"));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}


const hasClass = (node, name) => String(node.className).split(" ").includes(name);
const walk = (node, visit) => {
  visit(node);
  for (const c of node.children) walk(c, visit);
};
const settle = async () => {
  for (let i = 0; i < 200; i++) await new Promise((r) => setImmediate(r));
};

/* The window block as DarlingMcpQueryStoreClutterTools writes it: a span per arm, null when that arm reaches the window start, and the note naming
   the cut arms. Each scenario value is a JSON literal ("72", "null", "true") or "none" for a block that carries no such field, so a null or a boolean
   reaches the page as one. effective_hours_back is the first arm the note names: plan churn when it is cut, else the wait arm. */
const planChurn = process.argv[3] === "none" ? undefined : JSON.parse(process.argv[3]);
const wait = process.argv[4] === "none" ? undefined : JSON.parse(process.argv[4]);
const cut = (v) => v !== undefined && v !== null;
const arms = [];
const detail = [];
if (cut(planChurn)) {
  arms.push("the plan-churn arm");
  detail.push("The plan-churn arm holds " + planChurn + " hours of the window.");
}
if (cut(wait)) {
  arms.push("the wait arm");
  detail.push("The wait arm holds " + wait + " hours of the window.");
}
const NOTE =
  "The window reaches further back than this server's stored data for " + arms.join(" and ") + ". " + detail.join(" ") +
  " The read-cost and config arms read tables with longer retention and cover the whole window.";
const window = { start: "2026-01-01T00:00:00Z", end: "2026-01-08T00:00:00Z", effective_start: "2026-01-05T00:00:00Z", window_truncated: true, truncation_note: NOTE };
if (planChurn !== undefined) window.plan_churn_effective_hours_back = planChurn;
if (wait !== undefined) window.wait_effective_hours_back = wait;
window.effective_hours_back = cut(planChurn) ? planChurn : wait === undefined ? null : wait;
const body = {
  window,
  databases: [{ database_name: "db1", verdict: "ok" }],
  qs_overhead: { wait_stats: { included: [{ wait_type: "QDS_X", wait_ms_delta: 5 }], excluded_note: null }, memory_clerk: { note: null } },
};
globalThis.fetch = async (url) => {
  const u = String(url);
  return { status: 200, ok: true, text: async () => JSON.stringify(u.includes("/get_query_store_clutter") ? body : {}) };
};

const tab = tabs.SERVER_TABS.find((t) => t.id === "queries");
const holder = new FakeNode("div");
util.mount(holder, tab.build("SRV1", { hours: 168, label: "past week" }));
await settle();
const found = [];
walk(holder, (node) => {
  if (!hasClass(node, "panel")) return;
  const h3 = node.children.find((c) => c.tag === "h3");
  const notices = [];
  walk(node, (x) => {
    if (hasClass(x, "notice")) notices.push(x.textContent);
  });
  found.push({ title: h3 && h3.children[0] ? h3.children[0].textContent : "", notices });
});
console.log(JSON.stringify({ panels: found, rejections }));
