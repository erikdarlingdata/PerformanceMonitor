/* Runs the web viewer's Memory Grant Sizing and Activity charts (memoryGrantChartPanels in wwwroot/js/pages/memory-grant-charts.js,
   with util.js, panels.js and charts.js) against a scripted /api/read answer and prints what they drew, as one line of JSON (#6299).
   WebMemoryGrantChartBehaviourTests starts it as
       node web-memory-grant-chart-harness.mjs <path to wwwroot/js> <scenario>
   charts.js is the shipped renderer behind a wrapper that records each chart's spec. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

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
  getBoundingClientRect() {
    return { left: 0, top: 0, width: 1000, height: 320 };
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

const fetches = [];
let answer = () => ({ status: 200, body: {} });
globalThis.fetch = async (url) => {
  fetches.push(String(url));
  const reply = answer(new URL(String(url), "http://viewer.test"));
  const raw = reply.body === undefined ? "" : JSON.stringify(reply.body);
  return { status: reply.status, ok: reply.status >= 200 && reply.status < 300, text: async () => raw };
};

const rejections = [];
process.on("unhandledRejection", (e) => rejections.push(String(e && e.stack ? e.stack : e)));

/* The modules, unchanged. charts.js is the shipped renderer (copied as charts-real.js) behind a wrapper that records,
   for each chart, the lines and the unit it was asked to draw, the chart node itself, and the labels it printed
   while drawing: the value axis's tick labels, which are the only values it formats before a pointer arrives. */
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "grantchart-"));
let modules;
try {
  /* Copy the whole js tree (js/, js/pages/ and every subdirectory) rather than a hand-kept list (#5279): a page module that
     another PR adds then needs no edit here. Only imported files load, so the rest are inert; every stand-in below is
     written AFTER the copy, so it still replaces the real file. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.copyFileSync(path.join(scratch, "charts.js"), path.join(scratch, "charts-real.js"));
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { renderLineChart as drawLineChart } from "./charts-real.js";\n' +
      'export * from "./charts-real.js";\n' +
      "export const chartCalls = [];\n" +
      "export function renderLineChart(opts) {\n" +
      "  const axis = [];\n" +
      "  let drawing = true;\n" +
      "  const format = opts.formatValue || ((v) => String(v));\n" +
      "  const node = drawLineChart({ ...opts, formatValue: (v) => { const text = format(v); if (drawing) axis.push(text); return text; } });\n" +
      "  drawing = false;\n" +
      "  chartCalls.push({ series: (opts.series || []).map((s) => ({ key: s.key, label: s.label })), unit: opts.unit == null ? null : opts.unit, mode: opts.mode || null, points: opts.points, windowStart: opts.windowStart, windowEnd: opts.windowEnd, axis, node });\n" +
      "  return node;\n" +
      "}\n" +
      "export function zoomableLineChart(opts) { return renderLineChart(opts); }\n"
  );
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = {
    util: await load("util.js"),
    grants: await load(path.join("pages", "memory-grant-charts.js")),
    charts: await load("charts.js"),
  };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}



const data = (body) => ({ status: 200, body });
const entry = (t, pool, o) => ({ collection_time: t, pool_id: pool, available_memory_mb: 0, granted_memory_mb: 0, used_memory_mb: 0, target_memory_mb: 0, max_target_memory_mb: 0, grantee_count: 0, waiter_count: 0, timeout_error_count_delta: 0, forced_grant_count_delta: 0, snapshots: 1, ...o });
const ctx = { hours: 6, label: "last 6 hours" };

const scenarios = {
  // Three snapshots, two pools. The middle snapshot has no grants at all: a real point of zeros on both pools.
  series: () => {
    answer = () => data({ server: "SRV1", hours_back: 6, bucket_minutes: 5, total_entries: 6, shown: 6, aggregate_note: "Each point is one pool over one bucket.", entries: [
      entry("2026-01-02T10:00:00.0000000", 1, { granted_memory_mb: 400, available_memory_mb: 600, used_memory_mb: 300, target_memory_mb: 1000, max_target_memory_mb: 1200, grantee_count: 4, waiter_count: 2, timeout_error_count_delta: 1 }),
      entry("2026-01-02T10:00:00.0000000", 2, { granted_memory_mb: 50, available_memory_mb: 950 }),
      entry("2026-01-02T10:05:00.0000000", 1, { available_memory_mb: 1000, target_memory_mb: 1000, max_target_memory_mb: 1200 }),
      entry("2026-01-02T10:05:00.0000000", 2, { available_memory_mb: 1000 }),
      entry("2026-01-02T10:10:00.0000000", 1, { granted_memory_mb: 100, available_memory_mb: 900, grantee_count: 1 }),
      entry("2026-01-02T10:10:00.0000000", 2, {}),
    ] });
    return modules.grants.memoryGrantChartPanels("SRV1", ctx);
  },
  // The read had no collected snapshot in the window: its own sentence, and no chart at all (never a line of zeros).
  empty: () => {
    answer = () => data({ status: "unavailable", message: "No memory grant data available." });
    return modules.grants.memoryGrantChartPanels("SRV1", ctx);
  },
  // A data answer whose entries are empty: the same.
  noentries: () => {
    answer = () => data({ server: "SRV1", hours_back: 6, bucket_minutes: 5, total_entries: 0, shown: 0, entries: [] });
    return modules.grants.memoryGrantChartPanels("SRV1", ctx);
  },
  failed: () => {
    answer = () => ({ status: 500, body: { error: "boom" } });
    return modules.grants.memoryGrantChartPanels("SRV1", ctx);
  },
};

const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);

const root = new FakeNode("main");
const panels = chosen();
modules.util.mount(root, panels);
for (let i = 0; i < 200; i++) await new Promise((r) => setTimeout(r, 0));

const all = (node, tag, found = []) => {
  if (!node || typeof node !== "object") return found;
  if (node.tag === tag) found.push(node);
  node.children.forEach((c) => all(c, tag, found));
  return found;
};
const calls = modules.charts.chartCalls;

console.log(JSON.stringify({
  panels: panels.map((p) => p.attrs["data-title"]),
  charts: calls.map((c) => ({ series: c.series, unit: c.unit, points: c.points })),
  empties: all(root, "div").filter((n) => n.className === "strip empty").map((n) => n.textContent),
  errors: all(root, "div").filter((n) => n.className === "strip error").map((n) => n.textContent),
  fetches,
  rejections,
}));
