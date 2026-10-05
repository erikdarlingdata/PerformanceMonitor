/* Runs the web viewer's Memory Pressure Events panels (memoryPressurePanels in wwwroot/js/pages/server-tabs.js, with util.js,
   panels.js and charts.js) against a scripted /api/read answer and prints what they drew, as one line of JSON.
   WebMemoryPressureChartBehaviourTests starts it as
       node web-memory-pressure-harness.mjs <path to wwwroot/js> <scenario>
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
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "pressure-"));
let modules;
try {
  fs.mkdirSync(path.join(scratch, "pages"));
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.copyFileSync(path.join(jsDir, "util.js"), path.join(scratch, "util.js"));
  fs.copyFileSync(path.join(jsDir, "panels.js"), path.join(scratch, "panels.js"));
  fs.copyFileSync(path.join(jsDir, "grid-tools.js"), path.join(scratch, "grid-tools.js"));
  fs.copyFileSync(path.join(jsDir, "pages", "server-tabs.js"), path.join(scratch, "pages", "server-tabs.js"));
  fs.copyFileSync(path.join(jsDir, "read-fields.js"), path.join(scratch, "read-fields.js"));
  for (const rel of ["grid-tools.js", "multi-picker.js", path.join("pages", "analysis-findings.js"), path.join("pages", "plan-viewer.js")]) {
    const from = path.join(jsDir, rel);
    if (!fs.existsSync(from)) continue;
    fs.mkdirSync(path.dirname(path.join(scratch, rel)), { recursive: true });
    fs.copyFileSync(from, path.join(scratch, rel));
  }
  /* #5246: the Graph cell of the Deadlock Graphs grid. */
  fs.mkdirSync(path.join(scratch, "pages"), { recursive: true });
  fs.copyFileSync(path.join(jsDir, "pages", "deadlock-graph.js"), path.join(scratch, "pages", "deadlock-graph.js"));
  fs.copyFileSync(path.join(jsDir, "charts.js"), path.join(scratch, "charts-real.js"));
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
    tabs: await load(path.join("pages", "server-tabs.js")),
    charts: await load("charts.js"),
  };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}


const data = (body) => ({ status: 200, body });
const tool = (url) => url.pathname.replace("/api/read/", "");
const HOUR = 3600000;
const top = Math.floor(Date.now() / HOUR) * HOUR;
const at = (hoursAgo, minute) => new Date(top - hoursAgo * HOUR + minute * 60000).toISOString().slice(0, 19);
const ev = (t, p, s, n = "RESOURCE_MEMPHYSICAL_LOW") => ({ sample_time: t, memory_notification: n, memory_indicators_process: p, memory_indicators_system: s });

const scenarios = {
  // Four samples in one hour (two SQL medium, one SQL severe + OS medium, one normal that is not drawn), one in an hour 3 back.
  counts: () => {
    answer = () => data({ server: "SRV1", hours_back: 6, events: [ev(at(3, 5), 2, 0), ev(at(1, 5), 2, 1), ev(at(1, 10), 3, 2), ev(at(1, 20), 0, 1), ev(at(1, 30), 1, 3)] });
    return modules.tabs.memoryPressurePanels("SRV1", { hours: 6, label: "last 6 hours" });
  },
  // Only normal samples: nothing drawn, the table still lists them.
  quiet: () => {
    answer = () => data({ server: "SRV1", hours_back: 6, events: [ev(at(1, 5), 1, 0), ev(at(2, 5), 0, 1)] });
    return modules.tabs.memoryPressurePanels("SRV1", { hours: 6, label: "last 6 hours" });
  },
  // A data answer the server stamped with a partial-window note (#4966): the grid draws it once, the chart draws none.
  floor: () => {
    answer = () => data({
      server: "SRV1", hours_back: 6, window_truncated: true, effective_start: "2026-01-02T12:30:00Z",
      truncation_note: "partial window: this panel's data starts at 2026-01-02 12:30 UTC, after the window's start at 2026-01-02 06:00 UTC.",
      events: [ev(at(1, 5), 2, 0)],
    });
    return modules.tabs.memoryPressurePanels("SRV1", { hours: 6, label: "last 6 hours" });
  },
  empty: () => {
    answer = () => data({ status: "empty", message: "No memory pressure events found in the requested time range." });
    return modules.tabs.memoryPressurePanels("SRV1", { hours: 6, label: "last 6 hours" });
  },
};

const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);

const root = new FakeNode("main");
modules.util.mount(root, chosen());
for (let i = 0; i < 200; i++) await new Promise((r) => setTimeout(r, 0));

const all = (node, tag, found = []) => {
  if (!node || typeof node !== "object") return found;
  if (node.tag === tag) found.push(node);
  node.children.forEach((c) => all(c, tag, found));
  return found;
};
const table = all(root, "table")[0] || null;
const calls = modules.charts.chartCalls;

console.log(JSON.stringify({
  charts: calls.map((c) => ({ series: c.series, unit: c.unit, mode: c.mode, points: c.points.map((p) => [p.time, p.sql_medium, p.sql_severe, p.os_medium, p.os_severe]), windowStart: c.windowStart, windowEnd: c.windowEnd })),
  tableRows: table ? all(table, "tr").map((tr) => all(tr, "td").map((td) => td.textContent)).filter((cells) => cells.length).length : 0,
  notices: all(root, "div").filter((n) => n.className === "strip notice").map((n) => n.textContent),
  empties: all(root, "div").filter((n) => n.className === "strip empty").map((n) => n.textContent),
  errors: all(root, "div").filter((n) => n.className === "strip error").map((n) => n.textContent),
  fetches,
  rejections,
}));
