/* Runs the web viewer's Perfmon panel (perfmonPanel in wwwroot/js/pages/server-tabs.js, with util.js, panels.js and
   charts.js) against a scripted /api/read answer and prints the grid it drew and the chart it drew (its lines, its
   unit, the labels on its value axis and what its tooltip says at each end), as one line of JSON.
   WebPerfmonPerSecondBehaviourTests starts it as
       node web-perfmon-harness.mjs <path to wwwroot/js> <scenario>
   The modules are copied into a scratch folder and imported. charts.js is the shipped renderer, behind a thin
   wrapper that records what each chart was asked to draw and the value labels it printed. `fetch` and the DOM are
   stand-ins: a node tree of plain objects that keeps the listeners it is given, and a fetch that answers each URL
   from the scenario. Everything else is the shipped code. */
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
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "perfmon-grid-"));
let modules;
try {
  fs.mkdirSync(path.join(scratch, "pages"));
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.copyFileSync(path.join(jsDir, "util.js"), path.join(scratch, "util.js"));
  fs.copyFileSync(path.join(jsDir, "panels.js"), path.join(scratch, "panels.js"));
  fs.copyFileSync(path.join(jsDir, "grid-tools.js"), path.join(scratch, "grid-tools.js"));
  fs.copyFileSync(path.join(jsDir, "pages", "server-tabs.js"), path.join(scratch, "pages", "server-tabs.js"));
  fs.copyFileSync(path.join(jsDir, "pages", "analysis-findings.js"), path.join(scratch, "pages", "analysis-findings.js"));
  fs.copyFileSync(path.join(jsDir, "read-fields.js"), path.join(scratch, "read-fields.js"));
  for (const rel of ["grid-tools.js", "multi-picker.js", path.join("pages", "analysis-findings.js")]) {
    const from = path.join(jsDir, rel);
    if (!fs.existsSync(from)) continue;
    fs.mkdirSync(path.dirname(path.join(scratch, rel)), { recursive: true });
    fs.copyFileSync(from, path.join(scratch, rel));
  }
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
      "  chartCalls.push({ series: (opts.series || []).map((s) => ({ key: s.key, label: s.label })), unit: opts.unit == null ? null : opts.unit, axis, node });\n" +
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

/* get_perfmon_stats rows as the server builds them: a rate row carries per_second (null where no delta was
   knowable), and no other row has the key. */
const BATCHES = { counter_name: "Batch Requests/sec", instance_name: "", value: 11641, delta_value: 66, cntr_type: 272696576, counter_kind: "rate", per_second: 0.22 };
const COMPILES = { counter_name: "SQL Compilations/sec", instance_name: "", value: 4000, delta_value: 0, cntr_type: 272696576, counter_kind: "rate", per_second: null };
// One deadlock in the 300 s since the last collection: 37 since the counter started, 1 / 300 = 0.0033 a second.
const DEADLOCKS = { counter_name: "Number of Deadlocks/sec", instance_name: "", value: 37, delta_value: 1, cntr_type: 272696576, counter_kind: "rate", per_second: 0.0033 };
// Two rates with more digits than the rate format keeps, as the server publishes them (four decimals). 70 in the 300 s since
// the last collection is 70 / 300 = 0.2333 a second; 1,111,111 in 900 s is 1234.5678 a second. Below 1 the format keeps two
// significant digits (0.23), and from 1 up it keeps two decimals (1,234.57).
const PAGE_SPLITS = { counter_name: "Page Splits/sec", instance_name: "", value: 1113211, delta_value: 70, cntr_type: 272696576, counter_kind: "rate", per_second: 0.2333 };
const TRANSACTIONS = { counter_name: "Transactions/sec", instance_name: "", value: 4000000, delta_value: 1111111, cntr_type: 272696576, counter_kind: "rate", per_second: 1234.5678 };
const MEMORY = { counter_name: "Total Server Memory (KB)", instance_name: "", value: 8000000, delta_value: null, cntr_type: 65792, counter_kind: "gauge" };
const AVERAGE = { counter_name: "Lock waits", instance_name: "Average wait time (ms)", value: 5000, delta_value: 40, cntr_type: 1073874176, counter_kind: "other" };

const stats = (counters) => data({ server: "SRV1", captured_at: "2026-01-01T00:05:00.0000000", counters });

// A naive-UTC stamp the way the trend payload prints one, this many minutes before now.
const ago = (minutes) => new Date(Date.now() - minutes * 60000).toISOString().slice(0, 19);

const RATE_TREND = {
  server: "SRV1",
  counter_name: "Batch Requests/sec",
  cntr_type: 272696576,
  counter_kind: "rate",
  trend: [
    { time: ago(5), value: 11000, delta_value: 0, sample_interval_seconds: 0, per_second: null, peak_per_second: null },
    { time: ago(0), value: 11641, delta_value: 66, sample_interval_seconds: 300, per_second: 0.22, peak_per_second: 0.22 },
  ],
  discontinuities: [],
};
const GAUGE_TREND = {
  server: "SRV1",
  counter_name: "Total Server Memory (KB)",
  cntr_type: 65792,
  counter_kind: "gauge",
  trend: [{ time: ago(0), value: 8000000, delta_value: null, sample_interval_seconds: null, peak_value: 8000000 }],
  discontinuities: [],
};

// The deadlock counter over a day of 5-minute points: idle, idle, then one deadlock in 300 s (per_second 0.0033).
const LOW_RATE_TREND = {
  server: "SRV1",
  counter_name: "Number of Deadlocks/sec",
  cntr_type: 272696576,
  counter_kind: "rate",
  trend: [
    { time: ago(15), value: 36, delta_value: 0, sample_interval_seconds: 300, per_second: 0, peak_per_second: 0 },
    { time: ago(10), value: 36, delta_value: 0, sample_interval_seconds: 300, per_second: 0, peak_per_second: 0 },
    { time: ago(5), value: 37, delta_value: 1, sample_interval_seconds: 300, per_second: 0.0033, peak_per_second: 0.0033 },
  ],
  discontinuities: [],
};

// Page Splits/sec over its last two collections: a burst of 1,111,111 in 900 s (1234.5678 a second) took its total from 2,030 to
// 1,113,141, then 70 in 300 s (0.2333 a second) took it to 1,113,211, the total its grid row shows.
const DIGITS_TREND = {
  server: "SRV1",
  counter_name: "Page Splits/sec",
  cntr_type: 272696576,
  counter_kind: "rate",
  trend: [
    { time: ago(5), value: 1113141, delta_value: 1111111, sample_interval_seconds: 900, per_second: 1234.5678, peak_per_second: 1234.5678 },
    { time: ago(0), value: 1113211, delta_value: 70, sample_interval_seconds: 300, per_second: 0.2333, peak_per_second: 0.2333 },
  ],
  discontinuities: [],
};

// The same counter over a week in day-wide buckets: one deadlock in 86,400 s is 0.000012 a second.
const LONG_BUCKET_TREND = {
  server: "SRV1",
  counter_name: "Number of Deadlocks/sec",
  cntr_type: 272696576,
  counter_kind: "rate",
  trend: [
    { time: ago(3 * 1440), value: 36, delta_value: 0, sample_interval_seconds: 86400, per_second: 0, peak_per_second: 0 },
    { time: ago(2 * 1440), value: 36, delta_value: 0, sample_interval_seconds: 86400, per_second: 0, peak_per_second: 0 },
    { time: ago(1440), value: 37, delta_value: 1, sample_interval_seconds: 86400, per_second: 0.000012, peak_per_second: 0.0033 },
  ],
  discontinuities: [],
};

const scenarios = {
  // Every kind of row in one snapshot; the picker opens on the first name, Batch Requests/sec, a rate.
  mixed: () => {
    answer = (url) => (tool(url) === "get_perfmon_stats" ? stats([MEMORY, BATCHES, AVERAGE, COMPILES, DEADLOCKS]) : data(RATE_TREND));
    return modules.tabs.perfmonPanel("SRV1", { hours: 24, label: "last 24 hours" });
  },
  // A snapshot of one gauge: its grid row and its chart are what they were.
  gauge: () => {
    answer = (url) => (tool(url) === "get_perfmon_stats" ? stats([MEMORY]) : data(GAUGE_TREND));
    return modules.tabs.perfmonPanel("SRV1", { hours: 24, label: "last 24 hours" });
  },
  // One rate that seldom fires: its chart's axis and tooltip must not read 0 for it.
  lowrate: () => {
    answer = (url) => (tool(url) === "get_perfmon_stats" ? stats([DEADLOCKS]) : data(LOW_RATE_TREND));
    return modules.tabs.perfmonPanel("SRV1", { hours: 24, label: "last 24 hours" });
  },
  // The same rate over a long range, where one bucket spans a day.
  longbucket: () => {
    answer = (url) => (tool(url) === "get_perfmon_stats" ? stats([DEADLOCKS]) : data(LONG_BUCKET_TREND));
    return modules.tabs.perfmonPanel("SRV1", { hours: 168, label: "last 7 days" });
  },
  // Two rates with more digits than the rate format keeps: the grid cells, and the chart's tooltip at each end. The picker opens on
  // the first name, Page Splits/sec, the counter DIGITS_TREND is for.
  digits: () => {
    answer = (url) => (tool(url) === "get_perfmon_stats" ? stats([TRANSACTIONS, PAGE_SPLITS]) : data(DIGITS_TREND));
    return modules.tabs.perfmonPanel("SRV1", { hours: 24, label: "last 24 hours" });
  },
};

const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);

const root = new FakeNode("main");
modules.util.mount(root, chosen());
// Let both reads settle (each one is a few promise hops).
for (let i = 0; i < 200; i++) await new Promise((r) => setTimeout(r, 0));

const all = (node, tag, found = []) => {
  if (!node || typeof node !== "object") return found;
  if (node.tag === tag) found.push(node);
  node.children.forEach((c) => all(c, tag, found));
  return found;
};
const table = all(root, "table")[0] || null;

/* What a chart's tooltip says with the pointer at its left edge and at its right edge: the nearest point's rows, as
   [label, value] pairs. The chart registers one mousemove listener, on its overlay. */
const withListener = (node, type) => {
  if (!node || typeof node !== "object") return null;
  if (node.listeners && node.listeners[type]) return node;
  for (const child of node.children) {
    const found = withListener(child, type);
    if (found) return found;
  }
  return null;
};
const tooltipAt = (chart, clientX) => {
  const overlay = withListener(chart.node, "mousemove");
  if (!overlay) return null;
  overlay.listeners.mousemove[0]({ clientX });
  const tip = all(chart.node, "div").find((n) => n.className === "chart-tooltip");
  return all(tip, "div")
    .filter((n) => n.className === "t-row")
    .map((row) => {
      const spans = all(row, "span");
      return [spans[1].textContent, spans[2].textContent];
    });
};
const charts = modules.charts.chartCalls.map((chart) => ({
  series: chart.series,
  unit: chart.unit,
  axis: chart.axis,
  tooltips: { left: tooltipAt(chart, 0), right: tooltipAt(chart, 1000) },
}));

console.log(JSON.stringify({
  headers: table ? all(table, "th").map((th) => th.textContent) : [],
  numericHeaders: table ? all(table, "th").map((th) => String(th.className).split(" ").includes("num")) : [],
  rows: table ? all(table, "tr").map((tr) => all(tr, "td").map((td) => td.textContent)).filter((cells) => cells.length) : [],
  charts,
  errors: all(root, "div").filter((n) => n.className === "strip error").map((n) => n.textContent),
  fetches,
  rejections,
}));
