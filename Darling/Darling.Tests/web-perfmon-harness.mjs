/* Runs the web viewer's Perfmon panel (perfmonPanel in wwwroot/js/pages/server-tabs.js, with util.js and panels.js)
   against a scripted /api/read answer and prints the grid it drew and the chart lines it asked for, as one line of
   JSON. WebPerfmonPerSecondBehaviourTests starts it as
       node web-perfmon-harness.mjs <path to wwwroot/js> <scenario>
   The modules are copied into a scratch folder beside a recording stand-in for charts.js (the SVG renderer, which
   needs a real browser), then imported. `fetch` and the DOM are stand-ins: a node tree of plain objects, and a fetch
   that answers each URL from the scenario. Everything else is the shipped code. */
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
  addEventListener() {}
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

/* The modules, unchanged, with charts.js replaced by a stand-in that records the lines and the unit each chart was
   asked to draw. */
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "perfmon-grid-"));
let modules;
try {
  fs.mkdirSync(path.join(scratch, "pages"));
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.copyFileSync(path.join(jsDir, "util.js"), path.join(scratch, "util.js"));
  fs.copyFileSync(path.join(jsDir, "panels.js"), path.join(scratch, "panels.js"));
  fs.copyFileSync(path.join(jsDir, "pages", "server-tabs.js"), path.join(scratch, "pages", "server-tabs.js"));
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\n' +
      "export const SERIES_COLORS = ['#111', '#222', '#333', '#444', '#555', '#666'];\n" +
      "export const chartCalls = [];\n" +
      "export function normalizeColor(color) { return color; }\n" +
      "export function renderLineChart(opts) {\n" +
      "  chartCalls.push({ series: (opts.series || []).map((s) => ({ key: s.key, label: s.label })), unit: opts.unit == null ? null : opts.unit });\n" +
      "  return el('div', { class: 'chart-stub' });\n" +
      "}\n"
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
const MEMORY = { counter_name: "Total Server Memory (KB)", instance_name: "", value: 8000000, delta_value: null, cntr_type: 65792, counter_kind: "gauge" };
const AVERAGE = { counter_name: "Lock waits", instance_name: "Average wait time (ms)", value: 5000, delta_value: 40, cntr_type: 1073874176, counter_kind: "other" };

const stats = (counters) => data({ server: "SRV1", captured_at: "2026-01-01T00:05:00.0000000", counters });

const RATE_TREND = {
  server: "SRV1",
  counter_name: "Batch Requests/sec",
  cntr_type: 272696576,
  counter_kind: "rate",
  trend: [
    { time: "2026-01-01T00:00:00", value: 11000, delta_value: 0, sample_interval_seconds: 0, per_second: null, peak_per_second: null },
    { time: "2026-01-01T00:05:00", value: 11641, delta_value: 66, sample_interval_seconds: 300, per_second: 0.22, peak_per_second: 0.22 },
  ],
  discontinuities: [],
};
const GAUGE_TREND = {
  server: "SRV1",
  counter_name: "Total Server Memory (KB)",
  cntr_type: 65792,
  counter_kind: "gauge",
  trend: [{ time: "2026-01-01T00:05:00", value: 8000000, delta_value: null, sample_interval_seconds: null, peak_value: 8000000 }],
  discontinuities: [],
};

const scenarios = {
  // Every kind of row in one snapshot; the picker opens on the first name, Batch Requests/sec, a rate.
  mixed: () => {
    answer = (url) => (tool(url) === "get_perfmon_stats" ? stats([MEMORY, BATCHES, AVERAGE, COMPILES]) : data(RATE_TREND));
    return modules.tabs.perfmonPanel("SRV1", { hours: 24, label: "last 24 hours" });
  },
  // A snapshot of one gauge: its grid row and its chart are what they were.
  gauge: () => {
    answer = (url) => (tool(url) === "get_perfmon_stats" ? stats([MEMORY]) : data(GAUGE_TREND));
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

console.log(JSON.stringify({
  headers: table ? all(table, "th").map((th) => th.textContent) : [],
  rows: table ? all(table, "tr").map((tr) => all(tr, "td").map((td) => td.textContent)).filter((cells) => cells.length) : [],
  charts: modules.charts.chartCalls,
  errors: all(root, "div").filter((n) => n.className === "strip error").map((n) => n.textContent),
  fetches,
  rejections,
}));
