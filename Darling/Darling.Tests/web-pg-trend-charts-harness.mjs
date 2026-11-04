/* Runs the PostgreSQL Activity, Waits and I/O tabs' trend sections (pgDatabaseTrendSpecs, pgQueryDurationTrendSpecs, pgWaitTrendSpecs and
   pgIoTrendSpecs, built by the tabs in wwwroot/js/pages/server-tabs.js, drawn by panels.js and charts.js) against a scripted /api/read
   answer and prints the charts they drew, as one line of JSON (#7394).
   WebPgTrendChartsBehaviourTests starts it as
       node web-pg-trend-charts-harness.mjs <path to wwwroot/js> <scenario>
   charts.js is the shipped renderer behind a wrapper that records each chart's spec. `fetch` and the DOM are stand-ins; everything else is
   the shipped code. */
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
  focus() {}
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
  activeElement: null,
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

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "pg-trend-charts-"));
let modules;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.copyFileSync(path.join(scratch, "charts.js"), path.join(scratch, "charts-real.js"));
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { zoomableLineChart as realZoomable } from "./charts-real.js";\n' +
      'export * from "./charts-real.js";\n' +
      "export const chartCalls = [];\n" +
      "export function zoomableLineChart(spec, id, scope) {\n" +
      "  chartCalls.push({ spec, id, scope });\n" +
      "  return realZoomable(spec, id, scope);\n" +
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

const data = (b) => ({ status: 200, body: b });
const tool = (url) => url.pathname.replace("/api/read/", "");
const ago = (m) => new Date(Date.now() - m * 60000).toISOString().slice(0, 19);
const all = (node, pred, found = []) => {
  if (!node || typeof node !== "object") return found;
  if (pred(node)) found.push(node);
  (node.children || []).forEach((c) => all(c, pred, found));
  return found;
};
const settle = async () => { for (let i = 0; i < 100; i++) await new Promise((r) => setTimeout(r, 0)); };
const ctx = { hours: 24, label: "last 24 hours", signal: new AbortController().signal };

/* Three buckets 10 minutes apart. `reset` marks the middle one as an interval across a statistics reset. */
const times = [ago(30), ago(20), ago(10)];
const dbPoints = (reset) => times.map((t, i) => ({ collection_time: t, cache_hit_pct: 99 - i, worst_cache_hit_pct: 90 - i, rollback_pct: 0.5, temp_files: i, temp_bytes: 1000 * i, deadlocks: 0, transactions_per_second: 10 + i, counter_reset: reset && i === 1 }));
const queryPoints = () => times.map((t, i) => ({ collection_time: t, calls: i === 1 ? 0 : 5, total_exec_ms: 10, mean_exec_ms: i === 1 ? null : 2 + i, peak_mean_exec_ms: i === 1 ? null : 4, calls_per_second: 0.5 }));
const waitPoints = (reset) => times.map((t, i) => ({ collection_time: t, sample_count: 3, estimated_wait_ms_per_second: 12 + i, backend_count: 2, counter_reset: reset && i === 2 }));
/* `timing` false is a server with track_io_timing off: avg_read_ms is null on every point, as the MCP read writes it. */
const ioPoints = (timing, reset) => times.map((t, i) => ({ collection_time: t, reads_per_second: 100 + i, peak_reads_per_second: 150, writes_per_second: null, evictions_per_second: 1, cache_hit_pct: 98, avg_read_ms: timing ? (i === 1 ? null : 0.4 + i) : null, read_bytes_per_second: null, counter_reset: reset && i === 0 }));

let bodies = {};
answer = (url) => data(bodies[tool(url)] || {});

const tab = (id) => modules.tabs.POSTGRES_TABS.find((t) => t.id === id);
async function build(id) {
  const root = new FakeNode("main");
  modules.util.mount(root, [].concat(tab(id).build("SRV1", ctx)));
  await settle();
  return root;
}
const calls = () => modules.charts.chartCalls;
const byTitle = (title) => calls().find((c) => c.spec.title === title) || null;
const describe = (c) => c && { labels: c.spec.series.map((s) => s.label), points: c.spec.points.length, resets: (c.spec.annotations || []).flatMap((a) => a.events).length, resetName: c.spec.annotations ? c.spec.annotations[0].displayName : null };
const titles = () => calls().map((c) => c.spec.title);
const texts = (root, cls) => all(root, (n) => String(n.className).includes(cls)).map((n) => n.textContent);
const hasAll = (key, title) => { const c = byTitle(title); return c ? c.spec.points.map((p) => p[key]) : null; };

const scenarios = {
  database: async () => {
    bodies = { get_pg_database_trend: { points: dbPoints(true) } };
    const root = await build("activity");
    return {
      titles: titles().filter((t) => t.startsWith("Database")),
      percent: describe(byTitle("Database Trend")),
      xact: describe(byTitle("Database Transactions per Second")),
      bytes: describe(byTitle("Database Temp Bytes")),
      counts: describe(byTitle("Database Temp Files and Deadlocks")),
      keys: texts(root, "annotation-legend"),
      tables: all(root, (n) => n.tag === "table").length,
    };
  },
  databaseNoReset: async () => {
    bodies = { get_pg_database_trend: { points: dbPoints(false) } };
    const root = await build("activity");
    return { percent: describe(byTitle("Database Trend")), keys: texts(root, "annotation-legend") };
  },
  query: async () => {
    bodies = { get_pg_query_duration_trend: { points: queryPoints() } };
    await build("activity");
    return { duration: describe(byTitle("Query Duration Trend")), calls: describe(byTitle("Statement Calls per Second")), mean: hasAll("mean_exec_ms", "Query Duration Trend") };
  },
  wait: async () => {
    bodies = { get_pg_wait_trend: { points: waitPoints(true) } };
    const root = await build("waits");
    return { wait: describe(byTitle("Wait Trend")), keys: texts(root, "annotation-legend") };
  },
  io: async () => {
    bodies = { get_pg_io_trend: { points: ioPoints(true, true) } };
    const root = await build("io");
    return {
      ops: describe(byTitle("I/O Trend")),
      hit: describe(byTitle("I/O Hit %")),
      read: describe(byTitle("I/O Avg Read (ms)")),
      readValues: hasAll("avg_read_ms", "I/O Avg Read (ms)"),
      bytes: describe(byTitle("I/O Read Bytes per Second")),
      keys: texts(root, "annotation-legend"),
      empties: texts(root, "strip empty"),
    };
  },
  ioTimingOff: async () => {
    bodies = { get_pg_io_trend: { points: ioPoints(false, false) } };
    const root = await build("io");
    return { titles: titles(), empties: texts(root, "strip empty") };
  },
  empty: async () => {
    bodies = {
      get_pg_database_trend: { status: "empty", message: "No database trend." },
      get_pg_query_duration_trend: { status: "empty", message: "No query trend." },
      get_pg_wait_trend: { status: "empty", message: "No wait trend." },
      get_pg_io_trend: { status: "empty", message: "No io trend." },
    };
    const out = {};
    for (const id of ["activity", "waits", "io"]) {
      const root = await build(id);
      out[id] = texts(root, "strip empty").filter((t) => / trend.$/.test(t));
    }
    return { charts: calls().length, ...out };
  },
};

const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);
const result = await chosen();
console.log(JSON.stringify({ ...result, rejections }));
