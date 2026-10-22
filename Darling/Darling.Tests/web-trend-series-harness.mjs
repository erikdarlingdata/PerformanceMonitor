/* #6296, #6297, #6298: runs the server page's I/O Trends tab (the throughput chart, the tempdb allocated size and the tempdb file I/O latency)
   and its Collection Health tab (the collector duration chart), in wwwroot/js/pages/server-tabs.js with the shipped panels.js, util.js and
   charts.js, against a scripted /api/read answer for get_server_trend, and prints as one line of JSON what each panel drew: its charts' titles
   and legend labels, its strips and its notes. WebTrendSeriesBehaviourTests starts it as
       node web-trend-series-harness.mjs <path to wwwroot/js> <scenario>
   with <scenario> data (every metric answers series or points), empty (every metric answers the unavailable {status, message} envelope) or
   quiet (every metric answers the empty envelope the service words as "in the last 48 hours", and the page draws a custom past window of 48 hours). The whole js
   tree is copied into a scratch folder first (#5279). Only the DOM and fetch are stand-ins. */
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
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = {
  body: new FakeNode("body"),
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeText(t),
};
globalThis.location = { hash: "#/server/a/io" };

const T = (m) => "2026-10-21T" + String(10 + Math.floor(m / 60)).padStart(2, "0") + ":" + String(m % 60).padStart(2, "0") + ":00";
const ENVELOPE = (message) => ({ status: "unavailable", message });
const QUIET = (metric) => ({ status: "empty", message: "No " + metric + " rows recorded for sql2019 in the last 48 hours. This server HAS collected them before." });
const DATA = {
  file_io_throughput: {
    bucket: "15 minutes", bucket_minutes: 15, aggregate_note: "Each point summarizes one 15-minute bucket; peak_* is the most extreme collection; pass bucket_minutes (1-1440) for another width, or narrow hours_back.",
    series: [
      { file_label: "SalesDb.sales_data", trend: [{ time: T(0), read_mb_per_sec: 12.5, write_mb_per_sec: 3 }, { time: T(15), read_mb_per_sec: 14, write_mb_per_sec: null }] },
      { file_label: "SalesDb.sales_log", trend: [{ time: T(0), read_mb_per_sec: 0, write_mb_per_sec: 8 }, { time: T(15), read_mb_per_sec: 0, write_mb_per_sec: 9 }] },
    ],
  },
  tempdb_size: { trend: [{ time: T(0), allocated_mb: 800 }, { time: T(15), allocated_mb: 1024 }] },
  tempdb_file_io: {
    series: [
      { file_name: "tempdev", trend: [{ time: T(0), avg_read_latency_ms: 4, avg_write_latency_ms: 6 }, { time: T(15), avg_read_latency_ms: null, avg_write_latency_ms: 2 }] },
      { file_name: "temp2", trend: [{ time: T(0), avg_read_latency_ms: null, avg_write_latency_ms: null }] },
    ],
  },
  collector_duration: {
    series: [
      { collector_name: "wait_stats", trend: [{ time: T(0), max_duration_ms: 900, avg_duration_ms: 400, run_count: 5 }, { time: T(15), max_duration_ms: 700, avg_duration_ms: 300, run_count: 5 }] },
      { collector_name: "once_only", trend: [{ time: T(0), max_duration_ms: 50, avg_duration_ms: 50, run_count: 1 }] },
    ],
  },
};
const EMPTY = {
  file_io_throughput: "THROUGHPUT EMPTY SENTENCE",
  tempdb_size: "TEMPDB SIZE EMPTY SENTENCE",
  tempdb_file_io: "TEMPDB FILE IO EMPTY SENTENCE",
  collector_duration: "COLLECTOR EMPTY SENTENCE",
};
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const name = u.pathname.replace("/api/read/", "");
  const metric = u.searchParams.get("metric");
  let body = {};
  if (name === "get_server_trend" && DATA[metric]) body = scenario === "empty" ? ENVELOPE(EMPTY[metric]) : scenario === "quiet" ? QUIET(metric) : DATA[metric];
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "trend-series-"));
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
  const ctx = scenario === "quiet" ? { hours: 48, label: "custom: 10/19 10:00 to 10/21 10:00", custom: true } : { hours: 24, label: "last 24 hours" };
  modules.util.mount(root, tab.build("a", ctx));
  await flush();
  return root;
};
const cardOf = (root, title) =>
  all(root).find((n) => n.className && /\bpanel\b/.test(n.className) && all(n).some((c) => c.tag === "h3" && c.textContent.startsWith(title))) || null;
const has = (n, cls) => !!n.className && new RegExp("\\b" + cls + "\\b").test(n.className);
/* What a card drew: each chart's legend labels, each h4, every strip's text and every note's text. */
const report = (root, title) => {
  const card = cardOf(root, title);
  if (!card) return { found: false };
  const nodes = all(card);
  return {
    found: true,
    headings: nodes.filter((n) => n.tag === "h4").map((n) => n.textContent),
    /* A chart is its legend's labels and the text of its value axis (a tick carries the unit: "10 ms"), which shows what was plotted. */
    charts: nodes.filter((n) => has(n, "chart-legend")).map((c) => ({
      labels: all(c).filter((n) => has(n, "item")).map((n) => n.textContent),
      ticks: all(c.parent).filter((n) => n.tag === "text" && /\d (ms|MB|MB\/s)$/.test(n.textContent)).map((n) => n.textContent),
    })),
    strips: nodes.filter((n) => has(n, "strip")).map((n) => n.textContent),
    notes: nodes.filter((n) => has(n, "mp-metric-note")).map((n) => n.textContent),
  };
};

const io = await draw("iotrends");
const health = await draw("health");
console.log(JSON.stringify({
  throughput: report(io, "File I/O Throughput"),
  tempdbSize: report(io, "tempdb Allocated Size"),
  tempdbFileIo: report(io, "tempdb File I/O Latency"),
  collectors: report(health, "Collector Duration Trends"),
}));
