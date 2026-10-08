/* Runs the web server page's get_server_trend panels (serverTrendPanel and memoryClerksTrendPanel in
   wwwroot/js/pages/server-tabs.js, with multi-picker.js, util.js, panels.js and charts.js) against a scripted /api/read
   answer and prints the reads they sent, the picker's state and the charts they drew, as one line of JSON.
   WebServerTrendsBehaviourTests starts it as
       node web-server-trends-harness.mjs <path to wwwroot/js> <scenario>
   charts.js is the shipped renderer behind a wrapper that records each chart's spec, id and scope. `fetch` and the DOM
   are stand-ins; everything else is the shipped code. */
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
  focus() {
    globalThis.document.activeElement = this;
  }
  setSelectionRange(start, end) {
    this.selectionStart = start;
    this.selectionEnd = end;
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
  activeElement: null,
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
};

const fetches = [];
let inFlight = 0;
let maxInFlight = 0;
let aborted = 0;
let hold = null; /* when set, get_wait_trend reads wait for it (a promise) or for their signal's abort */
let answer = () => ({ status: 200, body: {} });
const signals = [];
globalThis.fetch = async (url, init) => {
  fetches.push(String(url));
  signals.push(init && init.signal ? true : false);
  if (hold && String(url).includes("get_wait_trend")) {
    inFlight++;
    maxInFlight = Math.max(maxInFlight, inFlight);
    let live = true;
    const done = () => { if (live) { live = false; inFlight--; } };
    try {
      await new Promise((resolve, reject) => {
        const signal = init && init.signal;
        const onAbort = () => {
          aborted++;
          done();
          const e = new Error("aborted");
          e.name = "AbortError";
          reject(e);
        };
        if (signal && signal.aborted) return onAbort();
        if (signal) signal.addEventListener("abort", onAbort);
        hold.then(resolve);
      });
    } finally {
      done();
    }
  }
  const reply = answer(new URL(String(url), "http://viewer.test"));
  const raw = reply.body === undefined ? "" : JSON.stringify(reply.body);
  return { status: reply.status, ok: reply.status >= 200 && reply.status < 300, text: async () => raw };
};

const rejections = [];
process.on("unhandledRejection", (e) => rejections.push(String(e && e.stack ? e.stack : e)));


const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "server-trends-"));
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
    picker: await load("multi-picker.js"),
  };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const data = (b) => ({ status: 200, body: b });
const tool = (url) => url.pathname.replace("/api/read/", "");
const ago = (m) => new Date(Date.now() - m * 60000).toISOString().slice(0, 19);
const CLERKS = ["MEMORYCLERK_SQLBUFFERPOOL", "CACHESTORE_SQLCP", "CACHESTORE_OBJCP", "OBJECTSTORE_LOCK_MANAGER", "MEMORYCLERK_SQLQERESERVATIONS", "USERSTORE_TOKENPERM", "MEMORYCLERK_XE"];
const pts = (f) => [{ time: ago(10), ...f(1) }, { time: ago(5), ...f(2) }];
let mode = "data";
let missing = null;
answer = (url) => {
  const t = tool(url);
  if (t === "get_memory_clerks") return mode === "noclerks" ? data({ status: "empty", message: "No snapshot." }) : data({ server: "SRV1", clerks: CLERKS.map((c, i) => ({ clerk_type: c, memory_mb: 100 - i })) });
  if (t === "get_file_io_trend") {
    if (mode === "ioempty") return data({ status: "empty", message: "No file I/O recorded." });
    const f = (name, r, w) => [{ time: ago(10), database_name: name, file_type: "ROWS", file_name: null, avg_read_latency_ms: r, avg_write_latency_ms: w }, { time: ago(5), database_name: name, file_type: "ROWS", file_name: null, avg_read_latency_ms: r + 1, avg_write_latency_ms: w == null ? null : w + 1 }];
    const trend = mode === "ionowrite" ? [...f("db_a", 5, null)] : [...f("db_a", 5, 20), ...f("db_b", 9, 3)];
    return data({ server: "SRV1", trend, discontinuities: mode === "iogap" ? [{ at: ago(7), reason: "restart", detail: "uptime reset" }] : [] });
  }
  if (t !== "get_server_trend") return data({});
  const metric = url.searchParams.get("metric");
  if (mode === "allMissing") return data({ status: "empty", message: "None of the named clerk types were recorded. The window does hold other clerk types: heaviest_clerk_types lists the heaviest of them.", hints: { missing_clerk_types: (url.searchParams.get("clerk_types") || "").split(",").filter(Boolean), heaviest_clerk_types: ["CLERK_ALPHA", "CLERK_BETA"] } });
  if (mode === "empty") return data({ status: "empty", message: "No " + metric + " recorded in the last 24 hour(s)." });
  const discontinuities = [{ at: ago(7), reason: "restart", detail: "uptime reset" }];
  if (metric === "cpu_scheduler") return data({ server: "SRV1", metric, trend: pts((k) => ({ runnable_tasks: k, blocked_tasks: 0, queued_requests: k })), aggregate_note: "Each point averages.", discontinuities });
  if (metric === "plan_cache") return data({ server: "SRV1", metric, trend: pts((k) => ({ single_use_mb: 10 * k, multi_use_mb: 20 * k })), discontinuities: [] });
  const named = (url.searchParams.get("clerk_types") || "").split(",").filter(Boolean);
  const present = named.filter((c) => c !== missing);
  const body = { server: "SRV1", metric, series: present.map((c) => ({ clerk_type: c, trend: pts((k) => ({ memory_mb: 50 * k })) })), discontinuities };
  if (missing && named.includes(missing)) body.missing_clerk_types = [missing];
  return data(body);
};

const all = (node, pred, found = []) => {
  if (!node || typeof node !== "object") return found;
  if (pred(node)) found.push(node);
  (node.children || []).forEach((c) => all(c, pred, found));
  return found;
};
const settle = async () => { for (let i = 0; i < 100; i++) await new Promise((r) => setTimeout(r, 0)); };
const ctx = { hours: 24, label: "last 24 hours", signal: new AbortController().signal };
const kinds = { fileio: () => modules.tabs.fileIoPanel("SRV1", ctx),  cpu: () => modules.tabs.serverTrendPanel("SRV1", ctx, "cpu_scheduler"), plan: (s) => modules.tabs.serverTrendPanel(s || "SRV1", ctx, "plan_cache"), clerks: (s) => modules.tabs.memoryClerksTrendPanel(s || "SRV1", ctx) };
async function build(kind, server) {
  const root = new FakeNode("main");
  modules.util.mount(root, kinds[kind](server));
  await settle();
  return root;
}
const boxes = (root) => all(root, (n) => n.tag === "input" && n.attrs.type === "checkbox");
const checkedNames = (root) => boxes(root).filter((b) => b.checked).map((b) => b.attrs["aria-label"]);
const toggle = async (box, on) => { box.checked = on; box.listeners.change.forEach((l) => l({})); await settle(); };
const trendReads = () => fetches.filter((f) => f.includes("get_server_trend")).map((f) => Object.fromEntries(new URL(f, "http://x").searchParams));
const lastChart = () => modules.charts.chartCalls[modules.charts.chartCalls.length - 1] || null;
const chartInfo = () => {
  const c = lastChart();
  return c && { labels: c.spec.series.map((s) => s.label), points: c.spec.points.length, unit: c.spec.unit, id: c.id };
};
const texts = (root, cls) => all(root, (n) => String(n.className).includes(cls)).map((n) => n.textContent);

const allCharts = () => modules.charts.chartCalls.map((c) => ({ id: c.id, labels: c.spec.series.map((x) => x.label), unit: c.spec.unit, scope: c.scope, points: c.spec.points.length }));
const scenarios = {
  fileio: async () => {
    const root = await build("fileio");
    return { reads: fetches.filter((f) => f.includes("get_file_io_trend")), charts: allCharts(), notices: texts(root, "notice"), empties: texts(root, "strip empty") };
  },
  fileioGap: async () => {
    mode = "iogap";
    const root = await build("fileio");
    return { charts: allCharts(), notices: texts(root, "notice"), empties: texts(root, "strip empty") };
  },
  fileioNoWrites: async () => {
    mode = "ionowrite";
    const root = await build("fileio");
    return { charts: allCharts(), notices: texts(root, "notice"), empties: texts(root, "strip empty") };
  },
  fileioEmpty: async () => {
    mode = "ioempty";
    const root = await build("fileio");
    return { charts: allCharts(), empties: texts(root, "strip empty") };
  },

  cpu: async () => {
    const root = await build("cpu");
    return { reads: trendReads(), chart: chartInfo(), notices: texts(root, "notice"), notes: texts(root, "mp-metric-note"), signals };
  },
  plan: async () => {
    const root = await build("plan");
    return { reads: trendReads(), chart: chartInfo(), notices: texts(root, "notice") };
  },
  clerks: async () => {
    const root = await build("clerks");
    return { reads: trendReads(), snapshotReads: fetches.filter((f) => f.includes("get_memory_clerks")), checked: checkedNames(root), listed: boxes(root).length, chart: chartInfo(), notices: texts(root, "notice") };
  },
  survives: async () => {
    const first = await build("clerks");
    await toggle(boxes(first).find((b) => b.attrs["aria-label"] === "CACHESTORE_SQLCP"), false);
    await toggle(boxes(first).find((b) => b.attrs["aria-label"] === "MEMORYCLERK_XE"), true);
    const rebuilt = await build("clerks");
    const other = await build("clerks", "SRV2");
    return { rebuilt: checkedNames(rebuilt), other: checkedNames(other), lastRead: trendReads()[trendReads().length - 2], chart: chartInfo() };
  },
  missing: async () => {
    missing = "CACHESTORE_OBJCP";
    const root = await build("clerks");
    return { notices: texts(root, "notice"), chart: chartInfo() };
  },
  allMissing: async () => {
    mode = "allMissing";
    const root = await build("clerks");
    return { notices: texts(root, "notice"), empties: texts(root, "strip empty"), notes: texts(root, "mp-metric-note"), chart: chartInfo() };
  },
  clerkPicker: async () => {
    const root = await build("clerks");
    const buttons = () => all(root, (n) => n.tag === "button").map((b) => b.textContent);
    const before = checkedNames(root);
    for (const b of boxes(root).filter((x) => x.checked)) await toggle(b, false);
    const cleared = checkedNames(root);
    const top = all(root, (n) => n.tag === "button" && n.textContent === "Top clerks")[0];
    if (top) { top.listeners.click.forEach((l) => l({})); await settle(); }
    const afterTop = checkedNames(root);
    const search = all(root, (n) => n.tag === "input" && n.attrs.type === "search")[0];
    search.value = "zzz-no-such";
    search.listeners.input.forEach((l) => l({}));
    await settle();
    return { buttons: buttons(), before, cleared, afterTop, noMatch: texts(root, "mp-none") };
  },
  emptyCpu: async () => {
    mode = "empty";
    const root = await build("cpu");
    return { chart: chartInfo(), empties: texts(root, "strip empty") };
  },
  emptyClerks: async () => {
    mode = "empty";
    const root = await build("clerks");
    return { chart: chartInfo(), empties: texts(root, "strip empty") };
  },
  noClerks: async () => {
    mode = "noclerks";
    const root = await build("clerks");
    return { reads: trendReads(), empties: texts(root, "strip empty") };
  },
};

const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);
const result = await chosen();
console.log(JSON.stringify({ ...result, rejections }));
