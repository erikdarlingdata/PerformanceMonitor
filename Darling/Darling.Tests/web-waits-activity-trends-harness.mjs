/* Runs the web server page's get_server_trend panels (namedTrendPanel, drawNamedTrends and sessionStatsTrendPanel in
   wwwroot/js/pages/server-tabs.js, with multi-picker.js, util.js, panels.js and charts.js) against a scripted /api/read
   answer and prints the reads they sent, the picker's state and the charts they drew, as one line of JSON.
   WebWaitsActivityTrendsBehaviourTests starts it as
       node web-waits-activity-trends-harness.mjs <path to wwwroot/js> <scenario>
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
  fs.mkdirSync(path.join(scratch, "pages"));
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  for (const f of ["util.js", "panels.js", "read-fields.js"]) fs.copyFileSync(path.join(jsDir, f), path.join(scratch, f));
  for (const rel of ["grid-tools.js", "multi-picker.js", path.join("pages", "analysis-findings.js"), path.join("pages", "plan-viewer.js"), path.join("pages", "pg-plan-viewer.js"), path.join("pages", "query-store-history.js")]) {
    const from = path.join(jsDir, rel);
    if (!fs.existsSync(from)) continue;
    fs.mkdirSync(path.dirname(path.join(scratch, rel)), { recursive: true });
    fs.copyFileSync(from, path.join(scratch, rel));
  }
  /* #5246: the Graph cell of the Deadlock Graphs grid. */
  fs.mkdirSync(path.join(scratch, "pages"), { recursive: true });
  fs.copyFileSync(path.join(jsDir, "pages", "deadlock-graph.js"), path.join(scratch, "pages", "deadlock-graph.js"));
  fs.copyFileSync(path.join(jsDir, "pages", "server-tabs.js"), path.join(scratch, "pages", "server-tabs.js"));
  fs.copyFileSync(path.join(jsDir, "charts.js"), path.join(scratch, "charts-real.js"));
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
const LATCHES = ["LATCH_A", "LATCH_B", "LATCH_C", "LATCH_D", "LATCH_E", "LATCH_F", "LATCH_G"];
const SPINS = ["SPIN_A", "SPIN_B", "SPIN_C"];
const pts = (f) => [{ time: ago(10), ...f(1) }, { time: ago(5), ...f(2) }];
let mode = "data";
let missing = null;
answer = (url) => {
  const t = tool(url);
  if (t === "get_latch_stats") return mode === "noOptions" ? data({ status: "empty", message: "No latch waits." }) : data({ latches: LATCHES.map((n) => ({ latch_class: n })) });
  if (t === "get_spinlock_stats") return data({ spinlocks: SPINS.map((n) => ({ spinlock_name: n })) });
  if (t !== "get_server_trend") return data({});
  const metric = url.searchParams.get("metric");
  const discontinuities = [{ at: ago(7), reason: "restart", detail: "uptime reset" }];
  if (mode === "allMissing") return data({ status: "empty", message: "None of the named were recorded.", hints: { missing_names: (url.searchParams.get("names") || "").split(",").filter(Boolean) } });
  if (mode === "empty") return data({ status: "empty", message: "No " + metric + " recorded in the last 24 hour(s)." });
  if (metric === "session_stats") {
    return data({ server: "SRV1", metric, trend: pts((k) => ({ total_sessions: 10 * k, running_sessions: k, sleeping_sessions: 5, background_sessions: 2, dormant_sessions: 1, idle_sessions_over_30min: 0, sessions_waiting_for_memory: 0, databases_with_connections: 3, top_application_name: "AppOne", top_application_connections: 4, top_host_name: "HostOne", top_host_connections: 6 })), aggregate_note: "Session note.", discontinuities });
  }
  const named = (url.searchParams.get("names") || "").split(",").filter(Boolean);
  const present = named.filter((c) => c !== missing);
  const key = metric === "latch" ? "latch_class" : "spinlock_name";
  const field = metric === "latch" ? "wait_time_ms_per_second" : "collisions_per_second";
  const body = { server: "SRV1", metric, series: present.map((c) => ({ [key]: c, trend: pts((k) => ({ [field]: 5 * k })) })), discontinuities };
  if (missing && named.includes(missing)) body.missing_names = [missing];
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
const kinds = {
  latch: (s) => modules.tabs.namedTrendPanel(s || "SRV1", ctx, "latch"),
  spinlock: (s) => modules.tabs.namedTrendPanel(s || "SRV1", ctx, "spinlock"),
  session: (s) => modules.tabs.sessionStatsTrendPanel(s || "SRV1", ctx),
};
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

const scenarios = {
  latch: async () => {
    const root = await build("latch");
    return { reads: trendReads(), checked: checkedNames(root), listed: boxes(root).length, chart: chartInfo(), notices: texts(root, "notice") };
  },
  spinlock: async () => {
    const root = await build("spinlock");
    return { reads: trendReads(), checked: checkedNames(root), chart: chartInfo(), notices: texts(root, "notice") };
  },
  session: async () => {
    const root = await build("session");
    return { reads: trendReads(), chart: chartInfo(), notices: texts(root, "notice"), notes: texts(root, "mp-metric-note") };
  },
  survives: async () => {
    const first = await build("latch");
    await toggle(boxes(first).find((b) => b.attrs["aria-label"] === "LATCH_B"), false);
    await toggle(boxes(first).find((b) => b.attrs["aria-label"] === "LATCH_G"), true);
    const rebuilt = await build("latch");
    const other = await build("latch", "SRV2");
    const spin = await build("spinlock");
    return { rebuilt: checkedNames(rebuilt), other: checkedNames(other), spin: checkedNames(spin), lastRead: trendReads()[trendReads().length - 3], chart: chartInfo() };
  },
  missing: async () => {
    missing = "LATCH_C";
    const root = await build("latch");
    return { notices: texts(root, "notice"), chart: chartInfo() };
  },
  allMissing: async () => {
    mode = "allMissing";
    const root = await build("latch");
    return { notices: texts(root, "notice"), empties: texts(root, "strip empty"), chart: chartInfo() };
  },
  emptyLatch: async () => {
    mode = "empty";
    const root = await build("latch");
    return { chart: chartInfo(), empties: texts(root, "strip empty") };
  },
  emptySession: async () => {
    mode = "empty";
    const root = await build("session");
    return { chart: chartInfo(), empties: texts(root, "strip empty") };
  },
  noOptions: async () => {
    mode = "noOptions";
    const root = await build("latch");
    return { reads: trendReads(), empties: texts(root, "strip empty") };
  },
};

const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);
const result = await chosen();
console.log(JSON.stringify({ ...result, rejections }));
