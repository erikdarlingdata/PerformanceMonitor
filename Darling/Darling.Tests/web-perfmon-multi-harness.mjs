/* Runs the web viewer's Perfmon panel (perfmonPanel in wwwroot/js/pages/server-tabs.js, with multi-picker.js, util.js,
   panels.js and charts.js) against a scripted /api/read answer, clicks its multi-select picker the way a reader would,
   and prints what the picker held, the reads it sent and the chart it drew, as one line of JSON.
   WebPerfmonMultiSelectBehaviourTests starts it as
       node web-perfmon-multi-harness.mjs <path to wwwroot/js> <scenario>
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
let hold = null; /* when set, get_perfmon_trend reads wait for it (a promise) or for their signal's abort */
let answer = () => ({ status: 200, body: {} });
globalThis.fetch = async (url, init) => {
  fetches.push(String(url));
  if (hold && String(url).includes("get_perfmon_trend")) {
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


const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "perfmon-multi-"));
let modules;
try {
  fs.mkdirSync(path.join(scratch, "pages"));
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  for (const f of ["util.js", "panels.js", "read-fields.js"]) fs.copyFileSync(path.join(jsDir, f), path.join(scratch, f));
  for (const rel of ["grid-tools.js", "multi-picker.js", path.join("pages", "analysis-findings.js"), path.join("pages", "plan-viewer.js"), path.join("pages", "query-store-history.js")]) {
    const from = path.join(jsDir, rel);
    if (!fs.existsSync(from)) continue;
    fs.mkdirSync(path.dirname(path.join(scratch, rel)), { recursive: true });
    fs.copyFileSync(from, path.join(scratch, rel));
  }
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

const DEFAULTS = ["Batch Requests/sec", "SQL Compilations/sec", "SQL Re-Compilations/sec", "Query optimizations/sec", "Network IO waits"];
const NAMES = [...DEFAULTS, "Total Server Memory (KB)", "Lock Waits/sec", "Page Splits/sec", "Memory Grants Pending",
  "C09", "C10", "C11", "C12", "C13", "C14"];
const GAUGES = new Set(["Total Server Memory (KB)", "Memory Grants Pending"]);
const statsFor = (names) => data({ server: "SRV1", captured_at: "2026-01-01T00:05:00.0000000",
  counters: names.map((n, i) => GAUGES.has(n)
    ? { counter_name: n, instance_name: "", value: 100 + i, delta_value: null, counter_kind: "gauge" }
    : { counter_name: n, instance_name: "", value: 1000 + i, delta_value: 5, counter_kind: "rate", per_second: 0.5 + i }) });
const trendFor = (name) => data({
  server: "SRV1", counter_name: name,
  trend: GAUGES.has(name)
    ? [{ time: ago(10), value: 100, delta_value: null }, { time: ago(5), value: 120, delta_value: null }]
    : [{ time: ago(10), value: 1000, delta_value: 0, per_second: 1 }, { time: ago(5), value: 1100, delta_value: 100, per_second: 2 }],
  discontinuities: [],
});
let failing = new Set();
let emptyTrends = false;
let hintedEmpty = false;
let statNames = NAMES;
answer = (url) => {
  const t = tool(url);
  if (t === "get_perfmon_stats") return statsFor(statNames);
  if (t === "get_perfmon_trend") {
    const w = url.searchParams.get("counter_name");
    if (hintedEmpty && w === "SQL Compilations/sec") return data({ status: "no_data", message: "No trend data for counter '" + w + "'. It may not be a counter this server collects \u2014 see hints.collected_counters for the 2 that are.", hints: { collected_counters: ["Alpha/sec", "Beta pages"] } });
    if (emptyTrends) return data({ status: "no_data", message: "No trend rows for " + w + "." });
    return failing.has(w) ? { status: 500, body: { error: "boom" } } : trendFor(w);
  }
  return data({});
};

const all = (node, pred, found = []) => {
  if (!node || typeof node !== "object") return found;
  if (pred(node)) found.push(node);
  (node.children || []).forEach((c) => all(c, pred, found));
  return found;
};
const settle = async () => { for (let i = 0; i < 100; i++) await new Promise((r) => setTimeout(r, 0)); };
const ctx = { hours: 24, label: "last 24 hours" };

async function build(server) {
  const root = new FakeNode("main");
  modules.util.mount(root, modules.tabs.perfmonPanel(server, ctx));
  await settle();
  return root;
}
const boxes = (root) => all(root, (n) => n.tag === "input" && n.attrs.type === "checkbox");
const checkedNames = (root) => boxes(root).filter((b) => b.checked).map((b) => b.attrs["aria-label"]);
const listed = (root) => boxes(root).map((b) => b.attrs["aria-label"]);
const button = (root, text) => all(root, (n) => n.tag === "button" && n.textContent === text)[0];
const click = async (node) => { node.listeners.click.forEach((l) => l({})); await settle(); };
const toggle = async (box, on) => { box.checked = on; box.listeners.change.forEach((l) => l({})); await settle(); };
const typeSearch = async (root, text) => {
  const s = all(root, (n) => n.tag === "input" && n.attrs.type === "search")[0];
  s.value = text;
  s.listeners.input.forEach((l) => l({}));
  await settle();
};
const trendReads = () => fetches.filter((f) => f.includes("get_perfmon_trend")).map((f) => new URL(f, "http://x").searchParams.get("counter_name"));
const lastChart = () => modules.charts.chartCalls[modules.charts.chartCalls.length - 1] || null;
const chartInfo = () => {
  const c = lastChart();
  return c && { labels: c.spec.series.map((s) => s.label), points: c.spec.points.length, id: c.id, unit: c.spec.unit == null ? null : c.spec.unit };
};
const notes = (root) => all(root, (n) => String(n.className).includes("notice")).map((n) => n.textContent);

const scenarios = {
  allEmpty: async () => {
    emptyTrends = true;
    const root = await build("SRV1");
    return { chart: chartInfo(), errors: all(root, (n) => n.className === "strip error").map((n) => n.textContent), empties: all(root, (n) => n.className === "strip empty").map((n) => n.textContent) };
  },
  hintedEmpty: async () => {
    hintedEmpty = true;
    const root = await build("SRV1");
    return { chart: chartInfo(), notes: notes(root), errors: all(root, (n) => n.className === "strip error").map((n) => n.textContent) };
  },
  fastClicks: async () => {
    const root = await build("SRV1");
    await click(button(root, "Clear All"));
    let release;
    hold = new Promise((r) => (release = r));
    fetches.length = 0;
    maxInFlight = 0;
    aborted = 0;
    for (const name of ["Select All", "Default counters", "Select All", "Default counters"]) {
      button(root, name).listeners.click.forEach((l) => l({}));
      await new Promise((r) => setTimeout(r, 0));
    }
    const stillInFlight = inFlight;
    const sent = trendReads().length;
    release();
    await settle();
    return { sent, maxInFlight, stillInFlight, aborted, chart: chartInfo() };
  },
  focusKept: async () => {
    const first = await build("SRV1");
    const search = all(first, (n) => n.tag === "input" && n.attrs.type === "search")[0];
    search.value = "Batch";
    search.listeners.input.forEach((l) => l({}));
    search.selectionStart = 1;
    search.selectionEnd = 2;
    search.focus();
    const rebuilt = await build("SRV1");
    const next = all(rebuilt, (n) => n.tag === "input" && n.attrs.type === "search")[0];
    return { sameNode: next === search, focused: globalThis.document.activeElement === next, start: next.selectionStart, end: next.selectionEnd, value: next.value };
  },
  noFocusStolen: async () => {
    await build("SRV1");
    globalThis.document.activeElement = null;
    const rebuilt = await build("SRV1");
    const next = all(rebuilt, (n) => n.tag === "input" && n.attrs.type === "search")[0];
    return { focused: globalThis.document.activeElement === next };
  },
  capInDraw: async () => {
    await build("SRV1");
    fetches.length = 0;
    const slot = new FakeNode("div");
    await modules.tabs.drawPerfmonTrends(slot, "SRV9", ctx, NAMES);
    return { reads: trendReads().length };
  },
  droppedOut: async () => {
    const first = await build("SRV1");
    await click(button(first, "Clear All"));
    await toggle(boxes(first).find((b) => b.attrs["aria-label"] === "C13"), true);
    statNames = NAMES.filter((n) => n !== "C13");
    fetches.length = 0;
    const rebuilt = await build("SRV1");
    const out = { checked: checkedNames(rebuilt), listedLast: listed(rebuilt).slice(-1), reads: trendReads() };
    statNames = NAMES;
    const back = await build("SRV1");
    return { ...out, checkedBack: checkedNames(back) };
  },
  defaults: async () => {
    const root = await build("SRV1");
    return { checked: checkedNames(root), reads: trendReads(), chart: chartInfo(), count: all(root, (n) => n.className === "mp-count")[0].textContent, selects: all(root, (n) => n.tag === "select").length };
  },
  noPackCounters: async () => {
    statNames = ["Zeta", "Alpha", "Mid"];
    const root = await build("SRV1");
    return { checked: checkedNames(root), reads: trendReads() };
  },
  checkUncheck: async () => {
    const root = await build("SRV1");
    const before = checkedNames(root);
    fetches.length = 0;
    await toggle(boxes(root).find((b) => b.attrs["aria-label"] === "Batch Requests/sec"), false);
    const afterUncheck = checkedNames(root);
    const readsAfterUncheck = trendReads();
    fetches.length = 0;
    await toggle(boxes(root).find((b) => b.attrs["aria-label"] === "Page Splits/sec"), true);
    return { before, afterUncheck, readsAfterUncheck, afterCheck: checkedNames(root), readsAfterCheck: trendReads(), chart: chartInfo() };
  },
  search: async () => {
    const root = await build("SRV1");
    await typeSearch(root, "lock");
    const found = listed(root);
    await typeSearch(root, "zzz");
    const none = listed(root);
    const noneText = all(root, (n) => n.className === "mp-none").map((n) => n.textContent);
    await typeSearch(root, "");
    return { found, none, noneText, all: listed(root).length };
  },
  cap: async () => {
    const root = await build("SRV1");
    await click(button(root, "Clear All"));
    const afterClear = checkedNames(root);
    await click(button(root, "Select All"));
    const disabled = boxes(root).filter((b) => b.disabled).length;
    const hint = all(root, (n) => n.className === "mp-hint").map((n) => n.textContent);
    const checked = checkedNames(root);
    await click(button(root, "Clear All"));
    const emptyChart = all(root, (n) => n.className === "strip empty").length;
    await click(button(root, "Default counters"));
    return { afterClear, checked, disabled, hint, emptyChart, top: checkedNames(root) };
  },
  survives: async () => {
    const first = await build("SRV1");
    await click(button(first, "Clear All"));
    await toggle(boxes(first).find((b) => b.attrs["aria-label"] === "Lock Waits/sec"), true);
    await typeSearch(first, "Lock");
    const rebuilt = await build("SRV1");
    const search = all(rebuilt, (n) => n.tag === "input" && n.attrs.type === "search")[0].value;
    const other = await build("SRV2");
    return { rebuilt: checkedNames(rebuilt), search, other: checkedNames(other) };
  },
  oneFailed: async () => {
    failing = new Set(["SQL Compilations/sec"]);
    const root = await build("SRV1");
    return { chart: chartInfo(), notes: notes(root), errors: all(root, (n) => n.className === "strip error").map((n) => n.textContent) };
  },
  allFailed: async () => {
    failing = new Set(NAMES);
    const root = await build("SRV1");
    return { chart: chartInfo(), notes: notes(root), errors: all(root, (n) => n.className === "strip error").map((n) => n.textContent) };
  },
  mixedKinds: async () => {
    const root = await build("SRV1");
    await toggle(boxes(root).find((b) => b.attrs["aria-label"] === "Total Server Memory (KB)"), true);
    return { chart: chartInfo(), rates: undefined };
  },
};

const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);
const result = await chosen();
console.log(JSON.stringify({ ...result, rejections }));
