/* Runs the web viewer's Wait Stats panel (waitsPanel in wwwroot/js/pages/server-tabs.js, with multi-picker.js, util.js,
   panels.js and charts.js) against a scripted /api/read answer, clicks its multi-select picker the way a reader would,
   and prints what the picker held, the reads it sent and the chart it drew, as one line of JSON.
   WebWaitStatsMultiSelectBehaviourTests starts it as
       node web-waits-harness.mjs <path to wwwroot/js> <scenario>
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


const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "waits-multi-"));
let modules;
try {
  fs.mkdirSync(path.join(scratch, "pages"));
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  for (const f of ["util.js", "panels.js", "read-fields.js", "multi-picker.js"]) fs.copyFileSync(path.join(jsDir, f), path.join(scratch, f));
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

const NAMES = ["CXPACKET", "PAGEIOLATCH_SH", "LCK_M_X", "WRITELOG", "ASYNC_NETWORK_IO", "SOS_SCHEDULER_YIELD", "THREADPOOL",
  "W08", "W09", "W10", "W11", "W12", "W13", "W14"];
const stats = data({ server: "SRV1", waits: NAMES.map((n, i) => ({ wait_type: n, wait_time_ms: 1000 - i, signal_wait_time_ms: 1, waiting_tasks: 1 })) });
const trendFor = (name) => data({
  server: "SRV1", wait_type: name,
  trend: [{ time: ago(10), wait_time_ms_per_second: 5, signal_wait_time_ms_per_second: 1 }, { time: ago(5), wait_time_ms_per_second: 7, signal_wait_time_ms_per_second: 2 }],
  discontinuities: [],
});
let failing = new Set();
answer = (url) => {
  const t = tool(url);
  if (t === "get_wait_stats") return stats;
  if (t === "get_wait_trend") {
    const w = url.searchParams.get("wait_type");
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
  modules.util.mount(root, modules.tabs.waitsPanel(server, ctx));
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
const trendReads = () => fetches.filter((f) => f.includes("get_wait_trend")).map((f) => new URL(f, "http://x").searchParams.get("wait_type"));
const lastChart = () => modules.charts.chartCalls[modules.charts.chartCalls.length - 1] || null;
const chartInfo = () => {
  const c = lastChart();
  return c && { labels: c.spec.series.map((s) => s.label), points: c.spec.points.length, id: c.id };
};
const notes = (root) => all(root, (n) => String(n.className).includes("notice")).map((n) => n.textContent);

const scenarios = {
  defaults: async () => {
    const root = await build("SRV1");
    return { checked: checkedNames(root), reads: trendReads(), chart: chartInfo(), count: all(root, (n) => n.className === "mp-count")[0].textContent };
  },
  checkUncheck: async () => {
    const root = await build("SRV1");
    const before = checkedNames(root);
    fetches.length = 0;
    await toggle(boxes(root).find((b) => b.attrs["aria-label"] === "CXPACKET"), false);
    const afterUncheck = checkedNames(root);
    const readsAfterUncheck = trendReads();
    fetches.length = 0;
    await toggle(boxes(root).find((b) => b.attrs["aria-label"] === "W11"), true);
    return { before, afterUncheck, readsAfterUncheck, afterCheck: checkedNames(root), readsAfterCheck: trendReads(), chart: chartInfo() };
  },
  search: async () => {
    const root = await build("SRV1");
    await typeSearch(root, "lck");
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
    const emptyChart = notes(root).length + all(root, (n) => n.className === "strip empty").length;
    await click(button(root, "Top waits"));
    return { afterClear, checked, disabled, hint, emptyChart, top: checkedNames(root), readsLast: trendReads().length };
  },
  survives: async () => {
    const first = await build("SRV1");
    await toggle(boxes(first).find((b) => b.attrs["aria-label"] === "LCK_M_X"), true);
    await click(button(first, "Clear All"));
    await toggle(boxes(first).find((b) => b.attrs["aria-label"] === "WRITELOG"), true);
    await typeSearch(first, "WRITE");
    const sel = all(first, (n) => n.tag === "select")[0];
    sel.value = "signal_wait_time_ms_per_second";
    sel.listeners.change.forEach((l) => l({}));
    await settle();
    const rebuilt = await build("SRV1");
    const search = all(rebuilt, (n) => n.tag === "input" && n.attrs.type === "search")[0].value;
    const metric = all(rebuilt, (n) => n.tag === "select")[0].value;
    const other = await build("SRV2");
    return { rebuilt: checkedNames(rebuilt), search, metric, listedRebuilt: listed(rebuilt), other: checkedNames(other), otherMetric: all(other, (n) => n.tag === "select")[0].value };
  },
  oneFailed: async () => {
    failing = new Set(["WRITELOG"]);
    const root = await build("SRV1");
    return { chart: chartInfo(), notes: notes(root), errors: all(root, (n) => n.className === "strip error").map((n) => n.textContent) };
  },
  allFailed: async () => {
    failing = new Set(NAMES);
    const root = await build("SRV1");
    return { chart: chartInfo(), notes: notes(root), errors: all(root, (n) => n.className === "strip error").map((n) => n.textContent) };
  },
  zoom: async () => {
    const root = await build("SRV1");
    const c = lastChart();
    const stamps = c.spec.points.map((p) => p.time).sort();
    const from = Date.parse(stamps[stamps.length - 1] + "Z") - 1000;
    const z = modules.charts.applyChartZoom(c.spec, { from, to: from + 2000 });
    return { id: c.id, series: c.spec.series.length, total: c.spec.points.length, zoomed: z.zoomed, zoomedPoints: z.spec.points.length, wrapped: all(root, (n) => n.className === "zoomable-chart").length };
  },
};

const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);
const result = await chosen();
console.log(JSON.stringify({ ...result, rejections }));
