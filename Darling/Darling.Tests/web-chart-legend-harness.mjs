/* Runs the web viewer's shipped charts.js (the legend hide/isolate of zoomableLineChart, with util.js) against a stand-in DOM,
   clicks the legend, and prints what was drawn as one line of JSON.
   WebChartLegendIsolateBehaviourTests starts it as
       node web-chart-legend-harness.mjs <path to wwwroot/js>
   The DOM is a node tree of plain objects that keeps the listeners it is given. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 1];

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
  setPointerCapture() {}
  focus() {}
  get offsetWidth() { return 0; }
  get offsetHeight() { return 0; }
  dispatch(type, ev) {
    (this.listeners[type] || []).forEach((l) => l(ev || {}));
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
  body: new FakeNode("body"),
};
globalThis.location = { hash: "#/server/A/waits" };
let blobParts = null;
globalThis.Blob = class { constructor(parts) { blobParts = parts; } };
globalThis.URL.createObjectURL = () => "blob:stub";
globalThis.URL.revokeObjectURL = () => {};

const scratch = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "chart-legend-")));
let charts;
let compose;
try {
  /* Copy the whole js tree (js/, js/pages/ and every subdirectory) rather than a hand-kept list (#5279): a page module that
     another PR adds then needs no edit here. Only imported files load, so the rest are inert; every stand-in below is
     written AFTER the copy, so it still replaces the real file. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  charts = await import(pathToFileURL(path.join(scratch, "charts.js")).href);
  await import(pathToFileURL(path.join(scratch, "grid-tools.js")).href);
  fs.writeFileSync(path.join(scratch, "panels.js"), "export function navigateServer() {}\nexport function gridTable() { return document.createElement(\"div\"); }\n");
  fs.writeFileSync(path.join(scratch, "views-api.js"), "export async function getCatalog() { return { compose: {} }; }\n");
  compose = await import(pathToFileURL(path.join(scratch, "compose.js")).href);
} catch (e) {
  fs.rmSync(scratch, { recursive: true, force: true });
  throw e;
}

const find = (node, pred, out = []) => {
  if (pred(node)) out.push(node);
  node.children.forEach((c) => find(c, pred, out));
  return out;
};
const T0 = Date.UTC(2026, 0, 1, 0, 0, 0);
const MIN = 60000;
const points = Array.from({ length: 11 }, (_, i) => ({ t: new Date(T0 + i * MIN).toISOString().slice(0, 19), big: 1000 + i * 100, small: i % 5, mid: 3 + (i % 3) }));
const specFor = () => ({
  points,
  xKey: "t",
  series: [
    { key: "big", label: "BIG", color: "#f00" },
    { key: "small", label: "SMALL", color: "#0f0" },
    { key: "mid", label: "MID", color: "#00f" },
  ],
  windowStart: T0,
  windowEnd: T0 + 10 * MIN,
});
const cls = (n) => String(n.className || n.attrs.class || "");
const legendItems = (host) => find(host, (n) => /\bitem\b/.test(cls(n)) && n.tag === "span" && cls(n).includes("switchable"));
const entry = (host, label) => legendItems(host).find((n) => n.children[1].textContent === label);
const target = (host, label) => entry(host, label); // an undrillable entry carries the click itself
const lines = (host) => find(host, (n) => n.tag === "polyline" && cls(n).includes("series-line")).length;
const topTick = (host) => {
  const ticks = find(host, (n) => n.tag === "text" && /^[0-9.]+$/.test(n.textContent) && n.attrs["text-anchor"] === "end").map((n) => Number(n.textContent));
  return Math.max(...ticks);
};
const showAll = (host) => find(host, (n) => n.tag === "button" && cls(n).includes("legend-show-all"))[0];
const state = (host) => ({
  lines: lines(host),
  top: topTick(host),
  off: legendItems(host).filter((n) => cls(n).includes("legend-off")).map((n) => n.children[1].textContent),
  pressed: Object.fromEntries(legendItems(host).map((n) => [n.children[1].textContent, n.attrs["aria-pressed"]])),
  showAll: !!showAll(host),
  showAllText: showAll(host) ? showAll(host).textContent : null,
});
const click = (host, label, ev) => target(host, label).dispatch("click", ev);
const dbl = (host, label) => target(host, label).dispatch("dblclick", {});

globalThis.fetch = async () => ({
  status: 200,
  ok: true,
  text: async () => JSON.stringify({
    sql: "select 1",
    rows: Array.from({ length: 11 }, (_, i) => [["AAA", 1000 + i * 100], ["BBB", i % 5]].map(([w, v]) => ({ bucket: new Date(T0 + i * MIN).toISOString().slice(0, 19), wait: w, value: v }))).flat(),
  }),
});
const composedPanel = (title) => ({ source: "waits", viz: "line", measure: "wait_ms", title, groupBy: ["wait"] });
const composedScope = { server: "A", hours: 4 };
const drawComposed = async (title, slot) => {
  const body = new FakeNode("div");
  await compose.renderComposedInto(body, composedPanel(title), composedScope, { panelSlot: slot });
  return body;
};

const out = {};
const scope = charts.chartZoomScope(4);

// 1. toggle hides and re-shows
const a = charts.zoomableLineChart(specFor(), "c1", scope);
out.initial = state(a);
click(a, "BIG");
out.afterHide = state(a);
click(a, "BIG");
out.afterShow = state(a);

// 2. the axis rescales to the visible series
click(a, "BIG");
out.rescaled = state(a);
click(a, "BIG");

// 3. isolate shows exactly one; a second isolate of the same one restores
dbl(a, "SMALL");
out.isolated = state(a);
dbl(a, "SMALL");
out.isolateUndone = state(a);

// 4. show all after an isolate; a toggle never hides the last visible series
dbl(a, "MID");
out.beforeShowAll = state(a);
showAll(a).dispatch("click", {});
out.afterShowAll = state(a);
click(a, "BIG");
click(a, "SMALL");
click(a, "MID");
out.lastKept = state(a);
showAll(a).dispatch("click", {});

// 5. the state survives a rebuild (the poll) of the same chart
click(a, "BIG");
const rebuilt = charts.zoomableLineChart(specFor(), "c1", scope);
out.rebuilt = state(rebuilt);
out.held = charts.getChartHidden("c1", scope);

// 6. it does not leak to another server, another range, or another chart
globalThis.location.hash = "#/server/B/waits";
out.otherServer = state(charts.zoomableLineChart(specFor(), "c1", charts.chartZoomScope(4)));
globalThis.location.hash = "#/server/A/waits";
out.otherRange = state(charts.zoomableLineChart(specFor(), "c1", charts.chartZoomScope(24)));
out.otherChart = state(charts.zoomableLineChart(specFor(), "c2", scope));

// 7. the CSV export (through the chart menu) carries exactly the shown series
const exportCsv = async (host) => {
  const btn = find(host, (n) => n.tag === "button" && n.attrs["aria-label"] === "Chart menu")[0];
  if (!find(host, (n) => n.attrs.role === "menuitem").length) btn.listeners.click[0]();
  find(host, (n) => n.attrs.role === "menuitem" && n.textContent.startsWith("Export Data to CSV"))[0].listeners.click[0]();
  await new Promise((r) => setTimeout(r, 50));
  const rows = String(blobParts[1]).split("\r\n").slice(1).filter(Boolean);
  return [...new Set(rows.map((r) => r.split(",")[1]))];
};
const csvHost = charts.zoomableLineChart(specFor(), "c3", scope);
out.csvAll = await exportCsv(csvHost);
click(csvHost, "BIG");
out.csvHidden = await exportCsv(csvHost);

// 8. a stacked chart re-stacks over what is visible
const stackedSpec = () => ({ ...specFor(), mode: "stacked" });
const st = charts.zoomableLineChart(stackedSpec(), "c4", scope);
out.stackedBefore = state(st);
click(st, "BIG");
out.stackedAfter = state(st);

// 9. a composed panel: hidden state survives its rebuild and does not reach a second composed panel
const cp1 = await drawComposed("Waits one", 0);
click(cp1, "AAA");
out.composedHidden = state(cp1);
out.composedRebuilt = state(await drawComposed("Waits one", 0));
out.composedOtherPanel = state(await drawComposed("Waits two", 1));
globalThis.location.hash = "#/server/B/waits";
out.composedOtherServer = state(await drawComposed("Waits one", 0));
globalThis.location.hash = "#/server/A/waits";

fs.rmSync(scratch, { recursive: true, force: true });
console.log(JSON.stringify(out));
