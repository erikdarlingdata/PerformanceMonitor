/* Runs the Queries tab's three hourly-rollup trend charts on the web viewer (#8927): pages/server-tabs.js (fanout and linePanel),
   panels.js (the line chart) and util.js (chartNote), with a stand-in DOM and a stub fetch that answers get_query_duration_trend and
   get_procedure_duration_trend with the scenario's `source`. It prints, for every panel on the Queries tab, the metric-note lines drawn.
   WebQueriesHourlyNoteBehaviourTests starts it as
       node web-queries-hourly-note-harness.mjs <path to wwwroot/js> <source> [qs-hourly]
   The whole js tree is copied into a scratch folder first, then charts.js (the SVG renderer needs a real browser) is replaced. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];
const source = process.argv[3];

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
    child.parentNode = this;
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
globalThis.location = { hash: "#/server/SRV1" };

const rejections = [];
process.on("unhandledRejection", (e) => rejections.push(String(e && e.stack ? e.stack : e)));


const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "queries-hourly-note-"));
let util;
let panels;
let tabs;
try {
  /* The whole js tree (js/, js/pages/ and every subdirectory) is copied rather than a hand-kept list (#5279); every stand-in
     below is written AFTER the copy, so it still replaces the real file. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    "export const SERIES_COLORS = ['#111', '#222', '#333', '#444', '#555', '#666'];\n" +
      "export const CATEGORICAL_COLORS = SERIES_COLORS;\n" +
      "export function normalizeColor(color) { return color; }\n" +
      "export function renderLineChart() { return document.createElement('div'); }\n" +
      "export function zoomableLineChart(spec) { (globalThis.__charts = globalThis.__charts || []).push({ title: spec.title, labels: (spec.series || []).map((x) => x.label), unit: spec.unit, points: (spec.points || []).length }); return document.createElement('div'); }\n" +
      "export function chartZoomScope() { return ''; }\n"
  );
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  util = await load("util.js");
  panels = await load("panels.js");
  tabs = await load(path.join("pages", "server-tabs.js"));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}


const hasClass = (node, name) => String(node.className).split(" ").includes(name);
const walk = (node, visit) => {
  visit(node);
  for (const c of node.children) walk(c, visit);
};
const settle = async () => {
  for (let i = 0; i < 200; i++) await new Promise((r) => setImmediate(r));
};

const ROWS = [0, 1, 2].map((i) => ({ time: "2026-01-01T0" + i + ":00:00.0000000Z", value: 10 + i, executions_per_second: 2 + i }));
const body = { source, bucket: "hourly", bucket_minutes: 60, trend: ROWS };
/* The Query Store trend's real reply (DarlingMcpTrendTools.DescribeQueryStoreTrend): the source is "rollup+raw" or "raw" and never "hourly", the width is
   a sentence and `bucket_minutes` is null, and the seam instant is routing.raw_from. The fourth argument `qs-hourly` makes it a reply that WERE hourly (source "hourly", 60-minute buckets), to prove the panel does not opt in. */
const queryStoreBody = {
  source: process.argv[4] === "qs-hourly" ? "hourly" : "rollup+raw",
  bucket: "1 hour before routing.raw_from, per-interval from it",
  bucket_minutes: process.argv[4] === "qs-hourly" ? 60 : null,
  routing: { raw_from: "2026-01-01T01:30:00.0000000Z" },
  trend: ROWS,
};
globalThis.fetch = async (url) => {
  const u = String(url);
  const answer = u.includes("/get_query_store_duration_trend")
    ? queryStoreBody
    : u.includes("/get_query_duration_trend") || u.includes("/get_procedure_duration_trend") ? body : {};
  return { status: 200, ok: true, text: async () => JSON.stringify(answer) };
};

const tab = tabs.SERVER_TABS.find((t) => t.id === "queries");
const holder = new FakeNode("div");
util.mount(holder, tab.build("SRV1", { hours: 168, label: "past week" }));
await settle();
const found = [];
walk(holder, (node) => {
  if (!hasClass(node, "panel")) return;
  const h3 = node.children.find((c) => c.tag === "h3");
  const notes = [];
  walk(node, (x) => {
    if (hasClass(x, "mp-metric-note")) notes.push(x.textContent);
  });
  found.push({ title: h3 && h3.children[0] ? h3.children[0].textContent : "", notes });
});
console.log(JSON.stringify({ panels: found, charts: globalThis.__charts || [], rejections }));
