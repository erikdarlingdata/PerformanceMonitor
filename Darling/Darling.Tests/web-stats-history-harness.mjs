/* #6303: runs the shipped stats-history.js (the Top Queries and Top Procedures History button) with util.js on a small
   fake DOM and prints what a scenario did as one line of JSON. StatsHistoryBehaviourTests starts it as
       node web-stats-history-harness.mjs <path to the js folder> <scenario>
   Only the DOM, fetch, the clipboard and the download plumbing are stand-ins. */
import { pathToFileURL } from "node:url";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

/* isConnected is a plain flag here: true for a node that was built or put under a parent, false once removeChild took it
   out (the page throwing a cell away). */
class FakeNode {
  constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.style = {}; this.isConnected = FakeNode.newConnected; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); c.isConnected = true; return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; c.isConnected = false; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  click() { for (const fn of this.listeners.click || []) fn({ preventDefault() {}, target: this }); }
  all(pred, acc = []) { if (pred(this)) acc.push(this); for (const c of this.children) c.all && c.all(pred, acc); return acc; }
  byText(t) { return this.all((n) => n.tag === "button" && n.textContent === t)[0] || null; }
}
/* A new node starts on the page here. A scenario that builds a grid sets this to false, as a browser would: a node is not on
   the page until something puts it there. */
FakeNode.newConnected = true;
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
const body = new FakeNode("body");
const downloads = [];
globalThis.document = { body, createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
URL.createObjectURL = (blob) => { downloads.push({ blob }); return "blob:fake"; };
URL.revokeObjectURL = () => {};
const origAppend = body.appendChild.bind(body);
body.appendChild = (a) => { if (a.tag === "a") a.click = () => { downloads[downloads.length - 1].name = a.download; }; return origAppend(a); };
const clip = [];
Object.defineProperty(globalThis, "navigator", { value: { clipboard: { writeText: async (t) => { clip.push(t); } } }, configurable: true, writable: true });
globalThis.location = { hash: "#/server/a/queries" };

const fetches = [];
let reply = { status: 200, body: "{}" };
globalThis.fetch = async (url) => {
  fetches.push(String(url));
  return { status: reply.status, ok: reply.status < 400, text: async () => reply.body };
};

/* ResizeObserver stand-in that counts the observers still connected (plan-row.js: a closed panel must leave none). */
const observers = [];
globalThis.ResizeObserver = class { constructor() { this.live = true; observers.push(this); } observe() {} disconnect() { this.live = false; } };
const liveObservers = () => observers.filter((o) => o.live).length;

const root = process.argv[2];
/* charts.js draws SVG, which the stand-in DOM cannot: copy the page scripts and replace it with a recorder, so the
   harness sees exactly the spec the panel hands the chart. */
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "stats-history-"));
fs.cpSync(root, scratch, { recursive: true });
fs.writeFileSync(path.join(scratch, "charts.js"),
  'export const SERIES_COLORS = ["#2eaef1", "#4dd0e1"];\n' +
  'export const chartZoomScope = (h) => "scope|" + h;\n' +
  'export function zoomableLineChart(spec, id, scope) { globalThis.__charts.push({ spec, id, scope, hidden: globalThis.__chartState.get(id + "|" + scope) ?? null }); const d = document.createElement("div"); d.className = "zoomable-chart"; return d; }\n');
globalThis.__charts = [];
/* Stands for the zoom and legend state charts.js keeps per chart id and scope: a scenario writes it as a click on the legend would. */
globalThis.__chartState = new Map();
const mod = await import(pathToFileURL(scratch + "/pages/stats-history.js").href);
const flush = () => new Promise((r) => setTimeout(r, 5));
const out = {};
const respond = (data) => { reply = { status: 200, body: JSON.stringify(data) }; };
const query = (i) => { const u = new URL(fetches[i], "http://viewer.test"); return { path: u.pathname, query: Object.fromEntries(u.searchParams) }; };
const chartsNow = () => globalThis.__charts.map((c) => ({ id: c.id, series: c.spec.series.map((s) => s.key), xKey: c.spec.xKey, points: c.spec.points }));
const procRow = { database_name: "Orders", full_name: "dbo.usp_Ship" };
const queryRow = { database_name: "Orders", query_hash: "0xABC" };
const procTrend = () => ({
  server: "srv-a", hours_back: 24, database_name: "Orders", procedure_name: "dbo.usp_Ship", window_truncated: false,
  trend: [
    { time: "2026-03-04T05:00:00Z", elapsed_ms_per_second: 200, executions_per_second: 4, avg_cpu_ms: 12.5 },
    { time: "2026-03-04T06:00:00Z", elapsed_ms_per_second: null, executions_per_second: null, avg_cpu_ms: null },
    { time: "2026-03-04T07:00:00Z", elapsed_ms_per_second: 10, executions_per_second: 0 },
  ],
});

const scenarios = {
  async procOpen() {
    respond(procTrend());
    const cell = mod.statsHistoryColumn("procedures", "srv-a", 24).render(procRow);
    out.buttonBefore = cell.byText("History") !== null;
    cell.byText("History").click();
    out.loading = cell.textContent;
    await flush();
    Object.assign(out, query(0));
    out.fetches = fetches.length;
    out.chart = chartsNow();
    const select = cell.all((n) => n.tag === "select")[0];
    out.options = select.all((n) => n.tag === "option").map((o) => o.textContent);
    select.value = "executions_per_second";
    select.listeners.change[0]();
    out.afterPick = chartsNow().slice(-1)[0];
    out.fetchesAfterPick = fetches.length;
    cell.byText("Hide history").click();
    out.closed = cell.all((n) => n.tag === "select").length === 0;
  },
  async queryOpen() {
    respond({ hours_back: 24, trend: [{ collection_time: "2026-03-04T05:00:00", avg_cpu_ms: 40, avg_elapsed_ms: 100 }] });
    const cell = mod.statsHistoryColumn("queries", "srv-a", 24).render(queryRow);
    cell.byText("History").click();
    await flush();
    Object.assign(out, query(0));
    out.chart = chartsNow();
    out.selects = cell.all((n) => n.tag === "select").length;
  },
  async rebuild() {
    respond(procTrend());
    mod.statsHistoryColumn("procedures", "srv-a", 24).render(procRow).byText("History").click();
    await flush();
    const before = fetches.length;
    const second = mod.statsHistoryColumn("procedures", "srv-a", 24).render(procRow);
    out.open = second.byText("Hide history") !== null;
    out.refetched = fetches.length - before;
    mod.statsHistoryColumn("procedures", "srv-a", 48).render(procRow);
    await flush();
    out.refetchedForNewWindow = fetches.length - before;
  },
  async empty() {
    reply = { status: 200, body: JSON.stringify({ status: "empty", message: "No stored-procedure samples in the window." }) };
    const cell = mod.statsHistoryColumn("procedures", "srv-a", 24).render(procRow);
    cell.byText("History").click();
    await flush();
    out.empty = cell.all((n) => n.className === "strip empty").map((n) => n.textContent);
    out.charts = chartsNow().length;
  },
  async noKey() {
    const p = mod.statsHistoryColumn("procedures", "srv-a", 24);
    const q = mod.statsHistoryColumn("queries", "srv-a", 24);
    out.noName = p.render({ database_name: "Orders" }).textContent;
    out.noDb = p.render({ full_name: "dbo.usp_Ship" }).textContent;
    out.noHash = q.render({ database_name: "Orders" }).textContent;
  },
  async points() {
    out.points = mod.procedurePoints(procTrend().trend);
    out.none = mod.procedurePoints(null);
  },
};
await scenarios[process.argv[3]]();
console.log(JSON.stringify(out));
