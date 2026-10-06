/* #5234: runs the shipped Query Store history panel (wwwroot/js/pages/query-store-history.js with plan-viewer.js, util.js and grid-tools.js) on a small
   fake DOM and prints what a scenario did as one line of JSON. QueryStoreHistoryBehaviourTests starts it as
       node web-qs-history-harness.mjs <path to the js folder> <scenario>
   Only the DOM, fetch, the clipboard and the download plumbing are stand-ins. */
import { pathToFileURL } from "node:url";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

/* isConnected is a plain flag here: true for a node that was built or put under a parent, false once removeChild took it
   out (the page throwing a cell away). */
class FakeNode {
  constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.style = {}; this.isConnected = true; }
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

const root = process.argv[2];
/* charts.js draws SVG, which the stand-in DOM cannot: copy the page scripts and replace it with a recorder, so the
   harness sees exactly the spec the panel hands the chart. */
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "qs-history-"));
fs.cpSync(root, scratch, { recursive: true });
fs.writeFileSync(path.join(scratch, "charts.js"),
  'export const SERIES_COLORS = ["#2eaef1", "#4dd0e1"];\n' +
  'export const chartZoomScope = (h) => "scope|" + h;\n' +
  'export function zoomableLineChart(spec, id, scope) { globalThis.__charts.push({ spec, id, scope }); const d = document.createElement("div"); d.className = "zoomable-chart"; return d; }\n');
globalThis.__charts = [];
const mod = await import(pathToFileURL(scratch + "/pages/query-store-history.js").href);
const util = await import(pathToFileURL(scratch + "/util.js").href);
const pv = await import(pathToFileURL(scratch + "/pages/plan-viewer.js").href);
const flush = () => new Promise((r) => setTimeout(r, 5));
const out = {};
const row = { database_name: "Orders", query_id: 42 };
const KEY = "srv-a|Orders|42|||"; // the panel's key for `row` (server|database|query id|plan id|execution type|replica role)
const history = (extra = {}) => ({
  database_name: "Orders", query_id: 42, effective_start: "2026-03-04T00:00:00.0000000Z", window_truncated: false, points_truncated: false,
  plans: [
    { plan_id: 7, execution_count: 40, avg_duration_ms: 175, total_duration_ms: 7000, avg_cpu_ms: 70, total_cpu_ms: 2800, first_execution_time: "2026-03-04T04:00:00", last_execution_time: "2026-03-04T06:00:00", is_forced_plan: true },
    { plan_id: 9, execution_count: 5, avg_duration_ms: 10, total_duration_ms: 50, avg_cpu_ms: 5, total_cpu_ms: 25, first_execution_time: null, last_execution_time: null, is_forced_plan: false },
  ],
  points: [
    { collection_time: "2026-03-04T05:00:00", plan_id: 7, execution_count: 10, avg_duration_ms: 100, avg_cpu_ms: 40 },
    { collection_time: "2026-03-04T06:00:00", plan_id: 9, execution_count: 5, avg_duration_ms: 10, avg_cpu_ms: 5 },
  ],
  ...extra,
});
const respond = (data) => { reply = { status: 200, body: JSON.stringify(data) }; };
const col = () => mod.queryStoreHistoryColumn("srv-a", 24);
const query = (i) => { const u = new URL(fetches[i], "http://viewer.test"); return { path: u.pathname, query: Object.fromEntries(u.searchParams) }; };
const tables = (cell) => cell.all((n) => n.tag === "table");

const scenarios = {
  async open() {
    respond(history());
    const cell = col().render(row);
    out.buttonBefore = cell.byText("History") !== null;
    cell.byText("History").click();
    out.loading = cell.textContent;
    await flush();
    Object.assign(out, query(0));
    out.fetches = fetches.length;
    const t = tables(cell)[0];
    out.dataRows = t.all((n) => n.tag === "tr").length - 1;
    out.rowTexts = t.all((n) => n.tag === "tr").slice(1).map((r) => r.textContent);
    const planButtons = cell.all((n) => n.tag === "button" && n.textContent === "Plan");
    out.planButtons = planButtons.length;
    planButtons[1].click();
    await flush();
    Object.assign(out, { plan: query(1) });
    out.chart = globalThis.__charts.map((c) => ({ id: c.id, scope: c.scope, series: c.spec.series.map((s) => s.key), points: c.spec.points.length, xKey: c.spec.xKey }));
    cell.byText("Hide history").click();
    out.closed = tables(cell).length === 0;
  },
  async rebuild() {
    respond(history());
    const first = col().render(row);
    first.byText("History").click();
    await flush();
    const before = fetches.length;
    const second = col().render(row);
    out.open = second.byText("Hide history") !== null;
    out.showsTable = tables(second).length === 1;
    out.refetched = fetches.length - before;
  },
  async truncated() {
    respond(history({ window_truncated: true }));
    const cell = col().render(row);
    cell.byText("History").click();
    await flush();
    out.notices = cell.all((n) => n.className === "strip notice").map((n) => n.textContent);
  },
  async notTruncated() {
    respond(history());
    const cell = col().render(row);
    cell.byText("History").click();
    await flush();
    out.notices = cell.all((n) => n.className === "strip notice").length;
  },
  async empty() {
    reply = { status: 200, body: JSON.stringify({ status: "empty", message: "No Query Store history for query_id 42." }) };
    const cell = col().render(row);
    cell.byText("History").click();
    await flush();
    out.text = cell.textContent;
    out.tables = tables(cell).length;
  },
  async customRange() {
    respond(history());
    util.setActiveRange({ server: "srv-a", hours: 24, startMs: Date.parse("2026-03-04T02:00:00Z"), endMs: Date.parse("2026-03-04T06:00:00Z"), asOf: "2026-03-04T06:00:00.000Z" });
    const cell = col().render(row);
    cell.byText("History").click();
    await flush();
    Object.assign(out, query(0));
  },
  async windowChange() {
    respond(history());
    const first = col().render(row);
    first.byText("History").click();
    await flush();
    const before = fetches.length;
    const sameWindow = mod.queryStoreHistoryColumn("srv-a", 24).render(row);
    out.sameWindowRefetched = fetches.length - before;
    const wider = mod.queryStoreHistoryColumn("srv-a", 168).render(row);
    await flush();
    out.hoursRefetched = fetches.length - before;
    out.lastHours = query(fetches.length - 1).query.hours;
    out.stillOpen = wider.byText("Hide history") !== null && tables(wider).length === 1;
    util.setActiveRange({ server: "srv-a", hours: 168, startMs: 0, endMs: 1, asOf: "2026-03-04T06:00:00.000Z" });
    const ranged = mod.queryStoreHistoryColumn("srv-a", 168).render(row);
    await flush();
    out.rangeRefetched = fetches.length - before - 1;
    out.lastAsOf = query(fetches.length - 1).query.as_of;
  },
  /* The page throws a cell away (a grid rebuild, a tab switch) while the read is out. When the answer lands, the cell that
     left is not drawn into and leaves the redraw set, and the cell that replaced it is drawn. */
  async detachedCell() {
    respond(history());
    const gone = body.appendChild(col().render(row));
    gone.byText("History").click();
    body.removeChild(gone);
    const kept = body.appendChild(col().render(row));
    await flush();
    out.goneTables = tables(gone).length;
    out.keptTables = tables(kept).length;
    out.cells = mod.queryStoreHistoryViewCounts()[KEY];
  },
  /* The last cell for a key goes: the key itself leaves the redraw map, not an empty set left behind. */
  async lastCellGone() {
    respond(history());
    const only = body.appendChild(col().render(row));
    only.byText("History").click();
    body.removeChild(only);
    await flush();
    out.goneTables = tables(only).length;
    out.keys = Object.keys(mod.queryStoreHistoryViewCounts());
  },
  /* The grid has a row per plan, execution type and replica role of a query, and History opens under the row clicked and no
     other row of the query. Closing one leaves the others as they were. */
  async twoRows() {
    respond(history());
    const c = col();
    const planA = { database_name: "Orders", query_id: 42, plan_id: 7, execution_type_desc: "Regular", replica_role: "PRIMARY" };
    const rows = [planA, { ...planA, plan_id: 9 }, { ...planA, execution_type_desc: "Aborted" }, { ...planA, replica_role: "SECONDARY" }];
    const cells = rows.map((r) => body.appendChild(c.render(r)));
    const shown = () => cells.map((x) => x.byText("Hide history") !== null && tables(x).length === 1);
    cells[0].byText("History").click();
    await flush();
    out.afterFirst = shown();
    out.fetchesAfterFirst = fetches.length;
    out.keysAfterFirst = mod.openQueryStoreHistoryKeys();
    cells[1].byText("History")?.click();
    await flush();
    out.afterSecond = shown();
    cells[0].byText("Hide history")?.click();
    out.afterHide = shown();
    out.keysAfterHide = mod.openQueryStoreHistoryKeys();
  },
  /* The Plan button in a History table opens its own cell: not the grid's Plan cell for that plan, and not the same plan's button
     in the History table under another grid row. */
  async planButtonScope() {
    respond(history());
    const c = col();
    const rowA = { database_name: "Orders", query_id: 42, plan_id: 7, execution_type_desc: "Regular", replica_role: null };
    const rowB = { ...rowA, plan_id: 9 };
    const cellA = body.appendChild(c.render(rowA));
    const cellB = body.appendChild(c.render(rowB));
    const gridPlan = body.appendChild(pv.queryStorePlanColumn("srv-a").render(rowA));
    cellA.byText("History").click();
    cellB.byText("History")?.click();
    await flush();
    const planButtons = (cell) => cell.all((n) => n.tag === "button" && n.textContent === "Plan");
    const openPlans = (cell) => cell.all((n) => n.tag === "button" && n.textContent === "Hide plan").length;
    const before = fetches.length;
    planButtons(cellA)[0].click();
    await flush();
    out.planReads = fetches.length - before;
    out.afterTableButton = { a: openPlans(cellA), b: openPlans(cellB), grid: openPlans(gridPlan) };
    gridPlan.byText("Plan")?.click();
    await flush();
    out.afterGridButton = { a: openPlans(cellA), b: openPlans(cellB), grid: openPlans(gridPlan) };
    planButtons(cellB)[0]?.click();
    await flush();
    out.afterOtherTableButton = { a: openPlans(cellA), b: openPlans(cellB), grid: openPlans(gridPlan) };
    out.planPanels = pv.openPlanKeys().length;
  },
  async cutNotice() {
    respond(history({ points_truncated: true }));
    const cell = col().render(row);
    cell.byText("History").click();
    await flush();
    out.notices = cell.all((n) => n.className === "strip notice").map((n) => n.textContent);
  },
  async noKey() {
    out.noDb = col().render({ query_id: 1 }).textContent;
    out.noId = col().render({ database_name: "Orders" }).textContent;
  },
  async flags() {
    const c = col();
    Object.assign(out, { key: c.key, sortable: c.sortable, filter: c.filter, csv: c.csv, copy: c.copy });
  },
};
await scenarios[process.argv[3]]();
console.log(JSON.stringify(out));
