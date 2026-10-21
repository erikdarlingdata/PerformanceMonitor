/* #6301: runs the server page's Top Queries and Top Procedures cards (the CPU tab's two grids, wwwroot/js/pages/server-tabs.js with the shipped
   panels.js, util.js and time-slicer.js) against a scripted /api/read answer, drags the time slicer, and prints one line of JSON with what the
   page read and drew. TimeSlicerBehaviourTests starts it as
       node time-slicer-harness.mjs <path to wwwroot/js>
   The whole js tree is copied into a scratch folder first (#5279). Only the DOM and fetch are stand-ins. The window is the last 24 hours ending
   at a fixed minute, so the expected `as_of` of a drag is plain arithmetic. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 1];

class FakeNode {
  constructor(tag) {
    this.style = {};
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
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
  getBoundingClientRect() { return { left: 0, width: 1000 }; }
  appendChild(c) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    this.children.push(c);
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, stopPropagation() {}, target: this, ...e }); }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = {
  body: new FakeNode("body"),
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeText(t),
};
globalThis.location = { hash: "#/server/a/cpu" };

const END = Date.UTC(2026, 9, 21, 12, 0, 0);
const HOUR = 3600000;
const START = END - 24 * HOUR;
const iso = (ms) => new Date(ms).toISOString().replace("Z", "");

const asks = [];
const answerFor = (name, params) => {
  if (name === "get_top_queries_by_cpu") {
    return { server: "a", hours_back: 24, tier_used: "raw", queries: [{ query_hash: "0x1", database_name: "db1", query_text: "select 1", total_cpu_ms: 5, total_logical_reads: 7, execution_count: 2 }] };
  }
  if (name === "get_top_procedures_by_cpu") {
    return { server: "a", hours_back: 24, tier_used: "raw", procedures: [{ database_name: "db1", full_name: "dbo.p1", execution_count: 3, total_cpu_ms: 4, total_logical_reads: 9 }] };
  }
  if (name === "get_query_duration_trend" || name === "get_procedure_duration_trend") {
    return { trend: [0, 6, 12, 18].map((h, i) => ({ time: iso(START + h * HOUR), value: 10 + i * 5 })) };
  }
  if (name === "get_query_trend") {
    return { trend: [3, 9, 15].map((h, i) => ({ collection_time: iso(START + h * HOUR), avg_cpu_ms: 100 * (i + 1), avg_elapsed_ms: 50, execution_count: 4 })) };
  }
  if (name === "get_active_queries") {
    if (params.get("snapshots_only")) return { snapshots: [{ collection_time: iso(END - 2 * HOUR), row_count: 3 }, { collection_time: iso(END - 8 * HOUR), row_count: 5 }], total_snapshot_times: 2 };
    return { queries: [{ session_id: 51, database_name: "db1", query_text: "select active", collection_time: iso(END - 2 * HOUR) }] };
  }
  if (name === "get_query_store_top") {
    return { queries: [{ database_name: "db1", query_id: 7, plan_id: 3, query_text: "select qs", execution_count: 4, avg_duration_ms: 12 }] };
  }
  if (name === "get_query_store_duration_trend") {
    return { trend: [0, 6, 12, 18].map((h, i) => ({ time: iso(START + h * HOUR), value: 20 + i })) };
  }
  if (name === "get_query_store_query_history") {
    return { points: [{ h: 2, plan: 3 }, { h: 4, plan: 4 }, { h: 10, plan: 3 }].map((p) => ({ collection_time: iso(START + p.h * HOUR), plan_id: p.plan, avg_duration_ms: 9 + p.h })), plans: [{ plan_id: 3 }, { plan_id: 4 }] };
  }
  return {};
};
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const name = u.pathname.replace("/api/read/", "");
  asks.push({
    name,
    hours: u.searchParams.get("hours"),
    as_of: u.searchParams.get("as_of"),
    hash: u.searchParams.get("query_hash"),
    query_id: u.searchParams.get("query_id"),
    snapshots_only: u.searchParams.get("snapshots_only"),
    collection_time: u.searchParams.get("collection_time"),
  });
  return { status: 200, ok: true, text: async () => JSON.stringify(answerFor(name, u.searchParams)) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "time-slicer-"));
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
const ctxOf = (rangeKey = "preset:24") => ({ hours: 24, label: "last 24 hours", startMs: START, endMs: END, rangeKey });
const draw = async (ctx, tabId = "cpu", server = "a") => {
  const tab = modules.tabs.SERVER_TABS.find((t) => t.id === tabId);
  const root = new FakeNode("main");
  modules.util.mount(root, tab.build(server, ctx));
  await flush();
  return root;
};
const byClass = (root, cls) => all(root).filter((n) => new RegExp("(^|\\s)" + cls + "(\\s|$)").test(n.className || n.attrs.class || ""));
const topAsks = () => asks.filter((a) => a.name === "get_top_queries_by_cpu" || a.name === "get_top_procedures_by_cpu");
const lastTop = (name) => topAsks().filter((a) => a.name === name).pop();
const slicerOf = (root, title) => byClass(root, "slicer").find((n) => n.attrs["data-slicer"] === title);
const drag = async (root, fromX, toX, title = null) => {
  const track = byClass(title ? slicerOf(root, title) : root, "slicer-track")[0];
  track.fire("pointerdown", { clientX: fromX, button: 0, pointerId: 1 });
  track.fire("pointermove", { clientX: toX, pointerId: 1 });
  track.fire("pointerup", { clientX: toX, pointerId: 1 });
  await flush();
};
const readoutOf = (root) => byClass(root, "slicer-readout")[0].textContent;
const titlesOf = (root) => all(root).filter((n) => n.tag === "h3").map((n) => n.textContent);

const out = {};
let root = await draw(ctxOf());
out.slicers = byClass(root, "slicer").length;
out.stripDrawn = byClass(root, "slicer-line").every((n) => (n.attrs.d || "").startsWith("M"));
out.initial = { queries: lastTop("get_top_queries_by_cpu"), procedures: lastTop("get_top_procedures_by_cpu") };

/* 1. A drag from 25% to 50% of the strip is hours 6 to 12 of the window; on release both grids read that window. */
asks.length = 0;
await drag(root, 250, 500);
out.dragged = { queries: lastTop("get_top_queries_by_cpu"), procedures: lastTop("get_top_procedures_by_cpu"), expectedAsOf: new Date(START + 12 * HOUR).toISOString(), readout: readoutOf(root) };
out.draggedTitle = titlesOf(root).find((t) => t.startsWith("Top Queries")) || "";

/* 2. A row pick overlays that query's own trend (get_query_trend) on the strip. */
asks.length = 0;
const row = all(root).find((n) => n.tag === "tr" && n.attrs["aria-pressed"] === "false" && /select 1/.test(n.textContent));
row.fire("click", {});
await flush();
out.overlay = { ask: asks.find((a) => a.name === "get_query_trend") || null, path: byClass(root, "slicer-overlay")[0].attrs.d || "", note: byClass(root, "slicer-overlay-note")[0].textContent, selected: / row-selected/.test(" " + row.className) };

/* 3. The window survives a redraw of the tab (a tab switch, the 60 s rebuild); the overlay's pick comes back with it. */
asks.length = 0;
const again = await draw(ctxOf());
out.survived = { queries: lastTop("get_top_queries_by_cpu"), readout: readoutOf(again), overlayAgain: asks.some((a) => a.name === "get_query_trend") };

/* 4. The keyboard: an arrow on the selection moves it by a step and the grids read the new window after the keys stop. */
asks.length = 0;
const body = byClass(again, "slicer-sel")[0];
body.fire("keydown", { key: "ArrowRight" });
await new Promise((r) => setTimeout(r, 700));
await flush();
out.keyed = { queries: lastTop("get_top_queries_by_cpu"), readout: readoutOf(again) };

/* 5. Reset puts the page range back: the grids read the plain window again. */
asks.length = 0;
byClass(again, "slicer-reset")[0].fire("click", {});
await flush();
out.reset = { queries: lastTop("get_top_queries_by_cpu"), procedures: lastTop("get_top_procedures_by_cpu"), readout: readoutOf(again) };

/* 6. A narrowing is dropped when the page range is a different one. */
await drag(again, 250, 500);
asks.length = 0;
const other = await draw(ctxOf("preset:12"));
out.otherRange = { queries: lastTop("get_top_queries_by_cpu"), readout: readoutOf(other) };

/* 7. The Queries tab (a server of its own, so the CPU tab's narrowing is not in play) draws four slicers: Top Queries, Top Procedures, Active
   Queries and Query Store. Each of the last two charts its own read. */
const lastAsk = (name, filter = () => true) => asks.filter((a) => a.name === name && filter(a)).pop() || null;
asks.length = 0;
const qroot = await draw(ctxOf(), "queries", "b");
out.queriesTab = {
  slicers: byClass(qroot, "slicer").map((n) => n.attrs["data-slicer"]),
  activeStripDrawn: byClass(slicerOf(qroot, "Active queries"), "slicer-line").every((n) => (n.attrs.d || "").startsWith("M")),
  storeStripDrawn: byClass(slicerOf(qroot, "Query Store duration"), "slicer-line").every((n) => (n.attrs.d || "").startsWith("M")),
  activeList: lastAsk("get_active_queries", (a) => a.snapshots_only),
  activeGrid: lastAsk("get_active_queries", (a) => !a.snapshots_only),
  store: lastAsk("get_query_store_top"),
};

/* 8. A drag on the Active Queries strip narrows the Active Queries list and grid AND the Query Store grid (one narrowing per server). */
asks.length = 0;
await drag(qroot, 250, 500, "Active queries");
out.queriesDragged = {
  activeList: lastAsk("get_active_queries", (a) => a.snapshots_only),
  activeGrid: lastAsk("get_active_queries", (a) => !a.snapshots_only),
  store: lastAsk("get_query_store_top"),
  expectedAsOf: new Date(START + 12 * HOUR).toISOString(),
  title: titlesOf(qroot).find((t) => t.startsWith("Query Store") && !t.startsWith("Query Store Duration")) || "",
};

/* 9. Picking a Query Store row overlays THAT query's history (get_query_store_query_history, keyed on its query id) over the page range, only its own plan. */
asks.length = 0;
const qsRow = all(qroot).find((n) => n.tag === "tr" && n.attrs["aria-pressed"] === "false" && /select qs/.test(n.textContent));
/* A click that lands on a button inside the row (the plan and History buttons) is that button's, not a pick. */
qsRow.fire("click", { target: new FakeNode("button") });
await flush();
const buttonPicked = asks.some((a) => a.name === "get_query_store_query_history");
qsRow.fire("click", {});
await flush();
const qsStrip = slicerOf(qroot, "Query Store duration");
out.storeOverlay = {
  buttonPicked,
  ask: lastAsk("get_query_store_query_history"),
  path: byClass(qsStrip, "slicer-overlay")[0].attrs.d || "",
  note: byClass(qsStrip, "slicer-overlay-note")[0].textContent,
  segments: (byClass(qsStrip, "slicer-overlay")[0].attrs.d || "").split("L").length,
  selected: / row-selected/.test(" " + qsRow.className),
};

/* 10. Reset on the Query Store strip puts the whole range back for the Query Store grid and the Active Queries list and grid. */
asks.length = 0;
byClass(slicerOf(qroot, "Query Store duration"), "slicer-reset")[0].fire("click", {});
await flush();
out.queriesReset = {
  activeList: lastAsk("get_active_queries", (a) => a.snapshots_only),
  activeGrid: lastAsk("get_active_queries", (a) => !a.snapshots_only),
  store: lastAsk("get_query_store_top"),
};

console.log(JSON.stringify(out));
process.exit(0);
