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
  /* One procedure's own history (the History button's read, and the strip overlay): the two rates and the average CPU the read returns. */
  if (name === "get_procedure_duration_trend" && params.get("procedure_name")) {
    return { trend: [2, 8, 14].map((h, i) => ({ time: iso(START + h * HOUR), elapsed_ms_per_second: 40 * (i + 1), executions_per_second: 2, avg_cpu_ms: 7 * (i + 1) })) };
  }
  if (name === "get_blocking_trend" || name === "get_deadlock_trend") {
    return { trend: [1, 7, 13, 19].map((h, i) => ({ time: iso(START + h * HOUR), count: 2 + i })) };
  }
  /* Three events: one inside the 6-to-12-hour selection a drag from 25% to 50% makes, one before its start and one after its end. */
  if (name === "get_blocking") {
    return { events: [[13, "blk_in"], [20, "blk_before"], [3, "blk_after"]].map(([h, db]) => ({ event_time: iso(END - h * HOUR), database_name: db, blocked_session_id: 55, blocking_session_id: 51 })) };
  }
  if (name === "get_deadlocks") {
    return { deadlocks: [[13, "dl_in"], [20, "dl_before"], [3, "dl_after"]].map(([h, db]) => ({ deadlock_time: iso(END - h * HOUR), victim_process_id: db, process_summary: db })) };
  }
  if (name === "get_query_duration_trend" || name === "get_procedure_duration_trend") {
    return { trend: [0, 6, 12, 18].map((h, i) => ({ time: iso(START + h * HOUR), value: 10 + i * 5 })) };
  }
  if (name === "get_query_trend") {
    return { trend: [3, 9, 15].map((h, i) => ({ collection_time: iso(START + h * HOUR), avg_cpu_ms: 100 * (i + 1), avg_elapsed_ms: 50, execution_count: 4 })) };
  }
  if (name === "get_active_queries") {
    /* Server k's snapshot list was cut by the read's cap: the strip says so. */
    if (params.get("snapshots_only")) {
      const cut = params.get("server") === "k";
      return { snapshots: [{ collection_time: iso(END - 2 * HOUR), row_count: 3 }, { collection_time: iso(END - 8 * HOUR), row_count: 5 }], total_snapshot_times: cut ? 1234 : 2, truncated: cut };
    }
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
    server: u.searchParams.get("server"),
    hours: u.searchParams.get("hours"),
    as_of: u.searchParams.get("as_of"),
    hash: u.searchParams.get("query_hash"),
    query_id: u.searchParams.get("query_id"),
    database_name: u.searchParams.get("database_name"),
    procedure_name: u.searchParams.get("procedure_name"),
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
  modules = { util: await load("util.js"), tabs: await load(path.join("pages", "server-tabs.js")), panels: await load("panels.js"), slicer: await load("time-slicer.js") };
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

/* 11. The Blocking tab (a server of its own) draws two strips, the blocked process report counts and the deadlock counts, each from the trend the tab
   already reads. A drag on EITHER narrows the Blocking and Deadlocks grids, the deadlock graphs and the report XML together. */
asks.length = 0;
const broot = await draw(ctxOf(), "blocking", "c");
const blockingReads = ["get_blocking", "get_deadlocks", "get_deadlock_detail", "get_blocked_process_xml"];
out.blockingTab = {
  slicers: byClass(broot, "slicer").map((n) => n.attrs["data-slicer"]),
  stripsDrawn: byClass(broot, "slicer-line").length > 0 && byClass(broot, "slicer-line").every((n) => (n.attrs.d || "").startsWith("M")),
  trendReads: ["get_blocking_trend", "get_deadlock_trend"].map((n) => asks.filter((a) => a.name === n && !a.as_of).length),
  grids: Object.fromEntries(blockingReads.map((n) => [n, lastAsk(n)])),
};
asks.length = 0;
await drag(broot, 250, 500, "Deadlocks");
out.blockingDragged = { grids: Object.fromEntries(blockingReads.map((n) => [n, lastAsk(n)])), expectedAsOf: new Date(START + 12 * HOUR).toISOString() };
asks.length = 0;
byClass(slicerOf(broot, "Blocked process reports"), "slicer-reset")[0].fire("click", {});
await flush();
out.blockingReset = { grids: Object.fromEntries(blockingReads.map((n) => [n, lastAsk(n)])) };

/* 12. Picking a Top Procedures row overlays THAT procedure's own history (get_procedure_duration_trend limited to it) over the page range. */
asks.length = 0;
const proot = await draw(ctxOf(), "cpu", "d");
asks.length = 0;
const procRow = all(proot).find((n) => n.tag === "tr" && n.attrs["aria-pressed"] === "false" && /dbo\.p1/.test(n.textContent));
procRow.fire("click", { target: new FakeNode("button") });
await flush();
const procButtonPicked = asks.some((a) => a.name === "get_procedure_duration_trend");
procRow.fire("click", {});
await flush();
const procStrip = slicerOf(proot, "Procedure duration");
out.procedureOverlay = {
  buttonPicked: procButtonPicked,
  ask: lastAsk("get_procedure_duration_trend", (a) => a.procedure_name),
  path: byClass(procStrip, "slicer-overlay")[0].attrs.d || "",
  note: byClass(procStrip, "slicer-overlay-note")[0].textContent,
  selected: / row-selected/.test(" " + procRow.className),
};

/* 13. A History button reads the grid's EFFECTIVE window: the page range while nothing is narrowed, the narrowed window (hours and as_of) while a
   selection is held. A query's and a procedure's button each read their own history. */
const historyOf = async (root, rowText, name, filter = () => true) => {
  const tr = all(root).find((n) => n.tag === "tr" && rowText.test(n.textContent));
  const button = all(tr).find((n) => n.tag === "button" && n.textContent === "History");
  asks.length = 0;
  button.fire("click", {});
  await flush();
  button.fire("click", {});
  await flush();
  return lastAsk(name, filter);
};
const hroot = await draw(ctxOf(), "cpu", "e");
out.historyWhole = {
  queries: await historyOf(hroot, /select 1/, "get_query_trend", (a) => a.hash === "0x1"),
  procedures: await historyOf(hroot, /dbo\.p1/, "get_procedure_duration_trend", (a) => a.procedure_name),
};
await drag(hroot, 250, 500);
out.historyNarrowed = {
  queries: await historyOf(hroot, /select 1/, "get_query_trend", (a) => a.hash === "0x1"),
  procedures: await historyOf(hroot, /dbo\.p1/, "get_procedure_duration_trend", (a) => a.procedure_name),
  expectedAsOf: new Date(START + 12 * HOUR).toISOString(),
};

/* 14. The Query Store History button follows the narrowing the same way. */
const sroot = await draw(ctxOf(), "queries", "f");
await drag(sroot, 250, 500, "Query Store duration");
out.storeHistoryNarrowed = { ask: await historyOf(sroot, /select qs/, "get_query_store_query_history"), expectedAsOf: new Date(START + 12 * HOUR).toISOString() };

/* ───────────── review round 1 (#6301) ───────────── */
const pageText = (root) => all(root).filter((n) => n.tag === "tr").map((n) => n.textContent).join("\n");
const countOf = (name, server, filter = () => true) => asks.filter((a) => a.name === name && a.server === server && filter(a)).length;

/* 15. Rebuilding the page (the 60 s poll aborts the panel signal and builds every card again) leaves no listener behind: after six rebuilds a drag
   on the live card re-reads each grid ONCE, not once per dead card. */
let prevSignal = null;
let live = null;
const dead = [];
for (let i = 0; i < 6; i++) {
  if (live) dead.push({ root: live, title: all(live).find((n) => n.tag === "h3" && n.textContent.startsWith("Top Queries")) });
  if (prevSignal) prevSignal.abort();
  prevSignal = new AbortController();
  modules.panels.setPanelSignal(prevSignal.signal);
  live = await draw(ctxOf(), "cpu", "g");
}
asks.length = 0;
await drag(live, 250, 500);
/* A listener left by a dead card would draw that card's grid again (the same reads are joined into one fetch, so the cards' own nodes tell). */
out.rebuilds = { redrawnDeadCards: dead.filter((d) => !all(d.root).includes(d.title)).length, deadCards: dead.length, liveReads: countOf("get_top_queries_by_cpu", "g") };
modules.panels.setPanelSignal(undefined);

/* 16. The Blocking and Deadlocks grids list only the events inside the selection (the read itself is whole hours back from its end); unnarrowed they list all. */
asks.length = 0;
const eroot = await draw(ctxOf(), "blocking", "m");
out.eventsWhole = pageText(eroot);
await drag(eroot, 250, 500, "Deadlocks");
out.eventsNarrowed = pageText(eroot);

/* 17. A press with no travel on the selection, and Escape with nothing narrowed, change nothing: no grid reads again. */
const nroot = await draw(ctxOf(), "cpu", "n");
await drag(nroot, 250, 500);
asks.length = 0;
const nbody = byClass(nroot, "slicer-sel")[0];
nbody.fire("pointerdown", { clientX: 375, button: 0, pointerId: 2 });
byClass(nroot, "slicer-track")[0].fire("pointerup", { clientX: 375, pointerId: 2 });
await flush();
out.noTravel = countOf("get_top_queries_by_cpu", "n") + countOf("get_top_procedures_by_cpu", "n");
byClass(nroot, "slicer-reset")[0].fire("click", {});
await flush();
asks.length = 0;
byClass(nroot, "slicer-sel")[0].fire("keydown", { key: "Escape" });
await flush();
out.escapeIdle = countOf("get_top_queries_by_cpu", "n") + countOf("get_top_procedures_by_cpu", "n");

/* 18. N narrowings, then one Compare change: each card draws once for it, as it would with no narrowing. */
const compareChange = async (root, value) => {
  asks.length = 0;
  const select = all(root).find((n) => n.tag === "select" && n.attrs["aria-label"] === "Compare");
  select.value = value;
  select.fire("change", {});
  await flush();
  return countOf("get_top_queries_by_cpu", "j") + countOf("get_top_procedures_by_cpu", "j");
};
const croot = await draw(ctxOf(), "cpu", "j");
const compareBase = await compareChange(croot, "yesterday");
await compareChange(croot, "off");
await drag(croot, 100, 300);
await drag(croot, 150, 400);
await drag(croot, 250, 500);
out.compare = { once: compareBase, afterNarrowings: await compareChange(croot, "yesterday") };
await compareChange(croot, "off");

/* 19. The Active strip says so when the snapshot list was cut; a whole list says nothing. */
const kroot = await draw(ctxOf(), "queries", "k");
out.activeNote = { cut: byClass(slicerOf(kroot, "Active queries"), "slicer-strip-note")[0].textContent, whole: byClass(slicerOf(qroot, "Active queries"), "slicer-strip-note")[0].textContent };

/* 20. A row's overlay is read once: narrowing (a grid redraw that puts the pick back on its row) draws it from the first read. */
const oroot = await draw(ctxOf(), "cpu", "o");
asks.length = 0;
all(oroot).find((n) => n.tag === "tr" && n.attrs["aria-pressed"] === "false" && /select 1/.test(n.textContent)).fire("click", {});
await flush();
const overlayFirst = countOf("get_query_trend", "o");
asks.length = 0;
await drag(oroot, 250, 500);
out.overlayReads = { first: overlayFirst, afterNarrowing: countOf("get_query_trend", "o"), stillDrawn: (byClass(oroot, "slicer-overlay")[0].attrs.d || "").startsWith("M") };

/* 21. A held narrowing is clamped to a tab's reach, never dropped by it: the same page range read by a tab that reaches only the last 12 hours
   shows the part of the selection inside that reach (or none), and the longer tab has it back. */
const S = modules.slicer;
const longCtx = { hours: 24, startMs: START, endMs: END, rangeKey: "preset:24" };
const shortCtx = { hours: 12, startMs: END - 12 * HOUR, endMs: END, rangeKey: "preset:24" };
S.setSlice("r", "preset:24", { startMs: START + 6 * HOUR, endMs: START + 18 * HOUR });
const onShort = S.effectiveContext("r", shortCtx, END);
const outsideShort = S.effectiveContext("r", { ...shortCtx, startMs: END - 4 * HOUR, hours: 4 }, END);
out.reach = {
  shortNarrowed: onShort.narrowed === true,
  shortStart: onShort.startMs === END - 12 * HOUR ? "clamped" : onShort.startMs,
  heldAfter: !!S.getSlice("r", "preset:24"),
  longAgain: S.effectiveContext("r", longCtx, END).narrowed === true,
  outside: outsideShort.narrowed === true,
};

console.log(JSON.stringify(out));
process.exit(0);
