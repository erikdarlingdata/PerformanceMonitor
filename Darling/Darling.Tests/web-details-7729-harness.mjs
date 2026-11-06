/* #7729: runs the shipped util.js, panels.js and the Default Trace, Query Store Regressions and Query Store Overhead lists on a small fake DOM and
   prints what they show as one line of JSON.
   WebDetails7729BehaviourTests starts it as
       node web-details-7729-harness.mjs <path to the js folder>
   Only the DOM (and a ResizeObserver that runs its callback on demand) is a stand-in. */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.style = {}; this.classList = { add: (c) => { if (!this.className.split(" ").includes(c)) this.className = (this.className + " " + c).trim(); }, remove: (c) => { this.className = this.className.split(" ").filter((x) => x !== c).join(" "); } }; this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.value = ""; }
  get parentNode() { return this.parent; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  click() { this.fire("click", { target: this }); }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, ...e }); }
  focus() { globalThis.document.activeElement = this; this.fire("focus"); }
  contains(n) { for (; n; n = n.parent) if (n === this) return true; return false; }
  find(tag) { for (const c of this.children) { if (c.tag === tag) return c; const f = c.find && c.find(tag); if (f) return f; } return null; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
const body = new FakeNode("body");
globalThis.document = { activeElement: null, body, createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t), querySelectorAll: () => [] };
globalThis.location = { hash: "#/server/a/queries" };

const root = process.argv[2];
const imp = (f) => import(pathToFileURL(root + "/" + f).href);
const util = await imp("util.js");
const panels = await imp("panels.js");
const dt = await imp("pages/server-tabs/sql-events.js");
const qs = await imp("pages/server-tabs/sql-queries.js");
const blocking = await imp("pages/server-tabs/sql-blocking.js");
const templates = await imp("view-templates.js");

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const norm = (t) => t.replace(/[▲▼▾ ]/g, "");
/* The text of one column's cell in every row, found by the header's label. */
const cellsOf = (grid, label) => {
  const table = all(grid).find((n) => n.tag === "table");
  const heads = table.find("thead").find("tr").children.map((h) => norm(h.textContent));
  const at = heads.indexOf(norm(label));
  return at < 0 ? { missing: heads } : table.children[1].children.map((tr) => tr.children[at].textContent);
};

const out = {};

/* Item 4: a whole value prints without the decimal, a fractional one keeps its place. */
out.num1 = [1, 309, 2485, 1.5, 309.04, 0, null, 12.25].map((v) => util.FORMATTERS.num1(v));

/* Item 10: severity first (CRITICAL, HIGH, MEDIUM, LOW), then the added duration, most first. */
const regs = [
  { severity: "LOW", additional_duration_ms: 900, query_id: 1 },
  { severity: "HIGH", additional_duration_ms: 100, query_id: 2 },
  { severity: "LOW", additional_duration_ms: 5000, query_id: 3 },
  { severity: "HIGH", additional_duration_ms: 700, query_id: 4 },
  { severity: null, additional_duration_ms: 99999, query_id: 5 },
  { severity: "CRITICAL", additional_duration_ms: 1, query_id: 6 },
  { severity: "MEDIUM", additional_duration_ms: null, query_id: 7 },
];
out.regressionOrder = qs.orderRegressionRows(regs).map((r) => r.query_id);

/* Item 11: a wait with no wait time and no waiting tasks is left out, and the panel then prints its empty sentence. */
const waits = [
  { wait_type: "QDS_LOADDB", wait_ms_total: 0, waiting_tasks_total: 0, wait_ms_per_hour: 0 },
  { wait_type: "QDS_PERSIST_TASK_MAIN_LOOP_SLEEP", wait_ms_total: 12, waiting_tasks_total: 3, wait_ms_per_hour: 0.5 },
];
out.waitsKept = qs.qsWaitsWithDelta(waits).map((w) => w.wait_type);
const waitPanel = (rows) => panels.VIZ.table({ qs_overhead: { wait_stats: { included: rows } } }, {
  rowsKey: "qs_overhead.wait_stats.included", columns: qs.QS_OVERHEAD_WAIT_COLUMNS, orderRows: qs.qsWaitsWithDelta, title: "w",
  emptyText: "No non-sleep QDS_* wait deltas in this window.",
});
out.waitGridEmpty = all(waitPanel([waits[0]])).map((n) => n.textContent).join("|");
out.waitGridRows = cellsOf(waitPanel(waits), "Wait Type");

/* Item 3: the note counts every event of the window, most first. */
out.eventNote = dt.defaultTraceByEventNote({ by_event: [{ event_name: "Server Memory Change", count: 143 }, { event_name: "Object:Altered", count: 3 }] });
out.eventNoteNone = dt.defaultTraceByEventNote({ events: [] });
out.eventNoteMany = dt.defaultTraceByEventNote({ by_event: Array.from({ length: 15 }, (_, i) => ({ event_name: "E" + i, count: 15 - i })) });

/* Item 6: a Healthy row listed only for a tile no band reads says Attention, in the Warning colours; a real Warning row and a Healthy row with no tile do not change. */
const bandRows = [
  { display_name: "a", band: "Healthy", band_label: "Healthy", reason: "Agent alerts 3 gaps", advisory_tiles: ["Agent alerts"] },
  { display_name: "b", band: "Warning", band_label: "Warning", reason: "CPU 90%", advisory_tiles: ["Protection"] },
  { display_name: "c", band: "Healthy", band_label: "Healthy", reason: "", advisory_tiles: [] },
];
const bandDesc = { rowsKey: "w", primaryKey: "display_name", bandKey: "band", bandLabelKey: "band_label", reasonKey: "reason", advisoryKey: "advisory_tiles" };
const bandList = panels.VIZ.bandlist({ w: bandRows }, bandDesc);
out.bandPills = all(bandList).filter((n) => /\bbadge\b/.test(n.className)).map((n) => n.textContent + ":" + n.className);
out.bandDots = all(bandList).filter((n) => /\bdot\b/.test(n.className)).map((n) => n.className);
out.bandPillsWithoutKey = all(panels.VIZ.bandlist({ w: bandRows }, { ...bandDesc, advisoryKey: undefined })).filter((n) => /\bbadge\b/.test(n.className)).map((n) => n.textContent);

/* Item 5: with a ResizeObserver, the tiles take the fewest columns that keep the rows the width gives. 15 tiles at 7 across are 3 rows of 5. */
let observers = [];
globalThis.ResizeObserver = class { constructor(fn) { this.fn = fn; observers.push(this); } observe(el) { this.el = el; } };
globalThis.getComputedStyle = () => ({ columnGap: "12px" });
const statsAt = (count, width) => {
  observers = [];
  const stats = Array.from({ length: count }, (_, i) => ({ key: "k" + i, label: "L" + i }));
  const data = Object.fromEntries(stats.map((s, i) => [s.key, i]));
  const grid = panels.VIZ.stat(data, { stats });
  const target = observers[0] && observers[0].el;
  if (!target) return null;
  target.clientWidth = width;
  observers[0].fn();
  return target.style.gridTemplateColumns;
};
out.tiles = {
  fifteenAt1536: statsAt(15, 1189),
  fifteenAt1280: statsAt(15, 950),
  fifteenAt2560: statsAt(15, 2200),
  tenAt1189: statsAt(10, 1189),
  nineAt1189: statsAt(9, 1189),
  fourAt500: statsAt(4, 500),
  twoTiles: statsAt(2, 1189),
};

/* Item 2: a grid that overflows pins its name column; one that fits does not. */
const pinGrid = (scrollWidth) => {
  observers = [];
  const cols = [{ key: "n", label: "N", format: "int" }, { key: "d", label: "Database", name: true }, { key: "x", label: "X" }];
  const grid = panels.gridTable([{ n: 1, d: "db1", x: "a" }], { columns: cols, title: "t" });
  const wrap = all(grid).find((n) => /\btable-wrap\b/.test(n.className));
  wrap.scrollWidth = scrollWidth; wrap.clientWidth = 1000;
  observers.forEach((o) => o.fn());
  const table = all(wrap).find((n) => n.tag === "table");
  const pinned = all(table).filter((n) => /\bpin-col\b/.test(n.className)).map((n) => n.tag + ":" + n.textContent.replace(/[▲▼▾ ]/g, ""));
  return { wrapClass: wrap.className, pinned };
};
out.pinOverflow = pinGrid(2000);
out.pinFits = pinGrid(1000);
delete globalThis.ResizeObserver;

/* Item 7: no deadlock grid prints the graph's own process id. The Blocking page's summary grid and the saved-view template. */
const templateDeadlockKeys = [];
const walk = (v) => { if (Array.isArray(v)) v.forEach(walk); else if (v && typeof v === "object") { if (v.read === "get_deadlocks" && Array.isArray(v.columns)) templateDeadlockKeys.push(...v.columns.map((c) => c.key)); Object.values(v).forEach(walk); } };
walk(templates.DASHBOARD_TEMPLATES.map((t) => t.make("srv")));
out.deadlockGrids = { page: blocking.DEADLOCK_COLUMNS.map((c) => c.key), template: templateDeadlockKeys };

console.log(JSON.stringify(out));
