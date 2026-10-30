/* #5644 W11: runs the web dashboard's real shared table renderer (wwwroot/js/panels.js, VIZ.table) and the clampText helper
   (util.js) on a small fake DOM, and prints what a long-text column drew as one line of JSON. WebClampCellBehaviourTests starts it as
       node web-clamp-cell-harness.mjs <path to the js folder>
   Only the DOM is a stand-in; panels.js and util.js are the shipped modules. */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.listeners = {};
    this.parent = null;
    this._text = "";
    this.className = "";
    this.dataset = {};
    this.style = {};
    const set = new Set();
    this.classes = set;
    this.classList = { add: (c) => set.add(c), remove: (c) => set.delete(c), toggle: (c, on) => (on ? set.add(c) : set.delete(c)), contains: (c) => set.has(c) };
  }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); if (k === "class") this.className = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  contains(n) { for (let x = n; x; x = x.parent) if (x === this) return true; return false; }
  fire(t, e = {}) {
    const ev = { type: t, stopped: false, defaultPrevented: false, stopPropagation() { this.stopped = true; }, preventDefault() { this.defaultPrevented = true; }, ...e };
    for (let n = this; n && !ev.stopped; n = n.parent) for (const fn of n.listeners[t] || []) fn(ev);
    return ev;
  }
  findAll(pred, out = []) { if (pred(this)) out.push(this); this.children.forEach((c) => c.findAll(pred, out)); return out; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/a/health" };

const root = process.argv[2];
const { VIZ } = await import(pathToFileURL(root + "/panels.js").href);
const { clampText } = await import(pathToFileURL(root + "/util.js").href);

const longNote = "The collector read 0 items. " + "Nothing matched the filter on this server. ".repeat(20);
const cols = [
  { key: "collector", label: "Collector" },
  { key: "note", label: "Note", clamp: true, plain: true },
  { key: "plain", label: "Plain", wrap: true },
];
const rows = [
  { collector: "a", note: longNote, plain: longNote },
  { collector: "b", note: "short", plain: "short" },
];
const out = {};

const t = VIZ.table({ rows }, { rowsKey: "rows", columns: cols, title: "Collectors", tools: false });
const tr = t.findAll((n) => n.tag === "tr").filter((n) => n.parent && n.parent.tag === "tbody");
let rowClicks = 0;
tr[0].addEventListener("click", () => rowClicks++);
const tds = (r) => r.children;
const noteTd = tds(tr[0])[1];
const clampOf = (td) => td.findAll((n) => String(n.className).includes("clamp-text"))[0];
const longClamp = clampOf(noteTd);
out.noteTdClass = noteTd.className;
out.plainTdClass = tds(tr[0])[2].className;
out.longHasFullText = longClamp.textContent === longNote;
out.longTitleFull = longClamp.attrs.title === longNote;
out.longRole = longClamp.attrs.role;
out.longExpandedBefore = longClamp.attrs["aria-expanded"];
const ev = longClamp.fire("click");
out.afterClick = { expanded: longClamp.attrs["aria-expanded"], open: longClamp.classes.has("open"), stopped: ev.stopped, rowClicks };
longClamp.fire("click");
out.afterSecondClick = { expanded: longClamp.attrs["aria-expanded"], open: longClamp.classes.has("open") };
const key = longClamp.fire("keydown", { key: "Enter" });
out.afterEnter = { expanded: longClamp.attrs["aria-expanded"], prevented: key.defaultPrevented, stopped: key.stopped };
const other = longClamp.fire("keydown", { key: "a" });
out.otherKey = { expanded: longClamp.attrs["aria-expanded"], stopped: other.stopped };

const shortClamp = clampOf(tds(tr[1])[1]);
out.short = { role: shortClamp.attrs.role || null, tabindex: shortClamp.attrs.tabindex || null, text: shortClamp.textContent };
const fixed = clampText(longNote, { expandable: false });
out.notExpandable = { role: fixed.attrs.role || null, text: fixed.textContent === longNote, title: fixed.attrs.title === longNote };
out.empty = { text: clampText(null).textContent, title: clampText(null).attrs.title || null };
out.multiline = clampText("one\ntwo").attrs.role;

/* #5644 W20: only a column marked localizeText rewrites an ISO instant in its text; SQL text and details are data and print as written. */
const sqlText = "SELECT 1 FROM t WHERE d >= '2026-01-01T00:00:00'";
const msgText = "not collected since 2026-10-09T14:37:35Z";
const l = VIZ.table({ rows: [{ query_text: sqlText, details: msgText, message: msgText, collection_time: "2026-10-09T14:37:35Z" }] }, {
  rowsKey: "rows", title: "Localize", tools: false,
  columns: [
    { key: "query_text", label: "Query", wrap: true },
    { key: "details", label: "Details", wrap: true },
    { key: "message", label: "Message", wrap: true, localizeText: true },
    { key: "message", label: "Clamped message", clamp: true, localizeText: true },
  ],
});
const lr = l.findAll((n) => n.tag === "tr").filter((n) => n.parent && n.parent.tag === "tbody")[0];
const ltd = lr.children.map((c) => c.textContent);
out.localize = {
  sqlUnchanged: ltd[0] === sqlText,
  detailsUnchanged: ltd[1] === msgText,
  messageLocal: ltd[2],
  clampedLocal: ltd[3],
  expected: "not collected since " + new Date("2026-10-09T14:37:35Z").toLocaleString(),
};

/* H2 (#5644 W20 leftovers): the shapes the alert text, the sweep notes and the collection-health regression text use. Each is localized in an
   opted-in cell and printed as written in a cell that is not opted in; a SQL text cell holding a Z-marked literal stays as written. */
const shapes = [
  { raw: "until 2026-10-11 12:48:52Z", stamp: "2026-10-11T12:48:52Z", lead: "until " },
  { raw: "service started 2026-10-12T23:01:13.7792072Z", stamp: "2026-10-12T23:01:13.779Z", lead: "service started " },
  { raw: "Failed At: 2026-10-12 07:00:00Z", stamp: "2026-10-12T07:00:00Z", lead: "Failed At: " },
  { raw: "Incident Since: 2026-10-12 23:13:00 UTC", stamp: "2026-10-12T23:13:00Z", lead: "Incident Since: " },
  { raw: "at 2026-10-12 23:13:00.1234567Z", stamp: "2026-10-12T23:13:00.123Z", lead: "at " },
];
const sqlZ = "SELECT * FROM t WHERE d = '2026-01-01 00:00:00Z'";
const serverClock = "start time 2026-09-27 21:32:43 (server clock)";
const l2 = VIZ.table({ rows: [...shapes.map((x) => ({ query_text: sqlZ, message: x.raw, other: x.raw })), { query_text: sqlZ, message: serverClock, other: serverClock }] }, {
  rowsKey: "rows", title: "Localize shapes", tools: false,
  columns: [
    { key: "query_text", label: "Query", wrap: true },
    { key: "other", label: "Other", wrap: true },
    { key: "message", label: "Message", wrap: true, localizeText: true },
    { key: "message", label: "Clamped message", clamp: true, localizeText: true },
  ],
});
const l2rows = l2.findAll((n) => n.tag === "tr").filter((n) => n.parent && n.parent.tag === "tbody");
out.shapes = l2rows.map((tr, i) => {
  const t = tr.children.map((c) => c.textContent);
  return { sql: t[0], notOpted: t[1], opted: t[2], clamped: t[3], expected: i < shapes.length ? shapes[i].lead + new Date(shapes[i].stamp).toLocaleString() : null, raw: i < shapes.length ? shapes[i].raw : serverClock };
});
out.sqlZ = sqlZ;

/* #5644 W20 (H1): get_alert_history's detail_text opts in with localizeAlertDetail: the labelled time lines shift, the sample-age line
   shifts from the row's newest_sample, and the SQL lines (and a not-opted-in copy of the same text) print as written. */
const detail = "Incident Since: 2026-10-12 23:13:00Z\nNewest blocked process sample: 23:12:45 UTC, 5m 20s old\nBlocked Query: SELECT * FROM t WHERE d = '2026-01-01 00:00:00Z'";
const l3 = VIZ.table({ rows: [{ detail_text: detail, newest_sample: { instant_utc: "2026-10-12T23:12:45Z" } }] }, {
  rowsKey: "rows", title: "Alert detail", tools: false,
  columns: [
    { key: "detail_text", label: "Detail", wrap: true },
    { key: "detail_text", label: "Detail (opted in)", wrap: true, localizeAlertDetail: true },
  ],
});
const l3tr = l3.findAll((n) => n.tag === "tr").filter((n) => n.parent && n.parent.tag === "tbody")[0];
const l3t = l3tr.children.map((c) => c.textContent);
out.alertDetail = {
  notOpted: l3t[0],
  raw: detail,
  opted: l3t[1],
  sinceLocal: "Incident Since: " + new Date("2026-10-12T23:13:00Z").toLocaleString(),
  sampleLocal: "Newest blocked process sample: " + new Date("2026-10-12T23:12:45Z").toLocaleString() + ", 5m 20s old",
  sqlLine: "Blocked Query: SELECT * FROM t WHERE d = '2026-01-01 00:00:00Z'",
};

/* #6649: the cell's `plain` modes through the shared renderer. "counts" (the Collection Log's Error column) says the measurement counts in
   words and leaves a tool name in a real error message; true is the full pass (a tool name becomes its tab); "named" puts only the
   named tokens into words; no key prints as stored. */
const modeNote = "Connection failed (timeout=30): ring_buffer_truncated=1 ring_buffer_events_hidden=167 newest_visible_event_age_s=4300; ask get_collection_health for hours_back";
const pm = VIZ.table({ rows: [{ note: modeNote }] }, {
  rowsKey: "rows", title: "Plain modes", tools: false,
  columns: [
    { key: "note", label: "Counts", wrap: true, plain: "counts" },
    { key: "note", label: "Full", wrap: true, plain: true },
    { key: "note", label: "Named", wrap: true, plain: "named" },
    { key: "note", label: "As stored", wrap: true },
  ],
});
const pmr = pm.findAll((n) => n.tag === "tr").filter((n) => n.parent && n.parent.tag === "tbody")[0];
const pmt = pmr.children.map((c) => c.textContent);
out.plainModes = { raw: modeNote, counts: pmt[0], full: pmt[1], named: pmt[2], asStored: pmt[3] };

console.log(JSON.stringify(out));
