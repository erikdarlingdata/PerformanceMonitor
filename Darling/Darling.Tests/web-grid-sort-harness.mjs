/* #4843: runs the web dashboard's real shared table renderer (wwwroot/js/panels.js, VIZ.table) on a small fake DOM and
   prints what a scenario drew as one line of JSON. GridSortBehaviourTests starts it as
       node web-grid-sort-harness.mjs <path to the js folder> <scenario>
   Only the DOM is a stand-in; panels.js and util.js are the shipped modules. */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, ...e }); }
  find(tag) { for (const c of this.children) { if (c.tag === tag) return c; const f = c.find && c.find(tag); if (f) return f; } return null; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/a/queries" };

const root = process.argv[2];
const { VIZ, reapplyGridSort, gridRowOf } = await import(pathToFileURL(root + "/panels.js").href);

const cols = [
  { key: "name", label: "Name" },
  { key: "n", label: "N", format: "int" },
  { key: "t", label: "When", format: "time" },
  { key: "locked", label: "Fixed", format: "int", sortable: false },
];
const rows = [
  { name: "beta", n: 10, t: "2026-01-02T00:00:00Z", locked: 1 },
  { name: "Alpha", n: 9, t: "2026-01-10T00:00:00Z", locked: 2 },
  { name: null, n: null, t: null, locked: 3 },
  { name: "alpha", n: 100, t: "2026-01-03T00:00:00Z", locked: 4 },
  { name: "", n: 2, t: "2026-01-01T00:00:00Z", locked: 5 },
];
const build = (desc = {}) => VIZ.table({ rows }, { rowsKey: "rows", columns: cols, title: "T1", ...desc });
const ths = (w) => w.find("table").find("thead").find("tr").children;
const bodyCol = (w, i) => w.find("table").children[1].children.map((tr) => tr.children[i].textContent);
const click = (w, i) => ths(w)[i].fire("click");
const out = {};

const scenarios = {
  cycle() {
    const w = build();
    out.initial = bodyCol(w, 1);
    click(w, 1); out.asc = bodyCol(w, 1); out.ascSort = ths(w)[1].getAttribute("aria-sort"); out.ascInd = ths(w)[1].children[1].textContent;
    click(w, 1); out.desc = bodyCol(w, 1); out.descSort = ths(w)[1].getAttribute("aria-sort"); out.descInd = ths(w)[1].children[1].textContent;
    click(w, 1); out.back = bodyCol(w, 1); out.backSort = ths(w)[1].getAttribute("aria-sort");
    out.tabindex = ths(w)[1].getAttribute("tabindex");
  },
  text() {
    const w = build();
    click(w, 0); out.asc = bodyCol(w, 3); // Fixed column carries the original index (1-based) as a stable identity
    click(w, 0); out.desc = bodyCol(w, 3);
  },
  time() {
    const w = build();
    click(w, 2); out.asc = bodyCol(w, 3);
    click(w, 2); out.desc = bodyCol(w, 3);
  },
  keyboard() {
    const w = build();
    ths(w)[1].fire("keydown", { key: "Enter" }); out.enter = bodyCol(w, 1);
    ths(w)[1].fire("keydown", { key: " " }); out.space = bodyCol(w, 1);
    ths(w)[1].fire("keydown", { key: "x" }); out.other = bodyCol(w, 1);
  },
  rebuild() {
    const w = build(); click(w, 1); click(w, 1);
    const again = build();
    out.sameKey = bodyCol(again, 1); out.sameKeySort = ths(again)[1].getAttribute("aria-sort");
    const other = build({ title: "T2" });
    out.otherKey = bodyCol(other, 1);
    location.hash = "#/server/b/queries";
    out.otherRoute = bodyCol(build(), 1);
    location.hash = "#/server/a/queries?x=1";
    out.sameRouteWithQuery = bodyCol(build(), 1);
  },
  /* Alert History's shape: the body is reconciled in place into the server's order, then the sort is re-applied. */
  reapply() {
    const w = build(); click(w, 1); // ascending by N
    const tbody = w.find("table").children[1];
    out.rowsOf = tbody.children.map((tr) => gridRowOf(tr).name);
    const mk = (row) => { const t = VIZ.table({ rows: [row] }, { rowsKey: "rows", columns: cols, title: "T9" }); return t.find("table").children[1].children[0]; };
    // the server now sends the new row (n=50) first; the reconcile puts every tr back in that order
    const fresh = mk({ name: "gamma", n: 50, t: null, locked: 6 });
    const trs = [...tbody.children];
    for (const tr of [fresh, ...trs]) tbody.appendChild(tr);
    out.serverOrder = bodyCol(w, 1);
    reapplyGridSort(tbody);
    out.sorted = bodyCol(w, 1);
    out.freshRow = gridRowOf(fresh).name;
    click(w, 1); click(w, 1); // desc, then cleared
    out.cleared = bodyCol(w, 1);
    reapplyGridSort(tbody);
    out.clearedAfterReapply = bodyCol(w, 1);
    reapplyGridSort({}); // not a grid: ignored
  },
  /* sortValue wins over a text-typed key; object-valued and later-filled columns decide per the rows. */
  columns() {
    const c2 = [
      { key: "size_text", label: "Size", align: "right", sortValue: (r) => r.size },
      { key: "max", label: "Max", align: "right", sortValue: (r) => (r.max === -1 ? Infinity : r.max), render: (r) => new FakeText(String(r.max)) },
      { key: "list", label: "List" },
      { key: "later", label: "Later" },
    ];
    const rs = [
      { size_text: "1,234.500", size: 1234.5, max: 5, list: ["a"], later: null },
      { size_text: "99.000", size: 99, max: -1, list: ["b"], later: null },
      { size_text: "2,000.000", size: 2000, max: 10, list: { x: 1 }, later: null },
    ];
    const w = VIZ.table({ rows: rs }, { rowsKey: "rows", columns: c2.filter((c) => c.key !== "max"), title: "C1" });
    out.cls = ths(w).map((t) => t.className);
    click(w, 0); out.sizeAsc = bodyCol(w, 0);
    const m = VIZ.table({ rows: rs }, { rowsKey: "rows", columns: [c2[1], c2[0]], title: "C2" });
    click(m, 0); click(m, 0); out.maxDesc = bodyCol(m, 1);
    // a column empty on the first mount becomes sortable once the reconciled rows carry values
    const first = VIZ.table({ rows: [{ later: null, k: 1 }, { later: null, k: 2 }] }, { rowsKey: "rows", columns: [{ key: "k", label: "K", format: "int" }, { key: "later", label: "Later" }], title: "C3" });
    out.laterBefore = ths(first)[1].className;
    const tb = first.find("table").children[1];
    tb.children.forEach((tr, i) => { gridRowOf(tr).later = i === 0 ? "zeta" : "alpha"; });
    reapplyGridSort(tb);
    out.laterAfter = ths(first)[1].className;
    click(first, 1);
    out.laterSorted = first.find("table").children[1].children.map((tr) => gridRowOf(tr).later);
  },
  optOut() {
    const w = build(); out.fixedCls = ths(w)[3].className; out.fixedTab = ths(w)[3].getAttribute("tabindex") ?? null;
    const off = build({ sortable: false });
    out.offCls = ths(off).map((t) => t.className);
  },
};
scenarios[process.argv[3]]();
console.log(JSON.stringify(out));
