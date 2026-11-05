/* #7725: runs the web dashboard's real shared table renderer (wwwroot/js/panels.js, VIZ.table) on a small fake DOM and prints, as one line of JSON,
   what the Database cell of a row from a database the server no longer lists drew. WebDroppedDatabaseBehaviourTests starts it as
       node web-dropped-database-harness.mjs <path to the js folder>
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

const rows = [
  { database_name: "Sales", full_name: "dbo.usp_Live" },
  { database_name: "OldSales", database_dropped: true, full_name: "dbo.usp_Gone" },
  { database_name: "Other", database_dropped: false, full_name: "dbo.usp_Other" },
];
const t = VIZ.table({ procedures: rows }, {
  rowsKey: "procedures", title: "Top Procedures", tools: false,
  columns: [
    { key: "full_name", label: "Procedure" },
    { key: "database_name", label: "Database", name: true },
  ],
});
const tr = t.findAll((n) => n.tag === "tr").filter((n) => n.parent && n.parent.tag === "tbody");
console.log(JSON.stringify({ databaseCells: tr.map((r) => r.children[1].textContent), procedureCells: tr.map((r) => r.children[0].textContent) }));
