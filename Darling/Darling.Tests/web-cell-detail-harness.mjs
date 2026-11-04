/* #7482: runs the web dashboard's real shared table renderer (wwwroot/js/panels.js, VIZ.table) on a small fake DOM with a column that
   opens a per-row detail (detailKey), and prints one line of JSON: what opening that detail does to the grid. The opened text of a cell in
   a right-hand column ran past the grid's edge, so opening it must scroll it into view. WebCellDetailLayoutBehaviourTests starts it as
       node web-cell-detail-harness.mjs <path to the js folder>
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
    this._cn = "";
    this.dataset = {};
    this.style = {};
    const set = new Set();
    this.classes = set;
    this.classList = { add: (c) => set.add(c), remove: (c) => set.delete(c), toggle: (c, on) => (on ? set.add(c) : set.delete(c)), contains: (c) => set.has(c) };
  }
  get className() { return this._cn; }
  set className(v) { this._cn = String(v); String(v).split(/\s+/).filter(Boolean).forEach((c) => this.classes.add(c)); }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); if (k === "class") this.className = v; }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t) { for (const fn of this.listeners[t] || []) fn({ type: t }); }
  findAll(pred, out = []) { if (pred(this)) out.push(this); this.children.forEach((c) => c.findAll(pred, out)); return out; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/a/storage" };

const root = process.argv[2];
const { VIZ } = await import(pathToFileURL(root + "/panels.js").href);

const rows = [
  { name: "orders", can_it_go: "Yes", note: "Nothing has read this index since the stats were reset." },
  { name: "items", can_it_go: "No", note: "" },
];
const t = VIZ.table({ rows }, {
  rowsKey: "rows", title: "By Index", tools: false,
  columns: [{ key: "name", label: "Name" }, { key: "can_it_go", label: "Can it go?", detailKey: "note" }],
});
const details = t.findAll((n) => n.tag === "details");
const out = { detailBoxes: details.length };
const box = details[0];
const body = box.findAll((n) => String(n.className).includes("disc-body"))[0];
out.isCellDetail = box.classes.has("cell-detail");

const scrolls = [];
body.scrollIntoView = (o) => scrolls.push(o);
box.open = false;
box.fire("toggle");
out.scrollsWhenClosed = scrolls.length;
box.open = true;
box.fire("toggle");
out.scrollsWhenOpened = scrolls.length;
out.scrollArgs = scrolls[0] || null;
console.log(JSON.stringify(out));
