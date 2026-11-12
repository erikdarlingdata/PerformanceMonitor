/* #7926: renders the PostgreSQL Vacuum tab's Horizon Holders grid (PG_XMIN_COLUMNS from pg-vacuum.js, through the real
   table renderer in panels.js) on a small fake DOM, from a get_pg_xmin_horizon answer the real tool wrote.
   WebPgXminGridTests starts it as
       node web-pg-xmin-grid-harness.mjs <path to the js folder>
   with HARNESS_INPUT = the tool's JSON text, and prints the header labels with their tooltips and every row's cells by header.
   Only the DOM is a stand-in. */
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
  findAll(pred, out = []) { if (pred(this)) out.push(this); this.children.forEach((c) => c.findAll(pred, out)); return out; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/a/vacuum" };

const root = process.argv[2];
const { VIZ } = await import(pathToFileURL(root + "/panels.js").href);
const { PG_XMIN_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/pg-vacuum.js").href);
const data = JSON.parse(process.env.HARNESS_INPUT);

/* The same grid the Vacuum tab mounts (server-tabs.js): the tool's answer as the data, "holders" as the rows key. */
const t = VIZ.table(data, { rowsKey: "holders", title: "Horizon Holders", tools: false, columns: PG_XMIN_COLUMNS });
const headers = t.findAll((n) => n.tag === "th").map((n) => ({ text: n.textContent, title: n.attrs.title || null }));
const body = t.findAll((n) => n.tag === "tr").filter((n) => n.parent && n.parent.tag === "tbody");
const rows = body.map((tr) => {
  const cells = {};
  tr.children.forEach((td, i) => { if (headers[i]) cells[headers[i].text] = td.textContent; });
  return cells;
});
console.log(JSON.stringify({ headers, rows }));
