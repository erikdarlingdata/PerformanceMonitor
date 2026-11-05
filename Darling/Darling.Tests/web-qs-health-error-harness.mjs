/* #7675: runs the shipped Query Store Health grid (sql-config.js QS_HEALTH_COLUMNS through panels.js VIZ.table) on a small fake DOM and prints one line of JSON:
   the Actual and Read-only reason cells drawn for a store in ERROR and for a READ_WRITE one. WebQueryStoreHealthErrorBehaviourTests starts it as
       node web-qs-health-error-harness.mjs <path to the js folder>
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
  findAll(pred, out = []) { if (pred(this)) out.push(this); this.children.forEach((c) => c.findAll(pred, out)); return out; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/a/config" };

const root = process.argv[2];
const { VIZ } = await import(pathToFileURL(root + "/panels.js").href);
const { QS_HEALTH_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/sql-config.js").href);

const rows = [
  { database_name: "ErrDb", actual_state: "ERROR", desired_state: "OFF", state_matches_desired: false, readonly_reason_decoded: "Query Store stopped on an internal error. Its newest rows are old. Turn it off and on again, or run sp_query_store_consistency_check.", current_storage_size_mb: 688, max_storage_size_mb: 1000 },
  { database_name: "RwDb", actual_state: "READ_WRITE", desired_state: "READ_WRITE", state_matches_desired: true, readonly_reason_decoded: null, current_storage_size_mb: 10, max_storage_size_mb: 1000 },
];
const t = VIZ.table({ rows }, { rowsKey: "rows", title: "t", tools: false, columns: QS_HEALTH_COLUMNS });
const heads = t.findAll((n) => n.tag === "th").map((n) => n.textContent.replace(/[^A-Za-z -]/g, "").trim());
const bodyRows = t.findAll((n) => n.tag === "tr").filter((tr) => tr.findAll((n) => n.tag === "td").length > 0);
const cell = (tr, label) => {
  const td = tr.findAll((n) => n.tag === "td")[heads.indexOf(label)];
  return { text: td.textContent, classes: [...td.classes, ...td.findAll(() => true).flatMap((n) => [...n.classes])] };
};
console.log(JSON.stringify(bodyRows.map((tr) => ({ actual: cell(tr, "Actual"), reason: cell(tr, "Read-only reason") }))));
