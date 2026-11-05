/* #7552: runs the shipped grids (panels.js VIZ.table) on a small fake DOM and prints one line of JSON: the status cell of a review row on both
   Configuration Audits, the alignment class of three number columns, and the text of id cells. WebDisplayDetailsBehaviourTests starts it as
       node web-display-details-harness.mjs <path to the js folder>
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
const { AUDIT_COLUMNS, IDENTITY_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/sql-config.js").href);
const { PG_AUDIT_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/pg-config.js").href);
const { PG_INDEX_BLOAT_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/pg-storage.js").href);
const { PG_SESSION_STATE_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/pg-vacuum.js").href);
const { QUERY_STORE_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/sql-queries.js").href);

const draw = (columns, rows) => VIZ.table({ rows }, { rowsKey: "rows", title: "t", tools: false, columns });
const headers = (t) => t.findAll((n) => n.tag === "th").map((n) => n.textContent.replace(/[^A-Za-z %]/g, "").trim());
const cellOf = (columns, rows, label) => {
  const t = draw(columns, rows);
  const at = headers(t).indexOf(label);
  const tr = t.findAll((n) => n.tag === "tr").filter((r) => r.findAll((n) => n.tag === "td").length > 0)[0];
  const td = tr.findAll((n) => n.tag === "td")[at];
  return { cls: td.className, text: td.textContent };
};
const out = {};
out.sqlReview = cellOf(AUDIT_COLUMNS, [{ setting: "max server memory (MB)", status: "review", current_value: "9999999" }], "Status");
out.pgReview = cellOf(PG_AUDIT_COLUMNS, [{ setting: "work_mem", status: "review", current_value: "4" }], "Status");
out.sqlOk = cellOf(AUDIT_COLUMNS, [{ setting: "x", status: "ok", current_value: "1" }], "Status");
out.identityRemaining = cellOf(IDENTITY_COLUMNS, [{ database: "d", table: "t", column: "c", values_remaining: "1288348578" }], "Values remaining");
out.bloatSize = cellOf(PG_INDEX_BLOAT_COLUMNS, [{ schema_name: "s", table_name: "t", index_name: "i", index_bytes: 209715, estimated_reclaimable_bytes: null }], "Size");
out.bloatReclaimable = cellOf(PG_INDEX_BLOAT_COLUMNS, [{ schema_name: "s", table_name: "t", index_name: "i", index_bytes: 209715, estimated_reclaimable_bytes: null }], "Reclaimable");
out.pid = cellOf(PG_SESSION_STATE_COLUMNS, [{ pid: 5828 }], "PID");
out.queryId = cellOf(QUERY_STORE_COLUMNS, [{ query_id: 5568, plan_id: 7712 }], "Query ID");
out.planId = cellOf(QUERY_STORE_COLUMNS, [{ query_id: 5568, plan_id: 7712 }], "Plan ID");
console.log(JSON.stringify(out));
