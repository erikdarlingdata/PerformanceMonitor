/* #7486: runs the web viewer's real Analysis Findings grid (panels.js VIZ.table over shared.js FINDING_COLUMNS) on a small fake DOM and
   prints, as one line of JSON, the Category cells a person reads and the category map the page carries.
       node web-finding-category-harness.mjs <path to the js folder> */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) {
    this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = "";
    this.dataset = {}; this.style = {};
    const set = new Set();
    this.classList = { add: (c) => set.add(c), remove: (c) => set.delete(c), toggle: (c, on) => (on ? set.add(c) : set.delete(c)), contains: (c) => set.has(c) };
  }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); if (k === "class") this.className = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  findAll(pred, out = []) { if (pred(this)) out.push(this); this.children.forEach((c) => c.findAll(pred, out)); return out; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/a/analysis" };

const root = process.argv[2];
const { VIZ } = await import(pathToFileURL(root + "/panels.js").href);
const { FINDING_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/shared.js").href);
const { READ_FIELDS } = await import(pathToFileURL(root + "/read-fields.js").href);

const row = (category, category_name) => ({ last_seen: "2026-11-01T00:00:00Z", category, category_name, story_path: "X", severity: 1, occurrences: 1, first_seen: "2026-11-01T00:00:00Z", advice: { headline: "Head" } });
const named = [row("bad_actor", "Expensive query"), row("database_config", "Database settings")];
const categoryCells = (grid) => {
  const heads = grid.findAll((n) => n.tag === "th").map((n) => n.textContent.trim());
  const at = heads.findIndex((h) => h.startsWith("Category"));
  const body = grid.findAll((n) => n.tag === "tr").filter((n) => n.parent && n.parent.tag === "tbody");
  return body.map((r) => r.children[at].textContent);
};
const grid = VIZ.table({ rows: [...named, row("weird_new_source")] }, { rowsKey: "rows", columns: FINDING_COLUMNS, title: "Findings", tools: false });
const evidence = VIZ.table({ findings: named }, { ...READ_FIELDS.get_analysis_findings.table, title: "Finding evidence", tools: false });

console.log(JSON.stringify({
  cells: categoryCells(grid),
  evidence: categoryCells(evidence),
}));
