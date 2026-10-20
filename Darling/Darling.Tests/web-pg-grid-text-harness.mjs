/* #6154: runs the web dashboard's real table renderer (panels.js) and the short cell words (pg-grid-text.js) on a small fake DOM.
   WebPgGridTextTests starts it as
       node web-pg-grid-text-harness.mjs <path to the js folder>
   with HARNESS_INPUT = JSON {bloat:[text], droppability:[text], cost:[text], lastScan:[text]} written by the real C# reads.
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
globalThis.location = { hash: "#/server/a/storage" };

const root = process.argv[2];
const { VIZ } = await import(pathToFileURL(root + "/panels.js").href);
const g = await import(pathToFileURL(root + "/pg-grid-text.js").href);
const input = JSON.parse(process.env.HARNESS_INPUT);
const out = {
  bloat: input.bloat.map(g.bloatVerdict),
  droppability: input.droppability.map(g.droppabilityVerdict),
  cost: input.cost.map(g.costFigure),
  lastScan: input.lastScan.map(g.lastScanNoteWord),
  unknownSentence: g.droppabilityVerdict("A sentence the page has no word for yet. And more."),
  none: [g.bloatVerdict(null), g.droppabilityVerdict(""), g.costFigure(null)],
};

/* One real grid: the short word in the cell, the sentence behind a click, the shared explanation in the header tooltip. */
const rows = input.droppability.map((text, i) => ({ index: "ix" + i, droppability_finding: text }));
const word = (r) => g.droppabilityVerdict(r.droppability_finding);
const t = VIZ.table({ rows }, {
  rowsKey: "rows", title: "Index usage", tools: false,
  columns: [
    { key: "index", label: "Index" },
    { key: "droppability_finding", label: "Can It Go?", hint: "Whether the index could be dropped.", display: (r) => word(r) ?? "—", sortValue: word, detailKey: "droppability_finding" },
  ],
});
const th = t.findAll((n) => n.tag === "th").map((n) => ({ text: n.textContent, title: n.attrs.title || null }));
const body = t.findAll((n) => n.tag === "tr").filter((n) => n.parent && n.parent.tag === "tbody");
out.headers = th;
out.cells = body.map((tr) => {
  const td = tr.children[1];
  const summary = td.findAll((n) => n.tag === "summary")[0];
  const detail = td.findAll((n) => String(n.className).includes("disc-body"))[0];
  return { shown: summary ? summary.textContent : td.textContent, detail: detail ? detail.textContent : null, hasDetails: td.findAll((n) => n.tag === "details").length };
});
console.log(JSON.stringify(out));
