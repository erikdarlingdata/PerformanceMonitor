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

console.log(JSON.stringify(out));
