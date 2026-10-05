/* #4843: runs the shipped table renderer's column groups (wwwroot/js/panels.js) on a small fake DOM
   and prints what a scenario did as one line of JSON. ColumnGroupsBehaviourTests starts it as
       node web-column-groups-harness.mjs <path to the js folder> <scenario> */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.style = {}; this.classList = { add: (c) => { this.className = (this.className + " " + c).trim(); }, remove: (c) => { this.className = this.className.split(" ").filter((x) => x !== c).join(" "); } }; this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; }
  get parentNode() { return this.parent; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  click() { this.fire("click", { target: this }); }
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
const body = new FakeNode("body");
let downloads = [];
globalThis.document = { body, createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
const origCreate = URL.createObjectURL;
URL.createObjectURL = (blob) => { downloads.push({ blob }); return "blob:fake"; };
URL.revokeObjectURL = () => {};
const origAppend = body.appendChild.bind(body);
body.appendChild = (a) => { if (a.tag === "a") a.click = () => { downloads[downloads.length - 1].name = a.download; }; return origAppend(a); };
const setNav = (v) => Object.defineProperty(globalThis, "navigator", { value: v, configurable: true, writable: true });
globalThis.location = { hash: "#/server/a/queries" };

const root = process.argv[2];
const { VIZ } = await import(pathToFileURL(root + "/panels.js").href);

const cols = [
  { key: "name", label: "Name" },
  { key: "a", label: "A", format: "int", group: "G1" },
  { key: "b", label: "B", format: "int", group: "G2" },
  { key: "c", label: "C", format: "int", group: "G2" },
];
const rows = [{ name: "x", a: 3, b: 30, c: 1 }, { name: "y", a: 1, b: 10, c: 3 }, { name: "z", a: 2, b: 20, c: 2 }];
const grouped = { groups: ["G1", "G2"], defaultGroups: ["G1"] };
const build = (desc = {}, data = rows) => VIZ.table({ rows: data }, { rowsKey: "rows", columns: cols, title: "T", ...grouped, ...desc });
const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const picker = (w) => all(w).find((n) => n.className === "col-picker");
const toggle = (w, label) => picker(w).children.find((c) => c.textContent === label);
const ths = (w) => w.find("table").find("thead").find("tr").children;
const tbodyOf = (w) => w.find("table").children[1];
const shownHeads = (w) => ths(w).filter((t) => t.style.display !== "none").map((t) => t.textContent.replace(/[▲▼ ]/g, ""));
const shownCells = (w, r) => tbodyOf(w).children[r].children.filter((t) => t.style.display !== "none").map((t) => t.textContent);
const col0 = (w) => tbodyOf(w).children.map((tr) => tr.children[0].textContent);
const out = {};
const flush = () => new Promise((r) => setTimeout(r, 0));

const scenarios = {
  plain() {
    const w = VIZ.table({ rows }, { rowsKey: "rows", columns: cols.map(({ group, ...c }) => c), title: "P" });
    out.hasPicker = !!picker(w);
    out.heads = shownHeads(w);
    out.top = w.className;
    out.childClasses = w.children.map((c) => c.className);
    out.displayStyled = all(w).filter((n) => (n.tag === "th" || n.tag === "td") && n.style.display !== undefined).length;
    const bare = VIZ.table({ rows }, { rowsKey: "rows", columns: cols.map(({ group, ...c }) => c), title: "P", tools: false });
    out.bareClass = bare.className;
    out.bareHasTools = all(bare).some((n) => n.className === "grid-tools");
    out.bareDisplayStyled = all(bare).filter((n) => (n.tag === "th" || n.tag === "td") && n.style.display !== undefined).length;
  },
  defaults() {
    const w = build();
    out.labels = picker(w).children.map((c) => c.textContent);
    out.heads = shownHeads(w);
    out.cells = shownCells(w, 0);
    out.pressed = ["G1", "G2"].map((g) => toggle(w, g).getAttribute("aria-pressed"));
  },
  toggling() {
    const w = build();
    toggle(w, "G2").click();
    out.on = shownHeads(w);
    out.onCells = shownCells(w, 0);
    toggle(w, "G1").click();
    out.g1Off = shownHeads(w);
    toggle(w, "G2").click();
    out.allOff = shownHeads(w);
  },
  repaint() {
    globalThis.location = { hash: "#/server/a/tab" };
    toggle(build(), "G2").click();
    out.rebuilt = shownHeads(build());
    globalThis.location = { hash: "#/server/b/tab" };
    out.otherServer = shownHeads(build());
    globalThis.location = { hash: "#/server/a/tab" };
    out.backToA = shownHeads(build());
  },
  sortHidden() {
    globalThis.location = { hash: "#/server/s/tab" };
    // A ascending (y, x, z) differs from B ascending (y, z, x), so the final check can tell a sort from no sort.
    const data = [{ name: "x", a: 2, b: 30, c: 1 }, { name: "y", a: 1, b: 10, c: 3 }, { name: "z", a: 3, b: 20, c: 2 }];
    const w = build({}, data);
    toggle(w, "G2").click();
    ths(w)[2].fire("click"); // B ascending: 10, 20, 30
    out.asc = col0(w);
    toggle(w, "G2").click(); // B hidden; the order must hold and nothing throws
    out.hiddenAsc = col0(w);
    const w2 = build({}, data); // the repaint with B hidden re-applies the sort
    out.repaintAsc = col0(w2);
    out.heads = shownHeads(w2);
    ths(w2)[1].fire("click"); // sort a visible column afterwards
    out.byA = col0(w2);
  },
  async exportAll() {
    const texts = [];
    Object.defineProperty(globalThis, "navigator", { value: { clipboard: { writeText: async (t) => { texts.push(t); } } }, configurable: true, writable: true });
    const w = build();
    const strip = w.children.find((c) => c.className === "grid-tools");
    strip.children.find((c) => c.textContent === "Copy all").click(); await flush();
    out.copy = texts[0];
    out.stripOrder = w.children.map((c) => c.className);
  },
};
await scenarios[process.argv[3]]();
console.log(JSON.stringify(out));
