/* #4843: runs the shipped table renderer's column filters (wwwroot/js/panels.js) on a small fake DOM
   and prints what a scenario did as one line of JSON. GridFilterBehaviourTests starts it as
       node web-grid-filter-harness.mjs <path to the js folder> <scenario> */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.style = {}; this.classList = { add: (c) => { this.className = (this.className + " " + c).trim(); }, remove: (c) => { this.className = this.className.split(" ").filter((x) => x !== c).join(" "); } }; this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.value = ""; }
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
  focus() { globalThis.document.activeElement = this; this.fire("focus"); }
  contains(n) { for (; n; n = n.parent) if (n === this) return true; return false; }
  find(tag) { for (const c of this.children) { if (c.tag === tag) return c; const f = c.find && c.find(tag); if (f) return f; } return null; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
const body = new FakeNode("body");
let downloads = [];
globalThis.document = { activeElement: null, body, createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
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
  { key: "tags", label: "Tags", render: (r) => { const s = new FakeText(r.tags.join(", ")); return s; }, copyValue: (r) => r.tags.join("; ") },
];
const rows = [
  { name: "Alpha", a: 3, b: 30, tags: ["red", "hot"] },
  { name: "beta", a: 1, b: 10, tags: ["blue"] },
  { name: "ALPHA two", a: 2, b: 20, tags: ["blue", "hot"] },
];
const grouped = { groups: ["G1", "G2"], defaultGroups: ["G1"] };
const build = (desc = {}, data = rows) => VIZ.table({ rows: data }, { rowsKey: "rows", columns: cols, title: "T", ...grouped, ...desc });
const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const ths = (w) => w.find("table").find("thead").find("tr").children;
const tbodyOf = (w) => w.find("table").children[1];
const filterBtn = (w, i) => ths(w)[i].children.find((c) => c.tag === "button");
const open = (w, i) => filterBtn(w, i).click();
const box = (w) => all(w).find((n) => n.className === "grid-filter-input");
const type = (w, text) => { const b = box(w); b.value = text; b.fire("input"); };
const shown = (w) => tbodyOf(w).children.filter((t) => t.style.display !== "none").map((t) => t.children[0].textContent);
const chips = (w) => all(w).filter((n) => n.className === "grid-filter-chip").map((n) => n.textContent);
const count = (w) => (all(w).find((n) => n.className === "grid-filter-count") || { textContent: "" }).textContent;
const button = (w, cls) => all(w).find((n) => n.className === cls || (n.className || "").includes(cls));
const out = {};
const flush = () => new Promise((r) => setTimeout(r, 0));
const tool = (w, label) => all(w).find((n) => n.tag === "button" && n.textContent === label);
const clip = [];
Object.defineProperty(globalThis, "navigator", { value: { clipboard: { writeText: async (t) => { clip.push(t); } } }, configurable: true, writable: true });
const csvText = async () => { const d = downloads[downloads.length - 1]; return d ? await d.blob.text() : null; };

const scenarios = {
  narrows() {
    globalThis.location = { hash: "#/server/a/narrow" };
    const w = build();
    out.before = shown(w);
    out.hasButtons = [0, 1, 2, 3].map((i) => !!filterBtn(w, i));
    open(w, 0); type(w, "ALPHA");
    out.after = shown(w);
    out.count = count(w);
    out.chips = chips(w);
    out.headText = ths(w).map((t) => t.textContent.replace(/[▲▼ ]/g, ""));
    out.pressed = filterBtn(w, 0).getAttribute("aria-pressed");
  },
  listAndFormatted() {
    globalThis.location = { hash: "#/server/a/list" };
    const w = build();
    open(w, 3); type(w, "HOT");
    out.list = shown(w);
    type(w, "blue, hot");
    out.listExact = shown(w);
    const w2 = (globalThis.location = { hash: "#/server/a/fmt" }, build({}, [{ name: "n", a: 1234, b: 1, tags: [] }, { name: "m", a: 5, b: 1, tags: [] }]));
    open(w2, 1); type(w2, "1,234");
    out.formatted = shown(w2);
  },
  ands() {
    globalThis.location = { hash: "#/server/a/and" };
    const w = build();
    open(w, 0); type(w, "alpha");
    open(w, 3); type(w, "red");
    out.both = shown(w);
    out.chips = chips(w);
    out.count = count(w);
  },
  repaint() {
    globalThis.location = { hash: "#/server/a/repaint" };
    const w = build();
    open(w, 0); type(w, "beta");
    const w2 = build();
    out.rebuilt = shown(w2);
    out.count = count(w2);
    out.boxValue = box(w2) ? box(w2).value : null;
    out.boxLabel = box(w2) ? box(w2).attrs["aria-label"] : null;
    globalThis.location = { hash: "#/server/b/repaint" };
    const other = build();
    out.otherServer = shown(other);
    out.otherBox = !!box(other);
    out.otherCount = count(other);
    globalThis.location = { hash: "#/server/a/repaint" };
    out.backToA = shown(build());
  },
  async escape() {
    globalThis.location = { hash: "#/server/a/esc" };
    const w = build();
    open(w, 0);
    out.openFocus = document.activeElement === box(w);
    const panel = box(w).parent;
    panel.fire("keydown", { key: "Escape" });
    out.closed = !box(w);
    out.refocus = document.activeElement === filterBtn(w, 0);
    open(w, 0);
    out.reopened = !!box(w);
    open(w, 0);
    out.toggledClosed = !box(w);
    out.sortStateUntouched = ths(w)[0].getAttribute("aria-sort");
  },
  async exportFiltered() {
    globalThis.location = { hash: "#/server/a/export" };
    const w = build();
    ths(w)[0].fire("click"); // sort Name ascending: Alpha, ALPHA two, beta
    open(w, 3); type(w, "hot");
    out.shown = shown(w);
    tool(w, "Copy all").click(); await flush();
    out.copy = clip[clip.length - 1];
    tool(w, "Export CSV").click();
    out.csv = await csvText();
    out.status = all(w).find((n) => n.className === "grid-tools-status").textContent;
  },
  async clearing() {
    globalThis.location = { hash: "#/server/a/clear" };
    const w = build();
    open(w, 0); type(w, "alpha");
    open(w, 3); type(w, "red");
    const x = all(w).filter((n) => n.className === "grid-filter-x")[0];
    x.click();
    out.afterOne = shown(w);
    out.chipsAfterOne = chips(w);
    out.boxAfterOne = box(w) ? box(w).value : null;
    open(w, 0); type(w, "alpha");
    button(w, "grid-filter-clear-all").click();
    out.afterAll = shown(w);
    out.countAfterAll = count(w);
    out.chipsAfterAll = chips(w);
    const w2 = build();
    out.rebuiltAfterAll = shown(w2);
    out.openSurvivedClearAll = !!box(w2);
    type(w2, "beta");
    out.typedAfterRebuild = shown(w2);
    button(w2, "grid-filter-clear").click();
    out.afterBoxClear = shown(w2);
  },
  plain() {
    globalThis.location = { hash: "#/server/a/plain" };
    const w = build({ filter: false });
    out.noButtons = !filterBtn(w, 0);
    const c2 = cols.map((c, i) => (i === 1 ? { ...c, filter: false } : c));
    const w2 = build({ columns: c2 });
    out.perColumn = [0, 1, 2, 3].map((i) => !!filterBtn(w2, i));
    const w3 = build({ columns: [{ key: "name", label: "", copy: false }, ...cols.slice(1)] });
    out.controlColumn = !!filterBtn(w3, 0);
    out.heads = shown(build());
  },
};
await scenarios[process.argv[3]]();
console.log(JSON.stringify(out));
