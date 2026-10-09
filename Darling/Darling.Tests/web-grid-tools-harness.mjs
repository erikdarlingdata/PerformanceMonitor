/* #4843: runs the shipped table renderer's Copy and CSV tools (wwwroot/js/panels.js with grid-tools.js) on a small fake DOM
   and prints what a scenario did as one line of JSON. GridToolsBehaviourTests starts it as
       node web-grid-tools-harness.mjs <path to the js folder> <scenario>
   Only the DOM is a stand-in; panels.js and util.js are the shipped modules. */
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
const tools = await import(pathToFileURL(root + "/grid-tools.js").href);

const cols = [
  { key: "name", label: "Name" },
  { key: "n", label: "N", format: "int" },
  { key: "t", label: "When", format: "time" },
  { key: "tags", label: "Tags" },
];
const rows = [
  { name: "b, \"quoted\"\nline", n: 1234567, t: "2026-01-02T03:04:05Z", tags: ["x"] },
  { name: "=SUM(A1)", n: 5, t: "2026-01-01T00:00:00Z", tags: ["y"] },
  { name: "naïve — 日本", n: -3, t: null, tags: [] },
  { name: "-cmd", n: 40, t: "2026-01-03T00:00:00Z", tags: [] },
];
const build = (desc = {}) => VIZ.table({ rows }, { rowsKey: "rows", columns: cols, title: "Slow Queries", ...desc });
const strip = (w) => w.children[0];
const btn = (w, label) => strip(w).children.find((c) => c.textContent === label);
const status = (w) => strip(w).children.find((c) => c.className === "grid-tools-status").textContent;
const tbodyOf = (w) => w.find("table").children[1];
const ths = (w) => w.find("table").find("thead").find("tr").children;
const out = {};
const csvOf = async () => { const d = downloads[downloads.length - 1]; out.fileName = d.name; return (await d.blob.text()).replace(/^\uFEFF/, ""); };
const flush = () => new Promise((r) => setTimeout(r, 0));

const scenarios = {
  async csv() {
    const w = build();
    btn(w, "Export CSV").click();
    out.csv = await csvOf();
    out.status = status(w);
  },
  async csvSorted() {
    const w = build();
    ths(w)[1].fire("click"); // N ascending: -3, 5, 40, 1234567
    btn(w, "Export CSV").click();
    out.csv = await csvOf();
    ths(w)[1].fire("click"); // descending
    btn(w, "Export CSV").click();
    out.csvDesc = await csvOf();
  },
  async copy() {
    const texts = [];
    setNav({ clipboard: { writeText: async (t) => { texts.push(t); } } });
    const w = build();
    ths(w)[1].fire("click"); // sorted ascending, so Copy all must follow it
    btn(w, "Copy cell").click(); await flush();
    out.beforePick = status(w);
    const tb = tbodyOf(w);
    tb.children[1].children[1].fire("click", { target: tb.children[1].children[1] }); // second row after sort: N = 5
    tb.fire("click", { target: tb.children[1].children[1] });
    btn(w, "Copy cell").click(); await flush();
    btn(w, "Copy row").click(); await flush();
    btn(w, "Copy all").click(); await flush();
    out.texts = texts;
    out.status = status(w);
  },
  async noClipboard() {
    setNav({});
    const w = build();
    const tb = tbodyOf(w);
    tb.fire("click", { target: tb.children[0].children[0] });
    btn(w, "Copy all").click(); await flush();
    out.all = status(w);
    btn(w, "Copy cell").click(); await flush();
    out.cell = status(w);
    setNav({ clipboard: { writeText: async () => { throw new Error("denied"); } } });
    btn(w, "Copy row").click(); await flush();
    out.denied = status(w);
  },
  async hook() {
    const seen = [];
    const w = build({ onRow: (row, tr) => { seen.push(row.n); tr.setAttribute("data-n", String(row.n)); }, rowClass: (row) => (row.n > 100 ? "hot" : null) });
    out.seen = seen;
    out.attrs = tbodyOf(w).children.map((tr) => tr.getAttribute("data-n"));
    out.classes = tbodyOf(w).children.map((tr) => tr.className);
    const s = build({ rowClass: "flag", tools: false });
    out.stringClass = tbodyOf(s).children.map((tr) => tr.className);
    out.noTools = s.tag; // the bare table-wrap, no strip
  },
  async columnOptions() {
    setNav({ clipboard: { writeText: async (t) => { texts.push(t); } } });
    const texts = [];
    const drows = [{ id: "a", summary: "short…", full: "line one\nline two", link: "x" }, { id: "b", summary: "s2", full: "=cmd", link: "y" }];
    const dcols = [
      { key: "id", label: "Id" },
      { key: "summary", label: "Detail", copyValue: (r) => r.full },
      { key: "link", label: "Open", csv: false, render: (r) => document.createTextNode("Open " + r.link) },
    ];
    const w = VIZ.table({ rows: drows }, { rowsKey: "rows", columns: dcols, title: "Opts" });
    btn(w, "Export CSV").click();
    out.csv = await csvOf();
    const tb = tbodyOf(w);
    tb.fire("click", { target: tb.children[0].children[1] });
    btn(w, "Copy cell").click(); await flush();
    btn(w, "Copy row").click(); await flush();
    out.texts = texts;
  },
  async pickSurvivesRebuild() {
    const texts = [];
    setNav({ clipboard: { writeText: async (t) => { texts.push(t); } } });
    const w1 = build();
    const tb1 = tbodyOf(w1);
    tb1.fire("click", { target: tb1.children[1].children[1] }); // N = 5
    const w2 = build(); // the 60 s repaint: a new table over the same rows
    out.marked = tbodyOf(w2).children[1].children[1].className;
    btn(w2, "Copy cell").click(); await flush();
    const gone = VIZ.table({ rows: rows.slice(2) }, { rowsKey: "rows", columns: cols, title: "Slow Queries" });
    btn(gone, "Copy cell").click(); await flush();
    out.texts = texts;
    out.goneStatus = status(gone);
  },
  async helpers() {
    out.field = [tools.csvField("a,b"), tools.csvField('q"q'), tools.csvField("=1+1"), tools.csvField("+x"), tools.csvField("@y"), tools.csvField(-5), tools.csvField("-5"), tools.csvField("+3.5e-2"), tools.csvField("-5x"), tools.csvField(null), tools.csvField("plain")];
    out.name = tools.csvFileName("Wait Stats / Top 10!", new Date("2026-01-05T14:30:07Z"));
    out.nameEmpty = tools.csvFileName("", new Date("2026-01-05T14:30:07Z"));
  },
};
await scenarios[process.argv[3]]();
console.log(JSON.stringify(out));
