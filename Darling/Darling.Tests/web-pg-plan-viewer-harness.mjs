/* #5229: runs the shipped PostgreSQL plan viewer (wwwroot/js/pages/pg-plan-viewer.js with util.js and grid-tools.js) on a small
   fake DOM and prints what a scenario did as one line of JSON. PgPlanViewerBehaviourTests starts it as
       node web-pg-plan-viewer-harness.mjs <path to the js folder> <scenario>
   Only the DOM, the clipboard and the download plumbing are stand-ins; fetch is a counter that must stay at 0. */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.style = {}; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  click() { for (const fn of this.listeners.click || []) fn({ preventDefault() {}, target: this }); }
  all(pred, acc = []) { if (pred(this)) acc.push(this); for (const c of this.children) c.all && c.all(pred, acc); return acc; }
  byText(t) { return this.all((n) => n.tag === "button" && n.textContent === t)[0] || null; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
const body = new FakeNode("body");
const downloads = [];
globalThis.document = { body, createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
URL.createObjectURL = (blob) => { downloads.push({ blob }); return "blob:fake"; };
URL.revokeObjectURL = () => {};
const origAppend = body.appendChild.bind(body);
body.appendChild = (a) => { if (a.tag === "a") a.click = () => { downloads[downloads.length - 1].name = a.download; }; return origAppend(a); };
const clip = [];
Object.defineProperty(globalThis, "navigator", { value: { clipboard: { writeText: async (t) => { clip.push(t); } } }, configurable: true, writable: true });
globalThis.location = { hash: "#/server/a/queries" };
const fetches = [];
globalThis.fetch = async (url) => { fetches.push(String(url)); throw new Error("no fetch expected"); };

const root = process.argv[2];
const viewer = await import(pathToFileURL(root + "/pages/pg-plan-viewer.js").href);
const flush = () => new Promise((r) => setTimeout(r, 5));
const out = {};
const plan = { "Query Identifier": -8126435036642491494, Plan: { "Node Type": "Seq Scan", "Relation Name": "orders", Filter: "(note = '<script>alert(1)</script>')", "Total Cost": 12.5, "Plan Rows": 40 }, "Execution Time": 10.25 };
const row = { queryid: "-8126435036642491494", plan_hash: "ABC123", plan };
const badRow = { queryid: "q/1 x", plan_hash: "h<1>", plan };
const stringRow = { queryid: "7", plan_hash: "S1", plan: '{"Plan": {"Node Ty' };
const noPlanRow = { queryid: "8", plan_hash: "N1", plan: null };
const pre = (cell) => cell.all((n) => n.tag === "pre")[0] || null;

const scenarios = {
  async open() {
    viewer.resetPgPlanViewer();
    const cell = viewer.pgPlanCell("srv-a", row);
    out.buttonBefore = cell.byText("Plan") !== null;
    out.preBefore = pre(cell) !== null;
    cell.byText("Plan").click();
    out.text = pre(cell).textContent;
    out.hasCopy = cell.byText("Copy") !== null;
    out.hasDownload = cell.byText("Download .json") !== null;
    out.expanded = cell.byText("Hide plan").attrs["aria-expanded"];
    out.fetches = fetches.length;
    cell.byText("Hide plan").click();
    out.preAfterHide = pre(cell) !== null;
  },
  async textOnly() {
    viewer.resetPgPlanViewer();
    const cell = viewer.pgPlanCell("srv-a", row);
    cell.byText("Plan").click();
    const p = pre(cell);
    out.preChildElements = p.children.filter((c) => c.tag !== "#text").length;
    out.scriptNodes = cell.all((n) => n.tag === "script").length;
    out.text = p.textContent;
  },
  async download() {
    viewer.resetPgPlanViewer();
    const cell = viewer.pgPlanCell("srv-a", badRow);
    cell.byText("Plan").click();
    cell.byText("Download .json").click();
    out.name = downloads[0] && downloads[0].name;
    out.type = downloads[0] && downloads[0].blob.type;
    out.body = downloads[0] && await downloads[0].blob.text();
    out.shown = pre(cell).textContent;
    out.fileName = viewer.pgPlanFileName(row);
    out.missing = viewer.pgPlanFileName({});
  },
  async copy() {
    viewer.resetPgPlanViewer();
    const cell = viewer.pgPlanCell("srv-a", row);
    cell.byText("Plan").click();
    cell.byText("Copy").click();
    await flush();
    out.clip = clip[0];
    out.shown = pre(cell).textContent;
  },
  async unparsed() {
    viewer.resetPgPlanViewer();
    const cell = viewer.pgPlanCell("srv-a", stringRow);
    cell.byText("Plan").click();
    out.notice = cell.all((n) => n.className === "strip notice").map((n) => n.textContent)[0] || null;
    const dl = cell.byText("Download .json");
    out.downloadDisabled = dl.disabled === true && "disabled" in dl.attrs;
    dl.click();
    out.downloads = downloads.length;
    out.copyEnabled = cell.byText("Copy").disabled !== true;
    out.shown = pre(cell).textContent;
  },
  async summary() {
    viewer.resetPgPlanViewer();
    const cell = viewer.pgPlanCell("srv-a", row);
    cell.byText("Plan").click();
    out.text = cell.all((n) => n.className === "plan-summary").map((n) => n.textContent)[0] || "";
    out.caveat = cell.textContent.includes("Query ID is exact in the grid");
    const plain = viewer.pgPlanCell("srv-a", { queryid: "1", plan_hash: "P", plan: { Plan: { "Node Type": "Result" } } });
    plain.byText("Plan").click();
    out.plainCaveat = plain.textContent.includes("Query ID is exact in the grid");
    out.expectedCost = (12.5).toLocaleString(undefined, { maximumFractionDigits: 2 });
    out.expectedRows = (40).toLocaleString(undefined, { maximumFractionDigits: 2 });
    out.plainSummary = viewer.pgPlanSummary({ Plan: { "Node Type": "Result" } }).length;
  },
  async noPlan() {
    const a = viewer.pgPlanCell("srv-a", noPlanRow);
    const b = viewer.pgPlanCell("srv-a", { queryid: "9", plan_hash: "E", plan: "" });
    out.text = a.textContent;
    out.emptyText = b.textContent;
    out.hasButton = a.all && a.all((n) => n.tag === "button").length > 0;
  },
  async rebuild() {
    viewer.resetPgPlanViewer();
    viewer.pgPlanCell("srv-a", row).byText("Plan").click();
    const again = viewer.pgPlanCell("srv-a", row);
    out.sameOpen = pre(again) !== null;
    out.otherOpen = pre(viewer.pgPlanCell("srv-b", row)) !== null;
    const otherHash = viewer.pgPlanCell("srv-a", { queryid: row.queryid, plan_hash: "ZZZ999", plan });
    out.otherHashOpen = pre(otherHash) !== null;
    out.keys = viewer.openPgPlanKeys();
  },
  async column() {
    const c = viewer.pgPlanColumn("s");
    out.key = c.key; out.label = c.label; out.csv = c.csv; out.sortable = c.sortable; out.hideWhenEmpty = c.hideWhenEmpty;
    out.filter = c.filter; out.copy = c.copy;
    out.rendered = c.render(row).byText("Plan") !== null;
  },
};

await scenarios[process.argv[3]]();
console.log(JSON.stringify(out));
