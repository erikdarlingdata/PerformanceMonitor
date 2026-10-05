/* #4843: runs the shipped stored-plan viewer (wwwroot/js/pages/plan-viewer.js with util.js and grid-tools.js) on a small
   fake DOM and prints what a scenario did as one line of JSON. PlanViewerBehaviourTests starts it as
       node web-plan-viewer-harness.mjs <path to the js folder> <scenario>
   Only the DOM, fetch, the clipboard and the download plumbing are stand-ins. */
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
let reply = { status: 200, body: "{}" };
globalThis.fetch = async (url) => {
  fetches.push(String(url));
  return { status: reply.status, ok: reply.status < 400, text: async () => reply.body };
};
/* What the read route answers: a plan is a bare string, so the route wraps it as {"error": <xml>} under a 400. */
const planReply = (xml) => { reply = { status: 400, body: JSON.stringify({ error: xml }) }; };
const noPlanReply = () => { reply = { status: 200, body: JSON.stringify({ status: "unavailable", message: "No stored plan found for query_hash 'H1'." }) }; };

const root = process.argv[2];
const viewer = await import(pathToFileURL(root + "/pages/plan-viewer.js").href);
const flush = () => new Promise((r) => setTimeout(r, 5));
const out = {};
const row = { query_hash: "0xABCDEF0123456789", database_name: "Orders", query_text: "select 1" };
const XML = '<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><BatchSequence><Batch><Statements><StmtSimple StatementText="select * from t where a &gt; 1 and b &lt; &quot;&lt;script&gt;&quot;"><QueryPlan CachedPlanSize="16"><RelOp NodeId="0" PhysicalOp="Index Scan"/></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>';
const pre = (cell) => cell.all((n) => n.tag === "pre")[0] || null;
const params = () => new URL(fetches[fetches.length - 1], "http://viewer.test");

const scenarios = {
  async open() {
    planReply(XML);
    const cell = viewer.storedPlanCell("srv-a", row);
    out.buttonBefore = cell.byText("Plan") !== null;
    cell.byText("Plan").click();
    out.loading = cell.textContent;
    await flush();
    const u = params();
    out.path = u.pathname;
    out.query = Object.fromEntries(u.searchParams);
    out.pre = pre(cell).textContent;
    out.preTag = pre(cell).tag;
    out.hasCopy = cell.byText("Copy") !== null;
    out.hasDownload = cell.byText("Download .sqlplan") !== null;
    out.noHashCell = viewer.storedPlanCell("srv-a", { query_hash: null }).textContent;
    cell.byText("Hide plan").click();
    out.closed = pre(cell) === null;
  },
  async textOnly() {
    planReply(XML);
    const cell = viewer.storedPlanCell("srv-a", row);
    cell.byText("Plan").click();
    await flush();
    const p = pre(cell);
    out.preChildren = p.children.length;
    out.scriptElements = cell.all((n) => n.tag === "script").length;
    out.hasHtmlAttr = cell.all((n) => "innerHTML" in n && n.innerHTML !== undefined).length;
    out.text = p.textContent;
    out.lines = p.textContent.split("\n").length;
    out.pretty = viewer.prettyPrintXml('<a><b x="1>2">t</b><c/><d><e/></d></a>');
    out.notXml = viewer.prettyPrintXml("not xml <at all");
  },
  async download() {
    planReply(XML);
    const cell = viewer.storedPlanCell("srv-a", row);
    cell.byText("Plan").click();
    await flush();
    cell.byText("Download .sqlplan").click();
    const d = downloads[downloads.length - 1];
    out.name = d.name;
    out.type = d.blob.type;
    out.body = await d.blob.text();
    out.bodyIsStored = out.body === XML;
    out.safeName = viewer.planFileName("a/b c");
  },
  async copy() {
    planReply(XML);
    const cell = viewer.storedPlanCell("srv-a", row);
    cell.byText("Plan").click();
    await flush();
    cell.byText("Copy").click();
    await flush();
    out.copied = clip[0];
    out.matchesShown = clip[0] === pre(cell).textContent;
  },
  async truncated() {
    planReply(XML.slice(0, 120) + "... (truncated)");
    const cell = viewer.storedPlanCell("srv-a", row);
    cell.byText("Plan").click();
    await flush();
    const dl = cell.byText("Download .sqlplan");
    out.disabled = dl.attrs.disabled !== undefined && dl.disabled === true;
    dl.click();
    out.downloads = downloads.length;
    out.notice = cell.all((n) => n.className === "strip notice").map((n) => n.textContent)[0];
    out.shown = pre(cell).textContent.includes("(truncated)");
    out.copyOn = cell.byText("Copy") !== null && cell.byText("Copy").attrs.disabled === undefined;
  },
  async noPlan() {
    noPlanReply();
    const cell = viewer.storedPlanCell("srv-a", row);
    cell.byText("Plan").click();
    await flush();
    out.text = cell.textContent;
    out.hasPre = pre(cell) !== null;
    out.hasDownload = cell.byText("Download .sqlplan") !== null;
  },
  async rebuild() {
    planReply(XML);
    const first = viewer.storedPlanCell("srv-a", row);
    first.byText("Plan").click();
    await flush();
    const before = fetches.length;
    /* The 60 s rebuild draws a brand-new cell for the same row. */
    const second = viewer.storedPlanCell("srv-a", row);
    out.open = second.byText("Hide plan") !== null;
    out.showsPlan = pre(second) !== null && pre(second).textContent.includes("RelOp");
    out.refetched = fetches.length - before;
    const other = viewer.storedPlanCell("srv-b", row);
    out.otherServerOpen = other.byText("Hide plan") !== null;
    /* A rebuild while the read is still out: the new cell fills in when it lands. */
    viewer.resetPlanViewer();
    let release;
    const gate = new Promise((r) => { release = r; });
    globalThis.fetch = async () => { await gate; return { status: 400, ok: false, text: async () => JSON.stringify({ error: XML }) }; };
    const c1 = viewer.storedPlanCell("srv-a", row);
    c1.byText("Plan").click();
    const c2 = viewer.storedPlanCell("srv-a", row);
    out.loadingInNew = c2.textContent.includes("Loading");
    release();
    await flush();
    out.filledNew = pre(c2) !== null;
    out.keys = viewer.openPlanKeys();
  },
};
await scenarios[process.argv[3]]();
console.log(JSON.stringify(out));
