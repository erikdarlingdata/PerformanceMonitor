/* Runs the web viewer's wait links (#5235) against a scripted /api/read: the Wait cell of the Wait Stats, Waiting Tasks and
   Significant Waits grids, and the Active Queries panel a click opens, filtered to that wait. The shipped server-tabs.js,
   panels.js, util.js, charts.js and multi-picker.js run as they are; `fetch`, the DOM, the page's hash and pages/server.js
   (which records the range it is asked to apply) are stand-ins. WebWaitQueriesLinkBehaviourTests starts it as
       node web-wait-queries-link-harness.mjs <path to wwwroot/js>
   and reads the last line of its output, one JSON object. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 1];

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.listeners = {};
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() {
    return this.children[0] || null;
  }
  get lastChild() {
    return this.children[this.children.length - 1] || null;
  }
  appendChild(child) {
    this.children.push(child);
    return child;
  }
  insertBefore(child, ref) {
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i < 0) this.children.push(child);
    else this.children.splice(i, 0, child);
    return child;
  }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    return child;
  }
  setAttribute(name, value) {
    this.attrs[name] = String(value);
  }
  getAttribute(name) {
    return name in this.attrs ? this.attrs[name] : null;
  }
  addEventListener(type, listener) {
    (this.listeners[type] = this.listeners[type] || []).push(listener);
  }
  focus() {}
  getBoundingClientRect() {
    return { left: 0, top: 0, width: 1000, height: 320 };
  }
  set textContent(value) {
    this.children = [];
    this.text = String(value);
  }
  get textContent() {
    return (this.text || "") + this.children.map((c) => c.textContent).join("");
  }
}

globalThis.Node = FakeNode;
globalThis.document = {
  activeElement: null,
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
  addEventListener() {},
  removeEventListener() {},
  querySelectorAll: () => [],
};
globalThis.window = globalThis;

/* The page's hash: setting a NEW value fires hashchange a task later; setting the value it holds fires nothing. */
let hashValue = "#/server/A/waits";
const hashListeners = [];
let dispatched = 0;
globalThis.location = {
  get hash() {
    return hashValue;
  },
  set hash(value) {
    if (value === hashValue) return;
    hashValue = value;
    setTimeout(() => hashListeners.slice().forEach((l) => l.fn()), 0);
  },
};
globalThis.addEventListener = (type, fn, options) => {
  if (type === "hashchange") hashListeners.push({ fn, once: !!(options && options.once) });
};
globalThis.dispatchEvent = () => {
  dispatched++;
};

const fetches = [];
let answer = () => ({ status: 200, body: {} });
globalThis.fetch = async (url) => {
  fetches.push(String(url));
  const reply = answer(new URL(String(url), "http://viewer.test"));
  const raw = reply.body === undefined ? "" : JSON.stringify(reply.body);
  return { status: reply.status, ok: reply.status >= 200 && reply.status < 300, text: async () => raw };
};
const rejections = [];
process.on("unhandledRejection", (e) => rejections.push(String(e && e.stack ? e.stack : e)));

const scratch = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "wait-queries-link-")));
let util;
let tabs;
let stub;
try {
  /* The whole js tree first (#5279); the stand-in for pages/server.js is written AFTER the copy, so it replaces the real file. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "pages", "server.js"),
    "export const calls = [];\n" +
      "export function applyCustomRange(server, startMs, endMs, nowMs, { redraw = true } = {}) {\n" +
      "  calls.push({ server, startMs, endMs, redraw });\n" +
      "  return globalThis.__rangeError || null;\n" +
      "}\n"
  );
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  util = await load("util.js");
  tabs = await load(path.join("pages", "server-tabs.js"));
  stub = await load(path.join("pages", "server.js"));
} catch (e) {
  fs.rmSync(scratch, { recursive: true, force: true });
  throw e;
}
/* The copy stays until the run ends: a click loads pages/server.js (the stand-in) the first time it applies a range. */
process.on("exit", () => fs.rmSync(scratch, { recursive: true, force: true }));

const data = (b) => ({ status: 200, body: b });
const tool = (url) => url.pathname.replace("/api/read/", "");
const all = (node, pred, found = []) => {
  if (!node || typeof node !== "object") return found;
  if (pred(node)) found.push(node);
  (node.children || []).forEach((c) => all(c, pred, found));
  return found;
};
const settle = async () => {
  for (let i = 0; i < 60; i++) await new Promise((r) => setTimeout(r, 0));
};
const ctx = { hours: 24, label: "last 24 hours" };
const MIN = 60000;
const HALF = 30 * MIN;

/* An instant two days back, to the second, as a naive-UTC string with more fractional digits than a Date holds. */
const baseMs = Math.floor((Date.now() - 2 * 24 * 3600000) / 1000) * 1000;
const iso = (ms) => new Date(ms).toISOString().slice(0, 19);
const WAIT_MICRO = iso(baseMs) + ".123456";
const SIG_SEVEN = iso(baseMs + 5 * MIN) + ".1234567";
const MARKUP = "<img src=x onerror=alert(1)>";

const taskRows = [
  { collection_time: WAIT_MICRO, session_id: 51, wait_type: "LCK_M_X", wait_duration_ms: 10 },
  { collection_time: iso(baseMs), session_id: 52, wait_type: "QDS_ASYNC_QUEUE", wait_duration_ms: 10 },
  { collection_time: iso(baseMs), session_id: 53, wait_type: null, wait_duration_ms: 10 },
  { collection_time: "not a time", session_id: 54, wait_type: "CXPACKET", wait_duration_ms: 10 },
  { collection_time: iso(baseMs), session_id: 55, wait_type: MARKUP, wait_duration_ms: 10 },
];
const sigRows = [
  { event_time: SIG_SEVEN, wait_type: "PAGEIOLATCH_SH", duration_ms: 600, signal_duration_ms: 1, session_id: 60 },
  { event_time: iso(baseMs), wait_type: "QDS_PERSIST_TASK_MAIN_LOOP_SLEEP", duration_ms: 600, signal_duration_ms: 1, session_id: 61 },
];
const statRows = [
  { wait_type: "WRITELOG", total_wait_time_ms: 5000, resource_wait_ms: 4000, total_signal_wait_ms: 1000, waiting_tasks: 9, signal_wait_pct: 20 },
  { wait_type: "QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP", total_wait_time_ms: 10, resource_wait_ms: 9, total_signal_wait_ms: 1, waiting_tasks: 1, signal_wait_pct: 10 },
];
let activeAnswer = () => data({ queries: [] });
answer = (url) => {
  const t = tool(url);
  if (t === "get_waiting_tasks") return data({ tasks: taskRows });
  if (t === "get_health_parser_significant_waits") return data({ waits: sigRows });
  if (t === "get_wait_stats") return data({ server: "A", waits: statRows });
  if (t === "get_active_queries") return activeAnswer(url);
  return data({});
};

const panelOf = (root, title) =>
  all(root, (n) => n.tag === "div" && String(n.className).includes("panel") && n.children[0] && n.children[0].tag === "h3" && n.children[0].children[0] && n.children[0].children[0].textContent === title)[0];
const waitButtons = (panel) => all(panel, (n) => n.tag === "button" && String(n.className) === "wait-link");
const click = async (node) => {
  node.listeners.click.forEach((l) => l({}));
  await settle();
};
const takeCalls = () => stub.calls.splice(0, stub.calls.length);
const buildTab = async (id, server = "A") => {
  const root = new FakeNode("main");
  util.mount(root, tabs.findServerTab(id).build(server, ctx));
  await settle();
  return root;
};
const cellTexts = (panel, col) => {
  /* The text of one column's cell in every body row, in row order. */
  const table = all(panel, (n) => n.tag === "table")[0];
  const heads = all(table, (n) => n.tag === "th");
  const idx = heads.findIndex((h) => h.textContent.startsWith(col));
  const body = all(table, (n) => n.tag === "tbody")[0] || table;
  return all(body, (n) => n.tag === "tr").map((tr) => {
    const tds = tr.children.filter((c) => c.tag === "td");
    return tds[idx] ? tds[idx].textContent : null;
  });
};

const out = {};
const reset = () => {
  util.setQueryWaitFilter("A", "");
  takeCalls();
  globalThis.__rangeError = null;
  hashValue = "#/server/A/waits";
};

/* Waiting Tasks and Wait Stats, from the Wait Stats tab. */
const waits = await buildTab("waits");
const tasks = panelOf(waits, "Waiting Tasks");
const stats = panelOf(waits, "Wait Stats");
out.taskButtons = waitButtons(tasks).map((b) => b.textContent);
out.taskWaitCells = cellTexts(tasks, "Wait");
out.statButtons = waitButtons(stats).map((b) => b.textContent);
out.rejectionsAfterBuild = rejections.length;

reset();
const lck = waitButtons(tasks).find((b) => b.textContent === "LCK_M_X");
out.taskTitle = lck.attrs.title || null;
await click(lck);
out.taskCalls = takeCalls();
out.taskFilter = util.queryWaitFilter("A");
out.taskHash = hashValue;
out.taskRowMs = baseMs + 0; /* the row's instant, to the millisecond: .123 of the second */
out.taskRowFracMs = baseMs + 123;

reset();
const wrt = waitButtons(stats).find((b) => b.textContent === "WRITELOG");
await click(wrt);
out.statCalls = takeCalls();
out.statFilter = util.queryWaitFilter("A");
out.statHash = hashValue;

/* The markup wait is a button whose label is the text, with no element inside. */
const markup = waitButtons(tasks).find((b) => b.textContent === MARKUP);
out.markupFound = !!markup;
out.markupChildren = markup ? markup.children.length : -1;
reset();
await click(markup);
out.markupFilter = util.queryWaitFilter("A");
takeCalls();

/* Significant Waits, from the Events tab. */
const events = await buildTab("events");
const sig = panelOf(events, "Significant Waits");
out.sigButtons = waitButtons(sig).map((b) => b.textContent);
out.sigWaitCells = cellTexts(sig, "Wait Type");
reset();
await click(waitButtons(sig)[0]);
out.sigCalls = takeCalls();
out.sigFilter = util.queryWaitFilter("A");
out.sigHash = hashValue;
out.sigRowFloorMs = baseMs + 5 * MIN + 123;

/* A refused range sets no filter, does not route, and says why beside the button. */
reset();
globalThis.__rangeError = "The end cannot be in the future.";
const refusedBtn = waitButtons(tasks).find((b) => b.textContent === "LCK_M_X");
await click(refusedBtn);
out.refusedFilter = util.queryWaitFilter("A");
out.refusedHash = hashValue;
out.refusedCalls = takeCalls().length;
out.refusedNote = all(refusedBtn.parentNode || tasks, (n) => n.attrs && n.attrs.role === "status" && n.textContent !== "").map((n) => n.textContent);
globalThis.__rangeError = null;

/* The filtered Active Queries panel. */
reset();
const unfilteredRoot = await buildTab("queries");
const unfilteredReads = fetches.filter((f) => f.includes("get_active_queries"));
out.unfilteredQuery = unfilteredReads.length ? new URL(unfilteredReads[unfilteredReads.length - 1], "http://x").search : null;
out.unfilteredStrips = all(unfilteredRoot, (n) => String(n.className).includes("wait-filter-strip")).length;
const unfilteredPanel = panelOf(unfilteredRoot, "Active Queries");
out.unfilteredHeadText = unfilteredPanel ? unfilteredPanel.children[0].textContent : null;

fetches.length = 0;
util.setQueryWaitFilter("A", "LCK_M_X");
activeAnswer = () => data({ queries: [{ collection_time: iso(baseMs), session_id: 51, wait_type: "LCK_M_X", query_text: "select 1" }] });
const filteredRoot = await buildTab("queries");
const filteredReads = fetches.filter((f) => f.includes("get_active_queries"));
out.filteredQuery = filteredReads.length ? new URL(filteredReads[filteredReads.length - 1], "http://x").search : null;
const filteredPanel = panelOf(filteredRoot, "Active Queries");
out.filteredHeadText = filteredPanel ? filteredPanel.children[0].textContent : null;
out.filteredHeadChildOrder = filteredPanel ? filteredPanel.children.map((c) => c.tag + ":" + String(c.className).split(" ")[0]) : [];
const strips = all(filteredRoot, (n) => String(n.className).includes("wait-filter-strip"));
out.stripCount = strips.length;
out.stripText = strips[0] ? strips[0].textContent : null;
const showAll = strips[0] ? all(strips[0], (n) => n.tag === "button")[0] : null;
out.showAllLabel = showAll ? showAll.textContent : null;
dispatched = 0;
await click(showAll);
out.filterAfterShowAll = util.queryWaitFilter("A");
out.dispatchedAfterShowAll = dispatched;
out.hashAfterShowAll = hashValue;

/* A filtered empty answer: the server's own envelope names the wait; with no envelope the panel's text does. */
util.setQueryWaitFilter("A", "WRITELOG");
activeAnswer = () => data({ status: "empty", message: "No active-query snapshots with wait_type 'WRITELOG' were collected in this window." });
const emptyRoot = await buildTab("queries");
out.emptyEnvelopeText = all(panelOf(emptyRoot, "Active Queries"), (n) => String(n.className).includes("strip empty")).map((n) => n.textContent);
activeAnswer = () => data({ queries: [] });
const emptyRoot2 = await buildTab("queries");
out.emptyRowsText = all(panelOf(emptyRoot2, "Active Queries"), (n) => String(n.className).includes("strip empty")).map((n) => n.textContent);

/* A markup wait on the strip and the subtitle stays text. */
util.setQueryWaitFilter("A", MARKUP);
const markupRoot = await buildTab("queries");
const markupStrip = all(markupRoot, (n) => String(n.className).includes("wait-filter-strip"))[0];
out.markupStripText = markupStrip ? markupStrip.textContent : null;
out.markupStripSpanChildren = markupStrip ? markupStrip.children[0].children.length : -1;
const markupPanel = panelOf(markupRoot, "Active Queries");
out.markupSubtitle = markupPanel ? markupPanel.children[0].textContent : null;
util.setQueryWaitFilter("A", "");

out.rejections = rejections.slice(0, 3);
/* ASCII only, so the reader sees the same characters whatever code page its pipe uses. */
console.log(JSON.stringify(out).replace(/[\u0080-\uffff]/g, (c) => "\\u" + c.charCodeAt(0).toString(16).padStart(4, "0")));
