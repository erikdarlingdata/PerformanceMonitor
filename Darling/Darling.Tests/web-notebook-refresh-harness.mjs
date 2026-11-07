/* #7924: runs a saved notebook (pages/views.js renderView) with the shipped views.js, compose.js, charts.js, panels.js and util.js
   against a scripted /api/compose/run, polls it once the way app.js's auto-refresh does (renderView with { poll: true }), and prints as
   one line of JSON what stayed on the page and what the poll sent. WebNotebookRefreshBehaviourTests starts it as
       node web-notebook-refresh-harness.mjs <path to wwwroot/js>
   The whole js tree is copied into a scratch folder first (#5279). Only the DOM, the scroll and fetch are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];

/* The page's height, and the reader's scroll clamped to it: a browser lays out after every DOM change and keeps the scroll position
   within the page as it is then, so a page that collapses to loading strips loses a reader scrolled below the new bottom. Every drawn
   chart mark counts toward the height; a loading strip adds nothing. */
let scrollPos = 0;
const pageHeight = () => 200 + all(root).filter((n) => n.tag === "path" || n.tag === "rect" || n.tag === "circle" || n.tag === "tr").length * 20;
const layout = () => { scrollPos = Math.min(scrollPos, pageHeight()); };
let root = null;

class FakeNode {
  constructor(tag, text) {
    this.style = {};
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.listeners = {};
    this.parent = null;
    this._text = text == null ? "" : String(text);
    this.className = "";
    this.value = "";
  }
  get parentNode() { return this.parent; }
  get isConnected() { let n = this; while (n.parent) n = n.parent; return n.isRoot === true; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; layout(); }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  appendChild(c) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    this.children.push(c);
    layout();
    return c;
  }
  insertBefore(c, ref) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i < 0) this.children.push(c);
    else this.children.splice(i, 0, c);
    layout();
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; layout(); return c; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, stopPropagation() {}, target: this, ...e }); }
  setPointerCapture() {}
  focus() {}
  get offsetWidth() { return 1000; }
  get offsetHeight() { return 320; }
  getBoundingClientRect() { return { left: 0, top: 0, width: 1000, height: 320 }; }
}
globalThis.Node = FakeNode;
globalThis.document = {
  body: new FakeNode("body"),
  documentElement: { offsetHeight: 0 },
  querySelectorAll: () => [],
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeNode("#text", t),
  createDocumentFragment: () => new FakeNode("#fragment"),
  addEventListener() {},
  removeEventListener() {},
};
Object.defineProperty(globalThis, "scrollY", { get: () => scrollPos, configurable: true });
globalThis.scrollTo = (x, y) => { scrollPos = Math.min(y, pageHeight()); };
/* The page watches a refreshing panel for its loading strip to go; here the watcher checks on a short timer instead. */
globalThis.MutationObserver = class {
  constructor(fn) { this.fn = fn; this.timer = null; }
  observe() { if (!this.timer) { this.timer = setInterval(() => this.fn(), 5); this.timer.unref(); } }
  disconnect() { clearInterval(this.timer); this.timer = null; }
};
globalThis.location = { hash: "#/view/v1" };

/* The scripted service: one saved notebook, and every compose run held until the test answers it. `calls` is every run the page sent, in
   order, with the window the run asked for. */
const SOURCES = ["s1", "s2", "s3"];
let view = null;
const calls = [];
let autoAnswer = false;
const reply = (status, body) => ({ status, ok: status >= 200 && status < 300, headers: { get: () => null }, text: async () => JSON.stringify(body) });
const T0 = Date.UTC(2026, 0, 1, 0, 0, 0);
const MIN = 60000;
const lineRows = () => Array.from({ length: 11 }, (_, i) => [["AAA", 1000 + i * 100], ["BBB", i % 5]].map(([w, v]) => ({ bucket: new Date(T0 + i * MIN).toISOString().slice(0, 19), wait: w, value: v }))).flat();
function answer(call, status) {
  if (!call) return;
  call.answered = status;
  call.resolve(status === 200 ? reply(200, { sql: "select 1", rows: lineRows() }) : reply(500, { error: "The read failed." }));
}
globalThis.fetch = async (url, init) => {
  const u = new URL(String(url), "http://viewer.test");
  if (u.pathname === "/api/session") return reply(200, { can_edit: true });
  if (u.pathname === "/api/catalog") {
    return reply(200, { reads: [], viz: [], compose: { measures: SOURCES.map((s) => ({ source: s, measure: "m", label: s })), dimensions: [], annotationSources: [] } });
  }
  if (u.pathname === "/api/fleet") return reply(200, { cards: [{ server_name: "a", display_name: "Server A" }, { server_name: "b", display_name: "Server B" }] });
  if (u.pathname === "/api/views/v1") return reply(200, view);
  if (u.pathname === "/api/compose/run") {
    const body = JSON.parse(init.body);
    const call = { source: body.panel.source, windowStart: body.windowStart ?? null, windowEnd: body.windowEnd ?? null, answered: null };
    calls.push(call);
    const settled = new Promise((resolve) => { call.resolve = resolve; });
    if (autoAnswer) answer(call, 200);
    return settled;
  }
  return reply(404, { error: "not scripted" });
};
process.on("unhandledRejection", (e) => { if (process.env.HARNESS_DEBUG) console.error(String(e && e.stack)); });

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "web-notebook-refresh-"));
let modules;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = { views: await load(path.join("pages", "views.js")) };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const flush = async () => { for (let i = 0; i < 40; i++) await new Promise((r) => setTimeout(r, 0)); };
const cls = (n) => n.attrs.class || n.className || "";
const loadingCount = (node) => all(node).filter((n) => /\bstrip\b/.test(cls(n)) && /\bloading\b/.test(cls(n))).length;
const chipCount = (node, which) => all(node).filter((n) => cls(n).includes(which)).length;
const sourcesOf = (list) => list.map((c) => c.source);
const windowOf = (call) => (call ? { windowStart: call.windowStart, windowEnd: call.windowEnd } : null);
const docOf = (main) => Array.from(main.children).find((n) => cls(n) === "notebook-doc") || null;
const panelCell = (title, source) => ({ type: "panel", title, source, viz: "line", measure: "m", groupBy: ["wait"] });
async function settleAll() {
  for (let i = 0; i < 12; i++) {
    const open = calls.filter((c) => c.answered === null);
    if (!open.length) break;
    answer(open[0], 200);
    await flush();
  }
}
/* A brush-zoom on the panel cell's chart: pointer down and up on the chart's overlay, as the reader would drag. */
async function brush(cell) {
  const overlay = all(cell).find((n) => (n.listeners.pointerdown || []).length);
  overlay.listeners.pointerdown[0]({ button: 0, clientX: 300, pointerId: 1 });
  overlay.listeners.pointerup[0]({ clientX: 700 });
  await flush();
  await settleAll();
}

const out = {};

/* A saved notebook with its refresh on: a markdown cell and two line-chart panel cells. The reader zooms the first chart and scrolls
   down, then the auto-refresh tick arrives. */
{
  view = { id: "v1", name: "Night notes", definition: { kind: "notebook", refresh: "1m", cells: [{ type: "markdown", text: "# Notes" }, panelCell("Waits one", "s1"), panelCell("Waits two", "s2")] } };
  calls.length = 0;
  autoAnswer = true;
  const main = new FakeNode("main");
  main.isRoot = true;
  root = main;
  await modules.views.renderView(main, "v1");
  await flush();
  autoAnswer = false;
  const doc = docOf(main);
  const cells = Array.from(doc.children);
  out.firstCalls = sourcesOf(calls);
  out.firstLoading = loadingCount(main);
  out.cellCount = cells.length;

  await brush(cells[1]);
  out.zoomed = windowOf(calls.findLast((c) => c.source === "s1"));
  out.zoomChipBefore = chipCount(doc, "zoom-chip");

  globalThis.scrollTo(0, 600);
  out.heightBefore = pageHeight();
  out.scrollBefore = scrollPos;

  /* The tick: the view is read again and each panel runs again, while the reader is still looking at the page. */
  calls.length = 0;
  await modules.views.renderView(main, "v1", { poll: true });
  await flush();
  out.tickCalls = sourcesOf(calls);
  out.docKeptWhileReading = main.children.includes(doc);
  out.loadingOnPageWhileReading = loadingCount(main);
  out.scrollWhileReading = scrollPos;

  await settleAll();
  out.docKeptAfterTick = main.children.includes(doc);
  out.cellsKeptAfterTick = main.children.includes(doc) && doc.children.length === cells.length && cells.every((c, i) => doc.children[i] === c);
  out.loadingAfterTick = loadingCount(main);
  out.scrollAfterTick = scrollPos;
  out.tickWindowZoomed = windowOf(calls.find((c) => c.source === "s1"));
  out.tickWindowOther = windowOf(calls.find((c) => c.source === "s2"));
  out.zoomChipAfterTick = chipCount(main, "zoom-chip");
  out.markdownAfterTick = (doc.children[0] || { textContent: "" }).textContent;
  out.headingAfterTick = (all(main).find((n) => n.tag === "h2") || { textContent: "" }).textContent;

  /* A second tick lands the same way: the same document, no loading strip on the page. */
  calls.length = 0;
  await modules.views.renderView(main, "v1", { poll: true });
  await flush();
  out.secondTickLoadingOnPage = loadingCount(main);
  await settleAll();
  out.secondTickDocKept = main.children.includes(doc) && cells.every((c, i) => doc.children[i] === c);
  out.secondTickWindowZoomed = windowOf(calls.find((c) => c.source === "s1"));
}

console.log(JSON.stringify(out));
