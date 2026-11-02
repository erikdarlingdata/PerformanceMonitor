/* #7257: runs a saved custom view (pages/views.js renderView) with the shipped views.js, compose.js, panels.js and util.js against a scripted
   /api/compose/run, and prints as one line of JSON what the page asked for and drew. CustomViewPanelLimitBehaviourTests starts it as
       node custom-view-panel-limit-harness.mjs <path to wwwroot/js>
   The whole js tree is copied into a scratch folder first (#5279). Only the DOM and fetch are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];

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
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  appendChild(c) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    this.children.push(c);
    return c;
  }
  insertBefore(c, ref) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i < 0) this.children.push(c);
    else this.children.splice(i, 0, c);
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; return c; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, target: this, ...e }); }
}
globalThis.Node = FakeNode;
globalThis.document = {
  body: new FakeNode("body"),
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeNode("#text", t),
  createDocumentFragment: () => new FakeNode("#fragment"),
  addEventListener() {},
  removeEventListener() {},
};
/* The page watches a refreshing panel for its loading strip to go; here the watcher checks on a short timer instead. */
globalThis.MutationObserver = class {
  constructor(fn) { this.fn = fn; this.timer = null; }
  observe() { if (!this.timer) this.timer = setInterval(() => this.fn(), 5); }
  disconnect() { clearInterval(this.timer); this.timer = null; }
};
globalThis.location = { hash: "#/view/v1" };

/* The scripted service. A view is a list of panels (or notebook cells) by id. Every compose run is held until the test answers it:
   `calls` is every run the page sent, in order, and `answer(call, status)` settles one (200 with a one-row table, or 429 with Retry-After 1). */
const SOURCES = ["s1", "s2", "s3", "s4", "s5", "s6", "s7"];
const READS = ["r1", "r2", "r3", "r4"];
let view = null;
const calls = [];
let autoAnswer = false;
const BUSY_TEXT = "The service is running as many long reads as it allows (2), and 0 more are waiting. Try again in about 48 seconds.";
const reply = (status, body, headers = {}) => ({ status, ok: status >= 200 && status < 300, headers: { get: (h) => headers[h.toLowerCase()] ?? null }, text: async () => JSON.stringify(body) });
const okBody = () => ({ columns: [{ name: "n", label: "n" }], rows: [{ n: 1 }], row_count: 1 });
function answer(call, status) {
  if (!call) return; /* a read the page never sent: the test then sees the missing call in what it recorded */
  call.answered = status;
  call.resolve(status === 400 ? reply(400, { error: "hours exceeds maximum of 24 hours for this read." }) : status === 429 ? reply(429, { error: BUSY_TEXT }, { "retry-after": "1" }) : status === 500 ? reply(500, { error: "The read failed." }) : reply(200, call.source.startsWith("r") ? { rows: [{ n: 1 }] } : okBody()));
}
globalThis.fetch = async (url, init) => {
  const u = new URL(String(url), "http://viewer.test");
  if (u.pathname === "/api/session") return reply(200, { can_edit: true });
  if (u.pathname === "/api/catalog") {
    return reply(200, { reads: READS.map((r) => ({ name: r })), viz: [], compose: { measures: SOURCES.map((s) => ({ source: s, measure: "m", label: s })), dimensions: [], annotationSources: [] } });
  }
  if (u.pathname === "/api/fleet") return reply(200, { cards: [{ server_name: "a", display_name: "Server A" }, { server_name: "b", display_name: "Server B" }] });
  if (u.pathname === "/api/views/v1") return reply(200, view);
  if (u.pathname.startsWith("/api/read/")) {
    /* A read panel's read, held and answered like a compose run: `source` carries the read's name. */
    const call = { source: u.pathname.slice("/api/read/".length), server: JSON.stringify(null), answered: null };
    calls.push(call);
    const settled = new Promise((resolve) => { call.resolve = resolve; });
    if (autoAnswer) answer(call, 200);
    return settled;
  }
  if (u.pathname === "/api/compose/run") {
    const body = JSON.parse(init.body);
    const call = { source: body.panel.source, server: JSON.stringify(body.server ?? null), answered: null };
    calls.push(call);
    const settled = new Promise((resolve) => { call.resolve = resolve; });
    if (autoAnswer) answer(call, 200);
    return settled;
  }
  return reply(404, { error: "not scripted" });
};
process.on("unhandledRejection", (e) => { if (process.env.HARNESS_DEBUG) console.error(String(e && e.stack)); });

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "custom-view-panel-limit-"));
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
const flush = async () => { for (let i = 0; i < 30; i++) await new Promise((r) => setTimeout(r, 0)); };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const panel = (source, extra = {}) => ({ title: "Panel " + source, source, viz: "table", measure: "m", ...extra });
const rpanel = (read, extra = {}) => ({ title: "Read " + read, read, viz: "table", columns: [{ key: "n", label: "n" }], rowsKey: "rows", ...extra });
const dashboard = (panels) => ({ id: "v1", name: "V", definition: { panels } });
const notebook = (cells) => ({ id: "v1", name: "N", definition: { kind: "notebook", cells } });
const cls = (n) => n.attrs.class || n.className || "";
const stripsOf = (root) => all(root).filter((n) => /\bstrip\b/.test(cls(n))).map((n) => ({ cls: cls(n), text: n.textContent }));
const waitingCount = (root) => stripsOf(root).filter((s) => s.text.startsWith("Waiting")).length;
const loadingCount = (root) => stripsOf(root).filter((s) => /\bloading\b/.test(s.cls)).length;
const retryButtons = (root) => all(root).filter((n) => n.tag === "button" && n.textContent === "Retry");
const sourcesOf = (list) => list.map((c) => c.source);
async function open(def, opts) {
  view = def;
  calls.length = 0;
  const main = new FakeNode("main");
  main.isRoot = true;
  await modules.views.renderView(main, "v1", opts);
  await flush();
  return main;
}
const out = {};

/* Seven panels: two reads start, and each next read starts only when an earlier one ends. */
{
  const main = await open(dashboard(SOURCES.map((s) => panel(s))));
  const started = [sourcesOf(calls)];
  out.sevenWaitingAtStart = waitingCount(main);
  let maxOpen = 0;
  for (let i = 0; i < 7; i++) {
    const open = calls.filter((c) => c.answered === null);
    maxOpen = Math.max(maxOpen, open.length);
    answer(open[0], 200);
    await flush();
    started.push(sourcesOf(calls));
  }
  out.sevenStarted = started;
  out.sevenMaxOpen = maxOpen;
  out.sevenLoadingAtEnd = loadingCount(main);
  out.sevenErrorsAtEnd = stripsOf(main).filter((s) => /\berror\b/.test(s.cls)).length;
}

/* A 429 then a 200: the panel shows the waiting-to-retry strip, then draws. It sends two reads, and keeps its slot while it waits. */
{
  const main = await open(dashboard([panel("s1"), panel("s2"), panel("s3")]));
  answer(calls[0], 429);
  await flush();
  out.busyOnceStrips = stripsOf(main).map((s) => s.text);
  out.busyOnceCallsWhileWaiting = sourcesOf(calls);
  await sleep(1300);
  await flush();
  out.busyOnceCallsAfterWait = sourcesOf(calls);
  answer(calls[2], 200);
  answer(calls[1], 200);
  await flush();
  answer(calls[3], 200);
  await flush();
  out.busyOnceCallsAtEnd = sourcesOf(calls);
  out.busyOnceLoadingAtEnd = loadingCount(main);
  out.busyOnceErrorsAtEnd = stripsOf(main).filter((s) => /\berror\b/.test(s.cls)).length;
}

/* Two 429 answers: the panel shows the error and a Retry button. Retry sends one more read for that panel only, through the limiter. */
{
  const main = await open(dashboard([panel("s1"), panel("s2")]));
  answer(calls[1], 200);
  await flush();
  answer(calls[0], 429);
  await sleep(1300);
  await flush();
  answer(calls[2], 429);
  await flush();
  out.busyTwiceCalls = sourcesOf(calls);
  out.busyTwiceErrors = stripsOf(main).filter((s) => /\berror\b/.test(s.cls)).map((s) => s.text);
  out.busyTwiceRetryButtons = retryButtons(main).length;
  retryButtons(main)[0]?.fire("click");
  await flush();
  out.retryCalls = sourcesOf(calls);
  out.retryWaiting = stripsOf(main).map((s) => s.text);
  answer(calls[3], 200);
  await flush();
  out.retryLoadingAtEnd = loadingCount(main);
  out.retryButtonsAtEnd = retryButtons(main).length;
}

/* A server error shows Retry too, and a Retry while two other reads hold both slots waits for one of them to end. */
{
  const main = await open(dashboard([panel("s1"), panel("s2"), panel("s3")]));
  answer(calls[0], 500);
  await flush();
  out.serverErrorRetryButtons = retryButtons(main).length;
  retryButtons(main)[0]?.fire("click");
  await flush();
  out.serverErrorRetryCallsWhileFull = sourcesOf(calls);
  out.serverErrorRetryStrips = stripsOf(main).map((s) => s.text);
  answer(calls[1], 200);
  await flush();
  out.serverErrorRetryCallsAfterOneEnds = sourcesOf(calls);
}

/* A scope change while panels wait: no waiting panel of the old scope starts a read. */
{
  const main = await open(dashboard([panel("s1"), panel("s2"), panel("s3"), panel("s4")]));
  const oldScope = calls[0].server;
  const label = all(main).find((n) => n.tag === "label" && n.textContent === "Server B");
  const box = label.children[0];
  box.checked = true;
  box.fire("change");
  await flush();
  out.scopeCallsAfterChange = calls.map((c) => c.server === oldScope ? "old:" + c.source : "new:" + c.source);
  for (let i = 0; i < 8; i++) {
    const open = calls.filter((c) => c.answered === null);
    if (!open.length) break;
    answer(open[0], 200);
    await flush();
  }
  out.scopeCallsAtEnd = calls.map((c) => c.server === oldScope ? "old:" + c.source : "new:" + c.source);
  out.scopeLoadingAtEnd = loadingCount(main);
}

/* An in-place refresh that a newer render cancels: its waiting panels do not start either. */
{
  const def = dashboard([panel("s1"), panel("s2"), panel("s3"), panel("s4")]);
  view = def;
  autoAnswer = true;
  const main = await open(def);
  autoAnswer = false;
  calls.length = 0;
  await modules.views.renderView(main, "v1", { poll: true });
  await flush();
  out.pollFirstStarted = sourcesOf(calls);
  await modules.views.renderView(main, "v1", { poll: true });
  await flush();
  for (let i = 0; i < 12; i++) {
    const open = calls.filter((c) => c.answered === null);
    if (!open.length) break;
    answer(open[0], 200);
    await flush();
  }
  out.pollCallsAtEnd = sourcesOf(calls);
}

/* A markdown cell and a panel with an unknown source take no slot (and a panel the catalog refuses has no Retry button). */
{
  const cells = [
    { type: "markdown", text: "# Notes" },
    panel("nope", { type: "panel", title: "Unknown" }),
    panel("nope2", { type: "panel", title: "Unknown too" }),
    { ...panel("s1"), type: "panel" },
    { ...panel("s2"), type: "panel" },
    { ...panel("s3"), type: "panel" },
  ];
  const main = await open(notebook(cells));
  out.noSlotStarted = sourcesOf(calls);
  out.noSlotText = main.textContent;
  out.noSlotRetryButtons = retryButtons(main).length;
  out.noSlotWaiting = waitingCount(main);
  answer(calls[0], 200);
  await flush();
  out.noSlotStartedAfterOne = sourcesOf(calls);
}

/* A read panel answered 429 then 200: the busy strip, then its data, and the panel holds its slot while it waits. */
{
  const main = await open(dashboard([rpanel("r1"), rpanel("r2"), rpanel("r3")]));
  answer(calls[0], 429);
  await flush();
  out.readBusyOnceStrips = stripsOf(main).map((s) => s.text);
  out.readBusyOnceCallsWhileWaiting = sourcesOf(calls);
  await sleep(1300);
  await flush();
  out.readBusyOnceCallsAfterWait = sourcesOf(calls);
  answer(calls[2], 200);
  answer(calls[1], 200);
  await flush();
  answer(calls[3], 200);
  await flush();
  out.readBusyOnceCallsAtEnd = sourcesOf(calls);
  out.readBusyOnceLoadingAtEnd = loadingCount(main);
  out.readBusyOnceErrorsAtEnd = stripsOf(main).filter((s) => /\berror\b/.test(s.cls)).length;
}

/* A read panel answered 429 twice: the error and a Retry button. Retry sends one more read for that panel only, through the limiter.
   A panel naming an unknown read has no button. */
{
  const main = await open(dashboard([rpanel("r1"), rpanel("r2"), rpanel("nope")]));
  out.readUnknownRetryButtons = retryButtons(main).length;
  answer(calls[1], 200);
  await flush();
  answer(calls[0], 429);
  await sleep(1300);
  await flush();
  answer(calls[2], 429);
  await flush();
  out.readBusyTwiceCalls = sourcesOf(calls);
  out.readBusyTwiceErrors = stripsOf(main).filter((s) => /\berror\b/.test(s.cls)).map((s) => s.text);
  out.readBusyTwiceRetryButtons = retryButtons(main).length;
  retryButtons(main)[0]?.fire("click");
  await flush();
  out.readRetryCalls = sourcesOf(calls);
  answer(calls[3], 200);
  await flush();
  out.readRetryLoadingAtEnd = loadingCount(main);
  out.readRetryButtonsAtEnd = retryButtons(main).length;
  out.readRetryErrorsAtEnd = stripsOf(main).filter((s) => /\berror\b/.test(s.cls)).length;
}

/* A range-too-long answer shows the notice and no Retry button. */
{
  const main = await open(dashboard([rpanel("r1")]));
  answer(calls[0], 400);
  await flush();
  out.rangeNotice = stripsOf(main).map((s) => s.text);
  out.rangeRetryButtons = retryButtons(main).length;
}

console.log(JSON.stringify(out));
