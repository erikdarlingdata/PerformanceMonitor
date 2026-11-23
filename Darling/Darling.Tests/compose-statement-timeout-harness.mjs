/* #9357: runs a saved Custom View dashboard and a saved notebook (pages/views.js renderView) with the shipped views.js, compose.js, charts.js,
   panels.js and util.js against a scripted /api/compose/run, polls it the way app.js's auto-refresh does (renderView with { poll: true }), and
   prints as one line of JSON which cells each render sent. One cell is stopped at the store's statement timeout (400 with reason
   "statement_timeout"), one fails another way (400), one succeeds. ComposeStatementTimeoutRefreshBehaviourTests starts it as
       node compose-statement-timeout-harness.mjs <path to wwwroot/js>
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
  set textContent(v) { this._text = String(v); this.children = [];  }
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
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null;  return c; }
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
Object.defineProperty(globalThis, "scrollY", { get: () => 0, configurable: true });
globalThis.scrollTo = () => {};
/* The page watches a refreshing panel for its loading strip to go; here the watcher checks on a short timer instead. */
globalThis.MutationObserver = class {
  constructor(fn) { this.fn = fn; this.timer = null; }
  observe() { if (!this.timer) { this.timer = setInterval(() => this.fn(), 5); this.timer.unref(); } }
  disconnect() { clearInterval(this.timer); this.timer = null; }
};
globalThis.location = { hash: "#/view/v1" };

/* The scripted service: one saved view, and every compose run answered at once by its source. `calls` is every run the page sent, in order.
   s1 is the cell the store stops at its statement timeout (until `s1Works` is set), s2 fails with another 400, s3 succeeds. */
const SOURCES = ["s1", "s2", "s3"];
let view = null;
const calls = [];
let s1Works = false;
const reply = (status, body) => ({ status, ok: status >= 200 && status < 300, headers: { get: () => null }, text: async () => JSON.stringify(body) });
const T0 = Date.UTC(2026, 0, 1, 0, 0, 0);
const MIN = 60000;
const lineRows = () => Array.from({ length: 11 }, (_, i) => [["AAA", 1000 + i * 100], ["BBB", i % 5]].map(([w, v]) => ({ bucket: new Date(T0 + i * MIN).toISOString().slice(0, 19), wait: w, value: v }))).flat();
const TIMEOUT_TEXT = "The store stopped this cell at its statement timeout. It was reading the past 24 hours for every server.";
function answerFor(source) {
  if (source === "s1" && !s1Works) return reply(400, { error: TIMEOUT_TEXT, detail: "canceling statement due to statement timeout", reason: "statement_timeout" });
  if (source === "s2") return reply(400, { error: "Query failed: the panel's own error." });
  return reply(200, { sql: "select 1", rows: lineRows() });
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
    calls.push({ source: body.panel.source, hours: body.hours ?? null });
    return answerFor(body.panel.source);
  }
  return reply(404, { error: "not scripted" });
};
process.on("unhandledRejection", (e) => { if (process.env.HARNESS_DEBUG) console.error(String(e && e.stack)); });

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "compose-statement-timeout-"));
let modules;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = { views: await load(path.join("pages", "views.js")), compose: await load("compose.js") };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const flush = async () => { for (let i = 0; i < 40; i++) await new Promise((r) => setTimeout(r, 0)); };
const cls = (n) => n.attrs.class || n.className || "";
const sourcesOf = (list) => list.map((c) => c.source);
const retryButtons = (root) => all(root).filter((n) => n.tag === "button" && n.textContent === "Retry");
const timeoutStrips = (root) => all(root).filter((n) => (n._text || "").startsWith("The store stopped this cell")).length;
const panelCell = (title, source) => ({ type: "panel", title, source, viz: "line", measure: "m", groupBy: ["wait"] });
const dashPanel = (title, source) => ({ title, source, viz: "line", measure: "m", groupBy: ["wait"] });
const out = {};

async function render(main, opts) {
  calls.length = 0;
  await modules.views.renderView(main, "v1", opts);
  await flush();
  return sourcesOf(calls);
}

for (const kind of ["dashboard", "notebook"]) {
  const r = {};
  view = kind === "dashboard"
    ? { id: "v1", name: "Fleet", definition: { refresh: "1m", panels: [dashPanel("Stopped", "s1"), dashPanel("Failed", "s2"), dashPanel("Fine", "s3")] } }
    : { id: "v1", name: "Fleet", definition: { kind: "notebook", refresh: "1m", cells: [panelCell("Stopped", "s1"), panelCell("Failed", "s2"), panelCell("Fine", "s3")] } };
  s1Works = false;
  const main = new FakeNode("main");
  main.isRoot = true;

  r.open = await render(main, undefined);
  r.openTimeoutStrips = timeoutStrips(main);
  r.openRetryButtons = retryButtons(main).length;

  /* The auto-refresh tick, twice: the stopped cell is not sent, the failed and the good cells are. The stopped cell still says so. */
  r.tick1 = await render(main, { poll: true });
  r.tick1TimeoutStrips = timeoutStrips(main);
  r.tick1RetryButtons = retryButtons(main).length;
  r.tick2 = await render(main, { poll: true });

  /* The cell's own Retry button (the first Retry on the page is the stopped cell's) sends only that cell, and again after it fails again. */
  calls.length = 0;
  retryButtons(main)[0].fire("click");
  await flush();
  r.retryClick = sourcesOf(calls);
  r.afterRetryTick = await render(main, { poll: true });

  /* The Refresh button is a poll with `manual`: every cell is sent. */
  r.manual = await render(main, { poll: true, manual: true });
  /* Opening the page again is the viewer's ask too. */
  r.tick3 = await render(main, { poll: true });
  r.reopen = await render(main, undefined);

  /* The store answers the cell this time: a click, and from then on the cell refreshes as any other. */
  s1Works = true;
  r.tickWhileHeld = await render(main, { poll: true });
  calls.length = 0;
  retryButtons(main)[0].fire("click");
  await flush();
  r.retryWhenWorking = sourcesOf(calls);
  r.tickAfterRecovered = await render(main, { poll: true });
  r.tickAfterRecoveredTimeoutStrips = timeoutStrips(main);
  out[kind] = r;
}

/* A different window is a different cell: the 24 hour cell is held, the 4 hour one runs, and the held one stays held. */
{
  s1Works = false;
  calls.length = 0;
  modules.compose.forgetStoppedCells();
  const draw = async (hours) => {
    const card = modules.compose.renderComposedPanelCard(dashPanel("Stopped", "s1"), { server: null, hours }, null, 0);
    await flush();
    return card;
  };
  await draw(24);
  const first = calls.length;
  await draw(24);
  const sameWindow = calls.length - first;
  await draw(4);
  const otherWindow = calls.length - first - sameWindow;
  await draw(24);
  out.window = { first, sameWindow, otherWindow, backToFirst: calls.length - first - sameWindow - otherWindow };
}

console.log(JSON.stringify(out));
process.exit(0);
