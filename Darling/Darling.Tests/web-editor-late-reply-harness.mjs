/* #7937: runs the shipped dashboard composer's read-panel editor (editor.js renderEditor) and the alert-rule editor
   (alert-editor.js renderAlertEditor) against a scripted service whose replies are held until the test releases them, and prints as
   one line of JSON what each editor showed once a reply landed after the author had moved on. WebEditorLateReplyBehaviourTests
   starts it as
       node web-editor-late-reply-harness.mjs <path to wwwroot/js>
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
    this.disabled = false;
  }
  get parentNode() { return this.parent; }
  get isConnected() { return true; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  removeAttribute(k) { delete this.attrs[k]; }
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
  replaceChildren(...nodes) { this.children = []; this._text = ""; for (const n of nodes) this.appendChild(n); }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; return c; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  removeEventListener() {}
  focus() {}
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, stopPropagation() {}, target: this, ...e }); }
}
globalThis.Node = FakeNode;
globalThis.window = globalThis;
globalThis.document = {
  body: new FakeNode("body"),
  querySelectorAll: () => [],
  querySelector: () => null,
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeNode("#text", t),
  createDocumentFragment: () => new FakeNode("#fragment"),
  addEventListener() {},
  removeEventListener() {},
};
globalThis.MutationObserver = class { observe() {} disconnect() {} };
globalThis.location = { hash: "#/views/new", pathname: "/", search: "", href: "http://viewer.test/", origin: "http://viewer.test" };
const store = () => { const d = {}; return { getItem: (k) => (k in d ? d[k] : null), setItem: (k, v) => { d[k] = String(v); }, removeItem: (k) => { delete d[k]; } }; };
globalThis.localStorage = store();
globalThis.sessionStorage = store();

/* The scripted service. Every read sample, validate and test is HELD until the test releases it with answer(). */
const reads = [];
const checks = [];
const reply = (status, body) => ({ status, ok: status >= 200 && status < 300, headers: { get: () => null }, text: async () => JSON.stringify(body) });
const held = (list, call) => { list.push(call); return new Promise((resolve) => { call.resolve = resolve; }); };
function answer(call, body) {
  if (!call) return;
  call.answered = true;
  call.resolve(reply(200, body));
}
globalThis.fetch = async (url, init = {}) => {
  const u = new URL(String(url), "http://viewer.test");
  const method = (init.method || "GET").toUpperCase();
  if (u.pathname === "/api/session") return reply(200, { can_edit: true });
  if (u.pathname === "/api/catalog") {
    return reply(200, {
      reads: [{ name: "r1", category: "Waits", params: [] }, { name: "r2", category: "Waits", params: [] }],
      viz: ["table", "stat", "line"],
      compose: {
        measures: [{ source: "wait_stats", key: "wait_ms", measure: "wait_ms", displayName: "Wait time", label: "Wait time", kind: "measure", validAggs: ["sum"], defaultAgg: "sum", unitFamily: "duration", category: "Waits" }],
        dimensions: [], annotationSources: [],
      },
    });
  }
  if (u.pathname === "/api/fleet") return reply(200, { cards: [] });
  if (u.pathname === "/api/views" && method === "GET") return reply(200, []);
  if (u.pathname === "/api/alert-templates") return reply(200, { templates: [] });
  if (u.pathname.startsWith("/api/read/")) return held(reads, { tool: u.pathname.slice("/api/read/".length), answered: false });
  if (u.pathname === "/api/alerts/validate" && method === "POST") {
    const def = JSON.parse(init.body).definition || {};
    return held(checks, { kind: "validate", warn: def.predicate ? def.predicate.warnThreshold : null, answered: false });
  }
  if (u.pathname === "/api/alerts/test" && method === "POST") {
    const def = JSON.parse(init.body).definition || {};
    return held(checks, { kind: "test", warn: def.predicate ? def.predicate.warnThreshold : null, answered: false });
  }
  return reply(404, { error: "not scripted" });
};
process.on("unhandledRejection", (e) => { if (process.env.HARNESS_DEBUG) console.error(String(e && e.stack)); });

/* The page's timers, tracked so the test can fire what is pending (the preview and check debounces) instead of waiting for its
   wall-clock time: see firePendingTimers. The harness's own flush uses the untracked original. */
const realSetTimeout = globalThis.setTimeout;
const realClearTimeout = globalThis.clearTimeout;
const pendingTimers = new Map();
globalThis.setTimeout = (fn, ms, ...args) => {
  const run = () => { pendingTimers.delete(handle); fn(...args); };
  const handle = realSetTimeout(run, ms);
  pendingTimers.set(handle, run);
  return handle;
};
globalThis.clearTimeout = (handle) => { pendingTimers.delete(handle); realClearTimeout(handle); };

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "web-editor-late-reply-"));
let editor;
let alertEditor;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  editor = await load("editor.js");
  alertEditor = await load("alert-editor.js");
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const flush = async () => { for (let i = 0; i < 40; i++) await new Promise((r) => realSetTimeout(r, 0)); };
/* Fires every timer the page has pending, and whatever those schedule, until none is left. */
async function firePendingTimers() {
  for (let round = 0; pendingTimers.size; round++) {
    if (round === 50) throw new Error("the page keeps scheduling timers, so they cannot be run down");
    for (const [handle, run] of [...pendingTimers]) { realClearTimeout(handle); run(); }
    await flush();
  }
}
const cls = (n) => n.attrs.class || n.className || "";
const byLabel = (root, label) => all(root).find((n) => n.attrs["aria-label"] === label);
const byText = (root, tag, text) => all(root).find((n) => n.tag === tag && n.textContent === text);
const pick = async (root, label, value) => { const sel = byLabel(root, label); sel.value = value; sel.fire("change"); await flush(); };
const open = () => reads.filter((r) => !r.answered);
const openChecks = () => checks.filter((c) => !c.answered);
const rowsKeyOptions = (root) => { const box = all(root).find((n) => cls(n) === "vizcfg-box"); return box ? all(box).filter((n) => n.tag === "option").map((n) => n.attrs.value).filter((v) => v && /_rows$/.test(v)) : []; };

/* Settles the composer before this one. Its preview timer (editor.js, PREVIEW_DEBOUNCE_MS after the last change) sends a read of
   its panel's read at the same path a sample uses, and a sample the next composer sends for that path JOINS the open request
   (util.js apiGetJoined) instead of reaching `reads`. So fire what is pending, answer what is open, and let the joined request
   clear, until nothing of either is left. */
async function settleReads() {
  for (let round = 0; pendingTimers.size || open().length; round++) {
    if (round === 50) throw new Error("the composer keeps starting reads, so it cannot be settled");
    await firePendingTimers();
    for (const r of open()) answer(r, {});
    await flush();
  }
}

/* A fresh dashboard composer with its first panel switched to a read panel. */
async function readPanelEditor() {
  await settleReads();
  reads.length = 0;
  const main = new FakeNode("main");
  await editor.renderEditor(main, "new");
  await flush();
  byText(main, "button", "Read (advanced)").fire("click");
  await flush();
  return main;
}

const out = {};

/* 1. The author picks a read, then a chart type while the read's sample is still on its way. The sample's suggestion must not
      replace the pick. */
{
  const main = await readPanelEditor();
  await pick(main, "Read", "r1");
  out.pickInFlight = open().map((r) => r.tool);
  await pick(main, "Visualization", "line");
  out.pickVizBeforeReply = byLabel(main, "Visualization").value;
  answer(open()[0], { a1_rows: [{ x: 1 }] });
  await flush();
  out.pickVizAfterReply = byLabel(main, "Visualization").value;
  out.pickRowsKeys = rowsKeyOptions(main);
}

/* 2. The author changes the read twice before either sample returns, and the OLDER sample returns last. The fields must come from
      the read the panel is on, not the one it left. */
{
  const main = await readPanelEditor();
  await pick(main, "Read", "r1");
  await pick(main, "Read", "r2");
  out.twiceInFlight = open().map((r) => r.tool);
  answer(reads.find((r) => r.tool === "r2"), { b2_rows: [{ y: 1 }] });
  await flush();
  out.twiceRowsKeysAfterNewer = rowsKeyOptions(main);
  out.twiceVizAfterNewer = byLabel(main, "Visualization").value;
  answer(reads.find((r) => r.tool === "r1"), { a1_rows: [{ x: 1 }] });
  await flush();
  out.twiceRowsKeysAfterOlder = rowsKeyOptions(main);
  out.twiceReadAtEnd = byLabel(main, "Read").value;
}

/* 3. The alert-rule editor: a second check starts while the first is still waiting on validate. The first check's replies must not
      land over the second's "Checking…" strip. */
{
  checks.length = 0;
  const main = new FakeNode("main");
  await alertEditor.renderAlertEditor(main, "new");
  await flush();
  const preview = () => all(main).find((n) => cls(n) === "alert-preview").textContent;
  await pick(main, "Metric", "wait_ms");
  await pick(main, "Aggregate", "sum");
  const warn = byLabel(main, "Warning threshold");
  warn.value = "10";
  warn.fire("input");
  await firePendingTimers(); /* the check debounce fires: the first check has sent its validate */
  out.alertFirstChecks = openChecks().map((c) => c.kind + ":" + c.warn);
  warn.value = "20";
  warn.fire("input");
  await firePendingTimers();
  out.alertChecksAfterSecondEdit = openChecks().map((c) => c.kind + ":" + c.warn);
  out.alertPreviewWhileBothWait = preview();
  /* The OLDER validate answers valid first. */
  answer(checks.find((c) => c.kind === "validate" && c.warn === 10), { valid: true });
  await flush();
  out.alertTestsAfterOlderValidate = checks.filter((c) => c.kind === "test").map((c) => c.warn);
  const olderTest = checks.find((c) => c.kind === "test" && c.warn === 10);
  if (olderTest) {
    answer(olderTest, { status: "ok", note: "Tested with warning 10", results: [{ server_name: "srv-a", current_value: 15 }] });
    await flush();
  }
  out.alertPreviewAfterOlderReplies = preview();
  /* Then the newer validate and test land. */
  answer(checks.find((c) => c.kind === "validate" && c.warn === 20), { valid: true });
  await flush();
  answer(checks.find((c) => c.kind === "test" && c.warn === 20), { status: "ok", note: "Tested with warning 20", results: [{ server_name: "srv-a", current_value: 15 }] });
  await flush();
  out.alertPreviewAtEnd = preview();
  out.alertAllChecks = checks.map((c) => c.kind + ":" + c.warn);
}

console.log(JSON.stringify(out));
