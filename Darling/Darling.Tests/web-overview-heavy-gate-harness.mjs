/* Runs the web viewer's shipped Overview reads (pages/overview-strips.js and the daily summary panels of pages/server-tabs/sql-overview.js)
   under Node against a recording fetch whose answers land only when the harness says so, and prints as one line of JSON which requests
   the tab sends and when (#8554): the strips' six reads, the baseline read and the daily summary range, as one server tab load asks for
   them. No clock is read: every step runs to the end of the microtask queue and then looks at what was sent.
   WebOverviewHeavyGateBehaviourTests starts it as
       node web-overview-heavy-gate-harness.mjs <path to wwwroot/js> */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 1];

class FakeNode {
  constructor(tag, text) {
    this.tag = tag; this.children = []; this.attrs = {}; this.dataset = {}; this.style = {}; this.className = "";
    this.listeners = {}; this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(c) { c.parent = this; this.children.push(c); return c; }
  removeChild(c) { const i = this.children.indexOf(c); if (i >= 0) this.children.splice(i, 1); return c; }
  setAttribute(n, v) { this.attrs[n] = String(v); }
  getAttribute(n) { return n in this.attrs ? this.attrs[n] : null; }
  addEventListener(t, l) { (this.listeners[t] = this.listeners[t] || []).push(l); }
  setPointerCapture() {}
  replaceChild(next, old) { const i = this.children.indexOf(old); if (i >= 0) this.children[i] = next; return old; }
  getBoundingClientRect() { return { left: 0, top: 0, width: 1000, height: 300 }; }
  set textContent(v) { this.children = []; this.text = String(v); }
  get textContent() { return (this.text || "") + this.children.map((c) => c.textContent).join(""); }
}
globalThis.window = globalThis;
globalThis.Node = FakeNode;
globalThis.document = {
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
  querySelectorAll: () => [],
};
globalThis.location = { hash: "#/server/A/overview", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
const stored = {};
const storage = {
  getItem: (k) => (Object.prototype.hasOwnProperty.call(stored, k) ? stored[k] : null),
  setItem: (k, v) => { stored[k] = String(v); },
  removeItem: (k) => { delete stored[k]; },
};
globalThis.localStorage = storage;
globalThis.sessionStorage = storage;
globalThis.ResizeObserver = class { observe() {} disconnect() {} };
/* The panels draw whatever the stand-in answers hold; what they draw is not under test, only what is asked for and when. */
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "overview-heavy-gate-"));
let strips;
let overview;
let tabs;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  strips = await import(pathToFileURL(path.join(scratch, "pages", "overview-strips.js")).href);
  overview = await import(pathToFileURL(path.join(scratch, "pages", "server-tabs", "sql-overview.js")).href);
  tabs = await import(pathToFileURL(path.join(scratch, "pages", "server-tabs.js")).href);
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

/* Every request sent, as the tool name, in the order fetch was called; `inFlight` holds the answers not yet landed, oldest first. */
const sent = [];
const inFlight = [];
const toolOf = (url) => {
  const m = /\/api\/read\/([^?]+)/.exec(String(url));
  return m ? m[1] : String(url);
};
globalThis.fetch = (url, init) =>
  new Promise((resolve, reject) => {
    const tool = toolOf(url);
    sent.push(tool);
    inFlight.push({
      tool,
      answer: () => resolve({ ok: true, status: 200, url: String(url), headers: { get: () => null }, text: async () => JSON.stringify({ status: "ok", server: "A" }) }),
    });
    if (init && init.signal) init.signal.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })), { once: true });
  });

const settle = async () => {
  for (let i = 0; i < 8; i++) await new Promise((r) => setImmediate(r));
};

const HEAVY = new Set(["get_server_trend", "get_memory_trend", "get_file_io_trend", "get_overview_baselines", "get_daily_summary_range"]);
const out = {};
let maxHeavyInFlight = 0;
const noteHeavy = () => { maxHeavyInFlight = Math.max(maxHeavyInFlight, inFlight.filter((r) => HEAVY.has(r.tool)).length); };

/* A server tab load: the shipped Overview tab's own build, as the server page calls it on a first paint (#8554 part 2: the order its reads are
   asked for in is the order the heavy-read gate lets them go). */
const overviewTab = tabs.SERVER_TABS.find((t) => t.id === "overview");
overviewTab.build("A", { hours: 24, label: "24h", signal: null });
await settle();
noteHeavy();
out.load = { sent: [...sent], heavyInFlight: inFlight.filter((r) => HEAVY.has(r.tool)).map((r) => r.tool) };

/* The answers land oldest first, one at a time; each landing may free a slot for the next heavy read. */
while (inFlight.length > 0) {
  const next = inFlight.shift();
  next.answer();
  await settle();
  noteHeavy();
}
out.heavySentOrder = sent.filter((t) => HEAVY.has(t));
out.sentAll = [...sent];
out.maxHeavyInFlight = maxHeavyInFlight;
console.log(JSON.stringify(out));
