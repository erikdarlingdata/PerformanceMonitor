/* Runs two pieces of the shipped web viewer under Node (#5489): the fleet card / server header metric chips
   (pages/fleet.js metricBands) for a server that stopped collecting, and the server page's Wait Stats tab, counting the reads
   it sends. WebClickthroughStaleStateTests starts it as
       node web-clickthrough-stale-harness.mjs <path to wwwroot/js> <scenario>
   `fetch` and the DOM are stand-ins; everything else is the shipped code. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

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
  appendChild(child) {
    this.children.push(child);
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
  focus() {
    globalThis.document.activeElement = this;
  }
  setSelectionRange(start, end) {
    this.selectionStart = start;
    this.selectionEnd = end;
  }
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
};


globalThis.Node = FakeNode;
globalThis.document = {
  activeElement: null,
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
};

const fetches = [];
globalThis.fetch = async (url, init) => {
  fetches.push(String(url));
  await new Promise((r) => setTimeout(r, 5));
  const raw = JSON.stringify({});
  return { status: 200, ok: true, text: async () => raw };
};

const rejections = [];
process.on("unhandledRejection", (e) => rejections.push(String(e && e.stack ? e.stack : e)));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "clickthrough-stale-"));
let modules;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = {
    util: await load("util.js"),
    tabs: await load(path.join("pages", "server-tabs.js")),
    fleet: await load(path.join("pages", "fleet.js")),
  };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (node, pred, found = []) => {
  if (!node || typeof node !== "object") return found;
  if (pred(node)) found.push(node);
  (node.children || []).forEach((c) => all(c, pred, found));
  return found;
};
const settle = async () => { for (let i = 0; i < 60; i++) await new Promise((r) => setTimeout(r, 2)); };
const chips = (node) => all(node, (n) => String(n.className).startsWith("metric-chip")).map((c) => ({ cls: c.className, text: c.textContent }));

const iso = (msAgo) => new Date(Date.now() - msAgo).toISOString().slice(0, 19);
const card = (over) => ({
  is_online: true, band: "Healthy", cpu_percent: 0, total_cpu_percent: 0, cpu_severity: "Healthy",
  available_threads: 77, total_threads: 100, threads_severity: "Healthy", has_memory_pressure: false, memory_severity: "Healthy",
  memory_mb: 908, buffer_pool_mb: 400, blocking_count: 0, blocking_severity: "Healthy", deadlock_count: 0, deadlock_severity: "Healthy",
  collector_count: 5, healthy_collector_count: 5, failed_collector_count: 0, collector_severity: "Healthy",
  last_collection: iso(12 * 86400000), ...over,
});

const scenarios = {
  cards: async () => ({
    offline: chips(modules.fleet.metricBands(card({ is_online: false }))),
    online: chips(modules.fleet.metricBands(card({ last_collection: iso(1000) }))),
  }),
  waits: async () => {
    const tab = modules.tabs.SERVER_TABS.find((t) => t.id === "waits");
    const root = new FakeNode("main");
    modules.util.mount(root, tab.build("SRV1", { hours: 168, label: "last 7 days", signal: new AbortController().signal }));
    await settle();
    const count = (name) => fetches.filter((f) => f.includes("/api/read/" + name + "?")).length;
    return { spinlock: count("get_spinlock_stats"), latch: count("get_latch_stats"), total: fetches.length };
  },
  joinAbort: async () => {
    /* Two callers join one read; the first one's signal aborts: it gets "aborted", the second still gets its answer,
       and the request was sent once. */
    const a = new AbortController();
    const first = modules.util.readTool("get_spinlock_stats", { server: "SRV1", hours: 24 }, a.signal);
    const second = modules.util.readTool("get_spinlock_stats", { server: "SRV1", hours: 24 });
    a.abort();
    const r1 = await first;
    const r2 = await second;
    const sentTogether = fetches.length;
    const later = await modules.util.readTool("get_spinlock_stats", { server: "SRV1", hours: 24 });
    return { first: r1.kind, second: r2.kind, later: later.kind, sentTogether, sent: fetches.length };
  },
};

const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);
const result = await chosen();
console.log(JSON.stringify({ ...result, rejections }));
