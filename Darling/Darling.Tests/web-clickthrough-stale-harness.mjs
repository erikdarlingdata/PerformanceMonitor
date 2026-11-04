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
    /* Reads and writes className, which is what the CSS reads (.detail.clamp-lines.open), so a scenario can see whether a class was set. */
    const tokens = () => String(this.className).split(/\s+/).filter(Boolean);
    const write = (list) => { this.className = list.join(" "); };
    this.classList = {
      add: (name) => { if (!tokens().includes(name)) write([...tokens(), name]); },
      remove: (name) => write(tokens().filter((t) => t !== name)),
      toggle: (name, force) => {
        const on = force === undefined ? !tokens().includes(name) : !!force;
        write(on ? (tokens().includes(name) ? tokens() : [...tokens(), name]) : tokens().filter((t) => t !== name));
        return on;
      },
      contains: (name) => tokens().includes(name),
    };
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
/* The in-place refresh (#6933) watches a staged panel for its loading strip to go. These scenarios do not read what lands, so the observer never fires. */
globalThis.MutationObserver = class { observe() {} disconnect() {} };
/* renderAg and renderFleet stop when the route changed under them, so they read location.hash. */
if (typeof globalThis.location === "undefined") globalThis.location = { hash: "#/ag" };
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
let agPayload = {};
globalThis.fetch = async (url, init) => {
  fetches.push(String(url));
  await new Promise((r) => setTimeout(r, 5));
  const raw = JSON.stringify(String(url).includes("/api/ag") ? agPayload : {});
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
    ag: await load(path.join("pages", "ag.js")),
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
const chips = (node) => all(node, (n) => String(n.className).startsWith("metric-chip")).map((c) => {
  const detail = (c.children || []).find((d) => d.className === "detail");
  return { cls: c.className, text: c.textContent, detail: detail ? detail.textContent : null, detailTitle: detail ? detail.getAttribute("title") : null };
});

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
  /* #6901: a chip's detail is clamped to one line with an ellipsis, so the full sentence has to be on the detail's title. */
  longDetail: async () => ({
    chips: chips(modules.fleet.metricBands(card({
      last_collection: iso(1000),
      agent_alert_gaps: [{ message: "No alert is set for severity 19 through 25 errors." }, { message: "No alert is set for error 823, 824 or 825 (corruption)." }],
    }))),
  }),
  /* #7395: the server page passes the shared tiles through clampTileDetails. A detail past about three lines (the Protection tile
     of a server with 20 overdue items) is clamped and opens on demand, with the full text on its title. A short one shows in
     full. The fleet card's tiles, which never pass through it, stay exactly as metricBands built them. */
  serverTiles: async () => {
    const overdue = Array.from({ length: 20 }, (_, i) => ({ text: "AppDatabase" + i + " full backup 9 days" }));
    const c = card({ last_collection: iso(1000), database_protection_overdue: overdue, agent_alert_gaps: [{ message: "No alert for severity 19." }] });
    const detailOf = (chip) => (chip.children || []).find((d) => String(d.className).split(" ").includes("detail"));
    const view = (root) => all(root, (n) => String(n.className).startsWith("metric-chip")).map((chip) => {
      const d = detailOf(chip);
      return d ? { label: chip.children[0].textContent, cls: d.className, role: d.getAttribute("role"), expanded: d.getAttribute("aria-expanded"), titleLen: (d.getAttribute("title") || "").length, textLen: d.textContent.length } : { label: chip.children[0].textContent };
    });
    const server = modules.util.clampTileDetails(modules.fleet.metricBands(c));
    const fleetCard = modules.fleet.metricBands(c);
    const protection = detailOf(all(server, (n) => String(n.className).startsWith("metric-chip")).find((chip) => chip.children[0].textContent.startsWith("Protection")));
    let stopped = 0;
    const press = (type, extra = {}) => (protection.listeners[type] || []).forEach((fn) => fn({ type, stopPropagation() { stopped++; }, preventDefault() {}, ...extra }));
    const before = protection.getAttribute("aria-expanded");
    press("click");
    const afterClick = protection.getAttribute("aria-expanded");
    press("keydown", { key: "Enter" });
    const afterEnter = protection.getAttribute("aria-expanded");
    return { server: view(server), fleetCard: view(fleetCard), before, afterClick, afterEnter, stopped };
  },
  /* #7395: the server page draws its head again on every poll and tab click. A tile the reader opened stays open across the redraw
     (matched by label, not by text, which changes between polls), a tile that is now too short to clamp has nothing to reopen and
     is closed if it grows back, and a tile the reader never opened stays closed. The Set is what server.js keeps for one server. */
  serverTilesReopen: async () => {
    const items = (n) => Array.from({ length: n }, (_, i) => ({ text: "AppDatabase" + i + " full backup 9 days" }));
    const draw = (count, open) => modules.util.clampTileDetails(modules.fleet.metricBands(card({
      last_collection: iso(1000), database_protection_overdue: items(count), agent_alert_gaps: [{ message: "No alert for severity 19." }],
    })), undefined, open);
    const detailOf = (root, label) => {
      const chip = all(root, (n) => String(n.className).startsWith("metric-chip")).find((c) => c.children[0].textContent.startsWith(label));
      const d = chip && (chip.children || []).find((x) => String(x.className).split(" ").includes("detail"));
      return d ? { cls: d.className, expanded: d.getAttribute("aria-expanded") } : null;
    };
    const click = (root) => {
      const d = all(root, (n) => String(n.className).includes("clamp-lines"))[0];
      d.listeners.click.forEach((fn) => fn({ type: "click", stopPropagation() {} }));
    };
    const open = new Set();
    const first = draw(20, open);
    const closedAtFirst = detailOf(first, "Protection").expanded;
    click(first);
    const openedByClick = detailOf(first, "Protection").expanded;
    const clsAfterClick = detailOf(first, "Protection").cls;
    const labelsAfterClick = Array.from(open);
    const poll = draw(25, open); // the next poll: different text, same tile
    const afterPoll = detailOf(poll, "Protection");
    const labelsAfterPoll = Array.from(open);
    const short = draw(1, open); // one item overdue: the detail is short, so the tile is not clamped
    const afterShort = detailOf(short, "Protection");
    const labelsAfterShort = Array.from(open);
    const regrown = draw(20, open); // overdue again: the reader closed nothing, but the tile went short in between, so it starts closed
    const afterRegrown = detailOf(regrown, "Protection");
    const another = draw(20, new Set()); // another server: its own empty set
    const afterAnotherServer = detailOf(another, "Protection");
    return { closedAtFirst, openedByClick, clsAfterClick, labelsAfterClick, afterPoll, labelsAfterPoll, afterShort, labelsAfterShort, afterRegrown, afterAnotherServer };
  },
  /* #6828: a server whose collectors are 39 HEALTHY, 3 WARNING and 2 not collected: the Collectors chip shows the warning count and does not call them healthy. */
  collectorsWarning: async () => ({
    chips: chips(modules.fleet.metricBands(card({
      last_collection: iso(1000), collector_count: 44, healthy_collector_count: 39, warning_collector_count: 3, not_collected_collector_count: 2,
    }))),
  }),
  /* #6828: the same server with nothing uncollected shows no "not collected" count. */
  collectorsNoneNotCollected: async () => ({
    chips: chips(modules.fleet.metricBands(card({
      last_collection: iso(1000), collector_count: 44, healthy_collector_count: 41, warning_collector_count: 3, not_collected_collector_count: 0,
    }))),
  }),
  /* #6178: a server with no thread reading. On an Azure SQL Database (engine_edition 5) the Threads chip says why; on a box it stays a bare n/a. */
  azureThreads: async () => {
    const noThreads = { available_threads: null, total_threads: null, threads_severity: "Unknown", last_collection: iso(1000) };
    return {
      azure: chips(modules.fleet.metricBands(card({ ...noThreads, engine_edition: 5 }))),
      box: chips(modules.fleet.metricBands(card({ ...noThreads, engine_edition: 3 }))),
    };
  },
  agStale: async () => {
    /* The Availability Groups card subtitle of a group the page was told is stale, and of a fresh one. */
    const group = (over) => ({
      group_name: "AG1", server_name: "SRV1", collection_time: iso(12 * 86400000), severity: "Unknown",
      replicas: [], databases: [], ...over,
    });
    const subtitles = async (g) => {
      agPayload = { generated_at: iso(0), availability_groups: [g] };
      const root = new FakeNode("main");
      await modules.ag.renderAg(root);
      return all(root, (n) => String(n.className).includes("status-line")).map((n) => ({ text: n.textContent }));
    };
    return {
      stale: await subtitles(group({ is_stale: true })),
      fresh: await subtitles(group({ is_stale: false, collection_time: iso(1000), severity: "Healthy" })),
    };
  },
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
