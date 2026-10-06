/* Runs the web viewer's database filter (#5244, #5245): wwwroot/js/util.js (readTool, buildQuery, dbScopeChip),
   database-filter-reads.js (the four read classes) and pages/server-tabs.js, with a stand-in DOM and a recording fetch,
   and prints what the reads sent and what the chips drew as one line of JSON. WebDatabaseFilterBehaviourTests starts it as
       node web-database-filter-harness.mjs <path to wwwroot/js> <scenario>
   The whole js tree is copied into a scratch folder first, then the stand-ins are written over it: charts.js (the SVG
   renderer needs a real browser) always, and for the "seeded" scenarios database-filter-reads.js, which wraps the real
   module and moves reads into FILTERED. The shipped FILTERED list is empty until the pages that wire a read to the
   filter land, so a scenario that needs a filtered read seeds one; the plain census runs the module exactly as shipped.
   `fetch` and the DOM are the only other stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];
const scenario = process.argv[3];

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
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
};
globalThis.location = { hash: "#/server/SRV1" };

/* Every request, in order. `answer` is what the page reads back. */
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

/* The scenarios that need a filtered read seed FILTERED: "injection" and "chips" with a few reads the server tabs fetch,
   "censusSeeded" with every database-scoped read but the three deadlock reads, which stay unfiltered by design. */
const SEEDED = new Set(["injection", "chips", "censusSeeded"]);
const SEED_ALL_BUT_DEADLOCKS = scenario === "censusSeeded";
const SEED_SOME = ["get_top_queries_by_cpu", "get_active_queries", "get_query_store_top"];

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "database-filter-"));
let modules;
try {
  /* The whole js tree (js/, js/pages/ and every subdirectory) is copied rather than a hand-kept list (#5279), so a module a
     later PR adds needs no edit here. Every stand-in below is written AFTER the copy, so it still replaces the real file. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    "export const SERIES_COLORS = ['#111', '#222', '#333', '#444', '#555', '#666'];\n" +
      "export const CATEGORICAL_COLORS = SERIES_COLORS;\n" +
      "export function normalizeColor(color) { return color; }\n" +
      "export function renderLineChart() { return document.createElement('div'); }\n" +
      "export function zoomableLineChart() { return document.createElement('div'); }\n" +
      "export function chartZoomScope() { return ''; }\n"
  );
  if (SEEDED.has(scenario)) {
    fs.copyFileSync(path.join(scratch, "database-filter-reads.js"), path.join(scratch, "database-filter-reads-real.js"));
    fs.writeFileSync(
      path.join(scratch, "database-filter-reads.js"),
      'import * as real from "./database-filter-reads-real.js";\n' +
        "const DEADLOCKS = new Set(['get_deadlock_trend', 'get_deadlocks', 'get_deadlock_detail']);\n" +
        "const seed = " + (SEED_ALL_BUT_DEADLOCKS ? "[...real.UNFILTERED].filter((t) => !DEADLOCKS.has(t))" : JSON.stringify(SEED_SOME)) + ";\n" +
        "export const FILTERED = new Set([...real.FILTERED, ...seed]);\n" +
        "export const UNFILTERED = new Set([...real.UNFILTERED].filter((t) => !FILTERED.has(t)));\n" +
        "export const IDENTITY = real.IDENTITY;\n" +
        "export const SERVER_WIDE = real.SERVER_WIDE;\n" +
        "export function readScope(tool) {\n" +
        "  return FILTERED.has(tool) ? 'filtered' : UNFILTERED.has(tool) ? 'unfiltered' : IDENTITY.has(tool) ? 'identity' : SERVER_WIDE.has(tool) ? 'server' : null;\n" +
        "}\n"
    );
  }
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = {
    util: await load("util.js"),
    scopes: await load("database-filter-reads.js"),
    tabs: scenario.startsWith("census") ? await load(path.join("pages", "server-tabs.js")) : null,
  };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const { util, scopes } = modules;
const data = (body) => ({ status: 200, body });
const settle = async () => {
  for (let i = 0; i < 200; i++) await new Promise((r) => setImmediate(r));
};
/* The request a read sent, as the page built it (path and query string, still encoded). */
const sent = async (tool, params) => {
  const before = fetches.length;
  await util.readTool(tool, params);
  return fetches.slice(before);
};
const keys = (url, name) => new URL(url, "http://viewer.test").searchParams.getAll(name);

/* The six names a database can have that a careless list would mangle: a comma, a closing bracket, a leading space, a
   quote, a percent and a plus, and markup. Each must reach the wire as ONE key holding exactly that name. */
const AWKWARD = ["A,B", "x]", " SalesDb", "O'Brien", "50%+off", "<img src=x onerror=alert(1)>"];

const out = {};
const scenarios = {
  // buildQuery: an array is repeated keys, one encodeURIComponent per name, and nothing for an empty array.
  query: () => {
    const six = util.buildQuery({ database_name: AWKWARD });
    out.empty = util.buildQuery({ server: "SRV1", database_name: [] });
    out.comma = util.buildQuery({ database_name: ["A,B"] });
    out.pair = util.buildQuery({ server: "SRV1", database_name: ["SalesDb", "Orders"], hours: 4 });
    out.six = six;
    out.sixParts = six.slice(1).split("&");
    out.sixDecoded = out.sixParts.map((part) => decodeURIComponent(part.slice("database_name=".length)));
    out.sixViaUrl = keys("/x" + six, "database_name");
    out.scalar = util.buildQuery({ a: "x y", b: null, c: "", d: 0, e: undefined, f: false });
    out.none = util.buildQuery(null);
  },

  // readTool injection: only the active server's FILTERED reads, only on a server page, and an explicit name wins.
  injection: async () => {
    const names = ["SalesDb", "Orders"];
    const active = (hash = "#/server/SRV1") => {
      globalThis.location.hash = hash;
      util.setActiveDatabaseFilter({ server: "SRV1", databases: names });
    };
    active();
    out.filtered = await sent("get_top_queries_by_cpu", { server: "SRV1", hours: 4 });
    out.unsetName = await sent("get_top_queries_by_cpu", { server: "SRV1", hours: 4, database_name: undefined });
    out.emptyName = await sent("get_top_queries_by_cpu", { server: "SRV1", hours: 4, database_name: "" });
    out.explicit = await sent("get_top_queries_by_cpu", { server: "SRV1", hours: 4, database_name: "Other" });
    out.explicitArray = await sent("get_top_queries_by_cpu", { server: "SRV1", hours: 4, database_name: ["X", "Y"] });
    out.otherServer = await sent("get_top_queries_by_cpu", { server: "SRV2", hours: 4 });
    out.noServer = await sent("get_top_queries_by_cpu", { hours: 4 });
    out.unfilteredRead = await sent("get_blocking", { server: "SRV1", hours: 4 });
    out.serverWideRead = await sent("get_cpu_utilization", { server: "SRV1", hours: 4 });
    out.identityRead = await sent("get_query_trend", { server: "SRV1", query_hash: "0x1", database_name: "Sales" });
    out.identityNoName = await sent("get_query_trend", { server: "SRV1", query_hash: "0x1" });
    out.planRead = await sent("get_plan_xml", { server: "SRV1", query_hash: "0x1", database_name: "Sales" });
    out.pgRead = await sent("get_pg_top_queries", { server: "SRV1", hours: 4 });
    const own = { server: "SRV1", hours: 4 };
    await sent("get_top_queries_by_cpu", own);
    out.callerParamsAfter = Object.keys(own);
    out.filterNames = names;

    // The page's custom range anchors the same read; both ride the one request.
    util.setActiveRange({ server: "SRV1", hours: 4, startMs: Date.parse("2026-01-02T07:15:00.000Z"), endMs: Date.parse("2026-01-02T10:30:00.000Z"), asOf: "2026-01-02T10:30:00.000Z" });
    out.withRange = await sent("get_top_queries_by_cpu", { server: "SRV1", hours: 4 });
    util.setActiveRange(null);

    // A read narrowed to the hours the store keeps is asked again; the retry carries the keys too.
    let refused = false;
    answer = (url) => {
      if (url.pathname.endsWith("get_top_queries_by_cpu") && url.searchParams.get("hours") === "720" && !refused) {
        refused = true;
        return { status: 400, body: { status: "invalid", message: "hours_back value '720' exceeds maximum of 168 hours (7 days). Use a smaller value." } };
      }
      return data({});
    };
    const before = fetches.length;
    await util.readToolWithinKeptHistory("get_top_queries_by_cpu", { server: "SRV1", hours: 720 });
    out.keptHistory = fetches.slice(before);
    answer = () => data({});

    // Off the server page, with no filter, with an emptied filter, and with a filter for another server: nothing is added.
    active("#/fleet");
    out.offPage = await sent("get_top_queries_by_cpu", { server: "SRV1", hours: 4 });
    active();
    util.setActiveDatabaseFilter(null);
    out.cleared = await sent("get_top_queries_by_cpu", { server: "SRV1", hours: 4 });
    util.setActiveDatabaseFilter({ server: "SRV1", databases: [] });
    out.emptied = await sent("get_top_queries_by_cpu", { server: "SRV1", hours: 4 });
    util.setActiveDatabaseFilter({ databases: names });
    out.noFilterServer = await sent("get_top_queries_by_cpu", { hours: 4 });
    util.setActiveDatabaseFilter({ server: "SRV2", databases: names });
    out.filterForOtherServer = await sent("get_top_queries_by_cpu", { server: "SRV1", hours: 4 });
  },

  // dbScopeChip: filtered, unfiltered, server-wide and process-rows, and none for an identity read.
  chips: () => {
    const describe = (chip) =>
      chip === null
        ? null
        : {
            state: chip.dataset.dbScope,
            text: chip.textContent,
            title: chip.getAttribute("title"),
            className: chip.className,
            childElements: chip.children.length,
          };
    const withNames = (names, hash = "#/server/SRV1") => {
      globalThis.location.hash = hash;
      util.setActiveDatabaseFilter({ server: "SRV1", databases: names });
    };
    withNames(["SalesDb"]);
    out.one = {
      filtered: describe(util.dbScopeChip("get_top_queries_by_cpu")),
      unfiltered: describe(util.dbScopeChip("get_blocking")),
      deadlock: describe(util.dbScopeChip("get_deadlocks")),
      server: describe(util.dbScopeChip("get_cpu_utilization")),
      identity: describe(util.dbScopeChip("get_query_trend")),
      plan: describe(util.dbScopeChip("get_plan_xml")),
      unknown: describe(util.dbScopeChip("get_pg_top_queries")),
      noRead: describe(util.dbScopeChip(undefined)),
      overrideServer: describe(util.dbScopeChip("get_current_waits_trend", "server")),
      overrideUnfiltered: describe(util.dbScopeChip("get_blocking_stats", "unfiltered")),
      overrideDeadlock: describe(util.dbScopeChip("get_deadlock_detail", "unfiltered")),
      processRows: describe(util.dbScopeChip("get_deadlock_detail", "process-rows")),
      unknownOverride: describe(util.dbScopeChip("get_blocking", "nonsense")),
    };
    withNames(["SalesDb", "Orders", AWKWARD[5]]);
    out.three = {
      filtered: describe(util.dbScopeChip("get_top_queries_by_cpu")),
      processRows: describe(util.dbScopeChip("get_deadlock_detail", "process-rows")),
    };
    withNames([AWKWARD[5]]);
    out.markup = describe(util.dbScopeChip("get_top_queries_by_cpu"));
    withNames(AWKWARD);
    out.awkward = describe(util.dbScopeChip("get_top_queries_by_cpu"));
    util.setActiveDatabaseFilter(null);
    out.none = {
      filtered: describe(util.dbScopeChip("get_top_queries_by_cpu")),
      unfiltered: describe(util.dbScopeChip("get_blocking")),
      server: describe(util.dbScopeChip("get_cpu_utilization", "server")),
      processRows: describe(util.dbScopeChip("get_deadlock_detail", "process-rows")),
    };
    util.setActiveDatabaseFilter({ server: "SRV1", databases: [] });
    out.emptied = describe(util.dbScopeChip("get_blocking"));
    withNames(["SalesDb"], "#/fleet");
    out.offPage = describe(util.dbScopeChip("get_blocking"));
    withNames(["SalesDb"], "#/views");
    out.viewsPage = describe(util.dbScopeChip("get_cpu_utilization"));
    withNames(["SalesDb"]);
    out.state = {
      filtered: util.dbScopeState("get_top_queries_by_cpu"),
      unfiltered: util.dbScopeState("get_blocking"),
      server: util.dbScopeState("get_cpu_utilization"),
      identity: util.dbScopeState("get_query_trend"),
      processRows: util.dbScopeState("get_deadlock_detail", "process-rows"),
    };
  },
};

/* The runtime census: every SQL Server tab is built against a stub fetch with a filter active, and each read the tabs made
   is checked against the four classes. */
const CENSUS_NAMES = ["Filter,A", "Filter]B", " Filter C"];
const census = async () => {
  globalThis.location.hash = "#/server/SRV1";
  util.setActiveDatabaseFilter({ server: "SRV1", databases: CENSUS_NAMES });
  const classes = [scopes.FILTERED, scopes.UNFILTERED, scopes.IDENTITY, scopes.SERVER_WIDE];
  const requests = [];
  const holders = [];
  for (const tab of modules.tabs.SERVER_TABS) {
    const before = fetches.length;
    const holder = new FakeNode("div");
    util.mount(holder, tab.build("SRV1", { hours: 24, label: "last 24 hours" }));
    holders.push(holder);
    await settle();
    for (const url of fetches.slice(before)) requests.push({ tab: tab.id, url });
  }
  out.tabs = modules.tabs.SERVER_TABS.length;
  out.requests = requests.length;
  const reads = requests.filter((r) => r.url.startsWith("/api/read/"));
  const tool = (r) => new URL(r.url, "http://viewer.test").pathname.replace("/api/read/", "");
  out.tools = [...new Set(reads.map(tool))].sort();
  out.unclassified = out.tools.filter((t) => classes.every((c) => !c.has(t)));
  out.inSeveralClasses = out.tools.filter((t) => classes.filter((c) => c.has(t)).length > 1);
  const injected = (r) => CENSUS_NAMES.some((n) => keys(r.url, "database_name").includes(n));
  const whole = (r) => JSON.stringify(keys(r.url, "database_name")) === JSON.stringify(CENSUS_NAMES);
  /* A FILTERED request carries the keys: every chosen name, in order, unless its panel named a database of its own. */
  const filtered = reads.filter((r) => scopes.FILTERED.has(tool(r)));
  out.filteredReads = [...new Set(filtered.map(tool))].sort();
  out.filteredRequests = filtered.length;
  out.filteredWithKeys = filtered.filter(whole).length;
  out.filteredMissingKeys = filtered.filter((r) => !whole(r) && keys(r.url, "database_name").length === 0).map((r) => r.tab + ": " + r.url);
  out.filteredPartialKeys = filtered.filter((r) => !whole(r) && injected(r)).map((r) => r.tab + ": " + r.url);
  out.filteredExplicit = filtered.filter((r) => !whole(r) && !injected(r) && keys(r.url, "database_name").length > 0).map((r) => r.tab + ": " + r.url);
  /* No other request carries them, an identity read or a read of a page endpoint included. */
  out.strayKeys = requests.filter((r) => !(r.url.startsWith("/api/read/") && scopes.FILTERED.has(tool(r))) && injected(r)).map((r) => r.tab + ": " + r.url);
  out.scopes = Object.fromEntries(["FILTERED", "UNFILTERED", "IDENTITY", "SERVER_WIDE"].map((n) => [n, scopes[n].size]));
  return holders;
};
scenarios.census = census;
scenarios.censusSeeded = census;

const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);
await chosen();
await settle();

console.log(JSON.stringify({ ...out, fetches: scenario.startsWith("census") ? undefined : fetches, rejections }));
