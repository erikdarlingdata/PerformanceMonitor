/* Runs the shipped read layer (wwwroot/js/util.js) under Node against a stand-in browser (#8632): six connections to one origin
   over HTTP/1.1, requests sent to a free connection in the order fetch was called, each answered after a fixed time of its own, on a
   virtual clock (no real time passes). It plays a server's first Queries tab visit: the five reads that took 3 to 6 s each in the
   field (the three duration trends and the two top lists at detail=full), the four Query Store reads, the tab's small reads, and the
   page's own reads (ping, store host, alert badge), and prints as one line of JSON how long each page read waited for a connection
   and when the tab's last read landed. WebQueriesPageReadsBehaviourTests starts it as
       node web-queries-page-reads-harness.mjs <path to wwwroot/js> */
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];

globalThis.window = globalThis;
globalThis.location = { hash: "", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
const stored = {};
const storage = {
  getItem: (k) => (Object.prototype.hasOwnProperty.call(stored, k) ? stored[k] : null),
  setItem: (k, v) => { stored[k] = String(v); },
  removeItem: (k) => { delete stored[k]; },
};
globalThis.localStorage = storage;
globalThis.sessionStorage = storage;
globalThis.document = { createElement: () => ({ style: {}, dataset: {}, classList: { add() {}, remove() {} }, appendChild() {} }), createTextNode: (t) => ({ text: t }), querySelectorAll: () => [] };

const CONNECTIONS = 6;
/* How long each read takes once it has a connection, in ms: the field numbers of the first visit (#8632) for the five long reads
   and the page's own reads; the four Query Store reads and the small reads are a round figure each. */
const FIVE = {
  get_top_queries_by_cpu: 5825,
  get_query_duration_trend: 5284,
  get_top_procedures_by_cpu: 5261,
  get_procedure_duration_trend: 4446,
  get_query_store_duration_trend: 4409,
};
const QS = { get_query_store_clutter: 2329, get_query_store_regressions: 3154, get_query_heatmap: 3000, get_plan_corrections: 3000 };
const SMALL = ["get_active_queries", "get_query_store_top", "get_long_query_completions", "get_query_store_aborts", "get_query_store_state",
  "get_query_store_health", "get_automatic_tuning", "get_plan_cache_summary", "get_query_pileups", "get_query_fleet_summary"];
const SMALL_MS = 100;
const PAGE = { "/api/ping": 30, get_store_host: 276, get_alert_badge_summary: 46 };

let now = 0;
const waiting = [];
const active = [];
const log = [];
const durationOf = (tool) => (tool in FIVE ? FIVE[tool] : tool in QS ? QS[tool] : tool in PAGE ? PAGE[tool] : SMALL_MS);
const toolOf = (url) => {
  const m = /\/api\/read\/([^?]+)/.exec(String(url));
  return m ? m[1] : String(url).split("?")[0];
};
const dispatch = () => {
  while (active.length < CONNECTIONS && waiting.length > 0) {
    const r = waiting.shift();
    r.started = now;
    r.ends = now + durationOf(r.tool);
    active.push(r);
  }
};
globalThis.fetch = (url, init) =>
  new Promise((resolve, reject) => {
    const r = { tool: toolOf(url), asked: now, resolve, started: -1, ends: -1 };
    log.push(r);
    waiting.push(r);
    dispatch();
    if (init && init.signal) init.signal.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })), { once: true });
  });
const settle = async () => {
  for (let i = 0; i < 8; i++) await new Promise((r) => setImmediate(r));
};
/* Runs the clock until every request is answered. */
const runClock = async () => {
  await settle();
  while (active.length > 0) {
    active.sort((a, b) => a.ends - b.ends);
    const r = active.shift();
    now = r.ends;
    dispatch();
    r.resolve({
      ok: true,
      status: 200,
      url: "http://localhost/api/read/" + r.tool,
      headers: { get: () => null },
      text: async () => JSON.stringify({ status: "ok", server: "s" }),
    });
    await settle();
  }
};

const { readTool } = await import(pathToFileURL(path.join(jsDir, "util.js")).href);
const params = { server: "s", hours: 24 };

/* The visit, in the order the tab asks: the small reads and the five first, then the four Query Store reads, then (the page's reads
   come after the tab's own on a fresh load) ping, store host and the alert badge. `pageFirst` asks the page's three before the tab. */
const visit = async (pageFirst) => {
  log.length = 0;
  now = 0;
  const calls = [];
  const askPage = () => {
    calls.push(fetch("/api/ping"));
    calls.push(readTool("get_store_host", {}));
    calls.push(readTool("get_alert_badge_summary", {}));
  };
  if (pageFirst) askPage();
  calls.push(readTool(SMALL[0], params));
  for (const tool of Object.keys(FIVE)) calls.push(readTool(tool, tool.endsWith("_by_cpu") ? { ...params, top: 20, detail: "full" } : params));
  for (const tool of SMALL.slice(1)) calls.push(readTool(tool, params));
  for (const tool of Object.keys(QS)) calls.push(readTool(tool, params));
  /* The page's own reads are asked a few ms after the tab's (the field's first twenty-odd reads came within 3 ms), so every send the tab's
     reads make on their way out has happened by then. */
  if (!pageFirst) {
    await settle();
    askPage();
  }
  return calls;
};
const summarise = () => {
  const byTool = (t) => log.find((r) => r.tool === t);
  const queue = (t) => { const r = byTool(t); return r.started - r.asked; };
  const tabTools = [...Object.keys(FIVE), ...Object.keys(QS), ...SMALL];
  const tabEnd = Math.max(...log.filter((r) => tabTools.includes(r.tool)).map((r) => r.started + durationOf(r.tool)));
  /* The most reads of a group on the wire at one instant: every start is a candidate, since a count only rises when a read starts. */
  const peak = (tools) => {
    const group = log.filter((r) => tools.includes(r.tool));
    return Math.max(...group.map((r) => group.filter((o) => o.started <= r.started && o.started + durationOf(o.tool) > r.started).length));
  };
  return {
    maxHeavyInFlight: peak(Object.keys(QS)),
    maxGatedInFlight: peak([...Object.keys(FIVE), ...Object.keys(QS)]),
    pingQueuedMs: queue("/api/ping"),
    storeHostQueuedMs: queue("get_store_host"),
    badgeQueuedMs: queue("get_alert_badge_summary"),
    tabSettlesMs: tabEnd,
  };
};

const out = {};
let calls = await visit(false);
await runClock();
await Promise.all(calls);
out.pageReadsAfterTheTab = summarise();
calls = await visit(true);
await runClock();
await Promise.all(calls);
out.pageReadsFirst = summarise();
console.log(JSON.stringify(out));
