/* Runs the shipped read layer (wwwroot/js/util.js) under Node against a recording fetch whose answers land only when the harness
   says so, and prints as one line of JSON which requests a page's reads send and when (#8452): a Queries tab that issues its four
   Query Store reads together with the page's own reads and the tab's small reads, a slot freed by a finished heavy read, a heavy
   read whose caller leaves while it waits, and two callers joining one heavy read. No clock is read: every step runs to the end
   of the microtask queue and then looks at what was sent. WebHeavyReadGateBehaviourTests starts it as
       node web-heavy-read-gate-harness.mjs <path to wwwroot/js> */
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

/* Every request sent, as the tool name, in the order fetch was called; `pending` holds each request's resolver until the harness
   lands its answer. */
let sent = [];
const pending = new Map();
const toolOf = (url) => {
  const u = String(url);
  const m = /\/api\/read\/([^?]+)/.exec(u);
  return m ? m[1] : u;
};
globalThis.fetch = (url, init) =>
  new Promise((resolve, reject) => {
    const tool = toolOf(url);
    sent.push(tool);
    const answer = () =>
      resolve({
        ok: true,
        status: 200,
        url: String(url),
        headers: { get: () => null },
        text: async () => JSON.stringify({ status: "ok", server: "s" }),
      });
    pending.set(tool, answer);
    if (init && init.signal) init.signal.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })), { once: true });
  });

/* Lets every promise that can settle without an answer settle: the gate hands out slots and the sends happen on microtasks, so
   draining the microtask queue a few times is the whole wait. */
const settle = async () => {
  for (let i = 0; i < 8; i++) await new Promise((r) => setImmediate(r));
};
const land = async (tool) => {
  const answer = pending.get(tool);
  if (!answer) throw new Error("nothing pending for " + tool);
  pending.delete(tool);
  answer();
  await settle();
};

const util = await import(pathToFileURL(path.join(jsDir, "util.js")).href);
const { readTool } = util;

const HEAVY = ["get_query_store_clutter", "get_query_store_regressions", "get_query_heatmap", "get_plan_corrections"];
const PAGE = ["get_store_host", "get_alert_history"];
const SMALL = ["get_query_store_top", "get_top_queries_by_cpu", "get_active_queries", "get_long_query_completions"];
const params = { server: "s", hours: 24 };
const out = {};

/* 1. The tab's load: heavy, page and small reads issued in one tick, heavy ones first as the tab mounts them. */
const firstLoad = [...HEAVY, ...PAGE, ...SMALL].map((tool) => readTool(tool, params));
await settle();
out.load = { sent: [...sent], heavySent: sent.filter((t) => HEAVY.includes(t)), otherSent: sent.filter((t) => !HEAVY.includes(t)) };

/* 2. A finished heavy read frees its slot for the next one waiting, in the order asked. */
await land(HEAVY[0]);
out.afterFirstLands = { sent: [...sent] };
await land(HEAVY[1]);
out.afterSecondLands = { sent: [...sent] };
for (const tool of [...PAGE, ...SMALL, HEAVY[2], HEAVY[3]]) await land(tool);
const firstResults = await Promise.all(firstLoad);
out.load.kinds = firstResults.map((r) => r.kind);

/* 3. A heavy read whose caller leaves while it waits is never sent, and the next waiting read takes the slot instead. */
sent = [];
const leaving = new AbortController();
const second = [
  readTool(HEAVY[0], params),
  readTool(HEAVY[1], params),
  readTool(HEAVY[2], params, leaving.signal),
  readTool(HEAVY[3], params),
];
await settle();
leaving.abort();
await settle();
const leftKind = (await second[2]).kind;
await land(HEAVY[0]);
out.leaver = { sentBeforeAnyLands: [...sent].slice(0, 2), sentAfterFirstLands: [...sent], leftKind };
await land(HEAVY[1]);
await land(HEAVY[3]);
await Promise.all([second[0], second[1], second[3]]);

/* 4. Two callers of one heavy read share its request and its slot. */
sent = [];
const third = [readTool(HEAVY[0], params), readTool(HEAVY[0], params), readTool(HEAVY[1], params), readTool(HEAVY[2], params)];
await settle();
out.joined = { sent: [...sent] };
await land(HEAVY[0]);
await land(HEAVY[1]);
await land(HEAVY[2]);
await Promise.all(third);

console.log(JSON.stringify(out));
