/* Runs the web shell's real status-bar code (the region of wwwroot/js/app.js between "const STORE_SIZE_TTL_MS"
   and the refresh-loop banner) on a scenario and prints what the bar drew as one line of JSON.
   StatusBarBehaviourTests starts it as: node status-bar-harness.mjs <path to app.js> <scenario>
   app.js is the page's entry module (imports + top-level DOM wiring), so only the status-bar region is evaluated,
   against stand-ins for the DOM helpers, the session probe, the store read and fetch. */
import fs from "node:fs";
import vm from "node:vm";

const [appPath, scenario] = process.argv.slice(-2);
const full = fs.readFileSync(appPath, "utf8").replace(/\r\n/g, "\n");
const start = full.indexOf("const STORE_SIZE_TTL_MS");
const end = full.indexOf("/* ─────────────────────────── refresh loop");
if (start < 0 || end < start) throw new Error("app.js layout changed: status-bar region not found");
const source = full.slice(start, end);

const flat = (x) => (Array.isArray(x) ? x.flatMap(flat) : x == null ? [] : [x]);
const el = (tag, attrs, kids) => ({ tag, cls: (attrs && attrs.class) || "", text: attrs && attrs.text, kids: flat(kids) });
const sc = {
  session: { can_edit: true },
  ping: { status: "ok" },
  storeHost: { kind: "data", data: { store: { size_bytes: 5 * 1024 ** 3 } } },
};
const counts = { storeReads: 0 };
let bar = null;
const statusbar = {};
const context = vm.createContext({
  console,
  Date: { now: () => clock.t },
  Set, Number, isFinite, Promise, JSON,
  el,
  mount: (_p, nodes) => { bar = flat(nodes); },
  localTime: String,
  updateRefreshHint: () => {},
  statusbar,
  getSession: () => (sc.session instanceof Error ? Promise.reject(sc.session) : Promise.resolve(sc.session)),
  readTool: async (tool) => {
    counts.storeReads++;
    if (tool !== "get_store_host") throw new Error("unexpected read " + tool);
    if (sc.storeHost instanceof Error) throw sc.storeHost;
    return sc.storeHost;
  },
  fetch: async (path) => {
    if (path !== "/api/ping") throw new Error("unexpected fetch " + path);
    if (sc.ping instanceof Error) throw sc.ping;
    return { json: async () => sc.ping };
  },
});
const clock = { t: 1_000_000 };
vm.runInContext(source + "\nthis.updateStatusBar = updateStatusBar;", context);

const fleet = { cards: [{ healthy_collector_count: 3, failed_collector_count: 1 }], total_servers: 1, generated_at: "T" };
const settle = () => new Promise((r) => setTimeout(r, 20));
const texts = () => (bar || []).filter((n) => n.cls.includes("sb-item")).map((n) => n.text);
const poll = async () => { context.updateStatusBar(fleet); await settle(); return texts(); };

const run = {
  async seatWrite() { sc.session = { can_edit: true }; return { items: await poll() }; },
  async seatRead() { sc.session = { can_edit: false }; return { items: await poll() }; },
  async seatProbeFailed() { sc.session = { can_edit: false, probe_failed: true }; return { items: await poll() }; },
  async storeSize() { return { items: await poll() }; },
  async storeSizeMb() { sc.storeHost = { kind: "data", data: { store: { size_bytes: 300 * 1024 ** 2 } } }; return { items: await poll() }; },
  async cached() {
    await poll(); clock.t += 4 * 60 * 1000; const items = await poll();
    return { items, reads: counts.storeReads };
  },
  async refetched() {
    await poll(); clock.t += 6 * 60 * 1000; sc.storeHost = { kind: "data", data: { store: { size_bytes: 6 * 1024 ** 3 } } };
    const items = await poll();
    return { items, reads: counts.storeReads };
  },
};
const pings = ["starting", "degraded", "stopped", "ok"];
for (const p of pings) {
  run["ping_" + p] = async () => { sc.ping = { status: p }; return { items: await poll() }; };
}
run.storeFails = async () => {
  sc.storeHost = new Error("down");
  const items = await poll();
  return { items, hasBar: Array.isArray(bar) && bar.length > 0 };
};
run.storeFailsAfterGood = async () => {
  await poll(); clock.t += 6 * 60 * 1000; sc.storeHost = { kind: "error", message: "x" };
  return { items: await poll() };
};
run.pingFails = async () => { sc.ping = new Error("net"); return { items: await poll() }; };
run.sessionFails = async () => { sc.session = new Error("net"); return { items: await poll() }; };

if (!run[scenario]) throw new Error("unknown scenario " + scenario);
console.log(JSON.stringify(await run[scenario]()));
