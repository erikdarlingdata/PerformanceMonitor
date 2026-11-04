/* #7485: runs the shipped stat-tile renderer (wwwroot/js/panels.js, VIZ.stat) over the SQL Server Overview's own tile lists
   (sql-overview.js DAILY_STATS, shared.js PROPERTY_STATS) on a small fake DOM, and prints what a person would see as one line of JSON.
       node web-overview-tiles-harness.mjs <path to the js folder> */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) {
    this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = "";
    this.dataset = {}; this.style = {};
    const set = new Set();
    this.classList = { add: (c) => set.add(c), remove: (c) => set.delete(c), toggle: (c, on) => (on ? set.add(c) : set.delete(c)), contains: (c) => set.has(c) };
  }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); if (k === "class") this.className = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  findAll(pred, out = []) { if (pred(this)) out.push(this); this.children.forEach((c) => c.findAll(pred, out)); return out; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/a/overview" };

const root = process.argv[2];
const { VIZ } = await import(pathToFileURL(root + "/panels.js").href);
const { DAILY_STATS } = await import(pathToFileURL(root + "/pages/server-tabs/sql-overview.js").href);
const { PROPERTY_STATS } = await import(pathToFileURL(root + "/pages/server-tabs/shared.js").href);

/* One tile as "label=value", the way a person reads it. */
const tiles = (node) => node.findAll((n) => n.className === "stat").map((t) => {
  const v = t.findAll((n) => n.className.startsWith("value"))[0];
  const l = t.findAll((n) => n.className === "label")[0];
  return l.textContent + "=" + v.textContent;
});

const day = { summary_date: "2026-11-01", health_band: "Healthy", top_wait_type: "SOS_SCHEDULER_YIELD", total_wait_time_sec: 1082, unique_queries: 5, blocking_events: 0, deadlock_count: 0, alert_count: 0, collection_errors: 0 };
const props = (edition) => ({ engine_edition: edition, product_version: "1", edition: "e", product_level: "RTM", cpu_count: 2, socket_count: 1, cores_per_socket: 1, hyperthread_ratio: 1, physical_memory_mb: 1024, is_clustered: false, is_hadr_enabled: false, service_objective: edition === 5 ? "GP_S_Gen5_1" : null, vcore_count: 1 });

console.log(JSON.stringify({
  /* Built here, because the tile formats in this machine's locale. */
  expectedTotal: (1082).toLocaleString(undefined, { maximumFractionDigits: 0 }),
  daily: tiles(VIZ.stat(day, { stats: DAILY_STATS })),
  onPrem: tiles(VIZ.stat(props(3), { stats: PROPERTY_STATS })),
  azureDb: tiles(VIZ.stat(props(5), { stats: PROPERTY_STATS })),
}));
