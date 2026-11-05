/* #7557: runs the shipped Configuration Audit grids (wwwroot/js/pages/server-tabs/pg-config.js and sql-config.js through panels.js VIZ.table) on a small fake DOM
   and prints one line of JSON: the Why text each grid drew for the PostgreSQL sentences as PgTargetAdvice words them (tool names and all) and for the real SQL Server MAXDOP sentence.
   WebConfigAuditWhyBehaviourTests starts it as
       node web-config-audit-why-harness.mjs <path to the js folder>
   Only the DOM is a stand-in. */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.listeners = {};
    this.parent = null;
    this._text = "";
    this._cn = "";
    this.dataset = {};
    this.style = {};
    const set = new Set();
    this.classes = set;
    this.classList = { add: (c) => set.add(c), remove: (c) => set.delete(c), toggle: (c, on) => (on ? set.add(c) : set.delete(c)), contains: (c) => set.has(c) };
  }
  get className() { return this._cn; }
  set className(v) { this._cn = String(v); String(v).split(/\s+/).filter(Boolean).forEach((c) => this.classes.add(c)); }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); if (k === "class") this.className = v; }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  findAll(pred, out = []) { if (pred(this)) out.push(this); this.children.forEach((c) => c.findAll(pred, out)); return out; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/a/config" };

const root = process.argv[2];
const { VIZ } = await import(pathToFileURL(root + "/panels.js").href);
const { PG_AUDIT_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/pg-config.js").href);
const { AUDIT_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/sql-config.js").href);

const draw = (columns, rows) => VIZ.table({ rows }, { rowsKey: "rows", title: "t", tools: false, columns });
const bodyRows = (t) => t.findAll((n) => n.tag === "tr").filter((tr) => tr.findAll((n) => n.tag === "td").length > 0);
const whyOf = (columns, rows) => {
  const t = draw(columns, rows);
  const at = t.findAll((n) => n.tag === "th").map((n) => n.textContent.replace(/[^A-Za-z ]/g, "").trim()).indexOf("Why");
  return bodyRows(t).map((tr) => tr.findAll((n) => n.tag === "td")[at].textContent);
};

/* The PostgreSQL sentences that name a read, as the service words them (PgTargetAdvice), and two that start with the name. */
const pgWhy = [
  "max_wal_size is 1 GB. No measured WAL rate accompanies this card, so there is no number to size against yet: read get_pg_write_stats over a busy window for wal_bytes and the requested-vs-timed split, then raise max_wal_size.",
  "Set effective_cache_size to roughly 50-75 % of the host's RAM. Change it, then watch plan shapes on the top statements (get_pg_top_queries) rather than assuming the move was free.",
  "work_mem is 4 MB. Counter-objective: set too high, memory multiplies per sort. get_pg_top_queries has the statements writing the most temp blocks.",
  "Find out which statements are scanning in get_pg_predicate_stats, then follow their time in get_pg_query_duration_trend.",
  "get_pg_write_stats over a busy window carries the WAL counters.",
  "Read the captured graphs (get_pg_deadlocks, then get_pg_deadlock_detail for one) for the lock modes and the statements on each side.",
  "Check the log settings with get_pg_logging_audit before changing anything.",
];
const out = {};
out.pg = whyOf(PG_AUDIT_COLUMNS, pgWhy.map((recommendation, i) => ({ setting: "s" + i, status: "Warning", current_value: "1", recommendation })));
/* SQL Server: the real MAXDOP-0 message, as ConfigAuditGuidance.MaxdopUnlimitedMessage writes it. No SQL Server audit_config message names a get_ tool today,
   so plain: true changes nothing about it. */
out.sql = whyOf(AUDIT_COLUMNS, [{ setting: "max degree of parallelism", status: "Warning", current_value: "0", suggested_value: "8",
  recommendation: "MAXDOP is 0 (unlimited). This lets one query fan out across all schedulers, leading to CXPACKET waits and thread exhaustion under load. Microsoft's guidance is topology-based: keep MAXDOP at or under the logical processors in a single NUMA node, capped at 8. Start with 8 (8 logical processors in one NUMA node, capped at 8) and adjust to the workload." }]);
/* SYNTHETIC: no SQL Server message writes this today. It guards the case where one ever names a read, so the SQL grid keeps the same plain rule as the PostgreSQL grid. */
out.sqlSynthetic = whyOf(AUDIT_COLUMNS, [{ setting: "max degree of parallelism", status: "Warning", current_value: "0", suggested_value: "8",
  recommendation: "MAXDOP is 0. Read get_wait_stats for CXPACKET and get_top_queries_by_cpu for the statements that fan out, then set it." }]);
console.log(JSON.stringify(out));
