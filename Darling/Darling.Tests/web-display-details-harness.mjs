/* #7552: runs the shipped grids (panels.js VIZ.table) on a small fake DOM and prints one line of JSON: the status cell of a review row on both
   Configuration Audits, the alignment class of three number columns, and the text of id cells. WebDisplayDetailsBehaviourTests starts it as
       node web-display-details-harness.mjs <path to the js folder>
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
globalThis.document = { createElement: (t) => new FakeNode(t), createElementNS: (_ns, t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/a/config" };

const root = process.argv[2];
const { VIZ } = await import(pathToFileURL(root + "/panels.js").href);
const { AUDIT_COLUMNS, IDENTITY_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/sql-config.js").href);
const { PG_AUDIT_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/pg-config.js").href);
const { PG_INDEX_BLOAT_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/pg-storage.js").href);
const { PG_SESSION_STATE_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/pg-vacuum.js").href);
const { QUERY_STORE_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/sql-queries.js").href);

const draw = (columns, rows) => VIZ.table({ rows }, { rowsKey: "rows", title: "t", tools: false, columns });
const headers = (t) => t.findAll((n) => n.tag === "th").map((n) => n.textContent.replace(/[^A-Za-z %]/g, "").trim());
const cellOf = (columns, rows, label) => {
  const t = draw(columns, rows);
  const at = headers(t).indexOf(label);
  const tr = t.findAll((n) => n.tag === "tr").filter((r) => r.findAll((n) => n.tag === "td").length > 0)[0];
  const td = tr.findAll((n) => n.tag === "td")[at];
  return { cls: td.className, text: td.textContent };
};
const out = {};
out.sqlReview = cellOf(AUDIT_COLUMNS, [{ setting: "max server memory (MB)", status: "review", current_value: "9999999" }], "Status");
out.pgReview = cellOf(PG_AUDIT_COLUMNS, [{ setting: "work_mem", status: "review", current_value: "4" }], "Status");
out.sqlOk = cellOf(AUDIT_COLUMNS, [{ setting: "x", status: "ok", current_value: "1" }], "Status");
out.identityRemaining = cellOf(IDENTITY_COLUMNS, [{ database: "d", table: "t", column: "c", values_remaining: "1288348578" }], "Values remaining");
/* #7679 item 7: the Band word is capitalised as Fleet Overview prints it, and the one casing for status words keeps ok as OK. */
out.identityBandCritical = cellOf(IDENTITY_COLUMNS, [{ database: "d", table: "t", column: "c", band: "critical" }], "Band");
out.identityBandOk = cellOf(IDENTITY_COLUMNS, [{ database: "d", table: "t", column: "c", band: "ok" }], "Band");
out.bloatSize = cellOf(PG_INDEX_BLOAT_COLUMNS, [{ schema_name: "s", table_name: "t", index_name: "i", index_bytes: 209715, estimated_reclaimable_bytes: null }], "Size");
out.bloatReclaimable = cellOf(PG_INDEX_BLOAT_COLUMNS, [{ schema_name: "s", table_name: "t", index_name: "i", index_bytes: 209715, estimated_reclaimable_bytes: null }], "Reclaimable");
out.pid = cellOf(PG_SESSION_STATE_COLUMNS, [{ pid: 5828 }], "PID");
out.queryId = cellOf(QUERY_STORE_COLUMNS, [{ query_id: 5568, plan_id: 7712 }], "Query ID");
out.planId = cellOf(QUERY_STORE_COLUMNS, [{ query_id: 5568, plan_id: 7712 }], "Plan ID");

/* #7552 item 4: an axis tick keeps only the decimals its step needs; a tooltip (no step) keeps its two. */
{
  const { FORMATTERS } = await import(pathToFileURL(root + "/util.js").href);
  const { axisTickLabels } = await import(pathToFileURL(root + "/charts.js").href);
  const { formatComposedValue } = await import(pathToFileURL(root + "/compose.js").href);
  out.ticksRound = axisTickLabels([0, 100, 200, 300, 400], 100, FORMATTERS.num2).join(" | ");
  out.ticksSmall = axisTickLabels([0, 0.005, 0.01, 0.015], 0.005, FORMATTERS.num2).join(" | ");
  out.tooltip = FORMATTERS.num2(1234.5);
  out.composedTicks = [0, 200, 400, 800].map((v) => formatComposedValue(v, "s", { step: 200 })).join(" | ");
  out.composedTooltip = formatComposedValue(800, "s");
}

/* #7552 item 6: the Daily Summary Top wait tile may break after each underscore, and still reads as the one name. */
{
  const { DAILY_STATS } = await import(pathToFileURL(root + "/pages/server-tabs/sql-overview.js").href);
  const tile = VIZ.stat({ top_wait_type: "SOS_SCHEDULER_YIELD" }, { stats: DAILY_STATS.filter((st) => st.key === "top_wait_type") });
  out.topWait = { text: tile.textContent.replace("Top wait", ""), breaks: tile.findAll((n) => n.tag === "wbr").length };
}

/* The Memory tiles' System state tile (the server page, the FinOps utilization panel and the Memory pressure custom-view template) shows the
   server's own note where the state is null, not a bare dash (#6159, #7552). The descriptors are the shipped ones. */
{
  const { MEMORY_STATS: serverStats } = await import(pathToFileURL(root + "/pages/server-tabs/sql-memory.js").href);
  const { MEMORY_STATS: finopsStats } = await import(pathToFileURL(root + "/pages/finops/utilization.js").href);
  const { DASHBOARD_TEMPLATES } = await import(pathToFileURL(root + "/view-templates.js").href);
  const found = [];
  const seek = (n) => { if (!n || typeof n !== "object") return; if (n.key === "system_memory_state") found.push(n); (Array.isArray(n) ? n : Object.values(n)).forEach(seek); };
  DASHBOARD_TEMPLATES.forEach((t) => seek(t.make("example-sql-01")));
  const tile = (st, data) => VIZ.stat(data, { stats: [st] }).textContent.replace(st.label, "");
  const note = "n/a (Azure SQL Database)";
  const sets = { server: serverStats.filter((st) => st.key === "system_memory_state"), finops: finopsStats.filter((st) => st.key === "system_memory_state"), template: found };
  out.memoryState = {};
  for (const [name, list] of Object.entries(sets)) {
    out.memoryState[name] = { count: list.length, noted: list.map((st) => tile(st, { system_memory_state: null, system_memory_state_note: note })).join("|"), bare: list.map((st) => tile(st, { system_memory_state: null })).join("|"), valued: list.map((st) => tile(st, { system_memory_state: "Available physical memory is high", system_memory_state_note: note })).join("|") };
  }
}

/* #7679 item 5: a line panel with no format prints a whole-number axis with the thousands separator (60,000, not 60000). */
{
  const pts = [0, 1, 2].map((i) => ({ time: "2026-11-05T0" + i + ":00:00", total_duration_ms: i * 30000 }));
  const chart = VIZ.line({ rows: pts }, { rowsKey: "rows", xKey: "time", title: "Severity", series: [{ key: "total_duration_ms", label: "Total Wait (ms)" }] });
  out.axisTicks = chart.findAll((n) => n.tag === "text").map((n) => n.textContent).filter((t) => /^[\d,]+$/.test(t)).join(" | ");
}

/* #7552 item 10: an hour of memory pressure events is stamped at its middle, because the chart centres a bar on its stamp. */
{
  const { pressureEventBuckets } = await import(pathToFileURL(root + "/pages/server-tabs/sql-memory.js").href);
  out.pressureStamp = pressureEventBuckets([{ sample_time: "2026-11-05T17:48:00", memory_indicators_process: 2, memory_indicators_system: 0 }], null).points[0].time;
}

/* #7552 items 8 and 9 on the real Queries tab: Automatic Tuning names Plan Corrections as the panel that says why, and Top Queries with
   snapshots and no measured change says the short sentence. Only fetch is a stand-in. */
{
  const { SERVER_TABS } = await import(pathToFileURL(root + "/pages/server-tabs.js").href);
  const tool = "stored 8 query_stats snapshot(s) in the last 24 hours, but none carries a measured delta. Use as_of and hours_back to reach one.";
  const none = "No tuning recommendations for this server in the window.";
  globalThis.fetch = async (url) => {
    const u = String(url);
    const body = u.includes("get_top_queries_by_cpu")
      ? { status: "empty", message: tool, hints: { snapshots_in_window: 8, snapshots_with_measured_delta: 0, suggested_hours_back: 111 } }
      : u.includes("get_plan_corrections") ? { status: "empty", message: none, hints: null } : { status: "unavailable", message: "n/a" };
    return { ok: true, status: 200, headers: { get: () => null }, text: async () => JSON.stringify(body) };
  };
  const tab = SERVER_TABS.find((t) => t.id === "queries");
  const panels = tab.build("example-sql-01", { hours: 6, label: "last 6 hours", server: "example-sql-01" });
  await new Promise((resolve) => setTimeout(resolve, 150));
  const flat = [];
  const walk = (n) => { if (!n) return; if (Array.isArray(n)) return n.forEach(walk); flat.push(n); };
  walk(panels);
  const bodyOf = (match) => {
    const p = flat.flatMap((n) => n.findAll((x) => x.attrs && typeof x.attrs["data-title"] === "string" && match(x.attrs["data-title"])))[0];
    return p ? p.findAll((x) => String(x.className).includes("panel-body"))[0].textContent : null;
  };
  out.autoTuning = bodyOf((t) => t === "Automatic Tuning");
  out.topQueriesEmpty = bodyOf((t) => t.startsWith("Top Queries by"));
}
console.log(JSON.stringify(out));
