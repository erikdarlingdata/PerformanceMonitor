/* Runs the shipped web page script for the tiles, grid columns and legends of the Configuration, Activity and Memory tabs that said
   something other than what they hold (#7416) and prints one line of JSON:
     - the Overview tiles: Protection (what it counts) and Identity capacity (the fullest column, and what a click does)
     - the Service objective tile on a boxed server and on an Azure SQL Database
     - Health Checks: its sentence is not a stat tile
     - Alert Setup Gaps: its column headers with and without a named alert
     - Memory Pressure Events: the words in its cells, and its width
     - the Memory Grant Sizing legend: the pool names
     - Perfmon Counters: whole numbers, and the Object column when a counter and instance repeat
       node web-tile-truths-harness.mjs <path to wwwroot/js> */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.text = text == null ? null : String(text);
    this.listeners = {};
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() { return this.children[0] || null; }
  get parentNode() { return null; }
  appendChild(child) { this.children.push(child); return child; }
  removeChild(child) { const i = this.children.indexOf(child); if (i >= 0) this.children.splice(i, 1); return child; }
  setAttribute(name, value) { this.attrs[name] = String(value); }
  getAttribute(name) { return name in this.attrs ? this.attrs[name] : null; }
  addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); }
  set textContent(value) { this.children = []; this.text = String(value); }
  get textContent() { return (this.text || "") + this.children.map((c) => c.textContent).join(""); }
}

const scrolled = [];
globalThis.Node = FakeNode;
globalThis.location = { hash: "", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
globalThis.document = {
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
  querySelector: (sel) => ({ scrollIntoView: () => scrolled.push(sel) }),
};

const EVENTS = [
  { sample_time: "2026-11-01T10:00:00", memory_notification: "RESOURCE_MEM_STEADY", memory_indicators_process: 0, memory_indicators_system: 1 },
  { sample_time: "2026-11-01T10:05:00", memory_notification: "RESOURCE_MEMPHYSICAL_LOW", memory_indicators_process: 2, memory_indicators_system: 3 },
];
const COUNTERS = [
  { counter_name: "Active requests", instance_name: "", object_name: "SQLServer:Workload Group Stats", value: 1, delta_value: 0, counter_kind: "gauge", cntr_type: 65792 },
  { counter_name: "Active Temp Tables", instance_name: "", object_name: "SQLServer:General Statistics", value: 16, delta_value: 86, counter_kind: "gauge", cntr_type: 65792 },
  { counter_name: "Average Latch Wait Time (ms)", instance_name: "", object_name: "SQLServer:Latches", value: 12.5, delta_value: 0.25, counter_kind: "average", cntr_type: 1073874176 },
  { counter_name: "Transaction Delay", instance_name: "_Total", object_name: "SQLServer:Database Replica", value: 3, delta_value: 1, counter_kind: "gauge", cntr_type: 65792 },
  { counter_name: "Transaction Delay", instance_name: "_Total", object_name: "SQLServer:Database Mirroring", value: 4, delta_value: 2, counter_kind: "gauge", cntr_type: 65792 },
];
let counters = COUNTERS;
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const body = u.pathname.endsWith("/get_memory_pressure_events")
    ? { events: EVENTS }
    : u.pathname.endsWith("/get_perfmon_stats")
    ? { server: "srv", captured_at: "2026-11-01T10:00:00", counters }
    : u.pathname === "/api/session"
    ? { can_edit: false }
    : {};
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "tile-truths-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport const CATEGORICAL_COLORS = ["#111", "#222", "#333", "#444", "#555", "#666"];\nexport function normalizeColor(c) { return c; }\n' +
      'export function renderLineChart() { return el("div", {}); }\nexport function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const imp = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  const fleet = await imp("pages/fleet.js");
  const panels = await imp("panels.js");
  const shared = await imp("pages/server-tabs/shared.js");
  const config = await imp("pages/server-tabs/sql-config.js");
  const memory = await imp("pages/server-tabs/sql-memory.js");
  const activity = await imp("pages/server-tabs/sql-activity.js");
  const grant = await imp("pages/memory-grant-charts.js");

  const walk = (n, f) => { f(n); n.children.forEach((c) => walk(c, f)); };
  const findAll = (n, pred) => { const out = []; walk(n, (x) => { if (pred(x)) out.push(x); }); return out; };
  const hasClass = (n, c) => String(n.className || "").split(/\s+/).includes(c);
  const wait = (ms) => new Promise((r) => setTimeout(r, ms));
  const cellsOf = (root) => findAll(root, (n) => n.tag === "td").map((n) => n.textContent);
  const headersOf = (root) => findAll(root, (n) => n.tag === "th").map((n) => n.textContent);

  /* The tiles. */
  const protectionOverdue = [];
  for (const [db, kinds] of [["Sales", ["full_backup", "log_backup", "checkdb"]], ["Hr", ["full_backup", "checkdb"]], ["Ops", ["checkdb"]]]) {
    for (const kind of kinds) protectionOverdue.push({ kind, database: db, state: "overdue", age_seconds: 1, text: db + " " + kind });
  }
  const card = {
    server_name: "srv-a", display_name: "srv-a", is_online: true, database_protection_overdue: protectionOverdue,
    identity_capacity: { worst_band: "critical", columns_at_bar: 3, worst_database: "Sales", worst_table: "dbo.Orders", worst_column: "OrderId", worst_percent_used: 100, worst_values_remaining: "0" },
  };
  const bands = fleet.metricBands(card, null);
  const chips = findAll(bands, (n) => hasClass(n, "metric-chip"));
  const chipText = (label) => { const c = chips.find((x) => x.textContent.startsWith(label)); return c ? c.textContent : null; };
  const identityChip = chips.find((x) => x.textContent.startsWith("Identity capacity"));
  let stopped = false;
  (identityChip.listeners.click || []).forEach((fn) => fn({ stopPropagation: () => { stopped = true; } }));
  await wait(400);
  const legacy = fleet.metricBands({ server_name: "srv-b", display_name: "srv-b", is_online: true, identity_capacity: { worst_band: "warning", columns_at_bar: 1 } }, null);
  const legacyChip = findAll(legacy, (n) => hasClass(n, "metric-chip")).find((x) => x.textContent.startsWith("Identity capacity"));

  /* Server properties. */
  const labelsFor = (data) => findAll(panels.VIZ.stat(data, { stats: shared.PROPERTY_STATS }), (n) => hasClass(n, "label")).map((n) => n.textContent);
  const boxed = labelsFor({ product_version: "16.0", engine_edition: 3, cpu_count: 8, service_objective: null });
  const azure = labelsFor({ product_version: "12.0", engine_edition: 5, cpu_count: 2, service_objective: "GP_Gen5_2", vcore_count: 2 });

  /* Health Checks and Alert Setup Gaps. */
  const health = panels.VIZ.stat(
    { summary: { text: "11 of 13 judged databases overdue. full backup: a, b. CHECKDB: c." }, captured_at: "2026-11-01T10:00:00" },
    config.DATABASE_PROTECTION_SPECS[0]
  );
  const gapSpec = config.AGENT_ALERT_SPECS.find((s) => s.title === "Alert Setup Gaps");
  const gapsMissing = panels.VIZ.table({ gaps: [
    { code: "no_alerts_defined", message: "No SQL Server Agent alert is defined on this instance.", alerts: [], missing: [] },
    { code: "missing_severity_alerts", message: "No alert for severity 21, 22.", alerts: [], missing: [21, 22] },
  ] }, gapSpec);
  const gapsNamed = panels.VIZ.table({ gaps: [
    { code: "no_alerts_defined", message: "No SQL Server Agent alert is defined on this instance.", alerts: [], missing: [] },
    { code: "alert_disabled", message: "An alert is disabled, so it cannot fire.", alerts: ["Sev 20"], missing: [] },
  ] }, gapSpec);

  /* Memory. */
  const [, pressureGrid] = memory.memoryPressurePanels("srv", { hours: 24, label: "24 h" });
  await wait(400);
  const pressure = { cells: cellsOf(pressureGrid), headers: headersOf(pressureGrid), wide: hasClass(pressureGrid, "span-2") };
  const entries = [1, 2, 5].map((pool) => ({ collection_time: "2026-11-01T10:00:00", pool_id: pool, available_memory_mb: 10 }));
  const legend = grant.grantChartSeries(entries, [{ key: "available_memory_mb", label: "Available MB" }]).series.map((s) => s.label);

  /* Perfmon. */
  const perfmon = async (rows) => {
    counters = rows;
    const panel = activity.perfmonPanel("srv", { hours: 24, label: "24 h" });
    await wait(400);
    return { cells: cellsOf(panel), headers: headersOf(panel) };
  };
  const perfmonRepeat = await perfmon(COUNTERS);
  const perfmonPlain = await perfmon(COUNTERS.slice(0, 4));

  console.log(JSON.stringify({
    protectionChip: chipText("Protection"),
    identityChip: chipText("Identity capacity"),
    identityClickStopped: stopped,
    hashAfterClick: location.hash,
    scrolledTo: scrolled,
    legacyIdentityChip: legacyChip ? legacyChip.textContent : null,
    boxedLabels: boxed,
    azureLabels: azure,
    healthSentenceClasses: findAll(health, (n) => hasClass(n, "stat-sentence")).map((n) => n.textContent),
    healthTiles: findAll(health, (n) => hasClass(n, "stat")).map((n) => n.textContent),
    gapHeadersWithoutAlerts: headersOf(gapsMissing),
    gapHeadersWithAlerts: headersOf(gapsNamed),
    pressure,
    legend,
    perfmonRepeat,
    perfmonPlain,
  }));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
