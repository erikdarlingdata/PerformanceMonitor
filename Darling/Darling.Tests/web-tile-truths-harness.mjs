/* Runs the shipped web page script for the tiles, grid columns and legends of the Configuration, Activity and Memory tabs that said
   something other than what they hold (#7416) and prints one line of JSON:
     - the Overview tiles: Protection (what it counts) and Identity capacity (the fullest column, and what a click does)
     - the Service objective tile on a boxed server and on an Azure SQL Database
     - Health Checks: its sentence is not a stat tile
     - Alert Setup Gaps: its column headers with and without a named alert
     - Memory Pressure Events: the words in its cells, and its width
     - the Memory Grant Sizing legend: the pool names
     - Perfmon Counters: whole numbers, and the Object column when a counter and instance repeat
     - the Alert setup tile (a count of gaps) and the casing of the status words on the fleet card, the sweep and the skipped-cycles tiles (#7416 item 29)
     - the Recommendation tiles of Scheduler Pressure and Plan Cache, drawn as a sentence (item 30)
     - the Pool column of the Memory Grants grids, named
     - the Platform column of Backups and CHECKDB by Database, dropped when no row names a platform (item 33)
     - the Unrateable Rows column of Query Store Overhead, dropped when every row is zero (item 32)
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
let fleetReply = {};
let healthReply = {};
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const body = u.pathname === "/api/fleet"
    ? fleetReply
    : u.pathname.endsWith("/get_memory_pressure_events")
    ? { events: EVENTS }
    : u.pathname.endsWith("/get_collection_health")
    ? healthReply
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
  const cpu = await imp("pages/server-tabs/sql-cpu.js");
  const queries = await imp("pages/server-tabs/sql-queries.js");
  const tabs = await imp("pages/server-tabs.js");

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
  /* A bigint remaining count above 2^53 arrives as a digit string and must print its exact digits (Number() would round them). */
  const bigCard = { server_name: "srv-c", display_name: "srv-c", is_online: true, identity_capacity: { worst_band: "warning", columns_at_bar: 1, worst_database: "Sales", worst_table: "dbo.Orders", worst_column: "OrderId", worst_percent_used: 70, worst_values_remaining: "2767011611056432742" } };
  const bigChip = findAll(fleet.metricBands(bigCard, null), (n) => hasClass(n, "metric-chip")).find((x) => x.textContent.startsWith("Identity capacity"));
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
    { summary: { text: "11 of 13 judged databases overdue. Full backup: a, b. CHECKDB: c." }, captured_at: "2026-11-01T10:00:00" },
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

  /* Item 29: the Alert setup tile counts the gaps of the grid below it, and a status word has one casing wherever it is drawn. */
  const tiles = (root) => Object.fromEntries(findAll(root, (n) => hasClass(n, "stat")).map((n) => [n.children[1].textContent, n.children[0].textContent]));
  const agentSpec = config.AGENT_ALERT_SPECS[0];
  const gapRow = (message) => ({ code: "x", message, alerts: [], missing: [] });
  const alertSetup = {
    gaps3: tiles(panels.VIZ.stat({ instance_status: "gaps", gaps: [gapRow("a"), gapRow("b"), gapRow("c")] }, agentSpec))["Alert setup"],
    gaps1: tiles(panels.VIZ.stat({ instance_status: "gaps", gaps: [gapRow("a")] }, agentSpec))["Alert setup"],
    ok: tiles(panels.VIZ.stat({ instance_status: "ok", gaps: [] }, agentSpec))["Alert setup"],
  };
  const quietCard = fleet.metricBands({ server_name: "srv-q", display_name: "srv-q", is_online: true, has_memory_pressure: false, available_threads: null }, null);
  /* #7560: a PostgreSQL card (has_memory_pressure null, blocking_count a structural 0, Unknown severities) prints n/a on Memory and
     Blocking, and a SQL Server card with the same zero keeps printing it. */
  const unmeasuredChips = (card) => Object.fromEntries(
    findAll(fleet.metricBands(card, null), (n) => hasClass(n, "metric-chip"))
      .filter((x) => /^(Memory|Blocking)/.test(x.textContent))
      .map((x) => [x.textContent.replace(/(Memory|Blocking).*$/, "$1"), x.textContent]));
  const engineChips = {
    postgres: unmeasuredChips({ server_name: "pg", display_name: "pg", is_online: true, is_postgres: true, has_memory_pressure: null, memory_severity: "Unknown", blocking_count: 0, blocking_severity: "Unknown" }),
    sqlServer: unmeasuredChips({ server_name: "sq", display_name: "sq", is_online: true, is_postgres: false, has_memory_pressure: false, memory_severity: "Healthy", blocking_count: 0, blocking_severity: "Healthy" }),
    /* #7616 / #7654: a PostgreSQL card with captures prints the count of captures that saw blocking, a measured 0 prints 0, and only a target
       with no capture in the window prints n/a with a plain sentence. */
    postgresBlocking: unmeasuredChips({ server_name: "pgb", display_name: "pgb", is_online: true, is_postgres: true, has_memory_pressure: null, memory_severity: "Unknown", blocking_count: 2, blocking_captures: 60, blocking_measured: true, blocking_severity: "Healthy" }),
    postgresQuiet: unmeasuredChips({ server_name: "pgq", display_name: "pgq", is_online: true, is_postgres: true, has_memory_pressure: null, memory_severity: "Unknown", blocking_count: 0, blocking_captures: 60, blocking_measured: true, blocking_severity: "Healthy" }),
    postgresNoCapture: unmeasuredChips({ server_name: "pgn", display_name: "pgn", is_online: true, is_postgres: true, has_memory_pressure: null, memory_severity: "Unknown", blocking_count: 0, blocking_captures: 0, blocking_measured: false, blocking_severity: "Unknown" }),
    /* #7616: 2 captures is too few to band, so the card reads Unknown and the chip says why in the service's own sentence (blocking_detail). */
    postgresYoung: unmeasuredChips({ server_name: "pgy", display_name: "pgy", is_online: true, is_postgres: true, has_memory_pressure: null, memory_severity: "Unknown", blocking_count: 2, blocking_captures: 2, blocking_measured: true, blocking_severity: "Unknown", blocking_detail: "fewer than 3 captures in this window" }),
  };
  /* #7616 / #3631: the Blocking (recent) tile adds waits and captures into one number, so its hover names the units the service sent. */
  const blockingTileTitle = (detail) => {
    const tile = findAll(fleet.rollup({ total_servers: 2, total_blocking_events: 31, total_blocking_detail: detail, total_deadlocks: 0 }), (n) => hasClass(n, "tile"))
      .find((n) => n.children[1] && n.children[1].textContent === "Blocking (recent)");
    return tile ? tile.getAttribute("title") : "no tile";
  };
  const blockingTile = {
    both: blockingTileTitle("SQL Server blocked waits plus PostgreSQL blocking captures, in one number"),
    none: blockingTileTitle(null),
  };
  const quietChips = Object.fromEntries(findAll(quietCard, (n) => hasClass(n, "metric-chip")).map((n) => [n.textContent, true]));
  /* #7507: the Blocking and Deadlocks tiles name the window the reply covered. The reply is read through the shipped apiGetFleet, so the
     window each tile names is the one the server sent, and a quiet card (no events) still names it. */
  const util = await imp("util.js");
  const detailsOf = (bands) => Object.fromEntries(
    findAll(bands, (n) => hasClass(n, "metric-chip"))
      .filter((x) => /^(Blocking|Deadlocks)/.test(x.textContent))
      .map((x) => [findAll(x, (n) => hasClass(n, "label"))[0].textContent.replace(/\W.*$/, ""), (findAll(x, (n) => hasClass(n, "detail"))[0] || { textContent: null }).textContent]),
  );
  const windowTiles = async (hours) => {
    const end = Date.parse("2026-11-01T12:00:00Z");
    const quiet = { server_name: "q", display_name: "q", is_online: true, blocking_count: 0, deadlock_count: 0, deadlock_rate_per_hour: null };
    const busy = { server_name: "b", display_name: "b", is_online: true, blocking_count: 302, max_blocking_wait_ms: 24500, deadlock_count: 50, deadlock_rate_per_hour: 50, deadlock_last_seen: "2026-11-01T11:50:00" };
    fleetReply = { window_start: new Date(end - hours * 3600000).toISOString().replace("Z", ""), window_end: new Date(end).toISOString().replace("Z", ""), cards: [quiet, busy] };
    const res = await util.apiGetFleet();
    const [q, b] = res.data.cards;
    return { quiet: detailsOf(fleet.metricBands(q, null)), busy: detailsOf(fleet.metricBands(b, null)) };
  };
  const windowChips = { hour: await windowTiles(1), day: await windowTiles(24) };
  fleetReply = { cards: [{ server_name: "n", display_name: "n", is_online: true, blocking_count: 0, deadlock_count: 0, deadlock_rate_per_hour: null }] };
  windowChips.noWindow = detailsOf(fleet.metricBands((await util.apiGetFleet()).data.cards[0], null));
  const sweepTiles = tiles(panels.VIZ.stat({ sweep_pressure: { verdict: "ok" }, skipped_cycles: { status: "unavailable" } }, { stats: [...shared.SWEEP_STATS, ...shared.SKIPPED_CYCLES_STATS] }));

  /* #7660: the Skipped Cycles panel drawn the way the page draws it (the Collection Health tab of both products, through the real fanout): the last
     skip's sentence is a note under the tiles (never a tile), its gap in the browser's zone, the Cause tile in the sentence's words, and a server
     that skipped nothing has no note at all. */
  const skippedBlock = (extra) => ({ skipped_cycles: { status: "ok", count_24h: 1, slots_skipped_24h: 21, last_at: "2026-11-04T18:41:26Z", last_slowest_step: "running_jobs", last_slowest_step_ms: 15000,
    last_message: "Collection fell behind on this server: 21 slot(s) skipped. Cause: the previous collection pass took 73s and was still running when the next one was due (slowest: running jobs 15s). The gap ran from 2026-11-04 18:40:00 UTC to 2026-11-04 18:41:26 UTC.", ...extra } });
  const healthyBlock = { skipped_cycles: { status: "ok", count_24h: 0, slots_skipped_24h: 0, last_at: null, last_slowest_step: null, last_slowest_step_ms: null, last_message: null } };
  const drawSkipped = async (tabList, reply, tabId = "health") => {
    healthReply = reply;
    const root = new FakeNode("main");
    util.mount(root, tabList.find((t) => t.id === tabId).build("srv", { hours: 24, label: "last 24 hours" }));
    for (let i = 0; i < 25; i++) await wait(0);
    const panel = findAll(root, (n) => hasClass(n, "panel") && findAll(n, (c) => c.tag === "h3").some((h) => h.textContent.startsWith("Skipped Cycles")))[0];
    if (!panel) return { panel: false };
    const strips = findAll(panel, (n) => hasClass(n, "strip") && hasClass(n, "notice")).map((n) => n.textContent);
    const order = findAll(panel, (n) => hasClass(n, "stat") || (hasClass(n, "strip") && hasClass(n, "notice"))).map((n) => (hasClass(n, "stat") ? "stat" : "strip"));
    return { panel: true, strips, tiles: tiles(panel), order, localGap: new Date("2026-11-04T18:40:00Z").toLocaleString() };
  };
  const skippedCycles = {
    sql: await drawSkipped(tabs.SERVER_TABS, skippedBlock()),
    pg: await drawSkipped(tabs.POSTGRES_TABS, skippedBlock(), "overview"),
    healthy: await drawSkipped(tabs.SERVER_TABS, healthyBlock),
    healthyPg: await drawSkipped(tabs.POSTGRES_TABS, healthyBlock, "overview"),
    analysis: await drawSkipped(tabs.SERVER_TABS, skippedBlock({ last_slowest_step: "analysis_pass" })),
    unmeasured: await drawSkipped(tabs.SERVER_TABS, skippedBlock({ last_slowest_step: "collectors", last_slowest_step_ms: 0 })),
    /* #7660 review: the other side of the `collectors` branch, a measured time above 0 reads the step's own name, never "none measured". */
    measured: await drawSkipped(tabs.SERVER_TABS, skippedBlock({ last_slowest_step: "collectors", last_slowest_step_ms: 15000 })),
    measuredPg: await drawSkipped(tabs.POSTGRES_TABS, skippedBlock({ last_slowest_step: "collectors", last_slowest_step_ms: 15000 }), "overview"),
    gate: await drawSkipped(tabs.SERVER_TABS, skippedBlock({ last_slowest_step: "gate_wait", last_slowest_step_ms: 40000 })),
    guard: await drawSkipped(tabs.SERVER_TABS, skippedBlock({ last_slowest_step: "launch_guard_hold", last_slowest_step_ms: 40000 })),
  };

  /* Item 30: a sentence is text across the panel, never a stat tile. */
  const sentenceOf = (stats, key, text) => {
    const root = panels.VIZ.stat(key.split(".").reduceRight((v, k) => ({ [k]: v }), text), { stats });
    return { sentences: findAll(root, (n) => hasClass(n, "stat-sentence")).map((n) => n.textContent), tileLabels: Object.keys(tiles(root)) };
  };
  const recommendations = {
    scheduler: sentenceOf(cpu.SCHEDULER_STATS, "recommendation", "Scheduler pressure is low; no change needed."),
    planCache: sentenceOf(memory.PLAN_CACHE_STATS, "summary.bloat_recommendation", "Trim single-use plans with optimize for ad hoc workloads."),
  };

  /* #7416: the Pool column of the Memory Grants grids reads the pool's name. */
  const poolRow = (id) => ({ pool_id: id, resource_semaphore_id: 0, snapshots_in_window: 9 });
  const poolCells = (columns) => cellsOf(panels.VIZ.table({ rows: [poolRow(1), poolRow(2), poolRow(7)] }, { rowsKey: "rows", columns }));
  /* #7578: the Semaphore column prints the semaphore's name, and the scheduler's Runnable % tile prints 0.0 when the read answered with no share. */
  const semaphoreRow = (id) => ({ pool_id: 1, resource_semaphore_id: id });
  const semaphoreWords = {
    window: cellsOf(panels.VIZ.table({ rows: [semaphoreRow(0), semaphoreRow(1), semaphoreRow(5)] }, { rowsKey: "rows", columns: memory.SEMAPHORE_WINDOW_COLUMNS })),
    newest: cellsOf(panels.VIZ.table({ rows: [semaphoreRow(0), semaphoreRow(1)] }, { rowsKey: "rows", columns: memory.SEMAPHORE_COLUMNS })),
    /* The Resource Semaphore read of the Memory page's table (read-fields.js) names them too. */
    read: cellsOf(panels.VIZ.table({ grants: [semaphoreRow(0), semaphoreRow(1)] }, (await imp("read-fields.js")).READ_FIELDS.get_resource_semaphore.table)),
  };
  /* #7578: the date-time columns of the window grid wrap (class "wrap" and the narrow flow-cell floor), so the grid's last columns stay in the panel. */
  const stamp = "2026-11-05T10:00:00Z";
  const windowGrid = panels.VIZ.table({ rows: [{ pool_id: 1, resource_semaphore_id: 0, peak_waiters_at: stamp, peak_granted_at: stamp, first_snapshot_at: stamp, last_snapshot_at: stamp, snapshots_in_window: 9 }] }, { rowsKey: "rows", columns: memory.SEMAPHORE_WINDOW_COLUMNS });
  const windowCellClasses = Object.fromEntries(headersOf(windowGrid).map((h, i) => [h, findAll(windowGrid, (n) => n.tag === "td")[i].className || ""]));
  const windowHeaderClasses = Object.fromEntries(findAll(windowGrid, (n) => n.tag === "th").map((n) => [n.textContent, n.className || ""]));
  /* #7578: choosing a sort refreshes every header's class name, and that refresh must keep the wrap-head class. */
  const sortedGrid = panels.VIZ.table({ rows: [{ pool_id: 1, resource_semaphore_id: 0, peak_waiters_at: stamp }, { pool_id: 2, resource_semaphore_id: 0, peak_waiters_at: "2026-11-05T11:00:00Z" }] }, { rowsKey: "rows", columns: memory.SEMAPHORE_WINDOW_COLUMNS });
  const sortHeads = findAll(sortedGrid, (n) => n.tag === "th");
  const sortHead = sortHeads.find((n) => n.textContent.startsWith("Peak waiters at"));
  (sortHead.listeners.click || []).forEach((fn) => fn({}));
  const windowHeaderClassesAfterSort = Object.fromEntries(sortHeads.map((n) => [n.textContent.replace(/[ ▲▼]+$/, ""), n.className || ""]));
  const sortedState = sortHead.attrs["aria-sort"];
  const fileIo = (await imp("read-fields.js")).READ_FIELDS.get_file_io_stats.table;
  const fileIoHeaders = headersOf(panels.VIZ.table({ rows: [{}] }, { ...fileIo, rowsKey: "rows" }));
  const runnable = {
    nullPercent: tiles(panels.VIZ.stat({ pressure_level: "Low", schedulers: 4, runnable_percent: null }, { stats: cpu.SCHEDULER_STATS }))["Runnable %"],
    value: tiles(panels.VIZ.stat({ pressure_level: "Low", schedulers: 4, runnable_percent: 12.34 }, { stats: cpu.SCHEDULER_STATS }))["Runnable %"],
    tooltip: findAll(panels.VIZ.stat({ pressure_level: "Low", schedulers: 4, runnable_percent: null }, { stats: cpu.SCHEDULER_STATS }), (n) => hasClass(n, "stat") && n.children[1].textContent === "Runnable %").map((n) => n.attrs.title),
  };
  const pools = {
    grants: poolCells(memory.GRANT_COLUMNS),
    semaphores: poolCells(memory.SEMAPHORE_COLUMNS),
    semaphoreWindow: poolCells(memory.SEMAPHORE_WINDOW_COLUMNS),
  };

  /* Item 33: the Platform column of Backups and CHECKDB by Database. */
  const protectionSpec = config.DATABASE_PROTECTION_SPECS.find((s) => s.title === "Backups and CHECKDB by Database");
  const dbRow = (name, platform) => ({ database_name: name, database: name, state: "ONLINE", platform });
  const platformHeaders = (rows) => headersOf(panels.VIZ.table({ databases: rows }, protectionSpec));
  const platform = {
    boxed: platformHeaders([dbRow("a", "native"), dbRow("b", "native")]),
    managed: platformHeaders([dbRow("a", "native"), dbRow("b", "platform_managed")]),
    managedCells: cellsOf(panels.VIZ.table({ databases: [dbRow("a", "native"), dbRow("b", "platform_managed")] }, protectionSpec)),
  };

  /* Item 32: the Unrateable Rows column of Query Store Overhead. */
  const waitRow = (name, unknowable) => ({ wait_type: name, rows_observed: 4, rows_unknowable: unknowable });
  const overheadHeaders = (rows) => headersOf(panels.VIZ.table({ rows }, { rowsKey: "rows", columns: queries.QS_OVERHEAD_WAIT_COLUMNS }));
  const unrateable = {
    allZero: overheadHeaders([waitRow("QDS_A", 0), waitRow("QDS_B", 0)]),
    someSamples: overheadHeaders([waitRow("QDS_A", 0), waitRow("QDS_B", 2)]),
  };

  console.log(JSON.stringify({
    alertSetup, quietChips, engineChips, blockingTile, windowChips, sweepTiles, skippedCycles, recommendations, pools, semaphoreWords, windowCellClasses, windowHeaderClasses, windowHeaderClassesAfterSort, sortedState, fileIoHeaders, runnable, platform, unrateable,
    protectionChip: chipText("Protection"),
    identityChip: chipText("Identity capacity"),
    bigIdentityChip: bigChip ? bigChip.textContent : null,
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
