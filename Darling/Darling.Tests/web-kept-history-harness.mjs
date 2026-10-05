/* Runs the web viewer's real read path (wwwroot/js/util.js, panels.js and pages/server-tabs.js, plus editor.js for
   an editor scenario and pages/server.js for the offeredRanges scenario) against a scripted /api/read answer and
   prints what the panels fetched and drew as one line of JSON. WebRangeKeptHistoryBehaviourTests and
   WebServerPageRangeTests start it as
       node web-kept-history-harness.mjs <path to wwwroot/js> <scenario>
   The modules are copied into a scratch folder beside a recording stand-in for charts.js (the SVG renderer, which
   needs a real browser), then imported. `fetch` and the DOM are stand-ins: a node tree of plain objects, and a fetch
   that answers each URL from the scenario. Everything else is the shipped code. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenarioArg] = process.argv.slice(-2);
/* A scenario may carry one argument after a colon: "offeredRanges:168" is the offeredRanges scenario at 168 hours. */
const [scenario, scenarioValue] = scenarioArg.split(":");
/* A scenario whose name ends in "Local" draws a window note in the browser's zone (#4966): the zone follows the colon
   ("floorLocal:America/New_York"), and the answer the page reads is HARNESS_INPUT, the JSON the server sent. The zone
   is set before anything formats a date, so the page's own localTime renders in it. */
if (scenario.endsWith("Local") && scenarioValue) process.env.TZ = scenarioValue;
const INPUT = process.env.HARNESS_INPUT ? JSON.parse(process.env.HARNESS_INPUT) : null;

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
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
  addEventListener() {}
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

/* The field config an editor scenario derived, or null. */
let vizcfg = null;

/* The offeredRanges scenario's findings: the hours the server page offers, each read it would ask for more, every
   ranged read it asked for (as "hours engine tool"), and each tab's note as the page renders it. */
let offered = null;
let beyond = null;
let observed = null;
let notes = null;
/* The custom-range scenarios' findings, whatever shape the scenario wants to report. */
let found = null;

/* The modules, unchanged, with charts.js replaced by a stand-in that records the window each chart was given. An
   editor scenario also loads the view editor (editor.js) and the modules it imports, with compose.js (the composed
   panel card, which no editor scenario draws) as a stand-in. editor.js keeps its save-path sample read in a
   module-private function, so the scratch copy appends one line exporting ensureFieldConfigs; nothing else in the
   copy changes. */
const editorScenario = scenario.startsWith("editor");
/* The offeredRanges scenario also loads the server page (pages/server.js and the fleet page it imports). The page
   keeps its Range presets, and the widest of them that it hands tabNote, in module-private constants, so the scratch
   copy appends one line exporting RANGE_OPTIONS and WIDEST_RANGE_HOURS, the same way the editor copy exports
   ensureFieldConfigs. */
const serverPageScenario = scenario === "offeredRanges" || scenario.startsWith("custom");
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "kept-history-"));
let modules;
try {
  fs.mkdirSync(path.join(scratch, "pages"));
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.copyFileSync(path.join(jsDir, "util.js"), path.join(scratch, "util.js"));
  fs.copyFileSync(path.join(jsDir, "panels.js"), path.join(scratch, "panels.js"));
  fs.copyFileSync(path.join(jsDir, "grid-tools.js"), path.join(scratch, "grid-tools.js"));
  fs.copyFileSync(path.join(jsDir, "pages", "server-tabs.js"), path.join(scratch, "pages", "server-tabs.js"));
  /* Copied when present, so this harness keeps working in a tree where server-tabs.js does not import it. */
  const findings = path.join("pages", "analysis-findings.js");
  if (fs.existsSync(path.join(jsDir, findings))) fs.copyFileSync(path.join(jsDir, findings), path.join(scratch, findings));
  fs.copyFileSync(path.join(jsDir, "read-fields.js"), path.join(scratch, "read-fields.js"));
  for (const file of ["viewer-local.js", "viewer-local-ui.js"]) fs.copyFileSync(path.join(jsDir, file), path.join(scratch, file));
  for (const rel of ["grid-tools.js", "multi-picker.js", path.join("pages", "analysis-findings.js"), path.join("pages", "plan-viewer.js")]) {
    const from = path.join(jsDir, rel);
    if (!fs.existsSync(from)) continue;
    fs.mkdirSync(path.dirname(path.join(scratch, rel)), { recursive: true });
    fs.copyFileSync(from, path.join(scratch, rel));
  }
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\n' +
      "export const SERIES_COLORS = ['#111', '#222', '#333', '#444', '#555', '#666'];\n" +
      "export const CATEGORICAL_COLORS = SERIES_COLORS;\n" +
      "export const chartCalls = [];\n" +
      "export function normalizeColor(color) { return color; }\n" +
      "export function renderLineChart(opts) {\n" +
      "  chartCalls.push({ windowStart: opts.windowStart, windowEnd: opts.windowEnd, points: (opts.points || []).length });\n" +
      "  return el('div', { class: 'chart-stub' });\n" +
      "}\n" +
      "export function zoomableLineChart(opts) { return renderLineChart(opts); }\n" +
      "export function chartZoomScope() { return ''; }\n"
  );
  if (editorScenario) {
    for (const file of ["derive.js", "alert-seed.js", "views-api.js", "refresh-policy.js", "refresh-control.js"]) {
      fs.copyFileSync(path.join(jsDir, file), path.join(scratch, file));
    }
    fs.writeFileSync(
      path.join(scratch, "editor.js"),
      fs.readFileSync(path.join(jsDir, "editor.js"), "utf8") + "\nexport { ensureFieldConfigs };\n"
    );
    fs.writeFileSync(path.join(scratch, "compose.js"), "export function renderComposedPanelCard() { throw new Error('not drawn here'); }\n");
  }
  if (serverPageScenario) {
    fs.copyFileSync(path.join(jsDir, "pages", "fleet.js"), path.join(scratch, "pages", "fleet.js"));
    /* The fleet page imports the mute-rules writes and the session read; copied when present, with what they import. */
    for (const rel of ["alerts-api.js", "views-api.js", "derive.js", "alert-seed.js", "refresh-policy.js", "refresh-control.js", path.join("pages", "mute-rules.js")]) {
      const from = path.join(jsDir, rel);
      if (fs.existsSync(from) && !fs.existsSync(path.join(scratch, rel))) fs.copyFileSync(from, path.join(scratch, rel));
    }
    fs.writeFileSync(
      path.join(scratch, "pages", "server.js"),
      fs.readFileSync(path.join(jsDir, "pages", "server.js"), "utf8") + "\nexport { RANGE_OPTIONS, WIDEST_RANGE_HOURS };\n"
    );
  }
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = {
    util: await load("util.js"),
    panels: await load("panels.js"),
    tabs: await load(path.join("pages", "server-tabs.js")),
    charts: await load("charts.js"),
    editor: editorScenario ? await load("editor.js") : null,
    server: serverPageScenario ? await load(path.join("pages", "server.js")) : null,
  };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const refusal = (asked, kept, days) => ({
  status: 400,
  body: {
    status: "invalid",
    message: "hours_back value '" + asked + "' exceeds maximum of " + kept + " hours (" + days + " days). Use a smaller value.",
  },
});
const data = (body) => ({ status: 200, body });
const asked = (url) => url.searchParams.get("hours");
const tool = (url) => url.pathname.replace("/api/read/", "");

const CPU = { samples: [{ sample_time: "2026-01-01T00:00:00", cpu: 5 }] };
const cpuPanel = {
  title: "CPU Utilization",
  read: "get_cpu_utilization",
  params: { server: "SRV1", hours: 720 },
  viz: "line",
  rowsKey: "samples",
  xKey: "sample_time",
  series: [{ key: "cpu", label: "CPU" }],
};
const RANGE = { hours: 720, label: "last 30 days" };

/* #4966: a grid read whose table starts covering the server after the window does answers with these three fields. */
const FLOOR_NOTE =
  "partial window: this panel's data starts at 2026-01-02 00:00 UTC, after the window's start at 2025-12-26 00:00 UTC. " +
  "The panel covers 2026-01-02 00:00 to 2026-01-03 00:00 UTC.";
const WAITING_TASKS_FLOOR = {
  tasks: [{ wait_type: "LCK_M_X", wait_duration_ms: 250 }],
  window_truncated: true,
  effective_start: "2026-01-02T00:00:00.0000000",
  truncation_note: FLOOR_NOTE,
};
const waitingTasksPanel = {
  title: "Waiting Tasks",
  read: "get_waiting_tasks",
  params: { server: "SRV1", hours: 168 },
  viz: "table",
  rowsKey: "tasks",
  columns: [{ key: "wait_type", label: "Wait type" }],
  emptyText: "No waiting tasks in this window.",
};

/* The Queries tab's Query Store grid: it names `truncation_note` as its own note (#4231), so the loader draws it. */
const queryStorePanel = {
  title: "Query Store",
  read: "get_query_store_top",
  params: { server: "SRV1", hours: 168, top: 20 },
  viz: "table",
  rowsKey: "queries",
  columns: [{ key: "query_id", label: "Query" }],
  emptyText: "No Query Store rows in this window.",
  noteKey: "truncation_note",
};

/* A body for the reads whose rows start a second read (the wait, counter and query pickers), so the trend reads
   behind them are fetched too. Every other read answers an empty object. */
const PICKER_ROWS = {
  get_wait_stats: { waits: [{ wait_type: "LCK_M_X" }] },
  get_perfmon_stats: { counters: [{ counter_name: "Batch Requests/sec" }] },
  get_top_queries_by_cpu: { queries: [{ query_hash: "0x1", database_name: "db1", query_text: "select 1" }] },
};

/* Let every read and every retry settle (each one is a few promise hops). A timer turn lasts about 15 ms on
   Windows, so a scenario that settles once per tab turns the event loop with setImmediate instead. */
const settle = async () => {
  for (let i = 0; i < 200; i++) await new Promise((r) => setTimeout(r, 0));
};
const settleFast = async () => {
  for (let i = 0; i < 200; i++) await new Promise((r) => setImmediate(r));
};

/* One server tab built at RANGE with one read answering `body` and every other read answering nothing. Each panel the
   tab drew is marked with its heading (the `where` the strips report), so a notice comes back naming its panel. */
const tabFloor = (tabId, read, body) => {
  answer = (url) => data(tool(url) === read ? body : {});
  const tab = modules.tabs.SERVER_TABS.find((t) => t.id === tabId);
  const panels = [].concat(tab.build("SRV1", RANGE));
  for (const panel of panels) panel.where = panel.children[0] ? panel.children[0].textContent : "";
  return panels;
};

/* The PostgreSQL tabs, the same way (#4966): one read answering `body`, every other read answering nothing. */
const pgTabFloor = (tabId, read, body) => {
  answer = (url) => data(tool(url) === read ? body : {});
  const tab = modules.tabs.POSTGRES_TABS.find((t) => t.id === tabId);
  const panels = [].concat(tab.build("SRV1", RANGE));
  for (const panel of panels) panel.where = panel.children[0] ? panel.children[0].textContent : "";
  return panels;
};

/* Enough of each PostgreSQL window read's answer for every panel of its fanout to draw (a stat tile needs one of its keys). */
const PG_WINDOW_BODIES = {
  get_pg_top_queries: { evictions: { eviction_passes_in_window: 0, max_entries: 5000 }, queries: [{ queryid: "1" }] },
  get_pg_blocking: { captures_total: 10, captures_with_blocking: 1, chains: [{ root_pid: 1 }], cycles: [{ pid: 2 }] },
  get_pg_database_stats: { database_count: 1, databases: [{ database_name: "db1" }] },
  get_pg_session_states: { captures_in_window: 3, sessions: [{ pid: 1 }] },
  get_pg_io_stats: { combinations_returned: 1, combinations: [{ backend_type: "client backend" }] },
  get_pg_replication_stats: { replicas: [{ application_name: "r1" }] },
  get_pg_database_trend: { points: [{ collection_time: "2026-01-01T00:00:00" }] },
  get_pg_query_duration_trend: { points: [{ collection_time: "2026-01-01T00:00:00" }] },
  get_pg_wait_trend: { points: [{ collection_time: "2026-01-01T00:00:00" }] },
  get_pg_io_trend: { points: [{ collection_time: "2026-01-01T00:00:00" }] },
  get_pg_plans: { plans: [{ query_id: 1 }] },
  get_pg_autovacuum_health: { tables_returned: 1, growing_count: 1, tables: [{ table_name: "t1" }] },
  get_pg_replication_slots: { slot_count: 1, worst_slot: "s1", slots: [{ slot_name: "s1" }] },
  get_pg_xmin_horizon: { winning_source: "session", winning_xmin_age: 5, holders: [{ source: "session" }] },
  get_pg_wraparound_risk: {
    worst_database: "db1", worst_pct_toward_wraparound: 1, thresholds: { failsafe_engages_around_pct: 74.5 }, databases: [{ database_name: "db1" }],
  },
  get_pg_write_stats: { checkpoints_timed: 4, checkpoints_requested: 1 },
};

const scenarios = {
  // #4966: every panel of a PostgreSQL window read's fanout draws the note (the value is "tab|read"), the stat tiles with the grids.
  floorPg: () => {
    const [tabId, read] = scenarioValue.split("|");
    return pgTabFloor(tabId, read, { ...PG_WINDOW_BODIES[read], window_truncated: true, truncation_note: FLOOR_NOTE });
  },
  // The page asks for 30 days; the read keeps 7 and says so; the retry at 168 hours answers.
  loaderRefused: () => {
    answer = (url) => (asked(url) === "720" ? refusal(720, 168, 7) : data(CPU));
    return [modules.panels.renderPanel(cpuPanel)];
  },
  // The retry answers the empty envelope: the notice still says which window the empty answer covers.
  loaderRefusedEmpty: () => {
    answer = (url) => (asked(url) === "720" ? refusal(720, 168, 7) : data({ status: "empty", message: "No CPU samples in this window." }));
    return [modules.panels.renderPanel(cpuPanel)];
  },
  // A read that keeps 30 days answers the first call, and nothing about the panel changes.
  loaderAccepted: () => {
    answer = () => data(CPU);
    return [modules.panels.renderPanel(cpuPanel)];
  },
  // A failure that is not a window refusal stays an error, with no second call.
  loaderOtherError: () => {
    answer = () => ({ status: 500, body: { error: "The store did not answer." } });
    return [modules.panels.renderPanel(cpuPanel)];
  },
  // A row-cap refusal says "exceeds maximum of 1000" with no hours: it is not a window refusal.
  loaderTopRefusal: () => {
    answer = () => ({ status: 400, body: { status: "invalid", message: "top value '1001' exceeds maximum of 1000. Use a smaller value." } });
    return [modules.panels.renderPanel(cpuPanel)];
  },
  // The retry is refused too (the read now keeps less than it said): there is no third call.
  loaderRefusedTwice: () => {
    answer = (url) => (asked(url) === "720" ? refusal(720, 168, 7) : refusal(asked(url), 96, 4));
    return [modules.panels.renderPanel(cpuPanel)];
  },
  // #4966: a grid whose response says its table starts covering the server after the window does shows that note.
  floorTableTruncated: () => {
    answer = () => data(WAITING_TASKS_FLOOR);
    return [modules.panels.renderPanel(waitingTasksPanel)];
  },
  // The same grid over a window the table covered: the response carries no floor fields, and the grid draws none.
  floorTableCovered: () => {
    answer = () => data({ tasks: WAITING_TASKS_FLOOR.tasks });
    return [modules.panels.renderPanel(waitingTasksPanel)];
  },
  // A chart over a response that carries the fields draws no strip: its time axis already spans the asked range.
  floorChartIgnored: () => {
    answer = () => data({ ...CPU, window_truncated: true, truncation_note: FLOOR_NOTE });
    return [modules.panels.renderPanel(cpuPanel)];
  },
  // A grid that names `truncation_note` as its own note (the Queries tab's grids) draws it once, not twice.
  floorOwnNoteOnce: () => {
    answer = () => data(WAITING_TASKS_FLOOR);
    return [modules.panels.renderPanel({ ...waitingTasksPanel, noteKey: "truncation_note" })];
  },
  // #4966: the window notes in the browser's zone. Each draws the answer the server sent (HARNESS_INPUT) through a panel
  // that shows its `truncation_note`: a grid that has none of its own (the Waiting Tasks grid), a descriptor grid that
  // names the note as its own (the Query Store grid on the Queries tab), and the hand-built Top Queries composite.
  floorLocal: () => {
    answer = () => data(INPUT);
    return [modules.panels.renderPanel(waitingTasksPanel)];
  },
  queryStoreLocal: () => {
    answer = () => data(INPUT);
    return [modules.panels.renderPanel(queryStorePanel)];
  },
  topQueriesLocal: () => {
    answer = (url) => (tool(url) === "get_top_queries_by_cpu" ? data(INPUT) : data({}));
    return [modules.tabs.topQueriesPanel("SRV1", RANGE)];
  },
  // The Wait Stats composite draws the note above its grid.
  floorWaitStats: () => {
    answer = (url) =>
      tool(url) === "get_wait_stats"
        ? data({ waits: [{ wait_type: "LCK_M_X" }], window_truncated: true, truncation_note: FLOOR_NOTE })
        : data({ trend: [{ time: "2026-01-01T00:00:00", wait_time_ms_per_second: 1 }] });
    return [modules.tabs.waitsPanel("SRV1", RANGE)];
  },
  // #4966: a stat tile over a windowed read draws the note too (a stat is not a chart: its figures are the window's).
  floorStatDraws: () => {
    answer = () => data({ total: 5, window_truncated: true, truncation_note: FLOOR_NOTE });
    return [modules.panels.renderPanel({ title: "Totals", read: "get_x", params: { server: "SRV1", hours: 168 }, viz: "stat", stats: [{ key: "total", label: "Total" }] })];
  },
  // A descriptor that opts out of the note draws none, over the same truncated answer.
  floorOptOut: () => {
    answer = () => data(WAITING_TASKS_FLOOR);
    return [modules.panels.renderPanel({ ...waitingTasksPanel, windowNote: false })];
  },
  // A descriptor with a floorKey reads the nested fields, and a top-level pair beside them is not what it draws.
  floorNested: () => {
    answer = () =>
      data({
        tasks: WAITING_TASKS_FLOOR.tasks,
        window: { window_truncated: true, truncation_note: "nested: the raw tier starts later" },
        window_truncated: true,
        truncation_note: "top level: not this one",
      });
    return [modules.panels.renderPanel({ ...waitingTasksPanel, floorKey: "window" })];
  },
  // The server tabs' own specs. Each scenario answers one read with the fields a truncated window carries and marks
  // every panel the tab drew with its heading, so the test can say which panels drew the note.
  floorMemoryGrants: () => tabFloor("memory", "get_memory_grants", {
    window_truncated: true, truncation_note: FLOOR_NOTE,
    window: [{ pool_id: 1, snapshots_in_window: 3 }], grants: [{ pool_id: 1, waiter_count: 0 }],
  }),
  floorResourceSemaphore: () => tabFloor("memory", "get_resource_semaphore", {
    window_truncated: true, truncation_note: FLOOR_NOTE,
    window: [{ pool_id: 1, resource_semaphore_id: 0, snapshots_in_window: 3 }], grants: [{ pool_id: 1, resource_semaphore_id: 0 }],
  }),
  floorPlanCorrections: () => tabFloor("queries", "get_plan_corrections", {
    window_truncated: true, truncation_note: FLOOR_NOTE,
    recommendations: [{ database_name: "db1", query_id: 1 }], automatic_tuning: [{ database_name: "db1" }],
  }),
  // An empty Plan Corrections answer: the note rides on the envelope. With the flag set it draws; without it, none.
  floorPlanCorrectionsEmpty: () => tabFloor("queries", "get_plan_corrections", {
    status: "empty", message: "No plan corrections in this window.", window_truncated: true, truncation_note: FLOOR_NOTE,
  }),
  floorPlanCorrectionsEmptyUntruncated: () => tabFloor("queries", "get_plan_corrections", {
    status: "empty", message: "No plan corrections in this window.",
  }),

  floorClutter: () => tabFloor("queries", "get_query_store_clutter", {
    databases: [{ database_name: "db1" }],
    qs_overhead: { wait_stats: { included: [{ wait_type: "QDS_X" }] }, memory_clerk: { latest_memory_mb: 1 } },
    window: { window_truncated: true, truncation_note: "nested: the raw tier starts later" },
  }),
  // A hand-built composite over one read (File I/O).
  fileIoRefused: () => {
    answer = (url) => (asked(url) === "720" ? refusal(720, 168, 7) : data({ trend: [{ time: "2026-01-01T00:00:00", database_name: "db1", file_type: "ROWS", avg_read_latency_ms: 4 }] }));
    return [modules.tabs.fileIoPanel("SRV1", RANGE)];
  },
  fileIoAccepted: () => {
    answer = () => data({ trend: [{ time: "2026-01-01T00:00:00", database_name: "db1", file_type: "ROWS", avg_read_latency_ms: 4 }] });
    return [modules.tabs.fileIoPanel("SRV1", RANGE)];
  },
  // The Wait Stats composite: its table read keeps 7 days, the trend read beside it accepts 30 and is unchanged.
  waitsTableRefusedTrendAccepted: () => {
    answer = (url) =>
      tool(url) === "get_wait_stats"
        ? asked(url) === "720" ? refusal(720, 168, 7) : data(PICKER_ROWS.get_wait_stats)
        : data({ trend: [{ time: "2026-01-01T00:00:00", wait_time_ms_per_second: 1 }] });
    return [modules.tabs.waitsPanel("SRV1", RANGE)];
  },
  // The view editor's save path: a table panel with no fields asks its read for a sample to derive them from. The
  // panel asks for 30 days and the read keeps 7, so the sample must come from the retry, as the preview's does.
  editorSampleRefused: async () => {
    answer = (url) => (asked(url) === "720" ? refusal(720, 168, 7) : data(CPU));
    const panel = { read: "get_cpu_utilization", viz: "table", params: { server: "SRV1", hours: 720 }, vizcfg: null };
    await modules.editor.ensureFieldConfigs(
      { panels: [panel] },
      { reads: [{ name: "get_cpu_utilization", params: [{ name: "server" }, { name: "hours" }] }] }
    );
    vizcfg = panel.vizcfg;
    return [];
  },
  // Every tab of both registries at 30 days, with every read refusing it: each one is asked again at 168 hours.
  census: () => {
    answer = (url) => (asked(url) === "720" ? refusal(720, 168, 7) : data(PICKER_ROWS[tool(url)] || {}));
    const nodes = [];
    for (const registry of [modules.tabs.SERVER_TABS, modules.tabs.POSTGRES_TABS]) {
      for (const tab of registry) nodes.push(...[].concat(tab.build("SRV1", RANGE)));
    }
    return nodes;
  },
  // Every range the server page's Range offers, through every tab of both registries, against reads that refuse
  // more than the scenario's hours the way the capped reads do. Each read the page would ask for more than that is
  // listed by range, registry and tab. What each tab draws is mounted too, under a node that names its range and tab,
  // so a notice or an error the page shows at an offered range comes back saying where.
  offeredRanges: async () => {
    const maxHours = Number(scenarioValue);
    answer = (url) => {
      const hours = Number(asked(url));
      return hours > maxHours ? refusal(hours, maxHours, Math.round(maxHours / 24)) : data(PICKER_ROWS[tool(url)] || {});
    };
    offered = modules.server.RANGE_OPTIONS.map((option) => option.hours);
    beyond = [];
    observed = [];
    notes = [];
    const drawn = [];
    for (const option of modules.server.RANGE_OPTIONS) {
      for (const [engine, registry] of [["SQL Server", modules.tabs.SERVER_TABS], ["PostgreSQL", modules.tabs.POSTGRES_TABS]]) {
        for (const tab of registry) {
          const before = fetches.length;
          const holder = new FakeNode("div");
          holder.where = option.label + ": " + engine + " " + tab.id + " tab";
          modules.util.mount(holder, tab.build("SRV1", { hours: option.hours, label: option.label }));
          drawn.push(holder);
          await settleFast();
          for (const fetched of fetches.slice(before)) {
            const url = new URL(fetched, "http://viewer.test");
            if (asked(url) !== null) {
              observed.push(option.hours + " " + engine + " " + tool(url));
            }
            if (Number(asked(url)) > maxHours) {
              beyond.push(option.label + ": " + engine + " " + tab.id + " tab, " + tool(url) + " asked for " + asked(url) + " hours");
            }
          }
        }
      }
    }
    for (const [engine, registry] of [["SQL Server", modules.tabs.SERVER_TABS], ["PostgreSQL", modules.tabs.POSTGRES_TABS]]) {
      for (const tab of registry) {
        const note = modules.tabs.tabNote(tab, modules.server.WIDEST_RANGE_HOURS);
        if (note) {
          notes.push(engine + " " + tab.id + ": " + note.textContent);
        }
      }
    }
    return drawn;
  },
};

/* ── custom range (server page) ── */
const T0 = "2026-01-02T07:15:00.000Z";
const T1 = "2026-01-02T10:30:00.000Z";
const NOW = Date.parse("2026-06-01T00:00:00.000Z");
const customServerPage = async (server, opts) => {
  const holder = new FakeNode("div");
  modules.server.renderServer(holder, server, "cpu", opts);
  await settleFast();
  return holder;
};
const cpuRows = ["2026-01-02T06:00:00", "2026-01-02T07:30:00", "2026-01-02T09:00:00", "2026-01-02T10:00:00", "2026-01-02T11:00:00"].map((t) => ({ sample_time: t, cpu: 5 }));
Object.assign(scenarios, {
  // The mapping from a picked pair to as_of + whole hours, and every refusal.
  customMapping: () => {
    const r = (a, b) => modules.server.resolveCustomRange(Date.parse(a), Date.parse(b), NOW);
    found = {
      rounded: r(T0, T1),
      exact: r("2026-01-02T06:30:00.000Z", T1),
      subHour: r("2026-01-02T10:00:00.000Z", T1),
      reversed: r(T1, T0),
      future: r("2026-05-31T00:00:00.000Z", "2026-06-02T00:00:00.000Z"),
      tooWide: r("2025-12-01T00:00:00.000Z", "2026-01-02T00:00:00.000Z"),
      live: modules.server.resolveCustomRange(NOW - 5 * 3600000, NOW - 1000, NOW),
    };
    return [];
  },
  // A past-end custom range through the page: its reads carry as_of=end and the rounded hours.
  customPageReads: async () => {
    answer = (url) => (url.pathname === "/api/fleet" ? data({ cards: [] }) : data({ samples: cpuRows }));
    const holder = await customServerPage("SRV1");
    const before = fetches.length;
    const err = modules.server.applyCustomRange("SRV1", Date.parse(T0), Date.parse(T1), NOW);
    await settleFast();
    found = { err, reads: fetches.slice(before) };
    return [holder];
  },
  // A rebuild (the poll) of a past-end range reads nothing; a live range and a preset read again; another server resets.
  customPoll: async () => {
    answer = (url) => (url.pathname === "/api/fleet" ? data({ cards: [] }) : data({ samples: cpuRows }));
    await customServerPage("SRV1");
    modules.server.applyCustomRange("SRV1", Date.parse(T0), Date.parse(T1), NOW);
    await settleFast();
    const reads = (from) => fetches.slice(from).filter((f) => f.startsWith("/api/read/"));
    let mark = fetches.length;
    await customServerPage("SRV1", { poll: true });
    const pastPoll = reads(mark);
    mark = fetches.length;
    await customServerPage("SRV1");
    const tabClick = reads(mark);
    mark = fetches.length;
    await customServerPage("SRV2", { poll: true });
    const other = reads(mark);
    modules.server.applyCustomRange("SRV3", Date.now() - 6 * 3600000, Date.now(), Date.now());
    await settleFast();
    mark = fetches.length;
    await customServerPage("SRV3", { poll: true });
    const livePoll = reads(mark);
    found = { pastPoll, tabClick, other, livePoll };
    return [];
  },
  // The custom range trims a chart's rows to the exact pair, and a read with no as_of keeps the preset and says so.
  customTrimAndNotice: async () => {
    answer = (url) => data(tool(url) === "get_cpu_utilization" ? { samples: cpuRows } : { rows: [] });
    modules.util.setActiveRange({ server: "SRV1", hours: 4, startMs: Date.parse(T0), endMs: Date.parse(T1), asOf: T1 });
    const chart = modules.panels.renderPanel({ ...cpuPanel, params: { server: "SRV1", hours: 4 } });
    const latency = modules.panels.renderPanel({
      title: "Read latency", read: "get_read_latency", params: { server: "SRV1", hours: 4 }, viz: "table", rowsKey: "rows",
      columns: [{ key: "route", label: "Route" }], emptyText: "none",
    });
    const other = modules.panels.renderPanel({ ...cpuPanel, params: { server: "SRV2", hours: 4 } });
    return [chart, latency, other];
  },
  // A read wider than the store keeps is asked again for the hours it keeps: the retry keeps the range's end, draws only
  // the part of the range the store holds, and the notice says so.
  customKeptHistory: async () => {
    answer = (url) => (Number(asked(url)) > 4 ? refusal(asked(url), 4, 0) : data({ samples: cpuRows }));
    modules.util.setActiveRange({ server: "SRV1", hours: 24, startMs: Date.parse("2026-01-01T11:15:00.000Z"), endMs: Date.parse(T1), asOf: T1 });
    return [modules.panels.renderPanel({ ...cpuPanel, params: { server: "SRV1", hours: 24 } })];
  },
  // Aggregate rows stamped with their last sample survive the trim; per-point series rows do not.
  customAggregateRows: async () => {
    const latch = [{ latch_class: "A", captured_at: "2026-01-02T06:45:00" }, { latch_class: "B", captured_at: "2026-01-02T09:00:00" }];
    answer = (url) => data(tool(url) === "get_latch_stats" ? { latches: latch } : { samples: cpuRows });
    modules.util.setActiveRange({ server: "SRV1", hours: 4, startMs: Date.parse(T0), endMs: Date.parse(T1), asOf: T1 });
    const lat = await modules.util.readTool("get_latch_stats", { server: "SRV1", hours: 4 });
    const cpu = await modules.util.readTool("get_cpu_utilization", { server: "SRV1", hours: 4 });
    found = { latchRows: lat.data.latches.length, seriesRows: cpu.data.samples.length };
    return [];
  },
  // The label says where totals begin when the span is not whole hours.
  customRoundedLabel: async () => {
    answer = (url) => (url.pathname === "/api/fleet" ? data({ cards: [] }) : data({ samples: cpuRows }));
    await customServerPage("SRV1");
    const ctx = (a, b) => {
      modules.server.applyCustomRange("SRV1", Date.parse(a), Date.parse(b), NOW);
      return modules.server.rangeContext(NOW).label;
    };
    found = { rounded: ctx(T0, T1), whole: ctx("2026-01-02T06:30:00.000Z", T1) };
    return [];
  },
});

const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);

const root = new FakeNode("main");
modules.util.mount(root, await chosen());
await settle();

/* The text of every strip of one kind under the root. A node the scenario marked with `where` (the offeredRanges
   scenario names the range and tab each one drew) puts that in front of the text of the strips beneath it. */
const strips = (kind) => {
  const found = [];
  const walk = (n, where) => {
    if (!n || typeof n !== "object") return;
    const here = n.where || where;
    if (n.className === "strip " + kind) found.push(here ? here + ": " + n.textContent : n.textContent);
    n.children.forEach((child) => walk(child, here));
  };
  walk(root, null);
  return found;
};

/* What the page's own localTime makes of each instant the answer carries (#4966), keyed by the instant as sent and by
   its UTC minute ("2026-01-02 00:00", the form the server's sentence uses), and the zone's offset at the first one, so a
   test can tell the browser's clock from UTC. Empty without an input. */
const local = {};
let tzOffset = null;
for (const key of ["data_start_utc", "window_start_utc", "window_end_utc", "oldest_shown_utc", "effective_start"]) {
  const value = INPUT && INPUT[key];
  if (typeof value !== "string") continue;
  local[value] = modules.util.localTime(value);
  local[value.slice(0, 16).replace("T", " ")] = local[value];
  const instant = modules.util.parseUtc(value);
  if (tzOffset === null && instant) tzOffset = instant.getTimezoneOffset();
}

console.log(JSON.stringify({
  local,
  tzOffset,
  fetches,
  notices: strips("notice"),
  errors: strips("error"),
  empties: strips("empty"),
  loading: strips("loading").length,
  chartHours: modules.charts.chartCalls.map((c) => (c.windowStart == null ? null : Math.round((c.windowEnd - c.windowStart) / 3600000))),
  vizcfg,
  offered,
  beyond,
  observed,
  notes,
  found,
  chartPoints: modules.charts.chartCalls.map((c) => c.points),
  rejections,
}));
