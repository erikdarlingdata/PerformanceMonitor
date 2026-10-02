/* Runs the web viewer's real read path (wwwroot/js/util.js, panels.js and pages/server-tabs.js, plus editor.js for
   an editor scenario) against a scripted /api/read answer and prints what the panels fetched and drew as one line of
   JSON. WebRangeKeptHistoryBehaviourTests starts it as
       node web-kept-history-harness.mjs <path to wwwroot/js> <scenario>
   The modules are copied into a scratch folder beside a recording stand-in for charts.js (the SVG renderer, which
   needs a real browser), then imported. `fetch` and the DOM are stand-ins: a node tree of plain objects, and a fetch
   that answers each URL from the scenario. Everything else is the shipped code. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

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

/* The modules, unchanged, with charts.js replaced by a stand-in that records the window each chart was given. An
   editor scenario also loads the view editor (editor.js) and the modules it imports, with compose.js (the composed
   panel card, which no editor scenario draws) as a stand-in. editor.js keeps its save-path sample read in a
   module-private function, so the scratch copy appends one line exporting ensureFieldConfigs; nothing else in the
   copy changes. */
const editorScenario = scenario.startsWith("editor");
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "kept-history-"));
let modules;
try {
  fs.mkdirSync(path.join(scratch, "pages"));
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.copyFileSync(path.join(jsDir, "util.js"), path.join(scratch, "util.js"));
  fs.copyFileSync(path.join(jsDir, "panels.js"), path.join(scratch, "panels.js"));
  fs.copyFileSync(path.join(jsDir, "pages", "server-tabs.js"), path.join(scratch, "pages", "server-tabs.js"));
  fs.copyFileSync(path.join(jsDir, "read-fields.js"), path.join(scratch, "read-fields.js"));
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\n' +
      "export const SERIES_COLORS = ['#111', '#222', '#333', '#444', '#555', '#666'];\n" +
      "export const chartCalls = [];\n" +
      "export function normalizeColor(color) { return color; }\n" +
      "export function renderLineChart(opts) {\n" +
      "  chartCalls.push({ windowStart: opts.windowStart, windowEnd: opts.windowEnd });\n" +
      "  return el('div', { class: 'chart-stub' });\n" +
      "}\n"
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
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = {
    util: await load("util.js"),
    panels: await load("panels.js"),
    tabs: await load(path.join("pages", "server-tabs.js")),
    charts: await load("charts.js"),
    editor: editorScenario ? await load("editor.js") : null,
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

/* A body for the reads whose rows start a second read (the wait, counter and query pickers), so the trend reads
   behind them are fetched too. Every other read answers an empty object. */
const PICKER_ROWS = {
  get_wait_stats: { waits: [{ wait_type: "LCK_M_X" }] },
  get_perfmon_stats: { counters: [{ counter_name: "Batch Requests/sec" }] },
  get_top_queries_by_cpu: { queries: [{ query_hash: "0x1", database_name: "db1", query_text: "select 1" }] },
};

const scenarios = {
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
};

const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);

const root = new FakeNode("main");
modules.util.mount(root, await chosen());
// Let every read and every retry settle (each one is a few promise hops).
for (let i = 0; i < 200; i++) await new Promise((r) => setTimeout(r, 0));

const strips = (kind) => {
  const found = [];
  const walk = (n) => {
    if (!n || typeof n !== "object") return;
    if (n.className === "strip " + kind) found.push(n.textContent);
    n.children.forEach(walk);
  };
  walk(root);
  return found;
};

console.log(JSON.stringify({
  fetches,
  notices: strips("notice"),
  errors: strips("error"),
  empties: strips("empty"),
  loading: strips("loading").length,
  chartHours: modules.charts.chartCalls.map((c) => (c.windowStart == null ? null : Math.round((c.windowEnd - c.windowStart) / 3600000))),
  vizcfg,
  rejections,
}));
