/* Runs the web viewer's real alert-notebook renderer (wwwroot/js/pages/views.js, renderNotebookDoc in alert mode)
   on a scenario and prints what it drew as one line of JSON. AlertNotebookRenderBehaviourTests starts it as
       node alert-notebook-harness.mjs <path to views.js> <scenario>
   views.js is an ES module whose imports are browser pieces, so its import lines become the stand-ins below (a
   tree of plain objects for the DOM, recording stand-ins for the panel renderers) and its `export` keywords are
   dropped. Everything else is views.js as shipped: the catalog check, the cell routing, the in-flight limiter. */
import fs from "node:fs";
import vm from "node:vm";

const [viewsPath, scenario] = process.argv.slice(-2);

const source = fs.readFileSync(viewsPath, "utf8")
  .replace(/^import .*;[ \t]*\r?$/gm, "")
  .replace(/^export /gm, "");
if (/^import /m.test(source)) {
  throw new Error("views.js layout changed: the harness cannot strip a multi-line import");
}

const tablesSource = fs.readFileSync(new URL("../PerformanceMonitor.Darling.Service/wwwroot/js/read-tables.js", import.meta.url), "utf8")
  .replace(/^export /gm, "");

const flat = (x) => (Array.isArray(x) ? x.flatMap(flat) : x == null ? [] : [x]);
const el = (tag, attrs, kids) => ({ tag, cls: (attrs && attrs.class) || "", text: attrs && attrs.text, kids: flat(kids), addEventListener() {} });
const mount = (parent, nodes) => { parent.kids = flat(nodes); };

const calls = { reads: [], composed: [], markdown: [] };
const settleLater = (cb) => { if (cb) Promise.resolve().then(cb); };
const card = (title, cls) => el("div", { class: "panel card " + cls }, [el("h3", {}, [title])]);

const context = vm.createContext({
  console,
  el,
  mount,
  apiGetFleet: async () => ({ kind: "data", data: { cards: [] } }),
  loadingStrip: () => el("div", { class: "strip loading" }),
  errorStrip: (m) => el("div", { class: "strip error", text: m }),
  emptyStrip: (m) => el("div", { class: "strip empty", text: m }),
  noticeStrip: (m) => el("div", { class: "strip notice", text: m }),
  relTime: String,
  localTime: String,
  fmtNum: String,
  renderPanel: (desc, onSettled) => { calls.reads.push({ read: desc.read, title: desc.title, rowsKey: desc.rowsKey || null, columns: Array.isArray(desc.columns) ? desc.columns.length : 0, viz: desc.viz, series: Array.isArray(desc.series) ? desc.series.length : 0, stats: Array.isArray(desc.stats) ? desc.stats.length : 0, xKey: desc.xKey || null }); settleLater(onSettled); return card(desc.title, "read"); },
  setPanelSignal: () => {},
  VIZ: { table: () => null, line: () => null, stat: () => null, bandlist: () => null },
  renderComposedPanelCard: (spec, scope, onSettled) => {
    calls.composed.push({ title: spec.title, server: scope.server, hours: scope.hours, range: spec.range || null });
    settleLater(onSettled);
    return card(spec.title, "composed");
  },
  renderMarkdown: (text) => { calls.markdown.push(text); return el("div", { class: "md-out", text }); },
  NOTEBOOK_TEMPLATES: [],
  DASHBOARD_TEMPLATES: [],
  isNotebookDefinition: () => false,
  api: {},
  refreshChoiceOf: () => "off",
  buildRefreshControl: () => null,
  location: { hash: "#/triage" },
});
vm.runInContext(tablesSource, context);
vm.runInContext(source, context);

const RANGE = { windowStart: "2026-01-01T00:00:00Z", windowEnd: "2026-01-01T12:00:00Z" };
const header = { type: "header", title: "Alert" };
const read = { type: "read", read: "get_blocking", viz: "table", title: "Blocking chains", params: { server: "SRV1", hours: "24" } };
const markdown = { type: "markdown", text: "**Category:** blocking" };
const panel = (title, viz) => ({ type: "panel", title, source: "blocked_process_reports", measure: "bpr_wait_time_ms", aggregate: "count", viz, range: RANGE });
const badPanel = (n) => ({ type: "panel", title: "Bad panel " + n, source: "no_such_source", viz: "line" });

const catalog = { reads: [{ name: "get_blocking" }], compose: { measures: [{ source: "blocked_process_reports" }] } };
const alert = { server_name: "SRV1", metric_name: "Blocking Detected", alert_time: "2026-01-01T12:00:00Z" };

const scenarios = {
  // A blocking-shaped notebook: a read the catalog lists, prose, and charts with their own absolute range.
  good: { cells: [header, read, markdown, panel("Blocked-process reports over time", "line"), panel("Lock modes", "pie")], opts: { alert, catalog, scopeServer: "SRV1" } },
  // The caller passed no catalog: the empty one stays as the fallback, so the cells fail their own check.
  noCatalog: { cells: [header, read, panel("Lock modes", "pie")], opts: { alert } },
  // No matched alert row: the endpoint resolved the link's server (a display name) to its registry name, SRV9.
  linkServer: { cells: [header, panel("Lock modes", "pie")], opts: { alert: null, catalog, server: "Link display name", scopeServer: "SRV9" } },
  // The alert row and the link carry DISPLAY names; the charts scope by the registry name the endpoint sent.
  displayName: { cells: [header, panel("Lock modes", "pie")], opts: { alert: { ...alert, server_name: "Orders (display name)" }, catalog, server: "Orders (display name)", scopeServer: "registry-key-a" } },
  // The same display names with no scope_server (nothing resolved): the chart is not shown, never scoped by a name.
  displayNameOnly: { cells: [header, panel("Lock modes", "pie")], opts: { alert: { ...alert, server_name: "Orders (display name)" }, catalog, server: "Orders (display name)" } },
  // No alert row and no server on the link: nothing to scope a chart to.
  noServer: { cells: [header, panel("Lock modes", "pie")], opts: { alert: null, catalog } },
  // A cell kind the page has no renderer for must say so.
  unknownKind: { cells: [header, { type: "sparkline", title: "Mystery cell" }], opts: { alert, catalog } },
  // Three panels that cannot start a load, then a read: if a bad panel held its limiter slot the read never draws.
  badPanelsThenRead: { cells: [header, badPanel(1), badPanel(2), badPanel(3), read], opts: { alert, catalog, scopeServer: "SRV1" } },
};

// catalog: the whole field catalog, as plain JSON (what each read gives each viz).
if (scenario === "catalog") {
  const cat = vm.runInContext("typeof READ_FIELDS !== 'undefined' ? READ_FIELDS : READ_TABLES", context);
  console.log(JSON.stringify({ catalog: JSON.parse(JSON.stringify(cat)) }));
  process.exit(0);
}
// resolve:<viz>/<read>,...: what the page's lookup gives each read cell (a bare name is a table cell).
const splitVizRead = (entry) => { const i = entry.indexOf("/"); return i < 0 ? ["table", entry] : [entry.slice(0, i), entry.slice(i + 1)]; };
const keysOf = (arr) => (Array.isArray(arr) ? arr.map((c) => c.key) : []);
if (scenario.startsWith("resolve:")) {
  const resolved = scenario.slice(8).split(",").map((entry) => {
    const [viz, name] = splitVizRead(entry);
    const d = context.resolveReadTable({ type: "read", read: name, viz, title: name, params: {} });
    return { read: name, viz, rowsKey: d.rowsKey || null, xKey: d.xKey || null, columns: keysOf(d.columns), series: keysOf(d.series), stats: keysOf(d.stats) };
  });
  console.log(JSON.stringify({ resolved }));
  process.exit(0);
}
// draw:<viz>/<read>,...: read cells, as the templates emit them, through the alert notebook page.
if (scenario.startsWith("draw:")) {
  const entries = scenario.slice(5).split(",").map(splitVizRead);
  scenarios[scenario] = {
    cells: [header, ...entries.map(([viz, n]) => ({ type: "read", read: n, viz, title: n, params: { server: "SRV1", hours: "24" } }))],
    opts: { alert, catalog: { reads: entries.map(([, n]) => ({ name: n })), compose: { measures: [] } }, scopeServer: "SRV1" },
  };
}
const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);

const main = { tag: "main", cls: "", kids: [] };
await context.renderNotebookDoc(main, {
  mode: "alert",
  definition: { kind: "notebook", cells: chosen.cells },
  status: "Unknown",
  notes: [],
  canEdit: false,
  provenance: "from alert template test",
  onOpenLive: () => {},
  ...chosen.opts,
});
// Let the limiter's queued slots run (each grant is a promise hop).
for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 0));

const strips = (cls) => {
  const found = [];
  const walk = (n) => {
    if (!n || typeof n !== "object") return;
    if (n.tag === "div" && n.cls === "strip " + cls) found.push(n.text);
    n.kids.forEach(walk);
  };
  walk(main);
  return found;
};
const titles = [];
const walkTitles = (n) => {
  if (!n || typeof n !== "object") return;
  if (n.tag === "h3") titles.push(n.kids.join(""));
  n.kids.forEach(walkTitles);
};
walkTitles(main);

console.log(JSON.stringify({
  errors: strips("error"),
  notices: strips("notice"),
  loading: strips("loading").length,
  titles,
  reads: calls.reads,
  composed: calls.composed,
  markdown: calls.markdown,
}));
