/* Runs the shipped page scripts against a node-tree DOM and a recording fetch, and prints as one line of JSON what each of the web details
   of #7702 now shows: the FinOps Wait Stats Summary's duration cells, the version store chart's tick and tooltip figures, the Database Scoped
   Configuration subtitle, the Configuration Audit's grouped figures, the Buffer Pool Residency Kind words, the Admin Engine words, the Index
   Bloat Basis cell on a row with no estimate, the Alert Settings headings, the sp_server_diagnostics label and the notify severity word, the
   zero-filled event-count trend points, and the sidebar's Query Tags entry.
   A step that throws (a page script without the helper it drives) records the error under its key, so every other key is still printed.
       node web-details-7702-harness.mjs <path to wwwroot/js> */
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
    this.value = "";
    this.checked = false;
    this.handlers = {};
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
  addEventListener(type, fn) {
    this.handlers[type] = fn;
  }
  get isConnected() {
    return true;
  }
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
globalThis.window = globalThis;
globalThis.location = { hash: "" };

let body = "{}";
globalThis.fetch = async () => ({ status: 200, ok: true, text: async () => body });

const findAll = (node, test, out = []) => {
  if (test(node)) out.push(node);
  node.children.forEach((c) => findAll(c, test, out));
  return out;
};
const settle = () => new Promise((r) => setTimeout(r, 30));

const out = {};
const step = (key, fn) => {
  try {
    out[key] = fn();
  } catch (e) {
    out[key] = { error: String((e && e.message) || e) };
  }
};

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "web-details-7702-"));
try {
  /* The whole js tree is copied, so a page module another change adds needs no edit here. The real chart module stands, so every page's
     imports resolve; only the line chart is replaced, by one that records the spec it was given. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const specs = [];
  globalThis.__chartSpecs = specs;
  fs.renameSync(path.join(scratch, "charts.js"), path.join(scratch, "charts-real.js"));
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'export * from "./charts-real.js";\nimport { el } from "./util.js";\nexport function zoomableLineChart(spec) { globalThis.__chartSpecs.push(spec); return el("div", { class: "chart" }); }\n'
  );
  const load = (...segs) => import(pathToFileURL(path.join(scratch, ...segs)).href);
  const { VIZ, zeroFilledPoints } = await load("panels.js");

  /* The cells a column list draws for one row, by column label. */
  const cells = (columns, row) => {
    const table = VIZ.table({ rows: [row] }, { rowsKey: "rows", columns });
    const tds = findAll(table, (n) => n.tag === "td").map((n) => n.textContent);
    return Object.fromEntries(columns.map((c, i) => [c.label, tds[i]]));
  };

  // 2. FinOps Wait Stats Summary: the two wait figures print as durations, as the Wait Stats tab does.
  const { WAIT_COLUMNS } = await load("pages", "finops", "optimization.js");
  step("waitCells", () => cells(WAIT_COLUMNS, { category: "CPU", total_wait_time_ms: 400639, waiting_tasks: 3, pct_of_total: 50, top_wait_type: "SOS_SCHEDULER_YIELD", top_wait_time_ms: 133555, est_cost_usd: 1 }));
  step("waitLabels", () => WAIT_COLUMNS.map((c) => c.label));

  // 3. The version store chart's figures: a whole-number tick step prints no decimals, a tooltip keeps two.
  const versionStore = await load("pages", "finops", "version-store.js");
  body = JSON.stringify({
    as_of: "2026-11-05T12:00:00Z",
    databases: [{ database_name: "Alpha", pvs_size_mb: 20 }],
    trend: [{ database_name: "Alpha", points: [{ collection_time: "2026-11-05T11:00:00Z", pvs_size_mb: 10 }, { collection_time: "2026-11-05T12:00:00Z", pvs_size_mb: 20 }] }],
  });
  specs.length = 0;
  step("pvsBuild", () => versionStore.tab.build("srv-a", {}) && true);
  await settle();
  const pvsSpec = specs[0];
  step("pvsTicks", () => [pvsSpec.formatValue(20, { step: 5 }), pvsSpec.formatValue(0, { step: 5 }), pvsSpec.formatValue(0.5, { step: 0.25 }), pvsSpec.formatValue(12.345, null)]);

  // 4 + 5. Configuration Audit figures and the Database Scoped Configuration subtitle.
  const sqlConfig = await load("pages", "server-tabs", "sql-config.js");
  step("auditFigures", () => [sqlConfig.auditFigureText("92160"), sqlConfig.auditFigureText(2147483647), sqlConfig.auditFigureText("ON"), sqlConfig.auditFigureText("1.5"), sqlConfig.auditFigureText(null), sqlConfig.auditFigureText(" 57344 ")]);
  step("auditCells", () => cells(sqlConfig.AUDIT_COLUMNS, { setting: "max server memory (MB)", status: "review", current_value: "92160", suggested_value: 57344, recommendation: "Set it to 57,344 MB." }));
  body = JSON.stringify({ captured_at: "2026-11-05T12:00:00Z", databases: [{ database_name: "Alpha", settings: [{ name: "MAXDOP", value: "0", value_for_secondary: null }] }] });
  let scoped = new FakeNode("div");
  step("scopedBuild", () => (scoped = sqlConfig.scopedConfigPanel("srv-a")) && true);
  await settle();
  step("scopedHeading", () => findAll(scoped, (n) => n.tag === "h3")[0].textContent);

  // 7. Buffer Pool Residency: the catalog letter in words.
  const pgIo = await load("pages", "server-tabs", "pg-io.js");
  step("kindWords", () => ["r", "i", "t", "m", "x", null].map((k) => pgIo.pgRelationKindWord(k)));
  step("kindCell", () => cells(pgIo.PG_BUFFER_USAGE_COLUMNS, { relation_display: "pg_class", relation_kind: "r", buffer_bytes: 8192, pct_of_pool: 1, dirty_buffers: 0, pct_dirty: 0, avg_usage_count: 3 }).Kind);

  // 8. Admin Engine words, on the Servers tab and the Server Actions tab.
  const actions = await load("pages", "admin-server-actions.js");
  const admin = await load("pages", "admin.js");
  const serverRow = (e) => ({ display_name: "A", server_name: "a", auth: "Windows", engine: e, version: "x", status: "Enabled", freshness: "Online", monthly_cost: "$1", read_only: false });
  step("engineWords", () => ["sqlserver", "postgres", "PostgreSQL", "other", null].map((e) => actions.engineWord(e)));
  step("serverEngineCells", () => ["sqlserver", "postgres"].map((e) => cells(admin.SERVER_COLUMNS, serverRow(e)).Engine));
  step("actionEngineCells", () => ["sqlserver", "postgres"].map((e) => cells(actions.SERVER_ACTION_COLUMNS, { server: "a", engine: e, action: "x", state: "Off", set_by: null, changed: null, note: "" }).Engine));

  // 9. Index Bloat Basis: a row the read skipped has no basis.
  const pgStorage = await load("pages", "server-tabs", "pg-storage.js");
  const bloatRow = (extra) => ({ schema_name: "public", table_name: "t", index_name: "ix", index_bytes: 8192, measurement_kind: "estimated", ...extra });
  step("basisCells", () => [
    cells(pgStorage.PG_INDEX_BLOAT_COLUMNS, bloatRow({ skipped_reason: "ColumnWidthsNotVisible" })).Basis,
    cells(pgStorage.PG_INDEX_BLOAT_COLUMNS, bloatRow({ skipped_reason: null })).Basis,
    cells(pgStorage.PG_INDEX_BLOAT_COLUMNS, bloatRow({ measurement_kind: "measured" })).Basis,
  ]);

  // 14. Alert Settings: one heading style, the procedure's name, and the severity word.
  step("sectionTitles", () => admin.settingSections({
    alerts_enabled: true,
    excluded_databases: [],
    long_running_query: { enabled: true, exclude_sp_server_diagnostics: true },
    health_bands: { deadlock_warn_per_hour: 1 },
    analysis: { enabled: true, notify_severity: 1.5 },
    fleet_sweep: { enabled: true },
    daily_report: { enabled: true },
  }).map((s) => s.title));
  step("spServerDiagnosticsLabel", () => admin.settingLabel("long_running_query", "exclude_sp_server_diagnostics"));
  step("notifySeverity", () => [1.5, 0.75, 0.2, "1.5", null].map((v) => admin.notifySeverityText(v)));
  step("notifySeverityRow", () => admin.groupRows("analysis", { notify_severity: 1.5 }).map((r) => r.label + " = " + r.value));

  // 15. An event-count trend: the buckets the read left out are drawn at 0 across the window.
  const series = [{ key: "count", label: "Reports" }];
  const two = [{ time: "2026-11-05T10:00:00.000Z", count: 4 }, { time: "2026-11-05T10:01:00.000Z", count: 1 }];
  step("zeroFill", () => {
    const filled = zeroFilledPoints(two, "time", series, null, { windowStart: Date.parse("2026-11-05T09:50:00Z"), windowEnd: Date.parse("2026-11-05T10:05:00Z") });
    return {
      count: filled.length,
      first: filled[0].time,
      last: filled[filled.length - 1].time,
      zeros: filled.filter((p) => p.count === 0).length,
      kept: filled.filter((p) => two.includes(p)).length,
      sum: filled.reduce((s, p) => s + p.count, 0),
    };
  });
  step("zeroFillNoWindow", () => zeroFilledPoints(two, "time", series, 5, null).map((p) => p.count));
  step("zeroFillEmpty", () => zeroFilledPoints([], "time", series, null, null).length);

  // 17. The sidebar names the page as the page names itself.
  step("sidebarQueryTags", () => {
    const html = fs.readFileSync(path.join(jsDir, "..", "index.html"), "utf8");
    const m = html.match(/<a data-route="manage-query-tags"[^>]*>([^<]*)<\/a>/);
    return m ? m[1] : null;
  });

  process.stdout.write(JSON.stringify(out) + "\n");
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
