/* #7539: runs the shipped PostgreSQL Activity grids (wwwroot/js/pages/server-tabs/pg-activity.js through panels.js VIZ.table) on a small fake DOM and
   prints one line of JSON: the Plan Capture Readiness facet labels, and what the Logging Settings Audit drew for rows shaped like the read's answer
   (get_pg_logging_audit). WebPgLoggingAuditBehaviourTests starts it as
       node web-pg-logging-audit-harness.mjs <path to the js folder>
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
globalThis.location = { hash: "#/server/a/activity" };

const root = process.argv[2];
const { VIZ } = await import(pathToFileURL(root + "/panels.js").href);
const { PG_PLAN_CAPTURE_READINESS_COLUMNS, PG_LOGGING_AUDIT_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/pg-activity.js").href);

const draw = (columns, rows) => VIZ.table({ facets: rows }, { rowsKey: "facets", title: "t", tools: false, columns });
const bodyRows = (t) => t.findAll((n) => n.tag === "tr").filter((tr) => tr.findAll((n) => n.tag === "td").length > 0);
const cells = (tr) => tr.findAll((n) => n.tag === "td");
const headers = (t) => t.findAll((n) => n.tag === "th").map((n) => n.textContent);

const out = {};

/* Plan Capture Readiness: the facet column, one row per facet the collector writes. */
const facets = ["library_loaded", "capture_threshold", "extension_available", "plan_text_setting", "plan_attribution", "message_locale", "log_line_prefix_readable"];
const readiness = draw(PG_PLAN_CAPTURE_READINESS_COLUMNS, facets.map((facet) => ({ facet, is_satisfied: true, observed: "x", detail: "Fine." })));
out.facetCells = bodyRows(readiness).map((tr) => cells(tr)[0].textContent);

/* Logging Settings Audit: rows as DarlingPgLoggingAudit writes them, tool names and all. */
const unlocks = "One LOG line per EXECUTION that ran longer than the threshold, carrying the duration and the statement text. What this product has instead is pg_stat_statements through get_pg_top_queries: per-SHAPE aggregates that can say a shape averages 200 ms.";
const recommended = "A millisecond threshold sized to the workload, never 0 - 1000 is a common starting point on an OLTP workload. auto_explain.log_min_duration is the separate threshold for PLANS and is judged by get_pg_plan_capture_readiness.";
const remedy = "Run ALTER SYSTEM SET log_min_duration_statement = 1000; then SELECT pg_reload_conf(). get_pg_server_config shows the setting's source.";
const audit = draw(PG_LOGGING_AUDIT_COLUMNS, [
  { setting: "log_min_duration_statement", verdict: "Partial", value: "1000", unit: "ms", source: "configuration file", change_needs: "reload; applies to connections opened after it",
    unlocks, recommended, remedy, cost_note: "Costs one line per outlier. get_pg_blocking is the sampled read." },
  { setting: "shared_preload_libraries", verdict: "Off", value: "", change_needs: "restart", unlocks: "Short.", recommended: "Short.", remedy: "", cost_note: "" },
]);
out.auditHeaders = headers(audit);
const auditRows = bodyRows(audit);
/* Copy and the CSV read the row, so the export-only cells (hidden by the stylesheet) are not part of what a person sees. */
out.auditRows = auditRows.map((tr) => cells(tr).filter((td) => !td.classes.has("export-only")).map((td) => ({
  cls: String(td.className),
  text: td.textContent,
  summary: td.findAll((n) => n.tag === "summary").map((n) => n.textContent)[0] ?? null,
  detail: td.findAll((n) => String(n.className).includes("disc-body")).map((n) => n.textContent)[0] ?? null,
})));
console.log(JSON.stringify(out));
