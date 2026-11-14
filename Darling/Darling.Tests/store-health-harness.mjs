/* #8787: runs the Store Health page (wwwroot/js/pages/store-health.js) with the shipped panels.js and util.js against a scripted /api/read, and prints
   as one line of JSON what the page asked for and drew. StoreHealthPageBehaviourTests starts it as
       node store-health-harness.mjs <path to wwwroot/js>
   The whole js tree is copied into a scratch folder first (#5279). Only the DOM and fetch are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];

class FakeNode {
  constructor(tag, text) {
    this.style = {};
    const set = new Set();
    this.classList = { add: (c) => set.add(c), remove: (c) => set.delete(c), toggle: (c, on) => (on === undefined ? (set.has(c) ? set.delete(c) : set.add(c)) : on ? set.add(c) : set.delete(c)), contains: (c) => set.has(c) };
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.listeners = {};
    this.parent = null;
    this._text = text == null ? "" : String(text);
    this._class = "";
    this.value = "";
    this.disabled = false;
  }
  get className() { return this._class; }
  set className(v) { this._class = String(v); }
  get parentNode() { return this.parent; }
  get isConnected() { let n = this; while (n.parent) n = n.parent; return n.isRoot === true; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  contains(o) { for (let n = o; n; n = n.parent) if (n === this) return true; return false; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  removeAttribute(k) { delete this.attrs[k]; }
  hasAttribute(k) { return k in this.attrs; }
  appendChild(c) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    this.children.push(c);
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; return c; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  removeEventListener(t, fn) { this.listeners[t] = (this.listeners[t] || []).filter((f) => f !== fn); }
  querySelector() { return null; }
  querySelectorAll() { return []; }
  focus() {}
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, stopPropagation() {}, target: this, ...e }); }
}
globalThis.Node = FakeNode;
globalThis.document = {
  body: new FakeNode("body"),
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeNode("#text", t),
  addEventListener() {},
  removeEventListener() {},
  querySelector: () => null,
  querySelectorAll: () => [],
  getElementById: () => null,
};
globalThis.location = { hash: "#/store-health" };

const MB = 1024 * 1024;
const table = (o) => ({
  schema_name: "collect", table_name: "t", kind: "table", total_bytes: 10 * MB, heap_bytes: 8 * MB, index_bytes: 2 * MB, toast_bytes: 0, estimated_rows: 1000, live_rows: 1000,
  chunk_count: 0, leaf_count: 0, estimated_empty_bytes: null, estimated_empty_share: null, estimate_relations: 1, estimate_relations_usable: 0,
  estimate_unavailable_reason: null, estimate_unavailable_text: null, past_rewrite_floors: false, tail_occupied: null, rewrite_candidate: false,
  dead_rows: 0, vacuum_trigger_rows: 100, relations_past_vacuum_trigger: 0, autovacuum_disabled: false, pct_all_visible: 100, last_vacuum: null, last_autovacuum: null,
  last_analyze: null, last_autoanalyze: null, growth_bytes_7d: null, inserts_per_day: 10, updates_per_day: 0, deletes_per_day: 0, hot_update_share: null, turnover_per_day: 0,
  stats_since: "2026-11-01T00:00:00Z", ...o,
});
const NEVER = "never analyzed";
const TABLES = [
  table({ table_name: "store_metrics", total_bytes: 900 * MB, heap_bytes: 800 * MB, estimated_empty_bytes: 600 * MB, estimated_empty_share: 0.75, estimate_relations_usable: 1, past_rewrite_floors: true, tail_occupied: true, rewrite_candidate: true }),
  table({ table_name: "fresh_table", estimate_unavailable_reason: "never_analyzed", estimate_unavailable_text: NEVER }),
  table({ table_name: "query_stats", kind: "hypertable", chunk_count: 441, estimate_relations: 441, estimate_relations_usable: 400, estimated_empty_bytes: 5 * MB, estimated_empty_share: 0.05 }),
  table({ table_name: "emptied", estimated_empty_bytes: 300 * MB, estimated_empty_share: 0.8, estimate_relations_usable: 1, past_rewrite_floors: true, tail_occupied: false }),
];
const tablesAnswer = () => ({
  section: "tables", header: "x", estimate_note: "x", store_tables: 4, store_total_bytes: 1000 * MB, rewrite_candidates: 1, tables_without_estimate: 1,
  without_estimate_by_reason: { never_analyzed: 1 }, without_estimate_reason_text: { never_analyzed: NEVER }, truncated: false, tables_returned: 4, tables: TABLES,
});
const index = (o) => ({
  schema_name: "collect", table_name: "store_metrics", kind: "table", index_name: "ix_ok", on_compressed_data: false, index_count: 1, leaf_count: 0, bytes: 5 * MB, heap_bytes: 800 * MB,
  index_to_heap: 0.006, idx_scan: 100, is_primary_key: false, is_unique: false, valid: true, larger_than_heap: false, rarely_used: false,
  estimated_reclaimable_bytes: null, estimated_bloat_share: null, estimate_skipped_reason: null, ...o,
});
const INDEXES = [
  index({ index_name: "ix_broken", valid: false }),
  index({ index_name: "ix_big", larger_than_heap: true, bytes: 900 * MB, index_to_heap: 1.1 }),
  index({ index_name: "ix_ok" }),
];
const coverage = (o) => ({ schema_name: "collect", table_name: "parts", index_name: "ix_parts", parent_index_valid: true, leaf_count: 3, leaves_with_index: 3, leaves_missing_index: 0, missing_leaves: [], leaves_invalid_index: 0, invalid_leaves: [], ...o });
const indexesAnswer = (cov) => ({
  section: "indexes", index_header: "x", indexes_total: INDEXES.length, indexes_returned: INDEXES.length, indexes_truncated: false, indexes: INDEXES,
  partition_index_coverage_total: cov.length, partition_index_coverage_returned: cov.length, partition_index_coverage_truncated: false, partition_index_coverage: cov,
});
const MEASURE = {
  verdict: "Mostly empty and only a rewrite returns the space.", verdict_kind: "rewrite_needed", table: "collect.store_metrics", total_pages: 1000, pages_with_rows: 300, empty_pages: 700,
  empty_share: 0.7, rows: 3000, rows_per_used_page: 10, last_used_block: 998, last_file_block: 999, trailing_empty_pages: 0, vacuum_shrink_disabled: false,
  occupancy: { pages_with_1_row: 10, pages_with_2_to_4_rows: 20, pages_with_5_to_10_rows: 270, pages_with_over_10_rows: 0 }, elapsed_ms: 40,
};

const asks = [];
const gates = {};
let coverageRows = [coverage({ index_name: "ix_parts", leaves_missing_index: 1, missing_leaves: ["collect.parts_2"], leaves_with_index: 2 }), coverage({ index_name: "ix_fine" })];
const answerFor = (name, q) => {
  if (name === "get_store_table_health") return q.get("section") === "tables" ? tablesAnswer() : indexesAnswer(coverageRows);
  if (name === "get_store_maintenance_readiness") return { verdict: "Not safe for maintenance now: A vacuum is running on collect.store_metrics.", safe: false, reasons: ["A vacuum is running on collect.store_metrics."] };
  if (name === "measure_store_table") return MEASURE;
  return {};
};
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const name = u.pathname.replace("/api/read/", "");
  asks.push({ name, q: Object.fromEntries(u.searchParams) });
  const key = name === "get_store_table_health" ? name + ":" + u.searchParams.get("section") : name;
  if (gates[key]) await gates[key];
  return { status: 200, ok: true, headers: { get: () => null }, text: async () => JSON.stringify(answerFor(name, u.searchParams)) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "store-health-"));
let page;
let util;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  util = await load("util.js");
  page = await load(path.join("pages", "store-health.js"));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const flush = async () => { for (let i = 0; i < 40; i++) await new Promise((r) => setTimeout(r, 0)); };
const classOf = (n) => String(n.className || "");
const byClass = (root, cls) => all(root).filter((n) => classOf(n).split(" ").includes(cls));
const gridRows = (root) => all(root).filter((n) => n.tag === "tr" && n.parent && n.parent.tag === "tbody" && n.parent.parent && n.parent.parent.className === "data").map((tr) => tr.children.map((td) => td.textContent));
const gridHeads = (root) => { const th = all(root).filter((n) => n.tag === "thead"); return th.length ? th[0].children[0].children.map((c) => c.textContent) : []; };
const buttons = (root) => all(root).filter((n) => n.tag === "button" && n.textContent === "Measure");
const open = (deferred) => { let go; const p = new Promise((r) => { go = r; }); deferred.p = p; deferred.go = go; return deferred; };
const out = {};

/* 1. The first draw: the tables grid and header come as soon as the tables read returns, with the indexes read still held. */
const holdIndexes = open({});
gates["get_store_table_health:indexes"] = holdIndexes.p;
const main = new FakeNode("main");
main.isRoot = true;
page.renderStoreHealth(main);
await flush();
{
  const sections = main.children[0].children;
  const tilesText = byClass(main, "tile").map((t) => t.textContent);
  const headers = gridHeads(main);
  const rows = gridRows(main);
  out.first = {
    asks: asks.map((a) => ({ name: a.name, section: a.q.section ?? null, top: a.q.top ?? null })),
    tiles: tilesText,
    headerText: sections[1].textContent,
    tableHeaders: headers,
    tableRows: rows,
    note: byClass(main, "store-health-note").map((n) => n.textContent),
    indexesWhileHeld: sections[7].textContent,
    coverageWhileHeld: sections[8].textContent,
    readiness: sections[2].textContent,
    measureButtons: buttons(main).length,
  };
}
holdIndexes.go();
await flush();
{
  const sections = main.children[0].children;
  out.afterIndexes = {
    indexRows: gridRows(sections[7]),
    indexHeaders: gridHeads(sections[7]),
    indexSummary: byClass(sections[7], "meta").map((n) => n.textContent),
    coverageHeading: byClass(sections[8], "section-title").map((n) => n.textContent),
    coverageRows: gridRows(sections[8]),
  };
}

/* 2. Measure: one click asks for that one table, every Measure button is disabled while it runs, and the result shows under the grid. */
{
  const holdMeasure = open({});
  gates.measure_store_table = holdMeasure.p;
  const before = asks.length;
  const bs = buttons(main);
  bs[0].fire("click");
  await flush();
  bs[1] && bs[1].fire("click");
  await flush();
  const panelWhileRunning = main.children[0].children[5].textContent;
  const disabledWhileRunning = bs.map((b) => b.disabled === true);
  holdMeasure.go();
  await flush();
  const measureAsks = asks.slice(before).filter((a) => a.name === "measure_store_table").map((a) => a.q);
  const panel = main.children[0].children[5];
  out.measure = {
    asks: measureAsks,
    panelWhileRunning,
    disabledWhileRunning,
    disabledAfter: bs.map((b) => b.disabled === true),
    panelText: panel.textContent,
    histogramRows: gridRowsOfPlain(panel),
  };
}
function gridRowsOfPlain(root) {
  return all(root).filter((n) => n.tag === "tr" && n.parent && n.parent.tag === "tbody" && n.children.length === 3).map((tr) => tr.children.map((td) => td.textContent));
}

/* 3. A store whose partitions all have every index: the coverage section is one line. */
{
  coverageRows = [coverage({})];
  const main2 = new FakeNode("main");
  main2.isRoot = true;
  page.renderStoreHealth(main2);
  await flush();
  const sections = main2.children[0].children;
  out.cleanCoverage = { text: sections[8].textContent, headings: byClass(sections[8], "section-title").length };
}

/* 4. Refresh runs the tables and the indexes again, as two reads and never as section "all". */
{
  const before = asks.length;
  const refresh = all(main).find((n) => n.tag === "button" && n.textContent === "Refresh");
  refresh.fire("click");
  await flush();
  out.refresh = { asks: asks.slice(before).map((a) => ({ name: a.name, section: a.q.section ?? null })) };
}
out.neverAll = asks.filter((a) => a.q.section === "all").length;
console.log(JSON.stringify(out));
process.exit(0);
