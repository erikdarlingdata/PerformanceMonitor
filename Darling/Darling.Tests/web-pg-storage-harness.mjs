/* #7551: runs the shipped PostgreSQL Storage tiles (wwwroot/js/pages/server-tabs/pg-storage.js through panels.js VIZ.stat) on a small fake DOM and prints
   what they drew as one line of JSON: the Bloat Estimates and Index Usage tiles, the sentence under them when the page was cut, and the Index Bloat
   columns that say what an estimated row's figure rests on. WebPgStorageTruthsBehaviourTests starts it as
       node web-pg-storage-harness.mjs <path to the js folder>
   Only the DOM is a stand-in. */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.parent = null;
    this._text = "";
    this.className = "";
    this.dataset = {};
    this.style = {};
    const set = new Set();
    this.classList = { add: (c) => set.add(c), remove: (c) => set.delete(c), toggle: (c, on) => (on ? set.add(c) : set.delete(c)), contains: (c) => set.has(c) };
  }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); if (k === "class") this.className = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { c.parent = this; this.children.push(c); return c; }
  get firstChild() { return this.children[0] || null; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; return c; }
  addEventListener() {}
  findAll(pred, out = []) { if (pred(this)) out.push(this); this.children.forEach((c) => c.findAll(pred, out)); return out; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/a/storage" };

const root = process.argv[2];
const { VIZ } = await import(pathToFileURL(root + "/panels.js").href);
const { PG_BLOAT_STATS, PG_INDEX_USAGE_STATS, PG_INDEX_BLOAT_COLUMNS } = await import(pathToFileURL(root + "/pages/server-tabs/pg-storage.js").href);

const draw = (stats, data) => {
  const node = VIZ.stat(data, { stats });
  const values = node.findAll((n) => n.className && String(n.className).startsWith("value")).map((n) => n.textContent);
  const labels = node.findAll((n) => n.className === "label").map((n) => n.textContent);
  const tiles = {};
  labels.forEach((l, i) => { tiles[l] = values[i]; });
  const notices = node.findAll((n) => String(n.className).includes("notice")).map((n) => n.textContent);
  return { tiles, notices };
};

const out = {};
/* The reply of a server whose 25-row page was all suppressed rows, with 140 tables in the window. */
out.noEstimate = draw(PG_BLOAT_STATS, {
  estimated_bloat_bytes_over_trusted_rows: null, estimated_bloat_mb_over_trusted_rows: null, estimated_bloat_state: "No estimate published",
  tables_returned: 25, tables_in_window: 140, trusted_estimate_count: 0, suppressed_estimate_count: 140, pgstattuple_available_anywhere: true, truncated: true,
});
/* A published estimate that really is nothing stays 0 B. */
out.realZero = draw(PG_BLOAT_STATS, {
  estimated_bloat_bytes_over_trusted_rows: 0, estimated_bloat_mb_over_trusted_rows: 0, estimated_bloat_state: null,
  tables_returned: 3, tables_in_window: 3, trusted_estimate_count: 3, suppressed_estimate_count: 0, pgstattuple_available_anywhere: true, truncated: false,
});
out.usageCut = draw(PG_INDEX_USAGE_STATS, {
  indexes_returned: 25, indexes_in_window: 312, unscanned_without_a_structural_blocker: 4, mb_held_by_those_indexes: 12, invalid_index_count: 0,
  statistics_were_reset_in_window: false, truncated: true,
});
/* A reply from a build that sent no count keeps the plain sentence. */
out.usageCutNoCount = draw(PG_INDEX_USAGE_STATS, {
  indexes_returned: 25, unscanned_without_a_structural_blocker: 4, mb_held_by_those_indexes: 12, invalid_index_count: 0, statistics_were_reset_in_window: false, truncated: true,
});

const col = (key) => PG_INDEX_BLOAT_COLUMNS.find((c) => c.key === key);
const estimatedRow = { measurement_kind: "estimated", est_bloat_pct: 0, estimated_reclaimable_mb: 0, avg_leaf_density: null, leaf_fragmentation: null, skipped_reason: null };
out.indexBloat = {
  hasEstPct: !!col("est_bloat_pct"),
  basisEstimated: col("measurement_kind") ? col("measurement_kind").display(estimatedRow) : null,
  basisMeasured: col("measurement_kind") ? col("measurement_kind").display({ measurement_kind: "measured" }) : null,
  basisNone: col("measurement_kind") ? col("measurement_kind").display({}) : null,
};
console.log(JSON.stringify(out));
