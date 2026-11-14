/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Store Health page (#8787): the health of the monitoring store's OWN tables and indexes, one page for the whole store.
 * It answers three questions that nothing else in the product answers: which store table carries empty space, which one
 * autovacuum is behind on, and which one would gain from a rewrite.
 *
 * The page reads get_store_table_health twice, started together: section "tables" and section "indexes". The tables half
 * takes about a second and a half on a busy laptop and the indexes half about one, so each draws as its own read returns,
 * and the Indexes and coverage sections say "Loading" until theirs does. The page never asks for section "all".
 * get_store_maintenance_readiness is a third read and fills the verdict line. Refresh runs all three again.
 *
 * Measure runs measure_store_table for ONE plain table. Only one runs at a time, and the page shows how long it has run.
 *
 * No figure is worked out here. The read decides what a rewrite candidate is, what an estimate is and why one is missing.
 * The page words them. A missing estimate shows the reason the read gave (estimate_unavailable_text), never 0.
 *
 * The route is not polled (isNoPollRoute in app.js): a 60 second re-read would repeat a catalog read that takes seconds,
 * and the Refresh button on this page is the way to run it again.
 */

import { el, mount, loadingStrip, errorStrip, emptyStrip, noticeStrip, readTool, onPageLeave, fmtInt, fmtBytes, fmtPct } from "../util.js";
import { gridTable } from "../panels.js";
import { detailColumn, indexSkipWord } from "../pg-grid-text.js";
import { countWord, plainText } from "../plain-text.js";

/** Rows asked for per section. The read takes at most 1000. */
export const READ_TOP = 1000;

const KIND_LABELS = {
  table: "Table",
  partitioned: "Partitioned table",
  hypertable: "Hypertable",
  continuous_aggregate: "Continuous aggregate",
};

/** The sentence under the tables grid. */
export const ESTIMATE_NOTE = "Empty space is an estimate worked out from table statistics. Measure counts the pages of one table and gives a measured figure.";

/** The one line that replaces the coverage section when every partition has every index. */
export const COVERAGE_CLEAN_LINE = "Every partition of every partitioned table has each index of its parent, and none is invalid.";

const upperFirst = (s) => (s ? s.charAt(0).toUpperCase() + s.slice(1) : s);
const named = (r) => r.schema_name + "." + r.table_name;
const later = (a, b) => (a && b ? (Date.parse(a) >= Date.parse(b) ? a : b) : a || b || null);

/* ─────────────────────────── the words for one table row ─────────────────────────── */

/**
 * The derived, display-only fields of one table row. Every decision (candidate, past the floors, tail occupied) is the read's own.
 * Here they only get words.
 */
export function describeTable(r) {
  const out = { ...r };
  if (r.rewrite_candidate === true) {
    out.rewrite_text = "Rewrite candidate";
    out.rewrite_sev = "Critical";
    out.rewrite_rank = 3;
  } else if (r.past_rewrite_floors === true && r.tail_occupied === false) {
    out.rewrite_text = "VACUUM can shrink it";
    out.rewrite_sev = "Warning";
    out.rewrite_rank = 2;
  } else if (r.past_rewrite_floors === true) {
    out.rewrite_text = "Last pages not read";
    out.rewrite_sev = "Warning";
    out.rewrite_rank = 1;
  } else {
    out.rewrite_text = "No";
    out.rewrite_sev = null;
    out.rewrite_rank = 0;
  }
  if (r.past_rewrite_floors !== true) out.shrink_text = null;
  else if (r.tail_occupied === true) out.shrink_text = "No, a live row sits in the last pages";
  else if (r.tail_occupied === false) out.shrink_text = "Yes";
  else out.shrink_text = "Not known, the last pages could not be read";
  out.last_vacuum_at = later(r.last_vacuum, r.last_autovacuum);
  out.last_analyze_at = later(r.last_analyze, r.last_autoanalyze);
  out.vacuum_due_text = r.vacuum_trigger_rows == null ? null : Number(r.relations_past_vacuum_trigger) > 0 ? "Due" : "Not yet";
  out.autovacuum_text = r.autovacuum_disabled === true ? "Off" : "On";
  out.autovacuum_sev = r.autovacuum_disabled === true ? "Critical" : null;
  return out;
}

/** The kind column: "Hypertable, 441 chunks", "Partitioned table, 12 partitions", or the kind alone. */
function kindText(r) {
  const base = KIND_LABELS[r.kind] || String(r.kind ?? "");
  if (r.kind === "hypertable" && Number(r.chunk_count) > 0) return base + ", " + countWord(Number(r.chunk_count), "chunk");
  if (r.kind === "partitioned" && Number(r.leaf_count) > 0) return base + ", " + countWord(Number(r.leaf_count), "partition");
  return base;
}

/** What the empty-space share cell says: the estimate, or the read's own reason it has none. Never 0 for a missing estimate. */
export function emptyShareText(r) {
  if (r.estimated_empty_share != null) return fmtPct(Number(r.estimated_empty_share) * 100);
  return r.estimate_unavailable_text ? upperFirst(String(r.estimate_unavailable_text)) : "No estimate";
}

const dash = (v) => (v == null ? "—" : v);

export const TABLE_COLUMNS = [
  { key: "table_name", label: "Table", name: true, display: named, sortValue: named },
  { key: "kind", label: "Kind", display: kindText, sortValue: kindText },
  { key: "rewrite_text", label: "Rewrite", sevKey: "rewrite_sev", sortValue: (r) => r.rewrite_rank,
    hint: "Rewrite candidate: about half of the table or more is estimated empty, and that is a large amount of space. A live row in the last pages stops VACUUM from giving it back, so only a rewrite can." },
  { key: "total_bytes", label: "Total", format: "bytes" },
  { key: "heap_bytes", label: "Heap", format: "bytes", hint: "The table's own pages, without indexes and TOAST." },
  { key: "index_bytes", label: "Indexes", format: "bytes" },
  { key: "toast_bytes", label: "TOAST", format: "bytes" },
  { key: "estimated_empty_share", label: "Empty % (est.)", wrap: true, display: emptyShareText,
    sortValue: (r) => (r.estimated_empty_share == null ? null : Number(r.estimated_empty_share)),
    hint: "The estimated share of the table that is empty space. When no estimate can be made, the cell says why." },
  { key: "estimated_empty_bytes", label: "Empty (est.)", format: "bytes", hint: "The estimated empty space in bytes. Blank when no estimate can be made." },
  { key: "estimate_relations_usable", label: "Estimate covers",
    display: (r) => (Number(r.estimate_relations) > 1 ? r.estimate_relations_usable + " of " + r.estimate_relations + " parts" : "—"),
    sortValue: (r) => (Number(r.estimate_relations) > 1 ? Number(r.estimate_relations_usable) / Number(r.estimate_relations) : null),
    hint: "For a hypertable or a partitioned table: how many of its chunks or partitions had usable statistics." },
  { key: "shrink_text", label: "VACUUM can shrink it", wrap: true, display: (r) => dash(r.shrink_text),
    hint: "Only checked for a table with a lot of estimated empty space. A live row in the last pages means VACUUM cannot cut the file shorter." },
  { key: "estimated_rows", label: "Rows (est.)", format: "int" },
  { key: "measure", label: "Measure", sortable: false, render: (r) => measureCell(r), copyValue: () => "" },

  { key: "dead_rows", label: "Dead rows", format: "int", group: "Maintenance", hint: "Rows deleted or updated that vacuum has not cleaned up yet." },
  { key: "vacuum_trigger_rows", label: "Autovacuum starts at", format: "int", group: "Maintenance",
    hint: "The number of dead rows at which autovacuum starts, from the table's own settings or the server's." },
  { key: "vacuum_due_text", label: "Vacuum due", group: "Maintenance", display: (r) => dash(r.vacuum_due_text) },
  { key: "pct_all_visible", label: "All-visible pages %", group: "Maintenance", display: (r) => (r.pct_all_visible == null ? "—" : fmtPct(Number(r.pct_all_visible))),
    sortValue: (r) => (r.pct_all_visible == null ? null : Number(r.pct_all_visible)),
    hint: "How much of the table VACUUM marks all-visible. Index-only reads need this to be high." },
  { key: "last_vacuum_at", label: "Last vacuum", format: "time", group: "Maintenance" },
  { key: "last_analyze_at", label: "Last analyze", format: "time", group: "Maintenance" },
  { key: "autovacuum_text", label: "Autovacuum", sevKey: "autovacuum_sev", group: "Maintenance" },
  { key: "growth_bytes_7d", label: "7-day growth", format: "bytes", group: "Maintenance", hint: "Blank for a table the store does not record sizes for." },

  { key: "inserts_per_day", label: "Inserts per day", format: "int", group: "Churn" },
  { key: "updates_per_day", label: "Updates per day", format: "int", group: "Churn" },
  { key: "deletes_per_day", label: "Deletes per day", format: "int", group: "Churn" },
  { key: "hot_update_share", label: "HOT updates %", group: "Churn", display: (r) => (r.hot_update_share == null ? "—" : fmtPct(Number(r.hot_update_share) * 100)),
    sortValue: (r) => (r.hot_update_share == null ? null : Number(r.hot_update_share)),
    hint: "The share of updates that stayed on the same page and needed no new index entries." },
  { key: "turnover_per_day", label: "Deleted per day, % of rows", group: "Churn", display: (r) => (r.turnover_per_day == null ? "—" : fmtPct(Number(r.turnover_per_day) * 100)),
    sortValue: (r) => (r.turnover_per_day == null ? null : Number(r.turnover_per_day)),
    hint: "Deletes per day as a share of the live rows. 100% means the whole table is deleted and refilled each day." },
  { key: "stats_since", label: "Counted since", format: "time", group: "Churn",
    hint: "The counters above start here: the last statistics reset, or the last server start." },
];

/* ─────────────────────────── the indexes ─────────────────────────── */

/** The flags of one index row, as words. Empty when nothing is wrong. */
export function indexFlags(r) {
  const flags = [];
  if (r.valid === false) flags.push("Invalid");
  if (r.larger_than_heap === true) flags.push("Larger than its table");
  if (r.rarely_used === true) flags.push("Rarely used");
  return flags;
}

const indexFlagText = (r) => (indexFlags(r).join(", ") || "—");

export const INDEX_COLUMNS = [
  { key: "flags", label: "Flags", display: indexFlagText, sortValue: (r) => (r.valid === false ? 3 : r.larger_than_heap === true ? 2 : r.rarely_used === true ? 1 : 0),
    cellClass: (r) => (r.valid === false ? "sev-Critical" : indexFlags(r).length ? "sev-Warning" : null) },
  { key: "index_name", label: "Index", name: true },
  { key: "table_name", label: "Table", display: named, sortValue: named },
  { key: "kind", label: "Kind", display: (r) => KIND_LABELS[r.kind] || String(r.kind ?? "") },
  { key: "bytes", label: "Size", format: "bytes" },
  { key: "heap_bytes", label: "Table size", format: "bytes" },
  { key: "index_to_heap", label: "Index vs table %", display: (r) => (r.index_to_heap == null ? "—" : fmtPct(Number(r.index_to_heap) * 100)),
    sortValue: (r) => (r.index_to_heap == null ? null : Number(r.index_to_heap)),
    hint: "The index size as a share of its table's size. Over 100% means the index is larger than the table." },
  { key: "idx_scan", label: "Scans", format: "int", hint: "Reads that used the index since the counters began." },
  { key: "estimated_reclaimable_bytes", label: "Empty (est.)", format: "bytes", hint: "The estimated empty space in the index. Blank when no estimate can be made." },
  { key: "estimated_bloat_share", label: "Empty % (est.)", display: (r) => (r.estimated_bloat_share == null ? "—" : fmtPct(Number(r.estimated_bloat_share) * 100)),
    sortValue: (r) => (r.estimated_bloat_share == null ? null : Number(r.estimated_bloat_share)) },
  detailColumn("estimate_skipped_reason", "Not Estimated", indexSkipWord, "Why no empty-space figure could be estimated for this index. Blank means it was estimated."),
  { key: "index_count", label: "Copies", display: (r) => (Number(r.index_count) > 1 ? String(r.index_count) : "—"), sortValue: (r) => Number(r.index_count),
    hint: "For a hypertable or a partitioned table: how many chunk or partition indexes this row adds up." },
  { key: "is_primary_key", label: "Primary key", format: "bool" },
  { key: "is_unique", label: "Unique", format: "bool" },
];

/** The rows of the coverage grid: a parent index with a partition that lacks it or holds it invalid, or a parent index that is invalid. */
export function incompleteCoverage(rows) {
  return (Array.isArray(rows) ? rows : []).filter((c) => Number(c.leaves_missing_index) > 0 || Number(c.leaves_invalid_index) > 0 || c.parent_index_valid === false);
}

function leafList(names, total) {
  const list = Array.isArray(names) ? names : [];
  if (!list.length) return "—";
  const more = Number(total) - list.length;
  return list.join(", ") + (more > 0 ? " and " + more + " more" : "");
}

export const COVERAGE_COLUMNS = [
  { key: "table_name", label: "Partitioned table", name: true, display: named, sortValue: named },
  { key: "index_name", label: "Index" },
  { key: "parent_index_valid", label: "Index valid", format: "bool" },
  { key: "leaf_count", label: "Partitions", format: "int" },
  { key: "leaves_with_index", label: "With the index", format: "int" },
  { key: "leaves_missing_index", label: "Missing it", format: "int", cellClass: (r) => (Number(r.leaves_missing_index) > 0 ? "sev-Critical" : null) },
  { key: "missing_leaves", label: "Missing partitions", clamp: true, display: (r) => leafList(r.missing_leaves, r.leaves_missing_index), sortable: false },
  { key: "leaves_invalid_index", label: "Invalid", format: "int", cellClass: (r) => (Number(r.leaves_invalid_index) > 0 ? "sev-Critical" : null) },
  { key: "invalid_leaves", label: "Invalid partitions", clamp: true, display: (r) => leafList(r.invalid_leaves, r.leaves_invalid_index), sortable: false },
];

/* ─────────────────────────── page state ─────────────────────────── */

let loadSeq = 0;
let pageAbort = null;
/* The Measure state. `measuring` is the table being measured, or null. One at a time, so every Measure button is disabled while it is set.
   `measureButtons` are the buttons of the grid on screen. */
let measuring = null;
let measureSeq = 0;
let measureAbort = null;
let measureTimer = null;
let measureBox = null;
let measureButtons = [];

function stopMeasureTimer() {
  if (measureTimer !== null) {
    clearInterval(measureTimer);
    measureTimer = null;
  }
}

/* Leaving the page aborts its reads, and a Measure left running does not hold the next visit's buttons. */
onPageLeave(() => {
  if (pageAbort) pageAbort.abort();
  if (measureAbort) measureAbort.abort();
  stopMeasureTimer();
  measuring = null;
  measureSeq++;
  measureBox = null;
  measureButtons = [];
});

function applyMeasureLock() {
  for (const b of measureButtons) {
    b.disabled = measuring !== null;
    if (measuring !== null) b.setAttribute("disabled", "");
    else if (typeof b.removeAttribute === "function") b.removeAttribute("disabled");
  }
}

/* ─────────────────────────── the page ─────────────────────────── */

export function renderStoreHealth(main) {
  const updated = el("div", { class: "meta", text: "" });
  const refresh = el("button", { class: "btn", type: "button", text: "Refresh" });
  const headerBox = el("div", {});
  const readinessBox = el("div", {});
  const tablesBox = el("div", {});
  const measurePanel = el("div", {});
  const indexesBox = el("div", {});
  const coverageBox = el("div", {});

  measureBox = measurePanel;
  measureButtons = [];

  const page = el("div", { class: "store-health" }, [
    el("div", { class: "page-head" }, [
      el("h2", { text: "Store Health" }),
      el("div", { class: "spacer" }),
      updated,
      refresh,
    ]),
    headerBox,
    readinessBox,
    el("h3", { class: "section-title", text: "Tables" }),
    tablesBox,
    measurePanel,
    el("h3", { class: "section-title", text: "Indexes" }),
    indexesBox,
    coverageBox,
  ]);
  mount(main, page);

  const load = () => {
    const seq = ++loadSeq;
    if (pageAbort) pageAbort.abort();
    pageAbort = new AbortController();
    const signal = pageAbort.signal;
    const live = () => seq === loadSeq;
    measureButtons = [];

    refresh.disabled = true;
    refresh.setAttribute("disabled", "");
    mount(headerBox, loadingStrip("Loading tables…"));
    mount(readinessBox, loadingStrip("Checking whether the store is safe for maintenance…"));
    mount(tablesBox, loadingStrip("Loading tables…"));
    mount(indexesBox, loadingStrip("Loading indexes…"));
    mount(coverageBox, loadingStrip("Loading partition index coverage…"));

    const guard = (box, draw) => (res) => {
      if (!live()) return;
      try {
        draw(res);
      } catch (e) {
        mount(box, errorStrip("The page could not draw this section: " + (e && e.message ? e.message : String(e))));
      }
    };

    const tables = readTool("get_store_table_health", { section: "tables", top: READ_TOP }, signal)
      .then(guard(tablesBox, (res) => {
        const d = drawTables(res, headerBox, tablesBox);
        if (d) updated.textContent = "Read at " + new Date().toLocaleTimeString();
      }));
    const indexes = readTool("get_store_table_health", { section: "indexes", top: READ_TOP }, signal)
      .then(guard(indexesBox, (res) => drawIndexes(res, indexesBox, coverageBox)));
    const readiness = readTool("get_store_maintenance_readiness", {}, signal)
      .then(guard(readinessBox, (res) => drawReadiness(res, readinessBox)));

    Promise.allSettled([tables, indexes, readiness]).then(() => {
      if (!live()) return;
      refresh.disabled = false;
      refresh.removeAttribute("disabled");
    });
  };

  refresh.addEventListener("click", load);
  load();
}

/** Nothing to draw for a read the page left behind or a session that ended; true when the answer is data to draw. */
function usable(res) {
  return !!res && res.kind !== "aborted" && res.kind !== "auth";
}

/** The strip for a read that did not return data, or null when the answer is data. */
function failure(res) {
  if (res.kind === "error") return errorStrip(res.message);
  if (res.kind === "empty") return emptyStrip(res.message);
  return res.kind === "data" ? null : errorStrip("The store did not answer.");
}

/* ─────────────────────────── the header and the tables ─────────────────────────── */

function tile(num, label, sub, cls) {
  return el("div", { class: "tile" + (cls ? " " + cls : "") }, [
    el("div", { class: "num", text: num }),
    el("div", { class: "lbl", text: label }),
    sub ? el("div", { class: "sub", text: sub }) : null,
  ]);
}

/** Draws the header and the tables grid from the tables read. Returns the answer's data, or null when there was none. */
function drawTables(res, headerBox, tablesBox) {
  if (!usable(res)) return null;
  const bad = failure(res);
  if (bad) {
    mount(headerBox, []);
    mount(tablesBox, bad);
    return null;
  }
  const d = res.data;
  const rows = (Array.isArray(d.tables) ? d.tables : []).map(describeTable);
  const candidates = Number(d.rewrite_candidates) || 0;
  const noEstimate = Number(d.tables_without_estimate) || 0;
  const unread = rows.filter((r) => r.past_rewrite_floors === true && r.tail_occupied == null).length;

  const head = [
    el("div", { class: "rollup" }, [el("div", { class: "rollup-group" }, [
      tile(fmtBytes(d.store_total_bytes), "Store tables and indexes", countWord(Number(d.store_tables) || 0, "table")),
      tile(fmtInt(candidates), "Rewrite candidates", candidates > 0 ? "VACUUM cannot return the space" : "none", candidates > 0 ? "critical" : "healthy"),
      tile(fmtInt(noEstimate), "Tables with no usable estimate", noEstimate > 0 ? "empty space not known" : "every table has one", noEstimate > 0 ? "warning" : "healthy"),
    ])]),
  ];
  const reasons = d.without_estimate_by_reason && typeof d.without_estimate_by_reason === "object" ? Object.entries(d.without_estimate_by_reason) : [];
  if (reasons.length) {
    const text = d.without_estimate_reason_text || {};
    head.push(el("div", { class: "meta" }, ["No usable estimate:"]));
    head.push(el("ul", { class: "meta" }, reasons.map(([code, n]) => el("li", { text: upperFirst(String(text[code] || code)) + ": " + fmtInt(n) }))));
  }
  if (unread > 0) {
    head.push(noticeStrip(countWord(unread, "table") + " had a lot of estimated empty space, but the last pages could not be read, so " + (unread === 1 ? "it is" : "they are") + " not flagged as a rewrite candidate."));
  }
  mount(headerBox, head);

  const parts = [];
  if (d.truncated === true) parts.push(noticeStrip("The grid lists " + fmtInt(d.tables_returned) + " of the " + fmtInt(d.store_tables) + " tables."));
  parts.push(gridTable(rows, {
    id: "store-health|tables",
    title: "Store tables",
    columns: TABLE_COLUMNS,
    groups: ["Maintenance", "Churn"],
    defaultGroups: ["Maintenance"],
    emptyText: "The store has no tables to show.",
  }));
  parts.push(el("div", { class: "meta store-health-note", text: ESTIMATE_NOTE }));
  mount(tablesBox, parts);
  applyMeasureLock();
  return d;
}

/* ─────────────────────────── the indexes and the coverage ─────────────────────────── */

function drawIndexes(res, indexesBox, coverageBox) {
  if (!usable(res)) return;
  const bad = failure(res);
  if (bad) {
    mount(indexesBox, bad);
    mount(coverageBox, []);
    return;
  }
  const d = res.data;
  const rows = Array.isArray(d.indexes) ? d.indexes : [];
  const total = Number(d.indexes_total) || rows.length;
  const invalid = rows.filter((r) => r.valid === false).length;
  const larger = rows.filter((r) => r.larger_than_heap === true).length;
  const rarely = rows.filter((r) => r.rarely_used === true).length;

  const parts = [
    el("div", { class: "meta", text: total + (total === 1 ? " index" : " indexes") + ": " + fmtInt(invalid) + " invalid, " + fmtInt(larger) + " larger than their table, " + fmtInt(rarely) + " rarely used. Flagged indexes come first." }),
  ];
  if (d.indexes_truncated === true) parts.push(noticeStrip("The grid lists " + fmtInt(d.indexes_returned) + " of the " + fmtInt(total) + " indexes."));
  parts.push(gridTable(rows, {
    id: "store-health|indexes",
    title: "Store indexes",
    columns: INDEX_COLUMNS,
    emptyText: "The store has no indexes to show.",
  }));
  mount(indexesBox, parts);

  const incomplete = incompleteCoverage(d.partition_index_coverage);
  if (!incomplete.length) {
    mount(coverageBox, el("div", { class: "meta", text: COVERAGE_CLEAN_LINE }));
    return;
  }
  mount(coverageBox, [
    el("h3", { class: "section-title", text: "Partition index coverage" }),
    el("div", { class: "meta", text: "These partitioned tables have partitions that miss an index of their parent or hold it invalid." }),
    gridTable(incomplete, { id: "store-health|coverage", title: "Partition index coverage", columns: COVERAGE_COLUMNS }),
  ]);
}

/* ─────────────────────────── maintenance readiness ─────────────────────────── */

function drawReadiness(res, box) {
  if (!usable(res)) return;
  const bad = failure(res);
  if (bad) {
    mount(box, bad);
    return;
  }
  const d = res.data;
  const reasons = Array.isArray(d.reasons) ? d.reasons : [];
  if (d.safe === true) {
    mount(box, el("div", { class: "strip ok", role: "status" }, [plainText(String(d.verdict || "Safe for maintenance now."))]));
    return;
  }
  mount(box, el("div", { class: "strip notice", role: "status" }, [
    el("div", { text: "Not safe for maintenance now." }),
    reasons.length ? el("ul", {}, reasons.map((r) => el("li", { text: plainText(String(r)) }))) : el("div", { text: plainText(String(d.verdict || "")) }),
  ]));
}

/* ─────────────────────────── Measure ─────────────────────────── */

function measureCell(r) {
  if (r.kind !== "table") return el("span", { class: "meta", text: "—", title: "Measure works on one plain table. A hypertable or a partitioned table is measured through its chunks or partitions." });
  const name = named(r);
  const btn = el("button", { class: "btn small", type: "button", text: "Measure", title: "Count the pages of " + name + " that hold a live row. This reads every row location of the table." });
  btn.addEventListener("click", () => measureTable(name));
  measureButtons.push(btn);
  if (measuring !== null) {
    btn.disabled = true;
    btn.setAttribute("disabled", "");
  }
  return btn;
}

function elapsedText(startedAt) {
  return Math.max(0, Math.round((Date.now() - startedAt) / 1000)) + " s";
}

/** Runs measure_store_table for one table and shows the result in the panel under the tables grid. */
export async function measureTable(table) {
  if (measuring !== null || !measureBox) return;
  const box = measureBox;
  const seq = ++measureSeq;
  const startedAt = Date.now();
  measuring = table;
  applyMeasureLock();
  if (measureAbort) measureAbort.abort();
  measureAbort = new AbortController();

  const running = () => el("div", { class: "strip loading", role: "status" }, ["Measuring " + table + ". It has run for " + elapsedText(startedAt) + ". One table is measured at a time."]);
  mount(box, running());
  stopMeasureTimer();
  measureTimer = setInterval(() => { if (seq === measureSeq) mount(box, running()); }, 1000);

  let res;
  try {
    res = await readTool("measure_store_table", { table }, measureAbort.signal, { heavy: true });
  } finally {
    if (seq === measureSeq) {
      stopMeasureTimer();
      measuring = null;
      applyMeasureLock();
    }
  }
  if (seq !== measureSeq || !usable(res)) return;
  const took = "Took " + elapsedText(startedAt) + ".";
  if (res.kind === "data") mount(box, measureResult(table, res.data, took));
  else if (res.kind === "empty") mount(box, noticeStrip("Measuring " + table + " did not finish. " + res.message));
  else mount(box, errorStrip("Could not measure " + table + ". " + (res.message || "The store did not answer.")));
}

/** When VACUUM can give the space back, from the measure's own fields. */
export function truncateText(m) {
  if (m.vacuum_shrink_disabled === true) return "No. VACUUM shrinking is turned off for this table, so only a rewrite returns the space.";
  const trailing = Number(m.trailing_empty_pages) || 0;
  if (trailing > 0) return "Yes, for the last " + countWord(trailing, "page") + " (" + fmtBytes(trailing * 8192) + "). Space in front of a live row can only come back by a rewrite.";
  if (Number(m.empty_pages) > 0) return "No. A live row sits in the last pages, so only a rewrite returns the empty space.";
  return "There is no empty page to give back.";
}

function measureResult(table, m, took) {
  const pages = Number(m.total_pages) || 0;
  const occ = m.occupancy && typeof m.occupancy === "object" ? m.occupancy : {};
  const buckets = [
    ["1 row", occ.pages_with_1_row],
    ["2 to 4 rows", occ.pages_with_2_to_4_rows],
    ["5 to 10 rows", occ.pages_with_5_to_10_rows],
    ["Over 10 rows", occ.pages_with_over_10_rows],
  ];
  const fact = (label, value) => el("tr", {}, [el("th", { text: label }), el("td", { text: value })]);
  const histogram = el("table", { class: "data" }, [
    el("thead", {}, [el("tr", {}, [el("th", { text: "Rows on the page" }), el("th", { text: "Pages" }), el("th", { text: "Share of pages" })])]),
    el("tbody", {}, buckets.map(([label, n]) => el("tr", {}, [
      el("td", { text: label }),
      el("td", { class: "num", text: fmtInt(n) }),
      el("td", { class: "num", text: pages > 0 && n != null ? fmtPct((Number(n) / pages) * 100) : "—" }),
    ]))),
  ]);
  return el("div", { class: "card store-health-measure" }, [
    el("h4", { text: "Measured: " + table }),
    el("div", { text: plainText(String(m.verdict || "")) }),
    el("table", { class: "kv" }, [el("tbody", {}, [
      fact("Empty pages", fmtInt(m.empty_pages) + " of " + fmtInt(pages) + " (" + (m.empty_share == null ? "—" : fmtPct(Number(m.empty_share) * 100)) + ")"),
      fact("Rows per page that holds rows", m.rows_per_used_page == null ? "—" : Number(m.rows_per_used_page).toLocaleString(undefined, { maximumFractionDigits: 1 })),
      fact("Rows", fmtInt(m.rows)),
      fact("Can VACUUM shrink the file?", truncateText(m)),
    ])]),
    histogram,
    el("div", { class: "meta", text: took + " A page that holds only dead rows counts as empty, because VACUUM frees it." }),
  ]);
}
