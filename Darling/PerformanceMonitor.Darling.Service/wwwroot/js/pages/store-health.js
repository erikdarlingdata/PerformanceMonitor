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
 * get_store_maintenance_readiness is a third read and fills the verdict line. Refresh asks for all three again.
 *
 * The service reads the store's catalogs for the tables and the indexes at most once in a few minutes and holds each answer
 * in between (#8924), so a refresh inside that time gets the same read back. The page never stamps such an answer with the
 * time it arrived: the line beside Refresh and the line under the index summary show when the store answered
 * (tables_as_of, indexes_as_of) and how long ago that was (the _age_seconds beside each).
 *
 * Autovacuum: the tables read also says, per table, how many of its autovacuums the store log saw cancelled in the last 24 hours
 * and whether autovacuum is behind on it (a relation past its trigger with no vacuum completed in 24 hours). Tables behind come
 * first in the read's order. The header shows the store's autovacuum settings as they apply and the write budget they allow,
 * beside the store size (autovacuum_settings). The read decides what is behind and what is cancelled repeatedly. The page words it.
 *
 * Measure runs measure_store_table for ONE plain table. Only one runs at a time, and the page shows how long it has run.
 *
 * No figure is worked out here. The read decides what a rewrite candidate is, what an estimate is and why one is missing.
 * The page words them. A missing estimate shows the reason the read gave (estimate_unavailable_text), never 0.
 *
 * The route is not polled (isNoPollRoute in app.js): a 60 second re-read would repeat a catalog read that takes seconds,
 * and the Refresh button on this page is the way to run it again.
 */

import { el, mount, loadingStrip, errorStrip, emptyStrip, noticeStrip, readTool, onPageLeave, fmtInt, fmtBytes, fmtPct, rollupTextId, localTime } from "../util.js";
import { gridTable } from "../panels.js";
import { detailColumn, firstSentence } from "../pg-grid-text.js";
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
/** Under the grid when the statistics were never reset: why the per-day columns are empty (#8823). */
export const RATES_NOTE = "The per-day columns are empty because the statistics were never reset. The counters are older than the last start, so their age is not known. A statistics reset gives rates.";

export const ESTIMATE_NOTE = "Empty space is an estimate worked out from table statistics. Measure reads one table and gives a measured figure.";

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
    out.rewrite_text = "End not checked";
    out.rewrite_sev = "Warning";
    out.rewrite_rank = 1;
  } else {
    out.rewrite_text = "No";
    out.rewrite_sev = null;
    out.rewrite_rank = 0;
  }
  if (r.past_rewrite_floors !== true) out.shrink_text = null;
  else if (r.tail_occupied === true) out.shrink_text = "No, VACUUM cannot cut it shorter";
  else if (r.tail_occupied === false) out.shrink_text = "Yes";
  else out.shrink_text = "Not checked";
  out.last_vacuum_at = later(r.last_vacuum, r.last_autovacuum);
  out.last_analyze_at = later(r.last_analyze, r.last_autoanalyze);
  out.vacuum_due_text = r.vacuum_trigger_rows == null ? null : Number(r.relations_past_vacuum_trigger) > 0 ? "Due" : "Not yet";
  out.autovacuum_text = r.autovacuum_disabled === true ? "Off" : "On";
  out.autovacuum_sev = r.autovacuum_disabled === true ? "Critical" : null;
  if (r.behind === true) {
    out.behind_text = r.behind_hours == null ? "Behind" : "Behind, no vacuum in " + hoursText(Number(r.behind_hours));
    out.behind_sev = "Critical";
    out.behind_rank = 1 + (Number(r.behind_hours) || 0);
  } else {
    out.behind_text = "No";
    out.behind_sev = null;
    out.behind_rank = 0;
  }
  out.cancels_sev = r.repeatedly_cancelled === true ? "Warning" : null;
  return out;
}

/** A number of hours in whole hours up to three days, then in whole days: "30 h", "4 days". */
export function hoursText(hours) {
  if (!Number.isFinite(hours)) return "—";
  return hours < 72 ? fmtInt(Math.floor(hours)) + " h" : fmtInt(Math.floor(hours / 24)) + " days";
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

/** The Counted since cell. A statistics reset dates the counters. With none, the read says the counters began before the last start (#8823). */
export function countedSinceText(r) {
  if (!r.stats_since) return "—";
  const at = localTime(r.stats_since);
  return r.stats_since_source === "postmaster_start" ? "before " + at : at;
}

export const TABLE_COLUMNS = [
  { key: "table_name", label: "Table", name: true, display: named, sortValue: named },
  { key: "kind", label: "Kind", display: kindText, sortValue: kindText },
  { key: "behind_text", label: "Autovacuum behind", sevKey: "behind_sev", sortValue: (r) => r.behind_rank,
    hint: "Behind: a part of the table is past the point where autovacuum starts (its dead rows, or for a plain table its new rows) and no vacuum, automatic or manual, has finished on it in 24 hours. The time is since its last vacuum, or since the statistics began when it has had none. It crossed the point some time after that. A table with autovacuum off is never behind." },
  { key: "rewrite_text", label: "Rewrite", sevKey: "rewrite_sev", sortValue: (r) => r.rewrite_rank,
    hint: "Rewrite candidate: about half of the table or more is estimated empty, and that is a large amount of space. VACUUM gives space back only by cutting empty pages off the end of the file. A live row near the end, or a table set not to truncate, means only a rewrite can." },
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
    hint: "Only checked for a table with a lot of estimated empty space. No means a live row sits near the end of the file, or the table is set not to truncate. VACUUM cannot cut it shorter then. Not checked means the end of the file was too large to read, or the read failed. For a hypertable it can also mean more mostly empty parts than are checked." },
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
  { key: "autovacuum_cancels_24h", label: "Autovacuum cancels (24 h)", format: "int", sevKey: "cancels_sev", group: "Maintenance",
    hint: "How many times the store log saw an autovacuum of this table cancelled in the last 24 hours, its TOAST table's and its chunks' or partitions' included. Two or more is flagged. A lock request that waits on autovacuum cancels it, so the work starts over." },
  { key: "last_autovacuum_cancel", label: "Last cancel", format: "time", group: "Maintenance" },
  { key: "relations_past_trigger", label: "Parts past the start point", format: "int", group: "Maintenance",
    hint: "How many parts of the table (the table itself, or its chunks or partitions) are past the point where autovacuum starts, by dead rows or, for a plain table, by new rows." },
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
  { key: "stats_since", label: "Counted since", group: "Churn", display: countedSinceText, sortValue: (r) => (r.stats_since ? Date.parse(r.stats_since) : null),
    hint: "The counters above start at the last statistics reset. With no reset, statistics outlive a server restart, so they started before the last start and their age is not known. The per-day columns are then empty." },
];

/* ─────────────────────────── the indexes ─────────────────────────── */

/** The flags of one index row, as words. Empty when nothing is wrong. */
export function indexFlags(r) {
  const flags = [];
  if (r.valid === false) flags.push("Invalid");
  if (r.larger_than_heap === true) {
    /* A rolled-up row's flag is about its copies, not the sum the ratio beside it is of, so it always says how many, even one (#8897). */
    const copies = Number(r.index_count);
    flags.push(copies > 1 ? Number(r.larger_than_heap_count) + " of " + copies + " copies larger than its table" : "Larger than its table");
  }
  if (r.rarely_used === true) flags.push("Rarely used");
  return flags;
}

const indexFlagText = (r) => (indexFlags(r).join(", ") || "—");

/**
 * The share is of the size the estimate covered. When only some of a rolled-up row's copies have an estimate, that is less than the row's
 * Size, so the cell names its basis instead of sitting a bare 100% beside a bigger size (#8897). A row whose copies were all estimated keeps
 * the plain percent: the estimate and the Size are both pg_relation_size read in one statement, so the basis equals the Size.
 */
export function indexEmptyShareText(r) {
  if (r.estimated_bloat_share == null) return "—";
  const pct = fmtPct(Number(r.estimated_bloat_share) * 100);
  const partial = r.estimated_basis_bytes != null && Number(r.estimate_indexes_usable) < Number(r.index_count) && Number(r.estimated_basis_bytes) < Number(r.bytes);
  return partial ? pct + " of the " + fmtBytes(Number(r.estimated_basis_bytes)) + " estimated" : pct;
}

/** The read gives the reason an index has no estimate as one plain sentence. The grid shows it in a few words and opens the sentence on a click. */
const INDEX_SKIP_WORDS = [
  [/^The index covers only some rows/, "Partial index"],
  [/^The table has never been analyzed/, "Table not analyzed"],
  [/^A key column uses the name type/, "Key column type"],
  [/^The column sizes are not visible/, "No column sizes"],
  [/^The index holds no pages yet/, "Empty index"],
  [/^The estimate came out below zero/, "Estimate below zero"],
  [/^The index is not valid or not ready/, "Not valid or not ready"],
  [/^The estimate covers btree indexes only/, "Not a btree index"],
];

export function indexSkipText(text) {
  if (typeof text !== "string" || text === "") return null;
  for (const [re, word] of INDEX_SKIP_WORDS) if (re.test(text)) return word;
  return firstSentence(text);
}

export const INDEX_COLUMNS = [
  { key: "flags", label: "Flags", display: indexFlagText, sortValue: (r) => (r.valid === false ? 3 : r.larger_than_heap === true ? 2 : r.rarely_used === true ? 1 : 0),
    cellClass: (r) => (r.valid === false ? "sev-Critical" : indexFlags(r).length ? "sev-Warning" : null) },
  { key: "index_name", label: "Index", name: true },
  { key: "table_name", label: "Table", display: named, sortValue: named },
  { key: "kind", label: "Kind", group: "More", display: (r) => KIND_LABELS[r.kind] || String(r.kind ?? "") },
  { key: "bytes", label: "Size", format: "bytes" },
  { key: "heap_bytes", label: "Table size", group: "More", format: "bytes" },
  { key: "index_to_heap", label: "Index vs table %", display: (r) => (r.index_to_heap == null ? "—" : fmtPct(Number(r.index_to_heap) * 100)),
    sortValue: (r) => (r.index_to_heap == null ? null : Number(r.index_to_heap)),
    hint: "The index size as a share of its table's size. Over 100% means the index is larger than the table." },
  { key: "idx_scan", label: "Scans", format: "int", hint: "Reads that used the index since the counters began." },
  { key: "estimated_reclaimable_bytes", label: "Empty (est.)", format: "bytes", hint: "The estimated empty space in the index. Blank when no estimate can be made." },
  { key: "estimated_bloat_share", label: "Empty % (est.)", display: indexEmptyShareText,
    sortValue: (r) => (r.estimated_bloat_share == null ? null : Number(r.estimated_bloat_share)) },
  detailColumn("estimate_skipped_reason", "Not Estimated", indexSkipText, "Why no empty-space figure could be estimated for this index. Blank means it was estimated."),
  { key: "index_count", label: "Copies", group: "More", display: (r) => (Number(r.index_count) > 1 ? String(r.index_count) : "—"), sortValue: (r) => Number(r.index_count),
    hint: "For a hypertable or a partitioned table: how many chunk or partition indexes this row adds up." },
  { key: "is_primary_key", label: "Primary key", group: "More", format: "bool" },
  { key: "is_unique", label: "Unique", group: "More", format: "bool" },
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
        if (d) {
          updated.textContent = readAtLine("Tables", d.tables_as_of, d.tables_age_seconds);
          const held = heldLine(d.held_for_seconds);
          if (held) updated.setAttribute("title", held);
        }
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

/** A length of time in the largest whole unit that fits: "40 s", "4 min", "2 h". */
function spanText(seconds) {
  if (seconds < 60) return fmtInt(Math.floor(seconds)) + " s";
  if (seconds < 3600) return fmtInt(Math.floor(seconds / 60)) + " min";
  return fmtInt(Math.floor(seconds / 3600)) + " h";
}

/**
 * When the store answered one part of the read: "Tables read 11/17/2026, 3:11:02 PM (4 min ago)". The time is the answer's own
 * as-of time, never the time the answer reached the page, and the age is the answer's own figure. Empty when the answer carries no time.
 */
export function readAtLine(part, asOf, ageSeconds) {
  if (typeof asOf !== "string" || !asOf) return "";
  const age = Number(ageSeconds);
  return part + " read " + localTime(asOf) + (Number.isFinite(age) && age >= 1 ? " (" + spanText(age) + " ago)" : "");
}

/** Why a refresh can show the same read: how long the service holds one. Empty when the answer does not say. */
export function heldLine(heldForSeconds) {
  const held = Number(heldForSeconds);
  return Number.isFinite(held) && held > 0
    ? "The store's tables and indexes are read at most once every " + spanText(held) + ". A refresh inside that time shows the same read; after it, a refresh shows the last read at once and a new read runs behind it."
    : "";
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

/** One figure tile. The number points at its label and sub-line, so a screen reader reads the figure with its words. `used` keeps the ids unique within one draw. */
function tile(num, label, sub, cls, used) {
  const lblId = rollupTextId(label, "lbl", used);
  const subId = sub ? rollupTextId(label, "sub", used) : null;
  return el("div", { class: "tile" + (cls ? " " + cls : "") }, [
    el("div", { class: "num", text: num, "aria-describedby": subId ? lblId + " " + subId : lblId }),
    el("div", { class: "lbl", id: lblId, text: label }),
    sub ? el("div", { class: "sub", id: subId, text: sub }) : null,
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

  const behind = Number(d.tables_behind) || 0;
  const cancelled = Number(d.tables_repeatedly_cancelled) || 0;
  const av = d.autovacuum_settings && typeof d.autovacuum_settings === "object" ? d.autovacuum_settings : null;

  const usedIds = new Set();
  const head = [
    el("div", { class: "rollup" }, [el("div", { class: "rollup-group" }, [
      tile(fmtBytes(d.store_total_bytes), "Store tables and indexes", countWord(Number(d.store_tables) || 0, "table"), undefined, usedIds),
      av ? tile(writeBudgetText(av), "Autovacuum write budget", writeBudgetSub(av), undefined, usedIds) : null,
      tile(fmtInt(behind), behind === 1 ? "Table behind on autovacuum" : "Tables behind on autovacuum", behind > 0 ? "past the start point, no vacuum in 24 hours" : "none", behind > 0 ? "critical" : "healthy", usedIds),
      tile(fmtInt(cancelled), cancelled === 1 ? "Table with autovacuum cancelled repeatedly" : "Tables with autovacuum cancelled repeatedly", cancelled > 0 ? "2 or more cancels in 24 hours" : "none", cancelled > 0 ? "warning" : "healthy", usedIds),
      tile(fmtInt(candidates), candidates === 1 ? "Rewrite candidate" : "Rewrite candidates", candidates > 0 ? "VACUUM cannot return the space" : "none", candidates > 0 ? "critical" : "healthy", usedIds),
      tile(fmtInt(noEstimate), noEstimate === 1 ? "Table with no usable estimate" : "Tables with no usable estimate", noEstimate > 0 ? "empty space not known" : "every table has one", noEstimate > 0 ? "warning" : "healthy", usedIds),
    ])]),
  ];
  if (av) head.push(el("div", { class: "meta store-health-autovacuum" }, [autovacuumSettingsLine(av)]));
  else if (typeof d.autovacuum_settings_left_out_note === "string" && d.autovacuum_settings_left_out_note) {
    head.push(noticeStrip("The autovacuum settings were left out. " + d.autovacuum_settings_left_out_note));
  }
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
  if (d.rates_known === false) parts.push(el("div", { class: "meta store-health-note", text: RATES_NOTE }));
  mount(tablesBox, parts);
  applyMeasureLock();
  return d;
}

/** The write budget tile's figure: the most all autovacuum workers together may dirty per second, or "Unthrottled". */
export function writeBudgetText(s) {
  if (s.write_budget_mb_per_s == null) return "Unthrottled";
  return Number(s.write_budget_mb_per_s).toLocaleString(undefined, { maximumFractionDigits: 1 }) + " MB/s";
}

function writeBudgetSub(s) {
  if (s.write_budget_mb_per_s == null) return "cost delay 0, so vacuum does not sleep";
  return "the most autovacuum may write per second";
}

/** A memory setting in kB as words: "64 MB", "1 GB". */
function memText(kb) {
  const n = Number(kb);
  return Number.isFinite(n) && n >= 0 ? fmtBytes(n * 1024) : "—";
}

/**
 * The store's autovacuum settings as one line, from the read's own figures. A setting that falls back to another (an
 * autovacuum_ setting of -1) names the one in force.
 */
export function autovacuumSettingsLine(s) {
  const from = (setting, own) => (setting && setting !== own ? " (from " + setting + ")" : "");
  const parts = [
    (s.autovacuum_on === false ? "Autovacuum is off" : "Autovacuum is on") + ": " + fmtInt(s.workers_running) + " of " + fmtInt(s.max_workers) + " workers running",
    "it looks for work every " + spanText(Number(s.naptime_seconds) || 0),
    "cost limit " + fmtInt(s.cost_limit) + from(s.cost_limit_setting, "autovacuum_vacuum_cost_limit"),
    "cost delay " + Number(s.cost_delay_ms).toLocaleString(undefined, { maximumFractionDigits: 2 }) + " ms" + from(s.cost_delay_setting, "autovacuum_vacuum_cost_delay"),
    memText(s.work_mem_kb) + " of memory per worker" + from(s.work_mem_setting, "autovacuum_work_mem"),
  ];
  const cancels = Number(s.cancels_24h) || 0;
  const unresolved = Number(s.cancels_unresolved) || 0;
  const cancelText = cancels === 0
    ? "The store log has no cancelled autovacuum in the last 24 hours."
    : "The store log has " + countWord(cancels, "cancelled autovacuum") + " in the last 24 hours"
      + (unresolved > 0 ? ", " + fmtInt(unresolved) + " for a relation the store no longer has or one in another database." : ".");
  return parts.join(", ") + ". " + cancelText;
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
  const readAt = readAtLine("Indexes", d.indexes_as_of, d.indexes_age_seconds);
  if (readAt) parts.push(el("div", { class: "meta", text: readAt + "." }));
  if (d.indexes_truncated === true) parts.push(noticeStrip("The grid lists " + fmtInt(d.indexes_returned) + " of the " + fmtInt(total) + " indexes."));
  parts.push(gridTable(rows, {
    id: "store-health|indexes",
    title: "Store indexes",
    columns: INDEX_COLUMNS,
    groups: ["More"],
    defaultGroups: [],
    emptyText: "The store has no indexes to show.",
  }));
  mount(indexesBox, parts);

  /* The coverage read ran past its limit: the index rows above stand, and the coverage section says it was left out, never that every partition is fine. */
  if (typeof d.coverage_left_out_note === "string" && d.coverage_left_out_note) {
    mount(coverageBox, [
      el("h3", { class: "section-title", text: "Partition index coverage" }),
      noticeStrip("The partition index coverage was left out. " + d.coverage_left_out_note),
    ]);
    return;
  }
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
  const btn = el("button", { class: "btn small", type: "button", text: "Measure", title: "Measure how much of the pages of " + name + " its live rows fill. This reads the whole table once, but not its large stored values." });
  btn.addEventListener("click", () => measureTable(name));
  measureButtons.push(btn);
  if (measuring !== null) {
    btn.disabled = true;
    btn.setAttribute("disabled", "");
  }
  return btn;
}

function elapsedText(startedAt) {
  /* Tenths under 10 s, so a quick measure does not read "Took 0 s". */
  const seconds = Math.max(0, (Date.now() - startedAt) / 1000);
  return (seconds < 10 ? seconds.toFixed(1) : String(Math.round(seconds))) + " s";
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

/** What the live rows take and what they leave free, from the measure's own fields. */
export function freeText(m) {
  if (m.live_bytes == null || m.free_bytes == null) return "—";
  const ff = Number(m.fillfactor);
  const reserve = ff > 0 && ff < 100 ? ", beyond the " + ff + "% fillfactor the table keeps free on purpose" : "";
  return fmtBytes(m.live_bytes) + " used by live rows, " + fmtBytes(m.free_bytes) + " free (" + (m.free_share == null ? "—" : fmtPct(Number(m.free_share) * 100)) + ")" + reserve + ".";
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
      fact("Space in the pages", freeText(m)),
      fact("Empty pages", fmtInt(m.empty_pages) + " of " + fmtInt(pages) + " (" + (m.empty_share == null ? "—" : fmtPct(Number(m.empty_share) * 100)) + ")"),
      fact("Rows per page that holds rows", m.rows_per_used_page == null ? "—" : Number(m.rows_per_used_page).toLocaleString(undefined, { maximumFractionDigits: 1 })),
      fact("Rows", fmtInt(m.rows)),
      fact("Can VACUUM shrink the file?", truncateText(m)),
    ])]),
    histogram,
    el("div", { class: "meta", text: took + " Free space is worked out from the size of every live row. Column padding is not counted, so it reads a little high. A page that holds only dead rows counts as empty, because VACUUM frees it." }),
  ]);
}
