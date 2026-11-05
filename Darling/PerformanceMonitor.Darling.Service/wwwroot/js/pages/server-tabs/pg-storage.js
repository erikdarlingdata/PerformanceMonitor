/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { fmtBytes } from "../../util.js";
import { detailColumn, bloatVerdict, droppabilityVerdict, costFigure, lastScanNoteWord, fillfactorNoteWord, toastNoteWord, indexSkipWord } from "../../pg-grid-text.js";

/* #7564: a size column reads the reply's exact byte count and prints it with fmtBytes, never a rounded MB turned back into KB (0.45 MB of an index of
   456 KB read "460.8 KB"). The reads publish -1 for a size they could not measure, which is a dash and not "-1 B". */
const sizeOrDash = (key) => (row) => (row[key] != null && Number(row[key]) >= 0 ? fmtBytes(row[key]) : "—");

/* The sentence under a panel whose grid was cut at its row limit (#7551). It names the count when the read gave one, and falls back to the plain sentence when it did not. */
function pageCutLine(noun, shown, total, tilesNote) {
  const head = Number.isFinite(shown) && Number.isFinite(total) && total > shown
    ? "The grid below lists " + shown + " of the " + total + " " + noun + " the window held."
    : "The window held more " + noun + " than the grid below lists.";
  return head + " " + tilesNote;
}

/* The suppression count is a first-class tile rather than a per-row detail, because the usual cause is one
   instance-wide permissions gap: pg_stats is filtered by SELECT privilege and pg_monitor does not grant it,
   so either every row is publishable or almost none are. Naming it here is what gets it fixed.
   `estimated_bloat_*_over_trusted_rows` keeps the read's own name: it is a sum of ESTIMATES over the rows
   whose estimates were fit to publish, and a shorter label would promise reclaimable bytes. */
export const PG_BLOAT_STATS = [
  { key: "estimated_bloat_bytes_over_trusted_rows", label: "Est. bloat (trusted rows)", format: "bytes", nullKey: "estimated_bloat_state" },
  { key: "trusted_estimate_count", label: "Estimates published", format: "int" },
  { key: "suppressed_estimate_count", label: "Estimates suppressed", format: "int" },
  { key: "pgstattuple_available_anywhere", label: "pgstattuple installed", format: "bool" },
  { key: "truncated", pageCut: (d) => pageCutLine("tables", d.tables_returned, d.tables_in_window, "The counts and the estimated total above cover every table in the window."), label: "Truncated", format: "bool" },
];

/* "unscanned_without_a_structural_blocker" is deliberately not shortened to "droppable" on the tile either:
   the field name survived review for saying what it is, and a label that renamed it would undo that where a
   reader actually looks. */
export const PG_INDEX_USAGE_STATS = [
  { key: "unscanned_without_a_structural_blocker", label: "Unscanned, nothing blocking a drop", format: "int" },
  { key: "bytes_held_by_those_indexes", label: "Held by those indexes", format: "bytes" },
  { key: "invalid_index_count", label: "Invalid indexes", format: "int" },
  { key: "statistics_were_reset_in_window", label: "Stats reset in window", format: "bool" },
  { key: "truncated", pageCut: (d) => pageCutLine("indexes", d.indexes_returned, d.indexes_in_window, "The tiles above count only the indexes listed."), label: "Truncated", format: "bool" },
];

export const PG_INDEX_BLOAT_COLUMNS = [
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "index_name", label: "Index" },
  { key: "index_bytes", label: "Size", format: "bytes" },
  { key: "avg_leaf_density", label: "Leaf Density %", format: "num1" },
  { key: "leaf_fragmentation", label: "Fragmentation %", format: "num1" },
  { key: "estimated_reclaimable_bytes", label: "Reclaimable", format: "bytes" },
  /* #7551: a reclaimable figure with nothing beside it reads as a measured 0 B. An estimated row has no leaf density to show (nothing walked its pages), so the figure it comes from and its basis sit here. */
  { key: "est_bloat_pct", label: "Est. Bloat %", format: "num1", hint: "The estimated share of the index that is free space. Blank when no estimate could be made." },
  { key: "measurement_kind", label: "Basis", display: (row) => (row.measurement_kind === "estimated" ? "Estimated" : row.measurement_kind === "measured" ? "Measured" : "—"), hint: "Estimated: worked out from catalog statistics, so no leaf density or fragmentation exists. Measured: an older exact reading from pgstatindex." },
  detailColumn("skipped_reason", "Not Measured", indexSkipWord,
    "Why no bloat figure could be estimated for this index. Blank means it was estimated. Click a reason for the full text."),
];

/* bloat_pct_estimate and bloat_bytes_estimate are NULL on a suppressed row, which renders as an empty cell
   rather than a zero - and that is the point. estimate_suppression_reason is the column that says why, so it
   is wrapped and kept adjacent rather than pushed to the end of a wide grid. */
export const PG_BLOAT_COLUMNS = [
  { key: "database", label: "Database" },
  { key: "schema", label: "Schema" },
  { key: "table", label: "Table" },
  /* sevKey, not statusSev: the band is SERVER-computed (the browser never re-derives one) and it is what
     gives this grid the cue the WPF grid gets from its row-style triggers. A suppressed row comes back
     "Unknown" rather than a severity, so it renders neutral - colouring it would assert the very thing the
     suppression denies. */
  { key: "severity", label: "Severity", sevKey: "severity" },
  { key: "heap_bytes", label: "Heap", format: "bytes", display: sizeOrDash("heap_bytes") },
  { key: "bloat_bytes_estimate", label: "Bloat (est.)", format: "bytes" },
  { key: "bloat_pct_estimate", label: "Bloat % (est.)", format: "num2" },
  { key: "estimate_suppressed", label: "Suppressed", format: "bool" },
  { key: "estimate_suppression_reason", label: "Why Suppressed", clamp: true },
  /* Measured, from the server's own counters - what a suppressed estimate degrades TO rather than to
     nothing. */
  { key: "dead_tuples", label: "Dead Tuples", format: "int", hint: "Rows deleted or updated and not yet cleaned up by vacuum. A count from the server's own statistics, not an estimate." },
  { key: "dead_tuple_pct", label: "Dead Tuple %", format: "num2", hint: "Dead tuples as a share of live plus dead rows. Click a value for what it means for this table.", detailKey: "dead_tuple_finding" },
  /* #7410: "Bloat Level", not "Finding": the Severity column beside it reads "Low" too, and two columns headed differently that say the same word looked like a repeat. */
  detailColumn("bloat_finding", "Bloat Level", bloatVerdict,
    "How large the estimated bloat is: under 20% low, 20% to 50% moderate, above that high. Click a verdict for what to do about it."),
  { key: "mods_since_analyze_pct_of_rows", label: "Modified Since Analyze %", format: "num1" },
  { key: "last_analyzed", label: "Last Analyzed", format: "time" },
  { key: "fillfactor", label: "Fill Factor", format: "int" },
  detailColumn("fillfactor_note", "Fill Factor Note", fillfactorNoteWord, "Set when the table keeps free space on purpose for in-place updates. Click for how that affects the estimate."),
  { key: "toast_bytes", label: "TOAST Size", format: "bytes" },
  detailColumn("toast_note", "TOAST", toastNoteWord, "Set when the table stores large values out of line. The bloat estimate covers the main table only. Click for details."),
  { key: "index_bytes", label: "Index Size", format: "bytes" },
  { key: "heap_bytes_growth_in_window", label: "Heap Growth", format: "bytes" },
  { key: "pgstattuple_available", label: "pgstattuple", format: "bool" },
  { key: "exact_measurement_command", label: "Measure It Exactly", clamp: true, mono: true },
  { key: "sample_count", label: "Samples", format: "int" },
];

/* Both scan figures are shown. The lifetime count is what anyone querying the server directly would see, so
   omitting it would make this grid look like it disagreed with psql; the windowed one is the answer to the
   question actually being asked, and only stored history can produce it. */
export const PG_INDEX_USAGE_COLUMNS = [
  { key: "database", label: "Database" },
  { key: "schema", label: "Schema" },
  { key: "table", label: "Table" },
  { key: "index", label: "Index" },
  /* Server-computed, same as the bloat grid: INVALID is Critical, an unscanned index with no structural
     blocker is Warning, and an index we have not watched for two samples is Healthy rather than Warning -
     too-early-to-say must not look like a finding. */
  { key: "severity", label: "Severity", sevKey: "severity" },
  { key: "index_bytes", label: "Size", format: "bytes", display: sizeOrDash("index_bytes") },
  { key: "scans_in_window", label: "Scans in Window", format: "int" },
  { key: "total_scans_since_stats_reset", label: "Lifetime Scans", format: "int" },
  { key: "last_scan", label: "Last Scan", format: "time" },
  detailColumn("last_scan_note", "Last Scan Note", lastScanNoteWord,
    "Why the last-scan time is missing. PostgreSQL 16 and later record it; older versions do not. Click for the full note."),
  detailColumn("droppability_finding", "Can It Go?", droppabilityVerdict,
    "Whether the index could be dropped. Click a verdict for the reason and what to check first."),
  detailColumn("cost_finding", "What It Costs", costFigure,
    "The index size and its share of the table's heap. Click for what keeping it costs on every write."),
  /* The droppability facts, individually, so the grid can be sorted and filtered on them rather than only
     read as prose. supports_constraint is the one that most often makes a zero-scan index untouchable. */
  { key: "is_valid", label: "Valid", format: "bool" },
  { key: "is_primary_key", label: "Primary Key", format: "bool" },
  { key: "is_unique", label: "Unique", format: "bool" },
  { key: "supports_constraint", label: "Backs Constraint", format: "bool" },
  { key: "is_replica_identity", label: "Replica Identity", format: "bool" },
  { key: "is_partial", label: "Partial", format: "bool" },
  { key: "is_expression", label: "Expression", format: "bool" },
  { key: "index_method", label: "Method" },
  { key: "tuples_read", label: "Tuples Read", format: "int" },
  { key: "tuples_fetched", label: "Tuples Fetched", format: "int" },
  { key: "blocks_hit", label: "Blocks Hit", format: "int" },
  { key: "stats_were_reset_in_window", label: "Stats Reset", format: "bool" },
  { key: "sample_count", label: "Samples", format: "int" },
  { key: "index_definition", label: "Definition", wrap: true, valueList: false },
];

