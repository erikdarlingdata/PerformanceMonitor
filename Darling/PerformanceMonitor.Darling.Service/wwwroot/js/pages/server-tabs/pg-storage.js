/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { bloatVerdict, droppabilityVerdict, costFigure, lastScanNoteWord, fillfactorNoteWord, toastNoteWord, indexSkipWord } from "../../pg-grid-text.js";

/* The suppression count is a first-class tile rather than a per-row detail, because the usual cause is one
   instance-wide permissions gap: pg_stats is filtered by SELECT privilege and pg_monitor does not grant it,
   so either every row is publishable or almost none are. Naming it here is what gets it fixed.
   `estimated_bloat_*_over_trusted_rows` keeps the read's own name: it is a sum of ESTIMATES over the rows
   whose estimates were fit to publish, and a shorter label would promise reclaimable bytes. */
export const PG_BLOAT_STATS = [
  { key: "tables_returned", label: "Tables returned", format: "int" },
  { key: "estimated_bloat_mb_over_trusted_rows", label: "Est. bloat (trusted rows)", format: "mb" },
  { key: "trusted_estimate_count", label: "Estimates published", format: "int" },
  { key: "suppressed_estimate_count", label: "Estimates suppressed", format: "int" },
  { key: "pgstattuple_available_anywhere", label: "pgstattuple installed", format: "bool" },
  { key: "truncated", label: "Truncated", format: "bool" },
];

/* "unscanned_without_a_structural_blocker" is deliberately not shortened to "droppable" on the tile either:
   the field name survived review for saying what it is, and a label that renamed it would undo that where a
   reader actually looks. */
export const PG_INDEX_USAGE_STATS = [
  { key: "indexes_returned", label: "Indexes returned", format: "int" },
  { key: "unscanned_without_a_structural_blocker", label: "Unscanned, nothing blocking a drop", format: "int" },
  { key: "mb_held_by_those_indexes", label: "Held by those indexes", format: "mb" },
  { key: "invalid_index_count", label: "Invalid indexes", format: "int" },
  { key: "statistics_were_reset_in_window", label: "Stats reset in window", format: "bool" },
  { key: "truncated", label: "Truncated", format: "bool" },
];

/* #6154: a column whose read writes a paragraph per row. The cell shows the short word `word(text)` gives, sorts and filters on it, and the
   full text opens under it on a click (panels.js `detailKey`). What is true of every row goes in `hint`, the header tooltip. */
function detailColumn(key, label, word, hint, extra) {
  const short = (row) => word(row[key]);
  return { key, label, hint, display: (row) => short(row) ?? "—", sortValue: short, detailKey: key, ...extra };
}

export const PG_INDEX_BLOAT_COLUMNS = [
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "index_name", label: "Index" },
  { key: "index_mb", label: "Size", format: "mb" },
  { key: "avg_leaf_density", label: "Leaf Density %", format: "num1" },
  { key: "leaf_fragmentation", label: "Fragmentation %", format: "num1" },
  { key: "estimated_reclaimable_mb", label: "Reclaimable", format: "mb" },
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
  { key: "heap_mb", label: "Heap", format: "mb" },
  { key: "bloat_mb_estimate", label: "Bloat (est.)", format: "mb" },
  { key: "bloat_pct_estimate", label: "Bloat % (est.)", format: "num2" },
  { key: "estimate_suppressed", label: "Suppressed", format: "bool" },
  { key: "estimate_suppression_reason", label: "Why Suppressed", clamp: true },
  /* Measured, from the server's own counters - what a suppressed estimate degrades TO rather than to
     nothing. */
  { key: "dead_tuples", label: "Dead Tuples", format: "int", hint: "Rows deleted or updated and not yet cleaned up by vacuum. A count from the server's own statistics, not an estimate." },
  { key: "dead_tuple_pct", label: "Dead Tuple %", format: "num2", hint: "Dead tuples as a share of live plus dead rows. Click a value for what it means for this table.", detailKey: "dead_tuple_finding" },
  detailColumn("bloat_finding", "Finding", bloatVerdict,
    "How large the estimated bloat is: under 20% low, 20% to 50% moderate, above that high. Click a verdict for what to do about it."),
  { key: "mods_since_analyze_pct_of_rows", label: "Modified Since Analyze %", format: "num1" },
  { key: "last_analyzed", label: "Last Analyzed", format: "time" },
  { key: "fillfactor", label: "Fill Factor", format: "int" },
  detailColumn("fillfactor_note", "Fill Factor Note", fillfactorNoteWord, "Set when the table keeps free space on purpose for in-place updates. Click for how that affects the estimate."),
  { key: "toast_bytes", label: "TOAST Bytes", format: "int" },
  detailColumn("toast_note", "TOAST", toastNoteWord, "Set when the table stores large values out of line. The bloat estimate covers the main table only. Click for details."),
  { key: "index_bytes", label: "Index Bytes", format: "int" },
  { key: "heap_bytes_growth_in_window", label: "Heap Growth", format: "int" },
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
  { key: "index_mb", label: "Size", format: "mb" },
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

