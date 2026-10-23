/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* #6293: the Query Heatmap grid's logic, with no DOM and no imports so a Node scenario can run it: the metrics on offer, the bin
   width for a window, the cells laid out on a (magnitude row x time column) grid, and the shade for a count. query-heatmap.js draws it. */

/* The Viewer's metric picker, minus Logical Reads (ruled out for the whole web, #6293). `value` is get_query_heatmap's `metric`; `top` names
   what the top row of the grid holds under that metric (#6353: the rows are the metric's magnitude bands, highest at the top). */
export const HEATMAP_METRICS = [
  { value: "duration", label: "Duration (ms)", top: "the slowest queries" },
  { value: "cpu", label: "CPU (ms)", top: "the queries using the most CPU" },
  { value: "logical_writes", label: "Logical Writes", top: "the queries doing the most logical writes" },
  { value: "execution_count", label: "Execution Count", top: "the most-executed queries" },
];

/* The Viewer draws 5-minute columns. A 24 hour window at 5 minutes is 288 columns by up to 7 rows, past the 1000 cells one read
   returns (the read keeps the NEWEST bins and drops the oldest), so a longer window widens the bin to stay near 144 columns. Always
   a multiple of 5 so a bin edge is a clock edge, and never past the read's one-day limit. */
export const HEATMAP_TARGET_COLUMNS = 144;
export function heatmapBinMinutes(hours) {
  const h = Number.isFinite(hours) && hours > 0 ? hours : 24;
  const wanted = Math.ceil((h * 60) / HEATMAP_TARGET_COLUMNS / 5) * 5;
  return Math.min(1440, Math.max(5, wanted));
}

/* Shade levels 1..8; 0 is "no query", which is drawn as a different thing (an outlined, unfilled cell), never as the lightest shade. */
export const HEATMAP_SHADES = 8;

/** The shade level (0 to 8) for a count against the busiest cell. Log scale like the Viewer (Math.Log(1 + count)), so one busy cell does not flatten the rest. */
export function heatmapShade(count, max) {
  const c = Number(count);
  const m = Number(max);
  if (!Number.isFinite(c) || c <= 0) return 0;
  if (!Number.isFinite(m) || m <= 0) return 1;
  const t = Math.log1p(Math.min(c, m)) / Math.log1p(m);
  return Math.min(HEATMAP_SHADES, Math.max(1, Math.ceil(t * HEATMAP_SHADES)));
}

/* A store time is naive UTC ("2026-10-21T14:05:00.0000000"): read it as UTC, as util.parseUtc does. */
export function heatmapTimeMs(s) {
  if (typeof s !== "string" || !s) return NaN;
  const hasZone = /[zZ]$|[+\-]\d\d:?\d\d$/.test(s);
  return Date.parse(hasZone ? s : s + "Z");
}

/**
 * Lay get_query_heatmap's cells out on a grid. Columns run from the first bin that holds a cell to the last, one per bin width, so a
 * quiet stretch between two busy ones is an empty column rather than missing; rows are the 7 magnitude buckets (row index = bucket_index).
 * Returns { empty, binMinutes, binMs, startMs, columns, labels, rows, max, cellCount } where rows[r][c] is the cell or null.
 */
export function buildHeatmap(data) {
  const cells = data && Array.isArray(data.cells) ? data.cells : [];
  const binMinutes = Number(data && data.bucket_minutes) > 0 ? Number(data.bucket_minutes) : 5;
  const binMs = binMinutes * 60000;
  const labels = Array.isArray(data && data.magnitude_buckets) ? data.magnitude_buckets.map((b) => String(b.label)) : [];
  const placed = [];
  for (const c of cells) {
    const ms = heatmapTimeMs(c.time_bucket);
    const r = Number(c.bucket_index);
    const n = Number(c.query_count);
    if (!Number.isFinite(ms) || !Number.isInteger(r) || r < 0 || !Number.isFinite(n)) continue;
    placed.push({ ms, r, n, cell: c });
  }
  if (!placed.length) return { empty: true, binMinutes, binMs, startMs: NaN, columns: 0, labels, rows: [], max: 0, cellCount: 0 };
  const startMs = Math.min(...placed.map((p) => p.ms));
  const endMs = Math.max(...placed.map((p) => p.ms));
  const columns = Math.round((endMs - startMs) / binMs) + 1;
  const rowCount = Math.max(labels.length, ...placed.map((p) => p.r + 1));
  const rows = Array.from({ length: rowCount }, () => new Array(columns).fill(null));
  let max = 0;
  for (const p of placed) {
    const col = Math.round((p.ms - startMs) / binMs);
    rows[p.r][col] = { cell: p.cell, count: p.n, ms: p.ms, shade: 0 };
    if (p.n > max) max = p.n;
  }
  for (const row of rows) for (const g of row) if (g) g.shade = heatmapShade(g.count, max);
  return { empty: false, binMinutes, binMs, startMs, columns, labels, rows, max, cellCount: placed.length };
}

/** The instant "Show Active Queries at This Time" opens for a cell: the middle of its bin, so the hour the item opens holds the whole bin. */
export function heatmapDrillMs(grid, cellMs) {
  return cellMs + grid.binMs / 2;
}

/** The sentence for a cell, shared by its tooltip, the detail line and the cell's accessible name. `when` is the formatted bin time. */
export function heatmapCellText(when, label, count, metricWord) {
  const n = Number(count);
  return when + " | " + (metricWord ? metricWord + " " : "") + label + " | " + n.toLocaleString("en-US") + (n === 1 ? " query" : " queries");
}
