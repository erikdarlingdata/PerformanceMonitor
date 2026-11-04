/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


/* `total_reads` / `total_read_time_ms` are the WINDOW's (#3613); the combination count is the PAGE's and is
   labelled as such - the read spells it `combinations_returned` since #3653, the same `<noun>s_returned` key
   every other paged tool uses, so a bare "Combinations" tile beside two window totals cannot be read as the
   window's third. */
export const PG_IO_SUMMARY_STATS = [
  { key: "combinations_returned", label: "Combinations returned", format: "int" },
  { key: "total_reads", label: "Reads", format: "int" },
  { key: "total_read_time_ms", label: "Read time", format: "ms" },
  { key: "busiest_by_read_time", label: "Busiest", format: "text", small: true, appendKey: "busiest_rank_note" },
  { key: "write_counters_tracked_anywhere", label: "Write counters tracked", format: "bool" },
];

/* ─────────────────────────── PostgreSQL column descriptors ─────────────────────────── */

/* #2629: the stock-PostgreSQL wait grid. Samples FIRST and the estimate beside it, deliberately the
   opposite emphasis from PG_WAIT_COLUMNS above — that one reports measured time, this one reports how many
   times a periodic profiler caught a backend in a state. Leading with the estimate would present a derived
   number as the observation and hide that its error grows as the event gets rarer. */
/* #2629 column sets for the eight reads that reached the web only now. */

/* Rates lead, counts do not appear: the interval lengths are not uniform, so two raw counts side by side
   are not comparable and the per-second figures are. Avg read ms renders blank rather than 0.00 wherever
   track_io_timing is off - which is the DEFAULT - because a zero there is a fact about the configuration
   and, printed as a latency, the most reassuring wrong number on the page. */
/* The bucket's single worst interval is drawn in the ramp's light grey: beside its pooled twin in the ramp's second blue, two lines of one hue cannot be told apart in the legend. */
const PEAK_COLOR = "#e0e0e0";
/* #7394: avg_read_ms is null on every point for two different reasons, and the page says only the one the payload proves. The server's
   track_io_timing setting is stated only when get_pg_io_trend reports it as the target's own collected setting (io_timing_tracked false and an
   io_timing_source that is not "inferred"). With timing on, null means the pair did no reads. Otherwise the page cannot tell which, so it says so. */
function noReadLatencyText(data) {
  const prefix = "No read latency in this window: ";
  if (data && data.io_timing_tracked === true) return prefix + "this backend and context did no reads here, so there is no read time to average.";
  if (data && data.io_timing_tracked === false && !String(data.io_timing_source || "inferred").startsWith("inferred")) {
    return prefix + "the server has track_io_timing off (the default), so PostgreSQL does not time reads and there is no latency to chart.";
  }
  return prefix + "either this backend and context did no reads here, or the server is not timing reads (track_io_timing off, the default), and this page cannot tell which.";
}
/* read_bytes_per_second is null when the pair has no byte counters and no block size to estimate from (bytes_source "unavailable"),
   and also when the pair did no reads. */
function noReadBytesText(data) {
  const prefix = "No read bytes in this window: ";
  const source = data && data.bytes_source;
  if (source === "unavailable") return prefix + "this pair has no byte counters and no operation size to estimate them from.";
  if (source === "measured" || source === "estimated_from_block_size") return prefix + "this backend and context did no reads here.";
  return prefix + "this pair did no reads here, or has no byte counters and no operation size to estimate them from.";
}
export function pgIoTrendSpecs(ctx) {
  const common = { viz: "line", rowsKey: "points", xKey: "collection_time", span: 2, hideEmptySeries: true, resetKey: "counter_reset", resetLabel: "Stats reset" };
  const empty = "No I/O samples for this combination in the window. A trend needs at least two snapshots to difference, so a short window is legitimately empty here even while the combination is being collected.";
  return [
    {
      ...common,
      title: "I/O Trend",
      subtitle: ctx.label + ", the busiest backend and context in this window, per second; Writes/sec is left out on a server with no write counters (Amazon Aurora)",
      series: [
        { key: "reads_per_second", label: "Reads/sec" },
        /* #3897: a point is a bucket of intervals, so its busiest single interval rides beside the pooled rate. */
        { key: "peak_reads_per_second", label: "Peak Reads/sec", color: PEAK_COLOR },
        { key: "writes_per_second", label: "Writes/sec" },
        { key: "evictions_per_second", label: "Evictions/sec" },
      ],
      format: "num2",
      unit: "ops/s",
      emptyText: empty,
    },
    { ...common, hideWhenNoRows: true, title: "I/O Hit %", subtitle: ctx.label + ", the pair's buffer pool hit ratio per point", series: [{ key: "cache_hit_pct", label: "Hit %" }], format: "pct", unit: "%", emptyText: empty },
    {
      ...common,
      hideWhenNoRows: true,
      title: "I/O Avg Read (ms)",
      subtitle: ctx.label + ", drawn only when the server has track_io_timing on, which is off by default",
      series: [{ key: "avg_read_ms", label: "Avg Read (ms)" }],
      format: "num2",
      unit: "ms",
      emptyText: empty,
      noSeriesText: noReadLatencyText,
    },
    {
      ...common,
      hideWhenNoRows: true,
      title: "I/O Read Bytes per Second",
      subtitle: ctx.label,
      series: [{ key: "read_bytes_per_second", label: "Read Bytes/sec" }],
      format: "bytes",
      unit: "bytes/s",
      emptyText: empty,
      noSeriesText: noReadBytesText,
    },
  ];
}

export const PG_WRITE_STATS = [
  { key: "checkpoints_timed", label: "Timed", format: "int" },
  { key: "checkpoints_requested", label: "Requested", format: "int" },
  { key: "pct_checkpoints_requested", label: "% Requested", format: "num1" },
  { key: "checkpoint_write_time_ms", label: "Checkpoint write", format: "ms" },
  { key: "buffers_written_checkpoint", label: "Buffers (checkpoint)", format: "int" },
  { key: "buffers_clean", label: "Buffers (bgwriter)", format: "int" },
  { key: "buffers_backend", label: "Buffers (backend)", format: "int" },
  { key: "wal_records", label: "WAL records", format: "int" },
  { key: "wal_fpi", label: "WAL full-page images", format: "int" },
  { key: "counter_reset", label: "Counters reset", format: "bool", small: true },
  /* #3955: a restart inside the window is why Requested, % Requested, Checkpoint write and Buffers (checkpoint)
     render blank: the shutdown checkpoint is counted as requested and its own write work lands in the same counters. */
  { key: "postmaster_restarted", label: "Restarted in window", format: "bool", small: true },
];

export const PG_BUFFER_USAGE_COLUMNS = [
  { key: "relation_name", label: "Relation" },
  { key: "relation_kind", label: "Kind" },
  { key: "buffer_mb", label: "Resident", format: "mb" },
  { key: "pct_of_pool", label: "% of Pool", format: "num1" },
  { key: "dirty_buffers", label: "Dirty", format: "int" },
  { key: "pct_dirty", label: "% Dirty", format: "num1" },
  { key: "avg_usage_count", label: "Avg Usage", format: "num2" },
];

export const PG_IO_COLUMNS = [
  { key: "backend_type", label: "Backend" },
  { key: "object_type", label: "Object" },
  { key: "context", label: "Context", hint: "Where the I/O came from, such as normal reads or vacuum. Click a context for what it covers.", detailKey: "context_meaning" },
  { key: "reads", label: "Reads", format: "int" },
  { key: "read_time_ms", label: "Read Time", format: "ms" },
  { key: "avg_read_ms", label: "Avg Read (ms)", format: "num2" },
  { key: "pct_of_total_read_time", label: "% of Read Time", format: "num1" },
  { key: "hits", label: "Hits", format: "int" },
  { key: "hit_pct", label: "Hit %", format: "num1" },
  { key: "extends", label: "Extends", format: "int" },
  { key: "evictions", label: "Evictions", format: "int" },
  { key: "reuses", label: "Reuses", format: "int" },
  /* Blank rather than zero on Aurora, where backends do not write data files and pg_stat_io's whole write side
     is NULL. The read preserves the NULL and the "bool" beside it says which it is, so a blank cell here is
     "not measured" and never "measured zero". */
  { key: "writes", label: "Writes", format: "int" },
  { key: "write_time_ms", label: "Write Time", format: "ms" },
  { key: "write_counters_tracked", label: "Writes Tracked", format: "bool" },
  { key: "stats_reset", label: "Stats Reset", format: "time" },
];

