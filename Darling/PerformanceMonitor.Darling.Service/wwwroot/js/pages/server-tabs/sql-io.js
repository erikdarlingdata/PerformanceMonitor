/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { routeHash, el, readToolWithinKeptHistory, keptWindowStrip, mount, readErrorStrip, emptyStrip, noticeStrip, fmtInt, windowFromHours, getActiveDatabaseFilter } from "../../util.js";
import { getPanelSignal } from "../../panels.js";
import { zoomableLineChart, chartZoomScope, SERIES_COLORS } from "../../charts.js";
import { READ_FIELDS } from "../../read-fields.js";
import { panelShell, discontinuityNotes, line } from "./shared.js";

/**
 * What a File I/O Latency line is, for the subtitle (#5244 PR5): exactly one chosen database draws one line per file ("per
 * file"); every database, or two or more, draw one line per database and file type. It follows the same rule as the route
 * (one name: per file), and only for the active server on a server page, as the chip does.
 */
export function fileIoGrain(server) {
  const filter = getActiveDatabaseFilter();
  const hash = routeHash();
  const one = filter && filter.server === server && hash.startsWith("#/server/") && filter.databases.length === 1;
  return one ? "per file" : "per database and file type";
}

/**
 * File I/O latency: pivot the flat trend into one read-latency series per line. #3897: a line is a (database, file
 * type) pair — or the one "(other)" line pooling everything past the top five — so the label names both; before
 * #3897 the read projected only the database, and nine tempdb files wrote over each other in this pivot.
 */
export function fileIoPanel(server, ctx) {
  const { panel, body } = panelShell("File I/O Latency", "avg read and write latency " + fileIoGrain(server) + ", " + ctx.label, 2, "get_file_io_trend");
  const signal = getPanelSignal();
  (async () => {
    const res = await readToolWithinKeptHistory("get_file_io_trend", { server, hours: ctx.hours }, signal);
    if (res.kind === "aborted") return;
    if (res.kind === "error") return mount(body, readErrorStrip(res.message));
    if (res.kind === "empty") return mount(body, [keptWindowStrip(res), emptyStrip(res.message)]);

    const rows = (res.data.trend || []).map((r) => ({
      ...r,
      line: r.database_name === r.file_type ? r.database_name : r.database_name + " " + r.file_type + (r.file_name ? " " + r.file_name : ""),
    }));
    /* The desktop draws read and write latency as two stacked charts over the same lines; so does this panel, one read
       feeding both. Each chart keeps its own top series by its own peak; a bucket with no reads (or no writes) has a null figure and is left out of that chart rather than drawn as 0. */
    const read = pivot(rows.filter((r) => r.avg_read_latency_ms != null), { xKey: "time", seriesKey: "line", valueKey: "avg_read_latency_ms" });
    const write = pivot(rows.filter((r) => r.avg_write_latency_ms != null), { xKey: "time", seriesKey: "line", valueKey: "avg_write_latency_ms" });
    if (!read.series.length && !write.series.length) return mount(body, [keptWindowStrip(res), emptyStrip("No file I/O samples in this window.")]);
    /* #3653 A5: the payload's baseline discontinuities as a notice above the chart. */
    const notes = discontinuityNotes(res.data);
    /* #2802: axis spans the requested window (ctx.hours ending now), not the pivoted data's own extent. A
       narrowed read spans the hours it answered for. */
    const draw = (title, data, id, empty) =>
      data.series.length
        ? [el("h4", { text: title }), zoomableLineChart({ atTime: { server }, points: data.points, xKey: "time", series: data.series, formatValue: (v, o) => fmtInt(v, o) + " ms", unit: "ms", ...windowFromHours(res.keptHours || ctx.hours) }, id, chartZoomScope(ctx.hours))]
        : [el("h4", { text: title }), emptyStrip(empty)];
    mount(body, [
      keptWindowStrip(res),
      notes.length ? noticeStrip(notes.join(" ")) : null,
      ...draw("Read latency (ms)", read, "file-io-latency", "No reads in this window."),
      ...draw("Write latency (ms)", write, "file-io-write-latency", "No writes in this window."),
    ]);
  })();
  return panel;
}

/** Reshape flat rows into per-series points, keeping the top `maxSeries` series by peak value. */
function pivot(rows, { xKey, seriesKey, valueKey }, maxSeries = 8) {
  const byTime = new Map();
  const peak = new Map();
  for (const r of rows) {
    const t = r[xKey];
    const name = r[seriesKey];
    const v = r[valueKey];
    if (t == null || name == null) continue;
    if (!byTime.has(t)) byTime.set(t, { [xKey]: t });
    byTime.get(t)[name] = v;
    peak.set(name, Math.max(peak.get(name) ?? -Infinity, v ?? -Infinity));
  }
  const names = [...peak.keys()].sort((a, b) => peak.get(b) - peak.get(a)).slice(0, maxSeries);
  const points = [...byTime.values()].sort((a, b) => String(a[xKey]).localeCompare(String(b[xKey])));
  const series = names.map((n, i) => ({ key: n, label: n, color: SERIES_COLORS[i % SERIES_COLORS.length] }));
  return { points, series };
}

export const TEMPDB_SERIES = READ_FIELDS.get_tempdb_trend.line.series;

export const FILE_IO_COLUMNS = READ_FIELDS.get_file_io_stats.table.columns;

export const DB_SIZE_COLUMNS = READ_FIELDS.get_database_sizes.table.columns;

export const TABLE_SIZE_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "reserved_mb", label: "Reserved", format: "mb" },
  { key: "used_mb", label: "Used", format: "mb" },
  { key: "total_rows", label: "Rows", format: "int" },
  { key: "index_count", label: "Indexes", format: "int" },
  { key: "growth_7d_mb", label: "7d growth", format: "mb" },
  { key: "growth_30d_mb", label: "30d growth", format: "mb" },
  { key: "growth_pct_30d", label: "30d %", format: "num1" },
];

export const PVS_COLUMNS = READ_FIELDS.get_pvs_stats.table.columns;

