/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Version Store (PVS)" tab: the latest persistent version store state per database and a size trend for the
   top databases, both from one get_pvs_stats read. A server whose engine has no PVS shows the read's own message. PVS counts off-row versions only, matching the desktop label. */

import { VIZ } from "../../panels.js";
import { zoomableLineChart, chartZoomScope, SERIES_COLORS } from "../../charts.js";
import { el, readTool, readErrorStrip, emptyStrip, errorStrip, loadingStrip, mount, fmtNum, localTime, windowFromHours } from "../../util.js";

const TREND_HOURS = 24;

const COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "is_adr_on", label: "ADR on", format: "bool" },
  { key: "pvs_size_mb", label: "PVS off-row MB", format: "num2" },
  { key: "pct_of_database", label: "% of database", format: "num2", nullKey: "pct_of_database_reason", wrap: true },
  { key: "online_index_version_store_mb", label: "Online index store MB", format: "num2" },
  { key: "database_data_size_mb", label: "Database data size", format: "mb" },
  { key: "aborted_transaction_count", label: "Aborted transactions", format: "int" },
  { key: "aborted_version_cleaner_start_time", label: "Aborted cleaner start", format: "time" },
  { key: "aborted_version_cleaner_end_time", label: "Aborted cleaner end", format: "time" },
  { key: "offrow_version_cleaner_start_time", label: "Off-row cleaner start", format: "time" },
  { key: "offrow_version_cleaner_end_time", label: "Off-row cleaner end", format: "time" },
  { key: "oldest_active_transaction_id", label: "Oldest active transaction id", format: "int" },
  { key: "oldest_aborted_transaction_id", label: "Oldest aborted transaction id", format: "int" },
];

/* One row per collection time, one column per database, so the chart draws one line per database. */
function pivotTrend(trend) {
  const byTime = new Map();
  const series = [];
  (trend || []).forEach((g, i) => {
    const key = "db" + i;
    series.push({ key, label: g.database_name, color: SERIES_COLORS[i % SERIES_COLORS.length] });
    (g.points || []).forEach((p) => {
      const row = byTime.get(p.collection_time) || { collection_time: p.collection_time };
      row[key] = p.pvs_size_mb;
      byTime.set(p.collection_time, row);
    });
  });
  return { points: [...byTime.values()], series };
}

function panel(title, subtitle, body) {
  return el("div", { class: "panel card span-2" }, [
    el("h3", {}, [title, subtitle ? el("span", { class: "panel-sub", text: " " + subtitle }) : null]),
    el("div", { class: "panel-body" }, [body]),
  ]);
}

export const tab = {
  id: "version-store",
  label: "Version Store (PVS)",
  build(server, ctx) {
    const body = el("div", {}, [loadingStrip()]);
    const root = el("div", { class: "grid" }, [panel("Persistent version store", null, body)]);
    load(server, ctx, body);
    return root;
  },
};

async function load(server, ctx, body) {
  const signal = ctx && ctx.signal;
  const res = await readTool("get_pvs_stats", { server, trend_hours_back: TREND_HOURS }, signal);
  if (res.kind === "aborted" || res.kind === "auth") return;
  if (res.kind === "error") {
    mount(body, readErrorStrip(res.message));
    return;
  }
  if (res.kind === "empty") {
    mount(body, emptyStrip(res.message));
    return;
  }
  try {
    const data = res.data;
    const table = VIZ.table(data, {
      rowsKey: "databases",
      columns: COLUMNS,
      emptyText: "No PVS rows were returned for this server.",
    });
    const { points, series } = pivotTrend(data.trend);
    const chart = points.length
      ? zoomableLineChart({ points, xKey: "collection_time", series, formatValue: (v) => fmtNum(v, 2), unit: "PVS off-row MB", ...windowFromHours(TREND_HOURS) }, "pvs-size-trend", chartZoomScope(TREND_HOURS))
      : emptyStrip("No PVS size history in the last " + TREND_HOURS + " hours.");
    mount(body, [
      el("div", { class: "strip notice", role: "status", text: "As of " + localTime(data.as_of) + " (latest snapshot)." }),
      table,
      el("h4", { text: "PVS off-row size (MB), last " + TREND_HOURS + " hours, top databases by current size" }),
      chart,
    ]);
  } catch (e) {
    mount(body, errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e))));
  }
}
