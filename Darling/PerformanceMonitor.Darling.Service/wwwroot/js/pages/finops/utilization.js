/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Utilization" tab, raw panels: CPU over the last day (get_cpu_utilization), the latest memory snapshot
   (get_memory_stats) and allocated versus used size per database (get_database_sizes). Each section has its own
   container, so the provisioning verdict, health score, cost cards and trend can be added above them. */

import { el, noticeStrip } from "../../util.js";
import { renderPanel } from "../../panels.js";
import { READ_FIELDS } from "../../read-fields.js";

const CPU_HOURS = 24;

/* Both come from the shared catalog, so a label or format changed there changes here (the Server page does the same). */
const CPU_SERIES = READ_FIELDS.get_cpu_utilization.line.series;
const SIZE_COLUMNS = READ_FIELDS.get_database_sizes.table.columns;

/* The same predicate the Server page uses (server-tabs.js keeps its copy private): on an Azure SQL Database
   (engine_edition 5) the first two memory figures are the database's memory limit and the room left under it, not
   the host's RAM, so they are named that way and the read's own memory_note is shown. */
const AZURE_SQL_DATABASE = { key: "engine_edition", equals: 5 };

const MEMORY_STATS = [
  { key: "captured_at", label: "Collected", format: "reltime", small: true },
  { key: "total_physical_memory_mb", label: "Physical", format: "mb", hideWhen: AZURE_SQL_DATABASE },
  { key: "total_physical_memory_mb", label: "Memory limit", format: "mb", showWhen: AZURE_SQL_DATABASE },
  { key: "available_physical_memory_mb", label: "Available", format: "mb", hideWhen: AZURE_SQL_DATABASE },
  { key: "available_physical_memory_mb", label: "Available under limit", format: "mb", showWhen: AZURE_SQL_DATABASE },
  { key: "memory_utilization_pct", label: "Utilization", format: "pct" },
  { key: "total_server_memory_mb", label: "Total server", format: "mb" },
  { key: "target_server_memory_mb", label: "Target server", format: "mb" },
  { key: "buffer_pool_mb", label: "Buffer pool", format: "mb" },
  { key: "plan_cache_mb", label: "Plan cache", format: "mb" },
  { key: "system_memory_state", label: "System state", format: "text", small: true, nullKey: "system_memory_state_note" },
  { key: "sql_memory_model", label: "Memory model", format: "text", small: true },
];

const SIZES_STAMP = [{ key: "captured_at", label: "Collected", format: "reltime", small: true }];

function section(id, children) {
  return el("div", { class: "finops-section", "data-section": id }, children);
}

export const tab = {
  id: "utilization",
  label: "Utilization",
  build(server, ctx) {
    return el("div", { class: "finops-utilization" }, [
      section("verdict", [
        noticeStrip("Not on the web yet: the provisioning verdict, health score, cost cards, 7-day trend and the Top Databases by Total CPU and Top Databases by Avg CPU / Execution grids. The desktop viewer shows them."),
      ]),
      section("cpu", [
        renderPanel({
          title: "CPU Utilization",
          subtitle: "last " + CPU_HOURS + " hours",
          read: "get_cpu_utilization",
          params: { server, hours: CPU_HOURS },
          viz: "line",
          rowsKey: "samples",
          xKey: "sample_time",
          series: CPU_SERIES,
          format: "pct",
          unit: "%",
          emptyText: "No CPU samples in this window.",
          span: 2,
        }),
      ]),
      section("memory", [
        renderPanel({
          title: "Memory",
          subtitle: "latest snapshot",
          read: "get_memory_stats",
          params: { server },
          viz: "stat",
          stats: MEMORY_STATS,
          noteKey: "memory_note",
          span: 2,
        }),
      ]),
      section("sizes", [
        /* The sizes table has no tile row of its own, so the snapshot's stamp is a one-tile panel over the same read. */
        renderPanel({
          title: "Database Sizes snapshot",
          subtitle: "latest snapshot",
          read: "get_database_sizes",
          params: { server },
          viz: "stat",
          stats: SIZES_STAMP,
          span: 2,
        }),
        renderPanel({
          title: "Database Sizes",
          subtitle: "latest snapshot",
          read: "get_database_sizes",
          params: { server },
          viz: "table",
          rowsKey: "databases",
          columns: SIZE_COLUMNS,
          emptyText: "No database sizes in the latest snapshot.",
          noteKey: "note",
          span: 2,
        }),
      ]),
    ]);
  },
};
