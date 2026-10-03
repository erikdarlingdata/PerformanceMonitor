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

const CPU_HOURS = 24;

const CPU_SERIES = [
  { key: "sql_server_cpu", label: "SQL CPU %" },
  { key: "other_process_cpu", label: "Other %" },
  { key: "total_cpu", label: "Total %" },
];

const MEMORY_STATS = [
  { key: "total_physical_memory_mb", label: "Physical", format: "mb" },
  { key: "available_physical_memory_mb", label: "Available", format: "mb" },
  { key: "memory_utilization_pct", label: "Utilization", format: "pct" },
  { key: "total_server_memory_mb", label: "Total server", format: "mb" },
  { key: "target_server_memory_mb", label: "Target server", format: "mb" },
  { key: "buffer_pool_mb", label: "Buffer pool", format: "mb" },
  { key: "plan_cache_mb", label: "Plan cache", format: "mb" },
  { key: "system_memory_state", label: "System state", format: "text", small: true, nullKey: "system_memory_state_note" },
  { key: "sql_memory_model", label: "Memory model", format: "text", small: true },
];

const SIZE_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "total_size_mb", label: "Allocated", format: "mb" },
  { key: "used_size_mb", label: "Used", format: "mb" },
  { key: "size_note", label: "Note", wrap: true, hideWhenEmpty: true },
];

function section(id, children) {
  return el("div", { class: "finops-section", "data-section": id }, children);
}

export const tab = {
  id: "utilization",
  label: "Utilization",
  build(server, ctx) {
    return el("div", { class: "finops-utilization" }, [
      section("verdict", [
        noticeStrip("Coming: the provisioning verdict, health score, cost cards and 7-day trend are not on the web yet."),
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
          span: 2,
        }),
      ]),
      section("sizes", [
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
