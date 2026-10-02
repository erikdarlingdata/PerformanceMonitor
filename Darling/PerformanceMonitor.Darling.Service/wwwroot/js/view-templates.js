/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Starter DASHBOARD templates (#2476) — the dashboard twin of notebook.js's NOTEBOOK_TEMPLATES.
 *
 * No custom view ships seeded, so a new user's Custom Views page is empty and the composer is a blank canvas
 * over a 82-read catalog. These are five ready-made dashboards a reader can create in one click, pointed at a
 * server they pick.
 *
 * They are TEMPLATES, not seeded rows. A seeded row would need a migration rung, a StorageVersion bump, four
 * pinned test files and a viewer probe sentinel — for content that is not schema — and it would resurrect itself
 * on the next upgrade after the user deleted it. A template is created only when someone asks for it, and what
 * they get is an ordinary view they own, can edit, and can delete for good.
 *
 * A fresh install has nothing collected for its first cycles, so every table and chart panel here carries its
 * own `emptyText` and every read it names has an honest {status,message} envelope behind it. That matters more
 * for a STARTER dashboard than for anything else in the product: it is the first screen a UAT tester opens, on
 * the store with the least data it will ever have, and a wall of unexplained blank rectangles on day one is a
 * worse first impression than the feature not existing. Two reads are deliberately ABSENT for the same reason:
 * analysis findings (the pass needs 24 hours of history before it writes any) and Query Store (a target with it
 * switched off has nothing, ever) — both are on the built-in server page, where the reader arrived looking for
 * that specific thing rather than for a first impression.
 *
 * The panels here are v1 READ descriptors: `{title, read, params, viz, span, ...vizcfg}`, exactly the shape
 * renderPanel and the server's ValidateDefinition already take. The column and series sets come from READ_FIELDS
 * (read-fields.js), the one place a read's fields live, and are SPREAD into each panel here. They deliberately do
 * NOT share the built-in server page's descriptors: a template is JSON-serialized when a view is created, so a saved
 * view keeps its own copy from that moment on, and a template that shared arrays with a shipped page would
 * silently rewrite the dashboards people had already saved every time the page changed.
 */

import { READ_FIELDS } from "./read-fields.js";

/** The templates. Each `make(server)` returns a {name, description, definition} ready to POST to /api/views. */
export const DASHBOARD_TEMPLATES = [
  {
    key: "server-health",
    label: "Server health at a glance",
    description: "The first screen for a server you have not looked at before: load, memory, what it waits on, and whether collection is actually working.",
    make: (server) => ({
      name: "Server health — " + server,
      description: "Load, memory, waits and collection health for " + server + ".",
      definition: {
        panels: [
          {
            title: "Overview",
            read: "get_server_summary",
            params: { server },
            viz: "stat",
            span: 2,
            stats: [
              { key: "cpu_percent", label: "CPU", format: "pct" },
              { key: "memory_mb", label: "Memory", format: "mb" },
              { key: "blocking_count", label: "Blocking (recent)", format: "int" },
              { key: "deadlock_count", label: "Deadlocks (recent)", format: "int" },
              { key: "last_collection", label: "Last collection", format: "reltime", small: true },
            ],
          },
          {
            title: "CPU Utilization",
            read: "get_cpu_utilization",
            params: { server, hours: 24 },
            viz: "line",
            rowsKey: "samples",
            xKey: "sample_time",
            format: "pct",
            unit: "%",
            emptyText: "No CPU samples in this window.",
            series: [
              { key: "sql_server_cpu", label: "SQL CPU %" },
              { key: "other_process_cpu", label: "Other %" },
              { key: "total_cpu", label: "Total %" },
            ],
          },
          {
            title: "Memory",
            read: "get_memory_trend",
            params: { server, hours: 24 },
            viz: "line",
            rowsKey: "trend",
            xKey: "time",
            format: "mb",
            emptyText: "No memory samples in this window.",
            series: [
              { key: "total_server_memory_mb", label: "Total Server" },
              { key: "target_server_memory_mb", label: "Target" },
              { key: "buffer_pool_mb", label: "Buffer Pool" },
            ],
          },
          {
            title: "Wait Stats",
            read: "get_wait_stats",
            params: { server, hours: 24, limit: 20 },
            viz: "table",
            span: 2,
            ...READ_FIELDS.get_wait_stats.table,
          },
          {
            title: "Collection Health",
            read: "get_collection_health",
            /* #4198: errors, avg_duration_ms etc. are omitted by default on a boring-healthy collector row, and
               this template reads them - but the web read forces full detail itself (DarlingWebEndpoints'
               get_collection_health entry), and the read catalog lists no full_detail parameter, so sending one
               makes POST /api/views refuse the whole dashboard. The panel sends only the server. */
            params: { server },
            viz: "table",
            span: 2,
            ...READ_FIELDS.get_collection_health.table,
          },
        ],
      },
    }),
  },

  {
    key: "cpu-investigation",
    label: "CPU investigation",
    description: "Why is this server busy: the utilization curve, scheduler pressure, and the queries and procedures spending the CPU.",
    make: (server) => ({
      name: "CPU investigation — " + server,
      description: "Utilization, scheduler pressure and the top CPU consumers on " + server + ".",
      definition: {
        panels: [
          {
            title: "CPU Utilization",
            read: "get_cpu_utilization",
            params: { server, hours: 24 },
            viz: "line",
            span: 2,
            rowsKey: "samples",
            xKey: "sample_time",
            format: "pct",
            unit: "%",
            emptyText: "No CPU samples in this window.",
            series: [
              { key: "sql_server_cpu", label: "SQL CPU %" },
              { key: "other_process_cpu", label: "Other %" },
              { key: "total_cpu", label: "Total %" },
            ],
          },
          {
            title: "Scheduler Pressure",
            read: "get_cpu_scheduler_pressure",
            params: { server },
            viz: "stat",
            span: 2,
            ...READ_FIELDS.get_cpu_scheduler_pressure.stat,
          },
          {
            title: "Top Queries by CPU",
            read: "get_top_queries_by_cpu",
            params: { server, hours: 24, top: 20 },
            viz: "table",
            span: 2,
            ...READ_FIELDS.get_top_queries_by_cpu.table,
          },
          {
            title: "Top Procedures by CPU",
            read: "get_top_procedures_by_cpu",
            params: { server, hours: 24, top: 20 },
            viz: "table",
            span: 2,
            ...READ_FIELDS.get_top_procedures_by_cpu.table,
          },
        ],
      },
    }),
  },

  {
    key: "blocking-deadlocks",
    label: "Blocking and deadlocks",
    description: "When contention spiked and what was contended: both trends, the blocking chains, and the deadlocks with their victims.",
    make: (server) => ({
      name: "Blocking and deadlocks — " + server,
      description: "Contention trends, blocking chains and deadlocks on " + server + ".",
      definition: {
        panels: [
          {
            title: "Blocking Events",
            read: "get_blocking_trend",
            params: { server, hours: 24 },
            viz: "line",
            rowsKey: "trend",
            xKey: "time",
            emptyText: "No blocking events in this window — an empty trend here means none happened, not that nothing was collected.",
            series: [{ key: "count", label: "Events" }],
            format: "int",
          },
          {
            title: "Deadlocks",
            read: "get_deadlock_trend",
            params: { server, hours: 24 },
            viz: "line",
            ...READ_FIELDS.get_deadlock_trend.line,
          },
          {
            title: "Blocking",
            read: "get_blocking",
            params: { server, hours: 24, limit: 30 },
            viz: "table",
            span: 2,
            ...READ_FIELDS.get_blocking.table,
          },
          {
            title: "Deadlocks",
            read: "get_deadlocks",
            params: { server, hours: 24, limit: 20 },
            viz: "table",
            span: 2,
            rowsKey: "deadlocks",
            emptyText: "No deadlocks in this window.",
            columns: [
              { key: "deadlock_time", label: "Deadlock Time", format: "time" },
              { key: "victim_sql_text", label: "Victim SQL", wrap: true },
              { key: "victim_process_id", label: "Victim" },
              { key: "process_summary", label: "Processes", wrap: true },
            ],
          },
        ],
      },
    }),
  },

  {
    key: "memory-pressure",
    label: "Memory pressure",
    description: "Where the memory went: the target-versus-total curve, the clerks holding it, grant activity, and plan-cache bloat.",
    make: (server) => ({
      name: "Memory pressure — " + server,
      description: "Memory split, clerks, grants and plan-cache bloat on " + server + ".",
      definition: {
        panels: [
          {
            title: "Memory",
            read: "get_memory_stats",
            params: { server },
            viz: "stat",
            span: 2,
            stats: [
              /* On an Azure SQL Database (engine_edition 5) the memory figures are the database's memory limit and the room left
                 under it, so those tiles are captioned that way; panels.js reads `hideWhen` and `showWhen`. The condition is written
                 out in each tile because this file is a literal that the template tests read without running it. */
              { key: "total_physical_memory_mb", label: "Physical", format: "mb", hideWhen: { key: "engine_edition", equals: 5 } },
              { key: "total_physical_memory_mb", label: "Memory limit", format: "mb", showWhen: { key: "engine_edition", equals: 5 } },
              { key: "available_physical_memory_mb", label: "Available", format: "mb", hideWhen: { key: "engine_edition", equals: 5 } },
              { key: "available_physical_memory_mb", label: "Available under limit", format: "mb", showWhen: { key: "engine_edition", equals: 5 } },
              { key: "memory_utilization_pct", label: "Utilization", format: "pct" },
              { key: "total_server_memory_mb", label: "Total server", format: "mb" },
              { key: "target_server_memory_mb", label: "Target server", format: "mb" },
              { key: "buffer_pool_mb", label: "Buffer pool", format: "mb" },
              { key: "plan_cache_mb", label: "Plan cache", format: "mb" },
              { key: "system_memory_state", label: "System state", format: "text", small: true, nullKey: "system_memory_state_note" },
            ],
          },
          {
            title: "Memory Trend",
            read: "get_memory_trend",
            params: { server, hours: 24 },
            viz: "line",
            span: 2,
            rowsKey: "trend",
            xKey: "time",
            format: "mb",
            emptyText: "No memory samples in this window.",
            series: [
              { key: "total_server_memory_mb", label: "Total Server" },
              { key: "target_server_memory_mb", label: "Target" },
              { key: "buffer_pool_mb", label: "Buffer Pool" },
              { key: "plan_cache_mb", label: "Plan Cache" },
            ],
          },
          {
            title: "Memory Clerks",
            read: "get_memory_clerks",
            params: { server },
            viz: "table",
            rowsKey: "clerks",
            emptyText: "No memory clerks in the latest snapshot — the clerk collector may not have run yet.",
            columns: [
              { key: "clerk_type", label: "Clerk" },
              { key: "memory_mb", label: "Memory", format: "mb" },
            ],
          },
          {
            title: "Memory Grants",
            read: "get_memory_grants",
            params: { server, hours: 24 },
            viz: "line",
            rowsKey: "grants",
            xKey: "collection_time",
            format: "mb",
            emptyText: "No memory grant samples in this window.",
            series: [
              { key: "granted_memory_mb", label: "Granted" },
              { key: "used_memory_mb", label: "Used" },
              { key: "available_memory_mb", label: "Available" },
            ],
          },
          {
            title: "Plan Cache",
            read: "get_plan_cache_bloat",
            params: { server, hours: 24 },
            viz: "stat",
            span: 2,
            stats: [
              { key: "summary.bloat_level", label: "Bloat", format: "text", small: true },
              { key: "summary.total_plans", label: "Plans", format: "int" },
              { key: "summary.single_use_plans", label: "Single-use", format: "int" },
              { key: "summary.single_use_percent", label: "Single-use %", format: "num1" },
              { key: "summary.total_size_mb", label: "Cache size", format: "mb" },
              { key: "summary.wasted_percent", label: "Wasted %", format: "num1" },
            ],
          },
        ],
      },
    }),
  },

  {
    key: "configuration-review",
    label: "Configuration review",
    description: "What this server is set to and what the audit thinks of it: sp_configure, database options, trace flags, and recent changes.",
    make: (server) => ({
      name: "Configuration review — " + server,
      description: "Audit findings, server and database configuration, trace flags and recent changes on " + server + ".",
      definition: {
        panels: [
          {
            title: "Configuration Audit",
            read: "audit_config",
            params: { server },
            viz: "table",
            span: 2,
            rowsKey: "recommendations",
            emptyText: "The audit found nothing to flag.",
            columns: [
              { key: "setting", label: "Setting" },
              { key: "status", label: "Status", statusSev: true },
              { key: "current_value", label: "Current" },
              { key: "suggested_value", label: "Suggested" },
              { key: "recommendation", label: "Why", wrap: true },
            ],
          },
          {
            title: "Server Configuration",
            read: "get_server_config",
            params: { server },
            viz: "table",
            span: 2,
            rowsKey: "settings",
            emptyText: "No sp_configure snapshot yet.",
            columns: [
              { key: "name", label: "Setting" },
              { key: "value_configured", label: "Configured" },
              { key: "value_in_use", label: "In use" },
              { key: "values_match", label: "Match", format: "bool" },
              { key: "is_advanced", label: "Advanced", format: "bool" },
            ],
          },
          {
            title: "Database Configuration",
            read: "get_database_config",
            params: { server },
            viz: "table",
            span: 2,
            rowsKey: "databases",
            emptyText: "No database configuration snapshot yet.",
            columns: [
              { key: "database_name", label: "Database" },
              { key: "state", label: "State" },
              { key: "compatibility_level", label: "Compat", format: "int" },
              { key: "recovery_model", label: "Recovery" },
              { key: "rcsi", label: "RCSI", format: "bool" },
              { key: "auto_shrink", label: "Auto shrink", format: "bool" },
              { key: "query_store", label: "Query Store" },
              { key: "page_verify", label: "Page verify" },
            ],
          },
          {
            title: "Trace Flags",
            read: "get_trace_flags",
            params: { server },
            viz: "table",
            rowsKey: "trace_flags",
            emptyText: "No trace flags are enabled on this server.",
            columns: [
              { key: "trace_flag", label: "Flag", format: "int" },
              { key: "enabled", label: "Enabled", format: "bool" },
              { key: "is_global", label: "Global", format: "bool" },
            ],
          },
          {
            title: "Server Configuration Changes",
            read: "get_server_config_changes",
            params: { server, hours: 168 },
            viz: "table",
            span: 2,
            rowsKey: "changes",
            emptyText: "No server configuration changed in the last week.",
            columns: [
              { key: "change_time", label: "Changed", format: "time" },
              { key: "configuration_name", label: "Setting" },
              { key: "old_value_configured", label: "Old" },
              { key: "new_value_configured", label: "New" },
            ],
          },
        ],
      },
    }),
  },
];

/** A template by key, or null. */
export function findTemplate(key) {
  return DASHBOARD_TEMPLATES.find((t) => t.key === key) || null;
}
