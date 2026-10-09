/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>What a windowed web / MCP read returns, which decides how far back it may ever be asked to reach (#5562).</summary>
internal enum ReadShape
{
    /// <summary>Points grouped into time buckets (<c>bucket_minutes</c>): the payload is bounded by the bucket budget,
    /// not by the window, so it is the only shape allowed past <see cref="McpHelpers.MaxHoursBack"/>.</summary>
    BucketedTrend,

    /// <summary>A list or a ranking (top-N, newest-first rows, an unbucketed series): its payload grows with the window,
    /// so it stays at <see cref="McpHelpers.MaxHoursBack"/>.</summary>
    List,

    /// <summary>The latest snapshot: the window is only a freshness bound, so a longer range means nothing here.</summary>
    LatestSnapshot,
}

/// <summary>
/// One windowed read's reach: its <see cref="Shape"/>, the longest window it accepts (<see cref="MaxHours"/>, the number
/// the read's own validator enforces) and the collector whose cadence decides how many samples a short window holds.
/// </summary>
/// <param name="Shape">What the read returns.</param>
/// <param name="MaxHours">The longest <c>hours</c> the read accepts; the web picker greys out anything longer.</param>
/// <param name="Collector">The main collector id (a <see cref="CollectorScheduleDefaults"/> key), or null for a read
/// that no collector feeds (analysis, alert history, the store's own self-monitoring).</param>
internal sealed record ReadReach(ReadShape Shape, int MaxHours, string? Collector);

/// <summary>
/// The ONE table of how far back each windowed web read reaches and which collector feeds it (#5562, rulings R2 and R3).
///
/// <para><b>Why a table.</b> The picker on the server page offers presets from five minutes to a quarter, and it must
/// grey out a period a tab cannot read, with the reason. The answer cannot live in the page: the read's validator is the
/// authority, and a page that guessed would offer a window the read then refuses. So the validator of every read that
/// reaches past a week takes its ceiling from here (<see cref="MaxHoursFor"/>), the catalog serves the same number as
/// <c>max_hours</c> on the read's <c>hours</c> parameter, and a census (<c>WebReadReachTests</c>) fails when a windowed
/// read is added without a row, so a new read cannot ship with no declared reach.</para>
///
/// <para><b>What reaches past seven days.</b> Only a BUCKETED trend read that routes through the hourly rollup the
/// Custom Views way (<c>RetentionTierRouter</c>, <c>DurationTrendRouting</c>, <c>QueryStoreTrendRouting</c>): the
/// query, procedure and Query Store duration trends, at <see cref="RollupTrendHours"/> (90 days, what the rollups
/// keep). Their payload is whole-hour points, so 90 days is at most 2,160 of them. Lists and rankings stay at
/// <see cref="McpHelpers.MaxHoursBack"/> even where they route through a rollup (a top-N over 90 days is a different
/// payload, and the MCP ceiling rises for bucketed trends only); query text and plans are kept for a few days only
/// (<c>RetentionTierRouter.ClampToTextHorizon</c>), and none of the three opted-in reads carries either.
/// The raw-table bucketed trends reach <see cref="RawTrendHours"/> (30 days, the raw tables' default retention) once a
/// timing run on a large store measured a month under 10 seconds (#5562 L4b); a trend not yet timed, or over a table
/// that keeps less than 30 days on EITHER store type (plain PostgreSQL, or the raw tier a TimescaleDB store drops at four days), stays at 168 hours. Raising one is a one-line change to its row here, plus its
/// validator call, and nothing else. Alert History is the one list that reaches past a week (ruling R8): its reach is
/// the alert table's retention, <see cref="AlertHistoryHours"/>, and the read is capped by its row limit.</para>
/// </summary>
internal static class WebReadReach
{
    /// <summary>The reach of every read that has not opted in: <see cref="McpHelpers.MaxHoursBack"/>.</summary>
    public const int DefaultHours = McpHelpers.MaxHoursBack;

    /// <summary>The rollup-routed trends' reach: 90 days, the hourly rollup's retention (and Custom Views'
    /// <c>ComposeLimits.MaxWindowHours</c>).</summary>
    public const int RollupTrendHours = 24 * 90;

    /// <summary>The raw-table trends' reach: 30 days (#5562 L4b), what every raw collector table keeps by default.
    /// A read over a table with a shorter retention (<c>waiting_tasks</c> keeps 7 days) stays at
    /// <see cref="DefaultHours"/>, and a read that spans tables takes the shortest.</summary>
    public const int RawTrendHours = 24 * DarlingRetentionHorizons.DataRetentionBaseDays;

    /// <summary>Alert History's reach: the alert table's retention, 90 days (#5562 ruling R8), the longest choice the
    /// Viewer already offers (<see cref="DarlingRetentionHorizons.AlertHistoryRetentionDays"/>).</summary>
    public const int AlertHistoryHours = 24 * DarlingRetentionHorizons.AlertHistoryRetentionDays;

    /// <summary>
    /// A view of one read that reaches further than the read's own row (#5562 L4b): <c>get_finops</c> serves its other views
    /// at <see cref="DefaultHours"/>, but its Storage Growth view takes up to 90 days of whole days, validated by
    /// <see cref="Mcp.DarlingMcpFinOpsTools.MaxStorageGrowthHoursBack"/>. The catalog serves this as <c>view_max_hours</c> on the
    /// read's <c>hours</c> param, and the page reads it from there, so the page, the catalog and the server share one number.
    /// </summary>
    public static readonly IReadOnlyDictionary<string, IReadOnlyDictionary<string, int>> ViewMaxHours =
        new Dictionary<string, IReadOnlyDictionary<string, int>>(StringComparer.Ordinal)
        {
            ["get_finops"] = new Dictionary<string, int>(StringComparer.Ordinal)
            {
                ["storage_growth"] = Mcp.DarlingMcpFinOpsTools.MaxStorageGrowthHoursBack,
            },
        };

    private static ReadReach Trend(string? collector, int maxHours = DefaultHours) => new(ReadShape.BucketedTrend, maxHours, collector);
    private static ReadReach List(string? collector, int maxHours = DefaultHours) => new(ReadShape.List, maxHours, collector);
    private static ReadReach Snapshot(string? collector) => new(ReadShape.LatestSnapshot, DefaultHours, collector);

    /// <summary>Every windowed read (one with an <c>hours</c> parameter in the web catalog), by read name.</summary>
    public static readonly IReadOnlyDictionary<string, ReadReach> All = new Dictionary<string, ReadReach>(StringComparer.Ordinal)
    {
        /* Rollup-routed bucketed trends: the only reads past 168 hours. */
        ["get_query_duration_trend"] = Trend("query_stats", RollupTrendHours),
        ["get_procedure_duration_trend"] = Trend("procedure_stats", RollupTrendHours),
        ["get_query_store_duration_trend"] = Trend("query_store", RollupTrendHours),

        /* Raw-table bucketed trends: 720 hours where the large-store timing run measured a month under 10 s (#5562 L4b;
           each validator names its time). Left at 168: get_current_waits_trend (its waiting_tasks table keeps 7 days),
           get_server_trend (6 of its 11 metrics were never timed), get_pg_wait_trend (its timing table was empty),
           get_pg_query_duration_trend (not timed) and get_query_heatmap (raw query_stats, which a TimescaleDB
           store drops at four days, so a month is a window the table cannot fill). get_perfmon_trend reaches 720 since #5574 re-grouped
           the compressed perfmon_stats data by counter: the cold read at 30 days on a large store fell from 11.2 s to 1.3 s, inside the 10 s bar.
           get_blocking_stats is uncapped on the MCP side (ValidateUncappedWindow). */
        ["get_blocking_trend"] = Trend("blocked_process_report", RawTrendHours),
        ["get_deadlock_trend"] = Trend("deadlocks", RawTrendHours),
        ["get_lock_wait_trend"] = Trend("wait_stats", RawTrendHours),
        ["get_current_waits_trend"] = Trend("waiting_tasks"),
        ["get_blocking_stats"] = Trend("dmv_blocking_snapshot", RawTrendHours),
        ["get_cpu_utilization"] = Trend("cpu_utilization", RawTrendHours),
        ["get_query_heatmap"] = Trend("query_stats"),
        ["get_tempdb_trend"] = Trend("tempdb_stats", RawTrendHours),
        ["get_wait_trend"] = Trend("wait_stats", RawTrendHours),
        ["get_file_io_trend"] = Trend("file_io_stats", RawTrendHours),
        ["get_memory_trend"] = Trend("memory_stats", RawTrendHours),
        ["get_server_trend"] = Trend(null),
        ["get_perfmon_trend"] = Trend("perfmon_stats", RawTrendHours),
        ["get_pg_cpu_utilization"] = Trend("pg_cpu_utilization", RawTrendHours),
        ["get_pg_wait_trend"] = Trend("pg_wait_sampling"),
        ["get_pg_query_duration_trend"] = Trend("pg_statement_stats"),
        ["get_pg_io_trend"] = Trend("pg_io_stats", RawTrendHours),
        ["get_pg_database_trend"] = Trend("pg_database_stats", RawTrendHours),

        /* Lists, rankings and unbucketed series. */
        ["get_top_queries_by_cpu"] = List("query_stats"),
        ["get_top_procedures_by_cpu"] = List("procedure_stats"),
        ["get_query_trend"] = List("query_stats"),
        ["get_query_store_top"] = List("query_store"),
        ["get_query_store_regressions"] = List("query_store"),
        ["get_query_store_clutter"] = List("query_store"),
        ["get_query_store_query_history"] = List("query_store"),
        ["get_long_query_completions"] = List("long_query_completions"),
        ["get_plan_corrections"] = List("plan_correction"),
        ["get_blocked_process_xml"] = List("blocked_process_report"),
        ["get_blocking"] = List("blocked_process_report"),
        ["get_deadlocks"] = List("deadlocks"),
        ["get_deadlock_detail"] = List("deadlocks"),
        ["get_database_config_changes"] = List("database_config"),
        ["get_server_config_changes"] = List("server_config"),
        ["get_trace_flag_changes"] = List("trace_flags"),
        ["get_collection_log"] = List(null),
        ["get_wait_stats"] = List("wait_stats"),
        ["get_wait_types"] = List("wait_stats"),
        ["get_latch_stats"] = List("latch_stats"),
        ["get_spinlock_stats"] = List("spinlock_stats"),
        ["get_memory_pressure_events"] = List("memory_pressure_events"),
        ["get_resource_semaphore"] = List("memory_grant_stats"),
        ["get_plan_cache_bloat"] = List("plan_cache_stats"),
        ["get_job_history"] = List("job_history"),
        ["get_default_trace_events"] = List("default_trace_events"),
        ["get_health_parser_cpu_tasks"] = List("system_health_events"),
        ["get_health_parser_io_issues"] = List("system_health_events"),
        ["get_health_parser_memory_broker"] = List("system_health_events"),
        ["get_health_parser_memory_conditions"] = List("system_health_events"),
        ["get_health_parser_memory_node_oom"] = List("system_health_events"),
        ["get_health_parser_scheduler_issues"] = List("system_health_events"),
        ["get_health_parser_severe_errors"] = List("system_health_events"),
        ["get_health_parser_significant_waits"] = List("system_health_events"),
        ["get_health_parser_system_health"] = List("system_health_events"),
        ["get_pg_top_queries"] = List("pg_statement_stats"),
        ["get_pg_plans"] = List("pg_plan_capture"),
        ["get_pg_io_stats"] = List("pg_io_stats"),
        ["get_pg_wait_stats"] = List("pg_wait_stats"),
        ["get_pg_wait_sampling"] = List("pg_wait_sampling"),
        ["get_pg_kernel_stats"] = List("pg_kernel_stats"),
        ["get_pg_predicate_stats"] = List("pg_predicate_stats"),
        ["get_pg_lock_stats"] = List("pg_lock_stats"),
        ["get_pg_write_stats"] = List("pg_write_stats"),
        ["get_pg_server_config_changes"] = List("pg_server_config"),
        ["get_pg_deadlocks"] = List("pg_deadlocks"),
        ["get_pg_log_events"] = List("pg_log_events"),
        ["get_pg_blocking"] = List("pg_blocking"),
        ["get_pg_database_stats"] = List("pg_database_stats"),
        ["get_pg_session_states"] = List("pg_session_states"),

        /* Latest snapshots: the window is a freshness bound. */
        ["get_active_queries"] = Snapshot("query_snapshots"),
        ["get_waiting_tasks"] = Snapshot("waiting_tasks"),
        ["get_memory_grants"] = Snapshot("memory_grant_stats"),
        ["get_cpu_scheduler_pressure"] = Snapshot("cpu_scheduler_stats"),
        ["get_pg_plan_capture_readiness"] = Snapshot("pg_plan_capture_readiness"),
        ["get_pg_wraparound_risk"] = Snapshot("pg_wraparound_stats"),
        ["get_pg_xmin_horizon"] = Snapshot("pg_xmin_horizon"),
        ["get_pg_replication_slots"] = Snapshot("pg_replication_slots"),
        ["get_pg_replication_stats"] = Snapshot("pg_replication_stats"),
        ["get_pg_autovacuum_health"] = Snapshot("pg_autovacuum_stats"),
        ["get_pg_index_bloat"] = Snapshot("pg_index_bloat"),
        ["get_pg_column_stats"] = Snapshot("pg_column_stats"),
        ["get_pg_buffer_usage"] = Snapshot("pg_buffer_usage"),
        ["get_pg_extensions"] = Snapshot("pg_extension_availability"),
        ["get_pg_index_usage"] = Snapshot("pg_index_usage_stats"),
        ["get_pg_table_bloat"] = Snapshot("pg_table_bloat_stats"),

        /* No collector feeds these: analysis, alerts, fleet, and the monitoring tool's own self-monitoring. */
        ["compare_analysis"] = List(null),
        ["get_analysis_facts"] = List(null),
        ["get_analysis_findings"] = List(null),
        ["get_alert_history"] = List(null, AlertHistoryHours),
        ["get_fleet_overview"] = List(null),
        ["get_sweep_reports"] = List(null),
        ["get_store_log"] = List(null),
        ["get_store_query_history"] = List(null),
        ["get_slow_reads"] = List(null),
        ["get_read_latency"] = List(null),
        ["get_finops"] = List(null),
    };

    /// <summary>
    /// The ceiling a read's validator passes to <see cref="McpHelpers.ValidateWindow(int, string?, int, out DateTime)"/>.
    /// Throws for a read with no row: a validator that names a read the table does not know has drifted from the table,
    /// and failing loudly beats quietly accepting 168 for a read the picker was told reaches further.
    /// </summary>
    public static int MaxHoursFor(string read) =>
        All.TryGetValue(read, out var reach)
            ? reach.MaxHours
            : throw new InvalidOperationException($"#5562: '{read}' has no row in WebReadReach.All.");

    /// <summary>
    /// The collector's interval in minutes: <paramref name="overrides"/> (the schedule rows the store holds, per server
    /// when <paramref name="serverId"/> is given, else only the fleet-wide row) over the shipped default. Null for a read
    /// no collector feeds or a collector name the defaults do not know.
    /// </summary>
    public static int? CollectorIntervalMinutes(string? collector, int? serverId, IReadOnlyList<ScheduleOverride> overrides)
    {
        if (collector is null || !CollectorScheduleDefaults.All.ContainsKey(collector))
        {
            return null;
        }

        return CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes(collector, serverId ?? -1, overrides);
    }
}
