/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7167: every SQL Server read that answers <c>empty</c> for a window the store holds nothing for goes through the offline wrapper, so
/// a server the store has had no collection from for days answers <c>unavailable</c> with the offline sentence first, one tool family
/// at a time. A server being collected is not given the offline answer.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpSqlServerOfflineEmptyFamiliesLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly string[] NoTables = ["wait_stats", "query_stats", "procedure_stats", "query_store_stats", "system_health_events"];

    /// <summary>
    /// One old row in each collector table the routed reads probe, so the read finds the collector HAS stored data (the empty answer,
    /// not the never-collected one). Every row sits inside a 7-day window and before the default 24-hour windows.
    /// </summary>
    private static async Task SeedHistoryAsync(NpgsqlConnection c, string name, DateTime at)
    {
        var serverId = ServerIdHelper.GetDeterministicHashCode(name);
        var ct = TestContext.Current.CancellationToken;
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, 'SOS_SCHEDULER_YIELD', 5, 50)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at.AddHours(-1)), serverId, name);
        foreach (var table in new[] { "query_stats", "procedure_stats", "query_store_stats" })
        {
            await DarlingMcpTestData.ExecAsync(c, ct,
                $"INSERT INTO {table} (collection_id, collection_time, server_id, server_name) VALUES ($1, $2, $3, $4)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at.AddHours(-1)), serverId, name);
        }

        /* Three successful runs of each collector that stored no row: the empty-page answers read "the collector looked and nothing ran". */
        foreach (var collector in new[] { "procedure_stats", "query_store", "query_stats" })
        {
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT (SELECT COALESCE(MAX(log_id), 0) FROM collection_log) + row_number() OVER (), $1, $2, $3, t, 12, 'SUCCESS', 0
FROM generate_series($4::timestamp - interval '3 hours', $4::timestamp - interval '1 hour', interval '1 hour') AS t",
                serverId, name, collector, DarlingMcpTestData.Naive(at));
        }

        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO system_health_events (system_health_event_id, collection_time, server_id, server_name, event_type, event_xml) VALUES ($1, $2, $3, $4, 'other', '<event/>')",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, name);
    }

    public static TheoryData<string> Families() => new()
    {
        "get_active_queries", "get_waiting_tasks", "get_blocking", "get_deadlocks", "get_deadlock_detail", "get_blocked_process_xml",
        "get_long_query_completions", "get_alert_history", "get_analysis_findings", "get_query_store_clutter",
        "finops_application_connections", "finops_database_resources", "finops_high_impact", "finops_optimization",
        "get_wait_trend", "get_top_queries_by_cpu", "get_top_procedures_by_cpu", "get_query_trend",
        "get_query_duration_trend", "get_procedure_duration_trend", "get_query_store_duration_trend", "get_lock_wait_trend",
        "get_server_config_changes", "get_database_config_changes", "get_trace_flag_changes",
        "get_query_store_aborts", "get_query_store_query_history", "get_cpu_by_database",
        "get_health_parser_system_health", "get_health_parser_severe_errors",
    };

    private static Task<string> ReadAsync(string family, NpgsqlDataSource ds, string name) => family switch
    {
        "get_active_queries" => DarlingMcpSessionTools.GetActiveQueries(ds, name),
        "get_waiting_tasks" => DarlingMcpSessionTools.GetWaitingTasks(ds, name),
        "get_blocking" => DarlingMcpBlockingTools.GetBlocking(ds, name),
        "get_deadlocks" => DarlingMcpBlockingTools.GetDeadlocks(ds, name),
        "get_deadlock_detail" => DarlingMcpBlockingTools.GetDeadlockDetail(ds, name),
        "get_blocked_process_xml" => DarlingMcpBlockingTools.GetBlockedProcessXml(ds, name),
        "get_long_query_completions" => DarlingMcpLongQueryTools.GetLongQueryCompletions(ds, name),
        "get_alert_history" => DarlingMcpAlertTools.GetAlertHistory(ds, name),
        "get_analysis_findings" => DarlingMcpTools.GetAnalysisFindings(new DarlingAnalysisService(ds), ds, name),
        "get_query_store_clutter" => DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(ds, name, database_name: "nosuchdb"),
        "finops_application_connections" => DarlingMcpFinOpsTools.GetFinOps(ds, DarlingMcpFinOpsTools.ApplicationConnectionsView, name),
        "finops_database_resources" => DarlingMcpFinOpsTools.GetFinOps(ds, DarlingMcpFinOpsTools.DatabaseResourcesView, name),
        "finops_high_impact" => DarlingMcpFinOpsTools.GetFinOps(ds, DarlingMcpFinOpsTools.HighImpactView, name),
        "finops_optimization" => DarlingMcpFinOpsTools.GetFinOps(ds, DarlingMcpFinOpsTools.OptimizationView, name),
        "get_wait_trend" => DarlingMcpDataTools.GetWaitTrend(ds, "PAGEIOLATCH_SH", name, hours_back: 168),
        "get_top_queries_by_cpu" => DarlingMcpDataTools.GetTopQueriesByCpu(ds, name, hours_back: 168),
        "get_top_procedures_by_cpu" => DarlingMcpDataTools.GetTopProceduresByCpu(ds, name, hours_back: 168, database_name: "nosuchdb"),
        "get_query_trend" => DarlingMcpTrendTools.GetQueryTrend(ds, "0x0102030405060708", "nosuchdb", name),
        "get_query_duration_trend" => DarlingMcpTrendTools.GetQueryDurationTrend(ds, name),
        "get_procedure_duration_trend" => DarlingMcpTrendTools.GetProcedureDurationTrend(ds, name),
        "get_query_store_duration_trend" => DarlingMcpTrendTools.GetQueryStoreDurationTrend(ds, name),
        "get_lock_wait_trend" => DarlingMcpBlockingTools.GetLockWaitTrend(ds, name),
        "get_server_config_changes" => DarlingMcpConfigHistoryTools.GetServerConfigChanges(ds, name),
        "get_database_config_changes" => DarlingMcpConfigHistoryTools.GetDatabaseConfigChanges(ds, name),
        "get_trace_flag_changes" => DarlingMcpConfigHistoryTools.GetTraceFlagChanges(ds, name),
        "get_query_store_aborts" => DarlingMcpQueryStoreAbortsTools.GetQueryStoreAborts(ds, name),
        "get_query_store_query_history" => DarlingMcpQueryStoreHistoryTools.GetQueryStoreQueryHistory(ds, "nosuchdb", 1, name),
        "get_cpu_by_database" => DarlingMcpWhoDroveItTools.GetCpuByDatabase(ds, name),
        "get_health_parser_system_health" => DarlingMcpHealthParserTools.GetSystemHealth(ds, name),
        "get_health_parser_severe_errors" => DarlingMcpHealthParserTools.GetSevereErrors(ds, name),
        _ => throw new ArgumentOutOfRangeException(nameof(family), family, null),
    };

    /// <summary>Last collected five days ago: the default window holds nothing and the server is Offline by list_servers' rule.</summary>
    [Theory]
    [MemberData(nameof(Families))]
    public async Task AnEmptyRead_FromAnOfflineServer_IsUnavailable_WithTheOfflineLeadFirst_AgainstDevPostgres(string family)
    {
        var name = "sql7167-off-" + family.Replace('_', '-');
        await WindowFloorLiveHarness.RunAsync(ConnectionString, [], [name], NoTables, async (c, ds, now) =>
        {
            var lastCollection = now.AddDays(-5);
            await WindowFloorLiveHarness.SeedServerAsync(
                c, name, now.AddDays(-40), "wait_stats", lastCollection.AddHours(-3), 30, lastCollection, NoTables, TestContext.Current.CancellationToken);
            await SeedHistoryAsync(c, name, lastCollection);

            var root = WindowFloorLiveHarness.Parse(await ReadAsync(family, ds, name));

            Assert.Equal("unavailable", root.GetProperty("status").GetString());
            Assert.StartsWith(
                name + " is offline: the store has had no collection from it since "
                + lastCollection.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC",
                root.GetProperty("message").GetString()!,
                StringComparison.Ordinal);
        });
    }

    /// <summary>The same reads for a server collected a minute ago never carry the offline sentence.</summary>
    [Theory]
    [MemberData(nameof(Families))]
    public async Task AnEmptyRead_FromAServerCollectedJustNow_IsNotTheOfflineAnswer_AgainstDevPostgres(string family)
    {
        var name = "sql7167-on-" + family.Replace('_', '-');
        await WindowFloorLiveHarness.RunAsync(ConnectionString, [], [name], NoTables, async (c, ds, now) =>
        {
            await WindowFloorLiveHarness.SeedServerAsync(
                c, name, now.AddDays(-40), "wait_stats", DateTime.UtcNow.AddMinutes(-90), 1, DateTime.UtcNow.AddMinutes(-1), NoTables, TestContext.Current.CancellationToken);
            await SeedHistoryAsync(c, name, now.AddDays(-5));

            var root = WindowFloorLiveHarness.Parse(await ReadAsync(family, ds, name));

            var message = root.TryGetProperty("message", out var m) ? m.GetString() ?? "" : "";
            Assert.DoesNotContain("is offline", message, StringComparison.Ordinal);
        });
    }
}
