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

    private static readonly string[] NoTables = [];

    public static TheoryData<string> Families() => new()
    {
        "get_active_queries", "get_waiting_tasks", "get_blocking", "get_deadlocks", "get_deadlock_detail", "get_blocked_process_xml",
        "get_long_query_completions", "get_alert_history", "get_analysis_findings", "get_query_store_clutter",
        "finops_application_connections", "finops_database_resources", "finops_high_impact", "finops_optimization",
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
        _ => throw new ArgumentOutOfRangeException(nameof(family), family, null),
    };

    /// <summary>Last collected ten days ago: the default window holds nothing and the server is Offline by list_servers' rule.</summary>
    [Theory]
    [MemberData(nameof(Families))]
    public async Task AnEmptyRead_FromAnOfflineServer_IsUnavailable_WithTheOfflineLeadFirst_AgainstDevPostgres(string family)
    {
        var name = "sql7167-off-" + family.Replace('_', '-');
        await WindowFloorLiveHarness.RunAsync(ConnectionString, [], [name], NoTables, async (c, ds, now) =>
        {
            var lastCollection = now.AddDays(-10);
            await WindowFloorLiveHarness.SeedServerAsync(
                c, name, now.AddDays(-40), "wait_stats", lastCollection.AddHours(-3), 30, lastCollection, NoTables, TestContext.Current.CancellationToken);

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

            var root = WindowFloorLiveHarness.Parse(await ReadAsync(family, ds, name));

            var message = root.TryGetProperty("message", out var m) ? m.GetString() ?? "" : "";
            Assert.DoesNotContain("is offline", message, StringComparison.Ordinal);
        });
    }
}
