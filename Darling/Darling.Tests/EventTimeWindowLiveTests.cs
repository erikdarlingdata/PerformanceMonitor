/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// "Last 4 hours" counts the blocking and deadlock events that HAPPENED in the last 4 hours, as Lite does.
/// Each read gets the same pair of seeds: an event that happened before the window but was collected inside
/// it (a late pick-up, or a replayed ring buffer) must not be listed, and an event that happened inside the
/// window must be. <c>collection_time</c> stays a chunk-exclusion bound only.
/// </summary>
[Collection("live-postgres")]
public sealed class EventTimeWindowLiveTests
{
    private const string ServerName = "darling-event-time-window-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const int OldSpid = 111;
    private const int NewSpid = 222;

    [Fact]
    public async Task BlockingAndDeadlockReads_WindowOnEventTime_NotCollectionTime()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live event-time window test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
            var collectedRecently = now.AddMinutes(-20);

            /* Old: happened 6 h ago, collected 20 minutes ago. New: happened 30 minutes ago, collected 20 minutes ago. */
            foreach (var (spid, happened) in new[] { (OldSpid, now.AddHours(-6)), (NewSpid, now.AddMinutes(-30)) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, database_name)
VALUES ($1,$2,$3,$4,$5,12000,60,$6,'AppDb')",
                    CollectionIdGenerator.Next(), collectedRecently, ServerId, ServerName, happened, spid);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,'select 1','<deadlock/>')",
                    CollectionIdGenerator.Next(), collectedRecently, ServerId, ServerName, happened, $"process{spid}");
            }

            var start = now.AddHours(-4);
            var end = now.AddMinutes(1);
            var viewer = new ViewerDataService(cs!);

            /* MCP get_blocking / get_deadlocks readers. */
            var mcpBlocking = await DarlingBlockingReader.GetRecentBlockedProcessReportsAsync(postgres, ServerId, start, end, 50, ct);
            Assert.Equal([NewSpid], mcpBlocking.Select(r => r.BlockedSpid).ToArray());
            var mcpDeadlocks = await DarlingBlockingReader.GetRecentDeadlocksAsync(postgres, ServerId, start, end, 50, cancellationToken: ct);
            Assert.Equal([$"process{NewSpid}"], mcpDeadlocks.Select(r => r.VictimProcessId).ToArray());
            var mcpGraphs = await DarlingBlockingReader.GetRecentDeadlocksAsync(postgres, ServerId, start, end, 50, graphOnly: true, cancellationToken: ct);
            Assert.Equal([$"process{NewSpid}"], mcpGraphs.Select(r => r.VictimProcessId).ToArray());

            /* Viewer grids. */
            var gridBlocking = await viewer.GetRecentBlockedProcessReportsAsync(ServerId, start, end, cancellationToken: ct);
            Assert.Equal([NewSpid], gridBlocking.Where(r => r.BlockedSpid is OldSpid or NewSpid).Select(r => r.BlockedSpid).ToArray());
            var gridDeadlocks = await viewer.GetRecentDeadlocksAsync(ServerId, start, end, ct);
            Assert.Equal([$"process{NewSpid}"], gridDeadlocks.Select(r => r.VictimProcessId).ToArray());

            /* Alert rolling-window reads (hoursBack 4). */
            var adapter = new DarlingAlertReadAdapter(postgres);
            var key = ServerId.ToString(CultureInfo.InvariantCulture);
            var alertBlocking = await adapter.GetRecentBlockedProcessReportsAsync(key, 4, ct);
            Assert.Equal([NewSpid], alertBlocking.Select(r => r.BlockedSpid).ToArray());
            var alertDeadlocks = await adapter.GetRecentDeadlocksAsync(key, 4, ct);
            Assert.Equal([$"process{NewSpid}"], alertDeadlocks.Select(r => r.VictimProcessId).ToArray());

            /* MCP deadlock severity graphs and per-minute deadlock trend. */
            var graphs = await DarlingDataReader.GetDeadlockGraphsAsync(postgres, ServerId, start, end, ct);
            Assert.Single(graphs);
            var trend = await DarlingBlockingTrendReader.GetDeadlockTrendAsync(postgres, ServerId, start, end, ct);
            Assert.Equal(1, trend.Sum(p => p.Count));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The analysis drill-down reads have no live seam short of a full analysis run, so their SQL is
    /// pinned: each must carry the event-time predicate beside the collection-time bound.</summary>
    [Fact]
    public void DrillDownDeadlockAndBlockingSql_CarryEventTimePredicate()
    {
        Assert.Contains("deadlock_time", PgDrillDownCollector.TopDeadlocksSql.Split("ORDER BY")[0], StringComparison.Ordinal);
        Assert.Contains("event_time", PgDrillDownCollector.TopBlockingChainsSql.Split("UNION ALL")[0], StringComparison.Ordinal);
    }

    /// <summary>Lite already windows on the event time (build-only guard): the overview deadlock count and the
    /// blocking trend/stats reads carry <c>deadlock_time</c> / <c>event_time</c> bounds.</summary>
    [Fact]
    public void LiteWindows_OnEventTime_Guard()
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !Directory.Exists(Path.Combine(dir, "Lite", "Services"))) dir = Path.GetDirectoryName(dir);
        Assert.NotNull(dir);
        var overview = File.ReadAllText(Path.Combine(dir!, "Lite", "Services", "LocalDataService.Overview.cs"));
        Assert.Contains("AND   deadlock_time >= $2", overview, StringComparison.Ordinal);
        var stats = File.ReadAllText(Path.Combine(dir!, "Lite", "Services", "LocalDataService.BlockingStats.cs"));
        Assert.Contains("event_time >= $2 AND event_time <= $3", stats, StringComparison.Ordinal);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; DELETE FROM deadlocks WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
