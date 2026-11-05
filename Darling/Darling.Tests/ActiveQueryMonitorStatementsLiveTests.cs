/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7573: on a quiet server every Active Queries row was one of the monitor's own statements. The rows stay stored; the read leaves out a
/// row whose stored <c>program_name</c> starts with the monitor's prefix, keeps a user's application that only contains the words and a row
/// with no program name, and brings them back when asked (<c>include_monitor</c>), for the rows, the snapshot list the strip draws, and the
/// MCP tool. Skips without <c>DARLING_TEST_PG</c>, like its siblings.
/// </summary>
[Collection("live-postgres")]
public sealed class ActiveQueryMonitorStatementsLiveTests
{
    private const string ServerName = "darling-active-queries-monitor-statements-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live active-query monitor-statement test.";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>One capture a minute for each program name, newest first, so the snapshot list holds one row per name.</summary>
    private static async Task<DateTime> SeedAsync(string cs, string?[] apps, CancellationToken ct)
    {
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        for (var i = 0; i < apps.Length; i++)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end.AddMinutes(-2 - i)), ServerId, ServerName, 60 + i, "AppDb", "00 00:00:05.000",
                "SELECT " + i, "running", 0, null, 0L, 1000L, 1000L, 100L, 0L, 2000L, 0.5m, "Read Committed", 1, 0, "sa", "APP01", (object?)apps[i] ?? DBNull.Value, 1, 0);
        }

        return end;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collect.query_snapshots WHERE server_id = {ServerId}; DELETE FROM collect.servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static string?[] AppsOf(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("queries").EnumerateArray()
            .Select(r => r.TryGetProperty("program_name", out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null)
            .OrderBy(a => a, StringComparer.Ordinal)
            .ToArray();

    /* The user rows are a similar name (the words inside a longer name), an ordinary application, and a NULL program name. The old read had no
       program predicate and returned all six rows, so the default assertions fail on it. */
    [Fact]
    public async Task TheMonitorsOwnStatements_AreLeftOutByDefault_AndKeptWhenAsked_AUsersSimilarNameStays()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = await SeedAsync(cs!, new string?[]
            {
                "PerformanceMonitorDarling", "PerformanceMonitorLite", "PerformanceMonitorDarling-Remediation",
                "MyPerformanceMonitorTool", "Orders.Api", null,
            }, ct);
            var user = new string?[] { null, "MyPerformanceMonitorTool", "Orders.Api" };

            var rows = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, end.AddHours(-1), end, 100, DatabaseFilter.All, cancellationToken: ct);
            Assert.Equal(user, rows.Rows.Select(r => r.ProgramName).OrderBy(a => a, StringComparer.Ordinal).ToArray());
            Assert.Equal(3, rows.PopulationCount);

            var all = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, end.AddHours(-1), end, 100, DatabaseFilter.All, cancellationToken: ct, includeMonitor: true);
            Assert.Equal(6, all.Rows.Count);

            /* The strip: the snapshot list holds one capture per kept row, and all six when asked. */
            var list = await DarlingSessionReader.GetActiveQuerySnapshotsAsync(postgres, ServerId, end.AddHours(-1), end, 100, DatabaseFilter.All, cancellationToken: ct);
            Assert.Equal(3, list.Snapshots.Count);
            var listAll = await DarlingSessionReader.GetActiveQuerySnapshotsAsync(postgres, ServerId, end.AddHours(-1), end, 100, DatabaseFilter.All, cancellationToken: ct, includeMonitor: true);
            Assert.Equal(6, listAll.Snapshots.Count);

            var tool = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 24, cancellationToken: ct);
            Assert.Equal(user, AppsOf(tool));
            var toolAll = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 24, include_monitor: true, cancellationToken: ct);
            Assert.Equal(6, AppsOf(toolAll).Length);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* The reads OUTSIDE Active Queries (the long-running query alert, the who-drove-it reads, the same-statement pile-up reader and the Viewer's
       wait drill-down) are not behind the switch: with the monitor's rows stored again they have to leave them out themselves, as they did while
       the collector dropped them (#7416). Six rows share one hash, wait and long elapsed time; three are the monitor's. Each read must see three. */
    [Fact]
    public async Task TheReadsOutsideActiveQueries_LeaveTheMonitorOut()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = await SeedAsync(cs!, new string?[]
            {
                "PerformanceMonitorDarling", "PerformanceMonitorLite", "PerformanceMonitorDarling-Remediation",
                "MyPerformanceMonitorTool", "Orders.Api", null,
            }, ct);
            using (var connection = new NpgsqlConnection(cs))
            {
                await connection.OpenAsync(ct);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    $"UPDATE collect.query_snapshots SET query_hash = '0xABC', wait_type = 'LCK_M_X', total_elapsed_time_ms = 600000, session_id = session_id + 60, collection_time = (SELECT MAX(collection_time) FROM collect.query_snapshots WHERE server_id = {ServerId}) WHERE server_id = {ServerId}");
            }

            var start = end.AddHours(-1);

            var groups = await DarlingWhoDroveItReader.ReadLoadByCallerAsync(postgres, ServerId, start, end, 20, 3, ct);
            Assert.Equal(3, groups.Count);
            Assert.DoesNotContain(groups, g => g.ProgramName.StartsWith("PerformanceMonitor", StringComparison.Ordinal));

            var pileups = await DarlingWhoDroveItReader.ReadPileupsAsync(postgres, ServerId, start, end, 2, 10, ct);
            Assert.Equal(3, Assert.Single(pileups).Sessions);

            var window = await new PerformanceMonitor.Darling.Analysis.PgPileupSnapshotReader(postgres).ReadWindowAsync(ServerId, start, ct);
            Assert.Equal(3, window.Count);

            var longRunning = await new PerformanceMonitor.Darling.Service.DarlingAlertReadAdapter(postgres).GetLongRunningQueriesAsync(
                ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture), thresholdMinutes: 5, maxResults: 50,
                excludeSpServerDiagnostics: true, excludeWaitFor: true, excludeBackups: true, excludeMiscWaits: true, excludeCdc: true,
                excludedDatabases: new System.Collections.Generic.List<string>(), PerformanceMonitor.Alerting.LongRunningQueryExclusions.None, cancellationToken: ct);
            Assert.Equal(3, longRunning.Sessions.Count);
            Assert.DoesNotContain(longRunning.Sessions, s => s.ProgramName.StartsWith("PerformanceMonitor", StringComparison.Ordinal));

            await using var viewer = new PerformanceMonitor.Darling.Viewer.ViewerDataService(cs!);
            var waiters = await viewer.GetQuerySnapshotsByWaitTypeAsync(ServerId, "LCK_M_X", start, end, cancellationToken: ct);
            Assert.Equal(3, waiters.Count);
            Assert.DoesNotContain(waiters, w => (w.ProgramName ?? "").StartsWith("PerformanceMonitor", StringComparison.Ordinal));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* #7573: the Analysis reads (the ACTIVE_QUERIES fact and the CPU spike drill-down's queries) count only the application's rows. Four rows
       a minute apart, two the monitor's. The old reads had no program predicate and counted all four. */
    [Fact]
    public async Task TheAnalysisFactAndTheSpikeDrillDown_CountOnlyTheApplicationsRows()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = await SeedAsync(cs!, new string?[] { "PerformanceMonitorDarling", "Orders.Api", "PerformanceMonitorLite", null }, ct);
            using (var connection = new NpgsqlConnection(cs))
            {
                await connection.OpenAsync(ct);
                var peak = DarlingMcpTestData.Naive(end.AddMinutes(-3).AddSeconds(-30));
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO collect.cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, 95, 0)",
                    CollectionIdGenerator.Next(), peak, ServerId, ServerName, peak);
            }

            var context = new PerformanceMonitor.Analysis.AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = end.AddHours(-1),
                TimeRangeEnd = end,
                ServerUtcOffset = TimeSpan.Zero,
            };

            var fact = Assert.Single(await new PerformanceMonitor.Darling.Analysis.PgFactCollector(postgres).CollectFactsAsync(context), f => f.Key == "ACTIVE_QUERIES");
            Assert.Equal(2, fact.Metadata["total_snapshots"]);
            Assert.Equal(2, fact.Metadata["distinct_sessions"]);

            var finding = new PerformanceMonitor.Analysis.AnalysisFinding
            {
                ServerId = ServerId,
                RootFactKey = "CPU_SPIKE",
                StoryPath = "CPU_SPIKE",
                PathKeys = ["CPU_SPIKE"],
                Severity = 0.6,
            };
            await new PerformanceMonitor.Darling.Analysis.PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);
            Assert.NotNull(finding.DrillDown);
            Assert.True(finding.DrillDown!.TryGetValue("queries_at_spike", out var raw), "no queries_at_spike drill-down");
            var sessions = JsonSerializer.SerializeToElement(raw).EnumerateArray().Select(e => e.GetProperty("session_id").GetInt32()).OrderBy(i => i).ToArray();
            Assert.Equal(new[] { 61, 63 }, sessions);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, $"DELETE FROM collect.cpu_utilization_stats WHERE server_id = {ServerId}");
                await DeleteRowsAsync(cleanup, cleanupCt);
            });
        }
    }

    /* A quiet server whose only sessions are the monitor's answers empty, and the empty text says they were left out. */
    [Fact]
    public async Task OnlyMonitorRows_AnswerEmpty_AndTheTextSaysTheyAreLeftOut()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(cs!, new string?[] { "PerformanceMonitorDarling" }, ct);

            using var def = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 24, cancellationToken: ct));
            Assert.Equal("empty", def.RootElement.GetProperty("status").GetString());
            Assert.Contains("monitor's own statements are left out", def.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);

            Assert.Single(AppsOf(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 24, include_monitor: true, cancellationToken: ct)));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }
}
