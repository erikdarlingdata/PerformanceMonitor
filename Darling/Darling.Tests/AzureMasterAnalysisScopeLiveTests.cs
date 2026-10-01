/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// An Azure SQL Database <c>master</c> target's blocking and deadlock findings skip the databases that are
/// monitored as their own targets (<see cref="AnalysisContext.SeparatelyMonitoredDatabases"/>). Gated on
/// DARLING_TEST_PG; the facts and both anomaly spikes are read through the product's own collector and
/// detector, on both the blocked-process arm and the DMV fallback arm.
/// </summary>
/* #1776 own-store: every fact plants its rows under one dedicated server id and deletes them in cleanup. */
[Collection("live-postgres")]
public sealed class AzureMasterAnalysisScopeLiveTests
{
    private const string ServerName = "darling-azure-master-scope";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly string[] Separate = { "GP" };

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Graph(params string[] dbs) =>
        "<deadlock><process-list>" +
        string.Concat(dbs.Select((d, i) => $"<process id=\"p{i}\" currentdbname=\"{d}\" />")) +
        "</process-list></deadlock>";

    private sealed record Plan(bool Bpr, string?[] BlockingDbs, string[][] Deadlocks);

    private static async Task<(List<Fact> Facts, List<Fact> Anomalies)> RunAsync(Plan plan, IReadOnlyList<string>? separate, bool anomalies)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await Exec(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, now()::timestamp, now()::timestamp)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct, ServerId, ServerName);

            var end = new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerMinute), DateTimeKind.Unspecified).AddMinutes(-1);
            var start = end.AddHours(-4);
            await Exec(connection, "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms) VALUES ($1,$2,$3,$4,'OLD',1,1,0)",
                ct, CollectionIdGenerator.Next(), end.AddDays(-3), ServerId, ServerName);
            for (var m = 0; m <= 240; m += 15)
                await Exec(connection, "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms) VALUES ($1,$2,$3,$4,'CXPACKET',1,10,0)",
                    ct, CollectionIdGenerator.Next(), start.AddMinutes(m), ServerId, ServerName);

            for (var i = 0; i < plan.BlockingDbs.Length; i++)
            {
                var at = start.AddMinutes(10 + i);
                if (plan.Bpr)
                    await Exec(connection, "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1,$2,$3,$4,$2,12000,60,$5,'suspended',$6)",
                        ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, 70 + i, (object?)plan.BlockingDbs[i] ?? DBNull.Value);
                else
                    await Exec(connection, "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, blocking_status) VALUES ($1,$2,$3,$4,$2,$5,$6,60,12000,'suspended')",
                        ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, (object?)plan.BlockingDbs[i] ?? DBNull.Value, 70 + i);
            }
            for (var i = 0; i < plan.Deadlocks.Length; i++)
                await Exec(connection, "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml) VALUES ($1,$2,$3,$4,$2,$5)",
                    ct, CollectionIdGenerator.Next(), start.AddMinutes(20 + i), ServerId, ServerName, Graph(plan.Deadlocks[i]));

            var context = new AnalysisContext
            {
                ServerId = ServerId, ServerName = ServerName, TimeRangeStart = start, TimeRangeEnd = end,
                ServerUtcOffset = TimeSpan.Zero, SeparatelyMonitoredDatabases = separate, CancellationToken = ct
            };
            var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
            var spikes = new List<Fact>();
            if (anomalies)
                spikes = await new PgAnomalyDetector(postgres, new PgBaselineProvider(postgres)).DetectAnomaliesAsync(context);
            bodySucceeded = true;
            return (facts, spikes);
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* BLOCKING_EVENTS reads blocked_process_reports only; the DMV fallback arm exists in the anomaly read. */
    [Fact]
    public async Task BlockingEvents_SkipSeparatelyMonitoredDatabases()
    {
        var plan = new Plan(true, new string?[] { "GP", "gp", "HS", null, "GP" }, Array.Empty<string[]>());
        var (facts, _) = await RunAsync(plan, Separate, false);
        Assert.Equal(2.0, Assert.Single(facts, f => f.Key == "BLOCKING_EVENTS").Metadata["event_count"]);
    }

    [Fact]
    public async Task BlockingEvents_NullOrEmptyList_CountsEverything()
    {
        var plan = new Plan(true, new string?[] { "GP", "HS", null }, Array.Empty<string[]>());
        foreach (var list in new IReadOnlyList<string>?[] { null, Array.Empty<string>() })
        {
            var (facts, _) = await RunAsync(plan, list, false);
            Assert.Equal(3.0, Assert.Single(facts, f => f.Key == "BLOCKING_EVENTS").Metadata["event_count"]);
        }
    }

    [Fact]
    public async Task Deadlocks_AllSeparateIsSkipped_MixedStillCounts()
    {
        var plan = new Plan(true, Array.Empty<string?>(), new[] { new[] { "GP", "GP" }, new[] { "GP", "HS" }, new[] { "gp" } });
        var (facts, _) = await RunAsync(plan, Separate, false);
        Assert.Equal(1.0, Assert.Single(facts, f => f.Key == "DEADLOCKS").Metadata["deadlock_count"]);
        var (all, _) = await RunAsync(plan, null, false);
        Assert.Equal(3.0, Assert.Single(all, f => f.Key == "DEADLOCKS").Metadata["deadlock_count"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task BlockingSpike_MadeOnlyOfSeparateDatabases_GivesNoSpike(bool bpr)
    {
        var plan = new Plan(bpr, new string?[] { "GP", "GP", "GP", "GP", "GP", "GP" }, Array.Empty<string[]>());
        var (_, filtered) = await RunAsync(plan, Separate, true);
        Assert.DoesNotContain(filtered, f => f.Key == "ANOMALY_BLOCKING_SPIKE");
        var (_, control) = await RunAsync(plan, null, true);
        Assert.Contains(control, f => f.Key == "ANOMALY_BLOCKING_SPIKE");
    }

    [Fact]
    public async Task DeadlockSpike_MadeOnlyOfSeparateDatabases_GivesNoSpike()
    {
        var plan = new Plan(true, Array.Empty<string?>(), new[] { new[] { "GP" }, new[] { "GP", "GP" }, new[] { "GP" } });
        var (_, filtered) = await RunAsync(plan, Separate, true);
        Assert.DoesNotContain(filtered, f => f.Key == "ANOMALY_DEADLOCK_SPIKE");
        var (_, control) = await RunAsync(plan, null, true);
        Assert.Contains(control, f => f.Key == "ANOMALY_DEADLOCK_SPIKE");
    }

    /// <summary>The worker's fill: the list for an Azure master runtime, null or empty otherwise.</summary>
    [Fact]
    public void WorkerFill_GivesTheListForAnAzureMaster_AndNothingElse()
    {
        var method = typeof(DarlingWorker).GetMethod("AnalysisSeparatelyMonitoredDatabases",
            BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public);
        Assert.NotNull(method);
        var live = new List<MonitoredServer>
        {
            new() { Name = "m", Host = "srv.database.windows.net", Database = "master", StoredServerId = 1 },
            new() { Name = "g", Host = "srv.database.windows.net", Database = "GP", StoredServerId = 2 },
        };
        IReadOnlyList<string>? Call(bool azure, int id) => (IReadOnlyList<string>?)method!.Invoke(null,
            new object?[] { azure, id.ToString(CultureInfo.InvariantCulture), "srv.database.windows.net", "master", live });
        Assert.Equal(new[] { "GP" }, Call(true, 1));
        Assert.True(Call(false, 1) is null || Call(false, 1)!.Count == 0);
        Assert.True(Call(true, 2) is null || Call(true, 2)!.Count == 0);
    }

    private static async Task Exec(NpgsqlConnection c, string sql, CancellationToken ct, params object[] p)
    {
        using var cmd = new NpgsqlCommand(sql, c);
        foreach (var v in p) cmd.Parameters.AddWithValue(v);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; " +
            $"DELETE FROM dmv_blocking_snapshots WHERE server_id = {ServerId}; " +
            $"DELETE FROM deadlocks WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
