/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9340: the BLOCKING_EVENTS and BLOCKING_CHAIN facts leave out a pair whose two sessions are both the monitor's own
/// (the monitor's collectors queued behind each other on a compile or memory problem). A pair with a user's session on either
/// side, or with no program name recorded, is still real blocking evidence. Gated on DARLING_TEST_PG; the facts are read through
/// the product's own collector, on the blocked-process-report arm and the DMV snapshot arm of the chain.
/// </summary>
/* #1776 own-store: every fact plants its rows under one dedicated server id and deletes them in cleanup. */
[Collection("live-postgres")]
public sealed class MonitorOwnBlockingFactLiveTests
{
    private const string ServerName = "darling-monitor-own-blocking";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed record Pair(string? BlockedApp, string? BlockingApp, int BlockingSpid = 60);

    private static Task<List<Fact>> RunAsync(bool bpr, params Pair[] pairs) =>
        RunAsync(bpr, pairs, 0, (postgres, context) => new PgFactCollector(postgres).CollectFactsAsync(context));

    /// <summary>Plants the pairs (and <paramref name="flood"/> monitor-only pairs, each NEWER than every listed pair), then runs <paramref name="read"/>
    /// against the product's collector over the window.</summary>
    private static async Task<T> RunAsync<T>(bool bpr, Pair[] pairs, int flood, Func<NpgsqlDataSource, AnalysisContext, Task<T>> read,
        string? floodDatabase = null, IReadOnlyList<string>? separatelyMonitored = null)
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

            var end = new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerMinute * TimeSpan.TicksPerMinute, DateTimeKind.Unspecified).AddMinutes(-1);
            var start = end.AddHours(-4);
            await Exec(connection, "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms) VALUES ($1,$2,$3,$4,'OLD',1,1,0)",
                ct, CollectionIdGenerator.Next(), end.AddDays(-3), ServerId, ServerName);
            for (var m = 0; m <= 240; m += 15)
                await Exec(connection, "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms) VALUES ($1,$2,$3,$4,'CXPACKET',1,10,0)",
                    ct, CollectionIdGenerator.Next(), start.AddMinutes(m), ServerId, ServerName);

            /* One wait of its own per pair (blocked spid 70 + i), all blocked by spid 60. */
            for (var i = 0; i < pairs.Length; i++)
            {
                var at = start.AddMinutes(10 + i);
                var table = bpr ? "blocked_process_reports" : "dmv_blocking_snapshots";
                var idColumn = bpr ? "blocked_report_id" : "collection_id";
                await Exec(connection,
                    $"INSERT INTO {table} ({idColumn}, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, blocked_client_app, blocking_client_app) VALUES ($1,$2,$3,$4,$2,12000,$8,$5,'sleeping',$6,$7)",
                    ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, 70 + i,
                    (object?)pairs[i].BlockedApp ?? DBNull.Value, (object?)pairs[i].BlockingApp ?? DBNull.Value, pairs[i].BlockingSpid);
            }

            /* More monitor-only pairs than a read's 5,000-row cap, all newer than the pairs above (a read keeps the newest rows first, so a filter
               applied after the cap would keep only these): each a distinct wait, blocked spid 1000 + g, blocked by spid 61. */
            if (flood > 0)
            {
                var table = bpr ? "blocked_process_reports" : "dmv_blocking_snapshots";
                var idColumn = bpr ? "blocked_report_id" : "collection_id";
                await Exec(connection,
                    $"INSERT INTO {table} ({idColumn}, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, blocked_client_app, blocking_client_app, database_name) " +
                    "SELECT -939000000 - g, $1 + g * interval '1 second', $2, $3, $1 + g * interval '1 second', 12000, 61, 1000 + g, 'sleeping', " +
                    "CASE WHEN $5::text IS NULL THEN 'PerformanceMonitorDarling' ELSE 'OrderEntry' END, CASE WHEN $5::text IS NULL THEN 'PerformanceMonitorDarling' ELSE 'OrderEntry' END, $5::text FROM generate_series(0, $4 - 1) AS g",
                    ct, start.AddMinutes(60), ServerId, ServerName, flood, (object?)floodDatabase ?? DBNull.Value);
            }

            var context = new AnalysisContext
            {
                ServerId = ServerId, ServerName = ServerName, TimeRangeStart = start, TimeRangeEnd = end,
                ServerUtcOffset = TimeSpan.Zero, CancellationToken = ct, SeparatelyMonitoredDatabases = separatelyMonitored
            };
            var result = await read(postgres, context);
            bodySucceeded = true;
            return result;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static readonly Pair MonitorOnly = new("PerformanceMonitorDarling", "PerformanceMonitorDarling");

    [Fact]
    public async Task OnlyTheMonitorsOwnBlocking_ProducesNoBlockingFacts()
    {
        var facts = await RunAsync(true, MonitorOnly,
            new Pair("PerformanceMonitorDarling-Remediation", "PerformanceMonitorDarling"));

        Assert.DoesNotContain(facts, f => f.Key == "BLOCKING_EVENTS");
        Assert.DoesNotContain(facts, f => f.Key == "BLOCKING_CHAIN");
    }

    [Fact]
    public async Task BlockingWithAUserSessionOnEitherSide_OrNoProgramName_StillCounts()
    {
        var facts = await RunAsync(true,
            MonitorOnly,                                                                   // left out
            new Pair("PerformanceMonitorDarling", "OrderEntry"),                          // monitor blocked by a user
            new Pair("OrderEntry", "PerformanceMonitorDarling"),                          // user blocked by the monitor
            new Pair("OrderEntry", "OrderEntry"),
            new Pair(null, null),                                                         // no name recorded: never hidden
            new Pair("PerformanceMonitorDarling", null),
            new Pair("MyPerformanceMonitorTool", "MyPerformanceMonitorTool"));            // contains the words, does not begin with them

        Assert.Equal(6.0, Assert.Single(facts, f => f.Key == "BLOCKING_EVENTS").Metadata["event_count"]);
        Assert.Contains(facts, f => f.Key == "BLOCKING_CHAIN");
    }

    [Fact]
    public async Task MonitorOnlyDmvSnapshots_ProduceNoChain_AndARealPairBesideThemIsTheOnlyOne()
    {
        var none = await RunAsync(false, MonitorOnly, MonitorOnly);
        Assert.DoesNotContain(none, f => f.Key == "BLOCKING_CHAIN");

        var facts = await RunAsync(false, MonitorOnly, MonitorOnly, new Pair("OrderEntry", "OrderEntry"));
        Assert.Equal(1.0, Assert.Single(facts, f => f.Key == "BLOCKING_CHAIN").Metadata["worst_chain_victim_count"]);
    }

    private const int PastTheCap = 5100;

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task MoreMonitorOnlyPairsThanTheRowCap_DoNotPushTheRealChainOutOfTheRead(bool bpr)
    {
        var facts = await RunAsync(bpr, [new Pair("OrderEntry", "OrderEntry")], PastTheCap,
            (postgres, context) => new PgFactCollector(postgres).CollectFactsAsync(context));

        Assert.Equal(1.0, Assert.Single(facts, f => f.Key == "BLOCKING_CHAIN").Metadata["worst_chain_victim_count"]);
    }

    private const string SeparateDatabase = "ShardDb";

    /* #9346: a master target leaves out the pairs of a database monitored as its own target. 5,100 newer pairs of that database must not push
       the master's own pair (no database recorded) out of either read, so the chain still forms and the drill-down shows it. */
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task MoreSeparatelyMonitoredPairsThanTheRowCap_DoNotPushTheMastersChainOutOfTheFact(bool bpr)
    {
        var facts = await RunAsync(bpr, [new Pair("OrderEntry", "OrderEntry")], PastTheCap,
            (postgres, context) => new PgFactCollector(postgres).CollectFactsAsync(context),
            SeparateDatabase, [SeparateDatabase.ToLowerInvariant()]);

        Assert.Equal(1.0, Assert.Single(facts, f => f.Key == "BLOCKING_CHAIN").Metadata["worst_chain_victim_count"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task MoreSeparatelyMonitoredPairsThanTheRowCap_DoNotPushTheMastersChainOutOfTheDrillDown(bool bpr)
    {
        var apexes = await RunAsync(bpr, [new Pair("OrderEntry", "OrderEntry")], PastTheCap,
            (postgres, context) => DrillDownIntsAsync(postgres, context, "BLOCKING_CHAIN", "reconstructed_blocking_chains", "apex_spid"),
            SeparateDatabase, [SeparateDatabase.ToLowerInvariant()]);

        Assert.Equal([60], apexes);
    }

    private static async Task<List<int>> DrillDownIntsAsync(NpgsqlDataSource postgres, AnalysisContext context, string pathKey, string section, string field)
    {
        var finding = new AnalysisFinding { RootFactKey = pathKey, StoryPath = pathKey, PathKeys = [pathKey], Severity = 1.0 };
        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);
        return JsonSerializer.SerializeToNode(finding.DrillDown![section])!.AsArray()
            .Select(row => row![field]!.GetValue<int>()).Order().ToList();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TheTopBlockingList_ShowsTheRealPair_NotTheMonitorsOwn(bool bpr)
    {
        var spids = await RunAsync(bpr, [new Pair("OrderEntry", "OrderEntry"), new Pair("PerformanceMonitorDarling", "PerformanceMonitorDarling", 61)], 0,
            (postgres, context) => DrillDownIntsAsync(postgres, context, "BLOCKING_EVENTS", "top_blocking_chains", "blocked_spid"));

        Assert.Equal([70], spids);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TheReconstructedChains_ShowTheRealChain_NotTheMonitorsOwn(bool bpr)
    {
        var apexes = await RunAsync(bpr, [new Pair("OrderEntry", "OrderEntry"), new Pair("PerformanceMonitorDarling", "PerformanceMonitorDarling", 61)], 0,
            (postgres, context) => DrillDownIntsAsync(postgres, context, "BLOCKING_CHAIN", "reconstructed_blocking_chains", "apex_spid"));

        Assert.Equal([60], apexes);
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
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM server_properties WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
