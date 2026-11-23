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

    private sealed record Pair(string? BlockedApp, string? BlockingApp);

    private static async Task<List<Fact>> RunAsync(bool bpr, params Pair[] pairs)
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
                    $"INSERT INTO {table} ({idColumn}, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, blocked_client_app, blocking_client_app) VALUES ($1,$2,$3,$4,$2,12000,60,$5,'sleeping',$6,$7)",
                    ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, 70 + i,
                    (object?)pairs[i].BlockedApp ?? DBNull.Value, (object?)pairs[i].BlockingApp ?? DBNull.Value);
            }

            var context = new AnalysisContext
            {
                ServerId = ServerId, ServerName = ServerName, TimeRangeStart = start, TimeRangeEnd = end,
                ServerUtcOffset = TimeSpan.Zero, CancellationToken = ct
            };
            var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
            bodySucceeded = true;
            return facts;
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
