using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9340: the alert engine's blocking read leaves out a pair whose two sessions are both the monitor's own (the monitor's collectors queued
/// behind each other), on the blocked-process-report arm and the DMV snapshot arm. A pair with a user's session on either side, or with no
/// program name recorded, still alerts. Gated on DARLING_TEST_PG; the rows are read through the product's own alert adapter.
/// </summary>
/* #1776 own-store: every row is planted under one dedicated server id and deleted in cleanup. */
[Collection("live-postgres")]
public sealed class MonitorOwnBlockingAlertReadLiveTests
{
    private const string ServerName = "darling-monitor-own-blocking-alert";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed record Pair(string? BlockedApp, string? BlockingApp);

    private static readonly Pair MonitorOnly = new("PerformanceMonitorDarling", "PerformanceMonitorDarling");

    /* Blocked spid is 100 + i on the report arm and 200 + i on the snapshot arm, so the two arms never cover the same pair. */
    private static async Task<List<int>> RunAsync(Pair[] reportPairs, Pair[] snapshotPairs)
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

            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            await Plant(connection, "blocked_process_reports", "blocked_report_id", 100, reportPairs, now, ct);
            await Plant(connection, "dmv_blocking_snapshots", "collection_id", 200, snapshotPairs, now, ct);

            var rows = await new DarlingAlertReadAdapter(postgres).GetRecentBlockedProcessReportsAsync(ServerId.ToString(), 4, ct);
            bodySucceeded = true;
            return rows.Select(r => r.BlockedSpid).OrderBy(s => s).ToList();
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task Plant(NpgsqlConnection connection, string table, string idColumn, int spidBase, Pair[] pairs, DateTime now, CancellationToken ct)
    {
        for (var i = 0; i < pairs.Length; i++)
            await Exec(connection,
                $"INSERT INTO {table} ({idColumn}, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, blocked_client_app, blocking_client_app) VALUES ($1,$2,$3,$4,$2,12000,60,$5,'sleeping',$6,$7)",
                ct, CollectionIdGenerator.Next(), now.AddMinutes(-10 - i), ServerId, ServerName, spidBase + i,
                (object?)pairs[i].BlockedApp ?? DBNull.Value, (object?)pairs[i].BlockingApp ?? DBNull.Value);
    }

    [Fact]
    public async Task OnlyTheMonitorsOwnPairs_RaiseNoAlertRows()
    {
        var spids = await RunAsync(
            new[] { MonitorOnly, new Pair("PerformanceMonitorDarling-Remediation", "PerformanceMonitorDarling") },
            new[] { MonitorOnly, new Pair("PerformanceMonitorLite", "PerformanceMonitorLite") });

        Assert.Empty(spids);
    }

    [Fact]
    public async Task APairWithAUserSession_OrNoProgramName_StillRaisesAnAlertRow()
    {
        var pairs = new[]
        {
            MonitorOnly,                                                                   // left out
            new Pair("PerformanceMonitorDarling", "OrderEntry"),                          // monitor blocked by a user
            new Pair("OrderEntry", "PerformanceMonitorDarling"),                          // user blocked by the monitor
            new Pair(null, null),                                                         // no name recorded: never hidden
            new Pair("PerformanceMonitorDarling", null),
            new Pair("MyPerformanceMonitorTool", "MyPerformanceMonitorTool"),             // contains the words, does not begin with them
        };

        var spids = await RunAsync(pairs, pairs);

        Assert.Equal(new[] { 101, 102, 103, 104, 105, 201, 202, 203, 204, 205 }, spids);
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
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; " +
            $"DELETE FROM dmv_blocking_snapshots WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
