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
/// #7850: the blocking and deadlock trends say which stretches of their window the collector's successful runs read (<c>covered</c>),
/// so the web page draws a 0 only inside them. Seeded runs with a break between them give a list with the break in it; runs with no
/// break give one range; a failed run, or another collector's run, never bridges a break.
/// </summary>
[Collection("live-postgres")]
public sealed class TrendCoveredRangesLiveTests
{
    private const string ServerName = "darling-trend-covered-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string SkipReason =
        "Set DARLING_TEST_PG to a Postgres connection string to run the live trend coverage test (it mints its own scratch database).";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>Runs every 5 minutes from 170 to 100 minutes back, then none for 80 minutes, then 20, 15, 10, 5 and 2 minutes back. A failed
    /// run and the other blocking collector's run at 60 minutes back sit in the break and must not bridge it.</summary>
    [Fact]
    public async Task ARunBreakLongerThanTheCap_IsLeftOutOfTheCoveredRanges_AndFailedOrForeignRunsDoNotBridgeIt()
    {
        await RunAsync(async (postgres, now, ct) =>
        {
            foreach (var m in Enumerable.Range(0, 15).Select(i => 170 - (5 * i)))
            {
                await LogRunAsync(postgres, "blocked_process_report", "SUCCESS", now.AddMinutes(-m), ct);
            }

            foreach (var m in new[] { 20, 15, 10, 5, 2 })
            {
                await LogRunAsync(postgres, "blocked_process_report", "SUCCESS", now.AddMinutes(-m), ct);
            }

            await LogRunAsync(postgres, "blocked_process_report", "SESSION_MISSING", now.AddMinutes(-60), ct);
            await LogRunAsync(postgres, "blocked_process_report", "ERROR", now.AddMinutes(-61), ct);
            await LogRunAsync(postgres, "dmv_blocking_snapshot", "SUCCESS", now.AddMinutes(-60), ct);
            await LogRunAsync(postgres, "deadlocks", "SUCCESS", now.AddMinutes(-59), ct);

            var covered = await DarlingBlockingTrendReader.GetCoveredRangesAsync(postgres, ServerId, now.AddHours(-3), now, "blocked_process_report", ct);
            Assert.Equal(2, covered.Count);
            Assert.Equal(now.AddMinutes(-170), covered[0].Start);
            Assert.Equal(now.AddMinutes(-100), covered[0].End);
            Assert.Equal(now.AddMinutes(-20), covered[1].Start);
            Assert.Equal(now.AddMinutes(-2), covered[1].End);

            /* The deadlock collector ran once: no pair, nothing covered. */
            var deadlocks = await DarlingBlockingTrendReader.GetCoveredRangesAsync(postgres, ServerId, now.AddHours(-3), now, "deadlocks", ct);
            Assert.Empty(deadlocks);
        });
    }

    /// <summary>Runs every 10 minutes from 185 to 5 minutes back: no break, so one range. The first run is before the window's start, so the
    /// range is clipped to the start of the window and the stretch before the first in-window run is still covered.</summary>
    [Fact]
    public async Task RunsWithNoBreak_GiveOneRange_ClippedToTheWindowStart()
    {
        await RunAsync(async (postgres, now, ct) =>
        {
            foreach (var m in Enumerable.Range(0, 19).Select(i => 185 - (10 * i)))
            {
                await LogRunAsync(postgres, "blocked_process_report", "SUCCESS", now.AddMinutes(-m), ct);
            }

            var start = now.AddHours(-3);
            var covered = await DarlingBlockingTrendReader.GetCoveredRangesAsync(postgres, ServerId, start, now, "blocked_process_report", ct);
            var range = Assert.Single(covered);
            Assert.Equal(start, range.Start);
            Assert.Equal(now.AddMinutes(-5), range.End);

            /* A window the runs never reached: nothing covered. */
            var elsewhere = await DarlingBlockingTrendReader.GetCoveredRangesAsync(postgres, ServerId, now.AddDays(-9), now.AddDays(-8), "blocked_process_report", ct);
            Assert.Empty(elsewhere);
        });
    }

    /// <summary>Through both tools: a trend that has events carries the <c>covered</c> list beside the existing fields.</summary>
    [Fact]
    public async Task TheBlockingAndDeadlockTrends_CarryTheCoveredList_BesideTheirExistingFields()
    {
        await RunAsync(async (postgres, now, ct) =>
        {
            foreach (var collector in new[] { "blocked_process_report", "deadlocks" })
            {
                foreach (var m in new[] { 50, 40, 30, 20, 10, 2 })
                {
                    await LogRunAsync(postgres, collector, "SUCCESS", now.AddMinutes(-m), ct);
                }
            }

            var eventTime = DarlingMcpTestData.Naive(now.AddMinutes(-25));
            await using (var connection = await postgres.OpenConnectionAsync(ct))
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, database_name)
VALUES ($1,$2,$3,$4,$5,7000,60,444,'AppDb')",
                    CollectionIdGenerator.Next(), eventTime.AddSeconds(30), ServerId, ServerName, eventTime);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,'processX','select 1','<deadlock/>')",
                    CollectionIdGenerator.Next(), eventTime.AddSeconds(30), ServerId, ServerName, eventTime);
            }

            foreach (var reply in new[]
            {
                await DarlingMcpBlockingTools.GetBlockingTrend(postgres, ServerName, 1, cancellationToken: ct),
                await DarlingMcpBlockingTools.GetDeadlockTrend(postgres, ServerName, 1, cancellationToken: ct),
            })
            {
                using var doc = JsonDocument.Parse(reply);
                var root = doc.RootElement;
                Assert.Equal(1, root.GetProperty("trend").GetArrayLength());
                Assert.Equal(ServerName, root.GetProperty("server").GetString());
                var covered = root.GetProperty("covered");
                var range = Assert.Single(covered.EnumerateArray());
                Assert.Equal(now.AddMinutes(-50), TimeOf(range, "start"));
                Assert.Equal(now.AddMinutes(-2), TimeOf(range, "end"));
            }
        });
    }

    private static DateTime TimeOf(JsonElement range, string name) =>
        DateTime.Parse(range.GetProperty(name).GetString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);

    private static async Task LogRunAsync(NpgsqlDataSource postgres, string collector, string status, DateTime at, CancellationToken ct)
    {
        await using var connection = await postgres.OpenConnectionAsync(ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, 100, $6, NULL, 0, 80, 20)",
            CollectionIdGenerator.Next(), ServerId, ServerName, collector, DarlingMcpTestData.Naive(at), status);
    }

    private static async Task RunAsync(Func<NpgsqlDataSource, DateTime, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        // #1776 own-store: a scratch database, so no other class's chunks shape the store under test (#4650).
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var cs = scratch.ConnectionString;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PrepareStoreAsync(cs, connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            await body(postgres, now, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task PrepareStoreAsync(string connectionString, NpgsqlConnection connection, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);
        if (await LiveTimescaleProbe.TryEnableAsync(connectionString, ct))
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await using var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection);
            await stop.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; DELETE FROM deadlocks WHERE server_id = {ServerId}; DELETE FROM collection_log WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
