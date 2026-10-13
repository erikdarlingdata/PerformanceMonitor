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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5633 item 19 on the real store (gated, DARLING_TEST_PG): a trace_flags run that finds no flag enabled stores no rows, so turning
/// off the LAST enabled flag never reached the change diff. The three Darling readers of the diff (the MCP tool, the viewer and the
/// analysis pass) now also read the SUCCESSFUL runs that stored zero rows from <c>collection_log</c> and hand them to
/// <see cref="ConfigChangeDiff.DiffTraceFlagChanges(IReadOnlyList{ConfigChangeDiff.TraceFlagSnapshot}, IEnumerable{DateTime}, DateTime, DateTime)"/>
/// as empty snapshots. This runs each read's own SQL against Postgres: the bounded window arm, the one-run-before-the-window baseline
/// arm, and the exclusions (a failed run, a run whose count was not recorded, a run that stored rows).
/// </summary>
[Collection("live-postgres")]
public sealed class TraceFlagEmptyRunLivePostgresTests
{
    private const string ServerName = "darling-tf-empty-run-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ASuccessfulEmptyRunAfterAFlag_IsReportedAsTheFlagTurnedOff_ByEveryDarlingRead()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live trace-flag empty-run test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
            var windowStart = now.AddHours(-168);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var on = now.AddHours(-30);
            var emptyOk = now.AddHours(-3);
            await PlantFlagAsync(connection, on, 3226, ct);
            await PlantRunAsync(connection, on, "SUCCESS", 1, ct);
            /* The runs that are NOT empty snapshots: failed ones, an unrecorded count, a non-trace_flags collector's empty run. */
            await PlantRunAsync(connection, now.AddHours(-20), "ERROR", 0, ct);
            await PlantRunAsync(connection, now.AddHours(-19), "PERMISSIONS", 0, ct);
            await PlantRunAsync(connection, now.AddHours(-18), "SUCCESS", null, ct);
            await PlantRunAsync(connection, now.AddHours(-17), "SUCCESS", 0, ct, collector: "server_config");
            await PlantRunAsync(connection, emptyOk, "SUCCESS", 0, ct);
            /* Before the window: only the NEWEST of these is the baseline arm's answer. */
            await PlantRunAsync(connection, now.AddDays(-30), "SUCCESS", 0, ct);
            await PlantRunAsync(connection, now.AddDays(-40), "SUCCESS", 0, ct);

            var runs = await DarlingCollectorRunFactsReader.ReadEmptyTraceFlagRunsAsync(postgres, ServerId, windowStart, now, ct);
            Assert.Equal(2, runs.Count);
            Assert.Contains(runs, t => Within(t, emptyOk));
            Assert.Contains(runs, t => Within(t, now.AddDays(-30)));

            /* The viewer's and the analysis pass's SQL are twins of the MCP reader's: same rows. */
            Assert.Equal(
                runs.OrderBy(t => t),
                (await ReadTimesAsync(postgres, DarlingRetentionHorizons.EmptyTraceFlagRunsSql, windowStart, now, ct)).OrderBy(t => t));
            Assert.Equal(
                runs.OrderBy(t => t),
                (await ReadTimesAsync(postgres, DarlingAnalysisService.EmptyTraceFlagRunsForAttributionSql, windowStart, now, ct)).OrderBy(t => t));

            /* The tool: the empty run 30 days ago is the baseline, so the flag is 'enabled' at its capture and then 'disabled' at the later
               successful empty run (newest first). The failed and unrecorded runs in between change nothing. */
            var json = await DarlingMcpConfigHistoryTools.GetTraceFlagChanges(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(json, ServerName, "changes");
            var changes = JsonDocument.Parse(json).RootElement.GetProperty("changes").EnumerateArray().ToList();
            Assert.Equal(2, changes.Count);
            Assert.Equal("disabled", changes[0].GetProperty("change_type").GetString());
            Assert.Equal(3226, changes[0].GetProperty("trace_flag").GetInt32());
            Assert.True(Within(ParseUtc(changes[0].GetProperty("change_time").GetString()!), emptyOk));
            Assert.Equal("enabled", changes[1].GetProperty("change_type").GetString());
            Assert.True(Within(ParseUtc(changes[1].GetProperty("change_time").GetString()!), on));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task AFlagOnThenOnlyAFailedRun_IsNoChange()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live trace-flag empty-run test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await PlantFlagAsync(connection, now.AddHours(-30), 3226, ct);
            await PlantRunAsync(connection, now.AddHours(-30), "SUCCESS", 1, ct);
            await PlantRunAsync(connection, now.AddHours(-3), "ERROR", 0, ct);
            await PlantRunAsync(connection, now.AddHours(-2), "PERMISSIONS", 0, ct);

            Assert.Empty(await DarlingCollectorRunFactsReader.ReadEmptyTraceFlagRunsAsync(postgres, ServerId, now.AddHours(-168), now, ct));
            var json = await DarlingMcpConfigHistoryTools.GetTraceFlagChanges(postgres, ServerName);
            Assert.Empty(JsonDocument.Parse(json).RootElement.TryGetProperty("changes", out var changes) ? changes.EnumerateArray() : Enumerable.Empty<JsonElement>());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#5633 item 19 review (Medium-1, Low-3): the baseline arm stops at the window start minus the trace_flags retention (30 days
    /// by default, the fleet override when one is set). An empty run older than that is older than the oldest trace_flags rows the store
    /// holds, so a later capture must not pair with it as a flag turned on.</summary>
    [Fact]
    public async Task AnEmptyRunOlderThanTheRetention_IsNotABaseline_ByEveryDarlingRead_AndAFleetOverrideMovesTheBound()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live trace-flag empty-run test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var insertedFleetRow = false;
        var bodySucceeded = false;
        try
        {
            var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
            var windowStart = now.AddHours(-168);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* Day -50: an empty run. Day -3h: the flag appears (it has really been on since day -40, the rows that said so purged). */
            var tooOld = now.AddDays(-50);
            var on = now.AddHours(-3);
            await PlantRunAsync(connection, tooOld, "SUCCESS", 0, ct);
            await PlantFlagAsync(connection, on, 3226, ct);
            await PlantRunAsync(connection, on, "SUCCESS", 1, ct);

            /* Default retention 30 days: the floor is windowStart - 30 days = day -37, so the day -50 run is no baseline. */
            Assert.Empty(await DarlingCollectorRunFactsReader.ReadEmptyTraceFlagRunsAsync(postgres, ServerId, windowStart, now, ct));
            Assert.Empty(await ReadTimesAsync(postgres, DarlingRetentionHorizons.EmptyTraceFlagRunsSql, windowStart, now, ct));
            Assert.Empty(await ReadTimesAsync(postgres, DarlingAnalysisService.EmptyTraceFlagRunsForAttributionSql, windowStart, now, ct));
            var json = await DarlingMcpConfigHistoryTools.GetTraceFlagChanges(postgres, ServerName);
            Assert.DoesNotContain("enabled", json.Replace("\"change_type\":\"modified\"", string.Empty));

            /* A configured fleet retention is the bound's input: 90 days puts the floor at day -97, so the day -50 run is the baseline again. */
            var hasFleetRow = await ScalarCountAsync(connection, "SELECT count(*) FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = 'trace_flags'", ct) > 0;
            if (!hasFleetRow)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO config.config_collector_schedules (server_id, collector_name, retention_days, enabled) VALUES (NULL, 'trace_flags', 90, TRUE)");
                insertedFleetRow = true;

                var runs = await DarlingCollectorRunFactsReader.ReadEmptyTraceFlagRunsAsync(postgres, ServerId, windowStart, now, ct);
                Assert.Single(runs);
                Assert.True(Within(runs[0], tooOld));
                Assert.Single(await ReadTimesAsync(postgres, DarlingRetentionHorizons.EmptyTraceFlagRunsSql, windowStart, now, ct));
                Assert.Single(await ReadTimesAsync(postgres, DarlingAnalysisService.EmptyTraceFlagRunsForAttributionSql, windowStart, now, ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                if (insertedFleetRow)
                {
                    await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt,
                        "DELETE FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = 'trace_flags'");
                }

                await DeleteRowsAsync(cleanup, cleanupCt);
            });
        }
    }

    private static async Task<long> ScalarCountAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    private static DateTime ParseUtc(string value) =>
        DateTime.Parse(value, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);

    /// <summary>Postgres keeps microseconds and .NET ticks are finer: compare to the second.</summary>
    private static bool Within(DateTime actual, DateTime expected) => Math.Abs((actual - expected).TotalSeconds) < 1;

    private static async Task<List<DateTime>> ReadTimesAsync(NpgsqlDataSource postgres, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        var times = new List<DateTime>();
        await using var command = postgres.CreateCommand(sql);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(start);
        command.Parameters.AddWithValue(end);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            times.Add(reader.GetDateTime(0));
        return times;
    }

    private static async Task PlantFlagAsync(NpgsqlConnection connection, DateTime captureTime, int traceFlag, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO trace_flags
    (config_id, capture_time, server_id, server_name, trace_flag, status, is_global, is_session)
VALUES ($1, $2, $3, $4, $5, TRUE, TRUE, FALSE)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(captureTime), ServerId, ServerName, traceFlag);

    private static async Task PlantRunAsync(
        NpgsqlConnection connection, DateTime at, string status, long? rows, CancellationToken ct, string collector = "trace_flags") =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected)
VALUES ($1, $2, $3, $4, $5, 50, $6, NULL, $7)",
            CollectionIdGenerator.Next(), ServerId, ServerName, collector, DarlingMcpTestData.Naive(at), status, rows);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM trace_flags WHERE server_id = {ServerId}; " +
            $"DELETE FROM collection_log WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
