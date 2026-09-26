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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4231 stage 3b live pins: <c>DarlingDataReader.GetTopProceduresByCpuRoutedAsync</c> actually routes to
/// the hourly rollup once raw's floor ages past the window, THROUGH the product's own routed read (never by
/// running the rollup SQL directly). Raw and hourly must agree on the (database_name, schema_name,
/// object_name) totals for the same window, once rolled up.
///
/// <para><b>#1776 own-store</b> — mints a scratch database (it materializes a continuous aggregate the
/// shared fixture must never inherit), so it is deliberately NOT in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class TopProceduresHourlyRoutingLiveTests
{
    private const int ServerId = -943822;
    private const string ServerName = "a4231-3b-topn-hourly-routing";
    private const string Db = "TopNHourlyProcDb";

    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task GetTopProceduresByCpuRoutedAsync_RoutesToHourlyOnceRawIsPurged_AndTotalsAgree()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4231 stage-3b routing test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #4231 stage-3b routing test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var windowEnd = WindowStart.AddDays(1);
        var bodySucceeded = false;
        try
        {
            /* seed: three (schema, object) groups, every row a real nonzero sample_interval_seconds so the
               hourly rollup admits them all — the zero-interval exclusion is #4231's own finding, already
               pinned in TopQueriesHourlyRoutingLiveTests; not re-litigated here. */
            await PlantAsync(connection, ct, WindowStart.AddHours(1), "usp_ProcA", 500_000L, 400_000L, 10L, 3600);
            await PlantAsync(connection, ct, WindowStart.AddHours(2), "usp_ProcB", 200_000L, 180_000L, 5L, 3600);
            await PlantAsync(connection, ct, WindowStart.AddHours(3), "usp_ProcC", 50_000L, 40_000L, 2L, 3600);

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

            var rawResult = await DarlingDataReader.GetTopProceduresByCpuRoutedAsync(
                dataSource, ServerId, WindowStart, windowEnd, top: 10, databaseName: null, cancellationToken: ct);
            Assert.Equal(RetentionTier.Raw, rawResult.Tier);

            var rawTotalsByKey = RollUpByObject(rawResult.Rows);
            Assert.True(rawTotalsByKey.ContainsKey("usp_ProcA"), "seed group usp_ProcA must be present in the raw read");

            /* Refresh the hourly successor over the window BEFORE deleting raw — the product's own
               backfill/refresh path, not a hand-built rollup row. */
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, WindowStart, windowEnd.AddHours(1), ct);

            var floors = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, await TimescaleSupport.DetectRollupsAsync(dataSource, ct), ct);
            Assert.Equal(WindowStart.AddHours(1), floors.FloorOf(TimescaleSupport.ProcedureStatsIntervalHourlyView));

            /* move raw's floor past the window: delete W's raw rows, as retention would. */
            await using (var purge = new NpgsqlCommand(
                "DELETE FROM collect.procedure_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection))
            {
                purge.Parameters.AddWithValue(ServerId);
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(windowEnd);
                await purge.ExecuteNonQueryAsync(ct);
            }

            /* FRESH data source — #4231 3a's own cache-freshness finding applies here too:
               ComposeStoreAvailability caches coverage per NpgsqlDataSource for 5 minutes; reusing the first
               data source would replay the stale null hourly floor its probe measured before RefreshAsync
               ran, and the router's fallback would land back on Raw. */
            await using var hourlyDataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var hourlyResult = await DarlingDataReader.GetTopProceduresByCpuRoutedAsync(
                hourlyDataSource, ServerId, WindowStart, windowEnd, top: 10, databaseName: null, cancellationToken: ct);
            Assert.Equal(RetentionTier.Hourly, hourlyResult.Tier);
            Assert.NotEmpty(hourlyResult.Rows);

            var hourlyTotalsByKey = RollUpByObject(hourlyResult.Rows);

            foreach (var name in new[] { "usp_ProcA", "usp_ProcB", "usp_ProcC" })
            {
                Assert.True(rawTotalsByKey.TryGetValue(name, out var rawTotal), $"{name} missing from raw's totals");
                Assert.True(hourlyTotalsByKey.TryGetValue(name, out var hourlyTotal), $"{name} missing from the hourly-routed totals");
                Assert.Equal(rawTotal.CpuUs, hourlyTotal.CpuUs);
                Assert.Equal(rawTotal.Executions, hourlyTotal.Executions);
            }

            /* the disclosed precision loss: object_type is unavailable at rollup grain. */
            Assert.All(hourlyResult.Rows, r => Assert.Equal("", r.ObjectType));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4231 stage 3b live pin: an HOURLY-routed <c>get_top_procedures_by_cpu</c> MCP payload carries a
    /// <c>tier_used = "hourly"</c> and a <c>precision_note</c> saying <c>object_type</c> is unavailable at
    /// that tier (the rollup has no object_type column). A RAW-routed payload for the same request has
    /// <c>tier_used = "raw"</c> and no such note. RED on dev: no <c>tier_used</c>/<c>precision_note</c> field
    /// exists on dev's payload at all.
    /// </summary>
    [Fact]
    public async Task GetTopProceduresByCpu_HourlyRouted_CarriesTierUsedAndPrecisionNote()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4231 stage-3b routing test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #4231 stage-3b routing test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var windowEnd = WindowStart.AddDays(1);
        var bodySucceeded = false;
        try
        {
            await PlantAsync(connection, ct, WindowStart.AddHours(1), "usp_ProcA", 500_000L, 400_000L, 10L, 3600);
            await PlantAsync(connection, ct, WindowStart.AddHours(2), "usp_ProcB", 200_000L, 180_000L, 5L, 3600);

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

            var hoursBack = (int)Math.Ceiling((windowEnd - WindowStart).TotalHours);
            var asOf = windowEnd.ToString("o");

            var rawJson = await DarlingMcpDataTools.GetTopProceduresByCpu(
                dataSource, ServerName, hours_back: hoursBack, top: 10, as_of: asOf);
            using var rawDoc = System.Text.Json.JsonDocument.Parse(rawJson);
            Assert.True(rawDoc.RootElement.TryGetProperty("tier_used", out var rawTier));
            Assert.Equal("raw", rawTier.GetString());
            Assert.True(rawDoc.RootElement.TryGetProperty("precision_note", out var rawNote));
            Assert.Equal(System.Text.Json.JsonValueKind.Null, rawNote.ValueKind);

            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, WindowStart, windowEnd.AddHours(1), ct);

            await using (var purge = new NpgsqlCommand(
                "DELETE FROM collect.procedure_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection))
            {
                purge.Parameters.AddWithValue(ServerId);
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(windowEnd);
                await purge.ExecuteNonQueryAsync(ct);
            }

            await using var hourlyDataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var hourlyJson = await DarlingMcpDataTools.GetTopProceduresByCpu(
                hourlyDataSource, ServerName, hours_back: hoursBack, top: 10, as_of: asOf);
            using var hourlyDoc = System.Text.Json.JsonDocument.Parse(hourlyJson);
            Assert.True(hourlyDoc.RootElement.TryGetProperty("tier_used", out var hourlyTier));
            Assert.Equal("hourly", hourlyTier.GetString());
            Assert.True(hourlyDoc.RootElement.TryGetProperty("precision_note", out var hourlyNote));
            Assert.Equal(System.Text.Json.JsonValueKind.String, hourlyNote.ValueKind);
            Assert.False(string.IsNullOrEmpty(hourlyNote.GetString()));

            /* the disclosed precision loss on the payload: object_type is null at the hourly tier. */
            var procedures = hourlyDoc.RootElement.GetProperty("procedures").EnumerateArray().ToList();
            Assert.NotEmpty(procedures);
            Assert.All(procedures, p => Assert.Equal(System.Text.Json.JsonValueKind.Null, p.GetProperty("object_type").ValueKind));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    private static Dictionary<string, (long CpuUs, long Executions)> RollUpByObject(IEnumerable<DarlingDataReader.TopProcedureRow> rows)
    {
        var totals = new Dictionary<string, (long CpuUs, long Executions)>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            totals.TryGetValue(row.ObjectName, out var running);
            totals[row.ObjectName] = (running.CpuUs + row.TotalCpuUs, running.Executions + row.TotalExecutions);
        }
        return totals;
    }

    private static async Task PlantAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, string objectName,
        long cpuUs, long elapsedUs, long executions, int intervalSeconds)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, $8, $9, $10, $11)", connection);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.AddWithValue(DarlingMcpTestData.TruncateToSeconds(at));
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(ServerName);
        insert.Parameters.AddWithValue(Db);
        insert.Parameters.AddWithValue(objectName);
        insert.Parameters.AddWithValue("0x" + objectName);
        insert.Parameters.AddWithValue(cpuUs);
        insert.Parameters.AddWithValue(elapsedUs);
        insert.Parameters.AddWithValue(executions);
        insert.Parameters.AddWithValue(intervalSeconds);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }
}
