/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4394 live pin: a <c>query_stats</c> row with <c>delta_worker_time = NULL</c> and a real
/// <c>sample_interval_seconds</c> keeps its executions and duration through the hourly rollup, and the
/// Viewer's <c>worker_time_per_second</c> math tolerates the NULL rather than erroring.
///
/// <para><b>#1776 own-store</b> — mints a scratch database, so it is deliberately NOT in the
/// <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class QueryStatsUnknownCpuLiveTests
{
    private const int ServerId = -439412;
    private const string ServerName = "a4394-unknown-cpu";
    private const string Db = "UnknownCpuDb";

    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task AnUnknownCpuRow_KeepsItsExecutionsAndDuration_ThroughTheHourlyRollup_AndTheViewerMathTolerates_TheNull()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4394 rollup test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #4394 rollup test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

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
            /* One normal row: real CPU, real interval. One #4394 row: NULL CPU (unknowable), but a real
               interval and real executions/elapsed — the shape #4394's fix produces for the same query
               hash on a later collection pass whose worker counter reset while exec/elapsed did not. */
            await PlantAsync(connection, ct, WindowStart.AddHours(1), workerTime: 400_000L, elapsedTime: 500_000L, executions: 3L, intervalSeconds: 60);
            await PlantAsync(connection, ct, WindowStart.AddHours(2), workerTime: null, elapsedTime: 5_000_000L, executions: 3L, intervalSeconds: 60);

            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, WindowStart, windowEnd.AddHours(1), ct);

            await using var rollup = new NpgsqlCommand(
                "SELECT sum(execution_count_sum), sum(elapsed_time_sum), sum(worker_time_sum) " +
                "FROM collect.query_stats_interval_hourly WHERE server_id = $1", connection);
            rollup.Parameters.AddWithValue(ServerId);
            await using var reader = await rollup.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));

            /* The NULL-CPU row's executions (3) and elapsed (5,000,000) are INCLUDED — the hourly view's
               filter is on sample_interval_seconds, not on delta_worker_time, and sum()/NULL simply skips
               the NULL. Worker sum equals the normal row's 400,000 ALONE: the unknown row contributes
               nothing to it because it truly is unknown, not zero. */
            Assert.Equal(6L, reader.GetInt64(0));
            Assert.Equal(5_500_000L, reader.GetInt64(1));
            Assert.Equal(400_000L, reader.GetInt64(2));
            await reader.CloseAsync();

            /* The Viewer's worker_time_per_second math: a normal row divides cleanly; the NULL-CPU row
               must produce NULL rather than erroring (NULLIF/division against a NULL numerator is NULL in
               SQL, never a throw — this proves it against the real store and the real query, not by
               argument). */
            await using var viewerProbe = new NpgsqlCommand(
                "SELECT CAST(delta_worker_time AS double precision) / NULLIF(sample_interval_seconds, 0) / 1000.0 AS worker_time_per_second " +
                "FROM collect.query_stats WHERE server_id = $1 ORDER BY collection_time", connection);
            viewerProbe.Parameters.AddWithValue(ServerId);
            await using var viewerReader = await viewerProbe.ExecuteReaderAsync(ct);

            Assert.True(await viewerReader.ReadAsync(ct));
            Assert.False(viewerReader.IsDBNull(0));
            Assert.Equal(6.6667, viewerReader.GetDouble(0), 3);

            Assert.True(await viewerReader.ReadAsync(ct));
            Assert.True(viewerReader.IsDBNull(0), "the NULL-CPU row's worker_time_per_second must be NULL, not an error and not a fabricated zero");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM query_stats WHERE server_id = $1", ServerId));
        }
    }

    private static async Task PlantAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, long? workerTime, long elapsedTime, long executions, int intervalSeconds)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)", connection);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.AddWithValue(DarlingMcpTestData.TruncateToSeconds(at));
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(ServerName);
        insert.Parameters.AddWithValue(Db);
        insert.Parameters.AddWithValue("0xUNKNOWNCPU");
        insert.Parameters.AddWithValue("0xSH1");
        insert.Parameters.AddWithValue((object?)workerTime ?? DBNull.Value);
        insert.Parameters.AddWithValue(elapsedTime);
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
