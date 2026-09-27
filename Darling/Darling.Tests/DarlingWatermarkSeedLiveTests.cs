/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1575 gated-live contract for the batched watermark read (<see cref="DarlingWorker.ReadCollectorWatermarksAsync"/>)
/// the connect / reload seed path uses: seed a few collection_log rows for a negative sentinel server_id, read
/// them back, and prove the SELECT returns MAX(collection_time) PER collector (any status) and that the naive-UTC
/// <c>timestamp</c> value round-trips relabeled Kind=Utc. The seed POLICY itself (recently-run / overdue /
/// never-run) is the pure decision-table in <see cref="DarlingSweepSchedulingTests"/>; this pins only the store
/// read that feeds it. Runs against a real Postgres gated on <c>DARLING_TEST_PG</c> (skipped otherwise), mirroring
/// <see cref="DarlingSelfAlertTests"/>; shares the serialized "live-postgres" collection, uses a negative sentinel
/// server_id, and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingWatermarkSeedLiveTests
{
    private const int LiveServerId = -915757;
    private const string Name = "WATERMARK-SEED-SRV";

    [Fact]
    public async Task ReadCollectorWatermarks_ReturnsMaxPerCollector_RelabeledUtc()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live watermark read.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteLiveRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            /* Whole-second naive-UTC times (the storage form of the collection_time `timestamp` column) so the
               Postgres-microsecond / .NET-tick round-trip is exact and can't flake on precision. Anchored to
               "now", not a fixed calendar date (#4469): the read is bounded to
               DarlingWorker.WatermarkFloorLookback (2 days) behind now, so every seeded row here stays
               inside that floor — indexRun (25h back) is a daily collector's real last run, still inside the
               2-day floor even though it is already overdue on its own 24h cadence. */
            var baseTime = DateTime.SpecifyKind(
                new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, DateTimeKind.Utc),
                DateTimeKind.Unspecified);
            var waitOld = baseTime.AddMinutes(-10);
            var waitNew = baseTime;                    /* newest wait_stats row → the per-collector MAX */
            var indexRun = baseTime.AddHours(-25);     /* a daily collector, deliberately stale but inside the floor */

            long logId = 8_500_000;
            await InsertLogAsync(connection, ct, logId++, "wait_stats", waitOld, "SUCCESS");
            /* A FAILED run still counts toward the watermark — it reset the cadence clock, exactly as the
               steady-state advance retries every interval regardless of outcome. */
            await InsertLogAsync(connection, ct, logId++, "wait_stats", waitNew, "ERROR");
            await InsertLogAsync(connection, ct, logId++, "index_object_stats", indexRun, "SUCCESS");

            var watermarks = await DarlingWorker.ReadCollectorWatermarksAsync(postgres, LiveServerId, logger: null, ct);

            Assert.True(watermarks.ContainsKey("wait_stats"), "seeded wait_stats must appear");
            Assert.True(watermarks.ContainsKey("index_object_stats"), "seeded index_object_stats must appear");
            Assert.False(watermarks.ContainsKey("query_stats"), "a never-seeded collector must be absent (null watermark)");

            /* MAX per collector, relabeled Kind=Utc (a relabel, NOT a timezone shift). */
            Assert.Equal(DateTimeKind.Utc, watermarks["wait_stats"].Kind);
            Assert.Equal(waitNew.Ticks, watermarks["wait_stats"].Ticks);
            Assert.Equal(indexRun.Ticks, watermarks["index_object_stats"].Ticks);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteLiveRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertLogAsync(
        NpgsqlConnection connection, CancellationToken ct, long logId, string collector, DateTime time, string status)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, 0, $6, NULL, 0, 0, 0)", connection);
        command.Parameters.AddWithValue(logId);
        command.Parameters.AddWithValue(LiveServerId);
        command.Parameters.AddWithValue(Name);
        command.Parameters.AddWithValue(collector);
        command.Parameters.AddWithValue(time);
        command.Parameters.AddWithValue(status);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteLiveRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collection_log WHERE server_id = {LiveServerId.ToString(CultureInfo.InvariantCulture)};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
