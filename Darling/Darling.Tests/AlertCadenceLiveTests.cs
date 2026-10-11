/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The live half of the #5642 alert cadence tests: the recent-samples read the fleet pass makes for each connected server
/// runs against a real store. Runs in CI only (no <c>DARLING_TEST_PG</c> means skipped). The read feeds the engine's
/// persistence gate one sample at a time, so its bounds ARE the CPU alert's bounds: 15 minutes back, a real UTC instant
/// only, at most 32 samples, oldest first.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertCadenceLiveTests
{
    private const int LiveServerId = -770180;
    private const long FirstCollectionId = 9_200_000L;

    [Fact]
    public async Task LiveStore_TheRecentSamplesRead_IsBoundedTo32Samples_FifteenMinutes_RealUtcInstants_OldestFirst()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live recent-samples read.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            var newest = DateTime.SpecifyKind(now, DateTimeKind.Unspecified);

            /* 40 samples, 15 s apart, newest first: more than the bound, all inside the window. */
            for (var i = 0; i < 40; i++)
            {
                var at = newest.AddSeconds(-15 * i);
                await InsertAsync(connection, FirstCollectionId + i, at, at, sqlCpu: 80 + (i % 10), ct);
            }

            /* A sample with no UTC twin (a pre-V134 row) and one past the 15 minute window: neither is read. */
            await InsertAsync(connection, FirstCollectionId + 100, newest.AddSeconds(-5), null, sqlCpu: 99, ct);
            await InsertAsync(connection, FirstCollectionId + 101, newest.AddMinutes(-20), newest.AddMinutes(-20), sqlCpu: 98, ct);

            var worker = SelfAlertFleetPassTests.MakeWorker();
            worker.StoreForTests = postgres;

            var samples = await worker.ReadRecentCpuSamplesAsync(LiveServerId, now, ct);

            Assert.Equal(32, samples.Count);
            Assert.True(
                samples.Zip(samples.Skip(1), (a, b) => a.SampleTimeUtc <= b.SampleTimeUtc).All(ordered => ordered),
                "the samples must come back oldest first");
            /* The newest 32 of the 40 are kept (the LIMIT keeps the newest), so the last one is the newest. */
            Assert.True(Math.Abs((samples[^1].SampleTimeUtc - newest).TotalSeconds) < 1);
            Assert.True(Math.Abs((samples[0].SampleTimeUtc - newest.AddSeconds(-15 * 31)).TotalSeconds) < 1);
            Assert.DoesNotContain(samples, s => s.SqlCpuPercent is 98 or 99);
            Assert.All(samples, s => Assert.Equal(s.SqlCpuPercent, s.TotalCpuPercent));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteAsync(cleanup, cleanupCt);
            });
        }
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTime, DateTime? sampleTimeUtc, int sqlCpu, CancellationToken ct)
    {
        using var insert = new NpgsqlCommand(
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, "
            + "sqlserver_cpu_utilization, other_process_cpu_utilization, sample_time_utc) "
            + "VALUES ($1, $2, $3, 'example-sql-live', $4, $5, 0, $6)", connection);
        insert.Parameters.AddWithValue(collectionId);
        insert.Parameters.AddWithValue(collectionTime);
        insert.Parameters.AddWithValue(LiveServerId);
        insert.Parameters.AddWithValue(collectionTime);
        insert.Parameters.AddWithValue(sqlCpu);
        insert.Parameters.Add(new NpgsqlParameter { Value = sampleTimeUtc is { } utc ? utc : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp });
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM cpu_utilization_stats WHERE server_id = {LiveServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
