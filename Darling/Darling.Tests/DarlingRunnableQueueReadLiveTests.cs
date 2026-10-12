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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5641: the Runnable Queue alert's PostgreSQL sample read, <see cref="DarlingAlertReadAdapter.RunnableQueueSql"/>
/// through <see cref="DarlingAlertReadAdapter.GetRunnableQueueSamplesAsync"/>, run against a real store: one sample per
/// collection_time, the collector's split rows (#5710) folded by SUM(total) / MAX(scheduler_count), a lone row kept at
/// its own average. These are the cases the Lite twin pins too. Gated on DARLING_TEST_PG (the pin at the top is not).
///
/// <para>Rows are seeded relative to the real clock because the adapter computes its own floor from it, so every
/// in-window row sits at least a minute inside the window and every out-of-window row at least two minutes
/// outside: a slow test host cannot move one across the bar. The two microsecond cases run the SQL text directly
/// with a floor the test chooses, which is the only way to put a row exactly on the floor.</para>
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class DarlingRunnableQueueReadLiveTests
{
    private const int ServerId = -5641001;
    private const int OtherServerId = -5641002;
    private static readonly string ServerKey = ServerId.ToString(CultureInfo.InvariantCulture);
    private const int LookbackMinutes = RunnableQueueEvaluator.FallbackMinutes;

    /* ---------------- ungated pins ---------------- */

    [Fact]
    public void RunnableQueueSql_IsOneRowPerCollectionTimeWithTheSplitRowFold()
    {
        var sql = DarlingAlertReadAdapter.RunnableQueueSql;

        Assert.Contains("FROM cpu_scheduler_stats", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        /* A row whose average is NULL is not a row: out of the count, the sums and the maximum alike. */
        Assert.Contains("avg_runnable_tasks_count IS NOT NULL", sql, StringComparison.Ordinal);
        /* One row per collection_time (the collector can write several, #5710), newest first. */
        Assert.Contains("GROUP BY collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_id", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT", sql, StringComparison.Ordinal);
        /* A lone row keeps its own average; split rows sum the totals over the highest scheduler count, in numeric. */
        Assert.Contains("WHEN COUNT(*) = 1", sql, StringComparison.Ordinal);
        Assert.Contains("THEN MAX(avg_runnable_tasks_count)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(total_runnable_tasks_count)::numeric / NULLIF(MAX(scheduler_count), 0)", sql, StringComparison.Ordinal);
        Assert.Contains(")::double precision AS per_scheduler", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(total_runnable_tasks_count)::bigint", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(scheduler_count)::integer", sql, StringComparison.Ordinal);
        /* The collector's per-scheduler numbers, never the node or offline-CPU columns (#5656). */
        Assert.DoesNotContain("node", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("offline", sql, StringComparison.OrdinalIgnoreCase);
        /* A bare now() is timestamptz and would compare in the server's time zone; the floor is bound. */
        Assert.DoesNotContain("now(", sql.ToLowerInvariant(), StringComparison.Ordinal);
        Assert.DoesNotContain("FROM v_", sql, StringComparison.Ordinal);
    }

    /* ---------------- live ---------------- */

    [Fact]
    public async Task EmptyWindow_ReturnsNoSamples()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var samples = await adapter.GetRunnableQueueSamplesAsync(ServerKey, LookbackMinutes, ct);

            Assert.Empty(samples);
        });
    }

    [Fact]
    public async Task RowsOnlyBeforeTheFloor_ReturnNoSamples()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = NaiveUtcNow();
            await InsertAsync(connection, 1, now.AddMinutes(-LookbackMinutes - 2), 40.0m, 4, 160, ct);
            await InsertAsync(connection, 2, now.AddMinutes(-LookbackMinutes - 9), 50.0m, 4, 200, ct);

            var samples = await adapter.GetRunnableQueueSamplesAsync(ServerKey, LookbackMinutes, ct);

            Assert.Empty(samples);
        });
    }

    [Fact]
    public async Task AMixInsideAndOutsideTheLookback_ReturnsOnlyTheInsideCollections_NewestFirst()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = NaiveUtcNow();
            var newest = now.AddMinutes(-1);
            /* Inside: 9.00, 4.00, 2.00. Outside: two huge rows that must not come back. */
            await InsertAsync(connection, 1, newest.AddMinutes(-2), 9.0m, 4, 36, ct);
            await InsertAsync(connection, 2, newest.AddMinutes(-1), 4.0m, 4, 16, ct);
            await InsertAsync(connection, 3, newest, 2.0m, 8, 16, ct);
            await InsertAsync(connection, 4, now.AddMinutes(-LookbackMinutes - 2), 500.0m, 4, 2000, ct);
            await InsertAsync(connection, 5, now.AddMinutes(-LookbackMinutes - 20), 700.0m, 4, 2800, ct);

            var samples = await adapter.GetRunnableQueueSamplesAsync(ServerKey, LookbackMinutes, ct);

            Assert.Equal(3, samples.Count);
            Assert.Equal(new[] { 2.0, 4.0, 9.0 }, samples.Select(s => s.PerScheduler).ToArray());
            Assert.Equal(new[] { newest, newest.AddMinutes(-1), newest.AddMinutes(-2) }, samples.Select(s => s.CollectionTime).ToArray());
            /* The newest collection's own count and total ride on it. */
            Assert.Equal(8, samples[0].SchedulerCount);
            Assert.Equal(16L, samples[0].TotalRunnableTasks);
        });
    }

    [Fact]
    public async Task ARowOlderThanFiveMinutes_ButInsideTenMinutes_IsReturned_ForTheFallback()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = NaiveUtcNow();
            await InsertAsync(connection, 1, now.AddMinutes(-1), 20.0m, 4, 80, ct);
            await InsertAsync(connection, 2, now.AddMinutes(-4.5), 20.0m, 4, 80, ct);
            await InsertAsync(connection, 3, now.AddMinutes(-8), 20.0m, 4, 80, ct);

            var samples = await adapter.GetRunnableQueueSamplesAsync(ServerKey, LookbackMinutes, ct);

            Assert.Equal(3, samples.Count);
        });
    }

    [Fact]
    public async Task NullAverageRows_AreNotSamples_AndNeverCarryTheNewestCollection()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = NaiveUtcNow();
            await InsertAsync(connection, 1, now.AddMinutes(-3), 6.0m, 4, 24, ct);
            await InsertAsync(connection, 2, now.AddMinutes(-2), 12.0m, 4, 48, ct);
            /* The NEWEST row has no average: it is not a sample, so it must not come back, nor be the collection
               whose scheduler_count / total_runnable_tasks_count / collection_time ride along. */
            await InsertAsync(connection, 3, now.AddMinutes(-1), null, 16, 999, ct);

            var samples = await adapter.GetRunnableQueueSamplesAsync(ServerKey, LookbackMinutes, ct);

            Assert.Equal(2, samples.Count);
            Assert.Equal(new[] { 12.0, 6.0 }, samples.Select(s => s.PerScheduler).ToArray());
            Assert.Equal(4, samples[0].SchedulerCount);
            Assert.Equal(48L, samples[0].TotalRunnableTasks);
            Assert.Equal(now.AddMinutes(-2), samples[0].CollectionTime);
        });
    }

    [Fact]
    public async Task RowsAtOneCollectionTime_AreOneSample_SummedOverTheHighestSchedulerCount()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = NaiveUtcNow();
            var split = now.AddMinutes(-1);
            /* The 15:42:43 collection of the 2026-10-09 incident: two rows, each covering a subset of the four
               schedulers (27.67|83 is three of them, 18.00|18 is one), every row saying scheduler_count 4. The
               higher collection_id goes in FIRST so physical order cannot be what the test passes on. */
            await InsertAsync(connection, 20, split, 18.0m, 4, 18, ct);
            await InsertAsync(connection, 10, split, 27.67m, 4, 83, ct);
            await InsertAsync(connection, 15, now.AddMinutes(-2), 3.0m, 3, 9, ct);

            var samples = await adapter.GetRunnableQueueSamplesAsync(ServerKey, LookbackMinutes, ct);

            Assert.Equal(2, samples.Count);
            Assert.Equal(split, samples[0].CollectionTime);
            Assert.Equal(25.25, samples[0].PerScheduler, 9);
            Assert.Equal(101L, samples[0].TotalRunnableTasks);
            Assert.Equal(4, samples[0].SchedulerCount);
            /* The lone row is untouched. */
            Assert.Equal(3.0, samples[1].PerScheduler, 9);
        });
    }

    [Fact]
    public async Task ASplitCollection_UsesTheTotalsNotTheRowAverages()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = NaiveUtcNow();
            /* 15:49:01: three rows 60.00|60, 50.50|101, 49.00|49 on four schedulers. The mean of the row averages
               is 53.17; the collection is (60 + 101 + 49) / 4 = 52.5. */
            await InsertAsync(connection, 1, now.AddMinutes(-1), 60.0m, 4, 60, ct);
            await InsertAsync(connection, 2, now.AddMinutes(-1), 50.5m, 4, 101, ct);
            await InsertAsync(connection, 3, now.AddMinutes(-1), 49.0m, 4, 49, ct);

            var samples = await adapter.GetRunnableQueueSamplesAsync(ServerKey, LookbackMinutes, ct);

            var sample = Assert.Single(samples);
            Assert.Equal(52.5, sample.PerScheduler, 9);
            Assert.Equal(210L, sample.TotalRunnableTasks);
        });
    }

    [Fact]
    public async Task ASingleRow_UsesItsOwnAverageExactly_NotTotalOverSchedulerCount()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = NaiveUtcNow();
            /* The collector's own AVG is 3.33 here; total / scheduler_count would say 2.5. */
            await InsertAsync(connection, 1, now.AddMinutes(-1), 3.33m, 4, 10, ct);

            var samples = await adapter.GetRunnableQueueSamplesAsync(ServerKey, LookbackMinutes, ct);

            var sample = Assert.Single(samples);
            Assert.Equal(3.33, sample.PerScheduler, 9);
            Assert.Equal(10L, sample.TotalRunnableTasks);
        });
    }

    [Fact]
    public async Task SplitRowsThatAreAllZero_AreAZeroSample()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = NaiveUtcNow();
            /* 15:38:28 in the incident: four rows, all zero but one lone runnable task. And a collection of all-zero rows. */
            await InsertAsync(connection, 1, now.AddMinutes(-2), 0.0m, 4, 0, ct);
            await InsertAsync(connection, 2, now.AddMinutes(-2), 1.0m, 4, 1, ct);
            await InsertAsync(connection, 3, now.AddMinutes(-2), 0.0m, 4, 0, ct);
            await InsertAsync(connection, 4, now.AddMinutes(-1), 0.0m, 4, 0, ct);
            await InsertAsync(connection, 5, now.AddMinutes(-1), 0.0m, 4, 0, ct);

            var samples = await adapter.GetRunnableQueueSamplesAsync(ServerKey, LookbackMinutes, ct);

            Assert.Equal(2, samples.Count);
            Assert.Equal(0.0, samples[0].PerScheduler, 9);
            Assert.Equal(0L, samples[0].TotalRunnableTasks);
            Assert.Equal(0.25, samples[1].PerScheduler, 9);
        });
    }

    [Fact]
    public async Task ANullAverageRowInASplitCollection_IsLeftOutOfTheFold()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = NaiveUtcNow();
            var instant = now.AddMinutes(-1);
            await InsertAsync(connection, 1, instant, 8.0m, 4, 32, ct);
            await InsertAsync(connection, 2, instant, null, 4, 9999, ct);
            await InsertAsync(connection, 3, instant, 4.0m, 4, 16, ct);

            var samples = await adapter.GetRunnableQueueSamplesAsync(ServerKey, LookbackMinutes, ct);

            var sample = Assert.Single(samples);
            Assert.Equal(12.0, sample.PerScheduler, 9);
            Assert.Equal(48L, sample.TotalRunnableTasks);
        });
    }

    [Fact]
    public async Task ASplitCollectionWithoutASchedulerCount_IsNotASample()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = NaiveUtcNow();
            await InsertAsync(connection, 1, now.AddMinutes(-2), 5.0m, 4, 20, ct);
            /* Two rows, no scheduler_count: nothing to divide by, so the collection has no per-scheduler value. */
            await InsertAsync(connection, 2, now.AddMinutes(-1), 5.0m, null, 20, ct);
            await InsertAsync(connection, 3, now.AddMinutes(-1), 5.0m, null, 20, ct);

            var samples = await adapter.GetRunnableQueueSamplesAsync(ServerKey, LookbackMinutes, ct);

            var sample = Assert.Single(samples);
            Assert.Equal(now.AddMinutes(-2), sample.CollectionTime);
        });
    }

    [Fact]
    public async Task AnotherServersRows_AreIgnored()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = NaiveUtcNow();
            await InsertAsync(connection, 1, now.AddMinutes(-2), 4.0m, 4, 16, ct);
            await InsertAsync(connection, 2, now.AddMinutes(-1), 400.0m, 32, 12800, ct, OtherServerId);

            var samples = await adapter.GetRunnableQueueSamplesAsync(ServerKey, LookbackMinutes, ct);

            var sample = Assert.Single(samples);
            Assert.Equal(4.0, sample.PerScheduler, 9);
            Assert.Equal(4, sample.SchedulerCount);

            var otherOnly = await adapter.GetRunnableQueueSamplesAsync(
                OtherServerId.ToString(CultureInfo.InvariantCulture), LookbackMinutes, ct);
            Assert.Equal(400.0, Assert.Single(otherOnly).PerScheduler, 9);
        });
    }

    [Fact]
    public async Task SubMillisecondTimesAtTheFloor_KeepMicrosecondPrecision()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            /* The floor is a chosen value with a sub-millisecond part. PostgreSQL stores microseconds, and a
               floor truncated to milliseconds (or a row rounded) would move a row across the bar. */
            var floor = new DateTime(2026, 10, 12, 12, 0, 0, DateTimeKind.Unspecified).AddTicks(1234560); // .123456 s
            var oneMicrosecond = TimeSpan.FromTicks(10);
            await InsertAsync(connection, 1, floor - oneMicrosecond, 100.0m, 4, 400, ct);           // just before: out
            await InsertAsync(connection, 2, floor, 4.0m, 4, 16, ct);                                // exactly on: in
            await InsertAsync(connection, 3, floor + oneMicrosecond, 8.0m, 4, 32, ct);              // just after: in

            var samples = await ReadWithFloorAsync(connection, floor, ct);

            Assert.Equal(2, samples.Count);
            Assert.Equal(new[] { 8.0, 4.0 }, samples.Select(s => s.PerScheduler).ToArray());
            /* The microsecond survives the round trip: the newest collection's own time comes back unrounded. */
            Assert.Equal(floor + oneMicrosecond, samples[0].CollectionTime);
            Assert.Equal(floor.Ticks + 10, samples[0].CollectionTime.Ticks);

            /* One microsecond later the on-floor row drops out. */
            var later = await ReadWithFloorAsync(connection, floor + oneMicrosecond, ct);
            Assert.Equal(8.0, Assert.Single(later).PerScheduler, 9);
        });
    }

    /* ---------------- helpers ---------------- */

    /* Truncated to whole microseconds: PostgreSQL stores microseconds and a .NET tick (100 ns) is not stored, so a
       seeded time must be one the store can return unchanged. */
    private static DateTime NaiveUtcNow()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Ticks - now.Ticks % 10, DateTimeKind.Unspecified);
    }

    private static async Task RunAsync(Func<DarlingAlertReadAdapter, NpgsqlConnection, CancellationToken, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Runnable Queue read test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var adapter = new DarlingAlertReadAdapter(postgres);

        var bodySucceeded = false;
        try
        {
            await body(adapter, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTime, decimal? average,
        int? schedulerCount, int totalRunnable, CancellationToken ct, int serverId = ServerId)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO cpu_scheduler_stats
                  (collection_id, collection_time, server_id, server_name, scheduler_count,
                   total_runnable_tasks_count, avg_runnable_tasks_count)
              VALUES ($1, $2, $3, 'runnable-queue-live', $4, $5, $6)", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, collectionId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, collectionTime);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = (object?)schedulerCount ?? DBNull.Value });
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, totalRunnable);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Numeric, Value = (object?)average ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Runs the adapter's SQL text with a floor the test chooses, mapped exactly as the adapter maps it.</summary>
    private static async Task<List<RunnableQueueSample>> ReadWithFloorAsync(
        NpgsqlConnection connection, DateTime floor, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(DarlingAlertReadAdapter.RunnableQueueSql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(floor);
        var samples = new List<RunnableQueueSample>();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            samples.Add(new RunnableQueueSample(
                reader.GetDateTime(0),
                reader.GetDouble(1),
                reader.IsDBNull(2) ? null : reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetInt32(3)));
        }

        return samples;
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM cpu_scheduler_stats WHERE server_id IN ({ServerId}, {OtherServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
