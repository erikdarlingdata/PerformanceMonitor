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
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5641: the Runnable Queue alert's PostgreSQL window read, <see cref="DarlingAlertReadAdapter.RunnableQueueSql"/>
/// through <see cref="DarlingAlertReadAdapter.GetRunnableQueueWindowAsync"/>, run against a real store. The first
/// lane compiled the text and never ran it; these are the cases the Lite twin pins too. Gated on DARLING_TEST_PG
/// (the pins at the top are not).
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
    private const int WindowMinutes = RunnableQueueEvaluator.WindowMinutes;

    /* ---------------- ungated pins ---------------- */

    [Fact]
    public void RunnableQueueSql_IsTheNewestSampleCarryingTheWindowCountAndAverage()
    {
        var sql = DarlingAlertReadAdapter.RunnableQueueSql;

        Assert.Contains("FROM cpu_scheduler_stats", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        /* A sample whose average is NULL is not a sample: out of the count and the average alike. */
        Assert.Contains("avg_runnable_tasks_count IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) OVER ()::bigint", sql, StringComparison.Ordinal);
        Assert.Contains("(AVG(avg_runnable_tasks_count) OVER ())::double precision", sql, StringComparison.Ordinal);
        /* The newest sample rides along, and a same-instant tie is broken by the newer collection (#3936). */
        Assert.Contains("ORDER BY collection_time DESC, collection_id DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
        /* The collector's per-scheduler average, never the node or offline-CPU columns (#5656). */
        Assert.DoesNotContain("node", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("offline", sql, StringComparison.OrdinalIgnoreCase);
        /* A bare now() is timestamptz and would compare in the server's time zone; the floor is bound. */
        Assert.DoesNotContain("now(", sql.ToLowerInvariant(), StringComparison.Ordinal);
        Assert.DoesNotContain("FROM v_", sql, StringComparison.Ordinal);
    }

    /* ---------------- live ---------------- */

    [Fact]
    public async Task EmptyWindow_ReturnsEmpty()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var window = await adapter.GetRunnableQueueWindowAsync(ServerKey, WindowMinutes, ct);

            Assert.Equal(RunnableQueueWindow.Empty, window);
            Assert.Equal(0, window.SampleCount);
        });
    }

    [Fact]
    public async Task RowsOnlyBeforeTheFloor_ReturnEmpty()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = NaiveUtcNow();
            await InsertAsync(connection, 1, now.AddMinutes(-WindowMinutes - 2), 40.0m, 4, 160, ct);
            await InsertAsync(connection, 2, now.AddMinutes(-WindowMinutes - 9), 50.0m, 4, 200, ct);

            var window = await adapter.GetRunnableQueueWindowAsync(ServerKey, WindowMinutes, ct);

            Assert.Equal(RunnableQueueWindow.Empty, window);
        });
    }

    [Fact]
    public async Task AMixInsideAndOutsideTheWindow_CountsAndAveragesOnlyTheInWindowRows()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = NaiveUtcNow();
            var newest = now.AddMinutes(-1);
            /* Inside: 2.00, 4.00, 9.00 (average 5.00). Outside: two huge rows that would drag the average up. */
            await InsertAsync(connection, 1, newest.AddMinutes(-2), 9.0m, 4, 36, ct);
            await InsertAsync(connection, 2, newest.AddMinutes(-1), 4.0m, 4, 16, ct);
            await InsertAsync(connection, 3, newest, 2.0m, 8, 16, ct);
            await InsertAsync(connection, 4, now.AddMinutes(-WindowMinutes - 2), 500.0m, 4, 2000, ct);
            await InsertAsync(connection, 5, now.AddMinutes(-WindowMinutes - 20), 700.0m, 4, 2800, ct);

            var window = await adapter.GetRunnableQueueWindowAsync(ServerKey, WindowMinutes, ct);

            Assert.Equal(3L, window.SampleCount);
            Assert.Equal(5.0, window.AverageRunnableTasksPerScheduler, 6);
            /* The newest in-window sample rides along. */
            Assert.Equal(8, window.LatestSchedulerCount);
            Assert.Equal(16L, window.LatestTotalRunnableTasks);
            Assert.Equal(newest, window.NewestCollectionTime);
        });
    }

    [Fact]
    public async Task NullAverageRows_AreExcludedFromTheCountAndTheAverage_AndNeverCarryTheNewestSample()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = NaiveUtcNow();
            await InsertAsync(connection, 1, now.AddMinutes(-3), 6.0m, 4, 24, ct);
            await InsertAsync(connection, 2, now.AddMinutes(-2), 12.0m, 4, 48, ct);
            /* The NEWEST row has no average: it is not a sample, so it must not be counted, averaged, or be the
               row whose scheduler_count / total_runnable_tasks_count / collection_time ride along. */
            await InsertAsync(connection, 3, now.AddMinutes(-1), null, 16, 999, ct);

            var window = await adapter.GetRunnableQueueWindowAsync(ServerKey, WindowMinutes, ct);

            Assert.Equal(2L, window.SampleCount);
            Assert.Equal(9.0, window.AverageRunnableTasksPerScheduler, 6);
            Assert.Equal(4, window.LatestSchedulerCount);
            Assert.Equal(48L, window.LatestTotalRunnableTasks);
            Assert.Equal(now.AddMinutes(-2), window.NewestCollectionTime);
        });
    }

    [Fact]
    public async Task ASameInstantTie_IsBrokenByTheNewestCollectionId()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = NaiveUtcNow();
            var instant = now.AddMinutes(-1);
            /* Insert the higher collection_id FIRST, so physical order cannot be what the test passes on. */
            await InsertAsync(connection, 20, instant, 10.0m, 6, 60, ct);
            await InsertAsync(connection, 10, instant, 2.0m, 2, 4, ct);
            await InsertAsync(connection, 15, now.AddMinutes(-2), 3.0m, 3, 9, ct);

            var window = await adapter.GetRunnableQueueWindowAsync(ServerKey, WindowMinutes, ct);

            Assert.Equal(3L, window.SampleCount);
            Assert.Equal(5.0, window.AverageRunnableTasksPerScheduler, 6);
            Assert.Equal(6, window.LatestSchedulerCount);
            Assert.Equal(60L, window.LatestTotalRunnableTasks);
            Assert.Equal(instant, window.NewestCollectionTime);
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

            var window = await adapter.GetRunnableQueueWindowAsync(ServerKey, WindowMinutes, ct);

            Assert.Equal(1L, window.SampleCount);
            Assert.Equal(4.0, window.AverageRunnableTasksPerScheduler, 6);
            Assert.Equal(4, window.LatestSchedulerCount);

            var otherOnly = await adapter.GetRunnableQueueWindowAsync(
                OtherServerId.ToString(CultureInfo.InvariantCulture), WindowMinutes, ct);
            Assert.Equal(1L, otherOnly.SampleCount);
            Assert.Equal(400.0, otherOnly.AverageRunnableTasksPerScheduler, 6);
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

            var window = await ReadWithFloorAsync(connection, floor, ct);

            Assert.Equal(2L, window.SampleCount);
            Assert.Equal(6.0, window.AverageRunnableTasksPerScheduler, 6);
            Assert.Equal(32L, window.LatestTotalRunnableTasks);
            /* The microsecond survives the round trip: the newest sample's own time comes back unrounded. */
            Assert.Equal(floor + oneMicrosecond, window.NewestCollectionTime);
            Assert.Equal(floor.Ticks + 10, window.NewestCollectionTime.Ticks);

            /* One microsecond later the on-floor row drops out. */
            var later = await ReadWithFloorAsync(connection, floor + oneMicrosecond, ct);
            Assert.Equal(1L, later.SampleCount);
            Assert.Equal(8.0, later.AverageRunnableTasksPerScheduler, 6);
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
        int schedulerCount, int totalRunnable, CancellationToken ct, int serverId = ServerId)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO cpu_scheduler_stats
                  (collection_id, collection_time, server_id, server_name, scheduler_count,
                   total_runnable_tasks_count, avg_runnable_tasks_count)
              VALUES ($1, $2, $3, 'runnable-queue-live', $4, $5, $6)", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, collectionId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, collectionTime);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, schedulerCount);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, totalRunnable);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Numeric, Value = (object?)average ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Runs the adapter's SQL text with a floor the test chooses, mapped exactly as the adapter maps it.</summary>
    private static async Task<RunnableQueueWindow> ReadWithFloorAsync(
        NpgsqlConnection connection, DateTime floor, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(DarlingAlertReadAdapter.RunnableQueueSql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(floor);
        using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return RunnableQueueWindow.Empty;
        }

        return new RunnableQueueWindow(
            reader.GetInt64(0),
            reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
            reader.IsDBNull(2) ? null : reader.GetInt32(2),
            reader.IsDBNull(3) ? null : reader.GetInt64(3),
            reader.GetDateTime(4));
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM cpu_scheduler_stats WHERE server_id IN ({ServerId}, {OtherServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
