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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Latches &amp; Spinlocks tab's four reads against the Darling store contract (no live
/// Postgres): the two per-second trend reads (top-5 by delta, normalized to ms/sec and collisions/sec via
/// each row's stored sample_interval_seconds since V127/#3540, with the per-contender LAG interval — the
/// same truncate-then-diff epoch idiom the wait trend uses — as the fallback for pre-V127 rows that never
/// recorded one) and the two latest-snapshot grid reads (most recent collection in the window, ordered by
/// recent delta). All four run on the <c>v_latch_stats</c> / <c>v_spinlock_stats</c> passthrough views.
/// </summary>
public sealed class ViewerLatchSpinlockSqlTests
{
    [Fact]
    public void LatchTrendSql_TopFiveByDeltaWait_PerClassLagPerSecond_OverTheWindow()
    {
        var sql = ViewerDataService.LatchTrendSql;

        Assert.Contains("FROM v_latch_stats", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);

        /* Top-5 latch classes by total delta wait time (mirrors the Dashboard's GetLatchStatsTopNAsync). */
        Assert.Contains("WITH top_latches AS", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY SUM(delta_wait_time_ms) DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 5", sql, StringComparison.Ordinal);
        Assert.Contains("latch_class IN (SELECT latch_class FROM top_latches)", sql, StringComparison.Ordinal);

        /* #3540: the STORED interval first; the per-class LAG interval (the wait-stats truncate-then-diff
           epoch idiom) only for pre-V127 rows; 0 → NULL through NULLIF; no ELSE 0 on the rate. */
        AssertStoredIntervalIdiom(sql, "latch_class");
        Assert.Contains("CAST(delta_wait_time_ms AS DOUBLE PRECISION) / interval_seconds END AS wait_time_ms_per_second", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ELSE 0 END AS wait_time_ms_per_second", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY latch_class, collection_time", sql, StringComparison.Ordinal);
    }

    /// <summary>The #3540 reader idiom every per-second read over the four interval-carrying delta families
    /// shares: the stored interval when the row has one (0, the calculator's unknowable marker, mapped to
    /// NULL), the LAG derivation only when it does not (a pre-V127 row).</summary>
    internal static void AssertStoredIntervalIdiom(string sql, string partition)
    {
        Assert.Contains("CASE WHEN sample_interval_seconds IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains($"LAG(collection_time) OVER (PARTITION BY {partition} ORDER BY collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("ELSE NULLIF(sample_interval_seconds, 0)", sql, StringComparison.Ordinal);
        Assert.Contains("END AS interval_seconds", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void LatchSnapshotSql_LatestCollectionInWindow_OrderedByRecentDelta_CapAt20()
    {
        var sql = ViewerDataService.LatchSnapshotSql;

        Assert.Contains("FROM v_latch_stats", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT MAX(collection_time) AS mx", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time = (SELECT mx FROM latest)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY delta_wait_time_ms DESC, wait_time_ms DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 20", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SpinlockTrendSql_TopFiveByDeltaCollisions_PerNameLagPerSecond_OverTheWindow()
    {
        var sql = ViewerDataService.SpinlockTrendSql;

        Assert.Contains("FROM v_spinlock_stats", sql, StringComparison.Ordinal);
        Assert.Contains("WITH top_spinlocks AS", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY SUM(delta_collisions) DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 5", sql, StringComparison.Ordinal);
        Assert.Contains("spinlock_name IN (SELECT spinlock_name FROM top_spinlocks)", sql, StringComparison.Ordinal);
        AssertStoredIntervalIdiom(sql, "spinlock_name");
        Assert.Contains("CAST(delta_collisions AS DOUBLE PRECISION) / interval_seconds END AS collisions_per_second", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ELSE 0", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY spinlock_name, collection_time", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SpinlockSnapshotSql_LatestCollectionInWindow_OrderedByRecentDelta_CapAt20()
    {
        var sql = ViewerDataService.SpinlockSnapshotSql;

        Assert.Contains("FROM v_spinlock_stats", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT MAX(collection_time) AS mx", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time = (SELECT mx FROM latest)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY delta_collisions DESC, collisions DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 20", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("latch-trend")]
    [InlineData("latch-snapshot")]
    [InlineData("spinlock-trend")]
    [InlineData("spinlock-snapshot")]
    public void LatchSpinlockSql_PgDialect_PositionalParams_NoBareNow_NoNLiterals(string which)
    {
        var sql = which switch
        {
            "latch-trend" => ViewerDataService.LatchTrendSql,
            "latch-snapshot" => ViewerDataService.LatchSnapshotSql,
            "spinlock-trend" => ViewerDataService.SpinlockTrendSql,
            _ => ViewerDataService.SpinlockSnapshotSql,
        };

        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.Contains("$2", sql, StringComparison.Ordinal);
        Assert.Contains("$3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void LatchSql_ReadsColumnsThatExistInTheGeneratedLatchTable()
    {
        Assert.Equal("latch_stats", LatchStatsCollector.Instance.TargetTable);

        var ddl = PgSchemaGenerator.CreateTable(LatchStatsCollector.Instance);
        foreach (var column in new[]
        {
            "collection_time", "latch_class", "waiting_requests_count", "wait_time_ms",
            "max_wait_time_ms", "delta_waiting_requests_count", "delta_wait_time_ms",
        })
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void SpinlockSql_ReadsColumnsThatExistInTheGeneratedSpinlockTable()
    {
        Assert.Equal("spinlock_stats", SpinlockStatsCollector.Instance.TargetTable);

        var ddl = PgSchemaGenerator.CreateTable(SpinlockStatsCollector.Instance);
        foreach (var column in new[]
        {
            "collection_time", "spinlock_name", "collisions", "spins", "spins_per_collision",
            "sleep_time", "backoffs", "delta_collisions", "delta_spins",
        })
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void LatchAndSpinlockViews_ArePinnedInTheAuthoritativeViewList()
    {
        /* The tab reads the v_ passthrough views; both must be in the store's authoritative view set
           (so V14 refreshes them and the migration test cross-checks them). */
        Assert.Contains("v_latch_stats", PgSchemaGenerator.AllPassthroughViews);
        Assert.Contains("v_spinlock_stats", PgSchemaGenerator.AllPassthroughViews);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the Latches &amp; Spinlocks reads: the latch trend's
/// top-5 selection + per-class ms/sec math, and the spinlock snapshot's latest-collection + recent-delta
/// ordering. Shares the serialized "live-postgres" collection; uses negative sentinel server_ids and
/// cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerLatchSpinlockLivePostgresTests
{
    private const int LatchServerId = -940001;
    private const string LatchServerName = "viewer-latch-e2e";
    private const int SpinlockServerId = -940002;
    private const string SpinlockServerName = "viewer-spinlock-e2e";

    [Fact]
    public async Task LatchTrend_TopFiveByDelta_PerSecond_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live latch-trend test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteAsync(connection, "latch_stats", LatchServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var t2 = t1.AddMinutes(5);   // 300 seconds later
            var t3 = t2.AddMinutes(5);
            var t4 = t3.AddMinutes(5);

            /* BUFFER across four collections (#3540). t1/t2 are pre-V127 rows (NULL interval): t1 has no
               prior and is NOT a point (it used to plot as 0.00); t2 is 300 ms over the LAG's 300 s = 1.0.
               t3 stores interval 0 — the calculator's "no delta knowable" marker, a restart — and must be
               ABSENT rather than 0.00. t4 stores a measured 120 s beside a 600 ms delta = 5.0 ms/sec, and
               the stored interval wins over the LAG (which would say 300 s → 2.0). */
            await InsertLatchAsync(connection, 1, t1, "BUFFER", deltaWait: 100, deltaReqs: 5);
            await InsertLatchAsync(connection, 2, t2, "BUFFER", deltaWait: 300, deltaReqs: 3);
            await InsertLatchAsync(connection, 3, t3, "BUFFER", deltaWait: 0, deltaReqs: 0, sampleIntervalSeconds: 0);
            await InsertLatchAsync(connection, 4, t4, "BUFFER", deltaWait: 600, deltaReqs: 6, sampleIntervalSeconds: 120);

            var trend = await viewer.GetLatchStatsTrendAsync(LatchServerId, t1.AddMinutes(-1), t4.AddMinutes(1));

            var buffer = trend.Where(p => p.LatchClass == "BUFFER").OrderBy(p => p.CollectionTime).ToList();
            Assert.Equal(new[] { t2.Ticks, t4.Ticks }, buffer.Select(p => p.CollectionTime.Ticks).ToArray());
            Assert.Equal(1.0, buffer[0].WaitTimeMsPerSecond, precision: 3);
            Assert.Equal(5.0, buffer[1].WaitTimeMsPerSecond, precision: 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, "latch_stats", LatchServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task SpinlockSnapshot_LatestCollection_OrderedByRecentDelta_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live spinlock-snapshot test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteAsync(connection, "spinlock_stats", SpinlockServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var t2 = t1.AddMinutes(5);

            /* Older collection (t1) must be ignored; the snapshot is the latest (t2). At t2, LOCK_HASH
               has the larger delta so it sorts above SOS_CACHESTORE. */
            await InsertSpinlockAsync(connection, 1, t1, "LOCK_HASH", collisions: 10, deltaCollisions: 10);
            await InsertSpinlockAsync(connection, 2, t2, "SOS_CACHESTORE", collisions: 500, deltaCollisions: 50);
            await InsertSpinlockAsync(connection, 2, t2, "LOCK_HASH", collisions: 900, deltaCollisions: 400);

            var snapshot = await viewer.GetSpinlockStatsSnapshotAsync(SpinlockServerId, t1.AddMinutes(-1), t2.AddMinutes(1));

            Assert.Equal(2, snapshot.Count);
            Assert.Equal("LOCK_HASH", snapshot[0].SpinlockName);   // larger delta first
            Assert.Equal(400, snapshot[0].DeltaCollisions);
            Assert.Equal("SOS_CACHESTORE", snapshot[1].SpinlockName);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, "spinlock_stats", SpinlockServerId, cleanupCt));
        }
    }

    private static async Task InsertLatchAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTimeUtc,
        string latchClass, long deltaWait, long deltaReqs, int? sampleIntervalSeconds = null)
    {
        /* sample_interval_seconds NULL by default — a pre-V127 row, the shape every pin above was written
           against; a test that wants the V127 contract passes 0 (unknowable) or a measured value. */
        using var command = new NpgsqlCommand(@"
INSERT INTO latch_stats
    (collection_id, collection_time, server_id, server_name, latch_class,
     waiting_requests_count, wait_time_ms, max_wait_time_ms,
     delta_waiting_requests_count, delta_wait_time_ms, delta_max_wait_time_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 0, 0, 0, $6, $7, 0, $8)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(LatchServerId);
        command.Parameters.AddWithValue(LatchServerName);
        command.Parameters.AddWithValue(latchClass);
        command.Parameters.AddWithValue(deltaReqs);
        command.Parameters.AddWithValue(deltaWait);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)sampleIntervalSeconds ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertSpinlockAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTimeUtc,
        string spinlockName, long collisions, long deltaCollisions)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO spinlock_stats
    (collection_id, collection_time, server_id, server_name, spinlock_name,
     collisions, spins, spins_per_collision, sleep_time, backoffs,
     delta_collisions, delta_spins, delta_sleep_time, delta_backoffs)
VALUES ($1, $2, $3, $4, $5, $6, 0, 0, 0, 0, $7, 0, 0, 0)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(SpinlockServerId);
        command.Parameters.AddWithValue(SpinlockServerName);
        command.Parameters.AddWithValue(spinlockName);
        command.Parameters.AddWithValue(collisions);
        command.Parameters.AddWithValue(deltaCollisions);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteAsync(NpgsqlConnection connection, string table, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
