/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4349's own tests for the blocking trio's wide-window bucketing (lock waits, waiting-task duration,
/// blocked-session count), mirroring <c>ViewerCpuTempDbTests</c> (#4234) and Lite's
/// <c>LiteMemoryFileIoTrendBucketingTests</c> (#4340) shape: a source check pinning the SQL text, a
/// row-budget cap at a 7-day window, point equality for the rate/count formulas, and singleton-window
/// raw-timestamp stamping.
/// </summary>
public sealed class ViewerBlockingTrendBucketingSqlTests
{
    [Fact]
    public void LockWaitTrendSql_CarriesABucketWidth_AndKeepsThe3540RatedCteNoDeltaRule()
    {
        var sql = ViewerDataService.LockWaitTrendSql;

        Assert.Contains("date_bin(CAST($4 AS integer) * INTERVAL '1 minute'", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBucketSql.OriginSql, sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(date_bin(", sql, StringComparison.Ordinal);
        Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);

        /* #3540: a row whose interval is unknowable is NULLed into the rated CTE, not filtered out of
           the FROM clause — it still counts toward collection_count, and HAVING drops a bucket with no
           rated collection (never a plotted 0.00). */
        Assert.Contains("CASE WHEN interval_seconds > 0 AND delta_wait_time_ms >= 0 THEN delta_wait_time_ms END AS rated_wait_ms", sql, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN interval_seconds > 0 AND delta_wait_time_ms >= 0 THEN interval_seconds END AS rated_seconds", sql, StringComparison.Ordinal);
        Assert.Contains("HAVING COUNT(rated_seconds) > 0", sql, StringComparison.Ordinal);

        /* A bucket's rate is time-weighted (summed wait time over summed seconds), never an average of
           per-collection rates. */
        Assert.Contains("CAST(SUM(rated_wait_ms) AS double precision) / SUM(rated_seconds) AS wait_time_ms_per_second", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void WaitingTaskTrendSql_CarriesABucketWidth_AndSumsDurationPerBucket_CountStarRating()
    {
        var sql = ViewerDataService.WaitingTaskTrendSql;

        Assert.Contains("date_bin(CAST($4 AS integer) * INTERVAL '1 minute'", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBucketSql.OriginSql, sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(date_bin(", sql, StringComparison.Ordinal);
        Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);

        /* A waiting-task snapshot carries no delta or sample interval — every row is unconditionally
           rated, so the bucket total is a plain SUM, and collection_count is a plain COUNT(*) (no
           HAVING needed: a bucket the GROUP BY produced always has at least one row). */
        Assert.Contains("CAST(SUM(wait_duration_ms) AS bigint) AS total_wait_ms", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("HAVING", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BlockedSessionTrendSql_CarriesABucketWidth_AndAveragesThePerSnapshotCount_NeverSums()
    {
        var sql = ViewerDataService.BlockedSessionTrendSql;

        Assert.Contains("date_bin(CAST($5 AS integer) * INTERVAL '1 minute'", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBucketSql.OriginSql, sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(date_bin(", sql, StringComparison.Ordinal);
        Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);

        /* Blocked-session count is a PER-SNAPSHOT gauge, not a delta: the pre-bucket per_collection CTE
           keeps the un-bucketed read's own COUNT(*) per (collection, database), and the outer query
           AVERAGES that across the collections a bucket merges — a plain COUNT(*)/SUM over the merged
           rows would double- (or N-tuple-) count purely because a wide bucket merged N snapshots. The
           inner per_collection CTE legitimately keeps its own COUNT(*) AS blocked_count (that's the
           per-snapshot count being averaged); what must never sum is the OUTER select, so this pins the
           outer query's text specifically rather than the whole SQL string. */
        Assert.Contains("per_collection", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(ROUND(AVG(blocked_count)) AS bigint) AS blocked_count", sql, StringComparison.Ordinal);

        var normalizedSql = sql.ReplaceLineEndings("\n");
        var outerSelectStart = normalizedSql.IndexOf(")\nSELECT", StringComparison.Ordinal);
        Assert.True(outerSelectStart > 0, "expected to find the outer SELECT after the per_collection CTE closes");
        var outerSelect = normalizedSql[outerSelectStart..];
        Assert.Contains("AVG(blocked_count)", outerSelect, StringComparison.Ordinal);
        Assert.DoesNotContain("COUNT(*) AS blocked_count", outerSelect, StringComparison.Ordinal);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for #4349's blocking-trio bucketing: a 7-day window at the
/// waiting_tasks/wait_stats real per-minute cadence must not hand back every raw collection, and a
/// window whose budget covers every collection must return the raw points unchanged, stamped at their
/// own collection_time rather than the bucket grid line. Shares the serialized "live-postgres" collection
/// so the row churn can't race another class; uses negative sentinel server_ids and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerBlockingTrendBucketingLivePostgresTests
{
    private const int LockWaitBudgetServerId = -974901;
    private const int LockWaitSingletonServerId = -974902;
    private const int WaitingTaskBudgetServerId = -974903;
    private const int WaitingTaskSingletonServerId = -974904;
    private const int BlockedSessionBudgetServerId = -974905;
    private const int BlockedSessionSingletonServerId = -974906;
    private const int BlockedSessionMergeServerId = -974907;

    private const string ServerName = "viewer-4349-blocking-bucketing-e2e";

    /// <summary>7-day window, one collection per minute (10,080 rows for a single LCK_M_S wait type) —
    /// the bucketed read caps at <see cref="TrendBudget.Chart"/>'s single-series budget.</summary>
    [Fact]
    public async Task LockWaitTrend_SevenDayWindow_ReturnsAtMostBudgetRows_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live lock-wait budget-cap test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "wait_stats", LockWaitBudgetServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var end = new DateTime(2026, 3, 10, 0, 0, 0);
            var start = end.AddDays(-7);

            await BulkSeedWaitStatAsync(connection, TestContext.Current.CancellationToken, LockWaitBudgetServerId, start, end);

            var rows = await viewer.GetLockWaitTrendAsync(LockWaitBudgetServerId, start, end);

            var budget = TrendBudget.Chart.AutoPoints;
            Assert.True(rows.Count > 0 && rows.Count <= budget, $"lock wait: {rows.Count} rows over a budget of {budget}");
            /* This is the gap the un-bucketed read fails at: 10,080 raw one-minute collections would
               otherwise all survive #3540's rating rule (every row here is rated, second-apart, same
               wait type), so an unbucketed read returns 10,079 rows (the first has no prior LAG interval
               to rate against). */
            Assert.True(rows.Count < 10_079, "the unbucketed read would have returned nearly one row per minute");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "wait_stats", LockWaitBudgetServerId, cleanupCt));
        }
    }

    /// <summary>When the budget covers every collection (three, five minutes apart), every point is
    /// stamped at its own raw collection_time — seeded off the minute grid (:37 seconds) so a fix that
    /// floors to the date_bin grid line instead would lose the seconds and fail this.</summary>
    [Fact]
    public async Task LockWaitTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsUnchanged_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live lock-wait singleton test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "wait_stats", LockWaitSingletonServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = new DateTime(2026, 3, 10, 9, 0, 37);
            var t2 = t1.AddMinutes(5);
            var t3 = t2.AddMinutes(5);

            await InsertWaitStatAsync(connection, LockWaitSingletonServerId, t1, "LCK_M_S", deltaWaitTimeMs: 0, sampleIntervalSeconds: 0);
            await InsertWaitStatAsync(connection, LockWaitSingletonServerId, t2, "LCK_M_S", deltaWaitTimeMs: 6000, sampleIntervalSeconds: 60);
            await InsertWaitStatAsync(connection, LockWaitSingletonServerId, t3, "LCK_M_S", deltaWaitTimeMs: 12000, sampleIntervalSeconds: 60);

            var rows = await viewer.GetLockWaitTrendAsync(LockWaitSingletonServerId, t1.AddMinutes(-1), t3.AddMinutes(1));

            /* t1 has an unrated interval (dropped by HAVING). t2/t3 survive, each its own raw
               collection_time, not a bucket grid line. */
            Assert.Equal(new[] { t2.Ticks, t3.Ticks }, rows.Select(r => r.CollectionTime.Ticks));
            Assert.Equal(new[] { 100.0, 200.0 }, rows.Select(r => r.WaitTimeMsPerSecond));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "wait_stats", LockWaitSingletonServerId, cleanupCt));
        }
    }

    /// <summary>7-day window, one collection per minute — the waiting-task duration read caps at the
    /// single-series point budget too, even though every row is unconditionally rated (no delta).</summary>
    [Fact]
    public async Task WaitingTaskTrend_SevenDayWindow_ReturnsAtMostBudgetRows_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live waiting-task budget-cap test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "waiting_tasks", WaitingTaskBudgetServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var end = new DateTime(2026, 3, 11, 0, 0, 0);
            var start = end.AddDays(-7);

            await BulkSeedWaitingTaskAsync(connection, TestContext.Current.CancellationToken, WaitingTaskBudgetServerId, start, end);

            var rows = await viewer.GetWaitingTaskTrendAsync(WaitingTaskBudgetServerId, start, end);

            var budget = TrendBudget.Chart.AutoPoints;
            Assert.True(rows.Count > 0 && rows.Count <= budget, $"waiting task: {rows.Count} rows over a budget of {budget}");
            Assert.True(rows.Count < 10_080, "the unbucketed read would have returned one row per minute");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "waiting_tasks", WaitingTaskBudgetServerId, cleanupCt));
        }
    }

    /// <summary>When the budget covers every collection, waiting-task points are stamped at their own raw
    /// collection_time and each bucket's total is the SUM of its own single row (a merge test lives
    /// separately below).</summary>
    [Fact]
    public async Task WaitingTaskTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsUnchanged_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live waiting-task singleton test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "waiting_tasks", WaitingTaskSingletonServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = new DateTime(2026, 3, 11, 9, 0, 37);
            var t2 = t1.AddMinutes(5);

            await InsertWaitingTaskAsync(connection, WaitingTaskSingletonServerId, t1,
                sessionId: 55, waitType: "LCK_M_X", waitDurationMs: 100, blockingSessionId: 60, databaseName: "AppDb");
            await InsertWaitingTaskAsync(connection, WaitingTaskSingletonServerId, t2,
                sessionId: 56, waitType: "LCK_M_X", waitDurationMs: 200, blockingSessionId: 60, databaseName: "AppDb");

            var rows = await viewer.GetWaitingTaskTrendAsync(WaitingTaskSingletonServerId, t1.AddMinutes(-1), t2.AddMinutes(1));

            Assert.Equal(new[] { t1.Ticks, t2.Ticks }, rows.Select(r => r.CollectionTime.Ticks));
            Assert.Equal(new[] { 100L, 200L }, rows.Select(r => r.TotalWaitMs));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "waiting_tasks", WaitingTaskSingletonServerId, cleanupCt));
        }
    }

    /// <summary>7-day window, one blocked-session collection per minute — the blocked-session count read
    /// caps at the single-series point budget too.</summary>
    [Fact]
    public async Task BlockedSessionTrend_SevenDayWindow_ReturnsAtMostBudgetRows_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live blocked-session budget-cap test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "waiting_tasks", BlockedSessionBudgetServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var end = new DateTime(2026, 3, 12, 0, 0, 0);
            var start = end.AddDays(-7);

            await BulkSeedBlockedTaskAsync(connection, TestContext.Current.CancellationToken, BlockedSessionBudgetServerId, start, end);

            var rows = await viewer.GetBlockedSessionTrendAsync(BlockedSessionBudgetServerId, start, end);

            var budget = TrendBudget.Chart.AutoPoints;
            Assert.True(rows.Count > 0 && rows.Count <= budget, $"blocked session: {rows.Count} rows over a budget of {budget}");
            Assert.True(rows.Count < 10_080, "the unbucketed read would have returned one row per minute");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "waiting_tasks", BlockedSessionBudgetServerId, cleanupCt));
        }
    }

    /// <summary>When the budget covers every collection, blocked-session points are stamped at their own
    /// raw collection_time and each bucket's count is the un-bucketed per-collection count unchanged.</summary>
    [Fact]
    public async Task BlockedSessionTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsUnchanged_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live blocked-session singleton test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "waiting_tasks", BlockedSessionSingletonServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = new DateTime(2026, 3, 12, 9, 0, 37);
            var t2 = t1.AddMinutes(5);

            /* t1: two blocked rows in AppDb. t2: one blocked row in AppDb. */
            await InsertWaitingTaskAsync(connection, BlockedSessionSingletonServerId, t1,
                sessionId: 71, waitType: "LCK_M_X", waitDurationMs: 100, blockingSessionId: 90, databaseName: "AppDb");
            await InsertWaitingTaskAsync(connection, BlockedSessionSingletonServerId, t1,
                sessionId: 72, waitType: "LCK_M_X", waitDurationMs: 100, blockingSessionId: 90, databaseName: "AppDb");
            await InsertWaitingTaskAsync(connection, BlockedSessionSingletonServerId, t2,
                sessionId: 73, waitType: "LCK_M_X", waitDurationMs: 100, blockingSessionId: 91, databaseName: "AppDb");

            var rows = await viewer.GetBlockedSessionTrendAsync(BlockedSessionSingletonServerId, t1.AddMinutes(-1), t2.AddMinutes(1));

            Assert.Equal(new[] { t1.Ticks, t2.Ticks }, rows.Select(r => r.CollectionTime.Ticks));
            Assert.Equal(new[] { 2, 1 }, rows.Select(r => r.BlockedCount));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "waiting_tasks", BlockedSessionSingletonServerId, cleanupCt));
        }
    }

    /// <summary>The defect this lane found and fixed: two blocked-session collections merged into one
    /// bucket must AVERAGE the per-collection counts (2 blocked at t1, 4 at t2 -&gt; average 3), never SUM
    /// them (which would read 6, double-counting purely because the wide bucket merged two snapshots with
    /// no more blocking having happened). A window wide enough to force a multi-minute bucket, one merged
    /// bucket only (no other collections in range) makes the assertion exact.</summary>
    [Fact]
    public async Task BlockedSessionTrend_MergedBucket_AveragesThePerSnapshotCount_NeverSums_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live blocked-session merge test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "waiting_tasks", BlockedSessionMergeServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var end = new DateTime(2026, 3, 13, 0, 0, 0);
            var start = end.AddDays(-7);

            /* One collection every 5 minutes across a 7-day window (2,016 collections) — enough that
               TrendBuckets.AutoMinutes must widen past 1 minute and merge multiple collections per
               bucket, but few enough that BulkSeedBlockedTaskAsync's fixed row counts land predictably. */
            await BulkSeedBlockedTaskAtIntervalAsync(connection, TestContext.Current.CancellationToken,
                BlockedSessionMergeServerId, start, end, intervalMinutes: 5, blockedRowsPerCollection: 4);

            var rows = await viewer.GetBlockedSessionTrendAsync(BlockedSessionMergeServerId, start, end);

            Assert.NotEmpty(rows);
            /* Every collection in this fixture holds exactly 4 blocked rows, so every merged bucket's
               average is 4 regardless of how many collections it merged. A SUM-based regression would
               read a multiple of 4 (8, 12, ...) in any bucket that merged more than one collection. */
            Assert.All(rows, r => Assert.Equal(4, r.BlockedCount));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "waiting_tasks", BlockedSessionMergeServerId, cleanupCt));
        }
    }

    private static async Task InsertWaitStatAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, string waitType, long deltaWaitTimeMs,
        int? sampleIntervalSeconds = null)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(waitType);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(deltaWaitTimeMs);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)sampleIntervalSeconds ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Integer });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>One rated LCK_M_S collection per minute (60s stored interval, so #3540 never nulls it
    /// out), from <paramref name="start"/> to <paramref name="end"/> inclusive, generated server-side.</summary>
    private static async Task BulkSeedWaitStatAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, int serverId, DateTime start, DateTime end)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, sample_interval_seconds)
SELECT 1, g, $1, $2, 'LCK_M_S', 1, 6000, 60
FROM generate_series($3::timestamp, $4::timestamp, interval '1 minute') AS g", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertWaitingTaskAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc,
        int sessionId, string? waitType, long waitDurationMs, int blockingSessionId, string? databaseName)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO waiting_tasks
    (collection_id, collection_time, server_id, server_name,
     session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(sessionId);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)waitType ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.AddWithValue(waitDurationMs);
        command.Parameters.AddWithValue(blockingSessionId);
        command.Parameters.AddWithValue("keylock");
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)databaseName ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>One (unblocked, since blocking_session_id is 0) waiting-task collection per minute, for
    /// the waiting-task duration budget-cap test (that read has no blocked-session filter).</summary>
    private static async Task BulkSeedWaitingTaskAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, int serverId, DateTime start, DateTime end)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO waiting_tasks
    (collection_id, collection_time, server_id, server_name,
     session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name)
SELECT row_number() OVER (), g, $1, $2, 55, 'LCK_M_X', 100, 0, 'keylock', 'AppDb'
FROM generate_series($3::timestamp, $4::timestamp, interval '1 minute') AS g", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One blocked-session row per minute (blocking_session_id set, database AppDb), for the
    /// blocked-session-count budget-cap test.</summary>
    private static async Task BulkSeedBlockedTaskAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, int serverId, DateTime start, DateTime end)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO waiting_tasks
    (collection_id, collection_time, server_id, server_name,
     session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name)
SELECT row_number() OVER (), g, $1, $2, 71, 'LCK_M_X', 100, 90, 'keylock', 'AppDb'
FROM generate_series($3::timestamp, $4::timestamp, interval '1 minute') AS g", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>A fixed number of blocked-session rows in EVERY collection, spaced <paramref
    /// name="intervalMinutes"/> apart, across the whole window — the merged-bucket average test's seed.
    /// One INSERT per collection's row count via generate_series cross join, so no per-row round trip.</summary>
    private static async Task BulkSeedBlockedTaskAtIntervalAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, int serverId, DateTime start, DateTime end,
        int intervalMinutes, int blockedRowsPerCollection)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO waiting_tasks
    (collection_id, collection_time, server_id, server_name,
     session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name)
SELECT (row_number() OVER ()), g, $1, $2, 70 + s, 'LCK_M_X', 100, 90 + s, 'keylock', 'AppDb'
FROM generate_series($3::timestamp, $4::timestamp, ($5 || ' minutes')::interval) AS g
CROSS JOIN generate_series(1, $6) AS s", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(intervalMinutes.ToString());
        command.Parameters.AddWithValue(blockedRowsPerCollection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, string table, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
