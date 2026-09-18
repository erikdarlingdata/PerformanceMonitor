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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the File I/O-tab and Blocking-trend reads (W1c viewer copy-parity) against the Darling store
/// contract, no live Postgres. Both File I/O reads mirror Lite's view-based queries over
/// <c>v_file_io_stats</c> (a top-10 CTE by delta activity; the latency read averages stall/op with the
/// queued-stall COALESCE'd; the throughput read derives MB/s from the LAG-based collection interval).
/// The blocking-trend reads mirror Lite's: blocking incidents prefer <c>v_blocked_process_reports</c>
/// and fall back to <c>v_dmv_blocking_snapshots</c> only when the XE source is empty; deadlocks bucket on
/// <c>deadlock_time</c>; lock waits filter <c>v_wait_stats</c> to LCK%. The two Current-Waits reads hit
/// the base <c>waiting_tasks</c> table directly — the Darling store has no <c>v_waiting_tasks</c> view —
/// with the bigint-duration SUM CAST back to bigint for the typed reader.
/// </summary>
public sealed class ViewerFileIoBlockingSqlTests
{
    [Fact]
    public void FileIoLatencyTrendSql_TopFilesCte_PerFileAverageLatency_WithQueuedCoalesce()
    {
        Assert.Contains("FROM v_file_io_stats", ViewerDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);
        Assert.Contains("WITH top_files AS", ViewerDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY SUM(delta_reads + delta_writes) DESC", ViewerDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 10", ViewerDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);

        /* Read/write average latency casts the stall SUM to double before dividing by ops. */
        Assert.Contains("SUM(CAST(f.delta_stall_read_ms AS double precision)) / SUM(f.delta_reads)", ViewerDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);
        Assert.Contains("SUM(CAST(f.delta_stall_write_ms AS double precision)) / SUM(f.delta_writes)", ViewerDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);

        /* Queued latency COALESCEs the queued-stall column to 0 (a pre-queued-column build reads 0). */
        Assert.Contains("COALESCE(f.delta_stall_queued_read_ms, 0)", ViewerDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(f.delta_stall_queued_write_ms, 0)", ViewerDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);

        Assert.Contains("GROUP BY f.collection_time, f.database_name, f.file_name", ViewerDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY f.collection_time, f.database_name, f.file_name", ViewerDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);

        /* Both window bounds are applied (top_files CTE + main query). */
        Assert.Contains("collection_time >= $2", ViewerDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", ViewerDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);
    }

    [Fact]
    public void FileIoThroughputTrendSql_LagInterval_PerSecondMbRate()
    {
        Assert.Contains("FROM v_file_io_stats", ViewerDataService.FileIoThroughputTrendSql, StringComparison.Ordinal);
        Assert.Contains("WITH top_files AS", ViewerDataService.FileIoThroughputTrendSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY SUM(delta_read_bytes + delta_write_bytes) DESC", ViewerDataService.FileIoThroughputTrendSql, StringComparison.Ordinal);

        /* The per-file label is database.file, and the interval is the row's STORED sample_interval_seconds
           (0 → NULL) with the LAG over collection_time only for pre-V127 rows (#3540). */
        Assert.Contains("f.database_name || '.' || f.file_name AS file_label", ViewerDataService.FileIoThroughputTrendSql, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN f.sample_interval_seconds IS NULL", ViewerDataService.FileIoThroughputTrendSql, StringComparison.Ordinal);
        Assert.Contains("LAG(f.collection_time)", ViewerDataService.FileIoThroughputTrendSql, StringComparison.Ordinal);
        Assert.Contains("EXTRACT(EPOCH FROM", ViewerDataService.FileIoThroughputTrendSql, StringComparison.Ordinal);
        Assert.Contains("ELSE NULLIF(f.sample_interval_seconds, 0)", ViewerDataService.FileIoThroughputTrendSql, StringComparison.Ordinal);

        /* Bytes / interval-seconds / 1 MiB, and the NULL-interval rows (first per file, or unknowable) are dropped. */
        Assert.Contains("/ interval_seconds / 1048576.0", ViewerDataService.FileIoThroughputTrendSql, StringComparison.Ordinal);
        Assert.Contains("WHERE interval_seconds IS NOT NULL AND interval_seconds > 0", ViewerDataService.FileIoThroughputTrendSql, StringComparison.Ordinal);
    }

    [Fact]
    public void FileIoReads_ReadColumnsThatExistInTheGeneratedFileIoTable()
    {
        Assert.Equal("file_io_stats", FileIoStatsCollector.Instance.TargetTable);

        var ddl = PgSchemaGenerator.CreateTable(FileIoStatsCollector.Instance);
        foreach (var column in new[]
        {
            "collection_time", "database_name", "file_name",
            "delta_reads", "delta_writes", "delta_read_bytes", "delta_write_bytes",
            "delta_stall_read_ms", "delta_stall_write_ms",
            "delta_stall_queued_read_ms", "delta_stall_queued_write_ms",
        })
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void BlockingTrendSql_XePreferred_DmvFallbackOnlyWhenNoXe()
    {
        Assert.Contains("FROM v_blocked_process_reports", ViewerDataService.BlockingTrendSql, StringComparison.Ordinal);
        Assert.Contains("FROM v_dmv_blocking_snapshots", ViewerDataService.BlockingTrendSql, StringComparison.Ordinal);
        /* The DMV branch contributes only when the XE source has no rows in the window. */
        Assert.Contains("WHERE NOT EXISTS (SELECT 1 FROM bpr)", ViewerDataService.BlockingTrendSql, StringComparison.Ordinal);
        /* Bucketed on event_time (when blocking happened), not collection_time. */
        Assert.Contains("DATE_TRUNC('minute', event_time)", ViewerDataService.BlockingTrendSql, StringComparison.Ordinal);
        Assert.Contains("event_time >= $2 AND event_time <= $3", ViewerDataService.BlockingTrendSql, StringComparison.Ordinal);
    }

    [Fact]
    public void DeadlockTrendSql_BucketsOnDeadlockTime_WindowsOnCollection()
    {
        Assert.Contains("FROM v_deadlocks", ViewerDataService.DeadlockTrendSql, StringComparison.Ordinal);
        Assert.Contains("DATE_TRUNC('minute', deadlock_time)", ViewerDataService.DeadlockTrendSql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", ViewerDataService.DeadlockTrendSql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", ViewerDataService.DeadlockTrendSql, StringComparison.Ordinal);
    }

    [Fact]
    public void LockWaitTrendSql_FiltersLckPrefix_PerSecondRateViaLag()
    {
        Assert.Contains("FROM v_wait_stats", ViewerDataService.LockWaitTrendSql, StringComparison.Ordinal);
        Assert.Contains("wait_type LIKE 'LCK%'", ViewerDataService.LockWaitTrendSql, StringComparison.Ordinal);
        ViewerLatchSpinlockSqlTests.AssertStoredIntervalIdiom(ViewerDataService.LockWaitTrendSql, "wait_type");
        Assert.Contains("CAST(delta_wait_time_ms AS double precision) / interval_seconds END AS wait_time_ms_per_second", ViewerDataService.LockWaitTrendSql, StringComparison.Ordinal);
        Assert.DoesNotContain("ELSE 0", ViewerDataService.LockWaitTrendSql, StringComparison.Ordinal);
    }

    /// <summary>#3540: the latency reads drop rows whose stored interval is 0 — the calculator's "no delta
    /// knowable" marker — so a restart renders as an absent point, never "0.00 ms". IS DISTINCT FROM 0 keeps
    /// pre-V127 rows (NULL). Both the File I/O tab's read and the tempdb tab's file read.</summary>
    [Fact]
    public void FileIoLatencyReads_DropTheUnknowableMarker_KeepPreV127Rows()
    {
        Assert.Contains("AND   f.sample_interval_seconds IS DISTINCT FROM 0", ViewerDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);
        Assert.Contains("AND   sample_interval_seconds IS DISTINCT FROM 0", ViewerDataService.TempDbFileIoTrendSql, StringComparison.Ordinal);
    }

    [Fact]
    public void WaitingTaskTrendSql_ReadsBaseTable_CastsBigintSumToBigint()
    {
        /* No v_waiting_tasks view exists in the Darling store — the read hits the base table. */
        Assert.Contains("FROM waiting_tasks", ViewerDataService.WaitingTaskTrendSql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_waiting_tasks", ViewerDataService.WaitingTaskTrendSql, StringComparison.Ordinal);
        /* SUM of a bigint column is numeric in Postgres, CAST back to bigint for the typed GetInt64 reader. */
        Assert.Contains("CAST(SUM(wait_duration_ms) AS bigint)", ViewerDataService.WaitingTaskTrendSql, StringComparison.Ordinal);
        Assert.Contains("wait_type IS NOT NULL", ViewerDataService.WaitingTaskTrendSql, StringComparison.Ordinal);
    }

    [Fact]
    public void BlockedSessionTrendSql_ReadsBaseTable_OnlyBlockedRows_CountByDatabase()
    {
        Assert.Contains("FROM waiting_tasks", ViewerDataService.BlockedSessionTrendSql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_waiting_tasks", ViewerDataService.BlockedSessionTrendSql, StringComparison.Ordinal);
        Assert.Contains("blocking_session_id > 0", ViewerDataService.BlockedSessionTrendSql, StringComparison.Ordinal);
        Assert.Contains("database_name IS NOT NULL", ViewerDataService.BlockedSessionTrendSql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS blocked_count", ViewerDataService.BlockedSessionTrendSql, StringComparison.Ordinal);
    }

    [Fact]
    public void CurrentWaitsReads_ReadColumnsThatExistInTheGeneratedWaitingTasksTable()
    {
        Assert.Equal("waiting_tasks", WaitingTasksCollector.Instance.TargetTable);

        var ddl = PgSchemaGenerator.CreateTable(WaitingTasksCollector.Instance);
        foreach (var column in new[]
        {
            "collection_time", "session_id", "wait_type", "wait_duration_ms",
            "blocking_session_id", "database_name",
        })
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void DeadlockTrend_ReadsColumnThatExistsInTheGeneratedDeadlocksTable()
    {
        Assert.Equal("deadlocks", DeadlocksCollector.Instance.TargetTable);

        var ddl = PgSchemaGenerator.CreateTable(DeadlocksCollector.Instance);
        foreach (var column in new[] { "collection_time", "deadlock_time" })
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("fileio-latency")]
    [InlineData("fileio-throughput")]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    [InlineData("lockwait")]
    [InlineData("waiting-task")]
    [InlineData("blocked-session")]
    public void NewViewerReads_PgDialect_PositionalParams_NoBareNow_NoNLiterals(string which)
    {
        var sql = which switch
        {
            "fileio-latency" => ViewerDataService.FileIoLatencyTrendSql,
            "fileio-throughput" => ViewerDataService.FileIoThroughputTrendSql,
            "blocking" => ViewerDataService.BlockingTrendSql,
            "deadlock" => ViewerDataService.DeadlockTrendSql,
            "lockwait" => ViewerDataService.LockWaitTrendSql,
            "waiting-task" => ViewerDataService.WaitingTaskTrendSql,
            _ => ViewerDataService.BlockedSessionTrendSql,
        };

        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.Contains("$2", sql, StringComparison.Ordinal);
        Assert.Contains("$3", sql, StringComparison.Ordinal);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the seven new W1c reads: the two File I/O reads
/// (per-file latency with the queued overlay; LAG-derived MB/s dropping the first NULL-interval row),
/// the three Blocking-Trends reads (XE-preferred blocking with the DMV fallback exclusion; deadlock
/// bucketing on deadlock_time; LCK%-only lock waits), and the two Current-Waits reads (the bigint-SUM
/// duration read via GetInt64; the blocked-session count filtered to blocking rows). Shares the
/// serialized "live-postgres" collection so the row churn can't race another class; uses negative
/// sentinel server_ids and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerFileIoBlockingLivePostgresTests
{
    private const int FileIoServerId = -971001;
    private const int ThroughputServerId = -971002;
    private const int BlockingServerId = -971003;
    private const int DeadlockServerId = -971004;
    private const int LockWaitServerId = -971005;
    private const int WaitingTaskServerId = -971006;
    private const int BlockedSessionServerId = -971007;

    private const string ServerName = "viewer-w1c-e2e";

    [Fact]
    public async Task FileIoLatency_PerFileAverage_WithQueuedOverlay_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live File I/O latency test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "file_io_stats", FileIoServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));

            /* Two files in one collection. tempdb_data has queued stall; a zero-activity file is excluded
               from top_files by the (delta_reads > 0 OR delta_writes > 0) guard. */
            await InsertFileIoAsync(connection, FileIoServerId, t1, "tempdb", "tempdb_data",
                deltaReads: 10, deltaWrites: 4, deltaReadBytes: 0, deltaWriteBytes: 0,
                deltaStallReadMs: 100, deltaStallWriteMs: 20, deltaStallQueuedReadMs: 30, deltaStallQueuedWriteMs: 8);
            await InsertFileIoAsync(connection, FileIoServerId, t1, "StackOverflow", "so_data",
                deltaReads: 5, deltaWrites: 0, deltaReadBytes: 0, deltaWriteBytes: 0,
                deltaStallReadMs: 50, deltaStallWriteMs: 0, deltaStallQueuedReadMs: 0, deltaStallQueuedWriteMs: 0);
            await InsertFileIoAsync(connection, FileIoServerId, t1, "Idle", "idle_data",
                deltaReads: 0, deltaWrites: 0, deltaReadBytes: 0, deltaWriteBytes: 0,
                deltaStallReadMs: 0, deltaStallWriteMs: 0, deltaStallQueuedReadMs: 0, deltaStallQueuedWriteMs: 0);

            var rows = await viewer.GetFileIoLatencyTrendAsync(FileIoServerId, t1.AddMinutes(-1), t1.AddMinutes(1));

            /* Idle file excluded (no ops); the two active files survive, ordered by database.file. */
            Assert.Equal(2, rows.Count);
            Assert.Equal(new[] { "StackOverflow", "tempdb" }, rows.Select(r => r.DatabaseName));

            var tempdb = rows.Single(r => r.DatabaseName == "tempdb");
            /* 100 stall / 10 reads = 10 ms read; 20 stall / 4 writes = 5 ms write. */
            Assert.Equal(10.0, tempdb.AvgReadLatencyMs, precision: 3);
            Assert.Equal(5.0, tempdb.AvgWriteLatencyMs, precision: 3);
            /* Queued: 30 / 10 = 3 ms read; 8 / 4 = 2 ms write. */
            Assert.Equal(3.0, tempdb.AvgQueuedReadLatencyMs, precision: 3);
            Assert.Equal(2.0, tempdb.AvgQueuedWriteLatencyMs, precision: 3);

            var so = rows.Single(r => r.DatabaseName == "StackOverflow");
            /* 50 stall / 5 reads = 10 ms read; 0 writes → CASE ELSE 0 write latency. */
            Assert.Equal(10.0, so.AvgReadLatencyMs, precision: 3);
            Assert.Equal(0.0, so.AvgWriteLatencyMs, precision: 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "file_io_stats", FileIoServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task FileIoThroughput_LagInterval_DropsFirstNullInterval_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live File I/O throughput test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "file_io_stats", ThroughputServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var t2 = t1.AddSeconds(60);
            var t3 = t2.AddSeconds(60);

            /* One file across three collections 60s apart. 62914560 bytes / 60s / 1 MiB = 1.0 MB/s. */
            const long oneMbPerSecOverMinute = 1048576L * 60L;
            await InsertFileIoAsync(connection, ThroughputServerId, t1, "db1", "f1",
                deltaReads: 1, deltaWrites: 1, deltaReadBytes: oneMbPerSecOverMinute, deltaWriteBytes: oneMbPerSecOverMinute * 2,
                deltaStallReadMs: 0, deltaStallWriteMs: 0, deltaStallQueuedReadMs: 0, deltaStallQueuedWriteMs: 0);
            await InsertFileIoAsync(connection, ThroughputServerId, t2, "db1", "f1",
                deltaReads: 1, deltaWrites: 1, deltaReadBytes: oneMbPerSecOverMinute, deltaWriteBytes: oneMbPerSecOverMinute * 2,
                deltaStallReadMs: 0, deltaStallWriteMs: 0, deltaStallQueuedReadMs: 0, deltaStallQueuedWriteMs: 0);
            await InsertFileIoAsync(connection, ThroughputServerId, t3, "db1", "f1",
                deltaReads: 1, deltaWrites: 1, deltaReadBytes: oneMbPerSecOverMinute, deltaWriteBytes: oneMbPerSecOverMinute * 2,
                deltaStallReadMs: 0, deltaStallWriteMs: 0, deltaStallQueuedReadMs: 0, deltaStallQueuedWriteMs: 0);

            var rows = await viewer.GetFileIoThroughputTrendAsync(ThroughputServerId, t1.AddMinutes(-1), t3.AddMinutes(1));

            /* First collection has no prior sample (NULL interval) and is dropped; two rows remain. */
            Assert.Equal(2, rows.Count);
            Assert.All(rows, r => Assert.Equal("db1.f1", r.FileLabel));
            Assert.All(rows, r => Assert.Equal(1.0, r.ReadMbPerSec, precision: 3));
            /* Write bytes are double the read bytes → 2.0 MB/s. */
            Assert.All(rows, r => Assert.Equal(2.0, r.WriteMbPerSec, precision: 3));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "file_io_stats", ThroughputServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task BlockingTrend_XePreferred_DmvFallbackWhenNoXe_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live blocking-trend test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "blocked_process_reports", BlockingServerId, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "dmv_blocking_snapshots", BlockingServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var baseTime = TruncateToMinute(DateTime.UtcNow.AddMinutes(-10));
            var minuteA = baseTime;
            var minuteB = baseTime.AddMinutes(3);

            /* Two XE reports in minute A + one in minute B, plus DMV rows that must be excluded while XE exists. */
            await InsertBlockedProcessReportAsync(connection, BlockingServerId, minuteA, minuteA.AddSeconds(5));
            await InsertBlockedProcessReportAsync(connection, BlockingServerId, minuteA, minuteA.AddSeconds(40));
            await InsertBlockedProcessReportAsync(connection, BlockingServerId, minuteB, minuteB.AddSeconds(10));
            await InsertDmvBlockingSnapshotAsync(connection, BlockingServerId, minuteA, minuteA.AddSeconds(20));
            await InsertDmvBlockingSnapshotAsync(connection, BlockingServerId, minuteA, minuteA.AddSeconds(50));

            var withXe = await viewer.GetBlockingTrendAsync(BlockingServerId, baseTime.AddMinutes(-1), baseTime.AddMinutes(10));

            /* Only the XE buckets appear (DMV excluded by WHERE NOT EXISTS): minute A = 2, minute B = 1. */
            Assert.Equal(2, withXe.Count);
            Assert.Equal(2, withXe[0].Count);
            Assert.Equal(1, withXe[1].Count);
            Assert.True(withXe[0].Time < withXe[1].Time);

            /* Remove the XE rows: now the DMV fallback contributes its two minute-A rows. */
            await DeleteRowsAsync(connection, "blocked_process_reports", BlockingServerId, TestContext.Current.CancellationToken);
            var dmvOnly = await viewer.GetBlockingTrendAsync(BlockingServerId, baseTime.AddMinutes(-1), baseTime.AddMinutes(10));
            Assert.Single(dmvOnly);
            Assert.Equal(2, dmvOnly[0].Count);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, "blocked_process_reports", BlockingServerId, cleanupCt);
                await DeleteRowsAsync(cleanup, "dmv_blocking_snapshots", BlockingServerId, cleanupCt);
            });
        }
    }

    [Fact]
    public async Task DeadlockTrend_BucketsOnDeadlockTime_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock-trend test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "deadlocks", DeadlockServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var collectionTime = TruncateToMinute(DateTime.UtcNow.AddMinutes(-10));
            var minuteA = collectionTime;
            var minuteB = collectionTime.AddMinutes(2);

            /* Two deadlocks in minute A, one in minute B — all collected at the same collection_time. */
            await InsertDeadlockAsync(connection, DeadlockServerId, collectionTime, minuteA.AddSeconds(3));
            await InsertDeadlockAsync(connection, DeadlockServerId, collectionTime, minuteA.AddSeconds(45));
            await InsertDeadlockAsync(connection, DeadlockServerId, collectionTime, minuteB.AddSeconds(15));

            var rows = await viewer.GetDeadlockTrendAsync(DeadlockServerId, collectionTime.AddMinutes(-1), collectionTime.AddMinutes(1));

            Assert.Equal(2, rows.Count);
            Assert.Equal(2, rows[0].Count);
            Assert.Equal(1, rows[1].Count);
            Assert.True(rows[0].Time < rows[1].Time);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "deadlocks", DeadlockServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task LockWaitTrend_FiltersLckPrefix_PerSecondRate_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live lock-wait-trend test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "wait_stats", LockWaitServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var t2 = t1.AddSeconds(60);
            var t3 = t2.AddSeconds(60);

            /* Three collections of LCK_M_S 60s apart + a non-LCK wait that must be filtered out. t1/t2 are
               pre-V127 rows (NULL interval); t3 stores the unknowable marker (#3540). */
            await InsertWaitStatAsync(connection, LockWaitServerId, t1, "LCK_M_S", deltaWaitTimeMs: 3000);
            await InsertWaitStatAsync(connection, LockWaitServerId, t2, "LCK_M_S", deltaWaitTimeMs: 6000);
            await InsertWaitStatAsync(connection, LockWaitServerId, t2, "SOS_SCHEDULER_YIELD", deltaWaitTimeMs: 99999);
            await InsertWaitStatAsync(connection, LockWaitServerId, t3, "LCK_M_S", deltaWaitTimeMs: 0, sampleIntervalSeconds: 0);

            var rows = await viewer.GetLockWaitTrendAsync(LockWaitServerId, t1.AddMinutes(-1), t3.AddMinutes(1));

            /* One row: t1 has no prior sample and no stored interval (it used to plot as 0.00), t3 is a
               restart's unknowable marker (absent, never 0.00), and SOS_SCHEDULER_YIELD fails LIKE 'LCK%'. */
            var row = Assert.Single(rows);
            Assert.Equal("LCK_M_S", row.WaitType);
            Assert.Equal(t2.Ticks, row.CollectionTime.Ticks);
            /* 6000 ms / 60 s = 100 ms/sec. */
            Assert.Equal(100.0, row.WaitTimeMsPerSecond, precision: 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "wait_stats", LockWaitServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task WaitingTaskTrend_SumBigintCastToBigint_GroupedByWaitType_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live waiting-task-trend test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "waiting_tasks", WaitingTaskServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var collectionTime = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));

            /* Two LCK_M_X waiters summing past int.MaxValue (proves the numeric SUM → bigint read),
               plus a NULL-wait_type row that must be excluded. */
            await InsertWaitingTaskAsync(connection, WaitingTaskServerId, collectionTime,
                sessionId: 55, waitType: "LCK_M_X", waitDurationMs: 3_000_000_000L, blockingSessionId: 60, databaseName: "AppDb");
            await InsertWaitingTaskAsync(connection, WaitingTaskServerId, collectionTime,
                sessionId: 56, waitType: "LCK_M_X", waitDurationMs: 3_000_000_000L, blockingSessionId: 60, databaseName: "AppDb");
            await InsertWaitingTaskAsync(connection, WaitingTaskServerId, collectionTime,
                sessionId: 57, waitType: null, waitDurationMs: 123, blockingSessionId: 0, databaseName: "AppDb");

            var rows = await viewer.GetWaitingTaskTrendAsync(WaitingTaskServerId, collectionTime.AddMinutes(-1), collectionTime.AddMinutes(1));

            /* One group (LCK_M_X); the NULL-wait_type row is excluded. */
            Assert.Single(rows);
            Assert.Equal("LCK_M_X", rows[0].WaitType);
            Assert.Equal(6_000_000_000L, rows[0].TotalWaitMs);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "waiting_tasks", WaitingTaskServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task BlockedSessionTrend_OnlyBlockedRows_CountByDatabase_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live blocked-session-trend test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "waiting_tasks", BlockedSessionServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var collectionTime = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));

            /* Two blocked sessions in AppDb, one in ReportDb; an unblocked row (blocking_session_id 0)
               and a NULL-database row must both be excluded. */
            await InsertWaitingTaskAsync(connection, BlockedSessionServerId, collectionTime,
                sessionId: 71, waitType: "LCK_M_X", waitDurationMs: 100, blockingSessionId: 90, databaseName: "AppDb");
            await InsertWaitingTaskAsync(connection, BlockedSessionServerId, collectionTime,
                sessionId: 72, waitType: "LCK_M_X", waitDurationMs: 100, blockingSessionId: 90, databaseName: "AppDb");
            await InsertWaitingTaskAsync(connection, BlockedSessionServerId, collectionTime,
                sessionId: 73, waitType: "LCK_M_S", waitDurationMs: 100, blockingSessionId: 91, databaseName: "ReportDb");
            await InsertWaitingTaskAsync(connection, BlockedSessionServerId, collectionTime,
                sessionId: 74, waitType: "CXPACKET", waitDurationMs: 100, blockingSessionId: 0, databaseName: "AppDb");
            await InsertWaitingTaskAsync(connection, BlockedSessionServerId, collectionTime,
                sessionId: 75, waitType: "LCK_M_X", waitDurationMs: 100, blockingSessionId: 92, databaseName: null);

            var rows = await viewer.GetBlockedSessionTrendAsync(BlockedSessionServerId, collectionTime.AddMinutes(-1), collectionTime.AddMinutes(1));

            /* Ordered by database_name: AppDb = 2, ReportDb = 1. */
            Assert.Equal(2, rows.Count);
            Assert.Equal("AppDb", rows[0].DatabaseName);
            Assert.Equal(2, rows[0].BlockedCount);
            Assert.Equal("ReportDb", rows[1].DatabaseName);
            Assert.Equal(1, rows[1].BlockedCount);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "waiting_tasks", BlockedSessionServerId, cleanupCt));
        }
    }

    private static async Task InsertFileIoAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, string databaseName, string fileName,
        long deltaReads, long deltaWrites, long deltaReadBytes, long deltaWriteBytes,
        long deltaStallReadMs, long deltaStallWriteMs, long deltaStallQueuedReadMs, long deltaStallQueuedWriteMs)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, file_name, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes,
     delta_stall_read_ms, delta_stall_write_ms, delta_stall_queued_read_ms, delta_stall_queued_write_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(fileName);
        command.Parameters.AddWithValue(deltaReads);
        command.Parameters.AddWithValue(deltaWrites);
        command.Parameters.AddWithValue(deltaReadBytes);
        command.Parameters.AddWithValue(deltaWriteBytes);
        command.Parameters.AddWithValue(deltaStallReadMs);
        command.Parameters.AddWithValue(deltaStallWriteMs);
        command.Parameters.AddWithValue(deltaStallQueuedReadMs);
        command.Parameters.AddWithValue(deltaStallQueuedWriteMs);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertBlockedProcessReportAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, DateTime eventTimeUtc)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text,
     blocked_process_report_xml, contentious_object)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue("AppDb");
        command.Parameters.AddWithValue(55);
        command.Parameters.AddWithValue(60);
        command.Parameters.AddWithValue(1200L);
        command.Parameters.AddWithValue("X");
        command.Parameters.AddWithValue("SELECT 1");
        command.Parameters.AddWithValue("UPDATE t SET c = 1");
        command.Parameters.AddWithValue("<blocked/>");
        command.Parameters.AddWithValue("dbo.t");
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertDmvBlockingSnapshotAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, DateTime eventTimeUtc)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO dmv_blocking_snapshots
    (collection_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, contentious_object)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue("AppDb");
        command.Parameters.AddWithValue(155);
        command.Parameters.AddWithValue(160);
        command.Parameters.AddWithValue(900L);
        command.Parameters.AddWithValue("S");
        command.Parameters.AddWithValue("SELECT 2");
        command.Parameters.AddWithValue("UPDATE t SET c = 2");
        command.Parameters.AddWithValue("dbo.t");
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertDeadlockAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, DateTime deadlockTimeUtc)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time,
     victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(deadlockTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue("process123");
        command.Parameters.AddWithValue("UPDATE t SET c = 1");
        command.Parameters.AddWithValue("<deadlock/>");
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertWaitStatAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, string waitType, long deltaWaitTimeMs,
        int? sampleIntervalSeconds = null)
    {
        /* sample_interval_seconds NULL by default — a pre-V127 row; 0 is the unknowable marker (#3540). */
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
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)sampleIntervalSeconds ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
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

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    /* The blocking/deadlock trends bucket by DATE_TRUNC('minute', ...), so the tests anchor on a minute
       boundary — otherwise added seconds could spill into the next minute and split a bucket. */
    private static DateTime TruncateToMinute(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, string table, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
