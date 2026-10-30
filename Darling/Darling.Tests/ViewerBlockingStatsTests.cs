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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Blocking Stats sub-tab's blocking-duration aggregate read against the Darling store contract
/// (no live Postgres). This is the SEVERITY companion to the blocking-incident COUNT trend: it buckets the
/// raw blocked-process rows by minute and aggregates each block's <c>wait_time_ms</c> into total / max / avg
/// duration. Critically it reuses the count trend's EXACT XE-preferred / DMV-fallback source selection
/// (<c>WHERE NOT EXISTS (SELECT 1 FROM bpr)</c>) so the severity and count surfaces reconcile. Postgres
/// dialect adaptations mirror the sibling reads: COUNT(*) / SUM are read as bigint (SUM CAST back from
/// numeric), AVG CAST to double precision.
/// </summary>
public sealed class ViewerBlockingStatsSqlTests
{
    [Fact]
    public void BlockingDurationStatsSql_XePreferred_DmvFallbackOnlyWhenNoXe()
    {
        Assert.Contains("FROM v_blocked_process_reports", ViewerDataService.BlockingDurationStatsSql, StringComparison.Ordinal);
        Assert.Contains("FROM v_dmv_blocking_snapshots", ViewerDataService.BlockingDurationStatsSql, StringComparison.Ordinal);
        /* The DMV branch contributes only when the XE source has no rows in the window — the SAME exclusion
           as the blocking-incident count trend, so the two surfaces reconcile row-for-row. */
        Assert.Contains("WHERE NOT EXISTS (SELECT 1 FROM bpr)", ViewerDataService.BlockingDurationStatsSql, StringComparison.Ordinal);
    }

    [Fact]
    public void BlockingDurationStatsSql_AggregatesWaitTimeMs_TotalMaxAvg()
    {
        /* The duration source is each blocked process's wait_time_ms (the verified block-duration column). */
        Assert.Contains("SUM(wait_time_ms)", ViewerDataService.BlockingDurationStatsSql, StringComparison.Ordinal);
        Assert.Contains("MAX(wait_time_ms)", ViewerDataService.BlockingDurationStatsSql, StringComparison.Ordinal);
        Assert.Contains("AVG(wait_time_ms)", ViewerDataService.BlockingDurationStatsSql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS event_count", ViewerDataService.BlockingDurationStatsSql, StringComparison.Ordinal);
        /* SUM of a bigint column is numeric in Postgres → CAST back to bigint for the typed GetInt64 reader;
           AVG is numeric → CAST to double precision for GetDouble (the sibling reads' exact adaptations). */
        Assert.Contains("CAST(SUM(wait_time_ms) AS bigint)", ViewerDataService.BlockingDurationStatsSql, StringComparison.Ordinal);
        Assert.Contains("CAST(AVG(wait_time_ms) AS double precision)", ViewerDataService.BlockingDurationStatsSql, StringComparison.Ordinal);
    }

    [Fact]
    public void BlockingDurationStatsSql_BucketsPerMinute_BothWindowBoundsOnEventTime()
    {
        /* Bucketed on event_time (when blocking happened), not collection_time — matching the count trend so
           the buckets line up 1:1 across the severity and count charts. */
        Assert.Contains("DATE_TRUNC('minute', event_time)", ViewerDataService.BlockingDurationStatsSql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY DATE_TRUNC('minute', event_time)", ViewerDataService.BlockingDurationStatsSql, StringComparison.Ordinal);
        /* Both window bounds are applied (the XE and DMV CTEs each carry them). */
        Assert.Contains("event_time >= $2 AND event_time <= $3", ViewerDataService.BlockingDurationStatsSql, StringComparison.Ordinal);
    }

    [Fact]
    public void BlockingDurationStatsSql_PgDialect_PositionalParams_NoBareNow_NoNLiterals()
    {
        var sql = ViewerDataService.BlockingDurationStatsSql;
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.Contains("$2", sql, StringComparison.Ordinal);
        Assert.Contains("$3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BlockingDurationStats_ReadsWaitTimeMsColumnThatExistsInBothGeneratedTables()
    {
        /* The verified duration column — wait_time_ms — must exist in BOTH the XE-report table and the
           DMV-snapshot fallback table (the read UNION-ALLs the two aggregates). */
        Assert.Equal("blocked_process_reports", BlockedProcessReportCollector.Instance.TargetTable);
        Assert.Equal("dmv_blocking_snapshots", DmvBlockingSnapshotCollector.Instance.TargetTable);

        var bprDdl = PgSchemaGenerator.CreateTable(BlockedProcessReportCollector.Instance);
        var dmvDdl = PgSchemaGenerator.CreateTable(DmvBlockingSnapshotCollector.Instance);
        foreach (var column in new[] { "event_time", "wait_time_ms" })
        {
            Assert.Contains(column, bprDdl, StringComparison.Ordinal);
            Assert.Contains(column, dmvDdl, StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the blocking-duration aggregate: proves the per-minute
/// bucketing, the total / max / avg wait_time_ms math, and the XE-preferred / DMV-fallback reconciliation
/// (DMV excluded while any XE row exists in the window; contributes once the XE rows are gone). Shares the
/// serialized "live-postgres" collection so the row churn can't race another class; uses a negative sentinel
/// server_id and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerBlockingStatsLivePostgresTests
{
    private const int StatsServerId = -972001;
    private const string ServerName = "viewer-blocking-stats-e2e";

    [Fact]
    public async Task BlockingDurationStats_BucketsPerMinute_TotalMaxAvg_XePreferred_DmvFallback_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live blocking-duration-stats test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "blocked_process_reports", StatsServerId, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "dmv_blocking_snapshots", StatsServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var baseTime = TruncateToMinute(DateTime.UtcNow.AddMinutes(-10));
            var minuteA = baseTime;
            var minuteB = baseTime.AddMinutes(3);

            /* minute A: two XE blocks (1000 ms + 3000 ms); minute B: one XE block (500 ms). Plus two DMV
               rows in minute A (9999 ms) that must be EXCLUDED while any XE row exists in the window. */
            await InsertBlockedProcessReportAsync(connection, StatsServerId, minuteA, minuteA.AddSeconds(5), 1000);
            await InsertBlockedProcessReportAsync(connection, StatsServerId, minuteA, minuteA.AddSeconds(40), 3000);
            await InsertBlockedProcessReportAsync(connection, StatsServerId, minuteB, minuteB.AddSeconds(10), 500);
            await InsertDmvBlockingSnapshotAsync(connection, StatsServerId, minuteA, minuteA.AddSeconds(20), 9999);
            await InsertDmvBlockingSnapshotAsync(connection, StatsServerId, minuteA, minuteA.AddSeconds(50), 9999);

            var withXe = await viewer.GetBlockingDurationStatsAsync(StatsServerId, baseTime.AddMinutes(-1), baseTime.AddMinutes(10));

            /* Only the XE buckets appear (DMV excluded by WHERE NOT EXISTS): A then B, ordered by bucket. */
            Assert.Equal(2, withXe.Count);
            Assert.True(withXe[0].Time < withXe[1].Time);

            /* minute A: 2 events, total 4000, max 3000, avg 2000. */
            Assert.Equal(2, withXe[0].EventCount);
            Assert.Equal(4000, withXe[0].TotalDurationMs);
            Assert.Equal(3000, withXe[0].MaxDurationMs);
            Assert.Equal(2000.0, withXe[0].AvgDurationMs, precision: 3);

            /* minute B: 1 event, total 500, max 500, avg 500. */
            Assert.Equal(1, withXe[1].EventCount);
            Assert.Equal(500, withXe[1].TotalDurationMs);
            Assert.Equal(500, withXe[1].MaxDurationMs);
            Assert.Equal(500.0, withXe[1].AvgDurationMs, precision: 3);

            /* Remove the XE rows: now the DMV fallback contributes its two minute-A rows (9999 ms each). */
            await DeleteRowsAsync(connection, "blocked_process_reports", StatsServerId, TestContext.Current.CancellationToken);
            var dmvOnly = await viewer.GetBlockingDurationStatsAsync(StatsServerId, baseTime.AddMinutes(-1), baseTime.AddMinutes(10));

            Assert.Single(dmvOnly);
            Assert.Equal(2, dmvOnly[0].EventCount);
            Assert.Equal(19998, dmvOnly[0].TotalDurationMs);
            Assert.Equal(9999, dmvOnly[0].MaxDurationMs);
            Assert.Equal(9999.0, dmvOnly[0].AvgDurationMs, precision: 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, "blocked_process_reports", StatsServerId, cleanupCt);
                await DeleteRowsAsync(cleanup, "dmv_blocking_snapshots", StatsServerId, cleanupCt);
            });
        }
    }

    /// <summary>
    /// #6705: a parallel query waiting on itself (the same non-zero spid on both sides) is not blocking, on the desktop trend,
    /// the duration stats, the existence check that picks the arm, and the MCP trend and stats reads. A minute with one self-wait
    /// report and one real report counts 1; a window whose only XE reports are self-waits draws the DMV arm, as the detector does.
    /// The Blocking-tab slicer reads the same pair by the hour and agrees (Lite's <c>BlockingSeries_LeaveSelfWaitsOut_OnTheXeArm</c>).
    /// A (0, 0) report is a system spid pair the predicate keeps, so it counts.
    /// </summary>
    [Fact]
    public async Task SelfWaitReports_AreNotBlocking_OnEveryTrendRead_AndOnlySelfWaitsDrawTheDmvArm_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live self-wait trend test.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, "blocked_process_reports", StatsServerId, ct);
        await DeleteRowsAsync(connection, "dmv_blocking_snapshots", StatsServerId, ct);

        await using var viewer = new ViewerDataService(connectionString!);
        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            var start = TruncateToMinute(DateTime.UtcNow.AddMinutes(-10));
            var end = start.AddMinutes(11);

            /* One real report (1000 ms), one self-wait (7000 ms, spid 7 on itself) and one (0, 0) report (3000 ms) in the same minute. */
            await InsertBlockedProcessReportAsync(connection, StatsServerId, start, start.AddSeconds(5), 1000);
            await InsertBlockedProcessReportAsync(connection, StatsServerId, start, start.AddSeconds(25), 3000, blockedSpid: 0, blockingSpid: 0);
            await InsertBlockedProcessReportAsync(connection, StatsServerId, start, start.AddSeconds(30), 7000, blockedSpid: 7, blockingSpid: 7);
            await InsertDmvBlockingSnapshotAsync(connection, StatsServerId, start, start.AddSeconds(20), 9999);
            /* A DMV self-wait (spid 166 on itself, 5555 ms) in the same minute: every DMV-arm read below must leave it out. */
            await InsertDmvBlockingSnapshotAsync(connection, StatsServerId, start, start.AddSeconds(25), 5555, blockedSpid: 166, blockingSpid: 166, collectionId: 2);

            var trend = await viewer.GetBlockingTrendAsync(StatsServerId, start.AddMinutes(-1), end);
            Assert.Equal(2, trend.Sum(p => p.Count));
            var mcpTrend = await PerformanceMonitor.Darling.Service.Mcp.DarlingBlockingTrendReader.GetBlockingTrendAsync(postgres, StatsServerId, start.AddMinutes(-1), end, ct);
            Assert.Equal(2, mcpTrend.Sum(p => p.Count));

            var stats = await viewer.GetBlockingDurationStatsAsync(StatsServerId, start.AddMinutes(-1), end);
            Assert.Equal(2, stats.Sum(p => p.EventCount));
            Assert.Equal(4000, stats.Sum(p => p.TotalDurationMs));
            Assert.Equal(3000, stats.Max(p => p.MaxDurationMs));
            var mcpStats = await PerformanceMonitor.Darling.Service.Mcp.DarlingDataReader.GetBlockingDurationStatsAsync(postgres, StatsServerId, start.AddMinutes(-1), end, ct);
            Assert.Equal(2, mcpStats.Sum(p => p.EventCount));
            Assert.Equal(4000, mcpStats.Sum(p => p.TotalDurationMs));
            Assert.True(await viewer.HasBlockedProcessReportsInWindowAsync(StatsServerId, start.AddMinutes(-1), end));

            /* The slicer: the real and (0, 0) reports count, the 7000 ms self-wait does not (4.0 s, not 11.0 s), and it
               names two blockers (60 and 0) and two blocked spids (55 and 0), not the self-wait's spid 7. */
            var slicer = Assert.Single(await viewer.GetBlockingSlicerDataAsync(StatsServerId, start.AddMinutes(-1), end));
            Assert.Equal(2, slicer.SessionCount);
            Assert.Equal(4.0, slicer.TotalCpu);
            Assert.Equal(2, slicer.TotalElapsed);
            Assert.Equal(2, slicer.TotalReads);

            /* Only the self-wait remains in XE: the window draws the DMV snapshot, and the existence check says so. */
            await DeleteRowsAsync(connection, "blocked_process_reports", StatsServerId, ct);
            await InsertBlockedProcessReportAsync(connection, StatsServerId, start, start.AddSeconds(30), 7000, blockedSpid: 7, blockingSpid: 7);

            var dmvTrend = await viewer.GetBlockingTrendAsync(StatsServerId, start.AddMinutes(-1), end);
            Assert.Equal(1, dmvTrend.Sum(p => p.Count));
            Assert.All(dmvTrend, p => Assert.Equal("DMV snapshot", p.Source));
            var dmvMcp = await PerformanceMonitor.Darling.Service.Mcp.DarlingBlockingTrendReader.GetBlockingTrendAsync(postgres, StatsServerId, start.AddMinutes(-1), end, ct);
            Assert.All(dmvMcp, p => Assert.Equal("DMV snapshot", p.Source));
            Assert.Equal(1, dmvMcp.Sum(p => p.Count));
            var dmvStats = await viewer.GetBlockingDurationStatsAsync(StatsServerId, start.AddMinutes(-1), end);
            Assert.Equal(9999, dmvStats.Sum(p => p.TotalDurationMs));
            Assert.Equal(1, dmvStats.Sum(p => p.EventCount));
            var dmvMcpStats = await PerformanceMonitor.Darling.Service.Mcp.DarlingDataReader.GetBlockingDurationStatsAsync(postgres, StatsServerId, start.AddMinutes(-1), end, ct);
            Assert.Equal(1, dmvMcpStats.Sum(p => p.EventCount));
            Assert.Equal(9999, dmvMcpStats.Sum(p => p.TotalDurationMs));
            Assert.False(await viewer.HasBlockedProcessReportsInWindowAsync(StatsServerId, start.AddMinutes(-1), end));

            /* The slicer draws the DMV arm too: its one real snapshot (9999 ms), none of the XE self-wait's 7000 ms or the DMV self-wait's 5555 ms. */
            var dmvSlicer = Assert.Single(await viewer.GetBlockingSlicerDataAsync(StatsServerId, start.AddMinutes(-1), end));
            Assert.Equal(1, dmvSlicer.SessionCount);
            Assert.Equal(9.999, dmvSlicer.TotalCpu, 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, "blocked_process_reports", StatsServerId, cleanupCt);
                await DeleteRowsAsync(cleanup, "dmv_blocking_snapshots", StatsServerId, cleanupCt);
            });
        }
    }

    private static async Task InsertBlockedProcessReportAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, DateTime eventTimeUtc, long waitTimeMs, int blockedSpid = 55, int blockingSpid = 60)
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
        command.Parameters.AddWithValue(blockedSpid);
        command.Parameters.AddWithValue(blockingSpid);
        command.Parameters.AddWithValue(waitTimeMs);
        command.Parameters.AddWithValue("X");
        command.Parameters.AddWithValue("SELECT 1");
        command.Parameters.AddWithValue("UPDATE t SET c = 1");
        command.Parameters.AddWithValue("<blocked/>");
        command.Parameters.AddWithValue("dbo.t");
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertDmvBlockingSnapshotAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, DateTime eventTimeUtc, long waitTimeMs,
        int blockedSpid = 155, int blockingSpid = 160, long collectionId = 1)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO dmv_blocking_snapshots
    (collection_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, contentious_object)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue("AppDb");
        command.Parameters.AddWithValue(blockedSpid);
        command.Parameters.AddWithValue(blockingSpid);
        command.Parameters.AddWithValue(waitTimeMs);
        command.Parameters.AddWithValue("S");
        command.Parameters.AddWithValue("SELECT 2");
        command.Parameters.AddWithValue("UPDATE t SET c = 2");
        command.Parameters.AddWithValue("dbo.t");
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /* The duration aggregate buckets by DATE_TRUNC('minute', event_time), so the test anchors on a minute
       boundary — otherwise added seconds could spill into the next minute and split a bucket. */
    private static DateTime TruncateToMinute(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, string table, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
