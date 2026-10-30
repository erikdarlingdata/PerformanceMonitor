/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Real-DuckDB round-trip pins for the Blocking Stats sub-tab readers
/// (<see cref="LocalDataService.GetBlockingDurationStatsAsync"/> +
/// <see cref="LocalDataService.GetDeadlockSeverityStatsAsync"/>) — the Lite port of Darling's Blocking Stats
/// severity aggregates, themselves the on-the-fly equivalent of the Dashboard's pre-aggregated
/// <c>collect.blocking_deadlock_stats</c>. These plant blocked_process_reports / dmv_blocking_snapshots /
/// deadlocks rows into the store, read back through the reader, and assert the per-minute duration aggregate
/// (event count + total/max/avg block duration), the XE-preferred / DMV-fallback union (so the severity
/// reconciles with the blocking-incident count trend), the #1319 database filter, window bounding, and the
/// deadlock-severity aggregate parsed on-the-fly from the graph XML (victim_count + total/max/avg wait,
/// reconciling in period with the deadlock count over the same v_deadlocks window). Mirrors the Darling
/// Blocking Stats scenarios (<c>ViewerBlockingStatsTests</c> / <c>ViewerDeadlockSeverityStatsTests</c>).
/// </summary>
public sealed class BlockingStatsReaderTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 9922;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public BlockingStatsReaderTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>
    /// One connection reused for every seeded row — opening a fresh connection per
    /// single-row INSERT measured ~90ms/row and dominated this class's runtime.
    /// </summary>
    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    /* Anchor the window in the recent past so the default 24h read includes it; truncate to the minute so the
       DATE_TRUNC('minute', …) bucket key is exactly predictable. Naive (Unspecified) kind matches what DuckDB
       returns for a stored TIMESTAMP, so bucket-time equality holds. */
    private static readonly DateTime MinuteA = MinuteFloor(DateTime.UtcNow.AddMinutes(-90));
    private static readonly DateTime MinuteB = MinuteA.AddMinutes(3);

    private static DateTime MinuteFloor(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    // ── Blocking duration aggregate: per-minute total / max / avg / event count ──

    [Fact]
    public async Task BlockingDurationStats_BucketsPerMinute_TotalMaxAvgEventCount()
    {
        var service = new LocalDataService(_duckDb);

        /* Minute A: two blocks (1000 + 3000 ms) → total 4000, max 3000, avg 2000, count 2.
           Minute B: one block (2000 ms)        → total 2000, max 2000, avg 2000, count 1. */
        await SeedBlockedProcessReportAsync(MinuteA.AddSeconds(10), "DB1", 1000);
        await SeedBlockedProcessReportAsync(MinuteA.AddSeconds(40), "DB1", 3000);
        await SeedBlockedProcessReportAsync(MinuteB.AddSeconds(20), "DB1", 2000);

        var points = await service.GetBlockingDurationStatsAsync(ServerId);
        Assert.Equal(2, points.Count);

        var a = points[0];
        Assert.Equal(MinuteA, a.Time);
        Assert.Equal(2, a.EventCount);
        Assert.Equal(4000, a.TotalDurationMs);
        Assert.Equal(3000, a.MaxDurationMs);
        Assert.Equal(2000.0, a.AvgDurationMs, 3);

        var b = points[1];
        Assert.Equal(MinuteB, b.Time);
        Assert.Equal(1, b.EventCount);
        Assert.Equal(2000, b.TotalDurationMs);
        Assert.Equal(2000, b.MaxDurationMs);
        Assert.Equal(2000.0, b.AvgDurationMs, 3);
    }

    [Fact]
    public async Task BlockingDurationStats_ExcludesRowsOutsideTheWindow()
    {
        var service = new LocalDataService(_duckDb);

        /* Only event is 48h ago — outside a 24h window (the read windows on event_time, like the count trend). */
        await SeedBlockedProcessReportAsync(MinuteFloor(DateTime.UtcNow.AddHours(-48)).AddSeconds(5), "DB1", 5000);

        Assert.Empty(await service.GetBlockingDurationStatsAsync(ServerId, hoursBack: 24));
    }

    [Fact]
    public async Task BlockingDurationStats_HonorsDatabaseFilter_1319()
    {
        var service = new LocalDataService(_duckDb);

        await SeedBlockedProcessReportAsync(MinuteA.AddSeconds(10), "DbA", 1000);
        await SeedBlockedProcessReportAsync(MinuteA.AddSeconds(20), "DbB", 8000);

        /* Unfiltered: both in one bucket → total 9000. */
        var all = Assert.Single(await service.GetBlockingDurationStatsAsync(ServerId));
        Assert.Equal(9000, all.TotalDurationMs);
        Assert.Equal(2, all.EventCount);

        /* Filtered to DbA: only the 1000 ms block. */
        var filtered = Assert.Single(await service.GetBlockingDurationStatsAsync(ServerId, databaseNames: new[] { "DbA" }));
        Assert.Equal(1000, filtered.TotalDurationMs);
        Assert.Equal(1, filtered.EventCount);
    }

    [Fact]
    public async Task BlockingDurationStats_FallsBackToDmvSnapshot_OnlyWhenNoXeInWindow()
    {
        var service = new LocalDataService(_duckDb);

        /* No blocked_process_reports at all → the DMV snapshot arm contributes (WHERE NOT EXISTS). */
        await SeedDmvBlockingSnapshotAsync(MinuteA.AddSeconds(15), "DB1", 6000);
        var dmvOnly = Assert.Single(await service.GetBlockingDurationStatsAsync(ServerId));
        Assert.Equal(MinuteA, dmvOnly.Time);
        Assert.Equal(6000, dmvOnly.TotalDurationMs);

        /* Now add an XE report: the XE arm is preferred and the DMV arm is fully excluded (no double-count). */
        await SeedBlockedProcessReportAsync(MinuteB.AddSeconds(15), "DB1", 1000);
        var withXe = await service.GetBlockingDurationStatsAsync(ServerId);
        var bucket = Assert.Single(withXe);
        Assert.Equal(MinuteB, bucket.Time);       // the XE minute only — the DMV minute is dropped
        Assert.Equal(1000, bucket.TotalDurationMs);
    }

    // ── #6645 / #6705: a self-wait (same non-zero blocked and blocking spid) is not blocking, on both arms ──

    [Fact]
    public async Task BlockingSeries_LeaveSelfWaitsOut_OnTheXeArm()
    {
        var service = new LocalDataService(_duckDb);

        /* One minute: a parallel query waiting on itself (spid 77 blocking 77), a real block and a (0, 0) report. A (0, 0) report
           is a system spid pair the predicate keeps; the self-wait goes, with its 9000 ms. */
        await SeedBlockedProcessReportAsync(MinuteA.AddSeconds(5), "DB1", 9000, blockedSpid: 77, blockingSpid: 77);
        await SeedBlockedProcessReportAsync(MinuteA.AddSeconds(15), "DB1", 1000);
        await SeedBlockedProcessReportAsync(MinuteA.AddSeconds(25), "DB1", 3000, blockedSpid: 0, blockingSpid: 0);

        var trend = Assert.Single(await service.GetBlockingTrendAsync(ServerId));
        Assert.Equal(2, trend.Count);

        var duration = Assert.Single(await service.GetBlockingDurationStatsAsync(ServerId));
        Assert.Equal(2, duration.EventCount);
        Assert.Equal(4000, duration.TotalDurationMs);
        Assert.Equal(3000, duration.MaxDurationMs);

        Assert.Equal(2, (await service.GetBlockingSlicerDataAsync(ServerId, 24)).Sum(b => b.SessionCount));
        Assert.True(await service.HasBlockedProcessReportsInWindowAsync(ServerId, MinuteA.AddMinutes(-1), MinuteA.AddMinutes(1)));
    }

    [Fact]
    public async Task BlockingSeries_OnlySelfWaitReportsInXe_DrawTheDmvArm_AndDmvSelfWaitsGo()
    {
        var service = new LocalDataService(_duckDb);

        /* The XE arm holds only a self-wait, so once the predicate filters it the arm is empty and the DMV arm is drawn. The DMV
           arm leaves its own self-wait out. */
        await SeedBlockedProcessReportAsync(MinuteA.AddSeconds(5), "DB1", 9000, blockedSpid: 77, blockingSpid: 77);
        await SeedDmvBlockingSnapshotAsync(MinuteB.AddSeconds(5), "DB1", 2000);
        await SeedDmvBlockingSnapshotAsync(MinuteB.AddSeconds(10), "DB1", 8000, blockedSpid: 88, blockingSpid: 88);

        var trend = Assert.Single(await service.GetBlockingTrendAsync(ServerId));
        Assert.Equal(MinuteB, trend.Time);
        Assert.Equal("DMV snapshot", trend.Source);
        Assert.Equal(1, trend.Count);

        var duration = Assert.Single(await service.GetBlockingDurationStatsAsync(ServerId));
        Assert.Equal(MinuteB, duration.Time);
        Assert.Equal("DMV snapshot", duration.Source);
        Assert.Equal(2000, duration.TotalDurationMs);

        var slicer = await service.GetBlockingSlicerDataAsync(ServerId, 24);
        Assert.Equal(1, slicer.Sum(b => b.SessionCount));
        Assert.False(await service.HasBlockedProcessReportsInWindowAsync(ServerId, MinuteA.AddMinutes(-1), MinuteB.AddMinutes(1)));
    }

    // ── Deadlock severity aggregate: victim_count + total / max / avg wait, reconciling with the count ──

    [Fact]
    public async Task DeadlockSeverity_BucketsPerMinute_VictimTotalMaxAvg()
    {
        var service = new LocalDataService(_duckDb);

        /* Minute A: two deadlocks — (p1 victim 1000, p2 3000) then (p3 victim 2000) → 2 victims, total 6000,
           max 3000, 3 processes → avg 2000. Minute B: one deadlock (p4 victim 500, p5 700) → 1 victim,
           total 1200, max 700, 2 processes → avg 600. */
        await SeedDeadlockAsync(MinuteA.AddSeconds(10), BuildGraph(("p1", 55, 1000, true), ("p2", 66, 3000, false)));
        await SeedDeadlockAsync(MinuteA.AddSeconds(40), BuildGraph(("p3", 77, 2000, true)));
        await SeedDeadlockAsync(MinuteB.AddSeconds(20), BuildGraph(("p4", 88, 500, true), ("p5", 99, 700, false)));

        var points = await service.GetDeadlockSeverityStatsAsync(ServerId);
        Assert.Equal(2, points.Count);

        var a = points[0];
        Assert.Equal(MinuteA, a.Time);
        Assert.Equal(2, a.VictimCount);
        Assert.Equal(6000, a.TotalWaitMs);
        Assert.Equal(3000, a.MaxWaitMs);
        Assert.Equal(2000.0, a.AvgWaitMs, 3);

        var b = points[1];
        Assert.Equal(MinuteB, b.Time);
        Assert.Equal(1, b.VictimCount);
        Assert.Equal(1200, b.TotalWaitMs);
        Assert.Equal(700, b.MaxWaitMs);
        Assert.Equal(600.0, b.AvgWaitMs, 3);
    }

    [Fact]
    public async Task DeadlockSeverity_ReconcilesWithDeadlockCountTrend_SameWindow()
    {
        var service = new LocalDataService(_duckDb);

        await SeedDeadlockAsync(MinuteA.AddSeconds(10), BuildGraph(("a1", 55, 1000, true), ("a2", 66, 3000, false)));
        await SeedDeadlockAsync(MinuteA.AddSeconds(40), BuildGraph(("b1", 77, 2000, true)));
        await SeedDeadlockAsync(MinuteB.AddSeconds(20), BuildGraph(("c1", 88, 500, true)));

        /* The severity aggregate (windowed on collection_time, bucketed on deadlock_time) draws from the SAME
           deadlock rows as the count trend, so the total victim count (>= one per real deadlock) reconciles
           with the deadlock count over the identical window. */
        var severity = await service.GetDeadlockSeverityStatsAsync(ServerId);
        var counts = await service.GetDeadlockTrendAsync(ServerId);

        Assert.Equal(3, counts.Sum(c => c.Count));               // 3 deadlocks
        Assert.Equal(3, severity.Sum(s => s.VictimCount));       // one victim per graph here
        Assert.True(severity.Sum(s => s.VictimCount) >= 1);
    }

    [Fact]
    public async Task DeadlockSeverity_ExcludesRowsOutsideTheWindow()
    {
        var service = new LocalDataService(_duckDb);

        /* Collected 48h ago — outside a 24h window (the read windows on collection_time). */
        await SeedDeadlockAsync(MinuteFloor(DateTime.UtcNow.AddHours(-48)).AddSeconds(5),
            BuildGraph(("z1", 55, 1000, true)), collectionTimeOverride: MinuteFloor(DateTime.UtcNow.AddHours(-48)));

        Assert.Empty(await service.GetDeadlockSeverityStatsAsync(ServerId, hoursBack: 24));
    }

    // ── Seeding helpers (raw INSERT via the shared initializer's connection) ──

    private async Task SeedBlockedProcessReportAsync(DateTime eventTime, string databaseName, long waitTimeMs, int blockedSpid = 55, int blockingSpid = 66)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocked_last_tran_started, blocking_spid, blocking_last_tran_started,
     wait_time_ms, lock_mode, blocked_status, blocking_status)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = databaseName });
        cmd.Parameters.Add(new DuckDBParameter { Value = blockedSpid });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = blockingSpid });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitTimeMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = "X" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "suspended" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "running" });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedDmvBlockingSnapshotAsync(DateTime eventTime, string databaseName, long waitTimeMs, int blockedSpid = 55, int blockingSpid = 66)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO dmv_blocking_snapshots
    (collection_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, monitor_loop)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = databaseName });
        cmd.Parameters.Add(new DuckDBParameter { Value = blockedSpid });
        cmd.Parameters.Add(new DuckDBParameter { Value = blockingSpid });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitTimeMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = "X" });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedDeadlockAsync(DateTime deadlockTime, string graphXml, DateTime? collectionTimeOverride = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml)
VALUES ($1, $2, $3, $4, $5, $6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeOverride ?? deadlockTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = deadlockTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = graphXml });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Builds a bare &lt;deadlock&gt; graph (what the store keeps) from known processes, so a test can
    /// assert exact victim_count / total / max / avg without depending on a captured fixture's wait times.</summary>
    private static string BuildGraph(params (string Id, int Spid, long WaitTime, bool IsVictim)[] processes)
    {
        var sb = new StringBuilder();
        sb.Append("<deadlock><victim-list>");
        foreach (var p in processes.Where(p => p.IsVictim))
            sb.Append($"<victimProcess id=\"{p.Id}\"/>");
        sb.Append("</victim-list><process-list>");
        foreach (var p in processes)
            sb.Append($"<process id=\"{p.Id}\" spid=\"{p.Spid}\" waittime=\"{p.WaitTime}\"><inputbuf>x</inputbuf></process>");
        sb.Append("</process-list></deadlock>");
        return sb.ToString();
    }
}
