/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #7895 and #8001, the Lite twin of the Darling live tests in <c>AnalysisCoverageLivePostgresTests</c>: the
/// BLOCKING_EVENTS peak divides each 4 h bucket's waits by the hours the collector observed inside that bucket, or,
/// when the witness never saw the bucket, by its span clipped to the window end and capped at the window's observed
/// hours; never under one hour; and the peak is the bucket with the highest rate, not the biggest count. The wait
/// samples planted here are the coverage witness, a reading every fifteen minutes.
/// </summary>
public sealed class BlockingPeakBucketHoursTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 78950;
    private const string ServerName = "SynthPeakBucketSrv";

    private static readonly DateTime WindowEnd = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerMinute * TimeSpan.TicksPerMinute),
        DateTimeKind.Unspecified);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public BlockingPeakBucketHoursTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task ASixHourWindow_StormInTheLastTwoHours_DividesByItsOwnTwoHours()
    {
        var start = WindowEnd.AddHours(-6);
        await PlantCoverageAsync(start, WindowEnd);
        /* Thirty waits inside the [4h, 6h) tail bucket, which spans two hours, not four. */
        for (var i = 0; i < 30; i++)
            await PlantBlockingAsync(start.AddHours(4).AddMinutes(3 * i), 100 + i);

        var blocking = await CollectBlockingAsync(start);

        Assert.Equal(15.0, blocking.Value, precision: 6);
        Assert.Equal(15.0, blocking.Metadata["events_per_hour_peak_4h"], precision: 6);
        Assert.Equal(30.0, blocking.Metadata["peak_4h_event_count"], precision: 6);
        Assert.True(blocking.Metadata.TryGetValue("peak_4h_covered_hours", out var covered), "the fact carries the peak bucket's covered hours");
        Assert.Equal(2.0, covered, precision: 6);
    }

    [Fact]
    public async Task ThePeak_IsTheBusiestRate_NotTheBiggestCount()
    {
        var start = WindowEnd.AddHours(-6);
        await PlantCoverageAsync(start, WindowEnd);
        /* Forty waits across the full first bucket (10/h) and thirty across the two-hour tail (15/h). */
        for (var i = 0; i < 40; i++)
            await PlantBlockingAsync(start.AddMinutes(6 * i), 100 + i);
        for (var i = 0; i < 30; i++)
            await PlantBlockingAsync(start.AddHours(4).AddMinutes(3 * i), 200 + i);

        var blocking = await CollectBlockingAsync(start);

        Assert.Equal(70.0, blocking.Metadata["event_count"], precision: 6);
        Assert.Equal(15.0, blocking.Value, precision: 6);
        Assert.Equal(30.0, blocking.Metadata["peak_4h_event_count"], precision: 6);
    }

    [Fact]
    public async Task ATailBucketUnderAnHour_IsJudgedAsOneHour()
    {
        var start = WindowEnd.AddMinutes(-270);
        await PlantCoverageAsync(start, WindowEnd);
        /* Three waits in a thirty-minute tail bucket: 3/h, not the 6/h a thirty-minute divisor would make of them. */
        for (var i = 0; i < 3; i++)
            await PlantBlockingAsync(start.AddHours(4).AddMinutes(5 * i), 100 + i);

        var blocking = await CollectBlockingAsync(start);

        Assert.Equal(3.0, blocking.Value, precision: 6);
        Assert.Equal(3.0, blocking.Metadata["peak_4h_event_count"], precision: 6);
        Assert.True(blocking.Metadata.TryGetValue("peak_4h_covered_hours", out var covered), "the fact carries the peak bucket's covered hours");
        Assert.Equal(1.0, covered, precision: 6);
    }

    [Fact]
    public async Task AHalfObservedEightHourWindow_StillDividesAFullBucketByFour()
    {
        var start = WindowEnd.AddHours(-8);
        /* The collector was up for the first half of the window only; twenty waits land in the second full bucket. The
           window's four observed hours cover the bucket, so it divides by its own four hours: 5/h, not 20/2. */
        await PlantCoverageAsync(start, start.AddHours(4));
        for (var i = 0; i < 20; i++)
            await PlantBlockingAsync(start.AddHours(4).AddMinutes(10 * i), 100 + i);

        var blocking = await CollectBlockingAsync(start);

        Assert.Equal(0.5, blocking.Metadata["observed_hours"] / blocking.Metadata["period_hours"], precision: 6);
        Assert.Equal(5.0, blocking.Value, precision: 6);
        Assert.Equal(20.0, blocking.Metadata["peak_4h_event_count"], precision: 6);
        Assert.True(blocking.Metadata.TryGetValue("peak_4h_covered_hours", out var covered), "the fact carries the peak bucket's covered hours");
        Assert.Equal(4.0, covered, precision: 6);
    }

    [Fact]
    public async Task AQuarterObservedDay_ReadsAFullBucketAtItsOwnFourHours()
    {
        var start = WindowEnd.AddHours(-24);
        /* The collector was up for the first six hours of the day only, and forty waits land in the fully observed first
           bucket: 10/h, the rate a fully observed day reads for the same bucket. One coverage fraction for the whole
           window must not shrink the divisor of a bucket the collector saw end to end. */
        await PlantCoverageAsync(start, start.AddHours(6));
        for (var i = 0; i < 40; i++)
            await PlantBlockingAsync(start.AddMinutes(6 * i), 100 + i);

        var blocking = await CollectBlockingAsync(start);

        Assert.Equal(0.25, blocking.Metadata["observed_hours"] / blocking.Metadata["period_hours"], precision: 6);
        Assert.Equal(10.0, blocking.Value, precision: 6);
        Assert.Equal(10.0, blocking.Metadata["events_per_hour_peak_4h"], precision: 6);
        Assert.Equal(40.0, blocking.Metadata["peak_4h_event_count"], precision: 6);
        Assert.True(blocking.Metadata.TryGetValue("peak_4h_covered_hours", out var covered), "the fact carries the peak bucket's covered hours");
        Assert.Equal(4.0, covered, precision: 6);
    }

    [Fact]
    public async Task ATwoHourCollectorGapInsideThePeakBucket_DividesByItsTwoObservedHours()
    {
        var start = WindowEnd.AddHours(-24);
        /* #8001: the collector was up for the first four hours and again from hour six to hour eight, six observed hours
           of twenty-four. Forty waits across the fully observed first bucket are 10/h. Thirty waits in the two hours the
           collector saw of the [4h, 8h) bucket are 15/h, the peak, and that bucket divides by its own two observed
           hours, not by four and not by the window's six. */
        await PlantCoverageAsync(start, start.AddHours(4));
        await PlantCoverageAsync(start.AddHours(6), start.AddHours(8));
        for (var i = 0; i < 40; i++)
            await PlantBlockingAsync(start.AddMinutes(6 * i), 100 + i);
        for (var i = 0; i < 30; i++)
            await PlantBlockingAsync(start.AddHours(6).AddMinutes(4 * i), 200 + i);

        var blocking = await CollectBlockingAsync(start);

        Assert.Equal(0.25, blocking.Metadata["observed_hours"] / blocking.Metadata["period_hours"], precision: 6);
        Assert.Equal(70.0, blocking.Metadata["event_count"], precision: 6);
        Assert.Equal(15.0, blocking.Value, precision: 6);
        Assert.Equal(15.0, blocking.Metadata["events_per_hour_peak_4h"], precision: 6);
        Assert.Equal(30.0, blocking.Metadata["peak_4h_event_count"], precision: 6);
        Assert.True(blocking.Metadata.TryGetValue("peak_4h_covered_hours", out var covered), "the fact carries the peak bucket's covered hours");
        Assert.Equal(2.0, covered, precision: 6);
    }

    [Fact]
    public async Task ABucketTheWitnessNeverSaw_DividesByItsOwnSpan_NotTheOneHourFloor()
    {
        var start = WindowEnd.AddHours(-24);
        /* #8001: the collector was up for the first eight hours of the day only. Eight waits across the first bucket are
           2/h. Twenty waits sit in the [8h, 12h) bucket, which the coverage witness never saw. Those waits prove their
           own collector was up there, so the witness does not describe that bucket: it divides by its own four-hour
           span, 5/h, not by the one-hour floor's 20/h. */
        await PlantCoverageAsync(start, start.AddHours(8));
        for (var i = 0; i < 8; i++)
            await PlantBlockingAsync(start.AddMinutes(30 * i), 100 + i);
        for (var i = 0; i < 20; i++)
            await PlantBlockingAsync(start.AddHours(8).AddMinutes(10 * i), 200 + i);

        var blocking = await CollectBlockingAsync(start);

        Assert.Equal(8.0, blocking.Metadata["observed_hours"], precision: 6);
        Assert.Equal(28.0, blocking.Metadata["event_count"], precision: 6);
        Assert.Equal(5.0, blocking.Value, precision: 6);
        Assert.Equal(5.0, blocking.Metadata["events_per_hour_peak_4h"], precision: 6);
        Assert.Equal(20.0, blocking.Metadata["peak_4h_event_count"], precision: 6);
        Assert.True(blocking.Metadata.TryGetValue("peak_4h_covered_hours", out var covered), "the fact carries the peak bucket's covered hours");
        Assert.Equal(4.0, covered, precision: 6);
    }

    private async Task<Fact> CollectBlockingAsync(DateTime start)
    {
        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = start,
            TimeRangeEnd = WindowEnd,
        });
        return Assert.Single(facts, f => f.Key == "BLOCKING_EVENTS");
    }

    /// <summary>A wait-stats reading every fifteen minutes from <paramref name="from"/> to <paramref name="to"/> inclusive, the
    /// first with no delta (a baseline), so the coverage witness sees the collector up for exactly that span.</summary>
    private async Task PlantCoverageAsync(DateTime from, DateTime to)
    {
        for (var t = from; t <= to; t = t.AddMinutes(15))
            await PlantWaitAsync(t, t == from ? 0L : 1_000L);
    }

    private async Task PlantWaitAsync(DateTime at, long deltaWaitMs)
    {
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, 'CXPACKET', 10, 10000, 0, $5, $6, 0)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaWaitMs > 0 ? 10L : 0L });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaWaitMs });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One blocked-process report, which is one wait: a distinct blocked session each time.</summary>
    private async Task PlantBlockingAsync(DateTime at, int blockedSpid)
    {
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name,
     event_time, blocked_spid, blocked_last_tran_started,
     blocking_spid, blocking_last_tran_started, wait_time_ms,
     lock_mode, blocked_status, blocking_status, monitor_loop)
VALUES ($1, $2, $3, $4, $2, $5, $2, 60, $6, 12000, 'X', 'suspended', 'suspended', 1)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = blockedSpid });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowEnd.AddDays(-1) });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            using var readLock = _duckDb.AcquireReadLock();
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }
}
