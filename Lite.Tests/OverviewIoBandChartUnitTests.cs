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
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7644, Lite side: the Overview's I/O band is in the unit of the line it sits behind (reads-weighted per collection:
/// the files' stall summed over their reads summed), not the per-file unit the anomaly detector reads, and the band
/// steps per target-local hour instead of drawing one flat range.
/// </summary>
public sealed class OverviewIoBandChartUnitTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private readonly BaselineProvider _provider;
    private DuckDBConnection? _seedConn;

    // Wednesday 2026-01-07, 10:00 UTC (no server_properties row, so keying is UTC).
    private static readonly DateTime WindowStart = new(2026, 1, 7, 10, 0, 0, DateTimeKind.Unspecified);

    public OverviewIoBandChartUnitTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _provider = new BaselineProvider(_duckDb);
        BaselineProvider.CacheTtl = TimeSpan.FromMilliseconds(1);
    }

    public void Dispose() => _seedConn?.Dispose();

    private async Task ExecuteSeedAsync(string sql, params object[] parameters)
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters)
            cmd.Parameters.Add(new DuckDBParameter { Value = p });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Three weeks of history at 10:00 UTC, thirty collections an hour, two files each. The busy file reads 10,000 times
    /// at about 1 ms; the quiet one reads 10 times at about 100 ms. The per-file ratios are 1 and 100 (mean 50 or so),
    /// the collection's reads-weighted ratio is about 1.1 ms.
    /// </summary>
    private async Task SeedTwoFileHistoryAsync(int serverId)
    {
        const string sql = @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
     delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
SELECT -8700000 - (d * 1000 + i * 10 + f), $1 - to_days(d) + to_minutes(2 * i + 1), $2, 'IoServer', 'AppDb',
       CASE WHEN f = 0 THEN 'AppDb_busy' ELSE 'AppDb_quiet' END, 'ROWS', 'D:\AppDb.mdf', 100,
       CASE WHEN f = 0 THEN 10000 ELSE 10 END, 0, 81920, 0,
       CASE WHEN f = 0 THEN 9000 + (i % 5) * 500 ELSE 900 + (i % 5) * 50 END, 0, 60
FROM range(1, 22) AS dd(d)
CROSS JOIN range(0, 30) AS ii(i)
CROSS JOIN range(0, 2) AS ff(f)";
        await ExecuteSeedAsync(sql, WindowStart, serverId);
    }

    [Fact]
    public async Task TheChartUnitArm_MeansThePerCollectionLine_AndThePerFileArmDoesNot_AndTheFloorApplies()
    {
        const int serverId = -8701;
        await SeedTwoFileHistoryAsync(serverId);

        var perFile = await _provider.GetBaselineAsync(serverId, MetricNames.IoLatency, WindowStart);
        _provider.ClearCache();
        var chart = await _provider.GetBaselineAsync(serverId, MetricNames.IoLatencyChart, WindowStart);

        // The per-file arm judges each file row, so the quiet file's 100 ms rows pull the mean to about 50 ms.
        Assert.InRange(perFile.Mean, 30.0, 70.0);

        // The chart-unit arm is the reads-weighted line the lane draws: about 1.1 ms, one sample per collection.
        Assert.InRange(chart.Mean, 0.9, 1.5);
        Assert.Equal(90, chart.SampleCount); // three Wednesdays of 30 collections: one sample per collection, never one per file (180)
        // The spread is a fraction of a millisecond, so the 2.5 ms floor decides the band's width.
        Assert.True(chart.EffectiveStdDev >= 2.5, $"EffectiveStdDev {chart.EffectiveStdDev} is under the 2.5 ms floor");
    }

    /// <summary>
    /// #7644: Lite computes a lane's steps at now, as the Viewer and the web page do, whatever window the page shows. A window a week
    /// back used to key its history to the window's own start, so a baseline day that lies after that start never counted. The window's
    /// own day is seeded at 21 ms (the other three same-weekday days at about 1 ms): the band for that hour has to include it.
    /// </summary>
    [Fact]
    public async Task AWindowAWeekBack_StepsFromTheSameThirtyDaysAsARequestAtNow()
    {
        const int serverId = -8702;
        var today = DateTime.UtcNow.Date;
        var windowStart = DateTime.SpecifyKind(today.AddDays(-7).AddHours(10), DateTimeKind.Unspecified);
        const string sql = @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
     delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
SELECT -8800000 - (d * 1000 + i), $1 - to_days(d) + to_hours(10) + to_minutes(2 * i + 1), $2, 'IoServer', 'AppDb',
       'AppDb_busy', 'ROWS', 'D:\AppDb.mdf', 100, 10000, 0, 81920, 0,
       CASE WHEN d = 7 THEN 210000 + (i % 5) * 500 ELSE 9000 + (i % 5) * 500 END, 0, 60
FROM (VALUES (7), (14), (21), (28)) AS dd(d)
CROSS JOIN range(0, 30) AS ii(i)";
        await ExecuteSeedAsync(sql, today, serverId);

        var steps = await new LocalDataService(_duckDb)
            .GetBaselineStepsForLaneAsync(serverId, MetricNames.IoLatencyChart, windowStart, windowStart.AddHours(1));

        var step = Assert.Single(steps);
        // 90 samples near 1 ms and 30 near 21 ms average about 6 ms. History that ended at the window's start would read about 1 ms.
        Assert.InRange(step.Mean, 5.0, 7.0);
    }

    private static BaselineProvider.CachedBaseline EntryAtFiveThirty(DateTime windowEndUtc)
    {
        // Buckets keyed by the target-local (hour, day of week): Wednesday local hours 15 to 18, each its own mean.
        var buckets = new Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>();
        foreach (var hour in new[] { 15, 16, 17, 18 })
            buckets[(hour, (int)DayOfWeek.Wednesday)] = new BaselineBucket { Mean = hour, StdDev = 1, SampleCount = 500 };

        return new BaselineProvider.CachedBaseline
        {
            Buckets = buckets,
            Clock = LocalClockWindow.FixedOffset(windowEndUtc, 330),
            UtcOffsetMinutes = 330
        };
    }

    [Fact]
    public void AZoneAtFiveThirty_StepsOnItsLocalHours_EachWithItsOwnHoursBucket()
    {
        var start = new DateTime(2026, 1, 7, 10, 0, 0, DateTimeKind.Unspecified);   // 15:30 local
        var end = start.AddHours(3);

        var steps = BaselineProvider.StepsFor(EntryAtFiveThirty(end), start, end, new BaselineLocalClock());

        // Local hours 15, 16, 17 and 18 (the last runs past the window's end, as the web read's does): they step at :30 UTC.
        Assert.Equal(4, steps.Count);
        Assert.Equal(start.AddMinutes(-30), steps[0].FromUtc);
        Assert.Equal(new[] { start.AddMinutes(30), start.AddMinutes(90), start.AddMinutes(150), start.AddMinutes(210) }, steps.Select(s => s.ToUtc).ToArray());
        Assert.Equal(new double[] { 15, 16, 17, 18 }, steps.Select(s => s.Bucket.Mean).ToArray());
        // Each step ends where the next begins.
        for (var i = 1; i < steps.Count; i++)
            Assert.Equal(steps[i - 1].ToUtc, steps[i].FromUtc);
    }

    [Fact]
    public void APointInAnHourWithNoBand_GetsNoMarker_AndTheOthersAreJudgedAgainstTheirOwnHour()
    {
        var t0 = new DateTime(2026, 11, 3, 0, 0, 0, DateTimeKind.Unspecified).ToOADate();
        // Ten points, all at 5 ms. Band 0..2 ms for the first 0.35 day and the last 0.35 day; nothing between (points 4, 5 and 6 sit there, so 7 of the 10 are marked).
        var data = Enumerable.Range(0, 10).Select(i => (t0 + i / 10.0, 5.0)).ToList();
        IReadOnlyList<PerformanceMonitor.Ui.BaselineBand.HourStep> bands =
        [
            new(t0, t0 + 0.35, 1, 0, 2),
            new(t0 + 0.65, t0 + 1, 1, 0, 2),
        ];

        var marked = new List<double>();
        StaTestThread.Run(() =>
        {
            var control = new CorrelatedTimelineLanesControl();
            control.Initialize(null!, 1, () => TimeZoneInfo.Utc);
            control.UpdateLane(control.FileIoChart, "I/O ms", data, "#81C784", bands: bands, minAnomalyValue: 2);
            marked = Darling.Tests.OverviewLaneMarks.MarkedX(control.FileIoChart.Plot);
        });

        Assert.Equal(new[] { 0, 1, 2, 3, 7, 8, 9 }.Select(i => t0 + i / 10.0).ToList(), marked);
    }

    [Fact]
    public void TheTooltipArrow_JudgesEachValueAgainstTheStepItsXFallsIn_AndNoneInAnHourWithoutOne()
    {
        string?[] arrows = new string?[3];
        StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            using var manager = new PerformanceMonitor.Ui.CorrelatedCrosshairManager();
            manager.AddLane(chart, "I/O ms", "ms");
            manager.SetLaneBaselineSteps(chart,
                [new(100, 101, 1, 0, 2), new(101, 102, 5, 4, 6)], minAnomalyValue: 2);
            manager.SetLaneData(chart, new[] { 100.5, 101.5, 102.5 }, new[] { 5.0, 5.0, 5.0 });
            arrows[0] = manager.HoverArrowAt(chart, 100.5);   // 5 ms over the first hour's 0..2 band
            arrows[1] = manager.HoverArrowAt(chart, 101.5);   // 5 ms inside the second hour's 4..6 band
            arrows[2] = manager.HoverArrowAt(chart, 102.5);   // an hour with no step
        });

        Assert.Equal("▲", arrows[0]);
        Assert.Null(arrows[1]);
        Assert.Null(arrows[2]);
    }
}
