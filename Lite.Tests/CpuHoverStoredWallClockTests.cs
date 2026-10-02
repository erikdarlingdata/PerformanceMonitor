/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: the CPU chart plots a bucket read from the server's STORED WALL CLOCK (rows collected before the UTC
/// column existed) at the first instant that wall time names, so in the repeated autumn hour the hover used to add
/// that first pass's UTC offset to a bucket that may be from the second pass, or that merged both. Three parts fix
/// it and are pinned here: the read marks a point whose stored wall time happens twice on the server's clock
/// (<see cref="CpuUtilizationRow.SampleTimeNamesTwoInstants"/>), the tab hands the hover a predicate over the
/// plotted X values of those points (<see cref="ServerTab.CpuHoverPlainTimes"/>), and the hover words a marked X
/// without the offset (<see cref="ChartHoverHelper.FormatHoverTime"/>, <see cref="ChartHoverHelper.PlainTimeAt"/>).
/// A stored wall time that happens once converts to one instant exactly, so it is not marked and the hover keeps
/// its offset, whatever zone the machine shows the chart in. The server is on US Eastern time, whose clocks fall
/// back at 06:00 UTC on 1 November 2026, so 01:30 on the wall happens at 05:30 UTC (-04:00) and again at 06:30 UTC
/// (-05:00).
/// </summary>
public sealed class CpuHoverStoredWallClockTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string EasternZone = "Eastern Standard Time";
    private const string TokyoZone = "Tokyo Standard Time";
    private const int ServerId = 4766;

    /* A 12-day window is cut in 15-minute buckets, so two rows in one quarter hour share a bucket. */
    private const int WindowHours = 12 * 24;
    private static readonly DateTime WindowEnd = new(2026, 11, 7, 0, 0, 0);
    private static readonly DateTime RepeatedWall = new(2026, 11, 1, 1, 30, 0);
    private static readonly DateTime FirstRepeatedInstant = new(2026, 11, 1, 5, 30, 0);
    private static readonly DateTime SecondRepeatedInstant = new(2026, 11, 1, 6, 30, 0);

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private long _nextId = -4766;
    private DuckDBConnection? _seedConn;

    public CpuHoverStoredWallClockTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
    }

    /// <summary>
    /// The hover line for the two readings of the repeated hour: with a zone it adds each pass's offset, and for an X
    /// whose stored wall time names two instants (<c>plain</c>) it words the wall time alone, the same for both.
    /// </summary>
    [Fact]
    public void TheHoverTime_InTheRepeatedHour_DropsTheOffsetForAPlainX_AndKeepsItOtherwise()
    {
        var eastern = TimeZoneInfo.FindSystemTimeZoneById(EasternZone);

        Assert.Equal("01:30:00 -04:00", ChartHoverHelper.FormatHoverTime(FirstRepeatedInstant, () => eastern));
        Assert.Equal("01:30:00 -05:00", ChartHoverHelper.FormatHoverTime(SecondRepeatedInstant, () => eastern));
        Assert.Equal("01:30:00 -04:00", ChartHoverHelper.FormatHoverTime(FirstRepeatedInstant, () => eastern, plain: false));

        Assert.Equal("01:30:00", ChartHoverHelper.FormatHoverTime(FirstRepeatedInstant, () => eastern, plain: true));
        Assert.Equal("01:30:00", ChartHoverHelper.FormatHoverTime(SecondRepeatedInstant, () => eastern, plain: true));
    }

    /// <summary>
    /// Outside the repeated hour the wall time names one instant and carries no offset, so <c>plain</c> changes
    /// nothing: the edges of the hour on both sides, a summer date, and a zone that has no repeated hour at all.
    /// </summary>
    [Fact]
    public void TheHoverTime_OutsideTheRepeatedHour_IsTheSameWithOrWithoutPlain()
    {
        var eastern = TimeZoneInfo.FindSystemTimeZoneById(EasternZone);
        var outside = new[]
        {
            new DateTime(2026, 11, 1, 4, 59, 59),
            new DateTime(2026, 11, 1, 7, 0, 0),
            new DateTime(2026, 7, 1, 12, 15, 30),
        };

        foreach (var instant in outside)
        {
            var text = ChartHoverHelper.FormatHoverTime(instant, () => eastern);
            Assert.DoesNotContain("-0", text, StringComparison.Ordinal);
            Assert.Equal(text, ChartHoverHelper.FormatHoverTime(instant, () => eastern, plain: true));
        }

        Assert.Equal("00:59:59", ChartHoverHelper.FormatHoverTime(outside[0], () => eastern, plain: true));
        Assert.Equal("02:00:00", ChartHoverHelper.FormatHoverTime(outside[1], () => eastern, plain: true));
        Assert.Equal("05:30:00", ChartHoverHelper.FormatHoverTime(FirstRepeatedInstant, () => TimeZoneInfo.Utc, plain: true));
        Assert.Equal("05:30:00", ChartHoverHelper.FormatHoverTime(FirstRepeatedInstant, () => TimeZoneInfo.Utc));
    }

    /// <summary>
    /// The plain line is the suffixed line minus its suffix in any culture: <see cref="DisplayZone.Format"/> words the
    /// time in the invariant culture, so the plain branch does too. A culture whose time separator is not a colon
    /// (the clone here uses a dot) would otherwise turn the plain line into "01.30.00" beside "01:30:00 -04:00".
    /// </summary>
    [Fact]
    public void ThePlainTime_IsWordedInTheInvariantCulture_LikeTheLineItReplaces()
    {
        var eastern = TimeZoneInfo.FindSystemTimeZoneById(EasternZone);
        var dotted = (CultureInfo)CultureInfo.InvariantCulture.Clone();
        dotted.DateTimeFormat.TimeSeparator = ".";
        var before = CultureInfo.CurrentCulture;

        try
        {
            CultureInfo.CurrentCulture = dotted;
            Assert.Equal("01.30.00", new DateTime(2026, 11, 1, 1, 30, 0).ToString("HH:mm:ss"));

            Assert.Equal("01:30:00", ChartHoverHelper.FormatHoverTime(FirstRepeatedInstant, () => eastern, plain: true));
            Assert.Equal("01:30:00 -04:00", ChartHoverHelper.FormatHoverTime(FirstRepeatedInstant, () => eastern));
        }
        finally
        {
            CultureInfo.CurrentCulture = before;
        }
    }

    /// <summary>
    /// A bucket of rows collected before the UTC column existed (no <c>sample_time_utc</c>) was cut on the wall
    /// time. Its point is marked only when that wall time happens twice on the server's clock, so a bucket in the
    /// repeated hour is marked and one before or after it is not: the wall time converts to one instant exactly. A
    /// bucket of rows that have the instant was cut on it, so its point is not marked either, in either pass of the
    /// repeated hour. The points come back in instant order.
    /// </summary>
    [Fact]
    public async Task TheUtcFrame_MarksAPreRungBucketInTheRepeatedHour_AndNotAPreRungBucketOutsideItNorAPostRungOne()
    {
        var clock = ServerClock.Resolve(EasternZone, -300);
        var beforeTheHour = new DateTime(2026, 11, 1, 0, 30, 0);
        var inTheHour = new DateTime(2026, 11, 1, 1, 15, 0);
        var afterTheHour = new DateTime(2026, 11, 1, 3, 0, 0);

        await SeedCpuAsync(beforeTheHour, null, 10);
        await SeedCpuAsync(inTheHour, null, 40);
        await SeedCpuAsync(RepeatedWall, FirstRepeatedInstant, 20);
        await SeedCpuAsync(RepeatedWall, SecondRepeatedInstant, 60);
        await SeedCpuAsync(afterTheHour, null, 30);

        var rows = await ReadCpuAsync(clock, CpuTimeFrame.Utc);

        Assert.Equal(
            new[] { new DateTime(2026, 11, 1, 4, 30, 0), new DateTime(2026, 11, 1, 5, 15, 0), FirstRepeatedInstant, SecondRepeatedInstant, new DateTime(2026, 11, 1, 8, 0, 0) },
            rows.Select(r => r.SampleTimeUtc).ToArray());
        Assert.Equal(new[] { 10, 40, 20, 60, 30 }, rows.Select(r => r.SqlServerCpu).ToArray());
        Assert.Equal(new[] { false, true, false, false, false }, rows.Select(r => r.SampleTimeNamesTwoInstants).ToArray());
    }

    /// <summary>
    /// The repeated hour is exactly the hours the wall time happens twice: a pre-change bucket from 01:00 to its last
    /// quarter hour, 01:45, is marked, and the quarter hour before it (00:45) and the first one after it (02:00) are
    /// not.
    /// </summary>
    [Fact]
    public async Task TheUtcFrame_MarksAPreRungBucketFromTheFirstToTheLastQuarterHourOfTheRepeatedHour_AndNoOther()
    {
        var clock = ServerClock.Resolve(EasternZone, -300);

        await SeedCpuAsync(new DateTime(2026, 11, 1, 0, 45, 0), null, 10);
        await SeedCpuAsync(new DateTime(2026, 11, 1, 1, 0, 0), null, 20);
        await SeedCpuAsync(new DateTime(2026, 11, 1, 1, 45, 0), null, 30);
        await SeedCpuAsync(new DateTime(2026, 11, 1, 2, 0, 0), null, 40);

        var rows = await ReadCpuAsync(clock, CpuTimeFrame.Utc);

        Assert.Equal(new[] { 10, 20, 30, 40 }, rows.Select(r => r.SqlServerCpu).ToArray());
        Assert.Equal(new[] { false, true, true, false }, rows.Select(r => r.SampleTimeNamesTwoInstants).ToArray());
    }

    /// <summary>
    /// The case the hover lied about: two pre-change readings of 01:30, one from each pass, share a bucket and
    /// become one point at the first pass's instant. The point is marked, and the hover words it "01:30:00" where
    /// it used to say "01:30:00 -04:00" for a bucket that held a reading from the -05:00 pass. A post-change
    /// reading of the second pass keeps its own offset.
    /// </summary>
    [Fact]
    public async Task ThePreRungBucketThatMergedBothPasses_IsMarked_AndTheHoverDropsItsOffset()
    {
        var clock = ServerClock.Resolve(EasternZone, -300);
        var eastern = TimeZoneInfo.FindSystemTimeZoneById(EasternZone);

        await SeedCpuAsync(RepeatedWall, null, 20);
        await SeedCpuAsync(RepeatedWall, null, 60);
        await SeedCpuAsync(RepeatedWall, SecondRepeatedInstant, 90);

        var rows = await ReadCpuAsync(clock, CpuTimeFrame.Utc);

        Assert.Equal(2, rows.Count);
        var merged = rows[0];
        var exact = rows[1];
        Assert.Equal(FirstRepeatedInstant, merged.SampleTimeUtc);
        Assert.Equal(40, merged.SqlServerCpu);
        Assert.True(merged.SampleTimeNamesTwoInstants);
        Assert.Equal(SecondRepeatedInstant, exact.SampleTimeUtc);
        Assert.False(exact.SampleTimeNamesTwoInstants);

        var plainAt = ServerTab.CpuHoverPlainTimes(rows);
        Assert.NotNull(plainAt);

        var mergedX = DateTime.FromOADate(merged.SampleTimeUtc.ToOADate());
        Assert.Equal("01:30:00", ChartHoverHelper.FormatHoverTime(mergedX, () => eastern, plainAt!(mergedX)));

        var exactX = DateTime.FromOADate(exact.SampleTimeUtc.ToOADate());
        Assert.Equal("01:30:00 -05:00", ChartHoverHelper.FormatHoverTime(exactX, () => eastern, plainAt(exactX)));
    }

    /// <summary>
    /// A post-change reading of the second pass keeps the offset of the pass it was in: it is not marked, so the
    /// predicate is null when it is the only point and the hover line stays "01:30:00 -05:00".
    /// </summary>
    [Fact]
    public async Task APostRungReadingOfTheSecondPass_IsNotMarked_AndTheHoverKeepsItsOffset()
    {
        var clock = ServerClock.Resolve(EasternZone, -300);
        var eastern = TimeZoneInfo.FindSystemTimeZoneById(EasternZone);

        await SeedCpuAsync(RepeatedWall, SecondRepeatedInstant, 60);

        var point = Assert.Single(await ReadCpuAsync(clock, CpuTimeFrame.Utc));

        Assert.False(point.SampleTimeNamesTwoInstants);
        Assert.Null(ServerTab.CpuHoverPlainTimes(new[] { point }));
        Assert.Equal("01:30:00 -05:00", ChartHoverHelper.FormatHoverTime(point.SampleTimeUtc, () => eastern));
    }

    /// <summary>
    /// The server-local frame cuts every bucket on the stored wall clock, whatever the row carries, so a point is
    /// marked exactly when its wall time happens twice: the 01:30 point is, the 03:00 one is not. The default read and
    /// the frame asked for by name answer alike.
    /// </summary>
    [Fact]
    public async Task TheServerLocalFrame_MarksOnlyThePointsWhoseWallTimeHappensTwice()
    {
        var clock = ServerClock.Resolve(EasternZone, -300);

        await SeedCpuAsync(RepeatedWall, FirstRepeatedInstant, 20);
        await SeedCpuAsync(new DateTime(2026, 11, 1, 3, 0, 0), null, 30);

        foreach (var rows in new[] { await ReadCpuAsync(clock, null), await ReadCpuAsync(clock, CpuTimeFrame.ServerLocal) })
        {
            Assert.Equal(new[] { RepeatedWall, new DateTime(2026, 11, 1, 3, 0, 0) }, rows.Select(r => r.SampleTime).ToArray());
            Assert.Equal(new[] { true, false }, rows.Select(r => r.SampleTimeNamesTwoInstants).ToArray());
        }
    }

    /// <summary>
    /// A server on a fixed offset, or on UTC, has no repeated hour, so no point is marked in either frame, even at
    /// the wall time where a US zone repeats an hour (2026-11-01 01:30): the wall time converts to one instant.
    /// </summary>
    [Fact]
    public async Task AServerWithNoRepeatedHour_MarksNoPoint_InEitherFrame()
    {
        var fixedOffset = ServerClock.FixedOffset(-300);

        await SeedCpuAsync(RepeatedWall, null, 20);

        foreach (var (clock, instant) in new[] { (fixedOffset, new DateTime(2026, 11, 1, 6, 30, 0)), (ServerClock.Utc, RepeatedWall) })
        {
            var utcRows = await ReadCpuAsync(clock, CpuTimeFrame.Utc);
            var point = Assert.Single(utcRows);
            Assert.Equal(instant, point.SampleTimeUtc);
            Assert.False(point.SampleTimeNamesTwoInstants);
            Assert.Null(ServerTab.CpuHoverPlainTimes(utcRows));

            var localRows = await ReadCpuAsync(clock, CpuTimeFrame.ServerLocal);
            var localPoint = Assert.Single(localRows);
            Assert.Equal(instant, localPoint.SampleTimeUtc);
            Assert.False(localPoint.SampleTimeNamesTwoInstants);
            Assert.Null(ServerTab.CpuHoverPlainTimes(localRows));
        }
    }

    /// <summary>
    /// The case the first fix got wrong: a server whose clock has no repeated hour (Tokyo) stores 14:30 on 1 November,
    /// which is 05:30 UTC exactly. The point is not marked, so a machine showing the chart in US Eastern time, whose
    /// 01:30 happens twice on that date, keeps the offset that says which pass this instant is: "01:30:00 -04:00".
    /// Dropping it would make this point read like the other pass.
    /// </summary>
    [Fact]
    public async Task APreRungBucketOnAServerWithNoRepeatedHour_IsNotMarked_AndTheHoverInAnotherZonesRepeatedHourKeepsItsOffset()
    {
        var clock = ServerClock.Resolve(TokyoZone, 540);
        var eastern = TimeZoneInfo.FindSystemTimeZoneById(EasternZone);

        await SeedCpuAsync(new DateTime(2026, 11, 1, 14, 30, 0), null, 20);

        var rows = await ReadCpuAsync(clock, CpuTimeFrame.Utc);

        var point = Assert.Single(rows);
        Assert.Equal(FirstRepeatedInstant, point.SampleTimeUtc);
        Assert.False(point.SampleTimeNamesTwoInstants);

        var plainAt = ServerTab.CpuHoverPlainTimes(rows);
        Assert.Null(plainAt);

        var plottedX = DateTime.FromOADate(point.SampleTimeUtc.ToOADate());
        Assert.Equal("01:30:00 -04:00", ChartHoverHelper.FormatHoverTime(plottedX, () => eastern, plainAt?.Invoke(plottedX) == true));
    }

    /// <summary>
    /// The predicate answers for the X the hover reads back from the plot, <c>FromOADate(ToOADate(SampleTimeUtc))</c>,
    /// which is not the stamp itself when the stamp has a finer tick than a millisecond (the store keeps
    /// microseconds); true for a marked row's, false for an unmarked row's.
    /// </summary>
    [Fact]
    public void TheHoverPredicate_MatchesAMarkedRowsPlottedX_AndNotAnUnmarkedOnes()
    {
        var marked = new CpuUtilizationRow { SampleTimeUtc = FirstRepeatedInstant.AddTicks(1234), SampleTimeNamesTwoInstants = true };
        var unmarked = new CpuUtilizationRow { SampleTimeUtc = SecondRepeatedInstant.AddTicks(1234), SampleTimeNamesTwoInstants = false };

        var plain = ServerTab.CpuHoverPlainTimes(new[] { unmarked, marked });

        Assert.NotNull(plain);
        Assert.True(plain!(DateTime.FromOADate(marked.SampleTimeUtc.ToOADate())));
        Assert.False(plain(DateTime.FromOADate(unmarked.SampleTimeUtc.ToOADate())));
        Assert.False(plain(new DateTime(2026, 11, 1, 9, 0, 0)));
    }

    /// <summary>No row marked, or no row at all: there is nothing to say, so the hover is left without a predicate.</summary>
    [Fact]
    public void TheHoverPredicate_IsNull_WhenNoRowIsMarked()
    {
        var unmarked = new CpuUtilizationRow { SampleTimeUtc = FirstRepeatedInstant };

        Assert.Null(ServerTab.CpuHoverPlainTimes(new[] { unmarked, unmarked }));
        Assert.Null(ServerTab.CpuHoverPlainTimes(Array.Empty<CpuUtilizationRow>()));
    }

    /// <summary>
    /// The predicate belongs to the render that built it: <see cref="ChartHoverHelper.Clear"/>, which every chart
    /// render calls first, sets it back to <c>null</c>, so a re-render that finds no marked row cannot keep the
    /// last render's.
    /// </summary>
    [Fact]
    public void ClearingTheHover_DropsThePredicate()
    {
        var afterClear = OnStaThread(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var hover = new ChartHoverHelper(chart, "%", displayZone: () => TimeZoneInfo.Utc);
            hover.PlainTimeAt = t => true;
            Assert.NotNull(hover.PlainTimeAt);

            hover.Clear();
            return hover.PlainTimeAt;
        });

        Assert.Null(afterClear);
    }

    /// <summary>
    /// The CPU chart's render hands the hover its predicate, and does it after the hover was cleared (the same
    /// call would otherwise be undone by the clear).
    /// </summary>
    [Fact]
    public void TheCpuChartRender_SetsThePredicateAfterClearingTheHover()
    {
        var source = Lite.Tests.ParitySource.ReadFile("Lite/Controls/ServerTab.Charts.cs");
        var start = source.IndexOf("private void UpdateCpuChart(", StringComparison.Ordinal);
        Assert.True(start >= 0, "ServerTab.Charts.cs has no UpdateCpuChart");
        var end = source.IndexOf("private void UpdateMemoryChart(", start, StringComparison.Ordinal);
        Assert.True(end > start, "UpdateMemoryChart no longer follows UpdateCpuChart");
        var body = source[start..end];

        var clear = body.IndexOf("_cpuHover?.Clear();", StringComparison.Ordinal);
        var set = body.IndexOf("_cpuHover.PlainTimeAt = CpuHoverPlainTimes(data);", StringComparison.Ordinal);

        Assert.True(clear >= 0, "UpdateCpuChart no longer clears the hover");
        Assert.True(set > clear, "UpdateCpuChart does not set the hover's PlainTimeAt from the rows after clearing it");
    }

    private Task<List<CpuUtilizationRow>> ReadCpuAsync(ServerClock clock, CpuTimeFrame? frame) =>
        frame.HasValue
            ? _dataService.GetCpuUtilizationAsync(ServerId, hoursBack: WindowHours, asOfUtc: WindowEnd, serverClock: clock, frame: frame.Value)
            : _dataService.GetCpuUtilizationAsync(ServerId, hoursBack: WindowHours, asOfUtc: WindowEnd, serverClock: clock);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private async Task SeedCpuAsync(DateTime sampleTimeServerLocal, DateTime? sampleTimeUtc, int sqlCpu)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time,
             sqlserver_cpu_utilization, other_process_cpu_utilization, sample_time_utc)
            VALUES ($1, $2, $3, $4, $5, $6, 0, $7)";
        foreach (var v in new object[] { _nextId--, DateTime.UtcNow, ServerId, "CpuHoverSrv", sampleTimeServerLocal, sqlCpu, (object?)sampleTimeUtc ?? DBNull.Value })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }

        return result;
    }
}
