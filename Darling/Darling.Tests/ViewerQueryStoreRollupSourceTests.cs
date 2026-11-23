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
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9287: a Query Store read says which source its days came from. The slow-load line names the days a read took from the closed-day
/// rollups and the days it read from the interval table, and the grid's banner says when the rollups were not used, so a slow first open
/// can be told from a store that has not built its days.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerQueryStoreRollupSourceTests
{
    private static readonly DateTime Start = new(2026, 11, 11, 21, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime End = new(2026, 11, 21, 14, 0, 0, DateTimeKind.Utc);

    private static QueryStoreClosedDayRollup.ServedDays Run(int firstDay, int afterLastDay) =>
        new(new DateTime(2026, 11, firstDay, 0, 0, 0, DateTimeKind.Utc), new DateTime(2026, 11, afterLastDay, 0, 0, 0, DateTimeKind.Utc));

    [Fact]
    public void ARunOfBuiltDays_IsCountedAsRollupDays_AndTheRestAsIntervalDays()
    {
        /* 11 days touched: the partial first day (11), the built run 12 to 14, and 15 to 21 (the open day included). */
        var source = QueryStoreClosedDayRollup.DescribeSource(Start, End, Run(12, 15), QueryStoreClosedDayRollup.SourceReason.Served);

        Assert.Equal(new[] { 12, 13, 14 }, source.RollupDays.Select(d => d.Day));
        Assert.Equal(new[] { 11, 15, 16, 17, 18, 19, 20, 21 }, source.IntervalDays.Select(d => d.Day));
        Assert.Equal(
            "3 days from rollups (2026-11-12 to 2026-11-14), 8 days from the interval table (2026-11-11 partial first day, 2026-11-15 to 2026-11-20, 2026-11-21 partial last day)",
            source.Describe());
        Assert.Null(source.BannerNote());
    }

    [Fact]
    public void AWindowThatEndsAtMidnight_DoesNotTouchTheDayThatStartsThere()
    {
        var midnight = new DateTime(2026, 11, 21, 0, 0, 0, DateTimeKind.Utc);
        var source = QueryStoreClosedDayRollup.DescribeSource(Start.Date, midnight, Run(11, 21), QueryStoreClosedDayRollup.SourceReason.Served);

        Assert.Equal(10, source.RollupDays.Count);
        Assert.Empty(source.IntervalDays);
        /* #9363: a read served wholly from rollups names only the rollup days, and the interval part keeps the words it had. */
        Assert.Equal("10 days from rollups (2026-11-11 to 2026-11-20), 0 days from the interval table", source.Describe());
    }

    [Theory]
    [InlineData(QueryStoreClosedDayRollup.SourceReason.HeartbeatStale, "heartbeat is older than 2 hours")]
    [InlineData(QueryStoreClosedDayRollup.SourceReason.NoBuiltDays, "closed days are not built yet")]
    [InlineData(QueryStoreClosedDayRollup.SourceReason.FellBack, "rollup read failed")]
    public void ARollupFreeRead_NamesWhyInTheLineAndTheBanner(QueryStoreClosedDayRollup.SourceReason reason, string expected)
    {
        var source = QueryStoreClosedDayRollup.DescribeSource(Start, End, null, reason);

        Assert.Empty(source.RollupDays);
        Assert.Equal(11, source.IntervalDays.Count);
        Assert.Contains("11 days from the interval table", source.Describe(), StringComparison.Ordinal);
        Assert.Contains(expected, source.Describe(), StringComparison.Ordinal);
        Assert.Contains(expected, source.BannerNote(), StringComparison.Ordinal);
    }

    [Fact]
    public void ASlowLoad_NamesTheSourceOfEachRead_AfterItsTime()
    {
        var timer = new ViewerLoadTimer(0);
        timer.Track("grid read", Task.FromResult(1));
        timer.Track("slicer read", Task.FromResult(2));
        timer.NotePhase("grid read", QueryStoreClosedDayRollup.DescribeSource(Start, End, Run(12, 15), QueryStoreClosedDayRollup.SourceReason.Served).Describe());
        timer.NotePhase("slicer read", QueryStoreClosedDayRollup.DescribeSource(Start, End, null, QueryStoreClosedDayRollup.SourceReason.HeartbeatStale).Describe());

        var line = timer.Finish("Queries > Query Store by Duration");

        Assert.NotNull(line);
        Assert.Matches(@"grid read \d+ ms \(3 days from rollups \(2026-11-12 to 2026-11-14\), 8 days from the interval table \(.+\)\)", line);
        Assert.Matches(@"slicer read \d+ ms \(11 days from the interval table \(.+\), no rollup days: the server's heartbeat is older than 2 hours\)", line);
    }

    /* #9287: the grid read's line also says which shape its interval-table statements took, so a cold read can be told apart without a store read. */
    [Theory]
    [InlineData(3, 1, true, "edge margin 3 h, span 1 h, split arms")]
    [InlineData(2.5, 0.5, false, "edge margin 2.5 h, span 0.5 h, one arm, index not built")]
    [InlineData(26, 1, true, "edge margin fell back to 26 h, span 1 h, split arms")]
    [InlineData(26, 1, false, "edge margin fell back to 26 h, span 1 h, one arm, index not built")]
    [InlineData(3, 24, true, "edge margin 3 h, span fell back to 24 h, split arms")]
    [InlineData(26, 24, false, "edge margin fell back to 26 h, span fell back to 24 h, one arm, index not built")]
    public void ASlowLoad_NamesTheEdgeMarginTheSpanAndTheArmsOfTheGridRead(double marginHours, double spanHours, bool splitBuilt, string expected)
    {
        var timer = new ViewerLoadTimer(0);
        timer.Track("grid read", Task.FromResult(1));
        var source = QueryStoreClosedDayRollup.DescribeSource(Start, End, Run(12, 15), QueryStoreClosedDayRollup.SourceReason.Served)
            with { Shape = QueryStoreClosedDayRollup.DescribeShape(TimeSpan.FromHours(marginHours), TimeSpan.FromHours(spanHours), splitBuilt) };
        timer.NotePhase("grid read", source.Describe());

        var line = timer.Finish("Queries > Query Store by Duration");

        Assert.NotNull(line);
        Assert.Matches(@"grid read \d+ ms \(3 days from rollups \(.+\), 8 days from the interval table \(.+\), " + System.Text.RegularExpressions.Regex.Escape(expected) + @"\)", line);
    }

    private static readonly DateTime Now = new(2026, 11, 23, 15, 0, 0, DateTimeKind.Utc);

    private static DateOnly D(int day) => new(2026, 11, day);

    private static string? LabelOf(QueryStoreClosedDayRollup.ReadSource source, int day) =>
        source.IntervalNotes!.Single(n => n.Day == D(day)).Label;

    /* #9363: a read with a closed day outside the built run. Retention is 10 days, the window starts at the retention edge, and five closed days are built.
       The oldest whole day was never built and the purge may have thinned it, day 21 is closed with no rollup, and day 22 is not closed yet. */
    [Fact]
    public void WholeDaysReadFromTheIntervalTable_EachNameWhy()
    {
        var start = Now.AddDays(-10);
        var source = QueryStoreClosedDayRollup.DescribeSource(
            start, Now, Run(15, 20), QueryStoreClosedDayRollup.SourceReason.Served, false, Now, 10, Enumerable.Range(15, 5).Select(D).ToList());

        Assert.Equal(Enumerable.Range(15, 5), source.RollupDays.Select(d => d.Day));
        Assert.Equal(new[] { 13, 14, 20, 21, 22, 23 }, source.IntervalDays.Select(d => d.Day));
        Assert.Equal("partial first day", LabelOf(source, 13));
        Assert.Equal("never built at the retention edge", LabelOf(source, 14));
        Assert.Equal("no rollup for this day", LabelOf(source, 20));
        Assert.Equal("no rollup for this day", LabelOf(source, 21));
        Assert.Equal("not closed yet", LabelOf(source, 22));
        Assert.Equal("open day", LabelOf(source, 23));
        Assert.Equal(
            "5 days from rollups (2026-11-15 to 2026-11-19), 6 days from the interval table (2026-11-13 partial first day, 2026-11-14 never built at the retention edge, "
            + "2026-11-20 to 2026-11-21 no rollup for this day, 2026-11-22 not closed yet, 2026-11-23 open day)",
            source.Describe());
    }

    [Fact]
    public void ABuiltDayOutsideTheServedRun_SaysSo_AndADayNotBuiltDoesNot()
    {
        /* Built: 15, 16 and 18 to 20. The longest run is 18 to 20, so 15 and 16 are built but read from the interval table. */
        var source = QueryStoreClosedDayRollup.DescribeSource(
            Now.AddDays(-10), Now, Run(18, 21), QueryStoreClosedDayRollup.SourceReason.Served, false, Now, 10, new[] { D(15), D(16), D(18), D(19), D(20) });

        Assert.Equal("built but outside the longest run", LabelOf(source, 15));
        Assert.Equal("built but outside the longest run", LabelOf(source, 16));
        Assert.Equal("no rollup for this day", LabelOf(source, 17));
        Assert.Equal("no rollup for this day", LabelOf(source, 21));
    }

    /* #9363: the builder skips only a thinned day that was never built, and a built day keeps its rollup, so a built day at the retention edge
       that the read still took from the interval table is "built but outside the longest run", and only an unbuilt one is "never built". */
    [Fact]
    public void ABuiltDayAtTheRetentionEdge_IsNotCalledNeverBuilt_AndAnUnbuiltOneIs()
    {
        Assert.True(QueryStoreClosedDayRollup.MayBeThinned(D(14), Now, 10), "the day is not at the retention edge, so the test proves nothing");

        var built = Enumerable.Range(15, 5).Select(D).Append(D(14)).ToList();
        var withEdgeBuilt = QueryStoreClosedDayRollup.DescribeSource(
            Now.AddDays(-10), Now, Run(15, 20), QueryStoreClosedDayRollup.SourceReason.Served, false, Now, 10, built);
        Assert.Equal("built but outside the longest run", LabelOf(withEdgeBuilt, 14));
        Assert.Equal("no rollup for this day", LabelOf(withEdgeBuilt, 20));

        var edgeNotBuilt = QueryStoreClosedDayRollup.DescribeSource(
            Now.AddDays(-10), Now, Run(15, 20), QueryStoreClosedDayRollup.SourceReason.Served, false, Now, 10, Enumerable.Range(15, 5).Select(D).ToList());
        Assert.Equal("never built at the retention edge", LabelOf(edgeNotBuilt, 14));
    }

    [Fact]
    public void ARollupFreeRead_LabelsItsWholeClosedDays_WithTheWholeReadsReason()
    {
        var source = QueryStoreClosedDayRollup.DescribeSource(
            Now.AddDays(-10), Now, null, QueryStoreClosedDayRollup.SourceReason.HeartbeatStale, false, Now, 10, Array.Empty<DateOnly>());

        Assert.Equal("no rollups for this read", LabelOf(source, 17));
        Assert.Equal("never built at the retention edge", LabelOf(source, 14));
        Assert.Contains("no rollup days: the server's heartbeat is older than 2 hours", source.Describe(), StringComparison.Ordinal);
    }

    [Fact]
    public void ARead_WithNoNote_KeepsItsLineAsItWas()
    {
        var timer = new ViewerLoadTimer(0);
        timer.Track("grid read", Task.FromResult(1));

        Assert.Matches(@"\[grid read \d+ ms\], then", timer.Finish("Queries"));
    }

    /* The grid's table read is cut at the table's purge edge, so the banner says "interval table keeps 10 days". With the heartbeat
       older than two hours the rollups were not used, and the banner says that beside it. */
    [Fact]
    public void TheBanner_SaysTheRollupsWereNotUsed_BesideTheTablesPurgeEdge()
    {
        StaTestThread.Run(() =>
        {
            var tableStart = Start;
            var requested = Start.AddDays(-20);
            var stale = QueryStoreClosedDayRollup.DescribeSource(tableStart, End, null, QueryStoreClosedDayRollup.SourceReason.HeartbeatStale);
            var plan = new QueryStoreIntervalWide.WideReadPlan(true, tableStart, tableStart, tableStart, QueryStoreIntervalWide.WideStartBound.TablePurgeEdge, Source: stale);
            var banner = new TextBlock();

            ViewerServerTab.UpdateTruncationBanner(banner, tableStart, requested, widePlan: plan);

            Assert.Equal(Visibility.Visible, banner.Visibility);
            Assert.Contains("(interval table keeps 10 days)", banner.Text, StringComparison.Ordinal);
            Assert.Contains("closed-day rollups not used: the server's heartbeat is older than 2 hours", banner.Text, StringComparison.Ordinal);

            /* Rollup days were taken: nothing is added to the banner. */
            var served = QueryStoreClosedDayRollup.DescribeSource(tableStart, End, Run(12, 15), QueryStoreClosedDayRollup.SourceReason.Served);
            ViewerServerTab.UpdateTruncationBanner(banner, tableStart, requested, widePlan: plan with { Source = served });
            Assert.DoesNotContain("rollups not used", banner.Text, StringComparison.Ordinal);
        });
    }
}
