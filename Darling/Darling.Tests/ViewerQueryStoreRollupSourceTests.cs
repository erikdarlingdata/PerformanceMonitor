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
        Assert.Equal("3 days from rollups, 8 days from the interval table", source.Describe());
        Assert.Null(source.BannerNote());
    }

    [Fact]
    public void AWindowThatEndsAtMidnight_DoesNotTouchTheDayThatStartsThere()
    {
        var midnight = new DateTime(2026, 11, 21, 0, 0, 0, DateTimeKind.Utc);
        var source = QueryStoreClosedDayRollup.DescribeSource(Start.Date, midnight, Run(11, 21), QueryStoreClosedDayRollup.SourceReason.Served);

        Assert.Equal(10, source.RollupDays.Count);
        Assert.Empty(source.IntervalDays);
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
        Assert.Matches(@"grid read \d+ ms \(3 days from rollups, 8 days from the interval table\)", line);
        Assert.Matches(@"slicer read \d+ ms \(11 days from the interval table, no rollup days: the server's heartbeat is older than 2 hours\)", line);
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
        Assert.Matches(@"grid read \d+ ms \(3 days from rollups, 8 days from the interval table, " + System.Text.RegularExpressions.Regex.Escape(expected) + @"\)", line);
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
