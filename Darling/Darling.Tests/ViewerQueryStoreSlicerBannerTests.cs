/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: the Query Store grid's banner names the slicer's start from the source that drew the slicer. Over a window of seven days
/// or more the interval table serves the slicer as well as the grid, so both start at the same instant and the banner says so once.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerQueryStoreSlicerBannerTests
{
    private static readonly DateTime RangeStart = new(2026, 10, 13, 0, 0, 0, DateTimeKind.Utc);

    /* The interval table keeps ten days, so a 30 day window's grid and slicer both start twenty days in. */
    private static readonly DateTime TableStart = RangeStart.AddDays(20).AddHours(21);

    private static QueryStoreIntervalWide.WideReadPlan TablePlan(DateTime start) =>
        new(true, RangeStart.AddDays(26), start, start, QueryStoreIntervalWide.WideStartBound.TablePurgeEdge);

    [Fact]
    public void ASlicerThatStartsWhereTheGridStarts_IsNotNamedBesideIt()
    {
        StaTestThread.Run(() =>
        {
            var banner = new TextBlock();

            ViewerServerTab.UpdateTruncationBanner(banner, TableStart, RangeStart, widePlan: TablePlan(TableStart));

            Assert.Equal(Visibility.Visible, banner.Visibility);
            Assert.StartsWith("Showing since ", banner.Text, StringComparison.Ordinal);
            Assert.EndsWith("(interval table keeps 10 days)", banner.Text, StringComparison.Ordinal);
            Assert.DoesNotContain("slicer", banner.Text, StringComparison.OrdinalIgnoreCase);
        });
    }

    /* The field case: raw's floor sits six days after the table's start, and the table served the grid and the slicer. Raw drew
       nothing, so its floor is not on the banner. */
    [Fact]
    public void ATableServedSlicer_IsNamedFromTheTablesStart_NeverFromRawsFloor()
    {
        InUtc(() =>
        {
            var rawFloor = RangeStart.AddDays(26).AddHours(1);
            var banner = new TextBlock();

            ViewerServerTab.UpdateTruncationBanner(banner, rawFloor, RangeStart, widePlan: TablePlan(TableStart), slicerPlan: TablePlan(TableStart));
            Assert.Equal("Showing since 2026-11-02 21:00:00 (interval table keeps 10 days)", banner.Text);

            /* A slicer plan that starts later than the grid's is named, at its own start. */
            ViewerServerTab.UpdateTruncationBanner(banner, rawFloor, RangeStart, widePlan: TablePlan(TableStart), slicerPlan: TablePlan(TableStart.AddDays(1)));
            Assert.Equal("Showing since 2026-11-02 21:00:00 (interval table keeps 10 days) · slicer since 2026-11-03 21:00:00", banner.Text);

            /* Raw served the slicer (no slicer plan): its cut floor is the slicer's start, as before. */
            ViewerServerTab.UpdateTruncationBanner(banner, rawFloor, RangeStart, widePlan: TablePlan(TableStart));
            Assert.Equal("Showing since 2026-11-02 21:00:00 (interval table keeps 10 days) · slicer since 2026-11-08 01:00:00", banner.Text);
        });
    }

    /* The grid's gate and the slicer's differ (a custom range's literal end, legacy rows), so raw can serve the grid while the table
       serves the slicer. Each start is then named for what it is. */
    [Fact]
    public void ARawServedGrid_WithATableServedSlicer_NamesBothStarts()
    {
        InUtc(() =>
        {
            var rawFloor = RangeStart.AddDays(26).AddHours(1);
            var banner = new TextBlock();

            ViewerServerTab.UpdateTruncationBanner(banner, rawFloor, RangeStart, slicerPlan: TablePlan(TableStart));
            Assert.Equal("Showing since 2026-11-08 01:00:00 · slicer since 2026-11-02 21:00:00", banner.Text);

            /* The table reaches the window's start: the slicer is not cut, so only the grid's start is named. */
            var whole = TablePlan(TableStart) with { ReadStart = RangeStart, StartBound = QueryStoreIntervalWide.WideStartBound.Window };
            ViewerServerTab.UpdateTruncationBanner(banner, rawFloor, RangeStart, slicerPlan: whole);
            Assert.Equal("Showing since 2026-11-08 01:00:00", banner.Text);

            /* Raw covers the window and the table does not: the slicer's shortfall alone. */
            ViewerServerTab.UpdateTruncationBanner(banner, RangeStart, RangeStart, slicerPlan: TablePlan(TableStart));
            Assert.Equal("Slicer since 2026-11-02 21:00:00 (the grid shows the full window)", banner.Text);

            /* Neither is cut: nothing to say. */
            ViewerServerTab.UpdateTruncationBanner(banner, RangeStart, RangeStart, slicerPlan: whole);
            Assert.Equal(Visibility.Collapsed, banner.Visibility);
        });
    }

    /* A slicer drag re-reads the grid over the dragged range and leaves the slicer as the last range load drew it. The interval table
       can serve that grid read, so the drag's banner takes the grid's plan and the slicer plan kept from that load. Here the drag
       starts fifteen days into the 30 day window: the table's ten days start after it, and raw's floor after that. */
    [Fact]
    public void ADraggedRangeTheTableServes_NamesTheTablesStart_NotRawsFloor()
    {
        InUtc(() =>
        {
            var dragStart = RangeStart.AddDays(15);
            var rawFloor = RangeStart.AddDays(26).AddHours(1);
            var banner = new TextBlock();

            /* The table served the last load's slicer, from the instant the dragged grid starts at. */
            ViewerServerTab.UpdateTruncationBanner(banner, rawFloor, dragStart, widePlan: TablePlan(TableStart), slicerPlan: TablePlan(TableStart));
            Assert.Equal("Showing since 2026-11-02 21:00:00 (interval table keeps 10 days)", banner.Text);

            /* Raw served the last load's slicer (a window under seven days): its floor is the slicer's, beside the grid's own start. */
            ViewerServerTab.UpdateTruncationBanner(banner, rawFloor, dragStart, widePlan: TablePlan(TableStart), slicerPlan: null);
            Assert.Equal("Showing since 2026-11-02 21:00:00 (interval table keeps 10 days) · slicer since 2026-11-08 01:00:00", banner.Text);

            /* A drag that starts after the table's start is whole: nothing to say. */
            var whole = TablePlan(TableStart) with { ReadStart = TableStart.AddDays(2), StartBound = QueryStoreIntervalWide.WideStartBound.Window };
            ViewerServerTab.UpdateTruncationBanner(banner, TableStart.AddDays(2), TableStart.AddDays(2), widePlan: whole, slicerPlan: TablePlan(TableStart));
            Assert.Equal(Visibility.Collapsed, banner.Visibility);
        });
    }

    [Fact]
    public async Task TheSlicersPlan_IsTheOneItsReadReports_AndNullWhenRawServedOrTheReadFailed()
    {
        var buckets = new List<TimeSliceBucket>();
        var table = TablePlan(TableStart);

        Assert.Equal(table, await ViewerServerTab.QueryStoreSlicerPlanAsync(Task.FromResult<(List<TimeSliceBucket>, QueryStoreIntervalWide.WideReadPlan?)>((buckets, table))));
        Assert.Null(await ViewerServerTab.QueryStoreSlicerPlanAsync(Task.FromResult<(List<TimeSliceBucket>, QueryStoreIntervalWide.WideReadPlan?)>((buckets, null))));
        Assert.Null(await ViewerServerTab.QueryStoreSlicerPlanAsync(
            Task.FromException<(List<TimeSliceBucket>, QueryStoreIntervalWide.WideReadPlan?)>(new InvalidOperationException("the slicer read failed"))));
    }

    private static void InUtc(Action body)
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        try
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            StaTestThread.Run(body);
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
        }
    }
}
