/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: the Query Store grid's banner names the slicer's start from the source that drew the slicer. Over a window of seven days
/// or more the interval table serves the slicer as well as the grid, so both start at the same instant and the banner says so once.
/// </summary>
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
}
