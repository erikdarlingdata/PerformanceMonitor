/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// What the Blocking tab's event and zero-fill charts draw as an event, and what their "Showing since" note names (#4966): the earlier
/// of the coverage start and the earliest bucket the chart actually draws. A zero bucket is the chart's baseline, not an event.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerBlockingChartsDataStartTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    private static readonly DateTime RangeStart = new(2026, 9, 10, 0, 0, 0, DateTimeKind.Utc);

    private static DateTime At(int days, int hours = 0) => RangeStart.AddDays(days).AddHours(hours);

    [Fact]
    public void ACountChart_DrawsOnlyTheNonZeroBuckets_AsEvents()
    {
        var data = new[] { new BlockingTrendPoint(At(1), 0), new BlockingTrendPoint(At(2), 3), new BlockingTrendPoint(At(3), 1) };

        Assert.Equal(new DateTime?[] { At(2), At(3) }, ViewerServerTab.BlockingTrendTimesDrawn(data).ToArray());
        Assert.Empty(ViewerServerTab.BlockingTrendTimesDrawn(Array.Empty<BlockingTrendPoint>()));
    }

    [Fact]
    public void TheLockWaitChart_DrawsEveryPointItHolds_AndNothingWhenEmpty()
    {
        var data = new[] { new LockWaitTrendPoint(At(2), "LCK_M_X", 1.5), new LockWaitTrendPoint(At(3), "LCK_M_S", 0.2) };

        Assert.Equal(new DateTime?[] { At(2), At(3) }, ViewerServerTab.LockWaitTimesDrawn(data).ToArray());
        Assert.Empty(ViewerServerTab.LockWaitTimesDrawn(Array.Empty<LockWaitTrendPoint>()));
    }

    /* The first non-zero bucket is later than the coverage: the note names the coverage. An event earlier than the coverage (a first
       collection's history) wins. A chart with only zero buckets still names the coverage. */
    [Fact]
    public void TheNote_NamesTheEarlierOfTheCoverageAndTheFirstBucketDrawn()
    {
        var later = new[] { new BlockingTrendPoint(At(2), 4) };
        var earlier = new[] { new BlockingTrendPoint(At(0, 6), 1), new BlockingTrendPoint(At(2), 4) };
        var zeros = new[] { new BlockingTrendPoint(At(1), 0) };

        Assert.Equal("Showing since 2026-09-12 00:00:00", Banner(At(2), later));
        Assert.Equal("Showing since 2026-09-12 00:00:00", Banner(At(2), zeros));
        Assert.Equal("Showing since 2026-09-10 06:00:00", Banner(At(2), earlier));
        Assert.Null(Banner(At(-5), later));
    }

    [Fact]
    public void AProbeThatThrows_CostsOnlyTheNote()
    {
        Assert.Null(Banner(null, Array.Empty<BlockingTrendPoint>(), throwing: true));
    }

    private static string? Banner(DateTime? coverage, BlockingTrendPoint[] drawn, bool throwing = false)
    {
        string? text = null;
        OnStaThread(() =>
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };
            Task<DateTime?> probe = throwing ? Task.FromException<DateTime?>(new InvalidOperationException("gone")) : Task.FromResult(coverage);
            ViewerServerTab.ShowEventDataStartAsync(banner, probe, "Blocking Trend", RangeStart, ViewerServerTab.BlockingTrendTimesDrawn(drawn)).GetAwaiter().GetResult();
            text = banner.Visibility == Visibility.Visible ? banner.Text : null;
        });
        return text;
    }

    private static void OnStaThread(Action body)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();
        if (error is not null)
        {
            throw error;
        }
    }
}
