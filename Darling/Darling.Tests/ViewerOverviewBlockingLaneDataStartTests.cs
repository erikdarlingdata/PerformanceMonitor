/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// What the Overview's blocking chart names as its data start (#4966): the later of its two series' starts, each the earlier of the
/// series' coverage and its earliest bar drawn. Pure logic, so no store and no WPF object is needed.
/// </summary>
public sealed class ViewerOverviewBlockingLaneDataStartTests
{
    private static readonly DateTime Day = new(2026, 9, 10, 0, 0, 0, DateTimeKind.Utc);

    private static Task<DateTime?> Answer(DateTime? start) => Task.FromResult(start);

    private static readonly DateTime[] NoBars = [];

    [Fact]
    public async Task TwoStarts_NameTheLaterOne_WhicheverSeriesHasIt()
    {
        Assert.Equal(Day.AddDays(2), await ViewerBlockingLaneDataStart.ChooseAsync(Answer(Day.AddDays(2)), Answer(Day), NoBars, NoBars));
        Assert.Equal(Day.AddDays(2), await ViewerBlockingLaneDataStart.ChooseAsync(Answer(Day), Answer(Day.AddDays(2)), NoBars, NoBars));
    }

    [Fact]
    public async Task OneAnswer_IsNamed_AndNeitherNamesNothing()
    {
        Assert.Equal(Day.AddDays(1), await ViewerBlockingLaneDataStart.ChooseAsync(Answer(Day.AddDays(1)), Answer(null), NoBars, NoBars));
        Assert.Equal(Day.AddDays(1), await ViewerBlockingLaneDataStart.ChooseAsync(Answer(null), Answer(Day.AddDays(1)), NoBars, NoBars));
        Assert.Null(await ViewerBlockingLaneDataStart.ChooseAsync(Answer(null), Answer(null), NoBars, NoBars));
    }

    /* The event-time rule: a bar earlier than the series' coverage moves that series' start back; the chart still names the later series. */
    [Fact]
    public async Task ABarBeforeTheCoverage_MovesThatSeriesStartBack()
    {
        Assert.Equal(Day.AddDays(2), await ViewerBlockingLaneDataStart.ChooseAsync(
            Answer(Day.AddDays(2)), Answer(Day.AddDays(3)), NoBars, [Day.AddDays(1), Day.AddDays(4)]));
        Assert.Equal(Day.AddDays(1), await ViewerBlockingLaneDataStart.ChooseAsync(
            Answer(null), Answer(Day.AddDays(3)), [Day.AddDays(1)], [Day.AddDays(1)]));
        /* No coverage anywhere but a bar drawn: the bar names the start. */
        Assert.Equal(Day.AddHours(5), await ViewerBlockingLaneDataStart.ChooseAsync(Answer(null), Answer(null), [Day.AddHours(5), Day.AddHours(9)], NoBars));
    }

    /* A probe that throws costs only its series: the other still answers, and two that throw name nothing, even with bars drawn. */
    [Fact]
    public async Task AProbeThatThrows_DropsItsSeries_AndNeverThrowsOut()
    {
        var failed = Task.FromException<DateTime?>(new InvalidOperationException("the store went away"));
        Assert.Equal(Day.AddDays(2), await ViewerBlockingLaneDataStart.ChooseAsync(failed, Answer(Day.AddDays(2)), [Day], NoBars));
        Assert.Null(await ViewerBlockingLaneDataStart.ChooseAsync(failed, Task.FromException<DateTime?>(new TimeoutException()), [Day], [Day]));
    }
}
