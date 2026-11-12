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
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7644: <see cref="BaselineBand.HourStep.At"/> looks a point up by binary search, for the marker loops and the crosshair over a long
/// custom range. It has to answer exactly what the linear walk it replaced answered: a point at a step's start is in that step, a point
/// at its end is in the next one (or none), and a point in a gap or outside the list is in none.
/// </summary>
public sealed class OverviewHourStepLookupTests
{
    /// <summary>The walk the lookup replaced: the first step, in list order, that holds the point.</summary>
    private static BaselineBand.HourStep? Linear(IReadOnlyList<BaselineBand.HourStep> steps, double x)
    {
        foreach (var step in steps)
        {
            if (x >= step.FromX && x < step.ToX)
                return step;
        }

        return null;
    }

    /// <summary>720 hourly steps over 30 days, with a few hours left out (an hour with no band) and one two-hour step, so there are gaps.</summary>
    private static List<BaselineBand.HourStep> LongList()
    {
        var t0 = new DateTime(2026, 10, 1, 0, 0, 0, DateTimeKind.Unspecified).ToOADate();
        var hour = 1.0 / 24;
        var steps = new List<BaselineBand.HourStep>();
        for (var i = 0; i < 720; i++)
        {
            if (i % 97 == 5)
                continue;
            var to = i == 300 ? t0 + ((i + 2) * hour) : t0 + ((i + 1) * hour);
            if (i == 301)
                continue;
            steps.Add(new BaselineBand.HourStep(t0 + (i * hour), to, i, 0, i + 1));
        }

        return steps;
    }

    [Fact]
    public void OverALongStepList_TheLookupAnswersWhatTheLinearWalkAnswered_AtBoundariesGapsAndOutsideTheList()
    {
        var steps = LongList();
        var points = new List<double>
        {
            double.NaN, double.NegativeInfinity, double.PositiveInfinity,
            steps[0].FromX - 1, steps[0].FromX - 1e-9, steps[^1].ToX, steps[^1].ToX + 1e-9, steps[^1].ToX + 5,
        };
        foreach (var step in steps)
        {
            var width = step.ToX - step.FromX;
            points.Add(step.FromX);
            points.Add(step.FromX + (width / 2));
            points.Add(step.ToX);
            points.Add(step.ToX - 1e-9);
            points.Add(step.FromX - 1e-9);
        }

        foreach (var x in points)
        {
            Assert.Same(Linear(steps, x), BaselineBand.HourStep.At(steps, x));
        }

        Assert.True(steps.Count > 600);
        // The gaps are real: some of the points above land in none of the steps.
        Assert.Contains(points, x => Linear(steps, x) is null && !double.IsNaN(x) && x > steps[0].FromX && x < steps[^1].ToX);
    }

    [Fact]
    public void AnEmptyOrMissingList_HasNoStep()
    {
        Assert.Null(BaselineBand.HourStep.At(null, 1));
        Assert.Null(BaselineBand.HourStep.At(Array.Empty<BaselineBand.HourStep>(), 1));
    }

    [Fact]
    public void AStepIncludesItsStart_AndExcludesItsEnd()
    {
        IReadOnlyList<BaselineBand.HourStep> steps = [new(1, 2, 5, 0, 10), new(2, 3, 6, 0, 12)];

        Assert.Equal(5, BaselineBand.HourStep.At(steps, 1)!.Mean);
        Assert.Equal(6, BaselineBand.HourStep.At(steps, 2)!.Mean);
        Assert.Null(BaselineBand.HourStep.At(steps, 3));
        Assert.Null(BaselineBand.HourStep.At(steps, 0.999));
    }
}
