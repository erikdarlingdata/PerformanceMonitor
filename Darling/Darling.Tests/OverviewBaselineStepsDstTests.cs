/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7644: the Overview band's local-hour walk across a daylight-saving change. The zone changes its offset inside the window, so the
/// walk has to cut a step where the clock's offset changes and keep every later step on its own local hour. Each hour-of-week bucket
/// holds its own number (day of week times 24 plus the hour), so a step that picked the wrong local hour reads as a wrong mean.
/// </summary>
public sealed class OverviewBaselineStepsDstTests
{
    private const string Zone = "America/New_York";

    private static Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket> EveryHourOfWeek()
    {
        var buckets = new Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>();
        for (var dow = 0; dow < 7; dow++)
        {
            for (var hour = 0; hour < 24; hour++)
            {
                buckets[(hour, dow)] = new BaselineBucket
                {
                    HourOfDay = hour, DayOfWeek = dow, Mean = (dow * 24) + hour, StdDev = 1, SampleCount = 500, DistinctDays = 20, Tier = BaselineTier.Full,
                };
            }
        }

        return buckets;
    }

    private static List<(DateTime From, DateTime To, double Mean)> Steps(LocalClockWindow clock, DateTime start, DateTime end)
        => PgBaselineProvider.StepsFor(new PgBaselineProvider.CachedBaseline { Buckets = EveryHourOfWeek(), Clock = clock }, start, end)
            .Select(s => (s.FromUtc, s.ToUtc, s.Bucket.Mean)).ToList();

    /* Saturday 23:00 local is bucket 167, Sunday 00:00 is bucket 0, and so on. */
    public static TheoryData<string, string, string, string> Windows => new()
    {
        // Fall back: 02:00 EDT becomes 01:00 EST at 06:00 UTC, so local hour 01 happens twice, in two different UTC hours.
        { "2026-11-01T03:00:00", "2026-11-01T12:00:00", "fall back", "167,0,1,1,2,3,4,5,6" },
        // The same cut with a window that starts on the half hour: the first step starts at its own hour's start, before the window start.
        { "2026-11-01T03:30:00", "2026-11-01T11:30:00", "fall back, half hour", "167,0,1,1,2,3,4,5,6" },
        // Spring forward: 02:00 EST becomes 03:00 EDT at 07:00 UTC, so local hour 02 never happens.
        { "2026-03-08T04:00:00", "2026-03-08T11:00:00", "spring forward", "167,0,1,3,4,5,6" },
    };

    [Theory]
    [MemberData(nameof(Windows))]
    public void AZoneThatChangesOffsetInsideTheWindow_StepsOncePerLocalHour_WithNoGapAndNoRepeat(string startText, string endText, string name, string expectedMeans)
    {
        var start = DateTime.SpecifyKind(DateTime.Parse(startText, CultureInfo.InvariantCulture), DateTimeKind.Unspecified);
        var end = DateTime.SpecifyKind(DateTime.Parse(endText, CultureInfo.InvariantCulture), DateTimeKind.Unspecified);
        var clock = new BaselineLocalClock().Resolve(Zone, -300, start.AddDays(-30), end);
        Assert.True(clock.HasTransition, name);

        var steps = Steps(clock, start, end);

        // One step per local hour, each on the bucket its own local hour and day name.
        Assert.Equal(expectedMeans, string.Join(",", steps.Select(s => s.Mean.ToString(CultureInfo.InvariantCulture))));
        // The first step starts at the start of its local hour, which is on the hour in UTC for this zone.
        Assert.Equal(new DateTime(start.Year, start.Month, start.Day, start.Hour, 0, 0), steps[0].From);
        // The last step reaches the window's end, and does not start after it.
        Assert.True(steps[^1].To >= end);
        Assert.True(steps[^1].From < end);
        // Contiguous: each step ends where the next begins, so none is missing and none is repeated.
        for (var i = 1; i < steps.Count; i++)
        {
            Assert.Equal(steps[i - 1].To, steps[i].From);
        }

        // Every step is one real hour: the cut falls on the hour, so the offset change does not make a short or a double step.
        Assert.All(steps, step => Assert.Equal(TimeSpan.FromHours(1), step.To - step.From));
        // The offset change lands on a step boundary in UTC.
        Assert.Contains(steps, step => step.From == clock.TransitionAtUtc);
    }
}
