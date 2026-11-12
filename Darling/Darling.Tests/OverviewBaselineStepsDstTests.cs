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

    /// <summary>The entry the way a compute builds it: the zone id and offset the server read, and a clock resolved over the COMPUTE
    /// window, which is not the window the page draws.</summary>
    private static PgBaselineProvider.CachedBaseline Entry(DateTime computeStart, DateTime computeEnd)
        => new()
        {
            Buckets = EveryHourOfWeek(),
            TimeZoneId = Zone,
            UtcOffsetMinutes = -300,
            Clock = new BaselineLocalClock().Resolve(Zone, -300, computeStart, computeEnd),
        };

    private static List<(DateTime From, DateTime To, double Mean)> Steps(PgBaselineProvider.CachedBaseline entry, DateTime start, DateTime end)
        => PgBaselineProvider.StepsFor(entry, start, end, new BaselineLocalClock())
            .Select(s => (s.FromUtc, s.ToUtc, s.Bucket.Mean)).ToList();

    private static DateTime Utc(string text)
        => DateTime.SpecifyKind(DateTime.Parse(text, CultureInfo.InvariantCulture), DateTimeKind.Unspecified);

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
        var entry = Entry(start.AddDays(-30), end);
        var clock = entry.Clock;
        Assert.True(clock.HasTransition, name);

        var steps = Steps(entry, start, end);

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

    private const string FallBackMeans = "167,0,1,1,2,3,4,5,6";

    /// <summary>
    /// A daily-cache arm computes up to today's UTC midnight, so a 24 hour window drawn at 12:00 UTC runs 12 hours past the compute window's end.
    /// New York falls back at 06:00 UTC on 2026-11-01, after that end: the compute clock is a fixed EDT offset with no transition. The walk
    /// has to follow the zone over the drawn window, or every step after 06:00 UTC lands on the hour before the one it shows.
    /// </summary>
    [Fact]
    public void ADailyArmWindowThatRunsPastTheComputeEnd_StillCutsAtTheChangeAfterIt()
    {
        var computeEnd = Utc("2026-11-01T00:00:00");
        var entry = Entry(computeEnd.AddDays(-30), computeEnd);
        Assert.False(entry.Clock.HasTransition);

        var steps = Steps(entry, Utc("2026-10-31T12:00:00"), Utc("2026-11-01T12:00:00"));

        // Saturday 08:00 local is bucket 152; the walk reaches Sunday 00:00 at 04:00 UTC, and local 01:00 happens twice, EDT then EST.
        Assert.Equal("152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,0,1,1,2,3,4,5,6",
            string.Join(",", steps.Select(s => s.Mean.ToString(CultureInfo.InvariantCulture))));
        Assert.Contains(steps, step => step.From == Utc("2026-11-01T06:00:00"));
        Assert.All(steps, step => Assert.Equal(TimeSpan.FromHours(1), step.To - step.From));
    }

    /// <summary>
    /// A custom range older than 30 days is drawn before the compute window starts (now minus 30 days to now). The daylight-saving change
    /// falls in the range and not in the compute window, so the compute clock is a fixed EST offset. The walk has to resolve the range's own clock.
    /// </summary>
    [Fact]
    public void ACustomRangeOlderThanTheComputeWindow_StillCutsAtItsOwnChange()
    {
        var now = Utc("2026-12-15T09:00:00");
        var entry = Entry(now.AddDays(-30), now);
        Assert.False(entry.Clock.HasTransition);

        var steps = Steps(entry, Utc("2026-11-01T03:00:00"), Utc("2026-11-01T12:00:00"));

        Assert.Equal(FallBackMeans, string.Join(",", steps.Select(s => s.Mean.ToString(CultureInfo.InvariantCulture))));
        Assert.Contains(steps, step => step.From == Utc("2026-11-01T06:00:00"));
        Assert.All(steps, step => Assert.Equal(TimeSpan.FromHours(1), step.To - step.From));
    }
}
