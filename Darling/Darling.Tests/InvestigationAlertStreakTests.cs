/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6261: the streak of a recurring alert, "the Nth day in a row at this time". The pure half: which fires count toward which
/// day, and how the days become a streak. The stores' reads and both products' <c>get_alert_history</c> are in the live and Lite
/// classes; this one holds the rule they all share.
/// </summary>
public sealed class InvestigationAlertStreakTests
{
    private static readonly DateTime Anchor = new(2026, 10, 10, 2, 10, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData(new int[0], 0)]
    [InlineData(new[] { 1 }, 1)]
    [InlineData(new[] { 1, 2, 3 }, 3)]
    [InlineData(new[] { 3, 1, 2, 2, 1 }, 3)]       // order and repeats do not matter
    [InlineData(new[] { 1, 2, 4, 5 }, 2)]          // a gap ends the run; the days past it do not count
    [InlineData(new[] { 2, 3, 4 }, 0)]             // no fire the day before: no streak, however long the earlier run
    [InlineData(new[] { 1, 2, 3, 4, 5, 6, 7 }, 7)] // the whole look-back
    [InlineData(new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }, 7)] // never more than Days, whatever is passed
    [InlineData(new[] { 0, -1, 1 }, 1)]            // numbers outside 1..Days are not days
    public void Length_IsTheRunFromDayOne_AndNeverMoreThanTheLookBack(int[] days, int expected) =>
        Assert.Equal(expected, InvestigationAlertStreak.Length(days));

    [Fact]
    public void TheLookBackConstant_IsTheStreakCap()
    {
        var all = Enumerable.Range(1, InvestigationAlertRecurrence.Days + 3);
        Assert.Equal(InvestigationAlertRecurrence.Days, InvestigationAlertStreak.Length(all));
    }

    [Theory]
    [InlineData(-1 * 24 * 60, 1)]                     // the same clock time a day back
    [InlineData(-1 * 24 * 60 - 29, 1)]                // 29 minutes before it, a day back
    [InlineData(-1 * 24 * 60 + 29, 1)]                // 29 minutes after it, a day back
    [InlineData(-1 * 24 * 60 - 30, 1)]                // the window's edge counts
    [InlineData(-1 * 24 * 60 - 31, null)]             // one past it does not
    [InlineData(-3 * 24 * 60, 3)]
    [InlineData(-7 * 24 * 60, 7)]                     // exactly Days back is still inside the look-back
    [InlineData(-7 * 24 * 60 - 1, null)]              // one minute older is not
    [InlineData(-8 * 24 * 60, null)]
    [InlineData(0, null)]                             // the alert's own row
    [InlineData(5, null)]                             // after it
    [InlineData(-10, null)]                           // this incident's own rise, 10 minutes before
    [InlineData(-30, null)]                           // the window's length before the anchor is still the rise
    [InlineData(-31, null)]                           // and a fire 31 minutes before is not the same time of day
    public void DaysAgo_PlacesAFireOnTheDayItCountsFor_OrOnNone(int minutesFromAnchor, int? expected) =>
        Assert.Equal(expected, InvestigationAlertStreak.DaysAgo(Anchor, Anchor.AddMinutes(minutesFromAnchor)));

    /// <summary>A 23:50 anchor's window crosses midnight: the 00:10 fire of the anchor's own calendar date is the occurrence 24
    /// hours before (day 1, not the anchor's day 0), and the 00:10 of the date before is day 2. Counting by calendar date would
    /// put the first on the anchor's date and the streak would lose a day.</summary>
    [Fact]
    public void A_2350Anchor_CountsA_0010FireOfItsOwnDate_AsDayOne()
    {
        var anchor = new DateTime(2026, 10, 10, 23, 50, 0, DateTimeKind.Utc);
        Assert.Equal(1, InvestigationAlertStreak.DaysAgo(anchor, new DateTime(2026, 10, 10, 0, 10, 0, DateTimeKind.Utc)));
        Assert.Equal(2, InvestigationAlertStreak.DaysAgo(anchor, new DateTime(2026, 10, 9, 0, 10, 0, DateTimeKind.Utc)));
        Assert.Equal(1, InvestigationAlertStreak.DaysAgo(anchor, new DateTime(2026, 10, 9, 23, 55, 0, DateTimeKind.Utc)));

        var fires = new[]
        {
            new DateTime(2026, 10, 10, 0, 10, 0, DateTimeKind.Utc), // day 1
            new DateTime(2026, 10, 8, 23, 40, 0, DateTimeKind.Utc), // day 2
            new DateTime(2026, 10, 8, 0, 5, 0, DateTimeKind.Utc),   // day 3
        };
        Assert.Equal(3, InvestigationAlertStreak.ForAlert(anchor, fires));
    }

    /// <summary>The mirror: a 00:10 anchor counts the 23:50 fire of the evening before the day-1 occurrence (that is two calendar
    /// dates back) as day 1, and a fire at 23:50 the evening just before the anchor is its own rise, not history.</summary>
    [Fact]
    public void A_0010Anchor_CountsA_2350FireOfTheDateBeforeItsDayOneDate_AsDayOne()
    {
        var anchor = new DateTime(2026, 10, 10, 0, 10, 0, DateTimeKind.Utc);
        Assert.Equal(1, InvestigationAlertStreak.DaysAgo(anchor, new DateTime(2026, 10, 8, 23, 50, 0, DateTimeKind.Utc)));
        Assert.Equal(1, InvestigationAlertStreak.DaysAgo(anchor, new DateTime(2026, 10, 9, 0, 30, 0, DateTimeKind.Utc)));
        Assert.Null(InvestigationAlertStreak.DaysAgo(anchor, new DateTime(2026, 10, 9, 23, 50, 0, DateTimeKind.Utc)));
    }

    [Fact]
    public void ForAlert_IgnoresFiresOutsideTheWindow_AndCountsOnlyTheRunFromTheDayBefore()
    {
        var fires = new[]
        {
            Anchor,                          // its own row
            Anchor.AddMinutes(-5),           // its own rise
            Anchor.AddDays(-1),              // day 1
            Anchor.AddDays(-2).AddMinutes(20), // day 2
            Anchor.AddDays(-3).AddHours(5),  // another time of day: not day 3
            Anchor.AddDays(-4),              // day 4, past the gap
        };
        Assert.Equal(2, InvestigationAlertStreak.ForAlert(Anchor, fires));
        Assert.Equal(0, InvestigationAlertStreak.ForAlert(Anchor, Array.Empty<DateTime>()));
    }

    [Fact]
    public void TheRecurrenceRecord_PublishesTheStreak_AndSaysWhenItFilledTheLookBack()
    {
        var partial = Record(1, 2, 3);
        Assert.Equal(3, partial.StreakDays);
        Assert.False(partial.StreakAtLeastDaysWindow);

        var full = Record(Enumerable.Range(1, InvestigationAlertRecurrence.Days).ToArray());
        Assert.Equal(InvestigationAlertRecurrence.Days, full.StreakDays);
        Assert.True(full.StreakAtLeastDaysWindow);

        var none = Record();
        Assert.Equal(0, none.StreakDays);
        Assert.False(none.StreakAtLeastDaysWindow);
    }

    private static InvestigationAlertRecurrence Record(params int[] days) =>
        new("Blocking", Anchor, days.Length, 0, Array.Empty<DateOnly>(), Array.Empty<DateOnly>(), days);
}
