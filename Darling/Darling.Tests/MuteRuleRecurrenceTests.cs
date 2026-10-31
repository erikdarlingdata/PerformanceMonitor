/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7040: a mute rule can repeat on a schedule (days, a daily start and end, an IANA zone). Pure evaluation: a same-day
/// window, a window that crosses midnight and belongs to the day it starts, the day boundary, a daylight-saving change in a
/// real zone, and the schedule ANDed with the rule's start, expiry and match fields.
/// </summary>
public sealed class MuteRuleRecurrenceTests
{
    private static DateTime Utc(int month, int day, int hour, int minute = 0, int year = 2026) =>
        new(year, month, day, hour, minute, 0, DateTimeKind.Utc);

    private static int Days(params DayOfWeek[] days)
    {
        var mask = 0;
        foreach (var d in days) mask |= MuteRecurrence.Bit(d);
        return mask;
    }

    private static MuteRule Rule(int? days, string start, string end, string zone = "UTC")
    {
        MuteRecurrence.TryParseTime(start, out var s);
        MuteRecurrence.TryParseTime(end, out var e);
        return new MuteRule { RecurDays = days, RecurStartTime = s, RecurEndTime = e, RecurTimeZone = zone };
    }

    // 2026-11-02 is a Monday, 11-06 a Friday, 11-07 a Saturday.
    [Theory]
    [InlineData(4, 10, 0, true)]   // Wednesday inside
    [InlineData(4, 9, 0, true)]    // start is inclusive
    [InlineData(4, 16, 59, true)]
    [InlineData(4, 17, 0, false)]  // end is exclusive
    [InlineData(4, 8, 59, false)]
    [InlineData(7, 10, 0, false)]  // Saturday is not listed
    public void SameDayWindow_IsInsideOnListedDaysBetweenStartAndEnd(int day, int hour, int minute, bool expected)
    {
        var rule = Rule(Days(DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday), "09:00", "17:00");
        Assert.Equal(expected, rule.IsInWindowAt(Utc(11, day, hour, minute)));
    }

    [Theory]
    [InlineData(6, 22, 0, true)]   // Friday 22:00 opens
    [InlineData(6, 23, 59, true)]
    [InlineData(7, 0, 0, true)]    // Saturday after midnight still belongs to Friday's window
    [InlineData(7, 1, 59, true)]
    [InlineData(7, 2, 0, false)]   // end exclusive
    [InlineData(7, 22, 0, false)]  // Saturday night: Saturday is not listed
    [InlineData(6, 1, 0, false)]   // Friday early morning would belong to Thursday, not listed
    [InlineData(6, 21, 59, false)]
    public void WindowCrossingMidnight_BelongsToTheDayItStarts(int day, int hour, int minute, bool expected)
    {
        var rule = Rule(Days(DayOfWeek.Friday), "22:00", "02:00");
        Assert.Equal(expected, rule.IsInWindowAt(Utc(11, day, hour, minute)));
    }

    [Fact]
    public void WindowCrossingMidnight_OnTheFirstDayOfTheWeek_WrapsToSunday()
    {
        // Saturday 22:00 to 02:00 runs into Sunday morning: yesterday of Sunday is Saturday (the mask wraps).
        var rule = Rule(Days(DayOfWeek.Saturday), "22:00", "02:00");
        Assert.True(rule.IsInWindowAt(Utc(11, 8, 1, 0)));   // Sunday 01:00
        Assert.False(rule.IsInWindowAt(Utc(11, 8, 2, 0)));  // Sunday 02:00
        Assert.False(rule.IsInWindowAt(Utc(11, 8, 23, 0))); // Sunday 23:00
    }

    [Fact]
    public void NoDays_MeansEveryDay()
    {
        var rule = Rule(null, "22:00", "02:00");
        Assert.True(rule.IsInWindowAt(Utc(11, 3, 23, 0)));
        Assert.True(rule.IsInWindowAt(Utc(11, 4, 1, 0)));
        Assert.False(rule.IsInWindowAt(Utc(11, 4, 12, 0)));
    }

    [Fact]
    public void RuleWithNoSchedule_IsAlwaysInWindow()
    {
        Assert.True(new MuteRule().IsInWindowAt(Utc(11, 4, 12, 0)));
        Assert.False(new MuteRule().IsRecurring);
        Assert.Null(new MuteRule().NextWindowStartUtc(Utc(11, 4, 12, 0)));
    }

    [Theory]
    [InlineData(10, 31, 3, 0, true)]    // Friday 23:00 EDT
    [InlineData(10, 31, 5, 59, true)]   // Saturday 01:59 EDT
    [InlineData(10, 31, 6, 0, false)]   // Saturday 02:00 EDT
    [InlineData(10, 31, 1, 59, false)]  // Friday 21:59 EDT
    public void TheWindowIsReadInTheRulesZone_NotInUtc(int month, int day, int hour, int minute, bool expected)
    {
        var rule = Rule(null, "22:00", "02:00", "America/New_York");
        Assert.Equal(expected, rule.IsInWindowAt(Utc(month, day, hour, minute)));
    }

    [Fact]
    public void DaylightSavingEnds_TheWallClockWindowCoversTheRepeatedHourTwice()
    {
        // America/New_York falls back on 2026-11-01 at 02:00 EDT (06:00Z): 01:00 to 02:00 happens twice.
        var rule = Rule(null, "01:00", "02:00", "America/New_York");
        Assert.True(rule.IsInWindowAt(Utc(11, 1, 5, 30)));   // 01:30 EDT
        Assert.True(rule.IsInWindowAt(Utc(11, 1, 6, 30)));   // 01:30 EST, the second pass
        Assert.False(rule.IsInWindowAt(Utc(11, 1, 7, 30)));  // 02:30 EST
        Assert.False(rule.IsInWindowAt(Utc(11, 1, 4, 30)));  // 00:30 EDT
    }

    [Fact]
    public void DaylightSavingStarts_TheSkippedHourIsAbsentAndLaterWindowsStillHold()
    {
        // America/New_York springs forward on 2026-03-08 at 02:00 EST (07:00Z): 02:00-03:00 never exists that day.
        var skipped = Rule(null, "02:00", "03:00", "America/New_York");
        Assert.False(skipped.IsInWindowAt(Utc(3, 8, 6, 59)));  // 01:59 EST
        Assert.False(skipped.IsInWindowAt(Utc(3, 8, 7, 0)));   // 03:00 EDT
        Assert.True(skipped.IsInWindowAt(Utc(3, 9, 6, 30)));   // next day 02:30 EDT

        var overnight = Rule(null, "22:00", "04:00", "America/New_York");
        Assert.True(overnight.IsInWindowAt(Utc(3, 8, 6, 0)));  // 01:00 EST, still the night window
        Assert.True(overnight.IsInWindowAt(Utc(3, 8, 7, 30))); // 03:30 EDT
        Assert.False(overnight.IsInWindowAt(Utc(3, 8, 8, 0))); // 04:00 EDT
    }

    [Fact]
    public void NextWindowStart_LandsOnTheNextListedDay_InUtc()
    {
        var rule = Rule(Days(DayOfWeek.Friday), "22:00", "02:00");
        // Saturday 2026-10-31 noon: the next Friday window opens 2026-11-06 22:00Z.
        Assert.Equal(Utc(11, 6, 22, 0), rule.NextWindowStartUtc(Utc(10, 31, 12, 0)));
        // Inside the window now: the NEXT window, a week later.
        Assert.Equal(Utc(11, 13, 22, 0), rule.NextWindowStartUtc(Utc(11, 6, 23, 0)));
    }

    [Fact]
    public void NextWindowStart_ReadsTheStartInTheRulesZone_AcrossADaylightSavingChange()
    {
        var rule = Rule(null, "02:00", "03:30", "America/New_York");
        // 03-08 02:00 does not exist; the window starts when the skipped hour ends, 03:00 EDT = 07:00Z.
        Assert.Equal(Utc(3, 8, 7, 0), rule.NextWindowStartUtc(Utc(3, 8, 0, 0)));
        // A winter start: 02:00 EST = 07:00Z.
        Assert.Equal(Utc(11, 2, 7, 0), rule.NextWindowStartUtc(Utc(11, 2, 0, 0)));
    }

    [Fact]
    public void NextWindowStart_HonoursTheRulesSpan()
    {
        var rule = Rule(null, "22:00", "02:00");
        rule.ExpiresAtUtc = Utc(11, 3, 12, 0);
        Assert.Equal(Utc(11, 2, 22, 0), rule.NextWindowStartUtc(Utc(11, 2, 12, 0)));
        Assert.Null(rule.NextWindowStartUtc(Utc(11, 2, 23, 0)));   // next is 11-03 22:00Z, past the expiry
        rule.ExpiresAtUtc = null;
        rule.StartsAtUtc = Utc(11, 10, 3, 0);
        /* The rule begins next week: the next window is the first one after it begins, not null. */
        Assert.Equal(Utc(11, 10, 22, 0), rule.NextWindowStartUtc(Utc(11, 2, 12, 0)));
        rule.ExpiresAtUtc = Utc(11, 10, 12, 0);
        Assert.Null(rule.NextWindowStartUtc(Utc(11, 2, 12, 0)));   // the span ends before any window opens
        rule.ExpiresAtUtc = null;
        rule.StartsAtUtc = Utc(11, 1, 0, 0);
        Assert.Equal(Utc(11, 2, 22, 0), rule.NextWindowStartUtc(Utc(11, 2, 12, 0)));   // already started: unchanged
    }

    [Fact]
    public void NextWindowStart_ForAFutureStartInZone_IsTheFirstWindowAfterTheStartInThatZone()
    {
        // A daily 22:00-02:00 rule in New York that starts Tuesday 2026-11-10 (UTC-5): reports that Tuesday 22:00 local = Wednesday 03:00Z.
        var rule = Rule(null, "22:00", "02:00", "America/New_York");
        rule.StartsAtUtc = Utc(11, 10, 8, 0);   // Tuesday 03:00 local, after the window that opened Monday night has closed
        Assert.Equal(Utc(11, 11, 3, 0), rule.NextWindowStartUtc(Utc(11, 2, 12, 0)));
    }

    [Fact]
    public void NextWindowStart_WhenAWindowIsOpenAtTheStart_IsTheStartItself()
    {
        var rule = Rule(null, "22:00", "02:00");
        rule.StartsAtUtc = Utc(11, 10, 23, 30);   // inside the 22:00-02:00 window that opened before the rule began
        Assert.Equal(Utc(11, 10, 23, 30), rule.NextWindowStartUtc(Utc(11, 2, 12, 0)));
        rule.StartsAtUtc = Utc(11, 10, 22, 0);    // a window opening exactly at the start
        Assert.Equal(Utc(11, 10, 22, 0), rule.NextWindowStartUtc(Utc(11, 2, 12, 0)));
    }

    [Fact]
    public void AWindowsIdWithNoIanaEquivalent_IsRefusedOnWrite_AndReadsAsOutsideTheWindow()
    {
        // Windows ids the host can find but .NET has no IANA mapping for (Windows-only); elsewhere the id is simply unknown. Either way: refused.
        MuteRecurrence.TryParseTime("00:00", out var s);
        MuteRecurrence.TryParseTime("23:00", out var e);
        foreach (var id in new[] { "Mid-Atlantic Standard Time", "British Columbia Standard Time" })
        {
            Assert.False(MuteRecurrence.TryNormalizeZone(id, out _), id);
            var zone = (string?)id;
            Assert.False(MuteRecurrence.TryNormalize(null, s, e, ref zone, out var error), id);
            Assert.Contains(id, error);
            var rule = new MuteRule { RecurStartTime = s, RecurEndTime = e, RecurTimeZone = id };
            Assert.False(rule.IsInWindowAt(Utc(11, 4, 12, 0)));
        }
    }

    [Fact]
    public void Matching_NeedsTheWindowAndTheSpanAndTheMatchFields()
    {
        var ctx = new AlertMuteContext { ServerName = "srv", MetricName = "Long-Running Query" };
        var inWindow = Utc(11, 3, 23, 0);   // Tuesday 23:00Z
        var outOfWindow = Utc(11, 3, 12, 0);

        var rule = Rule(null, "22:00", "02:00");
        rule.ServerName = "srv";
        rule.MetricName = "Long-Running Query";
        Assert.True(rule.MatchesAt(ctx, inWindow));
        Assert.False(rule.MatchesAt(ctx, outOfWindow));

        // The match fields still apply inside the window.
        Assert.False(rule.MatchesAt(new AlertMuteContext { ServerName = "other", MetricName = "Long-Running Query" }, inWindow));
        Assert.False(rule.MatchesAt(new AlertMuteContext { ServerName = "srv", MetricName = "Blocking" }, inWindow));

        // The span: expired, not yet started, disabled.
        rule.ExpiresAtUtc = Utc(11, 3, 22, 30);
        Assert.False(rule.MatchesAt(ctx, inWindow));
        rule.ExpiresAtUtc = Utc(11, 4, 0, 0);
        Assert.True(rule.MatchesAt(ctx, inWindow));
        rule.StartsAtUtc = Utc(11, 3, 23, 30);
        Assert.False(rule.MatchesAt(ctx, inWindow));
        rule.StartsAtUtc = null;
        rule.Enabled = false;
        Assert.False(rule.MatchesAt(ctx, inWindow));
    }

    [Fact]
    public void AServerOnlyRule_WithASchedule_MutesEveryAlertOnThatServerInsideTheWindow()
    {
        var rule = Rule(Days(DayOfWeek.Saturday, DayOfWeek.Sunday), "00:00", "23:59");
        rule.ServerName = "srv";
        var any = new AlertMuteContext { ServerName = "srv", MetricName = "Anything" };
        Assert.True(rule.MatchesAt(any, Utc(11, 7, 10, 0)));
        Assert.False(rule.MatchesAt(any, Utc(11, 4, 10, 0)));
    }

    [Fact]
    public void Clone_CarriesTheSchedule()
    {
        var rule = Rule(Days(DayOfWeek.Monday), "22:00", "02:00", "America/New_York");
        var copy = rule.Clone();
        Assert.Equal(rule.RecurDays, copy.RecurDays);
        Assert.Equal(rule.RecurStartTime, copy.RecurStartTime);
        Assert.Equal(rule.RecurEndTime, copy.RecurEndTime);
        Assert.Equal(rule.RecurTimeZone, copy.RecurTimeZone);
    }

    [Fact]
    public void ParseDays_TakesShortAndFullNames_AnyCase_AndRefusesAnUnknownOne()
    {
        Assert.True(MuteRecurrence.TryParseDays(["mon", "FRIDAY", " Sat "], out var mask, out _));
        Assert.Equal(Days(DayOfWeek.Monday, DayOfWeek.Friday, DayOfWeek.Saturday), mask);
        Assert.True(MuteRecurrence.TryParseDays([], out var none, out _));
        Assert.Null(none);
        Assert.False(MuteRecurrence.TryParseDays(["mon", "funday"], out _, out var error));
        Assert.Contains("funday", error);
        Assert.Equal(["Mon", "Fri", "Sat"], MuteRecurrence.DayNames(Days(DayOfWeek.Monday, DayOfWeek.Friday, DayOfWeek.Saturday)));
    }

    [Fact]
    public void Validation_RefusesHalfASchedule_EqualTimes_AndDaysOrZoneWithoutTimes()
    {
        MuteRecurrence.TryParseTime("22:00", out var ten);
        MuteRecurrence.TryParseTime("02:00", out var two);

        var zone = (string?)null;
        Assert.False(MuteRecurrence.TryNormalize(null, ten, null, ref zone, out var e1));
        Assert.Contains("both", e1);
        Assert.False(MuteRecurrence.TryNormalize(null, null, two, ref zone, out _));
        Assert.False(MuteRecurrence.TryNormalize(null, ten, ten, ref zone, out var e2));
        Assert.Contains("differ", e2);
        Assert.False(MuteRecurrence.TryNormalize(Days(DayOfWeek.Monday), null, null, ref zone, out _));
        var z = "UTC";
        Assert.False(MuteRecurrence.TryNormalize(null, null, null, ref z, out _));
        Assert.False(MuteRecurrence.TryNormalize(0, ten, two, ref zone, out _));
    }

    [Fact]
    public void Validation_StoresAnIanaZone_ConvertsAWindowsId_RefusesAnUnknownOne_AndDefaultsToTheHostZone()
    {
        MuteRecurrence.TryParseTime("22:00", out var s);
        MuteRecurrence.TryParseTime("02:00", out var e);

        var windows = (string?)"Eastern Standard Time";
        Assert.True(MuteRecurrence.TryNormalize(null, s, e, ref windows, out var err));
        Assert.Null(err);
        Assert.Equal("America/New_York", windows);

        var iana = (string?)"America/Chicago";
        Assert.True(MuteRecurrence.TryNormalize(null, s, e, ref iana, out _));
        Assert.Equal("America/Chicago", iana);

        var unknown = (string?)"Mars/Olympus_Mons";
        Assert.False(MuteRecurrence.TryNormalize(null, s, e, ref unknown, out var zoneError));
        Assert.Contains("Mars/Olympus_Mons", zoneError);

        var omitted = (string?)null;
        Assert.True(MuteRecurrence.TryNormalize(null, s, e, ref omitted, out _));
        Assert.Equal(MuteRecurrence.HostZoneIana(), omitted);
        Assert.False(string.IsNullOrWhiteSpace(omitted));
        // The host zone it stored is one this host can evaluate.
        Assert.True(MuteRecurrence.TryNormalizeZone(omitted, out _));
    }

    [Fact]
    public void TryNormalizeRecurrence_NormalisesTheRulesZoneInPlace()
    {
        var rule = Rule(null, "22:00", "02:00", "Pacific Standard Time");
        Assert.True(rule.TryNormalizeRecurrence(out _));
        Assert.Equal("America/Los_Angeles", rule.RecurTimeZone);
        Assert.Equal("daily 22:00-02:00 America/Los_Angeles", rule.ScheduleText);
    }

    [Fact]
    public void ARuleWhoseStoredZoneTheHostCannotResolve_MutesNothing_InsteadOfThrowing()
    {
        var rule = Rule(null, "00:00", "23:00", "Nowhere/Land");
        var ctx = new AlertMuteContext { ServerName = "s", MetricName = "m" };
        Assert.False(rule.MatchesAt(ctx, Utc(11, 3, 12, 0)));
    }
}
