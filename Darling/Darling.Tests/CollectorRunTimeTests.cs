/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4938: the shared rules for a daily collector's run time, <see cref="CollectorRunTime"/>. The run time is the
/// monitored server's own wall clock, so each test hands the rules a local-to-UTC conversion the way an app
/// does: <see cref="ServerClock.ToUtc"/> for a known clock, <see cref="CollectorRunTime.LocalIsUtc"/> while the
/// clock is not known yet. US Eastern springs forward on 2026-03-08 and falls back on 2026-11-01. A server id
/// whose <c>(uint)id % 3600</c> is 0 puts the slot exactly on the run time; 1800 adds 30 minutes and 3599 adds
/// 59 minutes 59 seconds.
/// </summary>
public sealed class CollectorRunTimeTests
{
    private const int NoSpreadId = 0;
    private const int HalfHourSpreadId = 1800;
    private const int LongSpreadId = 3599;
    private const int TwoAm = 120;
    private const int Daily = 1440;
    private const string EasternWindowsId = "Eastern Standard Time";
    private const string EasternIanaId = "America/New_York";

    private static DateTime Utc(int year, int month, int day, int hour = 0, int minute = 0, int second = 0) =>
        new(year, month, day, hour, minute, second, DateTimeKind.Utc);

    private static DateTime NextDue(DateTime now, DateTime? lastRun, int runAt, int interval, int serverId,
        Func<DateTime, DateTime> toUtc) =>
        CollectorRunTime.NextDue(now, lastRun, runAt, interval, serverId, toUtc);

    private static DateTime UtcClockNextDue(DateTime now, DateTime? lastRun, int runAt = TwoAm, int interval = Daily,
        int serverId = NoSpreadId) =>
        CollectorRunTime.NextDue(now, lastRun, runAt, interval, serverId, CollectorRunTime.LocalIsUtc);

    /* ---- before and after the slot, with no history ---- */

    [Fact]
    public void NeverRun_BeforeTodaysSlot_WaitsForTodaysSlot()
    {
        Assert.Equal(Utc(2026, 10, 2, 2, 0), UtcClockNextDue(Utc(2026, 10, 2, 1, 0), null));
        Assert.Equal(Utc(2026, 10, 2, 2, 0), UtcClockNextDue(Utc(2026, 10, 2, 2, 0).AddTicks(-1), null));
        Assert.Equal(Utc(2026, 10, 2, 2, 0), UtcClockNextDue(Utc(2026, 10, 2, 0, 0), null));
    }

    [Fact]
    public void NeverRun_AfterTodaysGrace_WaitsForTomorrowsSlot_NotNowAndNotAJitteredNow()
    {
        Assert.Equal(Utc(2026, 10, 3, 2, 0), UtcClockNextDue(Utc(2026, 10, 2, 3, 0).AddTicks(1), null));
        Assert.Equal(Utc(2026, 10, 3, 2, 0), UtcClockNextDue(Utc(2026, 10, 2, 12, 0), null));
        Assert.Equal(Utc(2026, 10, 3, 2, 0), UtcClockNextDue(Utc(2026, 10, 2, 23, 59, 59), null));
    }

    [Theory]
    [InlineData(2, 0, 0)]
    [InlineData(2, 30, 0)]
    [InlineData(2, 59, 59)]
    [InlineData(3, 0, 0)]
    public void NeverRun_InsideTheGrace_RunsNow(int hour, int minute, int second)
    {
        var now = Utc(2026, 10, 2, hour, minute, second);

        Assert.Equal(now, UtcClockNextDue(now, null));
    }

    [Fact]
    public void TheGrace_IsSixtyMinutes_AndClosedAtBothEnds()
    {
        Assert.Equal(TimeSpan.FromMinutes(60), CollectorRunTime.Grace);

        var slot = Utc(2026, 10, 2, 2, 0);
        Assert.Equal(slot, UtcClockNextDue(slot, null));
        Assert.Equal(slot + CollectorRunTime.Grace, UtcClockNextDue(slot + CollectorRunTime.Grace, null));
        Assert.Equal(Utc(2026, 10, 3, 2, 0), UtcClockNextDue(slot + CollectorRunTime.Grace + TimeSpan.FromTicks(1), null));
    }

    [Fact]
    public void ADateMinValueLastRun_IsReadAsNeverRun()
    {
        var now = Utc(2026, 10, 2, 12, 0);

        Assert.Equal(UtcClockNextDue(now, null), UtcClockNextDue(now, DateTime.MinValue));
    }

    /* ---- the spread ---- */

    [Fact]
    public void TheSpread_IsFixedPerId_AndUnderSixtyMinutes()
    {
        int[] ids = { 0, 1, 59, 1800, 3599, 3600, 3601, 123456789, -1, -61, -123456789, int.MaxValue, int.MinValue };
        foreach (var id in ids)
        {
            var spread = CollectorRunTime.Spread(id);

            Assert.Equal(TimeSpan.FromSeconds((uint)id % 3600), spread);
            Assert.Equal(spread, CollectorRunTime.Spread(id));
            Assert.True(spread >= TimeSpan.Zero, $"id {id}: the spread must not pull the slot before the run time");
            Assert.True(spread < TimeSpan.FromMinutes(60), $"id {id}: the spread must stay under 60 minutes");
        }
    }

    [Fact]
    public void TheSlot_IsTheRunTimePlusTheServersSpread_OnTheServersClock()
    {
        var date = new DateOnly(2026, 10, 2);

        Assert.Equal(Utc(2026, 10, 2, 2, 0), CollectorRunTime.SlotUtc(date, TwoAm, NoSpreadId, CollectorRunTime.LocalIsUtc));
        Assert.Equal(Utc(2026, 10, 2, 2, 30), CollectorRunTime.SlotUtc(date, TwoAm, HalfHourSpreadId, CollectorRunTime.LocalIsUtc));
        Assert.Equal(Utc(2026, 10, 2, 2, 59, 59), CollectorRunTime.SlotUtc(date, TwoAm, LongSpreadId, CollectorRunTime.LocalIsUtc));

        var eastern = ServerClock.Resolve(EasternIanaId, -300);
        Assert.Equal(Utc(2026, 7, 1, 6, 0), CollectorRunTime.SlotUtc(new DateOnly(2026, 7, 1), TwoAm, NoSpreadId, eastern.ToUtc));
        Assert.Equal(Utc(2026, 1, 15, 7, 0), CollectorRunTime.SlotUtc(new DateOnly(2026, 1, 15), TwoAm, NoSpreadId, eastern.ToUtc));
    }

    [Fact]
    public void WithASpread_TheGraceStartsAtTheSlot_NotAtTheRunTime()
    {
        /* Id 1800 puts the slot at 02:30: 02:15 is still before it, 03:20 is inside its grace, 03:31 is past it. */
        Assert.Equal(Utc(2026, 10, 2, 2, 30), UtcClockNextDue(Utc(2026, 10, 2, 2, 15), null, serverId: HalfHourSpreadId));
        Assert.Equal(Utc(2026, 10, 2, 3, 20), UtcClockNextDue(Utc(2026, 10, 2, 3, 20), null, serverId: HalfHourSpreadId));
        Assert.Equal(Utc(2026, 10, 3, 2, 30), UtcClockNextDue(Utc(2026, 10, 2, 3, 31), null, serverId: HalfHourSpreadId));
    }

    /* ---- after a run ---- */

    [Fact]
    public void AfterARun_TheNextDueIsTheNextDatesSlot()
    {
        var run = Utc(2026, 10, 2, 2, 10);

        Assert.Equal(Utc(2026, 10, 3, 2, 0), UtcClockNextDue(run, run));

        /* With 30 minutes of spread the slot is 02:30, so a run in its grace is 02:40; a run at 02:10 would be
           before the slot and count for the day before. */
        var spreadRun = Utc(2026, 10, 2, 2, 40);
        Assert.Equal(Utc(2026, 10, 3, 2, 30), UtcClockNextDue(spreadRun, spreadRun, serverId: HalfHourSpreadId));
        Assert.Equal(Utc(2026, 10, 2, 2, 30), UtcClockNextDue(run, run, serverId: HalfHourSpreadId));
    }

    [Fact]
    public void ARestartInsideTheGraceAfterTheRun_DoesNotRunAgain()
    {
        var run = Utc(2026, 10, 2, 2, 10);

        Assert.Equal(Utc(2026, 10, 3, 2, 0), UtcClockNextDue(Utc(2026, 10, 2, 2, 20), run));
        Assert.Equal(Utc(2026, 10, 3, 2, 0), UtcClockNextDue(Utc(2026, 10, 2, 14, 0), run));
    }

    [Fact]
    public void ADayMissedWhileDown_IsSkippedNotReplayed_AndWaitsForTheNextSlot()
    {
        var lastRun = Utc(2026, 9, 28, 2, 10);

        Assert.Equal(Utc(2026, 10, 3, 2, 0), UtcClockNextDue(Utc(2026, 10, 2, 12, 0), lastRun));
        Assert.Equal(Utc(2026, 10, 2, 2, 0), UtcClockNextDue(Utc(2026, 10, 2, 1, 0), lastRun));
    }

    [Fact]
    public void ADayMissedWhileDown_ButNowInsideTheGrace_RunsNow()
    {
        var lastRun = Utc(2026, 9, 28, 2, 10);
        var now = Utc(2026, 10, 2, 2, 20);

        Assert.Equal(now, UtcClockNextDue(now, lastRun));
    }

    [Fact]
    public void YesterdaysRunInsideItsGrace_LeavesTodaysSlotDue()
    {
        var lastRun = Utc(2026, 10, 1, 2, 50);

        Assert.Equal(Utc(2026, 10, 2, 2, 0), UtcClockNextDue(Utc(2026, 10, 2, 1, 0), lastRun));
        Assert.Equal(Utc(2026, 10, 2, 2, 5), UtcClockNextDue(Utc(2026, 10, 2, 2, 5), lastRun));
    }

    [Fact]
    public void ARunBeforeTodaysSlot_CountsForTheDayBefore_SoTodaysSlotIsStillOwed()
    {
        var connectRun = Utc(2026, 10, 2, 1, 0);

        Assert.Equal(Utc(2026, 10, 2, 2, 0), UtcClockNextDue(connectRun, connectRun));
    }

    [Fact]
    public void ASlotThatSpillsPastMidnight_BelongsToTheDateItWasComputedFor()
    {
        /* Run time 23:30 plus 59:59 of spread puts the 2026-10-02 slot at 00:29:59 on 2026-10-03. A run in its grace
           is still the 10-02 run, so the next one is the 10-03 slot, a day later, not two. */
        const int lateRunAt = 23 * 60 + 30;
        var run = Utc(2026, 10, 3, 0, 35);

        Assert.Equal(Utc(2026, 10, 3, 0, 29, 59), CollectorRunTime.SlotUtc(new DateOnly(2026, 10, 2), lateRunAt, LongSpreadId, CollectorRunTime.LocalIsUtc));
        Assert.Equal(Utc(2026, 10, 4, 0, 29, 59), UtcClockNextDue(run, run, lateRunAt, serverId: LongSpreadId));
    }

    /* ---- whole-day intervals longer than one day ---- */

    [Fact]
    public void ATwoDayInterval_SkipsADay()
    {
        var run = Utc(2026, 10, 1, 2, 10);

        Assert.Equal(Utc(2026, 10, 3, 2, 0), UtcClockNextDue(run, run, interval: 2880));
        Assert.Equal(Utc(2026, 10, 3, 2, 0), UtcClockNextDue(Utc(2026, 10, 2, 12, 0), run, interval: 2880));
        Assert.Equal(Utc(2026, 10, 2, 2, 0), UtcClockNextDue(Utc(2026, 10, 1, 3, 0), Utc(2026, 9, 30, 2, 10), interval: 2880));
    }

    [Fact]
    public void ATwoDayInterval_RunsInsideTheGraceOfItsDay_AndSkipsAMissedOne()
    {
        var run = Utc(2026, 10, 1, 2, 10);
        var inside = Utc(2026, 10, 3, 2, 30);

        Assert.Equal(inside, UtcClockNextDue(inside, run, interval: 2880));

        /* Past the grace of the 10-03 slot, that run is lost and the next one is two days on, on the same grid. */
        Assert.Equal(Utc(2026, 10, 5, 2, 0), UtcClockNextDue(Utc(2026, 10, 3, 3, 30), run, interval: 2880));
        Assert.Equal(Utc(2026, 10, 5, 2, 0), UtcClockNextDue(Utc(2026, 10, 4, 12, 0), run, interval: 2880));
    }

    [Fact]
    public void ATwoDayInterval_NeverRun_WaitsForTheNextDailySlot()
    {
        Assert.Equal(Utc(2026, 10, 3, 2, 0), UtcClockNextDue(Utc(2026, 10, 2, 12, 0), null, interval: 2880));
    }

    /* ---- the server's clock ---- */

    [Theory]
    [InlineData(EasternWindowsId)]
    [InlineData(EasternIanaId)]
    public void TheSpringForwardDay_KeepsTheRunTimeOnTheLocalClock(string zoneId)
    {
        var clock = ServerClock.Resolve(zoneId, -300);

        /* 04:00 local: 09:00Z in standard time on 03-07, 08:00Z in daylight time from 03-08. A run at 04:05 EST on the
           7th is followed by 04:00 EDT, 23 hours on; "due + 1440 minutes" would be 05:00 EDT. */
        var run = Utc(2026, 3, 7, 9, 5);
        Assert.Equal(Utc(2026, 3, 8, 8, 0), NextDue(run, run, 240, Daily, NoSpreadId, clock.ToUtc));
        var dayAfter = Utc(2026, 3, 8, 8, 5);
        Assert.Equal(Utc(2026, 3, 9, 8, 0), NextDue(dayAfter, dayAfter, 240, Daily, NoSpreadId, clock.ToUtc));

        /* 02:00 local is inside the gap on the 8th, so it reads as 03:00 EDT (07:00Z), and the 9th is 02:00 EDT (06:00Z). */
        var twoRun = Utc(2026, 3, 7, 7, 10);
        Assert.Equal(Utc(2026, 3, 8, 7, 0), NextDue(twoRun, twoRun, TwoAm, Daily, NoSpreadId, clock.ToUtc));
        var gapDayRun = Utc(2026, 3, 8, 7, 10);
        Assert.Equal(Utc(2026, 3, 9, 6, 0), NextDue(gapDayRun, gapDayRun, TwoAm, Daily, NoSpreadId, clock.ToUtc));
    }

    [Theory]
    [InlineData(EasternWindowsId)]
    [InlineData(EasternIanaId)]
    public void TheFallBackDay_KeepsTheRunTimeOnTheLocalClock(string zoneId)
    {
        var clock = ServerClock.Resolve(zoneId, -240);

        /* 02:00 EDT on 10-31 is 06:00Z; 02:00 EST on 11-01 is 07:00Z, 25 hours on. "due + 1440 minutes" would be
           06:00Z, which is 01:00 EST on the 1st. */
        var run = Utc(2026, 10, 31, 6, 10);
        Assert.Equal(Utc(2026, 11, 1, 7, 0), NextDue(run, run, TwoAm, Daily, NoSpreadId, clock.ToUtc));
        var dayAfter = Utc(2026, 11, 1, 7, 10);
        Assert.Equal(Utc(2026, 11, 2, 7, 0), NextDue(dayAfter, dayAfter, TwoAm, Daily, NoSpreadId, clock.ToUtc));

        /* Before the change a never-run collector sees the daylight slot, after it the standard one. */
        Assert.Equal(Utc(2026, 11, 1, 7, 0), NextDue(Utc(2026, 11, 1, 5, 0), null, TwoAm, Daily, NoSpreadId, clock.ToUtc));
    }

    [Theory]
    [InlineData(EasternWindowsId)]
    [InlineData(EasternIanaId)]
    public void EveryDayOfTheYear_TheNextSlotIsTheSameLocalTime(string zoneId)
    {
        var clock = ServerClock.Resolve(zoneId, -300);
        var springForwardDay = new DateOnly(2026, 3, 8);

        /* Run time 02:00 plus the 30 minutes of spread for id 1800: 02:30 local, which does not exist on the spring
           forward day and reads as 03:30 there. */
        var previous = CollectorRunTime.SlotUtc(new DateOnly(2026, 1, 1), TwoAm, HalfHourSpreadId, clock.ToUtc);
        for (var date = new DateOnly(2026, 1, 2); date <= new DateOnly(2026, 12, 31); date = date.AddDays(1))
        {
            var run = previous.AddMinutes(5);
            var next = NextDue(run, run, TwoAm, Daily, HalfHourSpreadId, clock.ToUtc);

            Assert.Equal(CollectorRunTime.SlotUtc(date, TwoAm, HalfHourSpreadId, clock.ToUtc), next);
            var expectedLocal = date.ToDateTime(date == springForwardDay ? new TimeOnly(3, 30) : new TimeOnly(2, 30));
            Assert.Equal(expectedLocal, clock.ToServerLocal(next));
            previous = next;
        }
    }

    [Fact]
    public void AFixedOffsetClock_ReadsTheRunTimeOnTheServersClock_AcrossTheUtcDate()
    {
        /* +05:30: 02:00 local on the 3rd is 20:30Z on the 2nd. */
        var india = ServerClock.FixedOffset(330);

        Assert.Equal(Utc(2026, 10, 2, 20, 30), NextDue(Utc(2026, 10, 2, 12, 0), null, TwoAm, Daily, NoSpreadId, india.ToUtc));
        Assert.Equal(Utc(2026, 10, 2, 20, 30), NextDue(Utc(2026, 10, 2, 20, 0), null, TwoAm, Daily, NoSpreadId, india.ToUtc));
        Assert.Equal(Utc(2026, 10, 2, 21, 0), NextDue(Utc(2026, 10, 2, 21, 0), null, TwoAm, Daily, NoSpreadId, india.ToUtc));
        Assert.Equal(Utc(2026, 10, 3, 20, 30), NextDue(Utc(2026, 10, 2, 22, 0), null, TwoAm, Daily, NoSpreadId, india.ToUtc));

        /* -10:00, run time 23:30: the 2026-10-02 slot is 09:30Z on the 3rd. */
        var hawaii = ServerClock.FixedOffset(-600);
        const int lateRunAt = 23 * 60 + 30;
        Assert.Equal(Utc(2026, 10, 3, 9, 30), NextDue(Utc(2026, 10, 2, 12, 0), null, lateRunAt, Daily, NoSpreadId, hawaii.ToUtc));
    }

    [Fact]
    public void AnUnknownClock_IsUtc()
    {
        var instant = new DateTime(2026, 10, 2, 2, 0, 0, DateTimeKind.Unspecified);

        Assert.Equal(instant, CollectorRunTime.LocalIsUtc(instant));
        Assert.Equal(Utc(2026, 10, 3, 2, 0), UtcClockNextDue(Utc(2026, 10, 2, 12, 0), null));
        Assert.Equal(Utc(2026, 10, 3, 2, 0), NextDue(Utc(2026, 10, 2, 12, 0), null, TwoAm, Daily, NoSpreadId, ServerClock.Utc.ToUtc));
    }

    [Fact]
    public void TheAnswers_AreUtcKind_WhateverKindTheClockReturns()
    {
        var eastern = ServerClock.Resolve(EasternIanaId, -300);
        var now = Utc(2026, 10, 2, 12, 0);

        Assert.Equal(DateTimeKind.Utc, NextDue(now, null, TwoAm, Daily, NoSpreadId, eastern.ToUtc).Kind);
        Assert.Equal(DateTimeKind.Utc, UtcClockNextDue(now, null).Kind);
        Assert.Equal(DateTimeKind.Utc, CollectorRunTime.SlotUtc(new DateOnly(2026, 10, 2), TwoAm, NoSpreadId, eastern.ToUtc).Kind);
    }

    /* ---- every clock, against a brute force over each candidate date ---- */

    private static IEnumerable<(string Name, Func<DateTime, DateTime> ToUtc)> Clocks()
    {
        yield return ("unknown", CollectorRunTime.LocalIsUtc);
        yield return ("-12:00", ServerClock.FixedOffset(-720).ToUtc);
        yield return ("-05:00", ServerClock.FixedOffset(-300).ToUtc);
        yield return ("+05:30", ServerClock.FixedOffset(330).ToUtc);
        yield return ("+14:00", ServerClock.FixedOffset(840).ToUtc);
        yield return ("US Eastern", ServerClock.Resolve(EasternIanaId, -300).ToUtc);
    }

    private static IEnumerable<DateTime> Samples()
    {
        foreach (var start in new[] { Utc(2026, 3, 6), Utc(2026, 10, 30), Utc(2026, 10, 1) })
        {
            for (var t = start; t < start.AddDays(4); t = t.AddMinutes(211))
            {
                yield return t;
            }
        }
    }

    /* The slow, obvious version: look at every date around now. The first slot at or after now, or now itself when
       now is inside a slot's grace. */
    private static DateTime NeverRunByBruteForce(DateTime now, int runAt, int serverId, Func<DateTime, DateTime> toUtc)
    {
        var around = DateOnly.FromDateTime(now);
        DateTime? next = null;
        for (var date = around.AddDays(-4); date <= around.AddDays(4); date = date.AddDays(1))
        {
            var slot = CollectorRunTime.SlotUtc(date, runAt, serverId, toUtc);
            if (slot <= now && now <= slot + CollectorRunTime.Grace)
            {
                return now;
            }

            if (slot > now && (next is null || slot < next))
            {
                next = slot;
            }
        }

        return next!.Value;
    }

    /* The date a run belongs to is the latest date whose slot is at or before it. The slots after it are every
       `days` dates on; the first whose grace has not passed is due, or now when now is already inside it. */
    private static DateTime WithLastRunByBruteForce(DateTime now, DateTime lastRun, int days, int runAt, int serverId,
        Func<DateTime, DateTime> toUtc)
    {
        var around = DateOnly.FromDateTime(lastRun);
        var lastDate = around.AddDays(-4);
        for (var date = around.AddDays(-4); date <= around.AddDays(4); date = date.AddDays(1))
        {
            if (CollectorRunTime.SlotUtc(date, runAt, serverId, toUtc) <= lastRun)
            {
                lastDate = date;
            }
        }

        for (var step = 1; ; step++)
        {
            var slot = CollectorRunTime.SlotUtc(lastDate.AddDays(step * days), runAt, serverId, toUtc);
            if (now <= slot + CollectorRunTime.Grace)
            {
                return slot <= now ? now : slot;
            }
        }
    }

    [Fact]
    public void NeverRun_OnEveryClock_IsTheFirstSlotAheadOrNowInsideAGrace()
    {
        foreach (var (name, toUtc) in Clocks())
        {
            foreach (var runAt in new[] { 0, 119, 1439 })
            {
                foreach (var id in new[] { NoSpreadId, HalfHourSpreadId, LongSpreadId })
                {
                    foreach (var now in Samples())
                    {
                        var actual = NextDue(now, null, runAt, Daily, id, toUtc);

                        Assert.True(NeverRunByBruteForce(now, runAt, id, toUtc) == actual,
                            $"{name}, run at {runAt}, id {id}, now {now:O}: got {actual:O}");
                        Assert.True(actual - now <= CollectorRunTime.MaxStampAhead(Daily),
                            $"{name}, run at {runAt}, id {id}, now {now:O}: {actual:O} is further ahead than the clamp bound");
                    }
                }
            }
        }
    }

    [Fact]
    public void WithALastRun_OnEveryClock_IsTheNextGridSlotOrNowInsideItsGrace()
    {
        var back = new[] { TimeSpan.FromMinutes(10), TimeSpan.FromHours(5), TimeSpan.FromHours(30), TimeSpan.FromHours(70) };
        foreach (var (name, toUtc) in Clocks())
        {
            foreach (var runAt in new[] { 0, 119, 1439 })
            {
                foreach (var id in new[] { NoSpreadId, LongSpreadId })
                {
                    foreach (var days in new[] { 1, 2, 3 })
                    {
                        foreach (var now in Samples())
                        {
                            foreach (var ago in back)
                            {
                                var lastRun = now - ago;
                                var actual = NextDue(now, lastRun, runAt, days * Daily, id, toUtc);

                                Assert.True(WithLastRunByBruteForce(now, lastRun, days, runAt, id, toUtc) == actual,
                                    $"{name}, run at {runAt}, id {id}, {days} d, now {now:O}, last run {lastRun:O}: got {actual:O}");
                                Assert.True(actual - now <= CollectorRunTime.MaxStampAhead(days * Daily),
                                    $"{name}, run at {runAt}, id {id}, {days} d, now {now:O}, last run {lastRun:O}: {actual:O} is further ahead than the clamp bound");
                            }
                        }
                    }
                }
            }
        }
    }

    /* ---- the clamp bound ---- */

    [Fact]
    public void TheClampBound_IsTheIntervalPlusSixtyMinutesOfSpreadPlusAnAutumnHour()
    {
        Assert.Equal(TimeSpan.FromMinutes(1440 + 60 + 60), CollectorRunTime.MaxStampAhead(1440));
        Assert.Equal(TimeSpan.FromMinutes(2880 + 60 + 60), CollectorRunTime.MaxStampAhead(2880));
    }

    [Fact]
    public void ARunTimeStampOnTheLongestDay_IsNotReadAsAClockStep()
    {
        var eastern = ServerClock.Resolve(EasternIanaId, -240);
        var run = Utc(2026, 10, 31, 6, 0);
        var due = NextDue(run, run, TwoAm, Daily, NoSpreadId, eastern.ToUtc);

        Assert.Equal(Utc(2026, 11, 1, 7, 0), due);
        Assert.Equal(TimeSpan.FromHours(25), due - run);

        /* The plain interval reads a 25-hour wait as a clock that stepped back and runs it now. The bound does not. */
        Assert.Equal(run, CollectorCadence.ClampDue(due, run, TimeSpan.FromMinutes(Daily)));
        Assert.Equal(due, CollectorCadence.ClampDue(due, run, CollectorRunTime.MaxStampAhead(Daily)));
    }

    [Fact]
    public void AStampMuchFurtherAheadThanTheBound_IsStillReadAsAClockStep()
    {
        var now = Utc(2026, 10, 2, 12, 0);
        var due = now.AddDays(3);

        Assert.Equal(now, CollectorCadence.ClampDue(due, now, CollectorRunTime.MaxStampAhead(Daily)));
    }

    /* ---- which collectors take a run time ---- */

    [Theory]
    [InlineData(60, false)]
    [InlineData(720, false)]
    [InlineData(1439, false)]
    [InlineData(1440, true)]
    [InlineData(1441, false)]
    [InlineData(2880, true)]
    [InlineData(4320, true)]
    [InlineData(10080, true)]
    [InlineData(0, false)]
    [InlineData(-1440, false)]
    public void AllowsRunAt_OnlyForAPositiveWholeNumberOfDays(int intervalMinutes, bool expected)
    {
        Assert.Equal(expected, CollectorRunTime.AllowsRunAt(intervalMinutes));
    }

    [Fact]
    public void NextDue_RefusesWhatNoRunTimeCanMean()
    {
        var now = Utc(2026, 10, 2, 12, 0);

        Assert.Throws<ArgumentOutOfRangeException>(() => UtcClockNextDue(now, null, interval: 60));
        Assert.Throws<ArgumentOutOfRangeException>(() => UtcClockNextDue(now, null, interval: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => UtcClockNextDue(now, null, runAt: -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => UtcClockNextDue(now, null, runAt: 1440));
        Assert.Throws<ArgumentNullException>(() => NextDue(now, null, TwoAm, Daily, NoSpreadId, null!));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            CollectorRunTime.SlotUtc(new DateOnly(2026, 10, 2), 1440, NoSpreadId, CollectorRunTime.LocalIsUtc));
    }

    /* ---- the HH:MM text ---- */

    [Theory]
    [InlineData("00:00", 0)]
    [InlineData("02:00", 120)]
    [InlineData("02:05", 125)]
    [InlineData("12:30", 750)]
    [InlineData("23:59", 1439)]
    [InlineData(" 02:00 ", 120)]
    public void TryParse_AcceptsA24HourTime(string text, int expectedMinute)
    {
        Assert.True(CollectorRunTime.TryParse(text, out var minute));
        Assert.Equal(expectedMinute, minute);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("24:00")]
    [InlineData("23:60")]
    [InlineData("99:99")]
    [InlineData("2:5")]
    [InlineData("2:00")]
    [InlineData("02:5")]
    [InlineData("abc")]
    [InlineData("ab:cd")]
    [InlineData("02-00")]
    [InlineData("0200")]
    [InlineData("02:000")]
    [InlineData("02:00:00")]
    [InlineData("-1:00")]
    [InlineData("+2:00")]
    [InlineData("０２:00")]
    [InlineData("02:00 PM")]
    public void TryParse_RefusesAnythingButHhMm(string? text)
    {
        Assert.False(CollectorRunTime.TryParse(text, out var minute));
        Assert.Equal(0, minute);
    }

    [Fact]
    public void Format_RoundTripsEveryMinuteOfTheDay()
    {
        var shape = new Regex("^[0-2][0-9]:[0-5][0-9]$");
        for (var minute = 0; minute < 1440; minute++)
        {
            var text = CollectorRunTime.Format(minute);

            Assert.Matches(shape, text);
            Assert.True(CollectorRunTime.TryParse(text, out var parsed), text);
            Assert.Equal(minute, parsed);
        }

        Assert.Equal("00:00", CollectorRunTime.Format(0));
        Assert.Equal("02:05", CollectorRunTime.Format(125));
        Assert.Equal("23:59", CollectorRunTime.Format(1439));
    }

    [Fact]
    public void Format_RefusesAMinuteOutsideTheDay()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => CollectorRunTime.Format(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => CollectorRunTime.Format(1440));
    }

    /* ---- the one copy of the error texts ---- */

    [Fact]
    public void TheErrorTexts_AreTheOnesEveryEditorShows()
    {
        Assert.Equal("Run at must be a 24-hour time from 00:00 to 23:59, such as 02:00.", CollectorRunTime.InvalidRunAtMessage);
        Assert.Equal(
            "'wait_stats' runs every 60 minutes. A run time works only for a collector that runs once a day or less often (1440 minutes, or a multiple of 1440). Change the frequency or clear the run time.",
            CollectorRunTime.IntervalRefusalMessage("wait_stats", 60));
        Assert.Equal(
            "'server_config' runs every 720 minutes. A run time works only for a collector that runs once a day or less often (1440 minutes, or a multiple of 1440). Change the frequency or clear the run time.",
            CollectorRunTime.IntervalRefusalMessage("server_config", 720));
    }
}
