/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Analysis.Baselines;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4766: <see cref="ServerClock"/> converts naive UTC to a server's wall clock and back with the server's
/// time zone, so a time on the far side of a daylight-saving change is not an hour off. The zone ids are the
/// ones <c>LocalClockBucketKeyTests</c> uses for <see cref="BaselineLocalClock"/>: US Eastern springs forward
/// on Sunday 2026-03-08 (07:00Z, -05:00 to -04:00) and falls back on Sunday 2026-11-01 (06:00Z).
/// </summary>
public sealed class ServerClockTests
{
    private const string EasternWindowsId = "Eastern Standard Time";
    private const string EasternIanaId = "America/New_York";

    private static DateTime Naive(int year, int month, int day, int hour, int minute = 0) =>
        new(year, month, day, hour, minute, 0, DateTimeKind.Unspecified);

    [Theory]
    [InlineData(EasternWindowsId)]
    [InlineData(EasternIanaId)]
    public void ToServerLocal_BeforeAndAfterTheFallBack_ShowsTheWallClockOnEachSide(string zoneId)
    {
        var clock = ServerClock.Resolve(zoneId, -300);

        /* 2026-10-31 16:00Z is still daylight time (-4 h); 2026-11-02 16:00Z is standard time (-5 h). */
        Assert.Equal(Naive(2026, 10, 31, 12), clock.ToServerLocal(Naive(2026, 10, 31, 16)));
        Assert.Equal(Naive(2026, 11, 2, 11), clock.ToServerLocal(Naive(2026, 11, 2, 16)));
        Assert.Equal(-240, clock.OffsetMinutesAt(Naive(2026, 10, 31, 16)));
        Assert.Equal(-300, clock.OffsetMinutesAt(Naive(2026, 11, 2, 16)));
    }

    [Fact]
    public void ToServerLocal_BeforeAndAfterTheSpringForward_ShowsTheWallClockOnEachSide()
    {
        var clock = ServerClock.Resolve(EasternWindowsId, -240);

        Assert.Equal(Naive(2026, 3, 7, 11), clock.ToServerLocal(Naive(2026, 3, 7, 16)));
        Assert.Equal(Naive(2026, 3, 9, 12), clock.ToServerLocal(Naive(2026, 3, 9, 16)));
    }

    [Fact]
    public void ToServerLocal_KeepsUnspecifiedKind()
    {
        var clock = ServerClock.Resolve(EasternWindowsId, -300);

        Assert.Equal(DateTimeKind.Unspecified, clock.ToServerLocal(Naive(2026, 6, 1, 12)).Kind);
        Assert.Equal(DateTimeKind.Unspecified, clock.ToUtc(Naive(2026, 6, 1, 8)).Kind);
    }

    [Fact]
    public void ToUtc_ASkippedLocalTime_MovesForwardByTheGapAndDoesNotThrow()
    {
        var clock = ServerClock.Resolve(EasternWindowsId, -300);

        /* 02:30 on 2026-03-08 never happened: the clock went from 02:00 straight to 03:00. It reads as 03:30
           daylight time, which is 07:30Z. TimeZoneInfo.ConvertTimeToUtc throws on this value. */
        var utc = clock.ToUtc(Naive(2026, 3, 8, 2, 30));

        Assert.Equal(Naive(2026, 3, 8, 7, 30), utc);
        Assert.Equal(Naive(2026, 3, 8, 3, 30), clock.ToServerLocal(utc));
    }

    [Fact]
    public void ToUtc_ARepeatedLocalTime_TakesTheFirstOccurrence()
    {
        var clock = ServerClock.Resolve(EasternWindowsId, -300);

        /* 01:30 on 2026-11-01 happens twice: 05:30Z (daylight, -4 h) and 06:30Z (standard, -5 h). The first
           occurrence wins. */
        Assert.Equal(Naive(2026, 11, 1, 5, 30), clock.ToUtc(Naive(2026, 11, 1, 1, 30)));
    }

    [Theory]
    [InlineData(2026, 3, 7, 12, 0, 17, 0)]   /* winter: -5 h */
    [InlineData(2026, 3, 9, 12, 0, 16, 0)]   /* after spring forward: -4 h */
    [InlineData(2026, 10, 31, 12, 0, 16, 0)] /* before fall back: -4 h */
    [InlineData(2026, 11, 2, 12, 0, 17, 0)]  /* after fall back: -5 h */
    public void ToUtc_AnOrdinaryLocalTime_UsesTheOffsetInForceAtThatTime(
        int year, int month, int day, int hour, int minute, int expectedUtcHour, int expectedUtcMinute)
    {
        var clock = ServerClock.Resolve(EasternWindowsId, -300);

        Assert.Equal(Naive(year, month, day, expectedUtcHour, expectedUtcMinute), clock.ToUtc(Naive(year, month, day, hour, minute)));
    }

    [Fact]
    public void ToUtc_RoundTripsToServerLocal_AcrossBothChanges()
    {
        var clock = ServerClock.Resolve(EasternWindowsId, -300);
        var utc = Naive(2026, 1, 1, 0);

        for (var i = 0; i < 24 * 365; i += 3)
        {
            var instant = utc.AddHours(i);
            var local = clock.ToServerLocal(instant);
            var back = clock.ToUtc(local);

            /* Only the repeated hour cannot round trip: its second occurrence comes back as the first. */
            if (back != instant)
            {
                Assert.Equal(instant.AddHours(-1), back);
                Assert.True(instant >= Naive(2026, 11, 1, 6) && instant < Naive(2026, 11, 1, 7));
            }
        }
    }

    [Fact]
    public void Resolve_AnIdThatDoesNotResolve_FallsBackToTheOffset()
    {
        var clock = ServerClock.Resolve("No Such Zone Id", -420);

        Assert.Equal(Naive(2026, 7, 1, 5), clock.ToServerLocal(Naive(2026, 7, 1, 12)));
        Assert.Equal(Naive(2026, 1, 1, 12), clock.ToUtc(Naive(2026, 1, 1, 5)));
        Assert.Equal(-420, clock.OffsetMinutesAt(Naive(2026, 1, 1, 12)));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Resolve_ANullOrBlankId_UsesTheOffset(string? zoneId)
    {
        var clock = ServerClock.Resolve(zoneId, 330);

        Assert.Equal(Naive(2026, 7, 1, 17, 30), clock.ToServerLocal(Naive(2026, 7, 1, 12)));
        Assert.Equal(Naive(2026, 7, 1, 12), clock.ToUtc(Naive(2026, 7, 1, 17, 30)));
    }

    [Fact]
    public void Resolve_NeitherAnIdNorAnOffset_IsUtc()
    {
        var clock = ServerClock.Resolve(null, null);

        Assert.Equal(Naive(2026, 7, 1, 12), clock.ToServerLocal(Naive(2026, 7, 1, 12)));
        Assert.Equal(Naive(2026, 7, 1, 12), clock.ToUtc(Naive(2026, 7, 1, 12)));
        Assert.Equal(0, clock.OffsetMinutesAt(Naive(2026, 7, 1, 12)));

        Assert.Equal(Naive(2026, 7, 1, 12), ServerClock.Resolve("No Such Zone Id", null).ToUtc(Naive(2026, 7, 1, 12)));
    }

    [Fact]
    public void Resolve_AResolvableId_WinsOverAStaleOffset()
    {
        /* The offset says winter; the zone knows it is July. */
        var clock = ServerClock.Resolve(EasternWindowsId, -300);

        Assert.Equal(Naive(2026, 7, 1, 8), clock.ToServerLocal(Naive(2026, 7, 1, 12)));
    }

    [Fact]
    public void TheEndsOfTheCalendar_ClampInsteadOfThrowing()
    {
        var eastern = ServerClock.Resolve(EasternWindowsId, -300);
        var ahead = ServerClock.FixedOffset(600);

        Assert.Equal(DateTime.MinValue, ahead.ToUtc(DateTime.MinValue));
        Assert.Equal(DateTime.MaxValue, ServerClock.FixedOffset(-600).ToUtc(DateTime.MaxValue));
        Assert.Equal(DateTime.MinValue, eastern.ToServerLocal(DateTime.MinValue));
        Assert.Equal(DateTime.MaxValue, ahead.ToServerLocal(DateTime.MaxValue));
    }
}
