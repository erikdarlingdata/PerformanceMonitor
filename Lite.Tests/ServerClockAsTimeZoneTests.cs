/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis.Baselines;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: <see cref="ServerClock.AsTimeZone"/> hands the shared chart project the server's clock as a
/// <see cref="TimeZoneInfo"/>, because that project does not reference the server-clock type: the server's own zone
/// when it resolved on this host, else a custom zone at the fixed offset, else UTC.
/// </summary>
public sealed class ServerClockAsTimeZoneTests
{
    [Fact]
    public void AResolvedZone_IsTheServersOwnZone_AndFollowsItsClockChanges()
    {
        var zone = ServerClock.Resolve("Eastern Standard Time", -300).AsTimeZone();

        Assert.Equal("Eastern Standard Time", zone.Id);
        Assert.Equal(TimeSpan.FromHours(-4), zone.GetUtcOffset(new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc)));
        Assert.Equal(TimeSpan.FromHours(-5), zone.GetUtcOffset(new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc)));
    }

    [Fact]
    public void AFixedOffset_IsACustomZoneAtThatOffset_BuiltOncePerInstance()
    {
        var clock = ServerClock.FixedOffset(330);

        var zone = clock.AsTimeZone();

        Assert.Equal(TimeSpan.FromMinutes(330), zone.BaseUtcOffset);
        Assert.False(zone.SupportsDaylightSavingTime);
        Assert.Equal("UTC+05:30", zone.Id);
        Assert.Equal(TimeSpan.FromMinutes(330), zone.GetUtcOffset(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc)));
        Assert.Equal(TimeSpan.FromMinutes(330), zone.GetUtcOffset(new DateTime(2026, 7, 1, 0, 0, 0, DateTimeKind.Utc)));
        Assert.Same(zone, clock.AsTimeZone());

        var west = ServerClock.FixedOffset(-570).AsTimeZone();
        Assert.Equal(TimeSpan.FromMinutes(-570), west.BaseUtcOffset);
        Assert.Equal("UTC-09:30", west.Id);
    }

    [Fact]
    public void NoZoneAndNoOffset_IsUtc()
    {
        Assert.Same(TimeZoneInfo.Utc, ServerClock.Utc.AsTimeZone());
        Assert.Same(TimeZoneInfo.Utc, ServerClock.Resolve(null, null).AsTimeZone());
        Assert.Same(TimeZoneInfo.Utc, ServerClock.FixedOffset(0).AsTimeZone());
        Assert.Same(TimeZoneInfo.Utc, ServerClock.Resolve("  ", 0).AsTimeZone());
    }

    [Fact]
    public void AZoneIdThisHostCannotResolve_FallsBackToTheFixedOffset()
    {
        var zone = ServerClock.Resolve("No Such Zone Anywhere", 120).AsTimeZone();

        Assert.Equal(TimeSpan.FromMinutes(120), zone.BaseUtcOffset);
        Assert.False(zone.SupportsDaylightSavingTime);
    }

    [Fact]
    public void TheZone_AgreesWithTheClocksOwnConversions()
    {
        var clock = ServerClock.Resolve("Eastern Standard Time", -300);
        var zone = clock.AsTimeZone();

        foreach (var utc in new[]
        {
            new DateTime(2026, 3, 8, 6, 59, 0), new DateTime(2026, 3, 8, 7, 0, 0),
            new DateTime(2026, 11, 1, 5, 59, 0), new DateTime(2026, 11, 1, 6, 0, 0), new DateTime(2026, 7, 4, 12, 0, 0)
        })
        {
            Assert.Equal(clock.ToServerLocal(utc), TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(utc, DateTimeKind.Utc), zone));
        }
    }

    [Fact]
    public void AnOffsetNoZoneCanHold_IsHeldAtFourteenHoursRatherThanThrown()
    {
        Assert.Equal(TimeSpan.FromHours(14), ServerClock.FixedOffset(20 * 60).AsTimeZone().BaseUtcOffset);
        Assert.Equal(TimeSpan.FromHours(-14), ServerClock.FixedOffset(-20 * 60).AsTimeZone().BaseUtcOffset);
    }

    [Fact]
    public void ManyThreadsAskingAtOnce_AllGetTheSameInstance()
    {
        var clock = ServerClock.FixedOffset(-210);

        var zones = new TimeZoneInfo[64];
        Parallel.For(0, zones.Length, i => zones[i] = clock.AsTimeZone());

        Assert.Single(zones.Distinct());
    }
}
