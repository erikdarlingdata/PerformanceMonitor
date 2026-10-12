/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The zones and instants the display-zone tests share. Every zone is looked up by its Windows id and passed in:
/// nothing here reads the machine's own zone, so a run on a desktop in any zone gives the same answers. The
/// examples use US Eastern as the "server" (clock change 2026-03-08 07:00Z forward, 2026-11-01 06:00Z back) and US
/// Pacific as the "desktop" (2026-03-08 10:00Z forward, 2026-11-01 09:00Z back).
/// </summary>
internal static class DisplayZoneFixtures
{
    internal static readonly TimeZoneInfo Eastern = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
    internal static readonly TimeZoneInfo Pacific = TimeZoneInfo.FindSystemTimeZoneById("Pacific Standard Time");
    internal static readonly TimeZoneInfo India = TimeZoneInfo.FindSystemTimeZoneById("India Standard Time");
    internal static readonly TimeZoneInfo LordHowe = TimeZoneInfo.FindSystemTimeZoneById("Lord Howe Standard Time");

    /// <summary>A naive (Kind Unspecified) time, the frame the stores keep.</summary>
    internal static DateTime At(int year, int month, int day, int hour, int minute = 0, int second = 0)
        => new(year, month, day, hour, minute, second, DateTimeKind.Unspecified);
}

/// <summary>
/// #4766: a time is held as a UTC instant and turned into a wall clock only when text is drawn. The old label path
/// turned the instant into the server's wall clock and then into the display mode, and a wall clock cannot tell the
/// two 01:30s of the autumn change day apart: 05:30Z and 06:30Z came back as the same time. These pin
/// <see cref="DisplayZone.ToDisplay"/> and the ambiguity suffix on the instants either side of a change.
/// </summary>
public sealed class DisplayZoneTests
{
    private const string GridFormat = "yyyy-MM-dd HH:mm:ss";

    [Fact]
    public void ASecondOccurrenceInstant_RendersAsItsOwnWallClock()
    {
        var second = DisplayZoneFixtures.At(2026, 11, 1, 6, 30);

        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 6, 30), DisplayZone.ToDisplay(second, TimeZoneInfo.Utc));
        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 1, 30), DisplayZone.ToDisplay(second, DisplayZoneFixtures.Eastern));
        Assert.Equal(DisplayZoneFixtures.At(2026, 10, 31, 23, 30), DisplayZone.ToDisplay(second, DisplayZoneFixtures.Pacific));
        Assert.Equal(DateTimeKind.Unspecified, DisplayZone.ToDisplay(second, DisplayZoneFixtures.Eastern).Kind);

        /* The first 01:30 reads the same on the server's clock and is an hour earlier: only the offset tells them apart. */
        var first = DisplayZoneFixtures.At(2026, 11, 1, 5, 30);
        Assert.Equal(DisplayZone.ToDisplay(first, DisplayZoneFixtures.Eastern), DisplayZone.ToDisplay(second, DisplayZoneFixtures.Eastern));
        Assert.Equal("-04:00", DisplayZone.AmbiguousOffsetSuffix(first, DisplayZoneFixtures.Eastern));
        Assert.Equal("-05:00", DisplayZone.AmbiguousOffsetSuffix(second, DisplayZoneFixtures.Eastern));
    }

    [Fact]
    public void AGridTime_InTheSecondOccurrence_PrintsItsOwnInstant()
    {
        var second = DisplayZoneFixtures.At(2026, 11, 1, 6, 30);

        Assert.Equal("2026-11-01 06:30:00", DisplayZone.ToDisplay(second, TimeZoneInfo.Utc).ToString(GridFormat, CultureInfo.InvariantCulture));
        Assert.Equal("2026-11-01 01:30:00", DisplayZone.ToDisplay(second, DisplayZoneFixtures.Eastern).ToString(GridFormat, CultureInfo.InvariantCulture));
        Assert.Equal("2026-10-31 23:30:00", DisplayZone.ToDisplay(second, DisplayZoneFixtures.Pacific).ToString(GridFormat, CultureInfo.InvariantCulture));
    }

    [Fact]
    public void TheOffsetSuffix_AppearsOnlyWhereTheWallTimeRepeats()
    {
        /* Not ambiguous: UTC never is, a desktop in another zone at that instant is not, and a time outside the hour is not. */
        var second = DisplayZoneFixtures.At(2026, 11, 1, 6, 30);
        Assert.Null(DisplayZone.AmbiguousOffsetSuffix(second, TimeZoneInfo.Utc));
        Assert.Null(DisplayZone.AmbiguousOffsetSuffix(second, DisplayZoneFixtures.Pacific));
        Assert.Null(DisplayZone.AmbiguousOffsetSuffix(DisplayZoneFixtures.At(2026, 11, 1, 4, 59), DisplayZoneFixtures.Eastern));
        Assert.Null(DisplayZone.AmbiguousOffsetSuffix(DisplayZoneFixtures.At(2026, 11, 1, 7, 0), DisplayZoneFixtures.Eastern));

        /* Both ends of the repeated hour are in it. */
        Assert.Equal("-04:00", DisplayZone.AmbiguousOffsetSuffix(DisplayZoneFixtures.At(2026, 11, 1, 5, 0), DisplayZoneFixtures.Eastern));
        Assert.Equal("-05:00", DisplayZone.AmbiguousOffsetSuffix(DisplayZoneFixtures.At(2026, 11, 1, 6, 59), DisplayZoneFixtures.Eastern));

        /* The desktop's own repeated hour, same rule. */
        Assert.Equal("-07:00", DisplayZone.AmbiguousOffsetSuffix(DisplayZoneFixtures.At(2026, 11, 1, 8, 15), DisplayZoneFixtures.Pacific));
        Assert.Equal("-08:00", DisplayZone.AmbiguousOffsetSuffix(DisplayZoneFixtures.At(2026, 11, 1, 9, 15), DisplayZoneFixtures.Pacific));
    }

    [Fact]
    public void ALordHoweHalfHourRepeat_HasItsOwnOffsets()
    {
        /* Lord Howe puts its clock back 30 minutes (+11:00 to +10:30) at 15:00Z on 2026-04-04: 01:30-02:00 happens twice. */
        var first = DisplayZoneFixtures.At(2026, 4, 4, 14, 45);
        var second = DisplayZoneFixtures.At(2026, 4, 4, 15, 15);

        Assert.Equal(DisplayZoneFixtures.At(2026, 4, 5, 1, 45), DisplayZone.ToDisplay(first, DisplayZoneFixtures.LordHowe));
        Assert.Equal(DisplayZoneFixtures.At(2026, 4, 5, 1, 45), DisplayZone.ToDisplay(second, DisplayZoneFixtures.LordHowe));
        Assert.Equal("+11:00", DisplayZone.AmbiguousOffsetSuffix(first, DisplayZoneFixtures.LordHowe));
        Assert.Equal("+10:30", DisplayZone.AmbiguousOffsetSuffix(second, DisplayZoneFixtures.LordHowe));
    }

    [Fact]
    public void Format_AddsTheOffsetExactlyWhenTheBareTimeWouldBeAmbiguous()
    {
        Assert.Equal("01:30:00 -04:00", DisplayZone.Format(DisplayZoneFixtures.At(2026, 11, 1, 5, 30), DisplayZoneFixtures.Eastern, "HH:mm:ss"));
        Assert.Equal("01:30:00 -05:00", DisplayZone.Format(DisplayZoneFixtures.At(2026, 11, 1, 6, 30), DisplayZoneFixtures.Eastern, "HH:mm:ss"));
        Assert.Equal("02:30:00", DisplayZone.Format(DisplayZoneFixtures.At(2026, 11, 1, 7, 30), DisplayZoneFixtures.Eastern, "HH:mm:ss"));
        Assert.Equal("06:30:00", DisplayZone.Format(DisplayZoneFixtures.At(2026, 11, 1, 6, 30), TimeZoneInfo.Utc, "HH:mm:ss"));
    }

    [Fact]
    public void TheSentinelEndsOfTheCalendar_StayPutRatherThanThrow()
    {
        Assert.Equal(DateTime.MinValue, DisplayZone.ToDisplay(DateTime.MinValue, DisplayZoneFixtures.Pacific));
        Assert.Equal(DateTime.MaxValue, DisplayZone.ToDisplay(DateTime.MaxValue, DisplayZoneFixtures.India));
        Assert.Equal(DateTime.MinValue, DisplayZone.ToUtcBound(DateTime.MinValue, DisplayZoneFixtures.India, BoundSide.From));
        Assert.Equal(DateTime.MaxValue, DisplayZone.ToUtcBound(DateTime.MaxValue, DisplayZoneFixtures.Pacific, BoundSide.To));
    }
}
