/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins <see cref="ViewerTimeHelper"/> — the viewer's port of Lite's ServerTimeHelper and the single
/// chokepoint every rendered timestamp routes through. The store is naive-UTC, so: UTC = raw, Local =
/// machine-local (SpecifyKind(Utc).ToLocalTime()), Server = UTC + the server's collected offset. Also pins
/// the zone the charts draw in and the custom-range pickers read in, and the per-server offset read SQL. The
/// conversion core is exercised through the pure <c>ConvertToDisplay</c>/<c>DisplayZoneFor</c> overloads so the
/// process-wide statics are not mutated; two focused tests confirm the static <c>ForDisplay</c>/<c>CurrentDisplayZone</c>
/// delegate to them (saving/restoring the statics).
/// </summary>
/* Serialized: these classes flip the process-wide ViewerTimeHelper.CurrentDisplayMode static (each
   restores in finally, but xUnit runs CLASSES in parallel — two flippers racing corrupts the mode a
   third class reads). One shared collection serializes them. */
[Collection("viewer-time-statics")]
public sealed class ViewerTimeHelperTests
{
    private static readonly DateTime NaiveUtc = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void ConvertToDisplay_Utc_ReturnsRawStoredValue()
    {
        Assert.Equal(NaiveUtc, ViewerTimeHelper.ConvertToDisplay(NaiveUtc, TimeDisplayMode.UTC, utcOffsetMinutes: 330));
    }

    [Fact]
    public void ConvertToDisplay_Local_MatchesMachineLocalConvention()
    {
        /* The viewer's historical convention (the former ViewerDataService.ToLocalTime): treat the
           Unspecified store value as UTC, then convert to the viewer machine's local time. Offset is ignored. */
        var expected = DateTime.SpecifyKind(NaiveUtc, DateTimeKind.Utc).ToLocalTime();
        var actual = ViewerTimeHelper.ConvertToDisplay(NaiveUtc, TimeDisplayMode.LocalTime, utcOffsetMinutes: 999);
        Assert.Equal(expected, actual);
        Assert.Equal(DateTimeKind.Local, actual.Kind);
    }

    [Theory]
    [InlineData(0, 0)]        // a UTC server: Server == UTC
    [InlineData(330, 330)]    // India +05:30
    [InlineData(-480, -480)]  // US Pacific standard -08:00
    [InlineData(-300, -300)]  // US Eastern standard -05:00
    public void ConvertToDisplay_Server_AddsCollectedOffset(int offsetMinutes, int expectedShiftMinutes)
    {
        Assert.Equal(
            NaiveUtc.AddMinutes(expectedShiftMinutes),
            ViewerTimeHelper.ConvertToDisplay(NaiveUtc, TimeDisplayMode.ServerTime, offsetMinutes));
    }

    [Theory]
    [InlineData(TimeDisplayMode.UTC)]
    [InlineData(TimeDisplayMode.LocalTime)]
    [InlineData(TimeDisplayMode.ServerTime)]
    public void DisplayZoneRead_InvertsConvertToDisplay(TimeDisplayMode mode)
    {
        const int offset = -300;
        var display = ViewerTimeHelper.ConvertToDisplay(NaiveUtc, mode, offset);
        var backToStore = DisplayZone.ToUtcBound(display, ViewerTimeHelper.DisplayZoneFor(mode, ServerClock.FixedOffset(offset)), BoundSide.From);

        /* Round-trips to the original naive-UTC store value, re-stamped Unspecified (the kind the reads send). */
        Assert.Equal(NaiveUtc, backToStore);
        Assert.Equal(DateTimeKind.Unspecified, backToStore.Kind);
    }

    [Fact]
    public void DisplayZoneRead_Server_SubtractsOffset()
    {
        /* A picker value the user typed in Server time maps back to the naive-UTC window bound. */
        var serverWallClock = new DateTime(2026, 7, 1, 7, 0, 0);   // 07:00 on a -05:00 server
        var expectedUtc = DateTime.SpecifyKind(new DateTime(2026, 7, 1, 12, 0, 0), DateTimeKind.Unspecified);
        var zone = ViewerTimeHelper.DisplayZoneFor(TimeDisplayMode.ServerTime, ServerClock.FixedOffset(-300));
        Assert.Equal(expectedUtc, DisplayZone.ToUtcBound(serverWallClock, zone, BoundSide.From));
    }

    // ── GetTimezoneLabel: the zone named beside a rendered time (#4766) ────────────────────────────────

    private static readonly ServerClock Eastern = ServerClock.Resolve("Eastern Standard Time", -240);

    [Fact]
    public void GetTimezoneLabel_Utc_IsUtc_WhateverTheServersClock()
    {
        Assert.Equal("UTC", ViewerTimeHelper.GetTimezoneLabel(TimeDisplayMode.UTC, Eastern, NaiveUtc));
        Assert.Equal("UTC", ViewerTimeHelper.GetTimezoneLabel(TimeDisplayMode.UTC, ServerClock.FixedOffset(330), NaiveUtc));
    }

    [Theory]
    [InlineData(2026, 1, 15)]   // mid-winter
    [InlineData(2026, 7, 15)]   // mid-summer: the machine zone's daylight name where it observes daylight saving
    public void GetTimezoneLabel_Local_IsTheMachinesZoneNameInForceAtThatInstant(int year, int month, int day)
    {
        var instant = new DateTime(year, month, day, 12, 0, 0, DateTimeKind.Unspecified);
        var zone = TimeZoneInfo.Local;
        var expected = zone.IsDaylightSavingTime(DateTime.SpecifyKind(instant, DateTimeKind.Utc))
            ? zone.DaylightName
            : zone.StandardName;

        Assert.Equal(expected, ViewerTimeHelper.GetTimezoneLabel(TimeDisplayMode.LocalTime, Eastern, instant));
    }

    [Theory]
    [InlineData(2026, 1, 15, "UTC-5:00")]   // EST
    [InlineData(2026, 7, 15, "UTC-4:00")]   // EDT
    public void GetTimezoneLabel_Server_NamesTheOffsetTheClockHadAtThatInstant(int year, int month, int day, string expected)
    {
        var instant = new DateTime(year, month, day, 12, 0, 0, DateTimeKind.Unspecified);

        Assert.Equal(expected, ViewerTimeHelper.GetTimezoneLabel(TimeDisplayMode.ServerTime, Eastern, instant));
    }

    [Fact]
    public void GetTimezoneLabel_Server_ChangesAtTheDaylightSavingChangeItself()
    {
        /* The 2026 US spring change is 02:00 EST = 07:00 UTC on 8 March. A label from the offset in force now would
           say the same thing on both sides of it. */
        var before = new DateTime(2026, 3, 8, 6, 59, 0, DateTimeKind.Unspecified);
        var after = new DateTime(2026, 3, 8, 7, 0, 0, DateTimeKind.Unspecified);

        Assert.Equal("UTC-5:00", ViewerTimeHelper.GetTimezoneLabel(TimeDisplayMode.ServerTime, Eastern, before));
        Assert.Equal("UTC-4:00", ViewerTimeHelper.GetTimezoneLabel(TimeDisplayMode.ServerTime, Eastern, after));
    }

    [Theory]
    [InlineData(0, "UTC+0:00")]
    [InlineData(330, "UTC+5:30")]     // India
    [InlineData(-210, "UTC-3:30")]    // Newfoundland
    [InlineData(-480, "UTC-8:00")]    // US Pacific standard
    [InlineData(780, "UTC+13:00")]    // Tonga
    public void GetTimezoneLabel_Server_FixedOffset_NamesTheOffset(int offsetMinutes, string expected)
    {
        Assert.Equal(
            expected,
            ViewerTimeHelper.GetTimezoneLabel(TimeDisplayMode.ServerTime, ServerClock.FixedOffset(offsetMinutes), NaiveUtc));
    }

    [Fact]
    public void ForDisplay_ReadsProcessWideStatics()
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedOffset = ViewerTimeHelper.UtcOffsetMinutes;
        try
        {
            ViewerTimeHelper.UtcOffsetMinutes = 90;

            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
            Assert.Equal(NaiveUtc.AddMinutes(90), ViewerTimeHelper.ForDisplay(NaiveUtc));

            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            Assert.Equal(NaiveUtc, ViewerTimeHelper.ForDisplay(NaiveUtc));

            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;
            Assert.Equal(DateTime.SpecifyKind(NaiveUtc, DateTimeKind.Utc).ToLocalTime(), ViewerTimeHelper.ForDisplay(NaiveUtc));
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.UtcOffsetMinutes = savedOffset;
        }
    }

    [Fact]
    public void CurrentDisplayZone_ReadsProcessWideStatics_AndRoundTripsForDisplay()
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedOffset = ViewerTimeHelper.UtcOffsetMinutes;
        try
        {
            ViewerTimeHelper.UtcOffsetMinutes = -300;
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

            var display = ViewerTimeHelper.ForDisplay(NaiveUtc);
            var zone = ViewerTimeHelper.CurrentDisplayZone();
            Assert.Equal(display, DisplayZone.ToDisplay(NaiveUtc, zone));
            Assert.Equal(NaiveUtc, DisplayZone.ToUtcBound(display, zone, BoundSide.From));
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.UtcOffsetMinutes = savedOffset;
        }
    }

    /// <summary>
    /// <c>ViewerDataService.FormatServerClock</c> — the renderer for a column already in the monitored
    /// server's own frame — HONOURS the display mode, through
    /// <see cref="ViewerTimeHelper.ForServerClockDisplay"/>. Server mode is the value itself; UTC mode is
    /// the value less the collected offset; Local mode is that instant in the viewer machine's zone.
    ///
    /// <para><b>The load-bearing assertion is that the modes DIFFER</b>, by exactly the offset, at the
    /// fleet's measured -240. A renderer that emitted the server's clock verbatim would satisfy the Server
    /// arm and nothing else, and it is untestable on this axis without this comparison — which is why the
    /// raw render went unnoticed until #3207 read the pair together.</para>
    ///
    /// <para>Lite's same-named <c>ServerTimeHelper.FormatServerClock</c> is the mirror: same input
    /// contract, and it reaches the modes through <c>ConvertForDisplay</c>, which starts from the server's
    /// clock instead of from naive UTC. One offset step between the frames, either way round.</para>
    /// </summary>
    [Fact]
    public void FormatServerClock_HonoursTheDisplayMode_AtTheFleetOffset()
    {
        var serverClock = new DateTime(2026, 7, 1, 13, 45, 7, DateTimeKind.Unspecified);
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedOffset = ViewerTimeHelper.UtcOffsetMinutes;
        try
        {
            ViewerTimeHelper.UtcOffsetMinutes = -240;

            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
            Assert.Equal("2026-07-01 13:45:07", ViewerDataService.FormatServerClock(serverClock));

            /* UTC mode is four hours LATER on this fleet: the stored value is utc + offset, so recovering
               UTC subtracts a negative offset. A sign error here renders 09:45:07 and is eight hours out. */
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            Assert.Equal("2026-07-01 17:45:07", ViewerDataService.FormatServerClock(serverClock));

            /* Local mode is that same instant in the viewer machine's zone, computed rather than pinned:
               CI and a developer machine are not in the same zone. */
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;
            Assert.Equal(
                DateTime.SpecifyKind(new DateTime(2026, 7, 1, 17, 45, 7), DateTimeKind.Utc)
                    .ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss"),
                ViewerDataService.FormatServerClock(serverClock));

            foreach (var mode in new[] { TimeDisplayMode.ServerTime, TimeDisplayMode.LocalTime, TimeDisplayMode.UTC })
            {
                ViewerTimeHelper.CurrentDisplayMode = mode;
                Assert.Equal("", ViewerDataService.FormatServerClock(null));
            }
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.UtcOffsetMinutes = savedOffset;
        }
    }

    [Fact]
    public void ServerUtcOffsetSql_ReadsLatestNonNullOffsetForServer()
    {
        var sql = ViewerDataService.ServerUtcOffsetSql;
        Assert.Contains("utc_offset_minutes", sql, StringComparison.Ordinal);
        Assert.Contains("FROM server_properties", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("utc_offset_minutes IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
        /* Darling has no v_server_properties view — the base table, bare-name resolved via the Search Path. */
        Assert.DoesNotContain("v_server_properties", sql, StringComparison.Ordinal);
    }
}
