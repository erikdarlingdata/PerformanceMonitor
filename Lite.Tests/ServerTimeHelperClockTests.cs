/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: Server-time mode follows the server's time zone across a daylight-saving change.
///
/// <para><c>ServerTimeHelper</c> used to hold ONE offset and add it to every time, so a time on the far side of
/// a clock change showed an hour off the server's wall clock. It now converts through a
/// <see cref="ServerClock"/>: the zone id when the engine reported one (SQL Server 2022 and later), else the
/// fixed offset. These tests use US Eastern time, whose 2026 changes are 8 March (02:00 EST jumps to 03:00 EDT,
/// 07:00 UTC) and 1 November (02:00 EDT falls back to 01:00 EST, 06:00 UTC), and pin the same never-throw rule
/// the Darling viewer's <c>ServerClock</c> tests do: a skipped local time moves forward by the gap and a
/// repeated one takes its first occurrence.</para>
/// </summary>
/* Installs ServerTimeHelper.ActiveServerClock and CurrentDisplayMode, process-wide mutable statics; joins the
   collection every other class that writes them uses. */
[Collection("server-time-helper")]
public sealed class ServerTimeHelperClockTests : IDisposable
{
    private const string EasternZone = "Eastern Standard Time";

    /* The offset a snapshot taken in winter recorded. The zone must win over it, or this is the old one-offset code. */
    private const int WinterOffset = -300;

    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;

    public void Dispose()
    {
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
    }

    private static ServerClock Eastern() => ServerClock.Resolve(EasternZone, WinterOffset);

    private static DateTime Utc(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    [Fact]
    public void ServerMode_ShowsTheServersWallClock_OnEachSideOfSpringForward()
    {
        var clock = Eastern();

        /* 06:30 UTC on 8 March is 01:30 EST; 07:30 UTC is 03:30 EDT (02:xx never happened). */
        Assert.Equal(Utc(2026, 3, 8, 1, 30), clock.ToServerLocal(Utc(2026, 3, 8, 6, 30)));
        Assert.Equal(Utc(2026, 3, 8, 3, 30), clock.ToServerLocal(Utc(2026, 3, 8, 7, 30)));

        /* Far side of the change: a July time is on daylight time, four hours behind UTC. */
        Assert.Equal(Utc(2026, 7, 1, 8, 0), clock.ToServerLocal(Utc(2026, 7, 1, 12, 0)));

        /* And through the process-wide clock + the rendered string the grids use. */
        ServerTimeHelper.ActiveServerClock = clock;
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("2026-03-08 01:30:00", ServerTimeHelper.FormatServerTime(Utc(2026, 3, 8, 6, 30)));
        Assert.Equal("2026-03-08 03:30:00", ServerTimeHelper.FormatServerTime(Utc(2026, 3, 8, 7, 30)));
    }

    [Fact]
    public void ServerMode_ShowsTheServersWallClock_OnEachSideOfFallBack()
    {
        var clock = Eastern();

        /* 05:30 UTC on 1 November is 01:30 EDT, 06:30 UTC is 01:30 EST (the repeated hour), 07:30 UTC is 02:30 EST. */
        Assert.Equal(Utc(2026, 11, 1, 1, 30), clock.ToServerLocal(Utc(2026, 11, 1, 5, 30)));
        Assert.Equal(Utc(2026, 11, 1, 1, 30), clock.ToServerLocal(Utc(2026, 11, 1, 6, 30)));
        Assert.Equal(Utc(2026, 11, 1, 2, 30), clock.ToServerLocal(Utc(2026, 11, 1, 7, 30)));

        /* And the rendered string the grids use. Both instants of the repeated hour read "01:30" on the server's clock,
           so the wall time cannot tell 05:30 UTC from 06:30 UTC; the text adds the instant's UTC offset for those two
           and only those (#4766). */
        ServerTimeHelper.ActiveServerClock = clock;
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("2026-11-01 01:30:00 -04:00", ServerTimeHelper.FormatServerTime(Utc(2026, 11, 1, 5, 30)));
        Assert.Equal("2026-11-01 01:30:00 -05:00", ServerTimeHelper.FormatServerTime(Utc(2026, 11, 1, 6, 30)));
        Assert.Equal("2026-11-01 02:30:00", ServerTimeHelper.FormatServerTime(Utc(2026, 11, 1, 7, 30)));
    }

    [Fact]
    public void ServerWithNoZoneId_KeepsItsFixedOffset_OnBothSidesOfTheChange()
    {
        var clock = ServerClock.Resolve(null, WinterOffset);

        Assert.Equal(Utc(2026, 3, 8, 2, 30), clock.ToServerLocal(Utc(2026, 3, 8, 7, 30)));
        Assert.Equal(Utc(2026, 7, 1, 7, 0), clock.ToServerLocal(Utc(2026, 7, 1, 12, 0)));
        Assert.Equal(Utc(2026, 7, 1, 12, 0), clock.ToUtc(Utc(2026, 7, 1, 7, 0)));

        /* The UtcOffsetMinutes setter is the same fixed clock, whatever zone the machine was in. */
        ServerTimeHelper.UtcOffsetMinutes = WinterOffset;
        Assert.Equal(WinterOffset, ServerTimeHelper.UtcOffsetMinutes);
        Assert.Equal(Utc(2026, 7, 1, 7, 0), ServerTimeHelper.ActiveServerClock.ToServerLocal(Utc(2026, 7, 1, 12, 0)));
    }

    [Fact]
    public void UtcOffsetMinutes_ReportsTheOffsetInForceNow_ForAZoneClock()
    {
        var clock = Eastern();
        ServerTimeHelper.ActiveServerClock = clock;

        var expected = clock.OffsetMinutesAt(DateTime.UtcNow);
        Assert.Equal(expected, ServerTimeHelper.UtcOffsetMinutes);
        Assert.True(expected is -300 or -240, $"Eastern time is UTC-5 or UTC-4, not {expected}");
    }

    [Fact]
    public void PickerInverse_SkippedLocalTime_MovesForwardByTheGap_AndDoesNotThrow()
    {
        var clock = Eastern();

        /* 02:30 on 8 March never happened in New York. */
        var skipped = Utc(2026, 3, 8, 2, 30);
        Assert.Equal(Utc(2026, 3, 8, 7, 30), clock.ToUtc(skipped));

        /* UTC and Local displays of that same server time do not throw either. */
        Assert.Equal(Utc(2026, 3, 8, 7, 30), ServerTimeHelper.ConvertForDisplay(skipped, TimeDisplayMode.UTC, clock));
        _ = ServerTimeHelper.ConvertForDisplay(skipped, TimeDisplayMode.LocalTime, clock);
    }

    [Fact]
    public void PickerInverse_RepeatedLocalTime_TakesTheFirstOccurrence_AndDoesNotThrow()
    {
        var clock = Eastern();

        /* 01:30 on 1 November happens twice, at 05:30 UTC (EDT) and 06:30 UTC (EST): the first one wins. */
        var repeated = Utc(2026, 11, 1, 1, 30);
        Assert.Equal(Utc(2026, 11, 1, 5, 30), clock.ToUtc(repeated));
        Assert.Equal(Utc(2026, 11, 1, 5, 30), ServerTimeHelper.ConvertForDisplay(repeated, TimeDisplayMode.UTC, clock));

        /* The second occurrence, 06:30 UTC, is the same 01:30 on the server's clock, so this inverse cannot name it. A
           range bound typed as 01:30 is the one place that can: it takes the first occurrence for the start of a
           range and the second for the end (#4766). */
        Assert.Equal(repeated, clock.ToServerLocal(Utc(2026, 11, 1, 6, 30)));
        Assert.Equal(Utc(2026, 11, 1, 5, 30), clock.ToUtc(clock.ToServerLocal(Utc(2026, 11, 1, 6, 30))));
        Assert.Equal(Utc(2026, 11, 1, 5, 30), DisplayZone.ToUtcBound(repeated, clock.AsTimeZone(), BoundSide.From));
        Assert.Equal(Utc(2026, 11, 1, 6, 30), DisplayZone.ToUtcBound(repeated, clock.AsTimeZone(), BoundSide.To));
    }

    [Fact]
    public void UtcDisplay_RoundTripsTheServerClock_OnBothSidesOfTheChange()
    {
        var clock = Eastern();

        /* A stored server-local stamp shown in UTC mode converts back through the server's clock: the pair has to
           cancel on BOTH sides of the change, not just on the side the snapshot offset came from. */
        foreach (var utc in new[] { Utc(2026, 3, 8, 6, 30), Utc(2026, 3, 8, 7, 30), Utc(2026, 7, 1, 12, 0), Utc(2026, 11, 1, 5, 30), Utc(2026, 11, 1, 7, 30) })
        {
            var serverLocal = clock.ToServerLocal(utc);
            Assert.Equal(utc, ServerTimeHelper.ConvertForDisplay(serverLocal, TimeDisplayMode.UTC, clock));
        }

        // Known limit (#4766): in the repeated autumn hour a stored server-local stamp resolves to its first occurrence.
        var secondOccurrence = Utc(2026, 11, 1, 6, 30);
        var repeatedLocal = clock.ToServerLocal(secondOccurrence);
        Assert.Equal(Utc(2026, 11, 1, 1, 30), repeatedLocal);
        Assert.Equal(Utc(2026, 11, 1, 5, 30), ServerTimeHelper.ConvertForDisplay(repeatedLocal, TimeDisplayMode.UTC, clock));
    }

    [Fact]
    public void FormatServerClock_OnAnExplicitClock_ConvertsOnThatClock_NotTheActiveOne()
    {
        var eastern = Eastern();
        ServerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(540);
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;

        /* 08:00 on the server's July wall clock is 12:00Z on US Eastern (UTC-4) and 23:00Z the day before on the active
           clock (UTC+9): the explicit clock decides. */
        Assert.Equal("2026-07-01 12:00:00", ServerTimeHelper.FormatServerClock(Utc(2026, 7, 1, 8, 0), eastern));
        Assert.Equal("2026-06-30 23:00:00", ServerTimeHelper.FormatServerClock(Utc(2026, 7, 1, 8, 0)));
        Assert.Equal("2026-07-01 12:00", ServerTimeHelper.FormatServerClock(Utc(2026, 7, 1, 8, 0), eastern, "yyyy-MM-dd HH:mm"));
        Assert.Equal("", ServerTimeHelper.FormatServerClock((DateTime?)null, eastern));

        /* Server mode shows the wall time as it stands, whichever clock it is on. */
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("2026-07-01 08:00:00", ServerTimeHelper.FormatServerClock(Utc(2026, 7, 1, 8, 0), eastern));
    }

    // ── the display zone of a mode, and the renderers that go through it (#4766) ──────────

    [Fact]
    public void DisplayZoneFor_IsUtc_ThisMachinesZone_OrTheServersOwnClock()
    {
        var clock = Eastern();

        Assert.Equal(TimeZoneInfo.Utc, ServerTimeHelper.DisplayZoneFor(TimeDisplayMode.UTC, clock));
        Assert.Equal(TimeZoneInfo.Local, ServerTimeHelper.DisplayZoneFor(TimeDisplayMode.LocalTime, clock));
        Assert.Equal(clock.AsTimeZone(), ServerTimeHelper.DisplayZoneFor(TimeDisplayMode.ServerTime, clock));

        /* A server with no zone id is a fixed-offset zone. */
        var fixedZone = ServerTimeHelper.DisplayZoneFor(TimeDisplayMode.ServerTime, ServerClock.FixedOffset(330));
        Assert.Equal(TimeSpan.FromMinutes(330), fixedZone.BaseUtcOffset);
    }

    /// <summary>
    /// 05:30Z and 06:30Z on the autumn change day are the two occurrences of 01:30 on a US Eastern wall clock. The
    /// renderer used to go UTC to the server's wall clock and back out of it for the UTC and Local modes, and a wall
    /// clock cannot say which occurrence it was: 06:30Z read 05:30 in UTC mode. It now converts the instant into the
    /// zone of the mode in one step. The two occurrences of 01:30 in Server mode read the same wall time, so the text
    /// adds each one's UTC offset: "-04:00" for 05:30Z (EDT) and "-05:00" for 06:30Z (EST). UTC mode and every time
    /// outside the repeated hour stay the bare text.
    /// </summary>
    [Fact]
    public void FormatServerTime_InTheRepeatedHour_ShowsTheInstantInUtcMode_AndTheWallClockWithItsOffsetInServerMode()
    {
        ServerTimeHelper.ActiveServerClock = Eastern();
        var first = Utc(2026, 11, 1, 5, 30);
        var second = Utc(2026, 11, 1, 6, 30);

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        Assert.Equal("2026-11-01 05:30:00", ServerTimeHelper.FormatServerTime(first));
        Assert.Equal("2026-11-01 06:30:00", ServerTimeHelper.FormatServerTime(second));
        Assert.Equal("2026-11-01 06:30:00", ServerTimeHelper.FormatServerTime((DateTime?)second));

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("2026-11-01 01:30:00 -04:00", ServerTimeHelper.FormatServerTime(first));
        Assert.Equal("2026-11-01 01:30:00 -05:00", ServerTimeHelper.FormatServerTime(second));
        Assert.Equal("2026-11-01 01:30:00 -05:00", ServerTimeHelper.FormatServerTime((DateTime?)second));
        Assert.Equal("01:30 -05:00", ServerTimeHelper.FormatServerTime(second, "HH:mm"));
        Assert.Equal("", ServerTimeHelper.FormatServerTime((DateTime?)null));

        /* The hour before, the hour after, and an ordinary summer time carry no offset. */
        Assert.Equal("2026-11-01 00:30:00", ServerTimeHelper.FormatServerTime(Utc(2026, 11, 1, 4, 30)));
        Assert.Equal("2026-11-01 02:30:00", ServerTimeHelper.FormatServerTime(Utc(2026, 11, 1, 7, 30)));
        Assert.Equal("2026-07-01 08:00:00", ServerTimeHelper.FormatServerTime(Utc(2026, 7, 1, 12, 0)));
    }

    /// <summary>
    /// The format string still runs on the current culture, as it did before the offset was added; only the offset
    /// is culture-free. A culture with a "." time separator words "HH:mm" as "01.30".
    /// </summary>
    [Fact]
    public void FormatServerTime_InTheRepeatedHour_AppliesTheFormatOnTheCurrentCulture_AndTheOffsetIsCultureFree()
    {
        ServerTimeHelper.ActiveServerClock = Eastern();
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        var dotted = (CultureInfo)CultureInfo.InvariantCulture.Clone();
        dotted.DateTimeFormat.TimeSeparator = ".";
        var saved = CultureInfo.CurrentCulture;

        try
        {
            CultureInfo.CurrentCulture = dotted;

            Assert.Equal("01.30 -05:00", ServerTimeHelper.FormatServerTime(Utc(2026, 11, 1, 6, 30), "HH:mm"));
            Assert.Equal("02.30", ServerTimeHelper.FormatServerTime(Utc(2026, 11, 1, 7, 30), "HH:mm"));
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    /// <summary>
    /// Local mode is the machine's zone at each instant, so the two occurrences stay two instants. On a machine whose
    /// own clock repeats an hour at these instants (a US Eastern desktop) the text carries that offset too, so the
    /// expected text names it the same way.
    /// </summary>
    [Fact]
    public void FormatServerTime_InLocalMode_IsThisMachinesZoneAtEachInstant()
    {
        ServerTimeHelper.ActiveServerClock = Eastern();
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;

        foreach (var utc in new[] { Utc(2026, 11, 1, 5, 30), Utc(2026, 11, 1, 6, 30), Utc(2026, 7, 1, 12, 0) })
        {
            Assert.Equal(MachineWallText(utc), ServerTimeHelper.FormatServerTime(utc));
        }
    }

    /// <summary>
    /// A fixed-offset server clock (a server that reported no time zone id) has no repeated hour, so it never takes the
    /// offset: at 05:30Z and 06:30Z on the day a US zone falls back, a -05:00 clock reads 00:30 and 01:30, each once. The
    /// same instants on a zone clock read 01:30 twice.
    /// </summary>
    [Fact]
    public void FormatServerTime_OnAFixedOffsetClock_NeverAddsAnOffset_AtTheInstantsWhereAUsZoneRepeatsAnHour()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        ServerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(-300);
        Assert.Equal("2026-11-01 00:30:00", ServerTimeHelper.FormatServerTime(Utc(2026, 11, 1, 5, 30)));
        Assert.Equal("2026-11-01 01:30:00", ServerTimeHelper.FormatServerTime(Utc(2026, 11, 1, 6, 30)));
        Assert.Equal("01:30", ServerTimeHelper.FormatServerTime(Utc(2026, 11, 1, 6, 30), "HH:mm"));

        ServerTimeHelper.ActiveServerClock = Eastern();
        Assert.Equal("2026-11-01 01:30:00 -04:00", ServerTimeHelper.FormatServerTime(Utc(2026, 11, 1, 5, 30)));
        Assert.Equal("2026-11-01 01:30:00 -05:00", ServerTimeHelper.FormatServerTime(Utc(2026, 11, 1, 6, 30)));
    }

    /// <summary>
    /// A zone east of UTC repeats its hour at a different UTC instant and with positive offsets: Berlin falls back at
    /// 01:00Z on 2026-10-25 (03:00 CEST to 02:00 CET), so 02:30 happens at 00:30Z (+02:00) and again at 01:30Z (+01:00).
    /// </summary>
    [Fact]
    public void FormatServerTime_OnAZoneEastOfUtc_GivesTheTwoPassesOfTheRepeatedHourTheirPositiveOffsets()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        ServerTimeHelper.ActiveServerClock = ServerClock.Resolve("W. Europe Standard Time", 60);

        Assert.Equal("2026-10-25 02:30:00 +02:00", ServerTimeHelper.FormatServerTime(Utc(2026, 10, 25, 0, 30)));
        Assert.Equal("2026-10-25 02:30:00 +01:00", ServerTimeHelper.FormatServerTime(Utc(2026, 10, 25, 1, 30)));

        /* The hour before and the hour after are bare. */
        Assert.Equal("2026-10-25 01:59:00", ServerTimeHelper.FormatServerTime(Utc(2026, 10, 24, 23, 59)));
        Assert.Equal("2026-10-25 03:00:00", ServerTimeHelper.FormatServerTime(Utc(2026, 10, 25, 2, 0)));
    }

    /// <summary>
    /// The text this machine's own zone gives the naive-UTC instant <paramref name="utc"/>: its wall clock, then a
    /// space and its UTC offset when that wall time happens twice on this machine. Worked out from the framework's
    /// own zone functions, not from the renderer it checks.
    /// </summary>
    internal static string MachineWallText(DateTime utc)
    {
        var instant = DateTime.SpecifyKind(utc, DateTimeKind.Utc);
        var wall = TimeZoneInfo.ConvertTimeFromUtc(instant, TimeZoneInfo.Local);
        var text = wall.ToString("yyyy-MM-dd HH:mm:ss");
        if (!TimeZoneInfo.Local.IsAmbiguousTime(wall))
        {
            return text;
        }

        var offset = TimeZoneInfo.Local.GetUtcOffset(instant);
        return text + " " + (offset < TimeSpan.Zero ? "-" : "+") + offset.Duration().ToString(@"hh\:mm");
    }

    /// <summary>
    /// The label names the zone AT THE INSTANT it labels: a US Eastern clock reads "UTC-4:00" in July and "UTC-5:00"
    /// in December, not the zone's standard name ("Eastern Standard Time" reads wrong on a July time) and not the
    /// offset in force now. The active clock is another one entirely, to show the label reads the clock it is given.
    /// </summary>
    [Fact]
    public void GetTimezoneLabel_ServerMode_ShowsTheOffsetAtTheInstant_OnEachSideOfADaylightSavingChange()
    {
        var clock = Eastern();
        ServerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(540);

        Assert.Equal("UTC-4:00", ServerTimeHelper.GetTimezoneLabel(TimeDisplayMode.ServerTime, clock, Utc(2026, 7, 1, 12, 0)));
        Assert.Equal("UTC-5:00", ServerTimeHelper.GetTimezoneLabel(TimeDisplayMode.ServerTime, clock, Utc(2026, 12, 1, 12, 0)));

        /* The repeated hour: 05:30Z is still on daylight time, 06:30Z is not. */
        Assert.Equal("UTC-4:00", ServerTimeHelper.GetTimezoneLabel(TimeDisplayMode.ServerTime, clock, Utc(2026, 11, 1, 5, 30)));
        Assert.Equal("UTC-5:00", ServerTimeHelper.GetTimezoneLabel(TimeDisplayMode.ServerTime, clock, Utc(2026, 11, 1, 6, 30)));
    }

    [Theory]
    [InlineData(-300, "UTC-5:00")]
    [InlineData(330, "UTC+5:30")]
    [InlineData(-570, "UTC-9:30")]
    [InlineData(-30, "UTC-0:30")]
    [InlineData(0, "UTC+0:00")]
    public void GetTimezoneLabel_ServerMode_ShowsTheOffset_OfAFixedOffsetClock(int offsetMinutes, string expected)
    {
        Assert.Equal(
            expected,
            ServerTimeHelper.GetTimezoneLabel(TimeDisplayMode.ServerTime, ServerClock.FixedOffset(offsetMinutes), Utc(2026, 7, 1, 12, 0)));
    }

    /// <summary>
    /// A zone that never observes daylight saving has one offset for all time, so the label is the same on either
    /// side of the year.
    /// </summary>
    [Fact]
    public void GetTimezoneLabel_ServerMode_ShowsTheOffset_OfAZoneWithNoDaylightSaving()
    {
        var clock = ServerClock.Resolve("India Standard Time", 330);

        Assert.Equal("UTC+5:30", ServerTimeHelper.GetTimezoneLabel(TimeDisplayMode.ServerTime, clock, Utc(2026, 7, 1, 12, 0)));
        Assert.Equal("UTC+5:30", ServerTimeHelper.GetTimezoneLabel(TimeDisplayMode.ServerTime, clock, Utc(2026, 12, 1, 12, 0)));
    }

    /// <summary>
    /// UTC mode is "UTC" whatever the server's clock is. Local mode names this machine's zone at the instant, its
    /// daylight name in summer and its standard name in winter, and not the server's zone.
    /// </summary>
    [Fact]
    public void GetTimezoneLabel_UtcAndLocalModes_AreNotTheServers()
    {
        var clock = Eastern();
        var summer = Utc(2026, 7, 1, 12, 0);
        var winter = Utc(2026, 12, 1, 12, 0);

        Assert.Equal("UTC", ServerTimeHelper.GetTimezoneLabel(TimeDisplayMode.UTC, clock, summer));
        Assert.Equal("UTC", ServerTimeHelper.GetTimezoneLabel(TimeDisplayMode.UTC, clock, winter));

        var local = TimeZoneInfo.Local;
        Assert.Equal(
            local.IsDaylightSavingTime(DateTime.SpecifyKind(summer, DateTimeKind.Utc)) ? local.DaylightName : local.StandardName,
            ServerTimeHelper.GetTimezoneLabel(TimeDisplayMode.LocalTime, clock, summer));
        Assert.Equal(
            local.IsDaylightSavingTime(DateTime.SpecifyKind(winter, DateTimeKind.Utc)) ? local.DaylightName : local.StandardName,
            ServerTimeHelper.GetTimezoneLabel(TimeDisplayMode.LocalTime, clock, winter));
    }

    [Fact]
    public void ClockForServer_TakesTheCollectedClock_ThenTheOpenTabs_ThenTheMachines()
    {
        var collected = Eastern();
        var openTab = ServerClock.FixedOffset(330);
        var machine = TimeZoneInfo.CreateCustomTimeZone("machine-minus-4", TimeSpan.FromHours(-4), "machine -4", "machine -4");
        var now = new DateTime(2026, 6, 1, 15, 0, 0, DateTimeKind.Utc);

        Assert.Same(collected, ServerTimeHelper.ClockForServer(collected, openTab, machine, now));
        Assert.Same(collected, ServerTimeHelper.ClockForServer(collected, null, machine, now));
        Assert.Same(openTab, ServerTimeHelper.ClockForServer(null, openTab, machine, now));

        var fallback = ServerTimeHelper.ClockForServer(null, null, machine, now);
        Assert.Equal(-240, fallback.OffsetMinutesAt(now));
        Assert.Equal(Utc(2026, 6, 1, 11, 0), fallback.ToServerLocal(now));
    }

    [Fact]
    public void ClockForServer_OfNothing_IsThisMachinesOffsetNow_NotUtc()
    {
        var clock = ServerTimeHelper.ClockForServer(null, null);

        var now = DateTime.UtcNow;
        Assert.Equal((int)TimeZoneInfo.Local.GetUtcOffset(now).TotalMinutes, clock.OffsetMinutesAt(now));
    }

    /* Files under Lite/Controls and Lite/Windows whose code may still add one UTC offset to a time, each with the
       reason it is right to. Empty on purpose: every chart time in the tabs, the lanes and the History windows
       converts through the server clock, and a file that keeps a one-offset AddMinutes must be listed here with why. */
    private static readonly Dictionary<string, string> OneOffsetExceptions = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// The tabs, the Overview lanes and the History windows convert each chart time through the clock instead of
    /// adding one offset to all of them, and the tab reads the clock again on every refresh before it derives the
    /// window. A source pin, because these are WPF controls this suite does not instantiate. The scan covers every
    /// file in Lite/Controls and Lite/Windows and every spelling of the offset (a <c>UtcOffsetMinutes</c> property,
    /// with or without <c>Services.</c> in front, or a <c>utcOffset</c> local); a file it still finds must be listed
    /// in <see cref="OneOffsetExceptions"/> with its reason, so a hit is never a silent pass.
    /// </summary>
    [Fact]
    public void ServerTab_ConvertsThroughTheClock_AndRereadsItOnEveryRefresh()
    {
        var controls = ControlsDir();
        var oneOffset = new Regex(@"\.AddMinutes\(-?\s*((Services\.)?ServerTimeHelper\.)?[uU]tcOffset(Minutes)?\)");
        var scanned = Directory.GetFiles(controls, "*.cs").Concat(Directory.GetFiles(WindowsDir(), "*.cs")).ToList();
        Assert.Contains(scanned, f => Path.GetFileName(f) == "CorrelatedTimelineLanesControl.xaml.cs");
        Assert.Contains(scanned, f => Path.GetFileName(f) == "ProcedureHistoryWindow.xaml.cs");

        foreach (var file in scanned)
        {
            var name = Path.GetFileName(file);
            var hit = oneOffset.IsMatch(File.ReadAllText(file));
            if (OneOffsetExceptions.ContainsKey(name))
            {
                Assert.True(hit, $"{name} is listed as a one-offset exception but no longer has one; remove the entry.");
                continue;
            }

            Assert.False(hit, $"{name} still adds one UTC offset to a time; convert through the server clock (#4766).");
        }

        var refresh = File.ReadAllText(Path.Combine(controls, "ServerTab.Refresh.cs"));
        /* #5371 moved the full pass's body out of RefreshAllDataAsync (now a one-line request to the refresh coordinator)
           into RefreshEverythingAsync; the clock-before-window order the pin is about is unchanged. */
        var body = refresh[refresh.IndexOf("private async Task RefreshEverythingAsync(", StringComparison.Ordinal)..];
        var readClock = body.IndexOf("await RefreshServerClockAsync();", StringComparison.Ordinal);
        var window = body.IndexOf("GetCurrentWindowUtc(", StringComparison.Ordinal);
        Assert.True(readClock >= 0 && window > readClock,
            "The full refresh pass has to read the server clock again before it derives the window (#4766).");

        var selected = File.ReadAllText(Path.Combine(controls, "..", "MainWindow.xaml.cs"));
        Assert.Contains("ServerTimeHelper.ActiveServerClock = serverTab.ServerClock;", selected, StringComparison.Ordinal);
    }

    private static string ControlsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));

    private static string WindowsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Windows"));
}
