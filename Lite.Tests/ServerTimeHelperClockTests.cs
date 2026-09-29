/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
        Assert.Equal(Utc(2026, 3, 8, 1, 30), ServerTimeHelper.ToServerTime(Utc(2026, 3, 8, 6, 30), clock));
        Assert.Equal(Utc(2026, 3, 8, 3, 30), ServerTimeHelper.ToServerTime(Utc(2026, 3, 8, 7, 30), clock));

        /* Far side of the change: a July time is on daylight time, four hours behind UTC. */
        Assert.Equal(Utc(2026, 7, 1, 8, 0), ServerTimeHelper.ToServerTime(Utc(2026, 7, 1, 12, 0), clock));

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
        Assert.Equal(Utc(2026, 11, 1, 1, 30), ServerTimeHelper.ToServerTime(Utc(2026, 11, 1, 5, 30), clock));
        Assert.Equal(Utc(2026, 11, 1, 1, 30), ServerTimeHelper.ToServerTime(Utc(2026, 11, 1, 6, 30), clock));
        Assert.Equal(Utc(2026, 11, 1, 2, 30), ServerTimeHelper.ToServerTime(Utc(2026, 11, 1, 7, 30), clock));
    }

    [Fact]
    public void ServerWithNoZoneId_KeepsItsFixedOffset_OnBothSidesOfTheChange()
    {
        var clock = ServerClock.Resolve(null, WinterOffset);

        Assert.Equal(Utc(2026, 3, 8, 2, 30), ServerTimeHelper.ToServerTime(Utc(2026, 3, 8, 7, 30), clock));
        Assert.Equal(Utc(2026, 7, 1, 7, 0), ServerTimeHelper.ToServerTime(Utc(2026, 7, 1, 12, 0), clock));
        Assert.Equal(Utc(2026, 7, 1, 12, 0), clock.ToUtc(Utc(2026, 7, 1, 7, 0)));

        /* The UtcOffsetMinutes setter is the same fixed clock, whatever zone the machine was in. */
        ServerTimeHelper.UtcOffsetMinutes = WinterOffset;
        Assert.Equal(WinterOffset, ServerTimeHelper.UtcOffsetMinutes);
        Assert.Equal(Utc(2026, 7, 1, 7, 0), ServerTimeHelper.ToServerTime(Utc(2026, 7, 1, 12, 0)));
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
    }

    [Fact]
    public void UtcDisplay_RoundTripsTheServerClock_OnBothSidesOfTheChange()
    {
        var clock = Eastern();

        /* Chart X values are the server's wall clock and the axis labels convert them back for UTC mode: the pair
           has to cancel on BOTH sides of the change, not just on the side the snapshot offset came from. */
        foreach (var utc in new[] { Utc(2026, 3, 8, 6, 30), Utc(2026, 3, 8, 7, 30), Utc(2026, 7, 1, 12, 0), Utc(2026, 11, 1, 5, 30), Utc(2026, 11, 1, 7, 30) })
        {
            var serverLocal = ServerTimeHelper.ToServerTime(utc, clock);
            Assert.Equal(utc, ServerTimeHelper.ConvertForDisplay(serverLocal, TimeDisplayMode.UTC, clock));
        }

        // Known limit (#4766): in the repeated autumn hour a server-local time resolves to its first occurrence.
        var secondOccurrence = Utc(2026, 11, 1, 6, 30);
        var repeatedLocal = ServerTimeHelper.ToServerTime(secondOccurrence, clock);
        Assert.Equal(Utc(2026, 11, 1, 1, 30), repeatedLocal);
        Assert.Equal(Utc(2026, 11, 1, 5, 30), ServerTimeHelper.ConvertForDisplay(repeatedLocal, TimeDisplayMode.UTC, clock));
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
        var body = refresh[refresh.IndexOf("private async System.Threading.Tasks.Task RefreshAllDataAsync()", StringComparison.Ordinal)..];
        var readClock = body.IndexOf("await RefreshServerClockAsync();", StringComparison.Ordinal);
        var window = body.IndexOf("GetCurrentWindow(", StringComparison.Ordinal);
        Assert.True(readClock >= 0 && window > readClock,
            "RefreshAllDataAsync has to read the server clock again before it derives the window (#4766).");

        var selected = File.ReadAllText(Path.Combine(controls, "..", "MainWindow.xaml.cs"));
        Assert.Contains("ServerTimeHelper.ActiveServerClock = serverTab.ServerClock;", selected, StringComparison.Ordinal);
    }

    private static string ControlsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));

    private static string WindowsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Windows"));
}
