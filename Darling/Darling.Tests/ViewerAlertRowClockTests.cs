/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4766: the viewer's Alerts History time and Manage Servers' "Last Collected" show one UTC instant, so in the repeated
/// autumn hour they carry the instant's UTC offset, as Lite's alert row does. Both used to build
/// <c>ConvertToDisplay(...).ToString(format)</c> themselves, so the alert at 05:30Z and the one at 06:30Z on 2026-11-01
/// both read "01:30" on a US Eastern server. They now word the text through
/// <see cref="ViewerTimeHelper.FormatForDisplay(DateTime, TimeZoneInfo, string)"/>, the twin of Lite's
/// <c>ServerTimeHelper.FormatInstant</c>, which the current-zone overload calls too, so there is one rule.
///
/// <para>US Eastern is the server: the autumn change is 2026-11-01 at 06:00 UTC, so 05:30Z is the first 01:30 (-04:00)
/// and 06:30Z the second (-05:00).</para>
/// </summary>
/* Flips the process-wide display mode and active server clock (each restored in Dispose); one shared collection
   serializes it with every other class that does. */
[Collection("viewer-time-statics")]
public sealed class ViewerAlertRowClockTests : IDisposable
{
    private static readonly ServerClock Eastern = ServerClock.Resolve("Eastern Standard Time", -300);

    private static readonly ServerClock India = ServerClock.Resolve("India Standard Time", 330);

    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;
    private readonly ServerClock _savedClock = ViewerTimeHelper.ActiveServerClock;
    private readonly CultureInfo _savedCulture = CultureInfo.CurrentCulture;

    public ViewerAlertRowClockTests()
    {
        /* The text runs on the current culture, as every grid's does; the invariant one fixes its separators. */
        CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
    }

    public void Dispose()
    {
        ViewerTimeHelper.CurrentDisplayMode = _savedMode;
        ViewerTimeHelper.ActiveServerClock = _savedClock;
        CultureInfo.CurrentCulture = _savedCulture;
    }

    private static DateTime Utc(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    private static ViewerAlertRow Alert(DateTime alertTimeUtc, ServerClock? clock) => new()
    {
        AlertTime = alertTimeUtc,
        ServerId = 1,
        MetricName = "High CPU",
        CurrentValue = 95,
        ThresholdValue = 90,
        AlertSent = false,
        NotificationType = "tray",
        Muted = false,
        Clock = clock,
    };

    private static ManagedServerListItem Server(DateTime collectedUtc, ServerClock? clock) =>
        new(new MonitoredServerRow { ServerId = 1, Host = "SQL1", Name = "SQL1" }, isFavorite: false)
        {
            LastCollectedUtc = collectedUtc,
            Clock = clock,
        };

    /// <summary>
    /// The alert at 05:30Z reads 01:30 -04:00 and the one at 06:30Z reads 01:30 -05:00 in Server mode, the hour after
    /// is the bare 02:30, and UTC mode is the stored instant with no offset.
    /// </summary>
    [Fact]
    public void AlertRow_InTheRepeatedHour_ReadsItsOffsetInServerMode_AndTheStoredInstantInUtcMode()
    {
        ViewerTimeHelper.ActiveServerClock = India;
        var first = Alert(Utc(2026, 11, 1, 5, 30), Eastern);
        var second = Alert(Utc(2026, 11, 1, 6, 30), Eastern);
        var after = Alert(Utc(2026, 11, 1, 7, 30), Eastern);

        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("2026-11-01 01:30:00 -04:00", first.TimeLocal);
        Assert.Equal("2026-11-01 01:30:00 -05:00", second.TimeLocal);
        Assert.Equal("2026-11-01 02:30:00", after.TimeLocal);

        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        Assert.Equal("2026-11-01 05:30:00", first.TimeLocal);
        Assert.Equal("2026-11-01 06:30:00", second.TimeLocal);
    }

    /// <summary>A row built without a clock reads on the active server's, the fallback it had before.</summary>
    [Fact]
    public void AlertRow_WithNoClockStamped_TakesTheActiveServersClock()
    {
        ViewerTimeHelper.ActiveServerClock = Eastern;
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        Assert.Equal("2026-11-01 01:30:00 -05:00", Alert(Utc(2026, 11, 1, 6, 30), clock: null).TimeLocal);
        Assert.Equal("2026-07-01 08:00:00", Alert(Utc(2026, 7, 1, 12, 0), clock: null).TimeLocal);
    }

    /// <summary>Manage Servers' Last Collected takes the same pair, in its own "yyyy-MM-dd HH:mm" shape.</summary>
    [Fact]
    public void ManageServersLastCollected_InTheRepeatedHour_ReadsItsOffsetInServerMode_AndTheStoredInstantInUtcMode()
    {
        ViewerTimeHelper.ActiveServerClock = India;
        var first = Server(Utc(2026, 11, 1, 5, 30), Eastern);
        var second = Server(Utc(2026, 11, 1, 6, 30), Eastern);
        var after = Server(Utc(2026, 11, 1, 7, 30), Eastern);

        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("2026-11-01 01:30 -04:00", first.LastCollectedDisplay);
        Assert.Equal("2026-11-01 01:30 -05:00", second.LastCollectedDisplay);
        Assert.Equal("2026-11-01 02:30", after.LastCollectedDisplay);

        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        Assert.Equal("2026-11-01 05:30", first.LastCollectedDisplay);
        Assert.Equal("2026-11-01 06:30", second.LastCollectedDisplay);
    }

    /// <summary>
    /// The zone overload is the one rule: an explicit zone words the instant the same way the current-zone overload does
    /// for that zone, UTC never takes an offset, and a wall time that happens once stays bare.
    /// </summary>
    [Fact]
    public void FormatForDisplay_WithAZone_IsTheRuleTheCurrentZoneOverloadUses()
    {
        var eastern = ViewerTimeHelper.DisplayZoneFor(TimeDisplayMode.ServerTime, Eastern);

        Assert.Equal("2026-11-01 01:30 -04:00", ViewerTimeHelper.FormatForDisplay(Utc(2026, 11, 1, 5, 30), eastern, "yyyy-MM-dd HH:mm"));
        Assert.Equal("2026-11-01 01:30 -05:00", ViewerTimeHelper.FormatForDisplay(Utc(2026, 11, 1, 6, 30), eastern, "yyyy-MM-dd HH:mm"));
        Assert.Equal("2026-11-01 05:30", ViewerTimeHelper.FormatForDisplay(Utc(2026, 11, 1, 5, 30), TimeZoneInfo.Utc, "yyyy-MM-dd HH:mm"));
        Assert.Equal("2026-07-01 10:30", ViewerTimeHelper.FormatForDisplay(Utc(2026, 7, 1, 14, 30), eastern, "yyyy-MM-dd HH:mm"));

        ViewerTimeHelper.ActiveServerClock = Eastern;
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        foreach (var instant in new[] { Utc(2026, 11, 1, 5, 30), Utc(2026, 11, 1, 6, 30), Utc(2026, 7, 1, 14, 30) })
        {
            Assert.Equal(
                ViewerTimeHelper.FormatForDisplay(instant, ViewerTimeHelper.CurrentDisplayZone(), "yyyy-MM-dd HH:mm:ss"),
                ViewerTimeHelper.FormatForDisplay(instant, "yyyy-MM-dd HH:mm:ss"));
        }
    }

    /// <summary>
    /// The current-zone overload hands its work to the zone overload rather than wording the suffix itself, and the two
    /// list rows call the zone overload with the zone they compute, so the rule stays in one place.
    /// </summary>
    [Fact]
    public void TheOneRule_LivesInTheZoneOverload_AndTheRowsCallIt()
    {
        var helper = ViewerTypedRangeTests.StripComments(ViewerTypedRangeTests.ViewerSource("ViewerTimeHelper.cs", ThisFile()));
        var current = ViewerTypedRangeTests.StripComments(FormatForDisplayMembers(helper, "string format) =>"));
        Assert.Contains("FormatForDisplay(naiveUtc, CurrentDisplayZone(), format)", current);
        Assert.Single(Regex.Matches(helper, @"AmbiguousOffsetSuffix\("));

        var alert = ViewerTypedRangeTests.StripComments(
            ViewerTypedRangeTests.MemberText(ViewerTypedRangeTests.ViewerSource("ViewerDataService.AlertHistory.cs", ThisFile()), "TimeLocal"));
        var manage = ViewerTypedRangeTests.StripComments(
            ViewerTypedRangeTests.MemberText(ViewerTypedRangeTests.ViewerSource("ManageServersWindow.xaml.cs", ThisFile()), "LastCollectedDisplay"));
        Assert.Matches(@"ViewerTimeHelper\.FormatForDisplay\(\s*AlertTime,\s*ViewerTimeHelper\.DisplayZoneFor\(", alert);
        Assert.Matches(@"ViewerTimeHelper\.FormatForDisplay\(\s*utc,\s*ViewerTimeHelper\.DisplayZoneFor\(", manage);
        Assert.DoesNotContain(".ToString(", alert);
        Assert.DoesNotContain(".ToString(", manage);
    }

    /// <summary>The text of the two-argument (DateTime, string) overload, from its declaration to its semicolon.</summary>
    private static string FormatForDisplayMembers(string helper, string signatureTail)
    {
        var at = helper.IndexOf("public static string FormatForDisplay(DateTime naiveUtc, " + signatureTail, StringComparison.Ordinal);
        Assert.True(at >= 0, "the (DateTime, string) overload of FormatForDisplay moved");
        return helper.Substring(at, helper.IndexOf(';', at) - at + 1);
    }

    private static string ThisFile([CallerFilePath] string thisFile = "") => thisFile;
}
