/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common.KnownPatterns;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4766, #6457: the times in the three fleet-wide windows, Notification Routes (Modified), Threshold Overrides (Updated)
/// and Known Patterns (Edited by). Each is one UTC instant and none belongs to a server, so "Server" display mode has no
/// clock to apply: Modified used to apply the active server's offset (a route saved at 10:07 UTC read 03:07), Updated read
/// this machine's time with no zone named, and Edited by read UTC. All three now read Local or UTC and name the zone.
/// </summary>
/* Flips the process-wide display mode and active server clock (each restored in Dispose); one shared collection
   serializes it with every other class that does. */
[Collection("viewer-time-statics")]
public sealed class ViewerNotificationRouteDisplayTests : IDisposable
{
    private static readonly ServerClock Eastern = ServerClock.Resolve("Eastern Standard Time", -300);

    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;
    private readonly ServerClock _savedClock = ViewerTimeHelper.ActiveServerClock;
    private readonly CultureInfo _savedCulture = CultureInfo.CurrentCulture;

    public ViewerNotificationRouteDisplayTests()
    {
        /* The time is culture-independent (yyyy-MM-dd HH:mm:ss, like every other grid); the invariant culture is kept so nothing else varies. */
        CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
    }

    public void Dispose()
    {
        ViewerTimeHelper.CurrentDisplayMode = _savedMode;
        ViewerTimeHelper.ActiveServerClock = _savedClock;
        CultureInfo.CurrentCulture = _savedCulture;
    }

    private static NotificationRouteRow Route(int y, int mo, int d, int h, int mi) => new()
    {
        RouteId = 1,
        MetricMatch = "High CPU",
        ModifiedAtUtc = DateTime.SpecifyKind(new DateTime(y, mo, d, h, mi, 0), DateTimeKind.Utc),
    };

    private static string LocalExpected(DateTime utc, string format)
    {
        var zone = TimeZoneInfo.Local;
        var name = zone.IsDaylightSavingTime(utc) ? zone.DaylightName : zone.StandardName;
        return TimeZoneInfo.ConvertTimeFromUtc(utc, zone).ToString(format, CultureInfo.InvariantCulture) + " " + name;
    }

    /// <summary>Server mode used to apply the active server's offset to a fleet-wide row: a summer instant on a US Eastern server read its wall time (10:30), not the instant.</summary>
    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]
    [InlineData(TimeDisplayMode.UTC)]
    public void Modified_InServerAndUtcMode_ReadsUtc_AndSaysSo(TimeDisplayMode mode)
    {
        ViewerTimeHelper.ActiveServerClock = Eastern;
        ViewerTimeHelper.CurrentDisplayMode = mode;

        Assert.Equal("2026-07-01 14:30:00 UTC", Route(2026, 7, 1, 14, 30).ModifiedDisplay);
        Assert.Equal("2026-11-01 05:30:00 UTC", Route(2026, 11, 1, 5, 30).ModifiedDisplay);
        Assert.Equal("2026-11-01 06:30:00 UTC", Route(2026, 11, 1, 6, 30).ModifiedDisplay);
    }

    /// <summary>Local mode reads this machine's zone and names it, so the repeated autumn hour's two 01:30s carry different zone names where the machine has daylight saving.</summary>
    [Fact]
    public void Modified_InLocalMode_ReadsThisMachinesTime_AndNamesItsZone()
    {
        ViewerTimeHelper.ActiveServerClock = Eastern;
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;

        foreach (var utc in new[] { new DateTime(2026, 7, 1, 14, 30, 0, DateTimeKind.Utc), new DateTime(2026, 11, 1, 5, 30, 0, DateTimeKind.Utc), new DateTime(2026, 11, 1, 6, 30, 0, DateTimeKind.Utc) })
        {
            Assert.Equal(LocalExpected(utc, "yyyy-MM-dd HH:mm:ss"), Route(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute).ModifiedDisplay);
        }
    }

    /// <summary>A route that has not been stored yet has no time to show.</summary>
    [Fact]
    public void Modified_OfARouteNotStoredYet_IsEmpty()
    {
        Assert.Equal("", new NotificationRouteRow().ModifiedDisplay);
    }

    /// <summary>The same instant reads the same way in the three windows in every display mode, which is the "one convention" the issue asked for.</summary>
    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]
    [InlineData(TimeDisplayMode.UTC)]
    [InlineData(TimeDisplayMode.LocalTime)]
    public void TheThreeFleetWideWindows_ReadAnInstantTheSameWay(TimeDisplayMode mode)
    {
        ViewerTimeHelper.ActiveServerClock = Eastern;
        ViewerTimeHelper.CurrentDisplayMode = mode;
        var instant = new DateTime(2026, 7, 1, 10, 7, 0, DateTimeKind.Utc);

        var modified = Route(2026, 7, 1, 10, 7).ModifiedDisplay;
        var updated = new ThresholdOverrideViewRow(new ThresholdOverrideRow(7, "Prod", null, "cpu", 80, 90, "erik", null, instant)).UpdatedText;
        var edited = new KnownPatternRow(
            new KnownPattern(1, null, null, null, null, 10, "n", null, instant, null, "erik", instant),
            new Dictionary<int, string>()).UpdatedDisplay;

        var shown = mode == TimeDisplayMode.LocalTime ? LocalExpected(instant, "yyyy-MM-dd HH:mm") : "2026-07-01 10:07 UTC";
        Assert.Equal(shown, updated);
        Assert.Equal("erik, " + shown, edited);

        /* Modified carries seconds, so compare the wall time to the minute and the zone name after it. */
        var zoneName = shown[17..];
        Assert.StartsWith(shown[..16], modified, StringComparison.Ordinal);
        Assert.EndsWith(" " + zoneName, modified, StringComparison.Ordinal);
    }

    /// <summary>An override stored with an unspecified-kind timestamp (a store read) is still one UTC instant, not this machine's wall time.</summary>
    [Fact]
    public void Updated_OfAnUnspecifiedKindTimestamp_IsReadAsUtc()
    {
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        var stamp = new DateTime(2026, 7, 1, 10, 5, 0, DateTimeKind.Unspecified);

        Assert.Equal("2026-07-01 10:05 UTC", new ThresholdOverrideViewRow(new ThresholdOverrideRow(7, "Prod", null, "cpu", 80, null, "erik", null, stamp)).UpdatedText);
    }
}
