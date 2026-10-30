/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The times on fleet-wide views (Fleet Sweeps, the Custom Views list and run window) belong to no server, so they must not
/// take the clock of whichever server tab refreshed last, and a server tab's toolbar time must use that tab's own clock.
/// </summary>
/* Flips the process-wide display mode and active server clock (each restored in Dispose). */
[Collection("viewer-time-statics")]
public sealed class ViewerFleetWideTimesTests : IDisposable
{
    private static readonly ServerClock Mountain = ServerClock.FixedOffset(-420);
    private static readonly ServerClock Eastern = ServerClock.FixedOffset(-240);

    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;
    private readonly ServerClock _savedClock = ViewerTimeHelper.ActiveServerClock;

    public void Dispose()
    {
        ViewerTimeHelper.CurrentDisplayMode = _savedMode;
        ViewerTimeHelper.ActiveServerClock = _savedClock;
    }

    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]
    [InlineData(TimeDisplayMode.UTC)]
    public void FleetSweepTimes_InServerAndUtcMode_ReadUtc_NotTheActiveServersClock(TimeDisplayMode mode)
    {
        ViewerTimeHelper.ActiveServerClock = Mountain;
        ViewerTimeHelper.CurrentDisplayMode = mode;

        Assert.Equal("2026-07-01 14:30:00 UTC", FleetSweepsRows.Local("2026-07-01T14:30:00Z"));
        Assert.Equal(TimeZoneInfo.Utc, ViewerTimeHelper.FleetWideZone());
    }

    [Fact]
    public void FleetSweepTimes_InLocalMode_ReadThisMachinesZone()
    {
        ViewerTimeHelper.ActiveServerClock = Mountain;
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;
        var utc = new DateTime(2026, 7, 1, 14, 30, 0, DateTimeKind.Utc);

        var text = FleetSweepsRows.Local("2026-07-01T14:30:00Z");

        Assert.StartsWith(TimeZoneInfo.ConvertTimeFromUtc(utc, TimeZoneInfo.Local).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + " ", text, StringComparison.Ordinal);
        Assert.Equal(TimeZoneInfo.Local, ViewerTimeHelper.FleetWideZone());
        Assert.Equal("", FleetSweepsRows.Local("not a time"));
    }

    private static CustomViewRow Row(string? by) => new(new CustomViewSummary(
        1, "v", null, 1, new DateTime(2026, 7, 1, 14, 30, 0, DateTimeKind.Unspecified), by, "dashboard", null, false, null, false));

    [Fact]
    public void CustomViewUpdated_InServerMode_ReadsUtcWithItsLabel_NotTheActiveServersClock()
    {
        ViewerTimeHelper.ActiveServerClock = Mountain;
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        Assert.Equal("2026-07-01 14:30 UTC by erik", Row("erik").UpdatedDisplay);
        Assert.Equal("2026-07-01 14:30 UTC", Row(null).UpdatedDisplay);
        Assert.Equal(new DateTime(2026, 7, 1, 14, 30, 0, DateTimeKind.Utc), Row("erik").UpdatedUtc);
    }

    [Fact]
    public void CustomViewUpdated_InLocalMode_ReadsThisMachinesZone()
    {
        var utc = new DateTime(2026, 7, 1, 14, 30, 0, DateTimeKind.Utc);
        var text = Row(null).UpdatedText(TimeDisplayMode.LocalTime);

        Assert.StartsWith(TimeZoneInfo.ConvertTimeFromUtc(utc, TimeZoneInfo.Local).ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " ", text, StringComparison.Ordinal);
    }

    /// <summary>A PostgreSQL tab's "collected" readout is written in that tab's own zone even when another server's clock is the process-wide one.</summary>
    [Fact]
    public void ServerToolbarFreshness_UsesTheTabsOwnClock_NotTheProcessWideOne()
    {
        var now = new DateTime(2026, 7, 1, 17, 0, 0, DateTimeKind.Utc);
        var last = new DateTime(2026, 7, 1, 16, 59, 0, DateTimeKind.Utc);
        var registered = now.AddDays(-30);
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        ViewerTimeHelper.ActiveServerClock = Eastern;
        var pgZone = ViewerTimeHelper.DisplayZoneFor(ViewerTimeHelper.CurrentDisplayMode, Mountain);
        Assert.Equal("collected 2026-07-01 09:59", ViewerServerTab.FreshnessLabel(last, registered, now, pgZone));

        ViewerTimeHelper.ActiveServerClock = Mountain;
        var sqlZone = ViewerTimeHelper.DisplayZoneFor(ViewerTimeHelper.CurrentDisplayMode, Eastern);
        Assert.Equal("collected 2026-07-01 12:59", ViewerServerTab.FreshnessLabel(last, registered, now, sqlZone));
    }
}
