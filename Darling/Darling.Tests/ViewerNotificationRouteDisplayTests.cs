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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4766: the Notification Routes grid's Modified column. <c>modified_at</c> is written as
/// <c>now() AT TIME ZONE 'UTC'</c>, so it is one UTC instant (the row reads it as <see cref="DateTimeKind.Utc"/>), and it
/// used to be <c>ToLocalTime().ToString("g")</c>: this machine's clock in every display mode, and bare in the repeated
/// autumn hour. It now goes through <see cref="ViewerTimeHelper.FormatForDisplay(DateTime, string)"/> like the grids
/// beside it, so it follows the mode and names the UTC offset of each 01:30 on a US Eastern server (autumn change
/// 2026-11-01 at 06:00 UTC).
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
        /* "g" runs on the current culture, as every grid's text does; the invariant one fixes its pattern (MM/dd/yyyy HH:mm). */
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

    [Fact]
    public void Modified_InTheRepeatedHour_ReadsItsOffsetInServerMode_AndTheStoredInstantInUtcMode()
    {
        ViewerTimeHelper.ActiveServerClock = Eastern;

        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("11/01/2026 01:30 -04:00", Route(2026, 11, 1, 5, 30).ModifiedDisplay);
        Assert.Equal("11/01/2026 01:30 -05:00", Route(2026, 11, 1, 6, 30).ModifiedDisplay);
        Assert.Equal("11/01/2026 02:30", Route(2026, 11, 1, 7, 30).ModifiedDisplay);

        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        Assert.Equal("11/01/2026 05:30", Route(2026, 11, 1, 5, 30).ModifiedDisplay);
        Assert.Equal("11/01/2026 06:30", Route(2026, 11, 1, 6, 30).ModifiedDisplay);
    }

    /// <summary>The column follows the display mode: a summer instant on the Eastern server reads its wall time, not this machine's.</summary>
    [Fact]
    public void Modified_InServerMode_ReadsTheServersWallTime_WhateverThisMachinesZoneIs()
    {
        ViewerTimeHelper.ActiveServerClock = Eastern;
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        Assert.Equal("07/01/2026 10:30", Route(2026, 7, 1, 14, 30).ModifiedDisplay);
    }

    /// <summary>A route that has not been stored yet has no time to show.</summary>
    [Fact]
    public void Modified_OfARouteNotStoredYet_IsEmpty()
    {
        Assert.Equal("", new NotificationRouteRow().ModifiedDisplay);
    }

    /// <summary>The column is worded by the shared renderer and not by a <c>ToLocalTime()</c> of its own.</summary>
    [Fact]
    public void Modified_IsWordedByTheSharedRenderer()
    {
        var source = ViewerTypedRangeTests.StripComments(
            ViewerTypedRangeTests.MemberText(
                ViewerTypedRangeTests.ViewerSource("ViewerDataService.NotificationRoutes.cs", ThisFile()), "ModifiedDisplay"));

        Assert.Contains("ViewerTimeHelper.FormatForDisplay(ModifiedAtUtc, \"g\")", source);
        Assert.DoesNotContain("ToLocalTime", source);
    }

    private static string ThisFile([CallerFilePath] string thisFile = "") => thisFile;
}
