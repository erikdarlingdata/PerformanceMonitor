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
using System.Linq;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6137: a grid that lists several servers reads every row on one clock, sorts on the true instant, and names the
/// clock in its header. A list of one server keeps its server's clock. The offset label is one spelling everywhere.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class MultiServerTimeTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;
    private readonly ServerClock _savedClock = ViewerTimeHelper.ActiveServerClock;

    public void Dispose()
    {
        ViewerTimeHelper.CurrentDisplayMode = _savedMode;
        ViewerTimeHelper.ActiveServerClock = _savedClock;
    }

    private static DateTime At(int h, int mi, int s = 0) => new(2026, 6, 10, h, mi, s, DateTimeKind.Unspecified);

    private static ViewerAlertRow Alert(int serverId, DateTime utc, ServerClock clock) => new()
    {
        AlertTime = utc,
        ServerId = serverId,
        ServerName = "server-" + serverId,
        Clock = clock,
        MetricName = "High CPU",
        CurrentValue = 90,
        ThresholdValue = 80,
        AlertSent = true,
        NotificationType = "email",
        Muted = false,
    };

    private static string InLocal(DateTime utc) =>
        DisplayZone.ToDisplay(utc, TimeZoneInfo.Local).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    [Fact]
    public void AlertsOfTwoServers_ServerTime_ReadOnOneClock_AndSortByTheInstant()
    {
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        var utcServer = ServerClock.FixedOffset(0);
        var minus4Server = ServerClock.FixedOffset(-240);
        var earlier = Alert(1, At(12, 0), utcServer);          // 12:00 on the UTC server
        var later = Alert(2, At(12, 30), minus4Server);        // 08:30 on its own clock, yet 30 minutes LATER
        var rows = new List<ViewerAlertRow> { later, earlier };

        // On their own clocks the later row would read 08:30, above the earlier one's 12:00.
        Assert.Equal("2026-06-10 08:30:00", later.TimeLocal);
        ViewerDataService.StampMixedServers(rows);

        Assert.Equal(InLocal(At(12, 0)), earlier.TimeLocal);
        Assert.Equal(InLocal(At(12, 30)), later.TimeLocal);
        Assert.Equal(
            new[] { earlier, later },
            rows.OrderBy(r => r.AlertTime).ToArray());
        var gap = DateTime.Parse(later.TimeLocal, CultureInfo.InvariantCulture) - DateTime.Parse(earlier.TimeLocal, CultureInfo.InvariantCulture);
        Assert.Equal(TimeSpan.FromMinutes(30), gap);
    }

    [Fact]
    public void AlertsOfOneServer_ServerTime_KeepTheServersClock()
    {
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        var a = Alert(2, At(12, 30), ServerClock.FixedOffset(-240));
        var b = Alert(2, At(13, 30), ServerClock.FixedOffset(-240));
        ViewerDataService.StampMixedServers(new List<ViewerAlertRow> { a, b });

        Assert.Equal("2026-06-10 08:30:00", a.TimeLocal);
        Assert.Equal("2026-06-10 09:30:00", b.TimeLocal);
    }

    [Fact]
    public void AlertsOfTwoServers_UtcMode_StayInUtc()
    {
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        var a = Alert(1, At(12, 0), ServerClock.FixedOffset(0));
        var b = Alert(2, At(12, 30), ServerClock.FixedOffset(-240));
        ViewerDataService.StampMixedServers(new List<ViewerAlertRow> { a, b });

        Assert.Equal("2026-06-10 12:00:00", a.TimeLocal);
        Assert.Equal("2026-06-10 12:30:00", b.TimeLocal);
    }

    [Fact]
    public void JobRunsOfTwoServers_ServerTime_ReadOnOneClock()
    {
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        ViewerJobHistoryRow Run(int serverId, DateTime utc, ServerClock clock) => ViewerJobHistoryRow.From(
            new DarlingJobHistoryRow(serverId, "s" + serverId, 1, "job", "Nightly", true, null, 0, null, 1, "Succeeded", utc, 5, 0, null, null, false),
            clock);
        var a = Run(1, At(12, 0), ServerClock.FixedOffset(0));
        var b = Run(2, At(12, 30), ServerClock.FixedOffset(-240));
        var rows = new List<ViewerJobHistoryRow> { a, b };

        Assert.Equal("2026-06-10 08:30:00", b.RunTimeLocal);
        ViewerDataService.StampMixedServers(rows);

        Assert.Equal(InLocal(At(12, 0)), a.RunTimeLocal);
        Assert.Equal(InLocal(At(12, 30)), b.RunTimeLocal);
    }

    [Theory]
    [InlineData(TimeDisplayMode.ServerTime, false, "Time (server)")]
    [InlineData(TimeDisplayMode.ServerTime, true, "Time (local)")]
    [InlineData(TimeDisplayMode.UTC, true, "Time (UTC)")]
    [InlineData(TimeDisplayMode.LocalTime, true, "Time (local)")]
    public void Header_NamesTheClockTheRowsAreReallyIn(TimeDisplayMode mode, bool mixed, string expected)
    {
        Assert.Equal(expected, TimeColumnTitle.For("Time", mode, mixed));
    }

    [Theory]
    [InlineData(-240, "UTC-04:00")]
    [InlineData(0, "UTC+00:00")]
    [InlineData(330, "UTC+05:30")]
    [InlineData(-30, "UTC-00:30")]
    public void OffsetLabel_IsOneSpelling_WithTheTimeRangePicker(int minutes, string expected)
    {
        Assert.Equal(expected, MultiServerTime.OffsetLabel(minutes));
        var zone = TimeZoneInfo.CreateCustomTimeZone("t", TimeSpan.FromMinutes(minutes), "t", "t");
        Assert.Equal(expected, "UTC" + DisplayZone.UtcOffsetText(At(12, 0), zone));
    }
}
