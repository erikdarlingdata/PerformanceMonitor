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
using System.Linq;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6137: Lite's Alert History and Job History grids read every row on one clock when they list several servers,
/// sort on the true instant, and keep a single server's own clock.
/// </summary>
[Collection("server-time-helper")]
public sealed class MultiServerTimeTests : IDisposable
{
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;

    public void Dispose()
    {
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
    }

    private static DateTime At(int h, int mi) => new(2026, 6, 10, h, mi, 0, DateTimeKind.Unspecified);

    private static string InLocal(DateTime utc) =>
        DisplayZone.ToDisplay(utc, TimeZoneInfo.Local).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    private static AlertHistoryRow Alert(int serverId, DateTime utc, ServerClock clock) =>
        new() { ServerId = serverId, AlertTime = utc, Clock = clock };

    /// <summary>A run on a server at <paramref name="clock"/>: the stored wall clock is the instant on that clock.</summary>
    private static JobHistoryRow Run(int serverId, DateTime utc, ServerClock clock) => new()
    {
        ServerId = serverId,
        RunDateTime = clock.ToServerLocal(utc),
        RunDateTimeUtc = utc,
        Clock = clock,
    };

    [Fact]
    public void AlertsOfTwoServers_ServerTime_ReadOnOneClock()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        var earlier = Alert(1, At(12, 0), ServerClock.FixedOffset(0));
        var later = Alert(2, At(12, 30), ServerClock.FixedOffset(-240));
        var rows = new List<AlertHistoryRow> { later, earlier };

        AlertsHistoryTab.StampClocks(rows, new Dictionary<int, ServerClock?> { [1] = earlier.Clock, [2] = later.Clock }, null);

        Assert.Equal(InLocal(At(12, 0)), earlier.TimeLocal);
        Assert.Equal(InLocal(At(12, 30)), later.TimeLocal);
        Assert.Equal(new[] { earlier, later }, rows.OrderBy(r => r.AlertTime).ToArray());
    }

    [Fact]
    public void AlertsOfOneServer_ServerTime_KeepTheServersClock()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        var clock = ServerClock.FixedOffset(-240);
        var a = Alert(2, At(12, 30), clock);

        AlertsHistoryTab.StampClocks(new List<AlertHistoryRow> { a }, new Dictionary<int, ServerClock?> { [2] = clock }, null);

        Assert.Equal("2026-06-10 08:30:00", a.TimeLocal);
    }

    [Fact]
    public void JobRunsOfTwoServers_ServerTime_ReadOnOneClock_AndSortOnTheInstant()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        var earlier = Run(1, At(12, 0), ServerClock.FixedOffset(0));
        var later = Run(2, At(12, 30), ServerClock.FixedOffset(-240));

        Assert.Equal("2026-06-10 08:30:00", later.RunTimeLocal);   // alone, a server's own wall clock
        earlier.MixedServers = later.MixedServers = true;

        Assert.Equal(InLocal(At(12, 0)), earlier.RunTimeLocal);
        Assert.Equal(InLocal(At(12, 30)), later.RunTimeLocal);
        Assert.True(later.RunDateTimeUtc > earlier.RunDateTimeUtc);
    }

    [Fact]
    public void JobRunsOfTwoServers_UtcMode_ReadInUtc_AndSingleServerLocalModeToo()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        var run = Run(2, At(12, 30), ServerClock.FixedOffset(-240));

        Assert.Equal("2026-06-10 12:30:00", run.RunTimeLocal);

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;
        Assert.Equal(InLocal(At(12, 30)), run.RunTimeLocal);
    }

    [Theory]
    [InlineData(-300, "UTC-05:00")]
    [InlineData(0, "UTC+00:00")]
    [InlineData(330, "UTC+05:30")]
    public void ServerOffsetLabel_IsOneSpelling(int minutes, string expected)
    {
        Assert.Equal(expected, ServerTimeHelper.GetTimezoneLabel(TimeDisplayMode.ServerTime, ServerClock.FixedOffset(minutes), At(12, 0)));
    }
}
