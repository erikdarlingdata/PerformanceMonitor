/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Job History picker and its "Showing since" note read in the zone the grid words its rows in, under the one Times setting. For
/// All Servers that is the one clock a list of several servers reads on (<see cref="MultiServerTime.EffectiveMode"/>, the rule Alert
/// History's rows follow): UTC in UTC mode, this machine's zone in Local mode and in Server mode, which has no one server clock to
/// apply. For one server it is that server's clock in Server mode and the chosen zone otherwise.
/// </summary>
[Collection("server-time-helper")]
public sealed class JobHistoryAllServersZoneTests : IDisposable
{
    private static readonly ServerClock Eastern = ServerClock.Resolve("Eastern Standard Time", -300);
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;

    public void Dispose() => ServerTimeHelper.CurrentDisplayMode = _savedMode;

    private static Dictionary<int, ServerClock> Clocks() => new() { [7] = Eastern };

    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]
    [InlineData(TimeDisplayMode.LocalTime)]
    public void AllServers_InServerOrLocalTime_ReadsThisMachinesZone_AsAlertHistoryDoes(TimeDisplayMode mode)
    {
        ServerTimeHelper.CurrentDisplayMode = mode;

        Assert.Equal(TimeZoneInfo.Local, JobHistoryTab.JobHistoryZone(null, Clocks(), null));
    }

    [Fact]
    public void AllServers_InUtc_ReadsUtc()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;

        Assert.Equal(TimeZoneInfo.Utc, JobHistoryTab.JobHistoryZone(null, Clocks(), null));
    }

    [Fact]
    public void OneServer_InUtcMode_ReadsUtc_NotItsServersClock()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;

        Assert.Equal(TimeZoneInfo.Utc, JobHistoryTab.JobHistoryZone(7, Clocks(), null));
    }

    [Fact]
    public void OneServer_InServerTime_ReadsItsOwnClock()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        Assert.Equal(Eastern.AsTimeZone().Id, JobHistoryTab.JobHistoryZone(7, Clocks(), null).Id);
    }
}
