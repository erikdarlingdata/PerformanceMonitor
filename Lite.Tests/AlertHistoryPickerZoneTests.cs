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
/// #8645: Alert History covers every server unless one is picked, so its range picker must not take the clock of whichever server
/// tab was opened last. With Times set to Server Time it read that tab's offset while Job History read this machine's.
/// </summary>
/* Sets ServerTimeHelper.ActiveServerClock, a process-wide mutable static, so it joins the collection every class that writes it
   uses; it is restored in Dispose. */
[Collection("server-time-helper")]
public sealed class AlertHistoryPickerZoneTests : IDisposable
{
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;

    public void Dispose() => ServerTimeHelper.ActiveServerClock = _savedClock;

    /* The machine is UTC-05:00 and the last server tab opened is UTC-08:00, as in the report. */
    private static readonly TimeZoneInfo Machine = TimeZoneInfo.CreateCustomTimeZone("machine", TimeSpan.FromHours(-5), "machine", "machine");
    private static readonly ServerClock Pacific = ServerClock.FixedOffset(-480);
    private static readonly IReadOnlyDictionary<int, ServerClock> NoRowClocks = new Dictionary<int, ServerClock>();

    [Fact]
    public void AllServers_ServerTime_ReadsTheMachinesZone_NotTheLastOpenedTabs()
    {
        ServerTimeHelper.ActiveServerClock = Pacific;

        var zone = AlertsHistoryTab.PickerZoneFor(TimeDisplayMode.ServerTime, null, NoRowClocks, _ => Pacific, Machine);

        Assert.Equal(Machine.BaseUtcOffset, zone.BaseUtcOffset);
        Assert.NotEqual(ServerTimeHelper.CurrentDisplayZone.BaseUtcOffset, zone.BaseUtcOffset);
    }

    [Fact]
    public void AllServers_UtcAndLocal_ReadUtcAndTheMachine()
    {
        Assert.Equal(TimeZoneInfo.Utc, AlertsHistoryTab.PickerZoneFor(TimeDisplayMode.UTC, null, NoRowClocks, null, Machine));
        Assert.Equal(Machine, AlertsHistoryTab.PickerZoneFor(TimeDisplayMode.LocalTime, null, NoRowClocks, null, Machine));
    }

    [Fact]
    public void OneServer_ServerTime_ReadsThatServersStampedClock_ThenItsTabs_ThenTheMachine()
    {
        ServerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(600);
        var rowClocks = new Dictionary<int, ServerClock> { [7] = Pacific };

        var stamped = AlertsHistoryTab.PickerZoneFor(TimeDisplayMode.ServerTime, 7, rowClocks, _ => ServerClock.FixedOffset(60), Machine);
        var tab = AlertsHistoryTab.PickerZoneFor(TimeDisplayMode.ServerTime, 8, rowClocks, _ => ServerClock.FixedOffset(60), Machine);
        var neither = AlertsHistoryTab.PickerZoneFor(TimeDisplayMode.ServerTime, 9, rowClocks, _ => null, Machine);

        Assert.Equal(TimeSpan.FromMinutes(-480), stamped.BaseUtcOffset);
        Assert.Equal(TimeSpan.FromMinutes(60), tab.BaseUtcOffset);
        Assert.Equal(Machine.BaseUtcOffset, neither.BaseUtcOffset);
    }
}
