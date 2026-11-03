/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #7176: the Server Inventory Uptime column subtracted the server's LOCAL start time from the client's local clock, so a client
/// in another zone saw an uptime off by the zone difference (a restart inside the gap read "0d -4h"). The uptime now comes from
/// the start instant the live read worked out from the server's own clock.
/// </summary>
public sealed class FinOpsInventoryUptimeTests
{
    [Theory]
    [InlineData(-480)]
    [InlineData(0)]
    [InlineData(600)]
    public void Uptime_ignores_the_server_local_start_time_and_follows_the_utc_start(int serverOffsetMinutes)
    {
        var utcNow = DateTime.UtcNow;
        var row = new ServerPropertyRow
        {
            /* The server's local wall clock for a start five hours ago, in a zone that may not be this machine's. */
            SqlServerStartTime = utcNow.AddHours(-5).AddMinutes(serverOffsetMinutes),
            SqlServerStartUtc = utcNow.AddHours(-5)
        };

        Assert.InRange(row.UptimeSeconds!.Value, 5 * 3600 - 5, 5 * 3600 + 60);
        Assert.Equal("0d 5h", row.UptimeDisplay);
    }

    [Fact]
    public void A_restart_inside_the_zone_gap_never_reads_negative()
    {
        var row = new ServerPropertyRow { SqlServerStartUtc = DateTime.UtcNow.AddMinutes(-30) };

        Assert.InRange(row.UptimeSeconds!.Value, 30 * 60 - 5, 30 * 60 + 60);
        Assert.Equal("0d 0h", row.UptimeDisplay);
    }

    [Fact]
    public void Uptime_is_blank_when_the_server_gave_no_start()
    {
        var row = new ServerPropertyRow { SqlServerStartTime = DateTime.Now.AddDays(-3) };

        Assert.Null(row.UptimeSeconds);
        Assert.Equal("", row.UptimeDisplay);
    }

    [Fact]
    public void A_start_before_a_daylight_saving_change_is_exactly_as_far_back_as_the_tick_count()
    {
        /* 20 days and 6 hours of server ticks, across a fall-back change: the server's wall clock reads 20d 7h apart, the ticks say 20d 6h. */
        var utcNow = new DateTime(2026, 11, 10, 12, 0, 0, DateTimeKind.Utc);
        var uptimeSeconds = (20 * 24 + 6) * 3600d;

        var start = LocalDataService.StartUtcFromUptimeSeconds(uptimeSeconds, utcNow);

        Assert.Equal(utcNow.AddHours(-(20 * 24 + 6)), start);
    }
}
