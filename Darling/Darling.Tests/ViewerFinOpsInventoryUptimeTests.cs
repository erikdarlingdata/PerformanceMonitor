/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7176: the Server Inventory Uptime column subtracted the server's LOCAL start time from the viewer's local clock, so a viewer
/// in another zone saw an uptime off by the zone difference. The stored start time moves to UTC by the collected offset first.
/// </summary>
public sealed class ViewerFinOpsInventoryUptimeTests
{
    [Theory]
    [InlineData(-480)]
    [InlineData(-240)]
    [InlineData(0)]
    [InlineData(330)]
    [InlineData(600)]
    public void Uptime_is_right_whatever_zone_the_server_is_in(int serverOffsetMinutes)
    {
        var utcNow = new DateTime(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);
        var row = new ServerPropertyRow
        {
            /* Started 53 hours before utcNow; the stored value is the server's wall clock. */
            SqlServerStartTime = utcNow.AddHours(-53).AddMinutes(serverOffsetMinutes),
            UtcOffsetMinutes = serverOffsetMinutes
        };

        Assert.Equal(53 * 3600d, row.UptimeSecondsAt(utcNow));
    }

    [Fact]
    public void A_restart_inside_the_zone_gap_never_reads_negative()
    {
        var utcNow = new DateTime(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);
        var row = new ServerPropertyRow { SqlServerStartTime = utcNow.AddMinutes(-30), UtcOffsetMinutes = 0 };

        Assert.Equal(30 * 60d, row.UptimeSecondsAt(utcNow));
        Assert.Equal(0d, new ServerPropertyRow { SqlServerStartTime = utcNow.AddHours(1), UtcOffsetMinutes = 0 }.UptimeSecondsAt(utcNow));
    }

    [Fact]
    public void The_display_text_follows_the_corrected_uptime()
    {
        var row = new ServerPropertyRow
        {
            SqlServerStartTime = DateTime.UtcNow.AddHours(-53).AddMinutes(-240),
            UtcOffsetMinutes = -240
        };

        Assert.Equal("2d 5h", row.UptimeDisplay);
    }

    [Fact]
    public void Uptime_is_blank_when_the_offset_was_never_collected()
    {
        var row = new ServerPropertyRow { SqlServerStartTime = DateTime.Now.AddDays(-3), UtcOffsetMinutes = null };

        Assert.Null(row.UptimeSeconds);
        Assert.Equal("", row.UptimeDisplay);
    }

    [Fact]
    public void A_start_before_a_daylight_saving_change_gives_the_true_elapsed_time()
    {
        /* US Eastern: started 15 Oct at 12:00 on the server's wall clock (EDT, UTC-4). The service last loaded after the fall-back
           change on 1 Nov, so the stored offset is EST (-300). Read on 10 Nov at 12:00 UTC, the true uptime is 25 days 20 hours. */
        var utcNow = new DateTime(2026, 11, 10, 12, 0, 0, DateTimeKind.Utc);
        var start = new DateTime(2026, 10, 15, 12, 0, 0, DateTimeKind.Unspecified);
        var truth = (25 * 24 + 20) * 3600d;

        var withZone = new ServerPropertyRow { SqlServerStartTime = start, UtcOffsetMinutes = -300, TimeZoneId = "Eastern Standard Time" };
        var offsetOnly = new ServerPropertyRow { SqlServerStartTime = start, UtcOffsetMinutes = -300 };

        Assert.Equal(truth, withZone.UptimeSecondsAt(utcNow));
        Assert.Equal(truth - 3600d, offsetOnly.UptimeSecondsAt(utcNow));
    }
}
