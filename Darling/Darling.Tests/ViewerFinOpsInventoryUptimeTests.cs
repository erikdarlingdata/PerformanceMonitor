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
}
