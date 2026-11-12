/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7416 item 21, Lite's side: a trend whose first point is later than the window's start names retention as the reason only when the
/// requested start is older than the archive keeps. A query first run later, or a server added mid-window, gives the same late head.
/// </summary>
public sealed class TrendAgedOutReasonTests
{
    private static readonly DateTime Now = new(2026, 11, 3, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void ATruncatedWindow_WithTheStartInsideTheArchive_IsNotAgedOut()
    {
        var (olderThanStore, keepsDays) = McpQueryTools.DescribeStoreReach(windowTruncated: true, Now.AddDays(-7), Now);

        Assert.False(olderThanStore);
        Assert.Equal(RetentionService.ArchiveRetentionMonths * 30, keepsDays);
    }

    [Fact]
    public void ATruncatedWindow_WithTheStartOlderThanTheArchive_IsAgedOut()
    {
        var (olderThanStore, keepsDays) = McpQueryTools.DescribeStoreReach(windowTruncated: true, Now.AddMonths(-RetentionService.ArchiveRetentionMonths).AddDays(-1), Now);

        Assert.True(olderThanStore);
        Assert.Equal(90, keepsDays);
    }

    [Fact]
    public void AWindowThatIsNotTruncated_IsNeverAgedOut()
    {
        Assert.False(McpQueryTools.DescribeStoreReach(windowTruncated: false, Now.AddMonths(-6), Now).StartOlderThanStore);
    }
}
