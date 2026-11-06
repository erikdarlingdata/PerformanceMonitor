/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>#7208: the route list picks its statement from the store version. A probe that cannot tell (null) must read every column,
/// or the list shows stored severities, report notices, approval settings and bot channels as defaults and the next save writes those
/// defaults back.</summary>
public class ViewerNotificationRouteColumnProbeTests
{
    [Fact]
    public void AProbeThatCannotTell_ReadsEveryRouteColumn()
    {
        Assert.Equal((true, true, true, true), ViewerDataService.RouteColumnsPresent(null));
    }

    [Theory]
    [InlineData(228, false, false, false, false)]
    [InlineData(229, true, false, false, false)]
    [InlineData(232, true, false, false, false)]
    [InlineData(233, true, true, false, false)]
    [InlineData(237, true, true, false, false)]
    [InlineData(238, true, true, true, false)]
    [InlineData(251, true, true, true, false)]
    [InlineData(252, true, true, true, true)]
    [InlineData(300, true, true, true, true)]
    public void AKnownVersion_ReadsOnlyTheColumnsItHas(int version, bool severities, bool reportNotice, bool approval, bool slackBotChannel)
    {
        Assert.Equal((severities, reportNotice, approval, slackBotChannel), ViewerDataService.RouteColumnsPresent(version));
    }

    /* #7656 (V252): the bot channel column is the newest the list reads, so its version is the one the probe compares against last. */
    [Fact]
    public void TheBotChannelColumn_ArrivesAtV252()
    {
        Assert.Equal(252, ViewerDataService.RouteSlackBotChannelStoreSchemaVersion);
        Assert.True(ViewerDataService.RouteColumnsPresent(ViewerDataService.RouteSlackBotChannelStoreSchemaVersion).SlackBotChannel);
        Assert.False(ViewerDataService.RouteColumnsPresent(ViewerDataService.RouteSlackBotChannelStoreSchemaVersion - 1).SlackBotChannel);
    }
}
