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
/// or the list shows stored severities, report notices and approval settings as defaults and the next save writes those defaults back.</summary>
public class ViewerNotificationRouteColumnProbeTests
{
    [Fact]
    public void AProbeThatCannotTell_ReadsEveryRouteColumn()
    {
        Assert.Equal((true, true, true), ViewerDataService.RouteColumnsPresent(null));
    }

    [Theory]
    [InlineData(228, false, false, false)]
    [InlineData(229, true, false, false)]
    [InlineData(232, true, false, false)]
    [InlineData(233, true, true, false)]
    [InlineData(237, true, true, false)]
    [InlineData(238, true, true, true)]
    [InlineData(300, true, true, true)]
    public void AKnownVersion_ReadsOnlyTheColumnsItHas(int version, bool severities, bool reportNotice, bool approval)
    {
        Assert.Equal((severities, reportNotice, approval), ViewerDataService.RouteColumnsPresent(version));
    }
}
