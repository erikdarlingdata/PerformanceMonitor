/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4534: <see cref="PlanWarningDisplay.OriginNavigationText"/> pulls the viewer's origin-link text
/// out of <c>PlanViewerControl.Rendering.cs</c> (WPF, can't run in a unit test on macOS) so this
/// suite can pin it directly. New member, so every assertion here is a compile-only RED against
/// dev: <c>OriginNavigationText</c> doesn't exist on dev.
/// </summary>
public sealed class PlanViewerOriginNavigationTests
{
    [Fact]
    public void OriginNavigationText_NoIds_ReturnsNull()
    {
        Assert.Null(PlanWarningDisplay.OriginNavigationText(new List<int>()));
    }

    [Fact]
    public void OriginNavigationText_OneId_HasSuffixAndSingleTooltip()
    {
        var nav = PlanWarningDisplay.OriginNavigationText(new List<int> { 7 });

        Assert.NotNull(nav);
        Assert.Equal("  \u2192", nav!.Value.Suffix);
        Assert.Equal("Go to operator (Node 7)", nav.Value.Tooltip);
    }

    [Fact]
    public void OriginNavigationText_ThreeIds_ListsRestInTooltip()
    {
        var nav = PlanWarningDisplay.OriginNavigationText(new List<int> { 7, 9, 12 });

        Assert.NotNull(nav);
        Assert.Equal("  \u2192", nav!.Value.Suffix);
        Assert.Equal("Go to Node 7 \u2014 also from Node 9, Node 12", nav.Value.Tooltip);
    }
}
