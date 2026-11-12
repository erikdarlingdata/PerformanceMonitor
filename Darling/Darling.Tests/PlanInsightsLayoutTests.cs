/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The plan viewer's Plan Insights panel starts collapsed in a viewer too short to give the plan graph most of the height with the panel
/// open (a 900x600 window leaves the graph about 130 px), and expanded in a full-size one.
/// </summary>
public sealed class PlanInsightsLayoutTests
{
    [Theory]
    [InlineData(520, false)]
    [InlineData(600, false)]
    [InlineData(639.5, false)]
    [InlineData(640, true)]
    [InlineData(900, true)]
    public void TheInsightsPanel_StartsCollapsedInAShortViewer_AndExpandedInATallOne(double viewerHeight, bool expanded)
    {
        Assert.Equal(expanded, PlanInsightsLayout.StartsExpanded(viewerHeight));
    }
}
