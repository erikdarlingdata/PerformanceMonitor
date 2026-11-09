/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Ui;

/// <summary>
/// Whether the plan viewer's Plan Insights panel opens expanded. The panel's cards take up to 220 px, under a 36 px toolbar and over the
/// query text expander's header, so in a viewer under <see cref="ExpandedFromHeight"/> px an expanded panel leaves the plan graph less than
/// half the height, and in a 900x600 window about 130 px. Under that height the panel starts collapsed and the graph gets the room; the
/// person opens the panel when they want it. At that height or more it starts expanded, as it does in a full-size window.
/// </summary>
public static class PlanInsightsLayout
{
    /// <summary>The viewer height, in px, from which the insights panel starts expanded.</summary>
    public const double ExpandedFromHeight = 640;

    /// <summary>True when a viewer <paramref name="viewerHeight"/> px tall opens the insights panel expanded.</summary>
    public static bool StartsExpanded(double viewerHeight) => viewerHeight >= ExpandedFromHeight;
}
