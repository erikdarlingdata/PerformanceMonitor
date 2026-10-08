/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Ui;

/// <summary>
/// What the Compare grids (Top Queries, Top Procedures, Query Store) say when the baseline period held nothing. A
/// store newer than the offset ("Yesterday" on a store a few hours old) has no baseline rows, so every row reads NEW
/// with no delta, and nothing told the user why.
/// </summary>
public static class ComparisonBaselineNote
{
    /// <summary>Appended to the "Comparing against baseline" banner when the baseline period has no rows.</summary>
    public const string NoBaselineData = " - no data was collected in that period, so there is nothing to compare against";

    /// <summary>True when no item has any baseline executions (an empty list included).</summary>
    public static bool HasNoBaselineData(IEnumerable<ComparisonItemBase> items) =>
        !items.Any(i => i.BaselineExecutionCount > 0);

    /// <summary>The banner text with the no-data note added when the baseline period held nothing.</summary>
    public static string Banner(string bannerText, IEnumerable<ComparisonItemBase> items) =>
        HasNoBaselineData(items) ? bannerText + NoBaselineData : bannerText;
}
