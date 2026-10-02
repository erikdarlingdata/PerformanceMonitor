/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3547/#3556: the grid sorting handlers translate a sorted column name into a slicer metric key and a
/// label, and the value switch below each translates that key into a bucket field — three hops where a
/// physical-reads sort silently plotted the LOGICAL aggregate (Query Store first, then its twin in the
/// procedures grid), and where slice TOTALS were labeled "Avg". The handlers are private event handlers on
/// a WPF <c>UserControl</c>, so like <see cref="AvailabilityGroupsGridSortTests"/> this pins the source
/// text rather than instantiating the control; the value-level halves are pinned with distinct per-column
/// fixture values by <c>QueryStoreDedupReadTests</c> and <c>ProcStatsSlicerReadTests</c>, so between them a
/// logical/physical swap on either side of the seam goes red.
/// </summary>
public sealed class QuerySlicerMetricMapTests
{
    private const string GridsSource = "Lite/Controls/ServerTab.Grids.cs";

    private static string HandlerBody(string handlerName)
    {
        var source = ParitySource.ReadFile(GridsSource);
        var match = Regex.Match(
            source,
            @"private void " + handlerName + @".*?(?=\n    private )",
            RegexOptions.Singleline);
        Assert.True(match.Success, $"{GridsSource}: {handlerName} not found — update this pin if the handler moved.");
        return match.Value;
    }

    [Fact]
    public void QueryStore_PhysicalSort_MapsToThePhysicalSeries()
    {
        var body = HandlerBody("QueryStoreGrid_Sorting");

        Assert.Matches(@"""AvgPhysicalReads""\s*=>\s*\(""TotalPhysReads"",\s*""Total Physical Reads""\)", body);
        Assert.Matches(@"""TotalPhysReads""\s*=>\s*bucket\.TotalPhysicalReads", body);
    }

    /// <summary>#3556's label half: the plotted bucket values are execution-weighted slice TOTALS, so an
    /// "Avg" label under-claimed what the bars showed.</summary>
    [Fact]
    public void QueryStore_SliceTotalMetrics_CarryTotalLabels()
    {
        var body = HandlerBody("QueryStoreGrid_Sorting");

        Assert.Matches(@"""AvgLogicalReads""\s*=>\s*\(""TotalReads"",\s*""Total Reads""\)", body);
        Assert.Matches(@"""AvgLogicalWrites""\s*=>\s*\(""TotalWrites"",\s*""Total Writes""\)", body);
    }

    [Fact]
    public void Procedures_PhysicalSort_MapsToThePhysicalSeries()
    {
        var body = HandlerBody("ProcedureStatsGrid_Sorting");

        Assert.Matches(@"""TotalPhysicalReads""\s*=>\s*\(""TotalPhysReads"",\s*""Total Physical Reads""\)", body);
        Assert.Matches(@"""TotalPhysReads""\s*=>\s*bucket\.TotalPhysicalReads", body);
    }
}
