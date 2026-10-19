/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3547/#3556's Darling half — the twin of <c>Lite.Tests.QuerySlicerMetricMapTests</c>. The viewer's grid
/// sorting handlers translate a sorted column name into a slicer metric key and a label, and the value
/// switch below each translates that key into a bucket field — three hops where a physical-reads sort
/// silently plotted the LOGICAL aggregate (both the procedures and Query Store grids here; Lite fixed its
/// copies in #3550/#3567 and this port stayed broken), and where slice TOTALS were labeled "Avg". The
/// handlers are private event handlers on a WPF <c>UserControl</c>, so this pins the source text rather than
/// instantiating the control; the value-level halves are pinned with distinct per-column fixture values by
/// the live slicer and dedup tests in <c>ViewerQueriesLivePostgresTests</c>, so between them a
/// logical/physical swap on either side of the seam goes red.
/// </summary>
public sealed class ViewerSlicerMetricMapTests
{
    private const string QueriesSource = "Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.Queries.cs";

    private static string HandlerBody(string handlerName)
    {
        var source = ParitySourceLocal.ReadFile(QueriesSource);
        var match = Regex.Match(
            source,
            @"private void " + handlerName + @".*?(?=\n    private )",
            RegexOptions.Singleline);
        Assert.True(match.Success, $"{QueriesSource}: {handlerName} not found — update this pin if the handler moved.");
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

    /// <summary>
    /// The viewer-specific half of the fix: Lite's sorting handlers re-run the SelectionChanged handler
    /// after a metric change so a selected row's overlay is re-projected onto the new metric, and this port
    /// shipped without that — so honest bars would have drawn under a stale-metric overlay, the exact
    /// mismatch the #3547 fix's commit warned about. Pinned per handler, because the omission was per
    /// handler.
    /// </summary>
    [Theory]
    [InlineData("QueryStatsGrid_Sorting", "QueryStatsGrid_SelectionChanged(QueryStatsGrid, null!)")]
    [InlineData("ProcedureStatsGrid_Sorting", "ProcedureStatsGrid_SelectionChanged(ProcedureStatsGrid, null!)")]
    [InlineData("QueryStoreGrid_Sorting", "QueryStoreGrid_SelectionChanged(QueryStoreGrid, null!)")]
    public void EverySortingHandler_RecomputesTheOverlayOnMetricChange(string handlerName, string retrigger)
    {
        var body = HandlerBody(handlerName);

        Assert.Contains(retrigger, body, System.StringComparison.Ordinal);
    }
}
