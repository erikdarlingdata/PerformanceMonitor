/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4233 ruling item 2: <c>BuildQueryHeatmapSql</c> is hand-copied into two files - the WPF viewer's chart
/// (<see cref="PerformanceMonitor.Darling.Viewer.ViewerDataService"/>) and the MCP/web reader behind
/// get_query_heatmap (<see cref="PerformanceMonitor.Darling.Service.Mcp.DarlingQueryHeatmapReader"/>) -
/// because the headless service must not reference the WPF viewer. A hand-edit that changes only one copy
/// is exactly the kind of drift a reviewer can miss in a large diff, so this pins the two byte-identical
/// through the final <c>WHERE rn = 1</c> filter - base/binned/ranked, the magnitude CASE, the top-1 window,
/// and #4233's own COALESCE/query_text_dim subquery - once the differences that predate #4233 are
/// normalized away:
/// <list type="bullet">
/// <item>the bin width: a literal 5-minute interval in the viewer vs. the reader's bound $5 parameter;</item>
/// <item>the preview width: a literal 120 in the viewer vs. the reader's bound $7 parameter (#4198).</item>
/// </list>
/// The <c>ORDER BY</c>/<c>LIMIT</c> tail AFTER <c>WHERE rn = 1</c> is a third, deliberate difference (the
/// reader caps and orders newest-first; the viewer has no cap and orders ascending) and is pinned on its
/// own by <c>DarlingQueryHeatmapSurfaceAndSqlTests.HeatmapSql_CapsFromTheRecentEnd</c>, so it is excluded
/// here by cutting the string rather than normalized.
///
/// <para>The two <c>HeatmapMetric</c> enums are kept fully qualified rather than imported side by side -
/// each namespace declares its own (the service must not reference the viewer's), and importing both would
/// make every unqualified <c>HeatmapMetric</c> in this file ambiguous. Casting the shared underlying int is
/// how "the same argument" reaches both builders without that ambiguity.</para>
///
/// <para>Proven live, not just by construction: temporarily changing one character inside either copy's
/// COALESCE/query_text_dim subquery text made this test fail before the character was reverted.</para>
/// </summary>
public sealed class QueryHeatmapSqlTwinParityTests
{
    private const string RnFilter = "WHERE rn = 1";

    [Theory]
    [InlineData(0)] // Duration
    [InlineData(1)] // Cpu
    [InlineData(2)] // LogicalReads
    [InlineData(3)] // LogicalWrites
    [InlineData(4)] // ExecutionCount
    public void BothCopies_AreByteIdentical_ThroughTheRnFilter(int metric)
    {
        var viewerSql = PerformanceMonitor.Darling.Viewer.ViewerDataService.BuildQueryHeatmapSql(
            (PerformanceMonitor.Darling.Viewer.HeatmapMetric)metric);
        var readerSql = PerformanceMonitor.Darling.Service.Mcp.DarlingQueryHeatmapReader.BuildQueryHeatmapSql(
            (PerformanceMonitor.Darling.Service.Mcp.HeatmapMetric)metric);

        /* Both known normalizations must actually fire - if a future edit changes the literal/parameter
           spelling on either side, this catches the normalization silently becoming a no-op, which the
           Assert.Equal below alone could miss if an unrelated one-sided edit happened to cancel it out. */
        const string ReaderBinWidth = "date_bin(($5::integer * INTERVAL '1 minute'), collection_time, TIMESTAMP '1970-01-01 00:00:00')";
        const string ViewerBinWidth = "date_bin(INTERVAL '5 minutes', collection_time, TIMESTAMP '1970-01-01 00:00:00')";
        const string ReaderPreviewWidth = "query_text_digest)), $7) AS top_query_text";
        const string ViewerPreviewWidth = "query_text_digest)), 120) AS top_query_text";

        Assert.Contains(ReaderBinWidth, readerSql, StringComparison.Ordinal);
        Assert.Contains(ViewerBinWidth, viewerSql, StringComparison.Ordinal);
        Assert.Contains(ReaderPreviewWidth, readerSql, StringComparison.Ordinal);
        Assert.Contains(ViewerPreviewWidth, viewerSql, StringComparison.Ordinal);

        var normalizedReader = readerSql
            .Replace(ReaderBinWidth, ViewerBinWidth, StringComparison.Ordinal)
            .Replace(ReaderPreviewWidth, ViewerPreviewWidth, StringComparison.Ordinal);

        var viewerHead = viewerSql[..(viewerSql.IndexOf(RnFilter, StringComparison.Ordinal) + RnFilter.Length)];
        var readerHead = normalizedReader[..(normalizedReader.IndexOf(RnFilter, StringComparison.Ordinal) + RnFilter.Length)];

        Assert.Equal(viewerHead, readerHead);
    }
}
