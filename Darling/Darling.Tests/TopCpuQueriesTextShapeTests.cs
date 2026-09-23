/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3959's read shape, pinned without a store so it fails on every build, not only on a run that has
/// <c>DARLING_TEST_PG</c>. The live half, which shows the rows are the old rows and the plan resolves text
/// only for the printed groups, is <see cref="TopCpuQueriesTextLiveTests"/>. The text is byte-identical to
/// Lite's (<see cref="DrillDownDopProvenanceParityTests"/>), so this pins both SKUs' read.
/// </summary>
public sealed class TopCpuQueriesTextShapeTests
{
    [Fact]
    public void TheTopCpuRead_ResolvesTextAfterTheCut_NotInsideTheWindow()
    {
        /* Comments stripped: this SQL explains itself in prose that names the shape it replaced. */
        var sql = Regex.Replace(PgDrillDownCollector.TopCpuQueriesSql, @"--[^\n]*", string.Empty);

        var cut = sql.IndexOf("LIMIT 5", StringComparison.Ordinal);
        Assert.True(cut >= 0, "the cut to five should still be there");

        /* v_query_stats resolves text from the fleet's dimension for every row it returns, so neither the
           window nor the ranking may project it... */
        Assert.DoesNotContain("query_text", sql[..cut], StringComparison.Ordinal);

        /* ...it is read after the cut: equality for a keyed group, NULL-safe for a group with a NULL key. */
        Assert.Contains("SELECT LEFT(MAX(v.query_text), 500)", sql[cut..], StringComparison.Ordinal);
        Assert.Contains("v.query_hash = t.query_hash", sql[cut..], StringComparison.Ordinal);
        Assert.Contains("CASE WHEN t.database_name IS NULL OR t.query_hash IS NULL THEN", sql[cut..], StringComparison.Ordinal);
        Assert.Contains("v.query_hash IS NOT DISTINCT FROM t.query_hash", sql[cut..], StringComparison.Ordinal);
    }
}
