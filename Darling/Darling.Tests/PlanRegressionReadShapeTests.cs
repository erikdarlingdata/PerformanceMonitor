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
/// #3902's two drill-down read shapes, pinned without a store so they fail on every build, not only on a
/// run that has <c>DARLING_TEST_PG</c>. The live halves — the rows are the old rows, and the plans read what
/// these shapes say they read — are <see cref="PlanRegressionDrillDownReuseLiveTests"/> and
/// <see cref="ParameterSensitiveDrillDownTextLiveTests"/>.
///
/// <para>Comments are stripped before every assertion: this SQL explains itself in prose that names the
/// shapes it replaced, and a pin that matched the prose would pass on the strength of a comment.</para>
/// </summary>
public sealed class PlanRegressionReadShapeTests
{
    [Fact]
    public void TheRegressedQueriesDedup_ReadsTheFactsOffenders_AndEveryQueryOnlyWithoutThem()
    {
        var sql = PgDrillDownCollector.RegressedQueriesSql;
        var start = sql.IndexOf("\ndeduped AS", StringComparison.Ordinal);
        var end = sql.IndexOf("\nplan_dedup AS", StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "the deduped CTE should still precede plan_dedup");

        var dedup = StripComments(sql[start..end]);

        /* Restricted inside the dedup, before the sort that is the read's whole cost — a filter anywhere
           later would still deduplicate the full slice first. NULL is the only unrestricted spelling. */
        Assert.Contains(
            "($5::text[] IS NULL OR (database_name = ANY($5::text[]) AND query_id = ANY($6::bigint[])))",
            dedup,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ThePlanRegressionFact_NamesEachOffendersDatabase_AfterEveryColumnItAlreadyReturned()
    {
        var sql = StripComments(PgFactCollector.PlanRegressionSql);

        /* The comparison carries the key the drill-down restricts on. */
        Assert.Contains("l.database_name", sql, StringComparison.Ordinal);

        /* Appended LAST, at ordinal 8, which is where the collector reads it: the eight columns before it are
           read by ordinal and must not move. */
        Assert.Matches(
            new Regex(@"regression_factor,\s+database_name\s+FROM compared", RegexOptions.Singleline),
            sql);
    }

    [Fact]
    public void TheParameterSensitivityDrillDown_ResolvesTextForThePrintedRowsOnly()
    {
        var sql = StripComments(PgDrillDownCollector.ParameterSensitiveSql);

        /* The view resolves text for every row it returns by joining the fleet's text dimension. */
        Assert.DoesNotContain("v_query_stats", sql, StringComparison.Ordinal);

        /* The dimension is joined after the cut, so only the printed rows are resolved... */
        var cut = sql.IndexOf("LIMIT 5", StringComparison.Ordinal);
        var join = sql.IndexOf("JOIN query_text_dim", StringComparison.Ordinal);
        Assert.True(cut >= 0 && join > cut, "query_text_dim must be joined after the LIMIT, not inside the window");

        /* ...with the view's own expression: inline legacy text first, then the digest's. */
        Assert.Contains("LEFT(COALESCE(o.query_text, qtd.query_text), 500)", sql, StringComparison.Ordinal);
    }

    private static string StripComments(string sql) => Regex.Replace(sql, @"--[^\n]*", string.Empty);
}
