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

        /* Appended at ordinal 8, which is where the collector reads it: the eight columns before it are read by
           ordinal and must not move. #3953 appended the best plan's last run after it, at ordinal 9, for the same
           reason, and both PLAN_REGRESSION reads share the text (the interval-table twin's suffix). */
        Assert.Matches(
            new Regex(@"regression_factor,\s+database_name,\s+best_last_exec\s+FROM compared", RegexOptions.Singleline),
            sql);
        Assert.Matches(
            new Regex(@"regression_factor,\s+database_name,\s+best_last_exec\s+FROM compared", RegexOptions.Singleline),
            StripComments(PgFactCollector.PlanRegressionTableSql));
    }

    [Fact]
    public void TheParameterSensitivityDrillDown_ReadsNoTextDimension_AndHandsBackTheDigestLast()
    {
        var sql = StripComments(PgDrillDownCollector.ParameterSensitiveSql);

        /* The view resolves text for every row it returns by joining the fleet's text dimension. */
        Assert.DoesNotContain("v_query_stats", sql, StringComparison.Ordinal);

        /* Since #4821 the cap of five is the reader's (the compiled-before-the-window test runs on the converted time
           before it), so this read carries no LIMIT and every plan that passes the rough filter comes back. A join to
           the text dimension here, however late in the statement, resolves text for all of them to print five (#3902).
           So the read names no dimension at all: it hands back the inline legacy text, and the digest rides last for
           the reader's second read, which resolves the plans the reader keeps. */
        Assert.DoesNotContain("LIMIT 5", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_text_dim", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT(o.query_text, 500) AS query_text", sql, StringComparison.Ordinal);
        Assert.Matches(
            new Regex(@"o\.time_zone_id,\s+o\.query_text_digest\s+FROM offenders AS o\s+ORDER BY", RegexOptions.Singleline),
            sql);
    }

    [Fact]
    public void TheParameterSensitivityTextRead_BindsTheDigestsAsOneArrayParameter_AndReadsOnlyTheDimension()
    {
        var sql = StripComments(PgDrillDownCollector.ParameterSensitiveTextSql);

        /* One dimension row per digest by primary key: the read touches the digests it was given and nothing else. The
           cut to 500 characters stays in SQL, so it is still PostgreSQL's character count. */
        Assert.Matches(
            new Regex(@"^\s*SELECT digest, LEFT\(query_text, 500\)\s+FROM query_text_dim\s+WHERE digest = ANY\(\$1\)\s*$"),
            sql);

        /* One array parameter however many plans print, never a parameter or a list literal per digest. */
        Assert.DoesNotContain("$2", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("JOIN", sql, StringComparison.OrdinalIgnoreCase);
    }

    private static string StripComments(string sql) => Regex.Replace(sql, @"--[^\n]*", string.Empty);
}
