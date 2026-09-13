/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Darling.Analysis;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The analysis engine's two plan-XML reads resolve both content forms.
///
/// <para>Plan content for <c>query_stats</c> is diverted into the <c>query_plan_dim</c> dimension
/// (#1767) and, since #2069 / V54, new dimension rows carry gzip bytes in <c>query_plan_gz</c> with the
/// dimension's text column NULL. <c>v_query_stats.query_plan_xml</c> is
/// <c>COALESCE(fact.query_plan_xml, dim.query_plan_xml)</c>, so on a store running the default
/// <c>plan_xml_compression = 'gzip'</c> it is NULL for every row the dimension holds.</para>
///
/// <para><b>What that costs, and why it is worth its own pin.</b> A guard on the text column alone
/// matches nothing, the reader loop sees an empty result, and both collectors take their
/// <c>planXmls.Count == 0</c> early return — so <c>analyze_server</c> emits no MISSING_INDEX and no
/// PLAN_WARNING fact, and the finding drill-down renders no CREATE statements or warning strings. Zero
/// rows, no error, indistinguishable from a server with nothing to advise. A store set to
/// <c>plan_xml_compression = 'none'</c> (#2171) keeps working, which is what lets the defect hide.</para>
///
/// <para>The SQL pins alone would pass on a half-fix, so the source pin below is the load-bearing one:
/// with the projection widened but the loop still taking <c>GetString(0)</c>, every gzip row resolves to
/// a NULL text column and is skipped exactly as before.</para>
/// </summary>
public sealed class AnalysisPlanAdvisoryGzReadTests
{
    [Fact]
    public void PlanAdvisoryFactSql_ProjectsAndGuardsBothContentForms()
    {
        var sql = PgFactCollector.PlanAdvisorySql;

        Assert.Contains("SELECT query_plan_xml, query_plan_gz", sql, StringComparison.Ordinal);
        /* The RESOLVING view, not the base table: query_stats.query_plan_xml is NULL on every row
           written since #1767, so the base table cannot answer this read at all. */
        Assert.Contains("FROM v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("(query_plan_xml IS NOT NULL OR query_plan_gz IS NOT NULL)", sql, StringComparison.Ordinal);
        /* THE regression: the bare text guard, which discards every post-V54 plan silently. */
        Assert.DoesNotContain("AND   query_plan_xml IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY delta_worker_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 10", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanAdvisoryDrillDownSql_ProjectsAndGuardsBothContentForms()
    {
        var sql = PgDrillDownCollector.PlanAdvisoryXmlSql;

        Assert.Contains("SELECT query_plan_xml, query_plan_gz", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("(query_plan_xml IS NOT NULL OR query_plan_gz IS NOT NULL)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("AND   query_plan_xml IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY delta_worker_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 10", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The two reads stay byte-identical to each other.
    ///
    /// <para>They select the same top-ten-by-cost set on purpose: the fact collector counts what it finds
    /// and the drill-down renders the specifics of the same plans. Letting them drift is not a cosmetic
    /// difference — fixing one and not the other produces facts whose drill-down is empty, or a drill-down
    /// describing plans no fact counted. The half-fixed state is the likely one, because they live in
    /// different files and only one has to be found.</para>
    /// </summary>
    [Fact]
    public void TheTwoPlanAdvisoryReads_StayIdenticalToEachOther()
        => Assert.Equal(PgFactCollector.PlanAdvisorySql, PgDrillDownCollector.PlanAdvisoryXmlSql);

    /// <summary>
    /// Both reader loops resolve text-else-gzip rather than taking the text column alone.
    ///
    /// <para>A source pin because the thing being asserted is the read loop, not a value any constant
    /// exposes. Widening the projection without changing the loop leaves the defect fully intact: the gzip
    /// row's text column is NULL, <c>GetString(0)</c> never runs, and the row is skipped — so the SQL pins
    /// above would go green while <c>analyze_server</c> still emitted nothing.</para>
    /// </summary>
    [Fact]
    public void BothPlanAdvisoryReadLoops_ResolveTextElseGzip()
    {
        foreach (var file in new[] { "PgFactCollector.QueryPerf.cs", "PgDrillDownCollector.Plans.cs" })
        {
            var source = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Analysis", file));

            Assert.Contains("PayloadDimensions.ResolveContent(", source, StringComparison.Ordinal);
            Assert.Contains("reader.GetFieldValue<byte[]>(1)", source, StringComparison.Ordinal);
            /* The pre-fix loop, which drops every gzip row. */
            Assert.DoesNotContain("planXmls.Add(reader.GetString(0))", source, StringComparison.Ordinal);
        }
    }
}
