/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="ExplainJoinProbe"/> against hand-written <c>EXPLAIN (ANALYZE, FORMAT JSON)</c> plans, so the two
/// read-shape measures the live drill-down text tests assert are themselves pinned on every build, not only on a
/// run that has <c>DARLING_TEST_PG</c>.
/// </summary>
public sealed class ExplainJoinProbeTests
{
    private const string Dimension = "query_text_dim";

    private static string Explain(string plan) => "[{\"Plan\": " + plan + "}]";

    [Fact]
    public void RowsFetchedFrom_CountsTheRowsAScanByKeyReturned_WhereTheJoinProbeReadsNone()
    {
        var plan = Explain("""
            {"Node Type": "Index Scan", "Relation Name": "query_text_dim", "Actual Rows": 4, "Actual Loops": 1}
            """);

        Assert.Equal(4, ExplainJoinProbe.RowsFetchedFrom(plan, Dimension));

        /* No join to the relation, so nothing arrives as a probe: this is why the text read needs its own measure. */
        Assert.Equal(0, ExplainJoinProbe.RowsResolvedAgainst(plan, Dimension));
    }

    [Fact]
    public void RowsFetchedFrom_MultipliesByLoops_ThroughAJoinAndASubPlan()
    {
        var plan = Explain("""
            {"Node Type": "Nested Loop", "Join Type": "Left", "Actual Rows": 24, "Actual Loops": 1, "Plans": [
                {"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "query_stats",
                 "Actual Rows": 24, "Actual Loops": 1},
                {"Node Type": "Index Scan", "Parent Relationship": "Inner", "Relation Name": "query_text_dim",
                 "Actual Rows": 1, "Actual Loops": 24},
                {"Node Type": "Index Scan", "Parent Relationship": "SubPlan", "Relation Name": "query_text_dim",
                 "Actual Rows": 1, "Actual Loops": 5}
            ]}
            """);

        /* 24 probes of the join's inner side, plus 5 executions of the subplan. */
        Assert.Equal(29, ExplainJoinProbe.RowsFetchedFrom(plan, Dimension));

        /* The join probe counts the rows arriving at the join and leaves the subplan, which is not one. */
        Assert.Equal(24, ExplainJoinProbe.RowsResolvedAgainst(plan, Dimension));
    }

    [Fact]
    public void RowsFetchedFrom_IsZeroForAPlanThatNeverScansTheRelation()
    {
        var plan = Explain("""
            {"Node Type": "Sort", "Actual Rows": 24, "Actual Loops": 1, "Plans": [
                {"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "query_stats",
                 "Actual Rows": 24, "Actual Loops": 1}
            ]}
            """);

        Assert.Equal(0, ExplainJoinProbe.RowsFetchedFrom(plan, Dimension));
        Assert.Equal(0, ExplainJoinProbe.RowsResolvedAgainst(plan, Dimension));
    }
}
