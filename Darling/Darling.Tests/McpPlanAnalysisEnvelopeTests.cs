/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A15/A16 — the shared plan-analysis envelope (<see cref="McpPlanAnalysisFormatter"/>, behind every
/// <c>analyze_*_plan</c> tool on both SKUs and the frozen Dashboard twin) states its cuts and labels its numbers.
///
/// <para>Two lies it told. <c>top_operators</c> was a silent <c>Take(10)</c> whose ranking basis switched between a
/// measured <c>actual_elapsed_ms</c> and the optimizer's <c>cost_percent</c> estimate with nothing in the payload
/// saying which — a caller comparing two plans' top operators could be comparing a stopwatch to a guess. And
/// <c>missing_indexes[].impact</c> was a bare number (the showplan <c>MissingIndexGroup/@Impact</c>, an estimated
/// statement-scoped percent) with nothing saying so — the unlabelled denominator was the A15/A16 item, and
/// <c>impact_basis</c> is its fix.</para>
///
/// <para>One correction it made and then took back (#3805). #3696 also dropped <c>missing_indexes[].create_statement</c>
/// — the parser's CREATE INDEX for each group — put a "a hint, not a design" <c>note</c> in its place and pinned the
/// absence here, on a "no-missing-index-recs rule" that was never made. The maintainer: "i never made that rule?
/// there are several things that recommend ddl, e.g. enabling rcsi in the analysis engine." — and then, as the spec
/// for the restore: the DMV's <c>uses</c>-class metric is plan-cache-bounded and <c>impact</c> is one operator's
/// statement-scoped estimated cost share; a suggestion is "legitimate as corroboration when traced FROM a
/// measured-slow query", "never a defining characteristic of the engine and never delivered without serious caveats
/// — including the regression risk a new index carries". So the restore is not "put the DDL back": the statement
/// returns on every row, <c>impact_basis</c> stays, and every row carries ONE fixed <c>caveat</c> sentence — the
/// maintainer's text verbatim, <see cref="McpPlanAnalysisFormatter.MissingIndexCaveat"/> — with the descriptions
/// reframed from suppression ("hint, not a design") to honesty ("corroboration, with caveats"). The pins below assert
/// the statement's PRESENCE and SHAPE on every group (the parser's key order, INCLUDE list and generated name), the
/// caveat's exact text on every group, and that the same sentence is the one the shared analysis engine opens its
/// MISSING_INDEX card with (<c>FactAdvice.MissingIndexCaveat</c>, a duplicated literal because
/// <c>PerformanceMonitor.Analysis</c> references no project) — one sentence, every emitter. DDL recommendations are
/// legitimate product output where the evidence supports them: the engine's RCSI remediation is the precedent, and the
/// engine's own MISSING_INDEX finding renders this very statement as <c>remediation_command</c>, so for the life of
/// #3696 the plan tools withheld what <c>get_analysis_findings</c> handed out.</para>
///
/// <para>Synthetic plans rather than fixtures from a server: the properties under test are counts and labels over a
/// known operator population, so the plan is built to have exactly the shape each assertion needs (twelve RelOps
/// nested one under the other, one missing-index group) and the expected numbers are facts of the test.</para>
/// </summary>
public sealed class McpPlanAnalysisEnvelopeTests
{
    /// <summary>The parser wraps the root RelOp in a synthetic statement node (NodeId -1), so a plan with N
    /// RelOps has N + 1 operators in the population <c>top_operators</c> ranks. Pinned as a fact so the
    /// assertions below read as arithmetic rather than magic.</summary>
    private const int SyntheticStatementNodes = 1;

    [Fact]
    public void TopOperators_PublishesTheCut_TheCount_ObservedTruncation_AndTheEstimateBasis()
    {
        const int relOps = 12;
        using var doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(EstimatedPlan(relOps, withMissingIndex: true), "s", "xml", null));
        var stmt = Assert.Single(doc.RootElement.GetProperty("statements").EnumerateArray());

        Assert.Equal(McpPlanAnalysisFormatter.TopOperatorCap, stmt.GetProperty("operators_cap").GetInt32());
        Assert.Equal(relOps + SyntheticStatementNodes, stmt.GetProperty("total_operators").GetInt32());
        Assert.Equal(McpPlanAnalysisFormatter.TopOperatorCap, stmt.GetProperty("operators_returned").GetInt32());
        Assert.Equal(McpPlanAnalysisFormatter.TopOperatorCap, stmt.GetProperty("top_operators").GetArrayLength());
        Assert.True(stmt.GetProperty("truncated").GetBoolean());

        /* No RunTimeInformation anywhere → an estimated plan → ranked by the optimizer's cost share, and the
           payload SAYS so rather than leaving has_actual_stats as the only hint. */
        Assert.False(stmt.GetProperty("has_actual_stats").GetBoolean());
        Assert.Equal(McpPlanAnalysisFormatter.RankedByEstimatedCostPercent, stmt.GetProperty("operators_ranked_by").GetString());
        Assert.Contains("estimate", McpPlanAnalysisFormatter.RankedByEstimatedCostPercent, StringComparison.Ordinal);

        /* The ranking IS by cost_percent, descending — the label names the order the rows are actually in. */
        var costs = stmt.GetProperty("top_operators").EnumerateArray().Select(o => o.GetProperty("cost_percent").GetInt32()).ToList();
        Assert.Equal(costs.OrderByDescending(c => c).ToList(), costs);
    }

    /// <summary>
    /// #7887. A row-mode operator's <c>ActualElapsedMs</c> is cumulative over its subtree, so a rank on it is the
    /// root path and the leaf that did the work is the first one the cap cuts. Shape: twelve operators nested one
    /// under the next, a Clustered Index Scan at the bottom owning 900 ms, and each of the eleven Nested Loops
    /// above it adding 5 ms of its own, so the root's cumulative elapsed is 955 ms and the scan sits under eleven
    /// ancestors, past the ten-operator cap. The scan must lead <c>top_operators</c>, the basis must say
    /// <c>self_elapsed_ms</c>, and every row carries its own time beside the cumulative one.
    /// </summary>
    [Fact]
    public void TopOperators_OnARowModeActualPlan_LeadWithTheOperatorThatOwnsTheTime_NotTheRootPath()
    {
        const int relOps = 12;
        const long scanMs = 900, ownMsPerAncestor = 5;
        using var doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(ActualRowModePlan(relOps, scanMs, ownMsPerAncestor), "s", "xml", null));
        var stmt = Assert.Single(doc.RootElement.GetProperty("statements").EnumerateArray());

        Assert.True(stmt.GetProperty("has_actual_stats").GetBoolean());
        Assert.Equal("self_elapsed_ms", McpPlanAnalysisFormatter.RankedBySelfElapsed);
        Assert.Equal(McpPlanAnalysisFormatter.RankedBySelfElapsed, stmt.GetProperty("operators_ranked_by").GetString());
        Assert.True(stmt.GetProperty("truncated").GetBoolean());

        var top = stmt.GetProperty("top_operators").EnumerateArray().ToList();
        Assert.Equal(McpPlanAnalysisFormatter.TopOperatorCap, top.Count);

        /* The scan is the leaf (NodeId 11): it owns 900 of the statement's 955 ms and leads the list. */
        Assert.Equal(relOps - 1, top[0].GetProperty("node_id").GetInt32());
        Assert.Equal("Clustered Index Scan", top[0].GetProperty("physical_op").GetString());
        Assert.Equal(scanMs, top[0].GetProperty("self_elapsed_ms").GetInt64());
        Assert.Equal(scanMs, top[0].GetProperty("actual_elapsed_ms").GetInt64());

        /* The root Nested Loops (NodeId 0) carries the whole 955 ms cumulatively but owns only 5 of it. */
        var root = Assert.Single(top, o => o.GetProperty("node_id").GetInt32() == 0);
        Assert.Equal(scanMs + (relOps - 1) * ownMsPerAncestor, root.GetProperty("actual_elapsed_ms").GetInt64());
        Assert.Equal(ownMsPerAncestor, root.GetProperty("self_elapsed_ms").GetInt64());

        /* Every row carries both figures, own time never above cumulative, and the list is in own-time order. */
        var selfs = top.Select(o => o.GetProperty("self_elapsed_ms").GetInt64()).ToList();
        Assert.Equal(selfs.OrderByDescending(s => s).ToList(), selfs);
        Assert.All(top, o => Assert.True(o.GetProperty("self_elapsed_ms").GetInt64() <= o.GetProperty("actual_elapsed_ms").GetInt64()));
    }

    /// <summary>
    /// #7887 on a real parallel plan (PerformanceStudio's <c>eager_index_spool_plan.sqlplan</c>, #4534). The root is a
    /// Gather Streams exchange whose cumulative elapsed is the whole 17,647 ms statement; it owns 1 ms of it. Under it a
    /// Nested Loops (1 ms own) and a Top (0 ms own) sit over the Eager Index Spool that owns 17,613 ms. Ranked by
    /// cumulative time the exchange leads and the spool is fourth; ranked by own time the spool leads and the exchange
    /// carries its cumulative figure beside its own.
    /// </summary>
    [Fact]
    public void TopOperators_OnAParallelFixture_LeadWithTheSpoolThatOwnsTheTime_NotTheGatherStreamsAboveIt()
    {
        var xml = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", "eager_index_spool_plan.sqlplan"));
        using var doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(xml, "s", "xml", null));
        var stmt = Assert.Single(doc.RootElement.GetProperty("statements").EnumerateArray());

        Assert.True(stmt.GetProperty("parallel").GetBoolean());
        Assert.Equal(McpPlanAnalysisFormatter.RankedBySelfElapsed, stmt.GetProperty("operators_ranked_by").GetString());
        var top = stmt.GetProperty("top_operators").EnumerateArray().ToList();

        Assert.Equal(5, top[0].GetProperty("node_id").GetInt32());
        Assert.Contains("Spool", top[0].GetProperty("physical_op").GetString(), StringComparison.Ordinal);
        Assert.Equal(17_613, top[0].GetProperty("self_elapsed_ms").GetInt64());

        var gather = Assert.Single(top, o => o.GetProperty("node_id").GetInt32() == 0);
        Assert.Equal("Parallelism", gather.GetProperty("physical_op").GetString());
        Assert.Equal(17_647, gather.GetProperty("actual_elapsed_ms").GetInt64());
        Assert.Equal(1, gather.GetProperty("self_elapsed_ms").GetInt64());
    }

    /// <summary>#7887: an estimated plan has no measurement, so <c>self_elapsed_ms</c> is null on every row (null,
    /// not 0, the #3541 A12 rule for every actual_* field) and the rank stays the optimizer's cost share.</summary>
    [Fact]
    public void TopOperators_OnAnEstimatedPlan_CarryANullSelfElapsed_AndKeepTheCostBasis()
    {
        using var doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(EstimatedPlan(4, withMissingIndex: false), "s", "xml", null));
        var stmt = Assert.Single(doc.RootElement.GetProperty("statements").EnumerateArray());

        Assert.Equal(McpPlanAnalysisFormatter.RankedByEstimatedCostPercent, stmt.GetProperty("operators_ranked_by").GetString());
        Assert.All(stmt.GetProperty("top_operators").EnumerateArray(), o =>
        {
            Assert.Equal(JsonValueKind.Null, o.GetProperty("self_elapsed_ms").ValueKind);
            Assert.Equal(JsonValueKind.Null, o.GetProperty("actual_elapsed_ms").ValueKind);
        });
    }

    [Fact]
    public void TopOperators_UnderTheCap_ReturnsEveryOperator_AndIsNotTruncated()
    {
        const int relOps = 3;
        using var doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(EstimatedPlan(relOps, withMissingIndex: false), "s", "xml", null));
        var stmt = Assert.Single(doc.RootElement.GetProperty("statements").EnumerateArray());

        Assert.Equal(relOps + SyntheticStatementNodes, stmt.GetProperty("total_operators").GetInt32());
        Assert.Equal(relOps + SyntheticStatementNodes, stmt.GetProperty("operators_returned").GetInt32());
        Assert.Equal(relOps + SyntheticStatementNodes, stmt.GetProperty("top_operators").GetArrayLength());
        Assert.False(stmt.GetProperty("truncated").GetBoolean());
        Assert.Equal(0, stmt.GetProperty("missing_indexes").GetArrayLength());
        Assert.Equal(0, doc.RootElement.GetProperty("total_missing_indexes").GetInt32());
    }

    [Fact]
    public void MissingIndexes_CarryColumnListsALabelledImpact_AndTheCreateStatement()
    {
        using var doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(EstimatedPlan(2, withMissingIndex: true), "s", "xml", null));
        var stmt = Assert.Single(doc.RootElement.GetProperty("statements").EnumerateArray());
        var idx = Assert.Single(stmt.GetProperty("missing_indexes").EnumerateArray());

        Assert.Equal("dbo.Posts", idx.GetProperty("table").GetString());
        Assert.Equal(87.5, idx.GetProperty("impact").GetDouble());
        Assert.Equal(McpPlanAnalysisFormatter.MissingIndexImpactBasis, idx.GetProperty("impact_basis").GetString());
        Assert.Contains("statement", McpPlanAnalysisFormatter.MissingIndexImpactBasis, StringComparison.Ordinal);
        Assert.Contains("estimate", McpPlanAnalysisFormatter.MissingIndexImpactBasis, StringComparison.Ordinal);

        Assert.Equal(new[] { "OwnerUserId" }, idx.GetProperty("equality_columns").EnumerateArray().Select(c => c.GetString()).ToArray());
        Assert.Equal(new[] { "CreationDate" }, idx.GetProperty("inequality_columns").EnumerateArray().Select(c => c.GetString()).ToArray());
        Assert.Equal(new[] { "Score" }, idx.GetProperty("include_columns").EnumerateArray().Select(c => c.GetString()).ToArray());

        /* #3805: the statement is on the wire again, and it is the parser's rendering of THIS group — key columns
           equality-then-inequality, the INCLUDE list, a generated name — not a placeholder and not the note. */
        Assert.True(idx.TryGetProperty("create_statement", out var create));
        Assert.Equal(
            "CREATE NONCLUSTERED INDEX [Posts_OwnerUserId_CreationDate]\nON [dbo].[Posts] ([OwnerUserId], [CreationDate])\nINCLUDE ([Score]);",
            create.GetString());

        /* The caveat beside it is the maintainer's fixed sentence, verbatim — the exact const, not a paraphrase —
           and #3696's "a hint, not a design" note is gone from the wire (no `note` key) with the rule that was never
           made. */
        Assert.Equal(McpPlanAnalysisFormatter.MissingIndexCaveat, idx.GetProperty("caveat").GetString());
        Assert.False(idx.TryGetProperty("note", out _));
        Assert.DoesNotContain("a hint, not a design", idx.GetRawText(), StringComparison.Ordinal);
        Assert.Equal(1, doc.RootElement.GetProperty("total_missing_indexes").GetInt32());
    }

    /// <summary>
    /// The caveat's TEXT, pinned verbatim (#3805): the maintainer's parity sentence for both engines and both SKUs,
    /// asserted as one exact string — a paraphrase that kept the meaning would still fail, because the point of a
    /// shared sentence is that every reader sees the same one. Then its three clauses named, so a future edit has to
    /// argue with each: the evidence grade (plan-cache-bounded counters, one operator's estimated cost), the role
    /// (corroborates, never drives a finding), and the regression risk (other statements, write cost — test it).
    /// </summary>
    [Fact]
    public void TheCaveat_IsTheMaintainersSentenceVerbatim()
    {
        const string parity = "Missing-index requests are weak evidence: uses are plan-cache-bounded and \"impact\" is one operator's estimated cost. A request corroborates a measured-slow plan; it never drives a finding. Any new index can regress other statements and adds write cost — test it.";
        Assert.Equal(parity, McpPlanAnalysisFormatter.MissingIndexCaveat);

        Assert.Contains("plan-cache-bounded", McpPlanAnalysisFormatter.MissingIndexCaveat, StringComparison.Ordinal);
        Assert.Contains("one operator's estimated cost", McpPlanAnalysisFormatter.MissingIndexCaveat, StringComparison.Ordinal);
        Assert.Contains("corroborates a measured-slow plan; it never drives a finding", McpPlanAnalysisFormatter.MissingIndexCaveat, StringComparison.Ordinal);
        Assert.Contains("regress other statements and adds write cost", McpPlanAnalysisFormatter.MissingIndexCaveat, StringComparison.Ordinal);
    }

    /// <summary>
    /// One sentence, every emitter (#3805). The shared analysis engine cannot reference the PlanAnalysis constant —
    /// <c>PerformanceMonitor.Analysis</c> references no project — so <c>FactAdvice.MissingIndexCaveat</c> is a
    /// duplicated literal, and this is the pin that keeps the two byte-identical. The engine's MISSING_INDEX card
    /// opens with it on BOTH paths: the static block (<c>GetForFactKey</c>) and the composed one, which REPLACES the
    /// static investigation whenever the fact carries its metadata — which it always does — so a caveat on the
    /// static block alone would never reach a reader (<c>Compose</c> is what the pass freezes into StoryText). And the fact scores the Information rung, below every standing
    /// misconfiguration advisory: a request is corroboration, not a peer of a bad MAXDOP.
    /// </summary>
    [Fact]
    public void TheCaveat_IsOneSentence_OnThePlanToolsAndTheAnalysisEngine()
    {
        Assert.Equal(McpPlanAnalysisFormatter.MissingIndexCaveat, FactAdvice.MissingIndexCaveat);

        var stat = FactAdvice.GetForFactKey("MISSING_INDEX");
        Assert.NotNull(stat);
        Assert.StartsWith(McpPlanAnalysisFormatter.MissingIndexCaveat, stat!.Investigation, StringComparison.Ordinal);

        var fact = new Fact
        {
            Source = "queries",
            Key = "MISSING_INDEX",
            Value = 3,
            Metadata = new Dictionary<string, double> { ["index_count"] = 3, ["max_impact"] = 42.5 }
        };
        var composed = FactAdvice.Compose("MISSING_INDEX", new[] { fact }.ToFactLookup());
        Assert.NotNull(composed);
        Assert.StartsWith(McpPlanAnalysisFormatter.MissingIndexCaveat, composed!.Investigation, StringComparison.Ordinal);
        Assert.Contains("3 missing-index suggestions", composed.Investigation, StringComparison.Ordinal);

        /* The rung: the Information severity, the same position ConfigChangeAttribution roots at, below the 0.4
           CONFIG_* advisory base and the 0.3 autogrowth base, above zero so it still roots. */
        Assert.Equal(ConfigChangeAttribution.InformationSeverity, FactScorer.MissingIndexCorroborationSeverity);
        var scored = new Fact { Source = "queries", Key = "MISSING_INDEX", Value = 2 };
        new FactScorer().ScoreAll(new List<Fact> { scored });
        Assert.Equal(FactScorer.MissingIndexCorroborationSeverity, scored.BaseSeverity, precision: 6);
        Assert.True(scored.BaseSeverity < 0.4);
        Assert.True(scored.BaseSeverity > 0);
    }

    [Fact]
    public void MissingIndexes_CarryTheCreateStatement_OnEveryGroup_NotJustTheFirst()
    {
        /* Two groups on one statement: the field is per ROW, so a formatter that emitted it on one group and
           omitted it on another (or copied the first group's text onto the second) would pass a single-group
           pin and fail this one. Each statement names its own table and its own key columns. */
        using var doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(EstimatedPlan(2, withMissingIndex: true, secondGroupOnUsers: true), "s", "xml", null));
        var stmt = Assert.Single(doc.RootElement.GetProperty("statements").EnumerateArray());
        var rows = stmt.GetProperty("missing_indexes").EnumerateArray().ToList();
        Assert.Equal(2, rows.Count);
        Assert.Equal(2, doc.RootElement.GetProperty("total_missing_indexes").GetInt32());

        Assert.All(rows, row =>
        {
            Assert.True(row.TryGetProperty("create_statement", out var cs));
            Assert.StartsWith("CREATE NONCLUSTERED INDEX [", cs.GetString(), StringComparison.Ordinal);
            Assert.Equal(McpPlanAnalysisFormatter.MissingIndexImpactBasis, row.GetProperty("impact_basis").GetString());
            Assert.Equal(McpPlanAnalysisFormatter.MissingIndexCaveat, row.GetProperty("caveat").GetString());
        });

        Assert.Equal("dbo.Posts", rows[0].GetProperty("table").GetString());
        Assert.Contains("ON [dbo].[Posts] ([OwnerUserId], [CreationDate])", rows[0].GetProperty("create_statement").GetString(), StringComparison.Ordinal);
        Assert.Equal("dbo.Users", rows[1].GetProperty("table").GetString());
        Assert.Equal(12.25, rows[1].GetProperty("impact").GetDouble());
        Assert.Equal(
            "CREATE NONCLUSTERED INDEX [Users_Reputation]\nON [dbo].[Users] ([Reputation]);",
            rows[1].GetProperty("create_statement").GetString());
    }

    /* ---------------- fixture ---------------- */

    /// <summary>An estimated plan (no RunTimeInformation) of <paramref name="relOps"/> operators nested one under the
    /// next, each with a distinct subtree cost so cost_percent ranks them unambiguously, and optionally one
    /// missing-index group on dbo.Posts with all three column usages — plus, when <paramref name="secondGroupOnUsers"/>,
    /// a second group on dbo.Users with a single equality column and no INCLUDE, so the per-row pin sees two
    /// statements that differ in table, key list, name and shape (#3805).</summary>
    private static string EstimatedPlan(int relOps, bool withMissingIndex, bool secondGroupOnUsers = false) =>
        ChainPlan(relOps, withMissingIndex, secondGroupOnUsers, elapsedMsFor: null);

    /// <summary>#7887: the same chain with row-mode runtime statistics on every operator. The leaf scan's elapsed is
    /// <paramref name="leafMs"/>, and each ancestor's elapsed is its child's plus <paramref name="ownMsPerAncestor"/>,
    /// the way showplan reports row mode: an operator's ActualElapsedms includes everything below it.</summary>
    private static string ActualRowModePlan(int relOps, long leafMs, long ownMsPerAncestor) =>
        ChainPlan(relOps, withMissingIndex: false, secondGroupOnUsers: false,
            elapsedMsFor: i => leafMs + (relOps - 1 - i) * ownMsPerAncestor);

    private static string ChainPlan(int relOps, bool withMissingIndex, bool secondGroupOnUsers, Func<int, long>? elapsedMsFor)
    {
        var sb = new StringBuilder();
        sb.Append("<?xml version=\"1.0\" encoding=\"utf-16\"?>");
        sb.Append("<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">");
        sb.Append("<BatchSequence><Batch><Statements>");
        sb.Append("<StmtSimple StatementText=\"SELECT 1\" StatementId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"100\" StatementEstRows=\"10\">");
        sb.Append("<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\">");
        if (withMissingIndex)
        {
            sb.Append("<MissingIndexes><MissingIndexGroup Impact=\"87.5\">");
            sb.Append("<MissingIndex Database=\"[StackOverflow]\" Schema=\"[dbo]\" Table=\"[Posts]\">");
            sb.Append("<ColumnGroup Usage=\"EQUALITY\"><Column Name=\"[OwnerUserId]\" ColumnId=\"3\" /></ColumnGroup>");
            sb.Append("<ColumnGroup Usage=\"INEQUALITY\"><Column Name=\"[CreationDate]\" ColumnId=\"5\" /></ColumnGroup>");
            sb.Append("<ColumnGroup Usage=\"INCLUDE\"><Column Name=\"[Score]\" ColumnId=\"9\" /></ColumnGroup>");
            sb.Append("</MissingIndex></MissingIndexGroup>");
            if (secondGroupOnUsers)
            {
                sb.Append("<MissingIndexGroup Impact=\"12.25\">");
                sb.Append("<MissingIndex Database=\"[StackOverflow]\" Schema=\"[dbo]\" Table=\"[Users]\">");
                sb.Append("<ColumnGroup Usage=\"EQUALITY\"><Column Name=\"[Reputation]\" ColumnId=\"4\" /></ColumnGroup>");
                sb.Append("</MissingIndex></MissingIndexGroup>");
            }

            sb.Append("</MissingIndexes>");
        }

        /* Subtree costs descend from the root so every operator's own cost (subtree minus children) is distinct. */
        for (var i = 0; i < relOps; i++)
        {
            var subtree = (relOps - i) * 10.0;
            var op = i == relOps - 1 ? "Clustered Index Scan" : "Nested Loops";
            sb.Append($"<RelOp NodeId=\"{i}\" PhysicalOp=\"{op}\" LogicalOp=\"{(i == relOps - 1 ? "Clustered Index Scan" : "Inner Join")}\" EstimateRows=\"{10 + i}\" EstimateIO=\"0.1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"{subtree}\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">");
            sb.Append("<OutputList />");
            if (elapsedMsFor != null)
            {
                var ms = elapsedMsFor(i);
                sb.Append($"<RunTimeInformation><RunTimeCountersPerThread Thread=\"0\" ActualRows=\"{10 + i}\" ActualExecutions=\"1\" ActualExecutionMode=\"Row\" ActualElapsedms=\"{ms}\" ActualCPUms=\"{ms}\" /></RunTimeInformation>");
            }

            sb.Append(i == relOps - 1 ? "<IndexScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" : "<NestedLoops Optimized=\"0\">");
            if (i == relOps - 1)
            {
                sb.Append("<DefinedValues /><Object Database=\"[StackOverflow]\" Schema=\"[dbo]\" Table=\"[Posts]\" Index=\"[PK_Posts]\" IndexKind=\"Clustered\" Storage=\"RowStore\" />");
            }
        }

        for (var i = relOps - 1; i >= 0; i--)
        {
            sb.Append(i == relOps - 1 ? "</IndexScan>" : "</NestedLoops>");
            sb.Append("</RelOp>");
        }

        sb.Append("</QueryPlan></StmtSimple>");
        sb.Append("</Statements></Batch></BatchSequence></ShowPlanXML>");
        return sb.ToString();
    }
}
