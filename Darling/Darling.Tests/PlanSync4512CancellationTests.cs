/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4512: <c>ShowPlanParser</c>, <c>PlanAnalyzer</c>, <c>BenefitScorer</c> and
/// <c>PlanAnalysisPipeline</c> now observe a caller's <see cref="CancellationToken"/> instead of
/// running a parse or an analysis pass to completion against a plan nobody is waiting for. A
/// cancellation must surface as <see cref="OperationCanceledException"/> — never as
/// <see cref="ParsedPlan.ParseError"/>, which callers treat as "this plan is bad", not "the
/// caller gave up".
/// </summary>
/// <para>Mutation checked while writing these tests: removing <c>Parse</c>'s dedicated-thread
/// catch that captures an <see cref="OperationCanceledException"/> and rethrows it on the
/// caller's thread turned that path into an unhandled exception on a non-pool <see cref="Thread"/>
/// — fatal to the whole process, per the class's own comment on
/// <c>ParseOnDedicatedThread</c> — rather than a failing assertion; the pin-runner process itself
/// terminated instead of reporting a [FAIL] line. Reverting restored the 8/8 green above.</para>
public sealed class PlanSync4512CancellationTests
{
    private const string Ns = "xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"";

    /// <summary>(a) A token cancelled BEFORE the call throws immediately, not a ParseError.</summary>
    [Fact]
    public void ParsePreCancelledTokenThrowsOperationCanceledExceptionNotParseError()
    {
        var xml = OneStatementPlan();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.Throws<OperationCanceledException>(() => ShowPlanParser.Parse(xml, cts.Token));
    }

    /// <summary>(a), async side: same pre-cancelled behaviour on <see cref="ShowPlanParser.ParseAsync"/>.</summary>
    [Fact]
    public async System.Threading.Tasks.Task ParseAsyncPreCancelledTokenThrowsOperationCanceledException()
    {
        var xml = OneStatementPlan();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ShowPlanParser.ParseAsync(xml, cts.Token));
    }

    /// <summary>
    /// (b) Cancelled MID-parse, deterministically: <see cref="ShowPlanParser.OnStatementParsedForTest"/>
    /// fires after each statement the walk finishes, on the dedicated parse thread, so the hook
    /// cancels the token itself as soon as it sees statement 3 of 50 — no timer, no race against
    /// how long <c>XDocument.Parse</c> takes on the machine running this. The walk must stop at
    /// statement 3: the hook is asserted to never see statement 5 or later, which proves the
    /// cancellation actually cut the walk short instead of racing to the end anyway.
    /// </summary>
    [Fact]
    public void ParseCancelledDuringTheWalkThrowsOperationCanceledException()
    {
        var xml = ManyStatementsPlan(50);
        using var cts = new CancellationTokenSource();
        var highestStatementSeen = 0;

        ShowPlanParser.OnStatementParsedForTest = count =>
        {
            highestStatementSeen = count;
            if (count == 3)
                cts.Cancel();
        };
        try
        {
            Assert.Throws<OperationCanceledException>(() => ShowPlanParser.Parse(xml, cts.Token));
        }
        finally
        {
            ShowPlanParser.OnStatementParsedForTest = null;
        }

        Assert.True(highestStatementSeen <= 4,
            $"the walk should have stopped at or just past statement 3, but saw statement {highestStatementSeen}");
    }

    /// <summary>(b), async side: same mid-walk cancellation on <see cref="ShowPlanParser.ParseAsync"/>.</summary>
    [Fact]
    public async System.Threading.Tasks.Task ParseAsyncCancelledDuringTheWalkThrowsOperationCanceledException()
    {
        var xml = ManyStatementsPlan(35_000);
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(50);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ShowPlanParser.ParseAsync(xml, cts.Token));
    }

    /// <summary>
    /// (c) <see cref="PlanAnalysisPipeline.Run"/> with a pre-cancelled token throws before either
    /// the analyzer or the scorer adds anything — <see cref="ParsedPlan.AllWarnings"/> stays
    /// empty, not partially filled.
    /// </summary>
    [Fact]
    public void PipelineRunCancelledTokenThrowsBeforeAnyFindingIsAdded()
    {
        var plan = ShowPlanParser.Parse(OneStatementPlan());
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.Throws<OperationCanceledException>(() => PlanAnalysisPipeline.Run(plan, cts.Token));
        Assert.Empty(plan.AllWarnings);
    }

    /// <summary>
    /// (d) <see cref="ShowPlanParser.ParseAsync"/> now enforces <see cref="ShowPlanParser.MaxParseCharacters"/>
    /// with the same up-front <c>xml.Length</c> check the synchronous path uses, so a document
    /// just one character over the limit is refused before any parse work runs —
    /// <see cref="ParsedPlan.ParseError"/> is set, with no exception and no
    /// <see cref="OperationCanceledException"/>. A valid small plan padded with one XML comment
    /// to exactly <c>MaxParseCharacters + 1</c> chars proves the boundary without building the
    /// ~150 MB string the old reader-level check needed to exercise.
    /// </summary>
    [Fact]
    public async System.Threading.Tasks.Task ParseAsyncOverTheSizeLimitSetsParseErrorWithNoException()
    {
        var body = OneStatementPlan();
        var padding = ShowPlanParser.MaxParseCharacters + 1 - body.Length - "<!---->".Length;
        var xml = $"<!--{new string('x', padding)}-->{body}";

        Assert.Equal(ShowPlanParser.MaxParseCharacters + 1, xml.Length);

        var plan = await ShowPlanParser.ParseAsync(xml, CancellationToken.None);

        Assert.NotNull(plan.ParseError);
        Assert.Equal(
            $"Plan XML exceeds the supported size limit of {ShowPlanParser.MaxParseCharacters:N0} characters.",
            plan.ParseError);
    }

    /// <summary>
    /// (e) The no-token overloads give the same result as the token overloads called with
    /// <see cref="CancellationToken.None"/>, on an ordinary plan that finishes normally.
    /// </summary>
    [Fact]
    public void NoTokenOverloadsMatchExplicitNoneTokenOnANormalPlan()
    {
        var xml = OneStatementPlan();

        var planNoToken = ShowPlanParser.Parse(xml);
        var planWithNone = ShowPlanParser.Parse(xml, CancellationToken.None);
        Assert.Equal(planNoToken.ParseError, planWithNone.ParseError);
        Assert.Equal(planNoToken.Batches.Count, planWithNone.Batches.Count);

        PlanAnalysisPipeline.Run(planNoToken);
        PlanAnalysisPipeline.Run(planWithNone, CancellationToken.None);
        Assert.Equal(planNoToken.AllWarnings.Count, planWithNone.AllWarnings.Count);
    }

    /// <summary>
    /// (f) A cancellation from <see cref="ShowPlanParser.Parse"/> inside a drill-down must reach
    /// the collector's own abandonment handling, not get swallowed as a bad plan. Both
    /// <c>Darling/PerformanceMonitor.Darling.Analysis/PgDrillDownCollector.Plans.cs</c> and
    /// <c>Lite/Analysis/DrillDownCollector.Plans.cs</c> wrap their single-plan
    /// <c>ShowPlanParser.Parse(planXml, context.CancellationToken)</c> call in
    /// <c>catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))</c>
    /// (Darling) / <c>!AnalysisAbandon.IsExpected(ex, context.CancellationToken)</c> (Lite).
    /// <see cref="AnalysisShutdown.IsExpectedAbandon"/> is true only when
    /// <c>passToken.IsCancellationRequested</c>; its filter runs before the catch body, so with
    /// the token cancelled the <c>when</c> clause is false and the
    /// <see cref="OperationCanceledException"/> propagates out of the catch instead of being
    /// caught and reported as a parse failure. This pin covers only the classifier itself
    /// (<see cref="AnalysisShutdown.IsExpectedAbandon"/> returning true for exactly that shape),
    /// which is the half of the guard both call sites share — it does not exercise the
    /// drill-down catch's <c>when</c> clause directly.
    /// </summary>
    [Fact]
    public void AbandonClassifier_TreatsCancelledParseAsExpected()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var oce = new OperationCanceledException(cts.Token);

        var isExpected = PerformanceMonitor.Darling.Analysis.AnalysisShutdown.IsExpectedAbandon(oce, cts.Token);

        Assert.True(isExpected);
    }

    private static string OneStatementPlan()
    {
        var xml = new StringBuilder($"<ShowPlanXML {Ns}><BatchSequence><Batch><Statements>");
        xml.Append("<StmtSimple StatementText=\"SELECT 1\" StatementType=\"SELECT\"><QueryPlan>");
        xml.Append("<RelOp NodeId=\"0\" PhysicalOp=\"Constant Scan\" LogicalOp=\"Constant Scan\" />");
        xml.Append("</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>");
        return xml.ToString();
    }

    private static string ManyStatementsPlan(int count)
    {
        var xml = new StringBuilder($"<ShowPlanXML {Ns}><BatchSequence><Batch><Statements>");
        for (var i = 0; i < count; i++)
        {
            xml.Append("<StmtSimple StatementText=\"SELECT 1\" StatementType=\"SELECT\"><QueryPlan>");
            xml.Append("<RelOp NodeId=\"0\" PhysicalOp=\"Nested Loops\" LogicalOp=\"Inner Join\"><NestedLoops>");
            xml.Append("<RelOp NodeId=\"1\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" />");
            xml.Append("<RelOp NodeId=\"2\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" />");
            xml.Append("</NestedLoops></RelOp>");
            xml.Append("</QueryPlan></StmtSimple>");
        }
        xml.Append("</Statements></Batch></BatchSequence></ShowPlanXML>");
        return xml.ToString();
    }
}
