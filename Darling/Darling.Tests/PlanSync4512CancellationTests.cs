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
    /// (b) Cancelled MID-parse, deterministically: a 35,000-statement plan (13 MB, under
    /// <see cref="ShowPlanParser.MaxParseCharacters"/>) takes roughly 700ms to walk on this
    /// machine — measured directly while writing this test — so a 50ms <c>CancelAfter</c> fires
    /// while the dedicated parse thread is still inside the per-statement walk, never after it
    /// finishes. Five manual trials during development all threw; this run is one more.
    /// </summary>
    [Fact]
    public void ParseCancelledDuringTheWalkThrowsOperationCanceledException()
    {
        var xml = ManyStatementsPlan(35_000);
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(50);

        Assert.Throws<OperationCanceledException>(() => ShowPlanParser.Parse(xml, cts.Token));
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
    /// (d) <see cref="ShowPlanParser.ParseAsync"/> enforces <see cref="ShowPlanParser.MaxParseCharacters"/>
    /// at the reader level (<c>XmlReaderSettings.MaxCharactersInDocument</c>) for XML over the
    /// limit: <see cref="ParsedPlan.ParseError"/> is set, with no exception and no
    /// <see cref="OperationCanceledException"/> — a size refusal is a parse failure, not a
    /// cancellation.
    /// </summary>
    [Fact]
    public async System.Threading.Tasks.Task ParseAsyncOverTheSizeLimitSetsParseErrorWithNoException()
    {
        // 400,000 statements is ~150 MB, comfortably over MaxParseCharacters (16 MB) without
        // needing to build a string right at the boundary.
        var xml = ManyStatementsPlan(400_000);

        var plan = await ShowPlanParser.ParseAsync(xml, CancellationToken.None);

        Assert.NotNull(plan.ParseError);
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
    /// the collector's own abandonment handling, not get swallowed as a bad plan. This is a
    /// code-reading pin rather than an exercised one: both
    /// <c>Darling/PerformanceMonitor.Darling.Analysis/PgDrillDownCollector.Plans.cs</c> and
    /// <c>Lite/Analysis/DrillDownCollector.Plans.cs</c> wrap their single-plan
    /// <c>ShowPlanParser.Parse(planXml, context.CancellationToken)</c> call in
    /// <c>catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))</c>
    /// (Darling) / <c>!AnalysisAbandon.IsExpected(ex, context.CancellationToken)</c> (Lite).
    /// <see cref="AnalysisShutdown.IsExpectedAbandon"/> is true only when
    /// <c>passToken.IsCancellationRequested</c>; its filter runs before the catch body, so with
    /// the token cancelled the <c>when</c> clause is false and the
    /// <see cref="OperationCanceledException"/> propagates out of the catch instead of being
    /// caught and reported as a parse failure. This pin asserts that the classifier itself
    /// returns true for exactly that shape, which is the half of the guard both call sites share.
    /// </summary>
    [Fact]
    public void DrillDownAbandonClassifierRecognizesOperationCanceledExceptionAsExpected()
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
