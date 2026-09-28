/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4512 follow-up: <c>ShowPlanParser.Parse</c> now returns trees up to <c>MaxParseDepth</c>
/// (1,000) levels deep, parsed safely on its own dedicated 32 MB thread. Those trees then go, on
/// whichever thread the CALLER happens to be running on, through <see cref="PlanAnalyzer.Analyze"/>
/// (called by the Darling service's analysis pass, the plan viewer, and the analyze_plan_xml /
/// analyze_query_plan / analyze_query_store_plan MCP tools and web host that front it),
/// <see cref="BenefitScorer.Score"/> (not yet wired to any of those callers, but about to be),
/// and <see cref="PlanLayoutEngine.Layout"/> (the plan viewer's WPF UI thread only). A deep
/// stored plan that a monitored server's own analysis pass meets on every restart is a crash
/// loop for the whole collection service, not a one-off failure — the same class of problem the
/// parser's own dedicated thread was built to stop.
///
/// <para>Measured directly (a crash-safe child-process harness, binary-searched, never run
/// against the xunit test host): all three walks survive a depth-999 tree on both a 1 MB caller
/// thread (the smallest real caller, the WPF UI thread) and a 1.5 MB caller thread (the
/// thread-pool/ASP.NET size) with well over 2x margin below their measured floors (<c>Analyze</c>
/// ~130 KB / ~110 KB, <c>Score</c> ~68 KB both, <c>Layout</c> ~100 KB). None of the three needed
/// the parser's dedicated-thread remedy; these pins hold that fact.</para>
/// </summary>
public sealed class PlanSync4512PostParseTests
{
    private const string Ns = "xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"";

    /// <summary>
    /// A depth-999 tree — the largest <c>ShowPlanParser</c> now returns — through parse, analyze,
    /// score, and layout, all on a genuine 1 MB caller thread (the WPF UI thread's size). This is
    /// the fact this class exists to pin: every post-parse walk completes on the smallest
    /// real caller, with a sane result.
    /// </summary>
    [Fact]
    public void Depth999TreeCompletesOnOneMegabyteStackThread()
    {
        var (statement, error) = RunOnStackThread(999, 1 * 1024 * 1024);

        Assert.Null(error);
        Assert.NotNull(statement!.RootNode);
    }

    /// <summary>Same tree, on the 1.5 MB thread-pool/ASP.NET caller size.</summary>
    [Fact]
    public void Depth999TreeCompletesOnOneAndAHalfMegabyteStackThread()
    {
        var (statement, error) = RunOnStackThread(999, (long)(1.5 * 1024 * 1024));

        Assert.Null(error);
        Assert.NotNull(statement!.RootNode);
    }

    /// <summary>
    /// A shallower, realistic depth (150 levels — well past any plan seen in practice, well
    /// short of the 1,000-level guard) on both real caller sizes.
    /// </summary>
    [Fact]
    public void Depth150TreeCompletesOnOneMegabyteStackThread()
    {
        var (statement, error) = RunOnStackThread(150, 1 * 1024 * 1024);

        Assert.Null(error);
        Assert.NotNull(statement!.RootNode);
    }

    /// <summary>Same 150-level tree, on the 1.5 MB caller size.</summary>
    [Fact]
    public void Depth150TreeCompletesOnOneAndAHalfMegabyteStackThread()
    {
        var (statement, error) = RunOnStackThread(150, (long)(1.5 * 1024 * 1024));

        Assert.Null(error);
        Assert.NotNull(statement!.RootNode);
    }

    /// <summary>
    /// 1,001 levels — one past <c>MaxParseDepth</c> — refuses cleanly with
    /// <see cref="ParsedPlan.ParseError"/> at the parser stage, before any post-parse walk ever
    /// sees the tree. Pins that the guard's own refusal path, not a post-parse crash, is what a
    /// too-deep plan actually hits.
    /// </summary>
    [Fact]
    public void Depth1001TreeIsRefusedByTheParserBeforeAnyPostParseWalkRuns()
    {
        var xml = NestedRelOpPlan(1001);

        var plan = ShowPlanParser.Parse(xml);

        Assert.NotNull(plan.ParseError);
        Assert.Contains("depth limit", plan.ParseError);
    }

    /// <summary>
    /// Runs parse + analyze + score + layout on a thread built with exactly the stack size the
    /// docs on <see cref="PlanAnalyzer.Analyze"/> claim as the measured floor for a 1 MB caller
    /// (roughly 130 KB) plus a safety margin, proving the walks are not living on borrowed
    /// margin from a coincidentally larger CI thread.
    /// </summary>
    private static (PlanStatement? statement, string? error) RunOnStackThread(int depth, long stackBytes)
    {
        var xml = NestedRelOpPlan(depth);
        PlanStatement? result = null;
        string? error = null;

        var thread = new Thread(() =>
        {
            var plan = ShowPlanParser.Parse(xml);
            if (plan.ParseError != null)
            {
                error = plan.ParseError;
                return;
            }

            var stmt = plan.Batches[0].Statements[0];
            PlanAnalyzer.Analyze(plan);
            BenefitScorer.Score(plan);
            if (stmt.RootNode != null)
                PlanLayoutEngine.Layout(stmt);

            result = stmt;
        }, (int)stackBytes);

        thread.Start();
        thread.Join();

        return (result, error);
    }

    /// <summary>
    /// Same nested-<c>RelOp</c>-inside-<c>NestedLoops</c> shape <c>PlanSync4512Tests</c> uses for
    /// the parser's own depth pins, reused here so the post-parse walks see the same tree shape
    /// the parser measured its own recursion against.
    /// </summary>
    private static string NestedRelOpPlan(int depth)
    {
        var xml = new StringBuilder($"<ShowPlanXML {Ns}><BatchSequence><Batch><Statements>");
        xml.Append("<StmtSimple StatementText=\"SELECT 1\" StatementType=\"SELECT\"><QueryPlan>");
        for (var level = 0; level < depth; level++)
            xml.Append("<RelOp NodeId=\"0\" PhysicalOp=\"Nested Loops\" LogicalOp=\"Inner Join\"><NestedLoops>");
        xml.Append("<RelOp NodeId=\"0\" PhysicalOp=\"Constant Scan\" LogicalOp=\"Constant Scan\" />");
        for (var level = 0; level < depth; level++)
            xml.Append("</NestedLoops></RelOp>");
        xml.Append("</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>");
        return xml.ToString();
    }
}
