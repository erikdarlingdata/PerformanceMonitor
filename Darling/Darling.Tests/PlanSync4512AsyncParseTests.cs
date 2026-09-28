/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="ShowPlanParser.ParseAsync"/> used to walk the parsed tree on its own
/// <c>LoadAsync</c> continuation — the caller's thread, not the dedicated 32 MB thread
/// <see cref="ShowPlanParser.Parse"/> runs on — so a plan nested deeper than a pool thread's own
/// stack could overflow it before <c>MaxParseDepth</c> ever fired. These tests pin that
/// <c>ParseAsync</c> is now a thin wrapper over the same dedicated-thread parse: it starts the
/// thread and returns its <c>Task</c> without ever walking the tree itself.
///
/// <para>The crash this closes cannot be pinned as a normal RED/GREEN assertion — on the
/// unfixed code it takes the whole test host down with it. It is demonstrated separately with a
/// throwaway console run as a child process against a worktree of the pre-fix commit; see the
/// PR body for the exit codes.</para>
/// </summary>
public sealed class PlanSync4512AsyncParseTests
{
    private const string Ns = "xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"";

    /// <summary>
    /// (i) A 500-deep plan is past a 1 MB thread's own overflow point (roughly depth 80) but
    /// well under <c>MaxParseDepth</c> (1,000), so a correct dedicated-thread parse returns all
    /// 502 operators (measured by running this test) with no <see cref="ParsedPlan.ParseError"/>
    /// even when awaited from a real 1 MB thread — never on the awaiting thread's own stack.
    /// </summary>
    [Fact]
    public void ParseAsyncOnA500DeepPlanAwaitedFromAOneMegabyteThreadSucceeds()
    {
        var xml = NestedRelOpPlan(500);

        var plan = ParseAsyncOnOneMegabyteStackThread(xml);

        Assert.NotNull(plan);
        Assert.Null(plan!.ParseError);
        var operatorCount = CountRelOps(plan);
        Assert.Equal(502, operatorCount);
    }

    /// <summary>
    /// (ii) The async path refuses an oversized document with the same message the synchronous
    /// path uses, since both now share the same up-front <c>xml.Length &gt; MaxParseCharacters</c>
    /// check rather than the reader-level limit the async path used before.
    /// </summary>
    [Fact]
    public async Task ParseAsyncOverTheSizeLimitGivesTheSyncPathsRefusalMessage()
    {
        var xml = new string('a', ShowPlanParser.MaxParseCharacters + 1);

        var plan = await ShowPlanParser.ParseAsync(xml, CancellationToken.None);

        Assert.NotNull(plan.ParseError);
        Assert.Equal(
            $"Plan XML exceeds the supported size limit of {ShowPlanParser.MaxParseCharacters:N0} characters.",
            plan.ParseError);
    }

    private static ParsedPlan? ParseAsyncOnOneMegabyteStackThread(string xml)
    {
        ParsedPlan? plan = null;
        var thread = new Thread(
            () => plan = ShowPlanParser.ParseAsync(xml, CancellationToken.None).GetAwaiter().GetResult(),
            1 * 1024 * 1024);
        thread.Start();
        thread.Join();
        return plan;
    }

    private static int CountRelOps(ParsedPlan plan)
    {
        var count = 0;
        foreach (var batch in plan.Batches)
        foreach (var statement in batch.Statements)
            count += CountNode(statement.RootNode);
        return count;
    }

    private static int CountNode(PlanNode? node)
    {
        if (node is null)
            return 0;
        var count = 1;
        foreach (var child in node.Children)
            count += CountNode(child);
        return count;
    }

    /// <summary>
    /// A RelOp's children live inside its own operator element (<c>NestedLoops</c> here), not
    /// directly under the RelOp itself — matches <c>PlanSync4512Tests.NestedRelOpPlan</c>'s shape.
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
