/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4531 — an adaptive join sizes its memory grant for its hash join branch. When it runs as
/// Nested Loops at runtime, most of that grant goes unused, and that's expected — the reader
/// should not chase a memory grant fix that doesn't apply. The Excessive Memory Grant finding
/// now notes it, ported from erikdarlingdata/PerformanceStudio@4a5efa5's
/// <c>HasAdaptiveJoinChoseNestedLoop</c>.
/// </summary>
public sealed class PlanSync4531Tests
{
    private static ParsedPlan Analyze(PlanStatement stmt)
    {
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalyzer.Analyze(plan);
        return plan;
    }

    private static PlanStatement StatementWithExcessiveGrant(PlanNode root) => new()
    {
        RootNode = root,
        MemoryGrant = new MemoryGrantInfo
        {
            GrantedMemoryKB = 2_097_152,   // 2 GB
            MaxUsedMemoryKB = 1024         // 1 MB used — well past the 10x/1GB thresholds
        }
    };

    [Fact]
    public void AdaptiveJoinRanNestedLoops_NotesTheUnusedGrant()
    {
        var root = new PlanNode
        {
            PhysicalOp = "Adaptive Join",
            LogicalOp = "Adaptive Join",
            IsAdaptive = true,
            ActualJoinType = "Nested Loops"
        };
        var plan = Analyze(StatementWithExcessiveGrant(root));
        var stmt = plan.Batches[0].Statements[0];

        var warning = Assert.Single(stmt.PlanWarnings, w => w.WarningType == "Excessive Memory Grant");
        Assert.Contains("adaptive join", warning.Message);
        Assert.Contains("Nested Loop", warning.Message);
    }

    [Fact]
    public void AdaptiveJoinRanHashJoin_SaysNothingAboutNestedLoops()
    {
        var root = new PlanNode
        {
            PhysicalOp = "Adaptive Join",
            LogicalOp = "Adaptive Join",
            IsAdaptive = true,
            ActualJoinType = "Hash Match"
        };
        var plan = Analyze(StatementWithExcessiveGrant(root));
        var stmt = plan.Batches[0].Statements[0];

        var warning = Assert.Single(stmt.PlanWarnings, w => w.WarningType == "Excessive Memory Grant");
        Assert.DoesNotContain("adaptive join", warning.Message);
    }

    [Fact]
    public void NonAdaptiveJoin_SaysNothingAboutAdaptiveJoins()
    {
        var root = new PlanNode { PhysicalOp = "Hash Match", LogicalOp = "Inner Join" };
        var plan = Analyze(StatementWithExcessiveGrant(root));
        var stmt = plan.Batches[0].Statements[0];

        var warning = Assert.Single(stmt.PlanWarnings, w => w.WarningType == "Excessive Memory Grant");
        Assert.DoesNotContain("adaptive join", warning.Message);
    }
}
