/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4535 step 3a — <c>IsRuleDisabled</c> guards and <c>RuleNumber</c> stamps on the node-level
/// rules 1, 2, 4, 5, 6, 7, 8, 10, 11 and 12, ported from erikdarlingdata/PerformanceStudio dev
/// (85492a1) <c>src/PlanViewer.Core/Services/PlanAnalyzer.Node.cs</c> at each rule's own
/// construction site. Rule 7 changes the severity of SQL Server's own spill warnings rather than
/// adding a new <see cref="PlanWarning"/>, so PS stamps no RuleNumber for it — this file pins the
/// guard only. Every fixture is built as an in-memory <see cref="PlanNode"/> tree (the same shape
/// <c>PlanSync4536Tests</c> uses), analyzed with <see cref="PlanAnalyzer.Analyze"/> so the config
/// threads through the real product call path, not a helper called directly.
/// </summary>
public sealed class PlanSync4535NodeRulesATests
{
    private static ParsedPlan RunWithConfig(PlanNode root, AnalyzerConfig? cfg)
    {
        var stmt = new PlanStatement { RootNode = root };
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalyzer.Analyze(plan, cfg, null, System.Threading.CancellationToken.None);
        return plan;
    }

    private static AnalyzerConfig Disable(int rule) =>
        new() { Rules = new RulesConfig { Disabled = [rule] } };

    // ---- Rule 1: Filter operator ------------------------------------------------------------

    private static PlanNode FilterNode() => new()
    {
        NodeId = 0,
        PhysicalOp = "Filter",
        LogicalOp = "Filter",
        Predicate = "[dbo].[t].[b]=(1)",
        HasActualStats = true,
        Children =
        [
            new PlanNode
            {
                NodeId = 1,
                PhysicalOp = "Table Scan",
                LogicalOp = "Table Scan",
                HasActualStats = true,
                ActualLogicalReads = 5000,
                ActualElapsedMs = 50
            }
        ]
    };

    [Fact]
    public void Rule1_Stamped_WhenEnabled()
    {
        var plan = RunWithConfig(FilterNode(), null);
        var warning = Assert.Single(AllWarnings(plan), w => w.WarningType == "Filter Operator");
        Assert.Equal(1, warning.RuleNumber);
    }

    [Fact]
    public void Rule1_Gone_WhenDisabled()
    {
        var plan = RunWithConfig(FilterNode(), Disable(1));
        Assert.DoesNotContain(AllWarnings(plan), w => w.WarningType == "Filter Operator");
    }

    // ---- Rule 2: Eager Index Spool -----------------------------------------------------------

    private static PlanNode EagerIndexSpoolNode() => new()
    {
        NodeId = 0,
        PhysicalOp = "Index Spool",
        LogicalOp = "Eager Spool"
    };

    [Fact]
    public void Rule2_Stamped_WhenEnabled()
    {
        var plan = RunWithConfig(EagerIndexSpoolNode(), null);
        var warning = Assert.Single(AllWarnings(plan), w => w.WarningType == "Eager Index Spool");
        Assert.Equal(2, warning.RuleNumber);
    }

    [Fact]
    public void Rule2_Gone_WhenDisabled()
    {
        var plan = RunWithConfig(EagerIndexSpoolNode(), Disable(2));
        Assert.DoesNotContain(AllWarnings(plan), w => w.WarningType == "Eager Index Spool");
    }

    // ---- Rule 4: UDF timing ------------------------------------------------------------------

    private static PlanNode UdfTimingNode() => new()
    {
        NodeId = 0,
        PhysicalOp = "Compute Scalar",
        LogicalOp = "Compute Scalar",
        UdfCpuTimeMs = 50,
        UdfElapsedTimeMs = 100
    };

    [Fact]
    public void Rule4_Stamped_WhenEnabled()
    {
        var plan = RunWithConfig(UdfTimingNode(), null);
        var warning = Assert.Single(AllWarnings(plan), w => w.WarningType == "UDF Execution");
        Assert.Equal(4, warning.RuleNumber);
    }

    [Fact]
    public void Rule4_Gone_WhenDisabled()
    {
        var plan = RunWithConfig(UdfTimingNode(), Disable(4));
        Assert.DoesNotContain(AllWarnings(plan), w => w.WarningType == "UDF Execution");
    }

    // ---- Rule 5: Row estimate mismatch (both construction sites: zero rows, and ratio) -------

    private static PlanNode RowEstimateMismatchZeroRowsNode() => new()
    {
        NodeId = 0,
        PhysicalOp = "Hash Match",
        LogicalOp = "Inner Join",
        HasActualStats = true,
        EstimateRows = 1000,
        ActualExecutions = 1,
        ActualRows = 0
    };

    private static PlanNode RowEstimateMismatchRatioNode()
    {
        // AssessEstimateHarm needs a real parent to attribute harm to (the root synthetic
        // node has NodeId -1 and returns null harm), so wrap the mismatched node as the
        // inner side of a parent Nested Loops join.
        // PhysicalOp deliberately avoids "Sort"/"Hash" — AssessEstimateHarm returns null harm
        // for a Sort/Hash node that did NOT spill, so use a join op that isn't one of those.
        var mismatched = new PlanNode
        {
            NodeId = 1,
            PhysicalOp = "Nested Loops",
            LogicalOp = "Inner Join",
            HasActualStats = true,
            EstimateRows = 10,
            ActualExecutions = 1,
            ActualRows = 1000
        };
        var outer = new PlanNode { NodeId = 2, PhysicalOp = "Table Scan", LogicalOp = "Table Scan" };
        var parent = new PlanNode
        {
            NodeId = 0,
            PhysicalOp = "Nested Loops",
            LogicalOp = "Inner Join",
            Children = [outer, mismatched]
        };
        mismatched.Parent = parent;
        outer.Parent = parent;
        return parent;
    }

    [Fact]
    public void Rule5_Stamped_WhenEnabled_ZeroRowsSite()
    {
        var plan = RunWithConfig(RowEstimateMismatchZeroRowsNode(), null);
        var warning = Assert.Single(AllWarnings(plan), w => w.WarningType == "Row Estimate Mismatch");
        Assert.Equal(5, warning.RuleNumber);
    }

    [Fact]
    public void Rule5_Stamped_WhenEnabled_RatioSite()
    {
        var plan = RunWithConfig(RowEstimateMismatchRatioNode(), null);
        var warning = Assert.Single(AllWarnings(plan), w => w.WarningType == "Row Estimate Mismatch");
        Assert.Equal(5, warning.RuleNumber);
    }

    [Fact]
    public void Rule5_Gone_WhenDisabled_BothSites()
    {
        var zeroPlan = RunWithConfig(RowEstimateMismatchZeroRowsNode(), Disable(5));
        var ratioPlan = RunWithConfig(RowEstimateMismatchRatioNode(), Disable(5));
        Assert.DoesNotContain(AllWarnings(zeroPlan), w => w.WarningType == "Row Estimate Mismatch");
        Assert.DoesNotContain(AllWarnings(ratioPlan), w => w.WarningType == "Row Estimate Mismatch");
    }

    // ---- Rule 6: Scalar UDF reference ---------------------------------------------------------

    private static PlanNode ScalarUdfNode() => new()
    {
        NodeId = 0,
        PhysicalOp = "Compute Scalar",
        LogicalOp = "Compute Scalar",
        ScalarUdfs = [new ScalarUdfReference { FunctionName = "dbo.f", IsClrFunction = false }]
    };

    [Fact]
    public void Rule6_Stamped_WhenEnabled()
    {
        var plan = RunWithConfig(ScalarUdfNode(), null);
        var warning = Assert.Single(AllWarnings(plan), w => w.WarningType == "Scalar UDF");
        Assert.Equal(6, warning.RuleNumber);
    }

    [Fact]
    public void Rule6_Gone_WhenDisabled()
    {
        var plan = RunWithConfig(ScalarUdfNode(), Disable(6));
        Assert.DoesNotContain(AllWarnings(plan), w => w.WarningType == "Scalar UDF");
    }

    // ---- Rule 7: Spill severity (guard only, no RuleNumber) -----------------------------------

    private static (PlanNode node, PlanStatement stmt) SpillNodeWithStatement()
    {
        var node = new PlanNode
        {
            NodeId = 0,
            PhysicalOp = "Sort",
            LogicalOp = "Sort",
            HasActualStats = true,
            ActualElapsedMs = 600
        };
        node.Warnings.Add(new PlanWarning
        {
            WarningType = "Sort Spill",
            Message = "Sort spill",
            Severity = PlanWarningSeverity.Warning,
            SpillDetails = new SpillDetail { SpillType = "Sort", WritesToTempDb = 100 }
        });
        var stmt = new PlanStatement { RootNode = node, QueryTimeStats = new QueryTimeInfo { ElapsedTimeMs = 1000 } };
        return (node, stmt);
    }

    private static ParsedPlan RunSpillWithConfig(AnalyzerConfig? cfg, out PlanNode node)
    {
        var (n, stmt) = SpillNodeWithStatement();
        node = n;
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalyzer.Analyze(plan, cfg, null, System.Threading.CancellationToken.None);
        return plan;
    }

    [Fact]
    public void Rule7_ElevatesSpillSeverity_WhenEnabled()
    {
        RunSpillWithConfig(null, out var node);
        var warning = Assert.Single(node.Warnings, w => w.WarningType == "Sort Spill");
        // 600ms of 1000ms statement elapsed = 60% -> Critical (>= 0.5 threshold)
        Assert.Equal(PlanWarningSeverity.Critical, warning.Severity);
        Assert.Contains("Operator time:", warning.Message);
    }

    [Fact]
    public void Rule7_KeepsOriginalSeverity_WhenDisabled()
    {
        RunSpillWithConfig(Disable(7), out var node);
        var warning = Assert.Single(node.Warnings, w => w.WarningType == "Sort Spill");
        // Guard skips the whole rule, so the parser's original severity (Warning) and
        // message (no "Operator time:" suffix) are untouched.
        Assert.Equal(PlanWarningSeverity.Warning, warning.Severity);
        Assert.DoesNotContain("Operator time:", warning.Message);
    }

    // ---- Rule 8: Parallel thread skew ---------------------------------------------------------

    private static PlanNode ParallelSkewNode() => new()
    {
        NodeId = 0,
        PhysicalOp = "Hash Match",
        LogicalOp = "Inner Join",
        PerThreadStats =
        [
            new PerThreadRuntimeInfo { ThreadId = 1, ActualRows = 9999 },
            new PerThreadRuntimeInfo { ThreadId = 2, ActualRows = 1 }
        ]
    };

    [Fact]
    public void Rule8_Stamped_WhenEnabled()
    {
        var plan = RunWithConfig(ParallelSkewNode(), null);
        var warning = Assert.Single(AllWarnings(plan), w => w.WarningType == "Parallel Skew");
        Assert.Equal(8, warning.RuleNumber);
    }

    [Fact]
    public void Rule8_Gone_WhenDisabled()
    {
        var plan = RunWithConfig(ParallelSkewNode(), Disable(8));
        Assert.DoesNotContain(AllWarnings(plan), w => w.WarningType == "Parallel Skew");
    }

    // ---- Rule 10: RID Lookup and Key Lookup ----------------------------------------------------

    private static PlanNode RidLookupNode() => new()
    {
        NodeId = 0,
        PhysicalOp = "RID Lookup (Heap)",
        LogicalOp = "RID Lookup"
    };

    private static PlanNode KeyLookupNode() => new()
    {
        NodeId = 0,
        PhysicalOp = "Key Lookup (Clustered)",
        LogicalOp = "Key Lookup",
        Lookup = true
    };

    [Fact]
    public void Rule10_Stamped_WhenEnabled_RidLookup()
    {
        var plan = RunWithConfig(RidLookupNode(), null);
        var warning = Assert.Single(AllWarnings(plan), w => w.WarningType == "RID Lookup");
        Assert.Equal(10, warning.RuleNumber);
    }

    [Fact]
    public void Rule10_Stamped_WhenEnabled_KeyLookup()
    {
        var plan = RunWithConfig(KeyLookupNode(), null);
        var warning = Assert.Single(AllWarnings(plan), w => w.WarningType == "Key Lookup");
        Assert.Equal(10, warning.RuleNumber);
    }

    [Fact]
    public void Rule10_Gone_WhenDisabled_BothSites()
    {
        var ridPlan = RunWithConfig(RidLookupNode(), Disable(10));
        var keyPlan = RunWithConfig(KeyLookupNode(), Disable(10));
        Assert.DoesNotContain(AllWarnings(ridPlan), w => w.WarningType == "RID Lookup");
        Assert.DoesNotContain(AllWarnings(keyPlan), w => w.WarningType == "Key Lookup");
    }

    // ---- Rule 11 / Rule 12 interaction ---------------------------------------------------------
    // A non-SARGable predicate on a scan: rule 12 fires (and, per PS dev, suppresses rule 11).
    // Disabling rule 12 makes GetNonSargableReason return null, so rule 11 can fire on the same
    // node instead — exactly PS dev's PlanAnalyzer.Node.cs ~379-421 interaction.

    private static PlanNode NonSargableScanNode() => new()
    {
        NodeId = 0,
        PhysicalOp = "Table Scan",
        LogicalOp = "Table Scan",
        ObjectName = "dbo.t",
        HasActualStats = true,
        ActualExecutions = 1,
        Predicate = "CONVERT_IMPLICIT(int,[dbo].[t].[col],0)=(1)"
    };

    [Fact]
    public void Rule12_Stamped_WhenEnabled()
    {
        var plan = RunWithConfig(NonSargableScanNode(), null);
        var warning = Assert.Single(AllWarnings(plan), w => w.WarningType == "Non-SARGable Predicate");
        Assert.Equal(12, warning.RuleNumber);
        // Rule 11 is suppressed while rule 12 already flagged the same node.
        Assert.DoesNotContain(AllWarnings(plan), w => w.WarningType == "Scan With Predicate");
    }

    [Fact]
    public void Rule12Disabled_LetsRule11FireOnTheSameNonSargableScan()
    {
        var plan = RunWithConfig(NonSargableScanNode(), Disable(12));

        Assert.DoesNotContain(AllWarnings(plan), w => w.WarningType == "Non-SARGable Predicate");
        var warning = Assert.Single(AllWarnings(plan), w => w.WarningType == "Scan With Predicate");
        Assert.Equal(11, warning.RuleNumber);
    }

    [Fact]
    public void Rule11_Gone_WhenDisabled_OnAnOrdinaryResidualScan()
    {
        var node = new PlanNode
        {
            NodeId = 0,
            PhysicalOp = "Table Scan",
            LogicalOp = "Table Scan",
            ObjectName = "dbo.t",
            HasActualStats = true,
            ActualExecutions = 1,
            Predicate = "[dbo].[t].[col]=(1)"
        };

        var enabledPlan = RunWithConfig(node, null);
        var warning = Assert.Single(AllWarnings(enabledPlan), w => w.WarningType == "Scan With Predicate");
        Assert.Equal(11, warning.RuleNumber);

        var disabledNode = new PlanNode
        {
            NodeId = 0,
            PhysicalOp = "Table Scan",
            LogicalOp = "Table Scan",
            ObjectName = "dbo.t",
            HasActualStats = true,
            ActualExecutions = 1,
            Predicate = "[dbo].[t].[col]=(1)"
        };
        var disabledPlan = RunWithConfig(disabledNode, Disable(11));
        Assert.DoesNotContain(AllWarnings(disabledPlan), w => w.WarningType == "Scan With Predicate");
    }

    private static List<PlanWarning> AllWarnings(ParsedPlan plan) =>
        plan.Batches.SelectMany(b => b.Statements).SelectMany(s => s.PlanWarnings)
            .Concat(CollectNodeWarnings(plan))
            .ToList();

    private static IEnumerable<PlanWarning> CollectNodeWarnings(ParsedPlan plan)
    {
        foreach (var stmt in plan.Batches.SelectMany(b => b.Statements))
        {
            if (stmt.RootNode == null)
                continue;

            foreach (var w in Walk(stmt.RootNode))
                yield return w;
        }
    }

    private static IEnumerable<PlanWarning> Walk(PlanNode node)
    {
        foreach (var w in node.Warnings)
            yield return w;

        foreach (var child in node.Children)
            foreach (var w in Walk(child))
                yield return w;
    }
}
