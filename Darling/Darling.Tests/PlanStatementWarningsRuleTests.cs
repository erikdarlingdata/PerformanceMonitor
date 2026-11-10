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
/// #8447: <see cref="PlanWarningDisplay.StatementWarnings"/> is the one rule for what a statement's warnings are:
/// the statement-level warnings, then every operator's warnings in tree order, at every severity. The plan viewer's
/// root badge, root tooltip and Statements grid all count this set, so the facts about it live here, outside WPF.
/// </summary>
public class PlanStatementWarningsRuleTests
{
    [Fact]
    public void StatementLevelWarnings_Count_WhenNoOperatorHasOne()
    {
        var statement = new PlanStatement
        {
            RootNode = Node(0),
            PlanWarnings = new List<PlanWarning> { Warn("Memory Grant Warning", PlanWarningSeverity.Critical), Warn("High Compile Time", PlanWarningSeverity.Warning) },
        };

        var all = PlanWarningDisplay.StatementWarnings(statement);

        Assert.Equal(new[] { "Memory Grant Warning", "High Compile Time" }, all.Select(w => w.WarningType));
    }

    [Fact]
    public void OperatorWarnings_FollowTheStatementLevelOnes_InTreeOrder()
    {
        var leaf = Node(3, Warn("Residual Predicate", PlanWarningSeverity.Info));
        var statement = new PlanStatement
        {
            RootNode = Node(0, Node(1, Warn("Spill", PlanWarningSeverity.Warning)), Node(2, leaf)),
            PlanWarnings = new List<PlanWarning> { Warn("Plan Hint", PlanWarningSeverity.Info) },
        };

        var all = PlanWarningDisplay.StatementWarnings(statement);

        Assert.Equal(new[] { "Plan Hint", "Spill", "Residual Predicate" }, all.Select(w => w.WarningType));
    }

    [Fact]
    public void EverySeverityCounts_InfoIncluded()
    {
        var statement = new PlanStatement
        {
            RootNode = Node(0, Warn("Residual Predicate", PlanWarningSeverity.Info)),
            PlanWarnings = new List<PlanWarning> { Warn("Memory Grant Warning", PlanWarningSeverity.Critical), Warn("Plan Hint", PlanWarningSeverity.Info) },
        };

        var all = PlanWarningDisplay.StatementWarnings(statement);

        Assert.Equal(3, all.Count);
        Assert.Equal(1, all.Count(w => w.Severity == PlanWarningSeverity.Critical));
        Assert.Equal(2, all.Count(w => w.Severity != PlanWarningSeverity.Critical));
    }

    [Fact]
    public void NoRootNode_LeavesTheStatementLevelWarnings()
    {
        var statement = new PlanStatement { RootNode = null, PlanWarnings = new List<PlanWarning> { Warn("High Compile Time", PlanWarningSeverity.Warning) } };

        Assert.Single(PlanWarningDisplay.StatementWarnings(statement));
    }

    [Fact]
    public void NoWarningsAnywhere_IsEmpty()
    {
        Assert.Empty(PlanWarningDisplay.StatementWarnings(new PlanStatement { RootNode = Node(0, Node(1)) }));
    }

    private static PlanWarning Warn(string type, PlanWarningSeverity severity) =>
        new() { WarningType = type, Message = type, Severity = severity };

    private static PlanNode Node(int id, params PlanNode[] children) =>
        new() { NodeId = id, PhysicalOp = "Op", LogicalOp = "Op", Children = children.ToList() };

    private static PlanNode Node(int id, PlanWarning warning, params PlanNode[] children)
    {
        var node = Node(id, children);
        node.Warnings.Add(warning);
        return node;
    }
}
