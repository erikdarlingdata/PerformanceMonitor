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
/// #4533 — when a scalar UDF prevents parallelism, rule 3 already reports a Serial Plan finding
/// that names the UDF as the reason. Rule 6 then adds a Scalar UDF finding for the same UDF, so
/// the reader gets two findings for one cause. Ported from
/// erikdarlingdata/PerformanceStudio@495750f (rule 6 checks the reason) plus @cc18844's #576
/// fix (rule 6 also requires the Serial Plan finding to actually be on the statement, so a
/// disabled or cost-gated-out rule 3 doesn't silently drop the UDF finding with nothing in its
/// place).
/// </summary>
public sealed class PlanSync4533Tests
{
    private static ParsedPlan Analyze(PlanStatement stmt)
    {
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalyzer.Analyze(plan);
        return plan;
    }

    private static bool Has(PlanStatement stmt, string warningType) =>
        stmt.PlanWarnings.Any(w => w.WarningType == warningType);

    private static bool Has(PlanNode node, string warningType) =>
        node.Warnings.Any(w => w.WarningType == warningType);

    private static (PlanStatement Stmt, PlanNode Node) UdfStatement(string nonParallelPlanReason, bool parallel)
    {
        var node = new PlanNode
        {
            PhysicalOp = "Compute Scalar",
            LogicalOp = "Compute Scalar",
            ScalarUdfs = [new ScalarUdfReference { FunctionName = "dbo.F" }]
        };
        var stmt = new PlanStatement
        {
            NonParallelPlanReason = parallel ? null : nonParallelPlanReason,
            StatementSubTreeCost = 10,
            RootNode = node
        };
        return (stmt, node);
    }

    [Fact]
    public void SerialPlan_TSQLUdfReason_SuppressesRule6()
    {
        var (stmt, node) = UdfStatement("TSQLUserDefinedFunctionsNotParallelizable", parallel: false);
        var plan = Analyze(stmt);
        var resultStmt = plan.Batches[0].Statements[0];
        var resultNode = resultStmt.RootNode!;

        Assert.True(Has(resultStmt, "Serial Plan"));
        Assert.False(Has(resultNode, "Scalar UDF"));
    }

    [Fact]
    public void SerialPlan_CLRUdfReason_SuppressesRule6()
    {
        var (stmt, node) = UdfStatement("CLRUserDefinedFunctionRequiresDataAccess", parallel: false);
        var plan = Analyze(stmt);
        var resultStmt = plan.Batches[0].Statements[0];
        var resultNode = resultStmt.RootNode!;

        Assert.True(Has(resultStmt, "Serial Plan"));
        Assert.False(Has(resultNode, "Scalar UDF"));
    }

    [Fact]
    public void SameUdf_ParallelPlan_Rule6StillFires()
    {
        var (stmt, node) = UdfStatement("TSQLUserDefinedFunctionsNotParallelizable", parallel: true);
        var plan = Analyze(stmt);
        var resultStmt = plan.Batches[0].Statements[0];
        var resultNode = resultStmt.RootNode!;

        Assert.False(Has(resultStmt, "Serial Plan"));
        Assert.True(Has(resultNode, "Scalar UDF"));
    }
}
