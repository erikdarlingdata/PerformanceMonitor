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
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4534 — the warning-origin decider required the pin to run against PerformanceStudio's OWN test
/// plans, not a synthetic one. These three fixtures (<c>table_variable_plan.sqlplan</c>,
/// <c>convert_implicit_plan.sqlplan</c>, <c>udf_plan.sqlplan</c>) are copied verbatim from
/// PerformanceStudio's public repo (<c>tests/PlanViewer.Core.Tests/Plans/</c>), and this class ports
/// the facts <c>WarningOriginTests</c> asserts against them, adapted to
/// <see cref="ShowPlanParser"/>/<see cref="PlanAnalyzer"/> and to <see cref="McpPlanAnalysisFormatter"/>
/// in place of PerformanceStudio's <c>ResultMapper</c>.
/// <see cref="PlanSync4534Tests"/> keeps the existing synthetic facts; this class adds the
/// real-fixture facts the decider asked for.
/// </summary>
public sealed class PlanSync4534OriginFixturesTests
{
    private static string FixturePath(string fileName) =>
        Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", fileName);

    private static ParsedPlan LoadAndAnalyze(string fileName)
    {
        var xml = File.ReadAllText(FixturePath(fileName));
        var plan = ShowPlanParser.Parse(xml);
        PlanAnalyzer.Analyze(plan);
        return plan;
    }

    private static IEnumerable<(PlanNode Node, PlanWarning Warning)> Walk(PlanNode node)
    {
        foreach (var w in node.Warnings)
            yield return (node, w);
        foreach (var child in node.Children)
            foreach (var pair in Walk(child))
                yield return pair;
    }

    /// <summary>
    /// Ported from PerformanceStudio's <c>EveryOperatorWarningKnowsItsOperator</c>: across every
    /// operator-level warning in the three copied fixtures, the origin is never empty and never
    /// points at a different node than the one it hangs off.
    /// </summary>
    [Fact]
    public void EveryOperatorWarningKnowsItsOperator()
    {
        var orphans = new List<string>();

        foreach (var file in new[] { "table_variable_plan.sqlplan", "convert_implicit_plan.sqlplan", "udf_plan.sqlplan" })
        {
            var plan = LoadAndAnalyze(file);
            foreach (var stmt in plan.Batches.SelectMany(b => b.Statements))
            {
                if (stmt.RootNode == null)
                    continue;

                foreach (var (node, warning) in Walk(stmt.RootNode))
                {
                    if (warning.OriginNodeIds.Count == 0)
                        orphans.Add($"{file}:{warning.WarningType}");
                    else if (!warning.OriginNodeIds.Contains(node.NodeId))
                        orphans.Add($"{file}:{warning.WarningType} points away from its own node");
                }
            }
        }

        Assert.True(orphans.Count == 0, "Operator warnings without a usable origin: " + string.Join(", ", orphans));
    }

    /// <summary>
    /// Ported from PerformanceStudio's <c>TheTableVariableWarningPointsAtTheOperatorsThatTouchOne</c>:
    /// the statement-level Table Variable warning on the real fixture carries the operators that
    /// touched the table variable, not an empty origin.
    /// </summary>
    [Fact]
    public void TheTableVariableWarningPointsAtTheOperatorsThatTouchOne()
    {
        var plan = LoadAndAnalyze("table_variable_plan.sqlplan");
        var statementWarnings = plan.Batches
            .SelectMany(b => b.Statements)
            .SelectMany(s => s.PlanWarnings)
            .Where(w => w.WarningType == "Table Variable")
            .ToList();

        Assert.NotEmpty(statementWarnings);
        Assert.All(statementWarnings, w => Assert.NotEmpty(w.OriginNodeIds));
    }

    /// <summary>
    /// Ported from PerformanceStudio's <c>WarningsWithNoResponsibleOperatorClaimNone</c>. High Compile
    /// CPU is measured before a row is read, so no operator is responsible for it, and it must claim
    /// none rather than pointing somewhere arbitrary.
    /// </summary>
    [Fact]
    public void HighCompileCpuWarningClaimsNoOperatorOrigin()
    {
        var plan = LoadAndAnalyze("convert_implicit_plan.sqlplan");
        var matching = plan.Batches
            .SelectMany(b => b.Statements)
            .SelectMany(s => s.PlanWarnings)
            .Where(w => w.WarningType == "High Compile CPU")
            .ToList();

        Assert.NotEmpty(matching);
        Assert.All(matching, w => Assert.Empty(w.OriginNodeIds));
    }

    /// <summary>
    /// The other half of PerformanceStudio's <c>WarningsWithNoResponsibleOperatorClaimNone</c> row for
    /// <c>udf_plan.sqlplan</c>: PM's "UDF Execution" is reported at the statement level from
    /// <c>QueryTimeStats</c> (SQL Server reports it there, not per-node), and it must stay empty too.
    /// </summary>
    [Fact]
    public void UdfExecutionWarningClaimsNoOperatorOrigin()
    {
        var plan = LoadAndAnalyze("udf_plan.sqlplan");
        var matching = plan.Batches
            .SelectMany(b => b.Statements)
            .SelectMany(s => s.PlanWarnings)
            .Where(w => w.WarningType == "UDF Execution")
            .ToList();

        Assert.NotEmpty(matching);
        Assert.All(matching, w => Assert.Empty(w.OriginNodeIds));
    }

    /// <summary>
    /// Ported from PerformanceStudio's <c>TheJsonOutputCarriesOrigins</c>: the JSON consumer
    /// (<see cref="McpPlanAnalysisFormatter"/>, PM's equivalent of PerformanceStudio's
    /// <c>ResultMapper</c>) carries origin_node_ids as a field on the Table Variable warning from the
    /// real fixture, not just on the synthetic repro.
    /// </summary>
    [Fact]
    public void TheJsonOutputCarriesOrigins()
    {
        var xml = File.ReadAllText(FixturePath("table_variable_plan.sqlplan"));
        var plan = ShowPlanParser.Parse(xml);
        PlanAnalyzer.Analyze(plan);

        var json = McpPlanAnalysisFormatter.BuildAnalysisResult(xml, "test-server", "showplan", null);

        Assert.Contains("origin_node_ids", json);

        var tableVariable = plan.Batches
            .SelectMany(b => b.Statements)
            .SelectMany(s => s.PlanWarnings)
            .Single(w => w.WarningType == "Table Variable");

        Assert.NotEmpty(tableVariable.OriginNodeIds);
    }
}
