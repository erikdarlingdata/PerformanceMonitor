/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8522: the rest of the plan-analysis sentences that carry a number read one convention and agree with a count of
/// one. The Key Lookup's residual-predicate and no-statement-time lines, the Row Estimate Mismatch message and the
/// Nested Loops outer-side detail are written in the invariant culture like the grade beside them, and read "filtered
/// 1 row", "1 row x 500 executions" and "estimated 1 row". The Wait Stats header and its rows read the same on a German
/// desktop as on an English one, and a single wait reads "(1 wait)". Each test sets the thread culture for the call and
/// restores it afterwards; the culture is per thread, so a test running beside it is not touched.
/// </summary>
public sealed class PlanAnalysisCultureAndCountTests
{
    private static T InCulture<T>(string name, Func<T> read)
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(name);
        try
        {
            return read();
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    private static IEnumerable<PlanNode> Descendants(PlanNode node) =>
        new[] { node }.Concat(node.Children.SelectMany(Descendants));

    private static List<PlanWarning> Analyze(PlanNode root, long? statementMs, double statementCost = 0)
    {
        var stmt = new PlanStatement { RootNode = root, StatementSubTreeCost = statementCost };
        if (statementMs is { } ms)
            stmt.QueryTimeStats = new QueryTimeInfo { ElapsedTimeMs = ms };
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalyzer.Analyze(plan, null, null, CancellationToken.None);
        return stmt.PlanWarnings.Concat(Descendants(root).SelectMany(n => n.Warnings)).ToList();
    }

    private static PlanNode Lookup(long executions, long rows, string? predicate = null) => new()
    {
        NodeId = 0,
        PhysicalOp = "Key Lookup (Clustered)",
        LogicalOp = "Key Lookup",
        Lookup = true,
        HasActualStats = true,
        ActualExecutions = executions,
        ActualRows = rows,
        ActualElapsedMs = 1200,
        Predicate = predicate
    };

    [Theory]
    [InlineData(5000, 4999, "en-US", "Residual predicate (filtered 1 row): [t].[c] = 1")]
    [InlineData(5000, 3766, "de-DE", "Residual predicate (filtered 1,234 rows): [t].[c] = 1")]
    public void KeyLookup_ResidualPredicateLine_CountsItsRowsInOneConvention(long executions, long rows, string culture, string expected)
    {
        var warnings = InCulture(culture, () => Analyze(Lookup(executions, rows, "[t].[c] = 1"), statementMs: 10_000));
        var lookup = Assert.Single(warnings, w => w.WarningType == "Key Lookup");

        Assert.Contains(expected, lookup.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void KeyLookup_WithoutStatementTime_OnAGermanDesktop_CountsItsLookupsInOneConvention()
    {
        var warnings = InCulture("de-DE", () => Analyze(Lookup(5000, 5000), statementMs: null));
        var lookup = Assert.Single(warnings, w => w.WarningType == "Key Lookup");

        Assert.Contains("Lookups performed: 5,000.", lookup.Message, StringComparison.Ordinal);
    }

    private static PlanNode NestedLoops(PlanNode outer, PlanNode inner)
    {
        var node = new PlanNode
        {
        NodeId = 0,
        PhysicalOp = "Nested Loops",
        LogicalOp = "Inner Join",
        HasActualStats = true,
        ActualRows = inner.ActualRows,
        ActualExecutions = 1,
        Children = [outer, inner]
        };
        outer.Parent = node;
        inner.Parent = node;
        return node;
    }

    private static PlanNode Seek(int id, double estimateRows, long actualRows, long executions) => new()
    {
        NodeId = id,
        PhysicalOp = "Index Seek",
        LogicalOp = "Index Seek",
        HasActualStats = true,
        EstimateRows = estimateRows,
        ActualRows = actualRows,
        ActualExecutions = executions
    };

    [Fact]
    public void RowEstimateMismatch_OneRowPerExecution_ReadsSingular_InOneConvention()
    {
        // An inner-side seek estimated at 10 rows per execution that returns one row on each of 500 executions.
        var root = NestedLoops(Seek(1, 500, 500, 1), Seek(2, 10, 500, 500));
        var warnings = InCulture("de-DE", () => Analyze(root, statementMs: 10_000));
        var mismatch = Assert.Single(warnings, w => w.WarningType == "Row Estimate Mismatch" && w.Message.Contains(" x 500 executions", StringComparison.Ordinal));

        Assert.Contains("Actual 500 (1 row x 500 executions)", mismatch.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void NestedLoops_OuterSideEstimatedAtOneRow_ReadsSingular_InOneConvention()
    {
        // An outer side estimated at one row that produced 150,000, driving the inner side 150,000 times.
        var root = NestedLoops(Seek(1, 1, 150_000, 1), Seek(2, 1, 150_000, 150_000));
        var warnings = InCulture("de-DE", () => Analyze(root, statementMs: 10_000));
        var detail = Assert.Single(warnings, w => w.Message.Contains("Outer side: estimated", StringComparison.Ordinal));

        Assert.Contains("Nested Loops inner side executed 150,000 times", detail.Message, StringComparison.Ordinal);
        Assert.Contains("Outer side: estimated 1 row, actual 150,000 (150000x underestimate).", detail.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    public void WaitStatsHeaderAndRows_ReadOneConvention_AndCountOneWait(string culture)
    {
        Assert.Equal("Wait Stats — 1,022ms total", InCulture(culture, () => PlanDisplayText.WaitStatsHeaderText(1022)));
        Assert.Equal("1,022 ms (1 wait)", InCulture(culture, () => PlanDisplayText.WaitRowText(1022, 1)));
        Assert.Equal("1,022 ms (3 waits)", InCulture(culture, () => PlanDisplayText.WaitRowText(1022, 3)));
    }

    [Fact]
    public void CeGuess_OnAGermanDesktop_WritesItsPercentageInOneConvention()
    {
        // An estimated scan whose estimate is 30% of a large table: the guess description's percentage is part of the same invariant sentence.
        var scan = new PlanNode
        {
            NodeId = 0,
            PhysicalOp = "Clustered Index Scan",
            LogicalOp = "Clustered Index Scan",
            HasActualStats = false,
            TableCardinality = 1_000_000,
            EstimateRows = 300_000,
            Predicate = "[t].[c] > 1",
            CostPercent = 100,
            EstimatedTotalSubtreeCost = 10
        };
        var warnings = InCulture("de-DE", () => Analyze(scan, statementMs: null, statementCost: 10));
        var guess = Assert.Single(warnings, w => w.Message.Contains("matches the 30% guess", StringComparison.Ordinal));

        Assert.Contains("matches the 30% guess for an inequality such as > or < (30.0%)", guess.Message, StringComparison.Ordinal);
        Assert.Contains("Estimated 300,000 rows from 1,000,000 row table", guess.Message, StringComparison.Ordinal);
    }
}
