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
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The plan node's row line, in its totals form: <c>"{ActualRows} of {expected} ({percent}%)"</c>, where the expected
/// figure is <see cref="RowEstimateHelper.GetExpectedRows"/> (the estimate times the executions on the inner side of a
/// Nested Loops join, the estimate itself everywhere else) and the text comes from
/// <see cref="PlanRowAccuracy.FormatActualOfExpected"/>. History: #4684 fixed a Key Lookup that ran 117 times for 1 row
/// reading <c>0 of 0 (89%)</c>; that fix printed the per-execution figures (<c>0.0085 of 0.0096 (89%)</c>), which divided
/// the total by <c>ActualExecutions</c> for every node and so read an accurate operator in a DOP 8 parallel zone as
/// <c>12 of 100 (12%)</c> and, from DOP 11, drew it in the critical colour (#4627). The label is now the totals, as
/// PerformanceStudio#594 prints them: that same lookup reads <c>1 of 1.128 (89%)</c>.
/// </summary>
public sealed class Viewer4684RowLabelTests
{
    private static readonly CultureInfo Invariant = CultureInfo.InvariantCulture;

    private static string FixturePath(string fileName) =>
        Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", fileName);

    private static PlanNode FixtureNode(string fileName, int nodeId)
    {
        var plan = ShowPlanParser.Parse(File.ReadAllText(FixturePath(fileName)));
        return plan.Batches
            .SelectMany(b => b.Statements)
            .Where(s => s.RootNode != null)
            .SelectMany(s => Flatten(s.RootNode!))
            .Single(n => n.NodeId == nodeId);
    }

    private static IEnumerable<PlanNode> Flatten(PlanNode node)
    {
        yield return node;
        foreach (var child in node.Children)
            foreach (var descendant in Flatten(child))
                yield return descendant;
    }

    /// <summary>The label text exactly as the plan viewer builds it: the node's total ActualRows against its expected rows.</summary>
    private static string LabelOf(PlanNode node, IFormatProvider? provider = null) =>
        PlanRowAccuracy.FormatActualOfExpected(node.ActualRows, RowEstimateHelper.GetExpectedRows(node), provider ?? Invariant);

    /// <summary>A Nested Loops join over a scan (outer) and <paramref name="inner"/>, parents wired as the parser does.</summary>
    private static PlanNode NestedLoopsOver(PlanNode inner, PlanNode? outer = null)
    {
        outer ??= new PlanNode { PhysicalOp = "Clustered Index Scan" };
        var join = new PlanNode { PhysicalOp = "Nested Loops", LogicalOp = "Inner Join", Children = { outer, inner } };
        outer.Parent = join;
        inner.Parent = join;
        return join;
    }

    /// <summary>A scan under a Gather Streams: ActualExecutions is the DOP (a thread count), not a loop count.</summary>
    private static PlanNode ParallelScan(long rows, int dop)
    {
        var scan = new PlanNode
        {
            PhysicalOp = "Clustered Index Scan",
            HasActualStats = true,
            EstimateRows = rows,
            ActualRows = rows,
            ActualExecutions = dop
        };
        var gather = new PlanNode { PhysicalOp = "Parallelism", LogicalOp = "Gather Streams", Children = { scan } };
        scan.Parent = gather;
        return scan;
    }

    // ---- the two gate vectors ---------------------------------------------------------------------

    /// <summary>A Key Lookup with 1 actual row and EstimateRows 0.00964372 over 117 executions: 1.12831524 expected.</summary>
    [Fact]
    public void GateVector_KeyLookupThatRan117Times_PrintsOneOfOnePointOneTwoEight()
    {
        var lookup = new PlanNode
        {
            PhysicalOp = "Key Lookup",
            HasActualStats = true,
            EstimateRows = 0.00964372,
            ActualRows = 1,
            ActualExecutions = 117
        };
        NestedLoopsOver(lookup);

        Assert.Equal(1.12831524, RowEstimateHelper.GetExpectedRows(lookup), precision: 8);
        Assert.Equal("1 of 1.128 (89%)", LabelOf(lookup));
    }

    /// <summary>Not on a Nested Loops inner side, so the expected rows are the plain estimate; 0.000005 must not read 0.</summary>
    [Fact]
    public void GateVector_TinyEstimateWithNoActualRows_KeepsItsFirstSignificantDigit()
    {
        var node = new PlanNode
        {
            PhysicalOp = "Clustered Index Scan",
            HasActualStats = true,
            EstimateRows = 0.000005,
            ActualRows = 0,
            ActualExecutions = 1
        };

        Assert.False(RowEstimateHelper.IsInnerSideOfNestedLoops(node));
        Assert.Equal("0 of 0.000005 (0%)", LabelOf(node));
    }

    // ---- real plans -------------------------------------------------------------------------------

    /// <summary>Node 4 of key_lookup_plan.sqlplan: the lookup the fractional label was written for. The per-execution
    /// label read "0.0085 of 0.0096 (89%)" here, and before that "0 of 0 (89%)".</summary>
    [Fact]
    public void KeyLookupFixture_Node4_PrintsTheTotals()
    {
        var node = FixtureNode("key_lookup_plan.sqlplan", 4);
        Assert.Equal(1L, node.ActualRows);
        Assert.Equal(117L, node.ActualExecutions);
        Assert.Equal(0.00964372, node.EstimateRows, precision: 8);
        Assert.True(RowEstimateHelper.IsInnerSideOfNestedLoops(node));

        Assert.Equal("1 of 1.128 (89%)", LabelOf(node));
    }

    /// <summary>eager_index_spool_plan.sqlplan runs at DOP 8. Node 1 is not on an inner side, so its 8 executions are
    /// threads and the expectation stays at the estimate (2,983); Nodes 4 and 5 are, so theirs is 1 x 613.</summary>
    [Theory]
    [InlineData(1, false, 8L, "609 of 2,983 (20%)")]
    [InlineData(4, true, 613L, "609 of 613 (99%)")]
    [InlineData(5, true, 613L, "609 of 613 (99%)")]
    public void EagerIndexSpoolFixture_PrintsTheTotals(int nodeId, bool innerSide, long executions, string expected)
    {
        var node = FixtureNode("eager_index_spool_plan.sqlplan", nodeId);
        Assert.Equal(innerSide, RowEstimateHelper.IsInnerSideOfNestedLoops(node));
        Assert.Equal(executions, node.ActualExecutions);
        Assert.Equal(609L, node.ActualRows);

        Assert.Equal(expected, LabelOf(node));
    }

    // ---- parallel zones and nested loops ----------------------------------------------------------

    /// <summary>An operator whose actual rows equal its estimate is exactly on target at any DOP. The per-execution label
    /// divided the 100 rows by the thread count: DOP 8 read 12.5%, printed "12 of 100 (12%)" (12.5 is an exact tie, which
    /// .NET formats half to even), and from DOP 11 the ratio fell under 0.1 and the label turned critical-orange.</summary>
    [Theory]
    [InlineData(8, "12 of 100 (12%)", false)]
    [InlineData(11, "9 of 100 (9%)", true)]
    public void AccurateOperatorInAParallelZone_ReadsOneHundredPercent_AndIsNotDrawnCritical(
        int dop, string oldLabel, bool oldLabelWasCritical)
    {
        var node = ParallelScan(100, dop);
        Assert.False(RowEstimateHelper.IsInnerSideOfNestedLoops(node));

        Assert.Equal("100 of 100 (100%)", LabelOf(node));

        // The label's brush is orange when this ratio is under 0.1 or over 10.0.
        var brushRatio = RowEstimateHelper.GetRowAccuracyRatio(node);
        Assert.Equal(1.0, brushRatio);
        Assert.InRange(brushRatio, 0.1, 10.0);

        // What the per-execution math did to the same node (arithmetic only, so this survives that code's removal).
        var perExecutionRatio = (double)node.ActualRows / node.ActualExecutions / node.EstimateRows;
        Assert.Equal(oldLabel, PerExecutionLabel(node));
        Assert.Equal(oldLabelWasCritical, perExecutionRatio < 0.1 || perExecutionRatio > 10.0);
    }

    private static string PerExecutionLabel(PlanNode node)
    {
        var perExecution = (double)node.ActualRows / node.ActualExecutions;
        var percent = (perExecution / node.EstimateRows * 100).ToString("F0", Invariant);
        return string.Create(Invariant, $"{perExecution:N0} of {node.EstimateRows:N0} ({percent}%)");
    }

    /// <summary>ActualExecutions on a node under two inner sides already counts the iterations of both joins, so the
    /// expectation is the estimate times that count, once: 0.5 x 1,000 = 500.</summary>
    [Fact]
    public void NodeUnderTwoNestedLoopsInnerSides_ExpectsTheEstimateTimesExecutionsOnce()
    {
        var seek = new PlanNode
        {
            PhysicalOp = "Index Seek",
            HasActualStats = true,
            EstimateRows = 0.5,
            ActualRows = 400,
            ActualExecutions = 1000
        };
        var innerJoin = NestedLoopsOver(seek);
        NestedLoopsOver(innerJoin);

        Assert.True(RowEstimateHelper.IsInnerSideOfNestedLoops(seek));
        Assert.Equal("400 of 500 (80%)", LabelOf(seek));
    }

    // ---- the rule ---------------------------------------------------------------------------------

    /// <summary>Whole numbers print N0 and a missing or zero expectation drops the percentage, exactly as the label always did.</summary>
    [Theory]
    [InlineData(1.0, 1.0, "1 of 1 (100%)")]
    [InlineData(5.0, 4.0, "5 of 4 (125%)")]
    [InlineData(1234.5, 1000.0, "1,234 of 1,000 (123%)")] // N0 rounds an exact .5 to even
    [InlineData(105.5128, 103.694, "106 of 104 (102%)")]
    [InlineData(1234567.0, 2000000.0, "1,234,567 of 2,000,000 (62%)")]
    [InlineData(1.0, 0.0, "1 of 0")]
    [InlineData(0.0, 0.0, "0 of 0")]
    [InlineData(12.0, 0.0, "12 of 0")]
    [InlineData(0.0, 250.0, "0 of 250 (0%)")]
    [InlineData(1.0, 1000000.0, "1 of 1,000,000 (0%)")] // the percentage is whole; only the row counts are kept off zero
    public void WholeNumbers_PrintN0(double actual, double expected, string label)
    {
        Assert.Equal(label, PlanRowAccuracy.FormatActualOfExpected(actual, expected, Invariant));
    }

    /// <summary>The fewest decimals at which the printed numbers give the printed percentage. A whole number keeps its N0
    /// text, and a fixed-point count means a trailing zero can appear ("0.60", "0.30").</summary>
    [Theory]
    [InlineData(0.0, 0.5, "0 of 0.5 (0%)")]
    [InlineData(0.5, 5.0, "0.5 of 5 (10%)")]
    [InlineData(3.0, 0.4, "3 of 0.4 (750%)")]
    [InlineData(0.1, 0.3, "0.1 of 0.3 (33%)")]
    [InlineData(2.5, 2.0, "2.5 of 2 (125%)")]
    [InlineData(100000.0, 12.5, "100,000 of 12.5 (800000%)")]
    [InlineData(0.6, 0.75, "0.60 of 0.75 (80%)")]
    [InlineData(0.25, 0.3, "0.25 of 0.30 (83%)")]
    [InlineData(0.99, 1.01, "0.99 of 1.01 (98%)")]
    [InlineData(0.6, 0.55555, "0.600 of 0.556 (108%)")]
    [InlineData(0.0104, 0.0096, "0.0104 of 0.0096 (108%)")]
    public void Decimals_AreAddedOnlyUntilTheNumbersAgreeWithThePercentage(double actual, double expected, string label)
    {
        Assert.Equal(label, PlanRowAccuracy.FormatActualOfExpected(actual, expected, Invariant));
    }

    /// <summary>Four decimals is the ceiling for making the numbers agree, so a label can still contradict its
    /// percentage there. It still never prints a non-zero value as 0: 0.00001234 needs five decimals to show a digit, and
    /// the cap does not apply to that.</summary>
    [Theory]
    [InlineData(1.0, 0.00012345, "1 of 0.0001 (810045%)")]
    [InlineData(0.000123, 0.000456, "0.0001 of 0.0005 (27%)")]
    [InlineData(1.0, 0.00001234, "1 of 0.00001 (8103728%)")]
    [InlineData(0.0000004, 1.0, "0.0000004 of 1 (0%)")]
    [InlineData(0.00001, 0.0, "0.00001 of 0")]
    public void Cap_StopsAtFourDecimals_ButANonZeroValueNeverPrintsAsZero(double actual, double expected, string label)
    {
        Assert.Equal(label, PlanRowAccuracy.FormatActualOfExpected(actual, expected, Invariant));
    }

    /// <summary>No magnitude prints an exponent.</summary>
    [Theory]
    [InlineData(1e15, 3e15, "1,000,000,000,000,000 of 3,000,000,000,000,000 (33%)")]
    [InlineData(1e21, 4e21, "1,000,000,000,000,000,000,000 of 4,000,000,000,000,000,000,000 (25%)")]
    public void LargeNumbers_PrintEveryDigit(double actual, double expected, string label)
    {
        Assert.Equal(label, PlanRowAccuracy.FormatActualOfExpected(actual, expected, Invariant));
    }

    [Theory]
    [InlineData(1e300, 3e300)]
    [InlineData(1.0, double.MaxValue)]
    [InlineData(5e-324, 1e-320)]
    [InlineData(1e-300, 1e300)]
    public void ExtremeMagnitudes_NeverPrintAnExponent(double actual, double expected)
    {
        var label = PlanRowAccuracy.FormatActualOfExpected(actual, expected, Invariant);

        Assert.DoesNotContain("E", label, StringComparison.OrdinalIgnoreCase);
        Assert.Matches(new Regex(@"^[\d,.]+ of [\d,.]+ \(\d+%\)$", RegexOptions.CultureInvariant), label);
    }

    // ---- culture ----------------------------------------------------------------------------------

    /// <summary>The numbers, the decimal mark and the group mark follow the caller's culture, and the agreement check
    /// parses the group mark back ("2.983" is 2983 in de-DE).</summary>
    [Theory]
    [InlineData(1.0, 1.12831524, "1 of 1,128 (89%)")]
    [InlineData(609.0, 2983.02, "609 of 2.983 (20%)")]
    [InlineData(0.6, 0.75, "0,60 of 0,75 (80%)")]
    public void Culture_FollowsTheCallers(double actual, double expected, string label)
    {
        Assert.Equal(label, PlanRowAccuracy.FormatActualOfExpected(actual, expected, new CultureInfo("de-DE")));
    }

    [Fact]
    public void KeyLookupFixture_InGerman_PrintsADecimalComma()
    {
        Assert.Equal("1 of 1,128 (89%)", LabelOf(FixtureNode("key_lookup_plan.sqlplan", 4), new CultureInfo("de-DE")));
    }

    [Fact]
    public void Culture_DefaultsToTheCurrentCulture()
    {
        var saved = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("de-DE");
            Assert.Equal("1 of 1,128 (89%)", PlanRowAccuracy.FormatActualOfExpected(1.0, 1.12831524));
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    // ---- a sweep ----------------------------------------------------------------------------------

    /// <summary>2,000 pairs from 1e-6 up to 1e6, whole and fractional, a twentieth with no actual rows. Each label must
    /// (1) agree with its own percentage or have stopped at four decimals, (2) carry no exponent, and (3) never print a
    /// non-zero value as zero.</summary>
    [Fact]
    public void Sweep_EveryLabelAgreesOrHitTheCap_HasNoExponent_AndNeverPrintsANonZeroValueAsZero()
    {
        var random = new Random(4684);
        var shape = new Regex(@"^(?<a>\S+) of (?<e>\S+) \((?<p>\d+)%\)$", RegexOptions.CultureInvariant);
        int pairs = 0, withDecimals = 0, atTheCap = 0, pastTheCap = 0;

        for (var i = 0; i < 2000; i++)
        {
            var expected = Math.Pow(10, random.NextDouble() * 12 - 6);
            if (i % 3 == 0)
                expected = Math.Max(Math.Round(expected), 1);
            var actual = expected * Math.Pow(10, random.NextDouble() * 2 - 1);
            if (i % 5 == 0)
                actual = Math.Round(actual);
            if (i % 20 == 0)
                actual = 0;

            var label = PlanRowAccuracy.FormatActualOfExpected(actual, expected, Invariant);
            var context = $"actual {actual:R}, expected {expected:R} -> \"{label}\"";
            var match = shape.Match(label);
            Assert.True(match.Success, $"unexpected label shape: {context}");
            pairs++;

            // (2) no exponent
            Assert.False(label.Contains('E') || label.Contains('e'), $"exponent in {context}");

            var actualText = match.Groups["a"].Value;
            var expectedText = match.Groups["e"].Value;
            var printedActual = double.Parse(actualText, NumberStyles.Number, Invariant);
            var printedExpected = double.Parse(expectedText, NumberStyles.Number, Invariant);

            // (3) a non-zero value never prints as zero
            Assert.True(actual == 0 || printedActual != 0, $"actual printed as zero: {context}");
            Assert.True(printedExpected != 0, $"expected printed as zero: {context}");

            // (1) the printed numbers give the printed percentage, unless the search ran out at four decimals
            var decimals = Math.Max(DecimalPlaces(actualText), DecimalPlaces(expectedText));
            var agrees = (printedActual / printedExpected * 100).ToString("F0", Invariant) == match.Groups["p"].Value;
            Assert.True(agrees || decimals >= 4, $"the label contradicts its percentage before the cap: {context}");

            if (decimals > 0)
                withDecimals++;
            if (decimals >= 4)
                atTheCap++;
            if (decimals >= 5)
                pastTheCap++;
        }

        Assert.Equal(2000, pairs);
        // The sweep has to reach the interesting paths, or the three properties above prove little.
        Assert.True(withDecimals > 800, $"only {withDecimals} labels needed decimals");
        Assert.True(atTheCap > 200, $"only {atTheCap} labels reached the cap");
        Assert.True(pastTheCap > 100, $"only {pastTheCap} labels needed the first-significant-digit rule");
    }

    private static int DecimalPlaces(string number)
    {
        var point = number.IndexOf('.');
        return point < 0 ? 0 : number.Length - point - 1;
    }

    // ---- the plan viewer ------------------------------------------------------------------------------

    /// <summary>The shared plan viewer builds the node text from the totals and the brush from the same expectation; the
    /// per-execution basis (and the interpolated N0 label before it) must not come back.</summary>
    [Fact]
    public void NodeLabel_IsBuiltFromTheTotalsAndTheSharedExpectation()
    {
        var source = File.ReadAllText(RenderingCs());

        Assert.Contains("PlanRowAccuracy.FormatActualOfExpected(node.ActualRows, RowEstimateHelper.GetExpectedRows(node))", source, StringComparison.Ordinal);
        Assert.Contains("var accuracyRatio = RowEstimateHelper.GetRowAccuracyRatio(node);", source, StringComparison.Ordinal);
        Assert.Contains("(accuracyRatio < 0.1 || accuracyRatio > 10.0) ? CriticalOrangeBrush : fgBrush", source, StringComparison.Ordinal);

        Assert.DoesNotContain("FormatActualOfEstimate", source, StringComparison.Ordinal);
        Assert.DoesNotContain("ActualRowsPerExecution", source, StringComparison.Ordinal);
        Assert.DoesNotContain("PlanRowAccuracy.Ratio(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("{actualRowsPerExec:N0} of", source, StringComparison.Ordinal);
    }

    private static string RenderingCs([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "PerformanceMonitor.Ui", "PlanViewerControl.Rendering.cs"));
}
