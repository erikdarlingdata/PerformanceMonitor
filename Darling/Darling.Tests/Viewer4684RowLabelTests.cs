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
/// The plan node's row line printed <c>0 of 0 (89%)</c> for the Key Lookup in <c>key_lookup_plan.sqlplan</c>: the
/// lookup ran 117 times for 1 row in total, so it returned 1/117 = 0.0085 rows per execution against a per-execution
/// estimate of 0.0096, and the label's <c>N0</c> rounded both to "0" while still printing the 89% they make. The
/// label now comes from <see cref="PlanRowAccuracy.FormatActualOfEstimate"/>: values of 1 and over are unchanged, a
/// value under 1 prints in enough significant digits that the numbers and the percentage agree.
/// </summary>
public sealed class Viewer4684RowLabelTests
{
    private static readonly CultureInfo Invariant = CultureInfo.InvariantCulture;

    private static string FixturePath(string fileName) =>
        Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", fileName);

    private static PlanNode KeyLookupNode()
    {
        var plan = ShowPlanParser.Parse(File.ReadAllText(FixturePath("key_lookup_plan.sqlplan")));
        return plan.Batches
            .SelectMany(b => b.Statements)
            .Where(s => s.RootNode != null)
            .SelectMany(s => Flatten(s.RootNode!))
            .Single(n => n.NodeId == 4);
    }

    private static IEnumerable<PlanNode> Flatten(PlanNode node)
    {
        yield return node;
        foreach (var child in node.Children)
            foreach (var descendant in Flatten(child))
                yield return descendant;
    }

    /// <summary>The fixture still holds the numbers the label was wrong about, and the OLD interpolation still
    /// prints the contradiction: this is the failing shape the formatter replaces.</summary>
    [Fact]
    public void KeyLookupFixture_OldInlineLabel_PrintedZeroOfZeroAtEightyNinePercent()
    {
        var node = KeyLookupNode();
        Assert.Equal(1L, node.ActualRows);
        Assert.Equal(117L, node.ActualExecutions);
        Assert.Equal(0.00964372, node.EstimateRows, precision: 8);

        var perExecution = PlanRowAccuracy.ActualRowsPerExecution(node.ActualRows, node.ActualExecutions);
        var ratio = PlanRowAccuracy.Ratio(perExecution, node.EstimateRows);
        var oldLabel = string.Create(Invariant, $"{perExecution:N0} of {node.EstimateRows:N0} ({ratio * 100:F0}%)");

        Assert.Equal("0 of 0 (89%)", oldLabel);
    }

    [Fact]
    public void KeyLookupFixture_NewLabel_AgreesWithItsOwnPercentage()
    {
        var node = KeyLookupNode();
        var perExecution = PlanRowAccuracy.ActualRowsPerExecution(node.ActualRows, node.ActualExecutions);

        Assert.Equal("0.0085 of 0.0096 (89%)", PlanRowAccuracy.FormatActualOfEstimate(perExecution, node.EstimateRows, Invariant));
    }

    [Theory]
    [InlineData(1.0, 1.0, "1 of 1 (100%)")]
    [InlineData(5.0, 4.0, "5 of 4 (125%)")]
    [InlineData(1234.5, 1000.0, "1,234 of 1,000 (123%)")] // N0 rounds an exact .5 to even, as the label always did
    [InlineData(105.5128, 103.694, "106 of 104 (102%)")]
    [InlineData(1.0, 0.0, "1 of 0")]
    [InlineData(0.0, 0.0, "0 of 0")]
    [InlineData(12.0, 0.0, "12 of 0")]
    [InlineData(0.0, 250.0, "0 of 250 (0%)")]
    public void ValuesOfOneAndOver_PrintExactlyAsBefore(double actualPerExecution, double estimate, string expected)
    {
        Assert.Equal(expected, PlanRowAccuracy.FormatActualOfEstimate(actualPerExecution, estimate, Invariant));
    }

    [Theory]
    [InlineData(0.0, 0.5, "0 of 0.5 (0%)")]
    [InlineData(0.6, 0.75, "0.6 of 0.75 (80%)")]
    [InlineData(0.0104, 0.0096, "0.0104 of 0.0096 (108%)")]
    [InlineData(0.5, 5.0, "0.5 of 5 (10%)")]
    [InlineData(3.0, 0.4, "3 of 0.4 (750%)")]
    public void ValuesUnderOne_PrintInSignificantDigitsThatAgreeWithThePercentage(double actualPerExecution, double estimate, string expected)
    {
        Assert.Equal(expected, PlanRowAccuracy.FormatActualOfEstimate(actualPerExecution, estimate, Invariant));
    }

    /// <summary>Whenever both numbers are fractions, dividing the printed numbers must give the printed
    /// percentage. A fixed-seed sweep over four orders of magnitude, so the digit growth is exercised.</summary>
    [Fact]
    public void TwoFractions_PrintedNumbersAlwaysReproduceThePrintedPercentage()
    {
        var random = new Random(4684);
        var labelShape = new Regex(@"^(?<a>\S+) of (?<e>\S+) \((?<p>-?\d+)%\)$", RegexOptions.CultureInvariant);

        for (var i = 0; i < 2000; i++)
        {
            var estimate = Math.Pow(10, -4 * random.NextDouble());
            var actual = estimate * Math.Pow(10, random.NextDouble() * 2 - 1);
            if (actual >= 1 || estimate >= 1)
                continue;

            var label = PlanRowAccuracy.FormatActualOfEstimate(actual, estimate, Invariant);
            var match = labelShape.Match(label);
            Assert.True(match.Success, $"unexpected label shape: {label}");

            var shownActual = double.Parse(match.Groups["a"].Value, NumberStyles.Float, Invariant);
            var shownEstimate = double.Parse(match.Groups["e"].Value, NumberStyles.Float, Invariant);
            Assert.True(
                (shownActual / shownEstimate * 100).ToString("F0", Invariant) == match.Groups["p"].Value,
                $"the label \"{label}\" contradicts its own percentage (actual {actual:R}, estimate {estimate:R}).");
        }
    }

    [Fact]
    public void Culture_FollowsTheCallers()
    {
        Assert.Equal("0,0085 of 0,0096 (89%)",
            PlanRowAccuracy.FormatActualOfEstimate(1.0 / 117, 0.00964372, new CultureInfo("de-DE")));
    }

    /// <summary>The shared plan viewer builds the node text through the formatter; the interpolated N0 label it
    /// replaced must not come back.</summary>
    [Fact]
    public void NodeLabel_IsBuiltByTheSharedFormatter()
    {
        var source = File.ReadAllText(RenderingCs());

        Assert.Contains("PlanRowAccuracy.FormatActualOfEstimate(actualRowsPerExec, estRows)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("{actualRowsPerExec:N0} of", source, StringComparison.Ordinal);
    }

    private static string RenderingCs([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "PerformanceMonitor.Ui", "PlanViewerControl.Rendering.cs"));
}
