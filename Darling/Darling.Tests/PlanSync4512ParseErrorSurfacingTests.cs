/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Runtime.CompilerServices;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4512: a combining merge resolved a conflict in both plan drill-down collectors by deleting
/// <c>if (!string.IsNullOrWhiteSpace(plan.ParseError)) return;</c> as "redundant" with
/// <c>PlanAnalysisPipeline.Run</c> skipping analysis on a parse-error plan. It isn't: the code after
/// <c>Run</c> still reads parser-extracted content (<c>PlanStatements.EnumerateAll(plan)</c>'s node
/// warnings and <c>plan.AllMissingIndexes</c>) that exists even when parsing only partially
/// succeeded, so a parse-error plan without the guard still contributes a partial drill-down. This
/// reads each collector's source and asserts the guard sits between the parse call and the first
/// read of that content.
/// </summary>
public sealed class PlanSync4512ParseErrorSurfacingTests
{
    [Theory]
    [InlineData("Darling", "PerformanceMonitor.Darling.Analysis", "PgDrillDownCollector.Plans.cs")]
    [InlineData("Lite", "Analysis", "DrillDownCollector.Plans.cs")]
    public void DrillDownCollector_ReturnsOnParseError_BeforeReadingParserExtractedContent(
        string dir1, string dir2, string file)
    {
        var path = Path.Combine(RepoRoot(), dir1, dir2, file);
        var code = File.ReadAllText(path);

        var parseIndex = code.IndexOf("ShowPlanParser.Parse(", System.StringComparison.Ordinal);
        Assert.True(parseIndex >= 0, $"{file}: expected a ShowPlanParser.Parse( call.");

        var batchesIndex = code.IndexOf("PlanStatements.EnumerateAll(plan)", parseIndex, System.StringComparison.Ordinal);
        var missingIndexesIndex = code.IndexOf("plan.AllMissingIndexes", parseIndex, System.StringComparison.Ordinal);

        Assert.True(batchesIndex >= 0, $"{file}: expected a later read of PlanStatements.EnumerateAll(plan).");
        Assert.True(missingIndexesIndex >= 0, $"{file}: expected a later read of plan.AllMissingIndexes.");

        var firstReadIndex = System.Math.Min(batchesIndex, missingIndexesIndex);

        var guardIndex = code.IndexOf(
            "if (!string.IsNullOrWhiteSpace(plan.ParseError))",
            parseIndex,
            System.StringComparison.Ordinal);

        Assert.True(
            guardIndex >= 0 && guardIndex < firstReadIndex,
            $"{file}: expected a ParseError return between ShowPlanParser.Parse( and the first read of " +
            "PlanStatements.EnumerateAll(plan) / plan.AllMissingIndexes.");
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}
