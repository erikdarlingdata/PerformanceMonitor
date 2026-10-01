/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The FinOps right-sizing advice states the window its samples cover (<see cref="RightSizingWindow"/>) and gives no
/// "reduce from X to X" advice. The helper's edges are exercised directly; the Viewer's rules run over the Postgres
/// store, so their text and equal-size guards are pinned on the rule source.
/// </summary>
public sealed class RightSizingWindowTests
{
    [Theory]
    [InlineData(0, "the last minute")]
    [InlineData(1, "the last minute")]
    [InlineData(45, "the last 45 minutes")]
    [InlineData(59, "the last 59 minutes")]
    [InlineData(60, "the last hour")]
    [InlineData(120, "the last 2 hours")]
    [InlineData(125, "the last 2 hours")]
    [InlineData(47 * 60, "the last 47 hours")]
    [InlineData(48 * 60, "the last 2 days")]
    [InlineData(3 * 24 * 60, "the last 3 days")]
    [InlineData(6 * 24 * 60 + 13 * 60, "the last 7 days")]
    [InlineData(7 * 24 * 60, "the last 7 days")]
    [InlineData(30 * 24 * 60, "the last 7 days")]
    public void Describe_RendersTheCoverageAndCapsAtSevenDays(int minutes, string expected)
    {
        Assert.Equal(expected, RightSizingWindow.Describe(TimeSpan.FromMinutes(minutes)));
    }

    [Theory]
    [InlineData("ViewerDataService.FinOps.Recommendations.cs")]
    public void ViewerRightSizing_NamesTheCoveredWindow_AndSkipsAnUnchangedSize(string file)
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file));
        var raw = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

        Assert.DoesNotContain("Over the last 7 days, P95 CPU", raw, StringComparison.Ordinal);
        Assert.DoesNotContain("memory over 7 days is {p95MemMb", raw, StringComparison.Ordinal);
        Assert.DoesNotContain("memory over 7 days is {p95Mb", raw, StringComparison.Ordinal);
        Assert.Contains("RightSizingWindow.Describe(", raw, StringComparison.Ordinal);
        Assert.Matches(new Regex(@"targetMb\s*/\s*1024\s*<\s*physMb\s*/\s*1024"), source);
        Assert.Matches(new Regex(@"targetCores\s*<\s*cpuCount"), source);
    }
}
