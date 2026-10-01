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
using PerformanceMonitor.Darling.Viewer;
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
    [InlineData(90, "the last hour")]
    [InlineData(47 * 60 + 30, "the last 47 hours")]
    [InlineData(47 * 60, "the last 47 hours")]
    [InlineData(48 * 60, "the last 2 days")]
    [InlineData(3 * 24 * 60, "the last 3 days")]
    [InlineData(6 * 24 * 60 + 13 * 60, "the last 6 days")]
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
        Assert.Matches(new Regex(@"memRatio\s*<\s*0\.50m\s*&&\s*targetMb\s*/\s*1024\s*<\s*util\.PhysicalMemoryMb\s*/\s*1024"), source);
        Assert.Matches(new Regex(@"targetCores\s*>\s*0\s*&&\s*targetCores\s*<\s*cpuCount"), source);
        Assert.DoesNotMatch(new Regex(@"targetMb\s*<\s*physMb\s*&&"), source);
        Assert.Contains("over {storageWindow}", raw, StringComparison.Ordinal);
        Assert.DoesNotContain("under 3ms over 7 days", raw, StringComparison.Ordinal);
        Assert.Contains("HasQueryStatsCoverageAsync(", raw, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(ViewerDataService.RecommendationsCpuP95Sql))]
    [InlineData(nameof(ViewerDataService.RecommendationsMemoryP95Sql))]
    [InlineData(nameof(ViewerDataService.RecommendationsStorageTierSql))]
    public void WindowedReads_SelectTheSamplesOwnFirstAndLastTime(string sqlName)
    {
        var sql = (string)typeof(ViewerDataService).GetField(sqlName)!.GetValue(null)!;
        Assert.Contains("MIN(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Readers_TakeTheWindowFromThoseColumns_AndTheCpuFallbackStaysNeutral()
    {
        var raw = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.FinOps.Recommendations.cs");

        Assert.Matches(new Regex(@"reader\.IsDBNull\(2\)\s*\|\|\s*reader\.IsDBNull\(3\)\s*\?\s*TimeSpan\.Zero\s*:\s*reader\.GetDateTime\(3\)\s*-\s*reader\.GetDateTime\(2\)"), raw);
        Assert.Matches(new Regex(@"cpuReader\.IsDBNull\(1\)\s*\|\|\s*cpuReader\.IsDBNull\(2\)\s*\?\s*TimeSpan\.Zero\s*:\s*cpuReader\.GetDateTime\(2\)\s*-\s*cpuReader\.GetDateTime\(1\)"), raw);
        Assert.Contains("var cpuWindow = \"recent samples\"", raw, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStatsFirstSampleSql_IsOneScalarOverTheServersCutoffWindow()
    {
        var sql = ViewerDataService.RecommendationsQueryStatsFirstSampleSql;
        Assert.Contains("SELECT MIN(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.Contains("$2", sql, StringComparison.Ordinal);
    }
}
