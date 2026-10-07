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
    [InlineData(0, 0.0, "no samples")]
    [InlineData(1, 0.0, "1 sample")]
    [InlineData(12, 0.5, "12 samples within a minute")]
    [InlineData(12, 45.0, "12 samples over 45 minutes")]
    [InlineData(1200, 90.0, "1,200 samples over 1 hour")]
    [InlineData(12, 47 * 60 + 30.0, "12 samples over 47 hours")]
    [InlineData(12, 48 * 60.0, "12 samples over 2 days")]
    [InlineData(12, 6 * 24 * 60 + 13 * 60.0, "12 samples over 6 days")]
    [InlineData(12, 30 * 24 * 60.0, "12 samples over 7 days")]
    [InlineData(24, 4 * 24 * 60.0, "24 samples over 4 days")]
    public void Describe_RendersTheCountAndSpan_CapsAtSevenDays_NeverTheLast(long count, double minutes, string expected)
    {
        var text = RightSizingWindow.Describe(count, TimeSpan.FromMinutes(minutes));
        Assert.Equal(expected, text);
        Assert.DoesNotContain("the last", text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("ViewerDataService.FinOps.Recommendations.cs")]
    public void ViewerRightSizing_NamesTheCoveredWindow_AndSkipsAnUnchangedSize(string file)
    {
        // The viewer keeps the reads and the window text; the thresholds and the advice wording moved to Darling.Storage.
        var raw = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);
        var storageRaw = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "FinOps", "FinOpsRecommendationFigures.cs");
        var source = CSharpSourceWalker.StripCommentsAndStrings(storageRaw);
        var readerRaw = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "FinOps", "DarlingFinOpsRecommendationsReader.cs");

        Assert.DoesNotContain("\"the last ", readerRaw, StringComparison.Ordinal);
        Assert.DoesNotContain("\"the last ", raw, StringComparison.Ordinal);
        Assert.DoesNotContain("\"the last ", storageRaw, StringComparison.Ordinal);
        Assert.Contains("From {cpuWindow}, P95 CPU utilization was", storageRaw, StringComparison.Ordinal);
        Assert.Contains("P95 SQL Server memory from {window} is", storageRaw, StringComparison.Ordinal);
        Assert.Contains("P95 SQL Server memory from {memWindow} is", storageRaw, StringComparison.Ordinal);
        Assert.DoesNotContain("memory over 7 days is {p95MemMb", storageRaw, StringComparison.Ordinal);
        Assert.DoesNotContain("memory over 7 days is {p95Mb", storageRaw, StringComparison.Ordinal);
        Assert.DoesNotContain("memory over 7 days is {p95MemMb", raw, StringComparison.Ordinal);
        Assert.DoesNotContain("memory over 7 days is {p95Mb", raw, StringComparison.Ordinal);
        Assert.DoesNotContain("memory over 7 days is {p95MemMb", readerRaw, StringComparison.Ordinal);
        Assert.DoesNotContain("memory over 7 days is {p95Mb", readerRaw, StringComparison.Ordinal);
        Assert.Contains("RightSizingWindow.Describe(", readerRaw, StringComparison.Ordinal);
        Assert.Matches(new Regex(@"targetMb\s*/\s*1024\s*<\s*physMb\s*/\s*1024"), source);
        Assert.Matches(new Regex(@"memRatio\s*<\s*0\.50m\s*&&\s*targetMb\s*/\s*1024\s*<\s*util\.PhysicalMemoryMb\s*/\s*1024"), source);
        Assert.Matches(new Regex(@"targetCores\s*>\s*0\s*&&\s*targetCores\s*<\s*cpuCount"), source);
        Assert.DoesNotMatch(new Regex(@"targetMb\s*<\s*physMb\s*&&"), source);
        Assert.Contains("under 3ms across {storageWindow}", storageRaw, StringComparison.Ordinal);
        Assert.DoesNotContain("under 3ms over 7 days", storageRaw, StringComparison.Ordinal);
        Assert.Contains("HasQueryStatsCoverageAsync(", readerRaw, StringComparison.Ordinal);
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
        Assert.Contains("AS window_samples", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Readers_TakeTheWindowFromThoseColumns_AndTheCpuFallbackStaysNeutral()
    {
        var raw = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "FinOps", "DarlingFinOpsRecommendationsReader.cs");

        Assert.Matches(new Regex(@"reader\.IsDBNull\(4\)\s*\?\s*0L\s*:\s*Convert\.ToInt64\(reader\.GetValue\(4\),\s*CultureInfo\.InvariantCulture\),\s*reader\.IsDBNull\(2\)\s*\|\|\s*reader\.IsDBNull\(3\)\s*\?\s*TimeSpan\.Zero\s*:\s*reader\.GetDateTime\(3\)\s*-\s*reader\.GetDateTime\(2\)"), raw);
        Assert.Matches(new Regex(@"cpuReader\.IsDBNull\(3\)\s*\?\s*0L\s*:\s*Convert\.ToInt64\(cpuReader\.GetValue\(3\),\s*CultureInfo\.InvariantCulture\),\s*cpuReader\.IsDBNull\(1\)\s*\|\|\s*cpuReader\.IsDBNull\(2\)\s*\?\s*TimeSpan\.Zero\s*:\s*cpuReader\.GetDateTime\(2\)\s*-\s*cpuReader\.GetDateTime\(1\)"), raw);
        Assert.Contains("var cpuWindow = \"recent samples\"", raw, StringComparison.Ordinal);
        Assert.Contains("reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7)", raw, StringComparison.Ordinal);
        var storageRaw = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "FinOps", "FinOpsRecommendationFigures.cs");
        Assert.Contains("storageSamples += row.WindowSamples", storageRaw, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStatsFirstSampleSql_IsOneScalarOverTheServersCutoffWindow()
    {
        var sql = ViewerDataService.RecommendationsQueryStatsFirstSampleSql;
        Assert.Contains("SELECT MIN(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$2", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_time >=", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStatsCoverage_IsExactlyTheSevenDayCutoff_WithNoSlack()
    {
        var raw = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "FinOps", "DarlingFinOpsRecommendationsReader.cs");
        var viewerRaw = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.FinOps.Recommendations.cs");
        Assert.Contains("firstSample <= cutoff", raw, StringComparison.Ordinal);
        Assert.DoesNotContain("TimeSpan.FromDays(1)", viewerRaw, StringComparison.Ordinal);
        Assert.DoesNotContain("TimeSpan.FromDays(1)", raw, StringComparison.Ordinal);
    }
}
