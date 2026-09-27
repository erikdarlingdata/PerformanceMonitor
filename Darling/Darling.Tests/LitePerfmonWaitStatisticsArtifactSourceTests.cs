/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Lite's twins of the #4476 one-sample Wait Statistics artifact exclusion, checked without a live DuckDB or a
/// project reference to Lite: a source-text scan of the two SQL-builder files, confirming both embed
/// <see cref="PerformanceMonitor.Common.WaitStatisticsArtifact.ArtifactPredicateSql"/>'s own emitted fragments
/// (never a second copy of the 1,000,000 / 1,000 literals). Runs on macOS. The wiring itself is proven by
/// Lite.Tests' Windows-only fact plus a local DuckDB check recorded in the PR body — this pin only proves the
/// text is present, not that DuckDB accepts it.
/// </summary>
public sealed class LitePerfmonWaitStatisticsArtifactSourceTests
{
    [Fact]
    public void PerfmonTrendsSql_SourceEmbedsTheArtifactPredicateAndTheSuffixMatch()
    {
        var text = ReadRepoFile("Lite/Services/LocalDataService.Perfmon.cs");

        Assert.Contains("WaitStatisticsArtifact.ArtifactPredicateSql(", text, StringComparison.Ordinal);
        Assert.Contains("lag(cntr_value) OVER w AS prev_value", text, StringComparison.Ordinal);
        Assert.Contains("lead(cntr_value) OVER w AS next_value", text, StringComparison.Ordinal);
        Assert.Contains("artifacts_set_aside", text, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE NOT is_artifact)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void PerfmonBucketsSql_SourceEmbedsTheArtifactPredicateAndTheSuffixMatch()
    {
        var text = ReadRepoFile("Lite/Services/LocalDataService.TrendBuckets.cs");

        Assert.Contains("WaitStatisticsArtifact.ArtifactPredicateSql(", text, StringComparison.Ordinal);
        Assert.Contains("WaitStatisticsArtifact.ObjectNameSuffixMatchSql(", text, StringComparison.Ordinal);
        Assert.Contains("artifacts_set_aside", text, StringComparison.Ordinal);
        Assert.Contains("PerfmonBucketsResult", text, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE NOT is_artifact)", text, StringComparison.Ordinal);
    }

    /// <summary>Walks up from this test assembly's build output to the repo root (marked by <c>.git</c>) and
    /// resolves <paramref name="relativePath"/> from there — no ProjectReference to Lite needed for a pure
    /// text scan.</summary>
    private static string ReadRepoFile(string relativePath)
    {
        var probe = Path.GetDirectoryName(typeof(LitePerfmonWaitStatisticsArtifactSourceTests).Assembly.Location);
        while (probe is not null)
        {
            var gitMarker = Path.Combine(probe, ".git");
            if (Directory.Exists(gitMarker) || File.Exists(gitMarker))
            {
                var candidate = Path.Combine(probe, relativePath);
                if (!File.Exists(candidate))
                {
                    throw new FileNotFoundException($"Found repo root at {probe} but not {relativePath}", candidate);
                }

                return File.ReadAllText(candidate);
            }

            probe = Path.GetDirectoryName(probe);
        }

        throw new DirectoryNotFoundException("Could not locate repo root (.git) above the test assembly.");
    }
}
