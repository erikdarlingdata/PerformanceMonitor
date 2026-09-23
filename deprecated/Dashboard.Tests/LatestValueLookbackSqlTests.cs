/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// #3896, the frozen Dashboard's mirror: its "latest value" reads — database size, percent autogrowth, disk
/// space, memory_stats, memory clerks and the autogrowth drill-down — take each series' newest sample within
/// <c>AnalysisContext.LatestValueLookback</c> of the window's end, the Darling/Lite twins' bound. Unbounded, a
/// dropped database's files stayed in <c>DATABASE_TOTAL_SIZE_MB</c> (and its ALTER DATABASE in the autogrowth
/// drill-down) for the whole collection retention. Pinned on the source text, the only idiom this project has
/// for its SQL-Server-side reads — there is no test database.
/// </summary>
public class LatestValueLookbackSqlTests
{
    [Theory]
    [InlineData("SqlServerFactCollector.Storage.cs", "CollectDatabaseSizeFactAsync")]
    [InlineData("SqlServerFactCollector.Storage.cs", "CollectFileAutogrowthFactsAsync")]
    [InlineData("SqlServerFactCollector.Storage.cs", "CollectDiskSpaceFactsAsync")]
    [InlineData("SqlServerFactCollector.Resources.cs", "CollectMemoryFactsAsync")]
    [InlineData("SqlServerFactCollector.Resources.cs", "CollectMemoryClerkFactsAsync")]
    [InlineData("SqlServerDrillDownCollector.Storage.cs", "CollectAutogrowthPercentFiles")]
    public void EachLatestValueRead_IsBoundedBelowByTheLookback_AndAboveByTheWindowEnd(string file, string method)
    {
        var body = MethodBody(Read("deprecated", "Dashboard", "Analysis", file), method);

        Assert.Contains("collection_time >= @lookbackStart", body);
        Assert.Contains("collection_time <= @endTime", body);
        Assert.Contains("new SqlParameter(\"@lookbackStart\", context.LatestValueStart)", body);
        Assert.Contains("new SqlParameter(\"@endTime\", context.TimeRangeEnd)", body);
    }

    /// <summary>The fact and the drill-down that lists its files must agree on WHICH files, or the list names
    /// a database the count never counted.</summary>
    [Fact]
    public void TheAutogrowthFact_AndItsDrillDown_ShareOneBound()
    {
        var fact = MethodBody(Read("deprecated", "Dashboard", "Analysis", "SqlServerFactCollector.Storage.cs"), "CollectFileAutogrowthFactsAsync");
        var drill = MethodBody(Read("deprecated", "Dashboard", "Analysis", "SqlServerDrillDownCollector.Storage.cs"), "CollectAutogrowthPercentFiles");

        const string Bound = "    WHERE database_name NOT IN ('master', 'msdb', 'model', 'tempdb')\r\n    AND   collection_time >= @lookbackStart\r\n    AND   collection_time <= @endTime";
        Assert.Contains(Normalize(Bound), Normalize(fact));
        Assert.Contains(Normalize(Bound), Normalize(drill));
    }

    /// <summary>The method's text from its signature to the next member's signature (or the end of the file).</summary>
    private static string MethodBody(string src, string method)
    {
        var start = src.IndexOf($"private async Task {method}(", StringComparison.Ordinal);
        Assert.True(start >= 0, $"method not found: {method}");
        var next = src.IndexOf("private async Task ", start + method.Length, StringComparison.Ordinal);
        return next < 0 ? src[start..] : src[start..next];
    }

    private static string Normalize(string text) => text.Replace("\r\n", "\n", StringComparison.Ordinal);

    private static string Read(params string[] pathParts)
    {
        var root = FindRepoRoot();
        Assert.True(root is not null, $"Could not locate the repo root (a directory containing deprecated/Dashboard/Dashboard.csproj) by walking up from {AppContext.BaseDirectory}.");
        var path = Path.Combine(new[] { root! }.Concat(pathParts).ToArray());
        Assert.True(File.Exists(path), $"Source file not found: {path}");
        return File.ReadAllText(path);
    }

    /// <summary>Walks up from the test output directory to the repo root — the directory holding the Dashboard project. Mirrors DashboardMirrorPinTests.</summary>
    private static string? FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        for (int i = 0; i < 10 && dir is not null; i++)
        {
            if (File.Exists(Path.Combine(dir.FullName, "deprecated", "Dashboard", "Dashboard.csproj")))
            {
                return dir.FullName;
            }
            dir = dir.Parent;
        }
        return null;
    }
}
