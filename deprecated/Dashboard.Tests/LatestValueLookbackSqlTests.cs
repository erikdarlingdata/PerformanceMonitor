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
using PerformanceMonitorDashboard.Analysis;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// #3896, the frozen Dashboard's mirror: its "latest value" reads — database size, percent autogrowth, disk
/// space, memory_stats, memory clerks and the autogrowth drill-down — take each series' newest sample within
/// its collector's lookback of the window's end (a day, or twice the collector's interval in
/// <c>config.collection_schedule</c> if that is longer), the Darling/Lite twins' bound. Unbounded, a dropped
/// database's files stayed in <c>DATABASE_TOTAL_SIZE_MB</c> (and its ALTER DATABASE in the autogrowth
/// drill-down) for the whole collection retention. The reads are pinned on the source text, the only idiom
/// this project has for its SQL-Server-side reads — there is no test database — and the bounds' arithmetic
/// directly.
/// </summary>
public class LatestValueLookbackSqlTests
{
    [Theory]
    [InlineData("SqlServerFactCollector.Storage.cs", "CollectDatabaseSizeFactAsync", "FileIoStats")]
    [InlineData("SqlServerFactCollector.Storage.cs", "CollectFileAutogrowthFactsAsync", "DatabaseSizeStats")]
    [InlineData("SqlServerFactCollector.Storage.cs", "CollectDiskSpaceFactsAsync", "DatabaseSizeStats")]
    [InlineData("SqlServerFactCollector.Resources.cs", "CollectMemoryFactsAsync", "MemoryStats")]
    [InlineData("SqlServerFactCollector.Resources.cs", "CollectMemoryClerkFactsAsync", "MemoryClerks")]
    [InlineData("SqlServerDrillDownCollector.Storage.cs", "CollectAutogrowthPercentFiles", "DatabaseSizeStats")]
    public void EachLatestValueRead_IsBoundedBelowByItsCollectorsLookback_AndAboveByTheWindowEnd(string file, string method, string collector)
    {
        var body = MethodBody(Read("deprecated", "Dashboard", "Analysis", file), method);

        Assert.Contains("collection_time >= @lookbackStart", body);
        Assert.Contains("collection_time <= @endTime", body);
        Assert.Contains($"new SqlParameter(\"@lookbackStart\", context.LatestValueStartFor(SqlServerLatestValueBounds.{collector}))", body);
        Assert.Contains("new SqlParameter(\"@endTime\", context.TimeRangeEnd)", body);
    }

    /// <summary>The bounds are stamped before the first read, and the drill-down stamps a context it is handed
    /// bare — the pass's own context arrives stamped, so the list and the count share one set.</summary>
    [Fact]
    public void TheBounds_AreStampedBeforeAnyRead_AndByTheDrillDown()
    {
        var collect = Read("deprecated", "Dashboard", "Analysis", "SqlServerFactCollector.cs");
        var stamp = collect.IndexOf("await SqlServerLatestValueBounds.EnsureAsync(_connectionString, context);", StringComparison.Ordinal);
        var firstRead = collect.IndexOf("await Collect", StringComparison.Ordinal);
        Assert.True(stamp >= 0 && stamp < firstRead, "the bounds must be stamped before the first read");

        var drill = MethodBody(Read("deprecated", "Dashboard", "Analysis", "SqlServerDrillDownCollector.Storage.cs"), "CollectAutogrowthPercentFiles");
        Assert.Contains("await SqlServerLatestValueBounds.EnsureAsync(_connectionString, context);", drill);
    }

    /// <summary>
    /// A day, or twice the collector's interval if that is longer — the daily collector keeps its fact for two
    /// days instead of flapping between runs, and every default cadence keeps the flat day. Frequency 0 on the
    /// Dashboard is "every master-collector tick", not on-load, so it takes the floor; so does a collector the
    /// schedule has no row for.
    /// </summary>
    [Fact]
    public void TheBounds_AreADayOrTwiceTheScheduledInterval()
    {
        var end = new DateTime(2026, 3, 1, 12, 0, 0);
        var starts = SqlServerLatestValueBounds.Starts(end, new Dictionary<string, int>
        {
            ["database_size_stats_collector"] = 1440,
            ["file_io_stats_collector"] = 1,
            ["memory_clerks_stats_collector"] = 0,
        });

        Assert.Equal(end.AddHours(-48), starts[SqlServerLatestValueBounds.DatabaseSizeStats]);
        Assert.Equal(end.AddHours(-24), starts[SqlServerLatestValueBounds.FileIoStats]);
        Assert.Equal(end.AddHours(-24), starts[SqlServerLatestValueBounds.MemoryClerks]);
        Assert.Equal(end.AddHours(-24), starts[SqlServerLatestValueBounds.MemoryStats]);
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
