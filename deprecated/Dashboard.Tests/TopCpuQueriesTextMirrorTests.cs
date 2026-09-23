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
using System.Text.RegularExpressions;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// #3959, the frozen Dashboard's mirror of the Darling/Lite top-CPU drill-down fix. The read took
/// <c>MAX(query_text)</c> over every window row's compressed <c>varbinary(max)</c> statement to print five
/// groups. It now ranks and cuts to five without the text and reads the text for those five, the same
/// <c>MAX</c> over the same rows.
///
/// <para>Pinned on the source text, the only idiom this project has for its SQL-Server-side reads (there is no
/// test database). The old and new SQL were also run side by side on a scratch database on SQL Server 2016
/// (compatibility level 130, the oldest the product supports) and returned identical rows, NULL
/// <c>query_hash</c> groups included.</para>
/// </summary>
public class TopCpuQueriesTextMirrorTests
{
    [Fact]
    public void TheTopCpuDrillDown_RanksWithoutTheText_AndReadsItForThePrintedGroups()
    {
        var sql = Regex.Replace(
            MethodBody(Read("SqlServerDrillDownCollector.Queries.cs"), "CollectTopCpuQueries"),
            @"--[^\r\n]*",
            string.Empty).Replace("\r\n", "\n", StringComparison.Ordinal);

        var ranking = sql.IndexOf("WITH top_queries AS", StringComparison.Ordinal);
        var rankingEnd = sql.IndexOf("\n)\nSELECT", ranking + 1, StringComparison.Ordinal);
        var apply = sql.IndexOf("OUTER APPLY", StringComparison.Ordinal);
        Assert.True(ranking >= 0 && rankingEnd > ranking && apply > rankingEnd, "the ranking CTE should precede the text read");

        /* The ranking and cut carry no text... */
        Assert.DoesNotContain("query_text", sql[ranking..rankingEnd], StringComparison.Ordinal);
        Assert.Contains("SELECT TOP (5)", sql[ranking..rankingEnd], StringComparison.Ordinal);

        /* ...the text is the same MAX, read per printed group under the window's own filter, NULL-safe on the
           nullable hash (IS NOT DISTINCT FROM needs SQL Server 2022). */
        var text = sql[apply..];
        Assert.Contains("SELECT MAX(qs.query_text) AS query_text", text, StringComparison.Ordinal);
        Assert.Contains("qs.collection_time >= @startTime AND qs.collection_time <= @endTime", text, StringComparison.Ordinal);
        Assert.Contains("qs.total_worker_time_delta > 0", text, StringComparison.Ordinal);
        Assert.Contains("(qs.query_hash = t.query_hash OR (qs.query_hash IS NULL AND t.query_hash IS NULL))", text, StringComparison.Ordinal);
        Assert.Contains("LEFT(CAST(DECOMPRESS(x.query_text) AS NVARCHAR(MAX)), 500) AS query_text", sql, StringComparison.Ordinal);
    }

    /// <summary>The method's text from its signature to the next member's signature (or the end of the file).</summary>
    private static string MethodBody(string src, string method)
    {
        var start = src.IndexOf($"private async Task {method}(", StringComparison.Ordinal);
        Assert.True(start >= 0, $"method not found: {method}");
        var next = src.IndexOf("private async Task ", start + method.Length, StringComparison.Ordinal);
        return next < 0 ? src[start..] : src[start..next];
    }

    private static string Read(string file)
    {
        var root = FindRepoRoot();
        Assert.True(root is not null, $"Could not locate the repo root (a directory containing deprecated/Dashboard/Dashboard.csproj) by walking up from {AppContext.BaseDirectory}.");
        var path = Path.Combine(root!, "deprecated", "Dashboard", "Analysis", file);
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
