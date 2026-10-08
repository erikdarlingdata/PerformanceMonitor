/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Source pins for the web PostgreSQL Activity tab's statement-eviction tile: it reads the same <c>get_pg_top_queries</c>
/// answer as the Top Query Shapes grid (one fetch), and every field it shows is one the tool emits under <c>evictions</c>.
/// </summary>
public sealed class WebPgStatementEvictionTilePageTests
{
    private static string Tabs => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js").ReplaceLineEndings("\n");

    private static string Tool => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpPgStatementTools.cs").ReplaceLineEndings("\n");

    [Fact]
    public void TheTile_ShowsOnlyFieldsTheToolEmitsUnderEvictions()
    {
        var stats = Regex.Match(Tabs, @"const PG_EVICTION_STATS = \[(.*?)\];", RegexOptions.Singleline);
        Assert.True(stats.Success, "PG_EVICTION_STATS is missing");
        var keys = Regex.Matches(stats.Groups[1].Value, "key: \"evictions\\.([a-z_]+)\"");
        Assert.Equal(2, keys.Count);

        var build = Tool.Substring(Tool.IndexOf("internal static object BuildEvictions", StringComparison.Ordinal));
        foreach (Match key in keys)
        {
            Assert.Contains(key.Groups[1].Value + " = ", build, StringComparison.Ordinal);
        }

        Assert.Contains("evictions = BuildEvictions(", Tool, StringComparison.Ordinal);
        Assert.Contains("eviction_passes_in_window", stats.Groups[1].Value, StringComparison.Ordinal);
        Assert.Contains("max_entries", stats.Groups[1].Value, StringComparison.Ordinal);
    }

    [Fact]
    public void TheTile_AndTheGrid_ShareOneRead_AndTheGridKeepsItsCaveatNote()
    {
        var region = Tabs.Substring(Tabs.IndexOf("...fanout(\"get_pg_top_queries\"", StringComparison.Ordinal));
        region = region.Substring(0, region.IndexOf("]),", StringComparison.Ordinal));
        Assert.Contains("stats: PG_EVICTION_STATS", region, StringComparison.Ordinal);
        Assert.Contains("columns: PG_TOP_QUERY_COLUMNS", region, StringComparison.Ordinal);
        Assert.Contains("noteKey: \"evictions.note\"", region, StringComparison.Ordinal);
        Assert.Contains("limit: 20", region, StringComparison.Ordinal);
        Assert.DoesNotContain("\"get_pg_top_queries\",\n        { server", Tabs, StringComparison.Ordinal);
    }
}
