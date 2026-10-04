/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Source pins for the web viewer's FinOps Version Store (PVS) tab: it reads get_pvs_stats, shows only keys the
/// tool emits, imports only the shared renderers, and no longer carries the placeholder text.
/// </summary>
public sealed class FinOpsTabPvsPageTests
{
    private static string Tab => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "version-store.js").ReplaceLineEndings("\n");

    private static string Tool => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpPvsTools.cs").ReplaceLineEndings("\n");

    [Fact]
    public void Tab_ReadsGetPvsStatsWithATrendWindow()
    {
        Assert.Contains("readTool(\"get_pvs_stats\", { server, trend_hours_back: TREND_HOURS }, signal)", Tab);
        Assert.Contains("const TREND_HOURS = 24;", Tab);
    }

    [Fact]
    public void Tab_EveryShownKeyIsEmittedByTheTool()
    {
        var tool = Tool;
        var columnKeys = Regex.Matches(Tab, "key: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToList();
        Assert.True(columnKeys.Count >= 13, "expected the column keys to be found");
        var keys = columnKeys
            .Concat(new[] { "databases", "trend", "points", "collection_time", "pvs_size_mb", "as_of", "database_name", "pct_of_database_reason" })
            .Distinct().ToList();
        foreach (var key in keys)
        {
            Assert.True(tool.Contains(key + " = ") || tool.Contains("\"" + key + "\"") || tool.Contains(key + ","),
                "get_pvs_stats does not emit " + key);
        }
    }

    [Fact]
    public void Tab_ImportsOnlyTheSharedRenderers()
    {
        var imports = Regex.Matches(Tab, "from \"([^\"]+)\"").Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(imports);
        Assert.All(imports, i => Assert.Contains(i, new[] { "../../panels.js", "../../charts.js", "../../util.js" }));
    }

    [Fact]
    public void Tab_IsNoLongerThePlaceholder()
    {
        Assert.DoesNotContain("Not on the web yet", Tab);
    }

    [Fact]
    public void Tab_ShowsAbortedLagCleanupAndTheThreeSkippedCountersInTheDesktopOrder()
    {
        var order = new[]
        {
            "key: \"oldest_aborted_transaction_id\"",
            "key: \"aborted_transaction_lag\", label: \"Aborted Lag\"",
            "key: \"cleanup_state\", label: \"Cleanup\"",
            "key: \"skipped_low_water_mark\", label: \"Skipped: Secondary\"",
            "key: \"skipped_min_useful_xts\", label: \"Skipped: Snapshot\"",
            "key: \"skipped_oldest_aborted\", label: \"Skipped: Aborted\"",
        };
        var at = -1;
        foreach (var pin in order)
        {
            var next = Tab.IndexOf(pin, System.StringComparison.Ordinal);
            Assert.True(next > at, "column missing or out of order: " + pin);
            at = next;
        }

        foreach (var key in new[] { "aborted_transaction_lag", "cleanup_state", "skipped_low_water_mark", "skipped_min_useful_xts", "skipped_oldest_aborted" })
        {
            Assert.Contains(key + " = ", Tool);
        }
    }
}
