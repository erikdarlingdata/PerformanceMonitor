/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>Source pins for the FinOps Database Resources tab: it reads get_finops with view database_resources and shows only columns that read emits.</summary>
public sealed class FinOpsTabDatabaseResourcesPageTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "database-resources.js")
            .ReplaceLineEndings("\n");

    private static string ToolSource() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFinOpsTools.DatabaseResources.cs")
            .ReplaceLineEndings("\n");

    [Fact]
    public void TheTabReadsGetFinOpsWithTheDatabaseResourcesViewWindow()
    {
        // The exact call is a no-noise guard: a limit would only cap the top lists, which this tab does not show.
        const string call = "readTool(\"get_finops\", { server, view: \"database_resources\", hours: HOURS }, ctx && ctx.signal)";
        var tab = Tab();
        Assert.Contains(call, tab);
    }

    [Fact]
    public void TheWindowIs24HoursAndThereIsNoLimitConstant()
    {
        var tab = Tab();
        Assert.Matches("(?m)^const HOURS = 24;$", tab);
        Assert.DoesNotContain("LIMIT", tab);
    }

    private static string RowSlice()
    {
        var source = ToolSource();
        var start = source.IndexOf("internal static object DatabaseResourcesRow(", System.StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = source.IndexOf("\n    };", start, System.StringComparison.Ordinal);
        Assert.True(end > start);
        return source.Substring(start, end - start);
    }

    [Fact]
    public void EveryShownColumnKeyIsEmittedByTheDatabaseResourcesRow()
    {
        var keys = Regex.Matches(Tab(), "\\bkey: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToList();
        var row = RowSlice();
        foreach (var key in keys)
            Assert.Matches("(?m)^\\s+" + Regex.Escape(key) + " = ", row);
        var emitted = Regex.Matches(row, "(?m)^\\s+[a-z_]+ = ").Count;
        Assert.Equal(11, emitted);
        Assert.Equal(emitted, keys.Count);
    }

    [Fact]
    public void TheColumnsAreInTheDesktopGridOrder()
    {
        var keys = Regex.Matches(Tab(), "\\bkey: \"([a-z_]+)\"").Select(m => m.Groups[1].Value);
        Assert.Equal(
            "database_name,cpu_time_ms,cpu_share_pct,logical_reads,physical_reads,logical_writes,execution_count,io_read_mb,io_write_mb,io_share_pct,io_stall_ms",
            string.Join(",", keys));
        Assert.Contains("{ key: \"io_stall_ms\", label: \"I/O stall\", format: \"ms\" }", Tab());
    }

    [Fact]
    public void TheNoticeCountsTheRowsReturnedAndTakesTheWindowFromTheAnswer()
    {
        var tab = Tab();
        Assert.Contains("(data.rows || []).length", tab);
        Assert.Contains("(n === 1 ? \"1 database\" : n + \" databases\") + \", last \" + (data.hours_back ?? HOURS) + \" hours\";", tab);
    }

    [Fact]
    public void TheTruncatedNoticeNamesTheShownCountThenTheTotal()
    {
        Assert.Contains(
            "text += data.truncated ? \"; the top \" + n + \" of \" + data.database_count + \" databases by CPU, then I/O.\" : \".\";",
            Tab());
    }

    [Fact]
    public void TheReadStatesAreHandledAbortedAndAuthReturnAndEmptyShowsItsMessage()
    {
        var tab = Tab();
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"aborted\" \\|\\| res\\.kind === \"auth\"\\) return;$", tab);
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"empty\"\\) return mount\\(body, emptyStrip\\(res\\.message\\)\\);$", tab);
    }

    [Fact]
    public void TheTabImportsOnlyTheSharedHelpers()
    {
        var imports = Regex.Matches(Tab(), "from \"([^\"]+)\";").Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(imports);
        Assert.All(imports, i => Assert.Contains(i, new[] { "../../panels.js", "../../charts.js", "../../util.js" }));
    }

    [Fact]
    public void TheStubTextIsGone()
    {
        Assert.DoesNotContain("Not on the web yet", Tab());
    }

    [Fact]
    public void TheErrorAndRenderFailureStatesAreHandled()
    {
        var tab = Tab();
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"error\"\\) return mount\\(body, readErrorStrip\\(res\\.message\\)\\);$", tab);
        Assert.Contains("Could not render this tab: ", tab);
    }
}
