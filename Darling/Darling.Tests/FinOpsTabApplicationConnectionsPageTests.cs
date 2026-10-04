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

/// <summary>Source pins for the FinOps Application Connections tab: it reads get_finops with view application_connections and shows only columns that read emits.</summary>
public sealed class FinOpsTabApplicationConnectionsPageTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "application-connections.js")
            .ReplaceLineEndings("\n");

    private static string ToolSource() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFinOpsTools.ApplicationConnections.cs")
            .ReplaceLineEndings("\n");

    [Fact]
    public void TheTabReadsGetFinOpsWithTheApplicationConnectionsViewWindow()
    {
        // Pins the exact read: the view refuses a non-default limit, so sending one would turn every load into a refusal.
        const string call = "readTool(\"get_finops\", { server, view: \"application_connections\", hours: HOURS }, ctx && ctx.signal)";
        var tab = Tab();
        Assert.Contains(call, tab);
    }

    [Fact]
    public void TheTabHasNoWindowPickerAndSaysItIs24HoursLikeTheDesktopHeader()
    {
        var tab = Tab();
        Assert.Contains("el(\"h3\", { text: \"Application Connections (24h)\" })", tab);
        Assert.DoesNotContain("<select", tab);
        Assert.DoesNotContain("\"select\"", tab);
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
        var start = source.IndexOf("internal static object ApplicationConnectionsRow(", System.StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = source.IndexOf("\n    };", start, System.StringComparison.Ordinal);
        Assert.True(end > start);
        return source.Substring(start, end - start);
    }

    [Fact]
    public void EveryShownColumnKeyIsEmittedByTheApplicationConnectionsRow()
    {
        var keys = Regex.Matches(Tab(), "\\bkey: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToList();
        var row = RowSlice();
        foreach (var key in keys)
            Assert.Matches("(?m)^\\s+" + Regex.Escape(key) + " = ", row);
        var emitted = Regex.Matches(row, "(?m)^\\s+[a-z_]+ = ").Count;
        Assert.Equal(20, emitted);
        Assert.Equal(emitted, keys.Count);
    }

    [Fact]
    public void TheColumnsAreInTheDesktopGridOrder()
    {
        // Source of the order: the Application Connections grid in Darling/PerformanceMonitor.Darling.Viewer/FinOpsTab.xaml; this is that grid's column order.
        var keys = Regex.Matches(Tab(), "\\bkey: \"([a-z_]+)\"").Select(m => m.Groups[1].Value);
        Assert.Equal(
            "application_name,avg_connections,max_connections,avg_running,max_running,avg_sleeping,max_sleeping,avg_dormant,max_dormant,avg_cpu_time_ms,max_cpu_time_ms,avg_reads,max_reads,avg_writes,max_writes,avg_logical_reads,max_logical_reads,sample_count,first_seen_utc,last_seen_utc",
            string.Join(",", keys));
        Assert.Contains("{ key: \"avg_cpu_time_ms\", label: \"Avg CPU\", format: \"ms\" }", Tab());
        Assert.Contains("{ key: \"max_cpu_time_ms\", label: \"Max CPU\", format: \"ms\" }", Tab());
        Assert.Contains("{ key: \"first_seen_utc\", label: \"First seen\", format: \"time\" }", Tab());
        Assert.Contains("{ key: \"last_seen_utc\", label: \"Last seen\", format: \"time\" }", Tab());
    }

    [Fact]
    public void TheNoticeCountsTheRowsReturnedAndTakesTheWindowFromTheAnswer()
    {
        var tab = Tab();
        Assert.Contains("(data.rows || []).length", tab);
        Assert.Contains("(n === 1 ? \"1 application\" : n + \" applications\") + \", last \" + (data.hours_back ?? HOURS) + \" hours\";", tab);
    }

    [Fact]
    public void TheTruncatedNoticeNamesTheShownCountThenTheTotal()
    {
        Assert.Contains(
            "text += data.truncated ? \"; the top \" + n + \" of \" + (data.application_count ?? \"more\") + \" applications by peak connections.\" : \".\";",
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
