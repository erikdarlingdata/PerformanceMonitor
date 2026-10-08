/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Source pins for the web Blocking tab's grids: each column key is a field its read returns, the Blocking grid shows the
/// desktop grid's columns in its order under its headers, and the two XML grids carry a Save XML column.
/// </summary>
public sealed class WebBlockingColumnsPageTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js")
            .ReplaceLineEndings("\n");

    private static string Source() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpBlockingTools.cs").ReplaceLineEndings("\n");

    private static string ConstBody(string constName)
    {
        var tab = Tab();
        var start = tab.IndexOf("const " + constName + " = [", StringComparison.Ordinal);
        Assert.True(start >= 0, constName + " not found");
        var end = tab.IndexOf("\n];", start, StringComparison.Ordinal);
        return tab[start..end];
    }

    private static List<string> Keys(string constName) =>
        Regex.Matches(ConstBody(constName), "\\bkey: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToList();

    private static List<string> Labels(string constName) =>
        Regex.Matches(ConstBody(constName), "\\blabel: \"([^\"]+)\"").Select(m => m.Groups[1].Value).ToList();

    private static List<string> Projection(string tool)
    {
        var src = Source();
        var start = src.IndexOf("Name = \"" + tool + "\"", StringComparison.Ordinal);
        Assert.True(start >= 0, tool + " not found");
        var next = src.IndexOf("McpServerTool(Name", start + 10, StringComparison.Ordinal);
        var seg = next > 0 ? src[start..next] : src[start..];
        var sel = seg.IndexOf(".Select(", StringComparison.Ordinal);
        Assert.True(sel >= 0, tool + " has no row projection");
        var close = seg.IndexOf("\n            });", sel, StringComparison.Ordinal);
        if (close < 0) close = seg.IndexOf("\n                })", sel, StringComparison.Ordinal);
        return Regex.Matches(seg[sel..close], "^\\s+([a-z_]+) = ", RegexOptions.Multiline).Select(m => m.Groups[1].Value).ToList();
    }

    [Theory]
    [InlineData("BLOCKING_COLUMNS", "get_blocking")]
    [InlineData("BPR_COLUMNS", "get_blocked_process_xml")]
    public void EveryColumnKeyIsAFieldTheReadReturns(string constName, string tool)
    {
        var keys = Keys(constName);
        var fields = Projection(tool);
        Assert.NotEmpty(fields);
        // The Save XML button column has no field of its own: its key is the XML field plus "_save".
        var own = keys.Where(k => !k.EndsWith("_save", StringComparison.Ordinal)).ToList();
        Assert.Empty(own.Except(fields));
        Assert.Equal(keys.Count, keys.Distinct().Count());
    }

    [Fact]
    public void TheBlockingGridShowsTheDesktopColumnsInTheDesktopOrderUnderTheDesktopHeaders()
    {
        Assert.Equal(new[]
        {
            "event_time", "blocked_sql_text", "blocking_sql_text", "source", "database_name", "blocked_spid", "blocking_spid",
            "wait_time_ms", "wait_resource", "lock_mode", "blocked_status", "blocking_status", "blocked_isolation_level",
            "blocking_isolation_level", "blocked_transaction_name", "blocked_priority", "blocked_transaction_count",
            "blocked_log_used", "blocked_login_name", "blocked_host_name", "blocked_client_app", "blocking_login_name",
            "blocking_host_name", "blocking_client_app", "contentious_object",
        }, Keys("BLOCKING_COLUMNS"));
        Assert.Equal(new[]
        {
            "Event Time", "Blocked SQL", "Blocking SQL", "Source", "Database", "Blocked SPID", "Blocking SPID", "Wait Time",
            "Wait Resource", "Lock Mode", "Blocked Status", "Blocking Status", "Blocked Isolation", "Blocking Isolation",
            "Blocked Tran", "Blocked Priority", "Blocked Tran Count", "Blocked Log Used", "Blocked Login", "Blocked Host",
            "Blocked App", "Blocking Login", "Blocking Host", "Blocking App", "Object",
        }, Labels("BLOCKING_COLUMNS"));
    }

    [Fact]
    public void TheBlockedProcessReportGridHasTheDesktopColumnsTheReadReturns_AndASaveXmlColumn()
    {
        Assert.Equal(
            new[] { "event_time", "blocked_process_report_xml_save", "database_name", "blocked_spid", "blocking_spid", "wait_time_ms", "blocked_process_report_xml" },
            Keys("BPR_COLUMNS"));
        Assert.Equal(new[] { "Event Time", "XML", "Database", "Blocked SPID", "Blocking SPID", "Wait Time", "Report" }, Labels("BPR_COLUMNS"));
    }

    [Fact]
    public void TheWideBlockingGridUsesColumnGroups_AndEveryGroupedColumnNamesAKnownGroup()
    {
        var tab = Tab();
        Assert.Contains("BLOCKING_GROUPS", tab);
        var groups = Regex.Match(tab, "const BLOCKING_GROUPS = \\{ groups: \\[([^\\]]+)\\]").Groups[1].Value;
        var named = Regex.Matches(groups, "\"([^\"]+)\"").Select(m => m.Groups[1].Value).ToHashSet();
        var used = Regex.Matches(ConstBody("BLOCKING_COLUMNS"), "group: \"([^\"]+)\"").Select(m => m.Groups[1].Value).ToHashSet();
        Assert.NotEmpty(used);
        Assert.Empty(used.Except(named));
    }

    [Fact]
    public void TheDeadlockGraphDownloadsAsXdl_AndTheReportAsXml_ThroughTheSharedDownloadHelper()
    {
        var tab = Tab();
        Assert.Contains("import { downloadText } from \"../grid-tools.js\";", tab);
        Assert.Contains("\".xdl\"", tab);
        Assert.Contains("\"blocked_process_\" + fileStamp(r.event_time) + \".xml\"", tab);
        Assert.Contains("\"deadlock_\" + fileStamp(r.deadlock_time) + \".xdl\"", tab);
        Assert.Contains("r.deadlock_graph_xml_truncated === true", tab);
        Assert.Contains("deadlock_graph_xml_truncated", Source());
    }
}
