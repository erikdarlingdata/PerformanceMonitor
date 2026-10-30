/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>Source pins for the read-only Admin page: the three reads and their params, each tab's columns, the nav entry and the route.</summary>
public sealed class AdminPageTests
{
    private static string Page() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "admin.js").ReplaceLineEndings("\n");

    private static string Wwwroot(params string[] rest) =>
        ReadRepoFile(new[] { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot" }.Concat(rest).ToArray()).ReplaceLineEndings("\n");

    [Theory]
    [InlineData("get_notification_routes")]
    [InlineData("get_alert_settings")]
    public void ThePage_ReadsEachToolWithNoParams(string tool)
    {
        Assert.Contains("readTool(\"" + tool + "\", {})", Page());
    }

    [Fact]
    public void TheServersTab_ReadsTheAdminRoute_NotListServers_AndDrawsDisabledRowsGrey()
    {
        var page = Page();
        Assert.Contains("apiGet(\"/api/admin/servers\")", page);
        Assert.DoesNotContain("list_servers", page);
        Assert.Contains("rowClass: serverRowClass", page);
        Assert.Contains("row.status === \"Disabled\"", page);
    }

    [Theory]
    [InlineData("SERVER_COLUMNS", new[] { "display_name", "server_name", "auth", "engine", "version", "status", "freshness", "monthly_cost", "read_only", "added", "last_collected", "same_machine_as" })]
    [InlineData("ROUTE_COLUMNS", new[] { "route_id", "enabled", "metric_match", "match_kind", "family", "applies_to", "severities", "status", "channels", "agent", "resolutions", "smtp_recipients", "modified_at_utc" })]
    [InlineData("SETTING_COLUMNS", new[] { "setting", "value" })]
    public void EachTab_DeclaresItsColumnKeys(string name, string[] keys)
    {
        var page = Page();
        var start = page.IndexOf("export const " + name + " = [", StringComparison.Ordinal);
        Assert.True(start >= 0, name);
        var block = page.Substring(start, page.IndexOf("];", start, StringComparison.Ordinal) - start);
        var found = System.Text.RegularExpressions.Regex.Matches(block, "key: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(keys, found);
    }

    [Fact]
    public void TheRoutesTab_UsesTheDesktopViewersScopeAndStatusWords()
    {
        var page = Page();
        foreach (var (name, word) in new[]
        {
            ("ROUTE_ALL_SERVERS", ViewerNotificationRouteScope.AllServersLabel),
            ("ROUTE_STATUS_OK", ViewerNotificationRouteScope.OkStatus),
            ("ROUTE_STATUS_DISABLED", ViewerNotificationRouteScope.DisabledStatus),
            ("ROUTE_STATUS_ORPHANED_SERVER", ViewerNotificationRouteScope.OrphanedServerStatus),
            ("ROUTE_STATUS_ORPHANED_TAG", ViewerNotificationRouteScope.OrphanedTagStatus),
            ("ROUTE_STATUS_TAG_UNREADABLE", ViewerNotificationRouteScope.TagDataUnreadableStatus),
            ("ROUTE_STATUS_SCOPE_UNREADABLE", ViewerNotificationRouteScope.ScopeDataUnreadableStatus),
        })
        {
            Assert.Contains("export const " + name + " = \"" + word + "\";", page, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheServersTab_CallsItsStatusColumnStatus_AndItsCollectionAgeFreshness_AsTheDesktopDoes()
    {
        var page = Page();
        Assert.Contains("{ key: \"status\", label: \"Status\" }", page);
        Assert.Contains("{ key: \"freshness\", label: \"Freshness\"", page);
        Assert.Contains("{ key: \"auth\", label: \"Auth\" }", page);
        Assert.Contains("label: \"Monthly Cost ($)\"", page);
        Assert.Contains("{ key: \"added\", label: \"Added\", format: \"time\" }", page);
        Assert.DoesNotContain("Not shown here", page);
        Assert.Contains("label: \"Email to\"", page);
        Assert.DoesNotContain("Email recipients", page);
    }

    [Fact]
    public void TheSettingsTab_SplitsTheLongRunningQueryFilters_IntoTheirOwnSectionAfterAlertThresholds()
    {
        var page = Page();
        var thresholds = page.IndexOf("title: \"Alert thresholds\"", StringComparison.Ordinal);
        var filters = page.IndexOf("title: \"Long Running Query Filters\"", StringComparison.Ordinal);
        var bands = page.IndexOf("title: \"Health bands\"", StringComparison.Ordinal);
        Assert.True(thresholds >= 0 && filters > thresholds && bands > filters);
    }

    [Fact]
    public void EveryTopLevelKeyOfTheAlertSettingsPayload_IsOnThePage()
    {
        var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs").ReplaceLineEndings("\n");
        var start = source.IndexOf("private static object BuildAlertSettingsPayload(", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var open = source.IndexOf("=> new\n    {\n", start, StringComparison.Ordinal) + "=> new\n    {\n".Length;
        var close = source.IndexOf("\n    };", open, StringComparison.Ordinal);
        Assert.True(open > 0 && close > open);
        // Top-level members sit at exactly eight spaces; nested members and comment text sit deeper or start with a comment mark.
        var keys = System.Text.RegularExpressions.Regex.Matches(source.Substring(open, close - open), "^ {8}([a-z_]+) = ", System.Text.RegularExpressions.RegexOptions.Multiline)
            .Select(m => m.Groups[1].Value).ToArray();
        Assert.True(keys.Length >= 24, "the payload parse found only " + keys.Length + " keys");

        var page = Page();
        var start2 = page.IndexOf("export const SETTING_SECTIONS = [", StringComparison.Ordinal);
        var block = page.Substring(start2, page.IndexOf("\n];", start2, StringComparison.Ordinal) - start2);
        var shown = System.Text.RegularExpressions.Regex.Matches(block, "\"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToHashSet();
        foreach (var k in keys) Assert.True(shown.Contains(k), "the Admin page's settings sections do not list the payload key '" + k + "'");

        // The per-group allow-list covers every group the sections name.
        var gstart = page.IndexOf("export const GROUP_FIELDS = {", StringComparison.Ordinal);
        var gblock = page.Substring(gstart, page.IndexOf("\n};", gstart, StringComparison.Ordinal) - gstart);
        foreach (var g in System.Text.RegularExpressions.Regex.Matches(block, "groups: \\[([^\\]]*)\\]").SelectMany(m => System.Text.RegularExpressions.Regex.Matches(m.Groups[1].Value, "\"([a-z_]+)\"")).Select(m => m.Groups[1].Value))
            Assert.Contains("  " + g + ": [", gblock);
    }

    /// <summary>#5641: the Runnable Queue group is drawn with its three fields (the switch and the two bars) and a
    /// label, beside Poison wait in the thresholds section. get_alert_settings emits the same three keys
    /// (<c>RunnableQueue_IsReportedThroughTheClamp_AndWrittenWithTheEnginesValidation</c>).</summary>
    [Fact]
    public void ThePage_DrawsTheRunnableQueueGroup_WithItsThreeFields()
    {
        var page = Page();
        Assert.Contains("  runnable_queue: \"Runnable queue\",", page);
        Assert.Contains("  runnable_queue: [\"enabled\", \"warning_per_scheduler\", \"critical_per_scheduler\"],", page);
        var sections = page.IndexOf("export const SETTING_SECTIONS = [", StringComparison.Ordinal);
        Assert.True(page.IndexOf("\"poison_wait\", \"runnable_queue\"", sections, StringComparison.Ordinal) > sections);
    }

    /// <summary>#5640: the Client Timeouts group is drawn with its three fields (the switch and the two per-minute bars), read-only
    /// like every group, beside Runnable queue in the thresholds section. get_alert_settings emits the same three keys.</summary>
    [Fact]
    public void ThePage_DrawsTheClientTimeoutsGroup_WithItsThreeFields()
    {
        var page = Page();
        Assert.Contains("  client_timeouts: \"Client timeouts\",", page);
        Assert.Contains("  client_timeouts: [\"enabled\", \"warning_per_minute\", \"critical_per_minute\", \"warning_multiple\", \"critical_multiple\"],", page);
        var sections = page.IndexOf("export const SETTING_SECTIONS = [", StringComparison.Ordinal);
        Assert.True(page.IndexOf("\"runnable_queue\", \"client_timeouts\"", sections, StringComparison.Ordinal) > sections);
    }

    [Fact]
    public void ThePage_OffersNoWriteCall()
    {
        var page = Page();
        Assert.DoesNotContain("apiSend", page);
        Assert.DoesNotContain("innerHTML", page);
        Assert.DoesNotContain("fetch(", page);
    }

    [Fact]
    public void TheNav_HasOneAdminEntry_RightAfterAlertRules()
    {
        var html = Wwwroot("index.html");
        var rules = html.IndexOf("<a data-route=\"alert-rules\" href=\"#/alert-rules\">Alert Rules</a>", StringComparison.Ordinal);
        var admin = html.IndexOf("<a data-route=\"admin\" href=\"#/admin\">Admin</a>", StringComparison.Ordinal);
        Assert.True(rules >= 0 && admin > rules);
        Assert.StartsWith("\n          <a data-route=\"admin\"", html.Substring(html.IndexOf("</a>", rules, StringComparison.Ordinal) + 4));
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(html, "data-route=\"admin\""));
    }

    [Fact]
    public void TheRouter_RoutesAdminRightAfterTheAlertRuleRoutes_AndRenders()
    {
        var app = Wwwroot("js", "app.js");
        Assert.Contains("import { renderAdmin } from \"./pages/admin.js\";", app);
        var editor = app.IndexOf("if (h.startsWith(\"#/alert-rule/\")) return", StringComparison.Ordinal);
        var admin = app.IndexOf("if (h === \"#/admin\" || h.startsWith(\"#/admin/\")) return { name: \"admin\"", StringComparison.Ordinal);
        Assert.True(editor >= 0 && admin > editor);
        Assert.DoesNotContain("\n  if (h.startsWith", app.Substring(editor + 10, admin - editor - 10));
        Assert.Contains("else if (r.name === \"admin\") renderAdmin(main, r.tab);", app);
    }
}
