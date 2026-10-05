/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
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
    [InlineData("SERVER_COLUMNS", new[] { "display_name", "server_name", "auth", "engine", "version", "status", "freshness", "monthly_cost", "read_only", "added", "last_collected" })]
    [InlineData("ROUTE_COLUMNS", new[] { "route_id", "enabled", "metric_match", "match_kind", "family", "channels", "smtp_recipients", "modified_at_utc" })]
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

/// <summary>What the Admin page draws, from the shipped <c>admin.js</c> run under Node: no credential-shaped field reaches any text.</summary>
public sealed class AdminPageBehaviourTests
{
    private static string[] Texts(string scenario, out JsonElement root)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "admin-page-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "admin.js"));
        psi.ArgumentList.Add(scenario);
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            root = default;
            return Array.Empty<string>();
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the admin harness did not finish for " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the admin harness failed for " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            root = doc.RootElement.Clone();
            return root.GetProperty("texts").EnumerateArray().Select(e => e.GetString()!).ToArray();
        }
    }

    [Theory]
    [InlineData("servers", "Alpha", "/api/admin/servers")]
    [InlineData("routes", "Blocking Detected", "get_notification_routes")]
    [InlineData("settings", "cpu.threshold_percent", "get_alert_settings")]
    public void EachTab_ReadsItsTool_AndDrawsTheRow(string tab, string expectedText, string tool)
    {
        var texts = Texts(tab, out var root);
        Assert.Contains(texts, t => t == expectedText);
        Assert.Equal(tool, Assert.Single(root.GetProperty("reads").EnumerateArray()).GetProperty("tool").GetString());
    }

    [Fact]
    public void TheServersTab_DrawsTheDisabledServer_WithItsAuthCostAndAddedTime_AndGreysOnlyTheDisabledRow()
    {
        var texts = Texts("servers", out var root);
        Assert.Contains("Enabled", texts);
        Assert.Contains("Disabled", texts);
        Assert.Contains("Windows", texts);
        Assert.Contains("SQL Server", texts);
        Assert.Contains("$1,234", texts);
        Assert.Contains("2026-01-02T00:00:00.0000000", texts);
        Assert.Contains("rowclass:band-Offline", texts);
        Assert.Contains("rowclass:", texts);
        Assert.Equal(1, texts.Count(t => t == "rowclass:band-Offline"));
        Assert.Contains("2 servers.", string.Join(" ", texts));
        Assert.Equal("/api/admin/servers", Assert.Single(root.GetProperty("reads").EnumerateArray()).GetProperty("tool").GetString());
    }

    [Theory]
    [InlineData("servers")]
    [InlineData("routes")]
    [InlineData("settings")]
    public void NoRenderedText_CarriesASecretShapedField(string tab)
    {
        var texts = Texts(tab, out _);
        Assert.NotEmpty(texts);
        Assert.DoesNotContain(texts, t => t.Contains("SECRET", StringComparison.Ordinal));
        Assert.DoesNotContain(texts, t => t.Contains("hooks.example.test", StringComparison.Ordinal));
    }

    [Fact]
    public void TheSettingsTab_DrawsOnlyTheFieldsItListsForAGroup_AndNeverTheStoresSecretNames()
    {
        var texts = Texts("settings", out _);
        Assert.Contains("cpu.threshold_percent", texts);
        Assert.DoesNotContain(texts, t => t.Contains("UNLISTED", StringComparison.Ordinal));
        Assert.DoesNotContain(texts, t => t.Contains("future_knob", StringComparison.Ordinal));
        Assert.DoesNotContain(texts, t => t.Contains("slack_url", StringComparison.Ordinal) || t.Contains("pagerduty_routing_key", StringComparison.Ordinal));
    }

    [Fact]
    public void TheLongRunningQueryFilters_AreTheirOwnSection()
    {
        var texts = Texts("settings", out _);
        Assert.Contains("Alert thresholds", texts);
        Assert.Contains("Long Running Query Filters", texts);
        Assert.Contains("long_running_query.exclude_backups", texts);
        Assert.Contains("long_running_query.threshold_minutes", texts);
        Assert.True(Array.IndexOf(texts, "Long Running Query Filters") < Array.IndexOf(texts, "long_running_query.exclude_backups"));
        Assert.True(Array.IndexOf(texts, "long_running_query.threshold_minutes") < Array.IndexOf(texts, "Long Running Query Filters"));
    }

    [Theory]
    [InlineData("repaint-settings")]
    [InlineData("repaint-servers")]
    public void ARepaintOfTheTabOnScreen_KeepsItsBody_UntilTheReadLands(string scenario)
    {
        Texts(scenario, out var root);
        var repaint = root.GetProperty("repaint");
        Assert.True(repaint.GetProperty("sameBody").GetBoolean());
        Assert.False(repaint.GetProperty("loadingShown").GetBoolean());
        Assert.True(repaint.GetProperty("rowsKept").GetBoolean());
    }

    [Fact]
    public void TheRoutesTab_ShowsChannelNames_ButNeverADestination()
    {
        var texts = Texts("routes", out _);
        Assert.Contains("slack, pagerduty", texts);
        Assert.Contains("ops@example.test", texts);
    }
}
