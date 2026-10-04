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
    [InlineData("list_servers")]
    [InlineData("get_notification_routes")]
    [InlineData("get_alert_settings")]
    public void ThePage_ReadsEachToolWithNoParams(string tool)
    {
        Assert.Contains("readTool(\"" + tool + "\", {})", Page());
    }

    [Theory]
    [InlineData("SERVER_COLUMNS", new[] { "display_name", "server_name", "engine_kind", "engine_version", "status", "read_only", "last_collection" })]
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
    [InlineData("servers", "Alpha", "list_servers")]
    [InlineData("routes", "Blocking Detected", "get_notification_routes")]
    [InlineData("settings", "cpu.threshold_percent", "get_alert_settings")]
    public void EachTab_ReadsItsTool_AndDrawsTheRow(string tab, string expectedText, string tool)
    {
        var texts = Texts(tab, out var root);
        Assert.Contains(texts, t => t == expectedText);
        Assert.Equal(tool, Assert.Single(root.GetProperty("reads").EnumerateArray()).GetProperty("tool").GetString());
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
    public void TheRoutesTab_ShowsChannelNames_ButNeverADestination()
    {
        var texts = Texts("routes", out _);
        Assert.Contains("slack, pagerduty", texts);
        Assert.Contains("ops@example.test", texts);
    }
}
