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
using System.Net;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Source pins for the web viewer's FinOps page shell (#4843): the nav entry, the hash route, the fixed
/// registry of twelve sub-tab modules in the desktop tab strip's order, the shape of each tab module, and the
/// rule that the browser derives no band, score or percentage threshold. The repository has no JavaScript test
/// runner, so these are text scans (the <see cref="ServerPageTabsTests"/> pattern).
/// </summary>
public sealed class FinOpsPageShellTests
{
    private static readonly string[] Wwwroot =
        ["Darling", "PerformanceMonitor.Darling.Service", "wwwroot"];

    /// <summary>The tab module ids in the order of FinOpsTab.xaml's sub-tab strip.</summary>
    internal static readonly string[] TabIds =
    [
        "utilization", "database-resources", "storage-growth", "locking", "database-sizes", "version-store",
        "optimization", "high-impact", "application-connections", "server-inventory", "index-analysis",
        "recommendations",
    ];

    /// <summary>The headers of FinOpsTab.xaml's sub-tabs, in order, paired with <see cref="TabIds"/>.</summary>
    private static readonly string[] XamlHeaders =
    [
        "Utilization", "Database Resources", "Storage Growth", "Locking &amp; Contention", "Database Sizes",
        "Version Store (PVS)", "Optimization", "High Impact", "Application Connections", "Server Inventory",
        "Index Analysis", "Recommendations",
    ];

    private static string Js(params string[] rel) =>
        ReadRepoFileLf(Wwwroot.Concat(new[] { "js" }).Concat(rel).ToArray());

    private static string TabFile(string id) => Js("pages", "finops", id + ".js");

    [Fact]
    public void RenderFinops_AFailedOrEmptyRereadOnAPollNeverMountsOverThePaintedTab()
    {
        var js = Js("pages", "finops.js").ReplaceLineEndings("\n");
        Assert.Contains("    const show = (node) => {\n      /* A failed or empty re-read on a poll says nothing about the painted page: keep it and the cache. */\n      if (hadCache) return;\n      lastRows = null;\n", js);
    }

    [Fact]
    public void IndexHtml_HasTheFinOpsNavEntry()
    {
        var html = ReadRepoFileLf(Wwwroot.Concat(new[] { "index.html" }).ToArray());
        Assert.Contains("<a data-route=\"finops\" href=\"#/finops\">FinOps</a>", html);
    }

    [Fact]
    public void AppJs_ImportsRenderFinopsAndRoutesServerAndTab()
    {
        var app = Js("app.js");
        Assert.Contains("import { renderFinops } from \"./pages/finops.js\";", app);
        Assert.Contains("h.startsWith(\"#/finops/\")", app);
        var start = app.IndexOf("function finopsRoute(", StringComparison.Ordinal);
        Assert.True(start >= 0, "finopsRoute must exist");
        var end = app.IndexOf("\n}\n", start, StringComparison.Ordinal);
        var body = app.Substring(start, end - start);
        Assert.Contains("name: \"finops\"", body);
        Assert.Contains("param: safeDecode(rest.slice(0, slash))", body);
        Assert.Contains("tab: safeDecode(rest.slice(slash + 1))", body);
        Assert.DoesNotContain("decodeURIComponent(", body);
        Assert.Contains("catch {", app.Substring(app.IndexOf("function safeDecode(", StringComparison.Ordinal), 120));
        Assert.Contains("renderFinops(main, r.param, r.tab, opts)", app);
    }

    [Fact]
    public void XamlOrder_MatchesTheRegistryOrder()
    {
        var xaml = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.xaml");
        var headers = Regex.Matches(xaml, "^\\s{16}<TabItem Header=\"([^\"]+)\"", RegexOptions.Multiline)
            .Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(XamlHeaders, headers);
    }

    [Fact]
    public void FinopsJs_ImportsAllTwelveTabsInXamlOrder_AndSetsThePanelSignal()
    {
        var src = Js("pages", "finops.js");
        var imported = Regex.Matches(src, "import \\{ tab as \\w+ \\} from \"\\./finops/([a-z-]+)\\.js\";")
            .Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(TabIds, imported);
        Assert.Contains("setPanelSignal(", src);
        Assert.Contains("list_servers", src);
        Assert.Contains("finops.server", src);
        Assert.True(src.IndexOf("setPanelSignal(", StringComparison.Ordinal) < src.IndexOf(".build(", StringComparison.Ordinal),
            "the panel signal must be set before a tab is built");
    }

    [Fact]
    public void FinopsTabsArray_IsInXamlOrder_AndEachLabelMatchesItsXamlHeader()
    {
        var src = Js("pages", "finops.js");
        var m = Regex.Match(src, "FINOPS_TABS = \\[(?<ids>[^\\]]*)\\];");
        Assert.True(m.Success, "FINOPS_TABS array not found");
        var ids = m.Groups["ids"].Value.Split(',').Select(x => x.Trim()).Where(x => x.Length > 0).ToArray();
        Assert.Equal(TabIds.Select(i => i.Replace('-', '_')).ToArray(), ids);
        for (var i = 0; i < TabIds.Length; i++)
            Assert.Contains($"label: \"{WebUtility.HtmlDecode(XamlHeaders[i])}\",", TabFile(TabIds[i]));
    }

    [Fact]
    public void RenderFinops_ResolvesToTheRowKey_PollsOnlyOnOpts_AndOpensTheControllerBeforeAnyAwait()
    {
        var src = Js("pages", "finops.js");
        var start = src.IndexOf("export function renderFinops(", StringComparison.Ordinal);
        var body = src.Substring(start);
        Assert.Contains("rows.find((r) => r.display_name === wanted)", src);
        Assert.Contains("const chosen = resolveRow(rows, wanted).server_name;", body);
        Assert.DoesNotContain("known(", src);
        Assert.Contains("opts.poll === true", body);
        Assert.DoesNotContain("keepPainted", body);
        var lines = body.ReplaceLineEndings("\n").Split('\n');
        var paintIdx = Array.FindIndex(lines, l => l.Trim() == "if (lastRows !== null) paint(lastRows);");
        var fetchIdx = Array.FindIndex(lines, l => l.Contains("readTool(\"list_servers\""));
        Assert.True(paintIdx >= 0 && paintIdx < fetchIdx, "the cached paint must run before the list_servers read, on polls too (no !isPoll guard)");
        Assert.Contains("const needFetch = lastRows === null || isPoll;", body);
        Assert.Contains("if (!needFetch) return;", body);
        var ctrlIdx = Array.FindIndex(lines, l => l.Trim() == "let controller = panelAbort = new AbortController();");
        Assert.True(ctrlIdx >= 0 && ctrlIdx < Array.FindIndex(lines, l => l.Contains("await ")),
            "the controller must be created unconditionally before the first await");
        Assert.Contains("{ signal: controller.signal }", body);
        Assert.Contains("aria-current", src);
        Assert.DoesNotContain("role: \"tab", src);
        Assert.DoesNotContain("aria-selected", src);
        Assert.Contains("No servers are registered", body);
    }

    [Fact]
    public void EveryTabFile_ExportsTabWithIdLabelBuild_AndTheIdIsTheFileName()
    {
        var onDisk = Directory.GetFiles(
                Path.GetDirectoryName(PathTo(Wwwroot.Concat(new[] { "js", "pages", "finops", "utilization.js" }).ToArray()))!,
                "*.js")
            .Select(Path.GetFileNameWithoutExtension).OrderBy(x => x, StringComparer.Ordinal).ToArray();
        Assert.Equal(TabIds.OrderBy(x => x, StringComparer.Ordinal).ToArray(), onDisk);

        foreach (var id in TabIds)
        {
            var src = TabFile(id);
            Assert.Contains("export const tab = {", src);
            Assert.Contains($"id: \"{id}\",", src);
            Assert.Matches("label: \"[^\"]+\",", src);
            Assert.Matches("build\\(server, ctx\\) \\{", src);
        }
    }

    [Fact]
    public void FinOpsTabFiles_DeriveNoBandOrThresholdInTheBrowser()
    {
        var dir = Path.GetDirectoryName(PathTo(Wwwroot.Concat(new[] { "js", "pages", "finops", "utilization.js" }).ToArray()))!;
        foreach (var file in Directory.GetFiles(dir, "*.js"))
        {
            var hits = BrowserDerivedThresholds(File.ReadAllText(file).ReplaceLineEndings("\n"));
            Assert.True(hits.Count == 0, Path.GetFileName(file) + " compares a band, score or percent: " + string.Join(" | ", hits));
        }
    }

    [Theory]
    [InlineData("if (row.score > 50) {", true)]
    [InlineData("x = pct >= 0.8 ? a : b;", true)]
    [InlineData("if (10 < row.band_value)", true)]
    [InlineData("if (row.avg_cpu_percent > 80) {", true)]
    [InlineData("if (r.utilization_percent >= 90) {", true)]
    [InlineData("if (row.cpu_ratio > 0.5) {", true)]
    [InlineData("if (row.cpu_pct > CRIT) {", true)]
    [InlineData("if (row.size_gb > MAX_SIZE) {", true)]
    [InlineData("const heat = v > 0.8 ? 3 : 2;", true)]
    [InlineData("const cls = v > 0.8 ? \"heat-3\" : \"heat-1\";", true)]
    [InlineData("const label = row.band;", false)]
    [InlineData("const n = rows.length > 0 ? 1 : 2;", false)]
    [InlineData("score: row.score,", false)]
    public void ThresholdScanner_FlagsOnlyNumericComparisons(string line, bool flagged)
    {
        Assert.Equal(flagged, BrowserDerivedThresholds(line).Count > 0);
    }

    /// <summary>
    /// Lines that compare a number against a band, score or percent identifier. The browser never derives a
    /// band: the read returns it. Reused by every FinOps tab's page test.
    /// </summary>
    internal static List<string> BrowserDerivedThresholds(string source)
    {
        const string ident = "(band|score|pct|percent|ratio|util)";
        var after = new Regex("\\b\\w*" + ident + "\\w*\\b[\\w.\\[\\]()]*\\s*(<=|>=|<|>|===|!==|==|!=)\\s*-?\\d", RegexOptions.IgnoreCase);
        var before = new Regex("-?\\d[\\d.]*\\s*(<=|>=|<|>|===|!==|==|!=)\\s*[\\w.\\[\\]()]*" + ident, RegexOptions.IgnoreCase);
        var constant = new Regex("(<=|>=|<|>)\\s*[A-Z][A-Z0-9_]{2,}\\b|\\b[A-Z][A-Z0-9_]{2,}\\s*(<=|>=|<|>)");
        var heatTernary = new Regex("(<=|>=|<|>)\\s*-?\\d[\\d.]*\\s*\\?[^:]*(heat|band)|heat\\w*\\s*=[^;]*(<=|>=|<|>)\\s*-?\\d", RegexOptions.IgnoreCase);
        return source.Split('\n').Where(l => after.IsMatch(l) || before.IsMatch(l) || constant.IsMatch(l) || heatTernary.IsMatch(l)).Select(l => l.Trim()).ToList();
    }
}
