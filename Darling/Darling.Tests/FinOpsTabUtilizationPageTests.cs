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

/// <summary>Source pins for the FinOps Utilization tab's verdict, summary, trend and top-database grids: the reads it makes and the keys it shows.</summary>
public sealed class FinOpsTabUtilizationPageTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "utilization.js")
            .ReplaceLineEndings("\n");

    private static string Mcp(string name) =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", name).ReplaceLineEndings("\n");

    private static string UtilizationSource() => Mcp("DarlingMcpFinOpsTools.Utilization.cs");

    private static string DatabaseResourcesSource() => Mcp("DarlingMcpFinOpsTools.DatabaseResources.cs");

    private static string Between(string s, string from, string? to)
    {
        var a = s.IndexOf(from, System.StringComparison.Ordinal);
        Assert.True(a >= 0, from);
        var b = to is null ? s.Length : s.IndexOf(to, a, System.StringComparison.Ordinal);
        Assert.True(b > a, to);
        return s.Substring(a, b - a);
    }

    private static System.Collections.Generic.List<string> KeysIn(string block) =>
        Regex.Matches(block, "\\b(?:key|nullKey|sevKey): \"([a-z_.0-9]+)\"").Select(m => m.Groups[1].Value).ToList();

    [Fact]
    public void TheTabReadsTheUtilizationViewWithNoWindowOrLimit()
    {
        Assert.Contains("readTool(\"get_finops\", { server, view: \"utilization\" }, ctx && ctx.signal)", Tab());
    }

    [Fact]
    public void TheTabReadsDatabaseResourcesForTheDesktopsFixed24HoursAndTop5()
    {
        var tab = Tab();
        Assert.Contains("const TOP_HOURS = 24;", tab);
        Assert.Contains("const TOP_LIMIT = 5;", tab);
        Assert.Contains("view: \"database_resources\", hours: TOP_HOURS, limit: TOP_LIMIT", tab);
        Assert.DoesNotContain("hours_back", tab);
    }

    [Fact]
    public void EveryUtilizationKeyShownIsEmittedByTheUtilizationView()
    {
        var tab = Tab();
        Assert.Contains("const DERIVED_KEYS = [\"verdict_label\", \"verdict_sev\", \"day_label\", \"monthly_cost_text\", \"annual_cost_text\", \"workers_in_use\"];", tab);
        var derived = new[] { "verdict_label", "verdict_sev", "day_label", "monthly_cost_text", "annual_cost_text", "workers_in_use" };
        var block = Between(tab, "/* utilization-keys:begin */", "/* utilization-keys:end */");
        var summary = Between(block, "const SUMMARY_STATS", "const TREND_COLUMNS");
        var trendCols = Between(block, "const TREND_COLUMNS", null);
        var source = UtilizationSource();
        // Each key is checked inside the object that emits it, so a leaf name that two objects share (avg_cpu_pct in
        // cpu and in the trend, memory_ratio in memory and in the trend) cannot be satisfied by the wrong one.
        var cpu = Between(source, "cpu = new", "memory = new");
        var memory = Between(source, "memory = new", "monthly_cost_usd = ");
        var trend = source.Substring(source.IndexOf("provisioning_trend = ", System.StringComparison.Ordinal));
        var topLevel = string.Join("\n", source.Split('\n').Where(l => Regex.IsMatch(l, "^ {12}[a-z_0-9]+ = ")));
        var checkedCount = 0;
        void Emitted(string key, string slice)
        {
            Assert.Matches("(?m)^\\s+" + Regex.Escape(key) + " = ", slice);
            checkedCount++;
        }
        var summaryKeys = KeysIn(summary).Concat(
            Regex.Matches(summary, "(?:showWhen|hideWhen): \\{ key: \"([a-z_.]+)\"").Select(m => m.Groups[1].Value))
            .Where(k => !derived.Contains(k)).Distinct().ToList();
        Assert.NotEmpty(summaryKeys);
        foreach (var key in summaryKeys)
        {
            var parts = key.Split('.');
            if (parts.Length == 1) Emitted(key, topLevel);
            else if (parts[0] == "cpu") Emitted(parts[1], cpu);
            else if (parts[0] == "memory") Emitted(parts[1], memory);
            else Assert.Fail("unmapped summary key " + key);
        }
        var trendKeys = KeysIn(trendCols).Where(k => !derived.Contains(k)).Distinct().ToList();
        Assert.Equal(new[] { "avg_cpu_pct", "max_cpu_pct", "memory_ratio", "p95_cpu_pct" }, trendKeys.OrderBy(k => k, System.StringComparer.Ordinal));
        foreach (var key in trendKeys) Emitted(key, trend);
        Assert.True(checkedCount > 10);
        foreach (var key in new[] { "provisioning_trend", "verdict_reason", "health_score_note", "monthly_cost_usd", "annual_cost_usd", "current_workers", "health_band" })
        {
            Assert.Contains(key, tab);
            Assert.Matches("(?m)^\\s+" + key + " = ", source);
        }
    }

    [Fact]
    public void TheSummaryNamesItsWindowAndFormatsMaxCpuAsAnInteger()
    {
        var tab = Tab();
        Assert.Contains("const SUMMARY_SUBTITLE = \"Last 24 hours\";", tab);
        Assert.Contains("el(\"span\", { class: \"panel-sub\", text: \" \" + SUMMARY_SUBTITLE })", tab);
        Assert.Contains("{ key: \"cpu.max_cpu_pct\", label: \"Max CPU %\", format: \"int\" }", tab);
        Assert.True(tab.IndexOf("SUMMARY_SUBTITLE })", System.StringComparison.Ordinal) < tab.IndexOf("VIZ.stat(view", System.StringComparison.Ordinal));
    }

    [Fact]
    public void EveryTopDatabaseColumnIsEmittedByItsRowShape()
    {
        var tab = Tab();
        var block = Between(tab, "/* top-keys:begin */", "/* top-keys:end */");
        var total = KeysIn(Between(block, "const TOP_TOTAL_COLUMNS", "const TOP_AVG_COLUMNS"));
        var avg = KeysIn(Between(block, "const TOP_AVG_COLUMNS", null));
        Assert.NotEmpty(total);
        Assert.NotEmpty(avg);
        var src = DatabaseResourcesSource();
        var totalSlice = Between(src, "TopByTotalRow(", "TopByAvgRow(");
        var avgSlice = src.Substring(src.IndexOf("TopByAvgRow(", System.StringComparison.Ordinal));
        foreach (var k in total) Assert.Matches("(?m)^\\s+" + k + " = ", totalSlice);
        foreach (var k in avg) Assert.Matches("(?m)^\\s+" + k + " = ", avgSlice);
        Assert.Matches("(?m)^\\s+top_by_total = ", src);
        Assert.Matches("(?m)^\\s+top_by_avg = ", src);
        Assert.Contains("rowsKey: \"top_by_total\"", tab);
        Assert.Contains("rowsKey: \"top_by_avg\"", tab);
    }

    [Fact]
    public void BothReadsHandleAbortAuthErrorAndEmptyOnTheirOwnContainer()
    {
        var tab = Tab();
        Assert.Equal(2, Regex.Matches(tab, "(?m)^\\s+if \\(res\\.kind === \"aborted\" \\|\\| res\\.kind === \"auth\"\\) return;$").Count);
        Assert.Equal(2, Regex.Matches(tab, Regex.Escape("emptyStrip(res.message)")).Count);
        Assert.Equal(2, Regex.Matches(tab, Regex.Escape("readErrorStrip(res.message)")).Count);
        Assert.Equal(2, Regex.Matches(tab, Regex.Escape("\"Could not render this tab: \"")).Count);
        Assert.DoesNotContain("Promise.all", tab);
    }

    [Fact]
    public void TheVerdictAndBandAreColouredFromServiceLabels()
    {
        var tab = Tab();
        Assert.Contains("const VERDICT_SEV = { RIGHT_SIZED: \"Healthy\", OVER_PROVISIONED: \"Warning\", UNDER_PROVISIONED: \"Critical\" };", tab);
        Assert.Contains("const BAND_SEV = { good: \"Healthy\", fair: \"Warning\", poor: \"Critical\" };", tab);
        Assert.Contains("sev: BAND_SEV[data.health_band]", tab);
        Assert.Contains("\"N/A\"", tab);
        Assert.Contains("health_band = FinOpsUtilizationFigures.HealthBand(score)", UtilizationSource());
        var figures = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "FinOps", "FinOpsUtilizationFigures.cs");
        Assert.Contains("BandGood = \"good\"", figures);
        Assert.Contains("BandFair = \"fair\"", figures);
        Assert.Contains("BandPoor = \"poor\"", figures);
    }

    [Fact]
    public void CostTilesShowOnlyWhenTheServiceSetsACost()
    {
        var tab = Tab();
        Assert.Contains("data.monthly_cost_usd != null", tab);
        Assert.Contains("\"/mo\"", tab);
        Assert.Contains("\"/yr\"", tab);
        Assert.Contains("hasCost || (s.key !== \"monthly_cost_text\"", tab);
    }

    [Fact]
    public void TheTrendIsCollapsedAndHasTheDesktopColumns()
    {
        var tab = Tab();
        Assert.Contains("el(\"details\"", tab);
        Assert.DoesNotContain("el(\"details\", { open", tab);
        Assert.Contains("\"7-Day Provisioning Trend\"", tab);
        Assert.Contains("timeZone: \"UTC\"", tab);
        var cols = KeysIn(Between(tab, "const TREND_COLUMNS", "/* utilization-keys:end */")).Where(k => k != "verdict_sev").ToList();
        Assert.Equal(new[] { "day_label", "verdict_label", "avg_cpu_pct", "p95_cpu_pct", "max_cpu_pct", "memory_ratio" }, cols);
    }

    [Fact]
    public void TheTabNeverWritesMarkup()
    {
        var tab = Tab();
        foreach (var bad in new[] { "innerHTML", "insertAdjacentHTML", "html:", "outerHTML" })
            Assert.DoesNotContain(bad, tab);
    }
}
