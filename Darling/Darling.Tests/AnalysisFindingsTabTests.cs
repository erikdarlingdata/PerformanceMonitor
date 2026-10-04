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

/// <summary>
/// The web Recommendations tab (#4843), the twin of the desktop viewer's analysis findings view. The source pins
/// name the read and its params and where the tab sits in the registry; the behaviour tests run the shipped
/// <c>analysis-findings.js</c> under Node (<c>analysis-findings-harness.mjs</c>), skipped when Node is not installed.
/// </summary>
public sealed class AnalysisFindingsTabTests
{
    private static string[] Tab() => new[] { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "analysis-findings.js" };

    [Fact]
    public void TheTab_ReadsGetAnalysisFindings_WithTheServerWindowAndFullText()
    {
        var src = ReadRepoFile(Tab());
        Assert.Contains("readToolWithinKeptHistory(\"get_analysis_findings\", { server, hours: ctx.hours, limit: 50, full_text: true })", src, StringComparison.Ordinal);
        Assert.DoesNotContain("innerHTML", src, StringComparison.Ordinal);
        Assert.DoesNotContain("analyze_server", src, StringComparison.Ordinal);
    }

    [Fact]
    public void TheTab_IsRegisteredLast_InTheSqlServerRegistryOnly()
    {
        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js").ReplaceLineEndings("\n");
        Assert.Contains("import { analysisFindingsTab } from \"./analysis-findings.js\";", tabs, StringComparison.Ordinal);
        Assert.Contains("  analysisFindingsTab,\n];\n\n/* ─", tabs, StringComparison.Ordinal);
        Assert.Equal(1, tabs.Split("analysisFindingsTab,", StringSplitOptions.None).Length - 1);
    }

    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "analysis-findings-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);
        Process proc;
        try { proc = Process.Start(psi)!; }
        catch (Win32Exception) { Assert.Skip("Node is not installed, so the shipped page script cannot be run."); return default; }
        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the analysis findings harness did not finish in 20 s for scenario " + scenario);
            }
            Assert.True(proc.ExitCode == 0, "the analysis findings harness failed for " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void TheRead_GoesToTheApiWithTheServerHoursLimitAndFullText()
    {
        var r = Run("read");
        Assert.Equal("/api/read/get_analysis_findings", r.GetProperty("path").GetString());
        var p = r.GetProperty("params");
        Assert.Equal("srv-a", p.GetProperty("server").GetString());
        Assert.Equal("24", p.GetProperty("hours").GetString());
        Assert.Equal("50", p.GetProperty("limit").GetString());
        Assert.Equal("true", p.GetProperty("full_text").GetString());
    }

    [Fact]
    public void Findings_GroupByIncident_MostSevereFirst_WithBadgesFromTheExistingBandClasses()
    {
        var r = Run("grouping");
        Assert.Equal(new[] { "Major · 2 findings · CRITICAL", "Config · CRITICAL", "Solo info · INFO" }, Strings(r.GetProperty("headers")));
        Assert.Equal(new[] { 2, 1, 1 }, r.GetProperty("cardsPerGroup").EnumerateArray().Select(x => x.GetInt32()).ToArray());
        // Info-only groups start collapsed, the rest open.
        Assert.Equal(new[] { true, true, false }, r.GetProperty("open").EnumerateArray().Select(x => x.GetBoolean()).ToArray());
        var classes = Strings(r.GetProperty("badgeClasses"));
        Assert.Contains("badge band-Critical", classes);
        Assert.Contains("badge band-Warning", classes);
        Assert.Contains("badge band-Unknown", classes);
    }

    [Fact]
    public void TheFixScript_IsText_InAPre_AndCopyPutsExactlyThatTextOnTheClipboard()
    {
        var r = Run("fix");
        const string fix = "ALTER DATABASE <b>x</b> SET AUTO_SHRINK OFF;";
        Assert.Equal(fix, Assert.Single(Strings(r.GetProperty("pre"))));
        Assert.Equal(0, Assert.Single(r.GetProperty("preChildren").EnumerateArray()).GetInt32());
        Assert.Equal(fix, Assert.Single(Strings(r.GetProperty("clipboard"))));
        Assert.Equal("Copied", r.GetProperty("label").GetString());
        Assert.Equal(1, r.GetProperty("buttons").GetInt32());
    }

    [Fact]
    public void OnlyIncidentFindings_LinkToTheServersQueriesTab_NotConfigFixes()
    {
        var links = Strings(Run("link"));
        Assert.Equal(3, links.Length);
        Assert.All(links, l => Assert.Equal("#/server/srv-a/queries", l));
    }

    [Fact]
    public void OpenAndClosedGroups_SurviveARebuild_PerServer()
    {
        var r = Run("rebuild");
        Assert.Equal(new[] { false, true, true }, r.GetProperty("again").EnumerateArray().Select(x => x.GetBoolean()).ToArray());
        Assert.Equal(new[] { true, true, false }, r.GetProperty("other").EnumerateArray().Select(x => x.GetBoolean()).ToArray());
    }

    [Fact]
    public void AnEmptyRead_ShowsTheServersSentence()
    {
        Assert.Contains("No findings in the requested time range.", Run("empty").GetProperty("text").GetString(), StringComparison.Ordinal);
    }
}
