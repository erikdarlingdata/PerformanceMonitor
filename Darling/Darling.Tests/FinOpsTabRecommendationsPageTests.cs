/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>Source pins for the FinOps Recommendations tab: it reads get_finops_recommendations and shows only keys that read emits.</summary>
public sealed class FinOpsTabRecommendationsPageTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "recommendations.js")
            .ReplaceLineEndings("\n");

    private static string ToolSource() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFinOpsRecommendationsTools.cs")
            .ReplaceLineEndings("\n");

    private static string Slice(string startMarker, string endMarker)
    {
        var source = ToolSource();
        var start = source.IndexOf(startMarker, System.StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = source.IndexOf(endMarker, start, System.StringComparison.Ordinal);
        Assert.True(end > start);
        return source.Substring(start, end - start);
    }

    private static string RowSlice() => Slice("internal static object RecommendationRow(", "\n    };");

    private static string EnvelopeSlice() => Slice("internal static object Envelope(", "\n        };");

    private static System.Collections.Generic.List<string> Keys() =>
        Regex.Matches(Tab(), "\\bkey: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToList();

    [Fact]
    public void TheTabReadsTheRecommendationsRoute()
    {
        Assert.Contains("readTool(\"get_finops_recommendations\", { server }, ctx && ctx.signal)", Tab());
    }

    [Fact]
    public void EveryColumnKeyIsEmittedByTheRow()
    {
        var keys = Keys();
        var row = RowSlice();
        foreach (var key in keys)
            Assert.Matches("(?m)^\\s+" + Regex.Escape(key) + " = ", row);
        Assert.Equal(6, Regex.Matches(row, "(?m)^\\s+[a-z_]+ = ").Count);
        Assert.Equal(6, keys.Count);
    }

    [Fact]
    public void TheColumnsAreInTheDesktopGridOrder()
    {
        Assert.Equal("category,severity,confidence,finding,detail,est_savings_usd_month", string.Join(",", Keys()));
    }

    [Theory]
    [InlineData("recommendations")]
    [InlineData("monthly_cost_usd")]
    [InlineData("skipped_checks")]
    public void TheEnvelopeKeysTheTabReadsAreEmitted(string key)
    {
        Assert.Matches("(?m)^\\s+" + key + " = ", EnvelopeSlice());
        Assert.Contains("data." + key, Tab());
    }

    [Fact]
    public void SeverityColourMapsTheThreeLevels()
    {
        Assert.Contains("const SEV = { High: \"Critical\", Medium: \"Warning\", Low: \"Healthy\" };", Tab());
        Assert.Contains("severity and confidence are High, Medium or Low", ToolSource());
        Assert.Contains("{ key: \"severity\", label: \"Severity\", sevKey: \"severity_sev\" }", Tab());
    }

    [Fact]
    public void TheTextColumnsAreNotFormatted()
    {
        var tab = Tab();
        foreach (var key in new[] { "finding", "detail" })
        {
            var line = Regex.Match(tab, "(?m)^\\s+\\{ key: \"" + key + "\".*$");
            Assert.True(line.Success);
            Assert.DoesNotContain("format:", line.Value);
            Assert.Contains("wrap: true", line.Value);
        }
    }

    [Fact]
    public void TheTabKeepsThePayloadOrder()
    {
        Assert.DoesNotContain(".sort(", Tab());
    }

    [Fact]
    public void TheReadStatesAreHandled()
    {
        var tab = Tab();
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"aborted\" \\|\\| res\\.kind === \"auth\"\\) return;$", tab);
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"empty\"\\) return mount\\(body, emptyStrip\\(res\\.message\\)\\);$", tab);
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"error\"\\) return mount\\(body, readErrorStrip\\(res\\.message\\)\\);$", tab);
        Assert.Contains("Could not render this tab: ", tab);
    }

    [Fact]
    public void TheTabBuildsItsDomFromTextOnly()
    {
        Assert.DoesNotContain("innerHTML", Tab());
    }

    [Fact]
    public void TheTabImportsOnlyTheSharedHelpers()
    {
        var imports = Regex.Matches(Tab(), "from \"([^\"]+)\";").Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(imports);
        Assert.All(imports, i => Assert.Contains(i, new[] { "../../panels.js", "../../util.js" }));
    }

    [Fact]
    public void TheStubTextIsGone()
    {
        Assert.DoesNotContain("Not on the web yet", Tab());
    }

    [Fact]
    public async System.Threading.Tasks.Task TheTabParsesUnderNode()
    {
        var path = Path.Combine(Path.GetTempPath(), "recommendations-" + System.Guid.NewGuid().ToString("N") + ".mjs");
        File.WriteAllText(path, Tab());
        try
        {
            var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
            psi.ArgumentList.Add("--check");
            psi.ArgumentList.Add(path);
            Process proc;
            try
            {
                proc = Process.Start(psi)!;
            }
            catch (Win32Exception)
            {
                Assert.Skip("Node is not installed, so the tab script cannot be parsed.");
                return;
            }

            using (proc)
            {
                var error = proc.StandardError.ReadToEndAsync();
                if (!proc.WaitForExit(20000))
                {
                    proc.Kill(entireProcessTree: true);
                    Assert.Fail("node --check did not finish in 20 s");
                }

                Assert.True(proc.ExitCode == 0, "node --check failed: " + await error);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }
}
