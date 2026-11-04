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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// An analysis finding's category is the source of its root fact, an internal word such as <c>bad_actor</c> (#7486). Text a person
/// reads shows a plain name instead; the raw value stays in stored rows, the MCP reply (beside <c>category_name</c>) and alert-route
/// matching. These cover the
/// shared map, the desktop viewer's finding row, and the web viewer's grid under Node (<c>web-finding-category-harness.mjs</c>,
/// skipped when Node is not installed).
/// </summary>
public sealed class FindingCategoryNamesTests
{
    [Theory]
    [InlineData("bad_actor", "Expensive query")]
    [InlineData("database_config", "Database settings")]
    [InlineData("pg_vacuum", "PostgreSQL vacuum")]
    public void DisplayName_GivesTheKnownCategoryItsPlainName(string raw, string expected) =>
        Assert.Equal(expected, FindingCategoryNames.DisplayName(raw));

    [Fact]
    public void DisplayName_ShowsAnUnknownCategoryAsItIs_AndNeverBlanksAKnownOne()
    {
        Assert.Equal("weird_new_source", FindingCategoryNames.DisplayName("weird_new_source"));
        Assert.Equal("", FindingCategoryNames.DisplayName(""));
        Assert.Equal("", FindingCategoryNames.DisplayName(null));
    }

    [Fact]
    public void EveryFactSource_HasAPlainName_ThatIsNotItsRawWord()
    {
        foreach (var source in FactScorer.KnownSources.Append("absolution"))
        {
            var name = FindingCategoryNames.DisplayName(source);
            Assert.False(string.IsNullOrWhiteSpace(name), source + " has no display name");
            Assert.NotEqual(source, name);
            Assert.DoesNotContain('_', name);
        }
    }

    [Fact]
    public void TheDesktopViewerRow_ShowsThePlainName_AndKeepsTheRawFindingBehindIt()
    {
        var finding = new AnalysisFinding { Category = "bad_actor", RootFactKey = "BAD_ACTOR_CPU" };
        var rows = ViewerDataService.MapFindings(new[] { finding });
        var row = Assert.Single(rows);
        Assert.Equal("Expensive query", row.Category);
        Assert.Equal("bad_actor", row.Finding.Category);
    }

    private static JsonElement RunGridHarness()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-finding-category-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the finding category harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the finding category harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void TheWebGrid_ShowsThePlainName_AndAnUnknownCategoryAsItIs()
    {
        var root = RunGridHarness();
        var cells = root.GetProperty("cells").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Equal(new[] { "Expensive query", "Database settings", "weird_new_source" }, cells);
    }

    /// <summary>The alert notebook's "Finding evidence" table draws from <c>READ_FIELDS.get_analysis_findings</c>, not the grid's columns.</summary>
    [Fact]
    public void TheNotebooksEvidenceTable_ShowsThePlainName()
    {
        var cells = RunGridHarness().GetProperty("evidence").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Equal(new[] { "Expensive query", "Database settings" }, cells);
    }
}
