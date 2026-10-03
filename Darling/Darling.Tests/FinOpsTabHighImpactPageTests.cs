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

/// <summary>Source pins for the FinOps High Impact tab: it reads get_finops with view high_impact and shows only columns that read emits.</summary>
public sealed class FinOpsTabHighImpactPageTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "high-impact.js")
            .ReplaceLineEndings("\n");

    private static string ToolSource() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFinOpsTools.HighImpact.cs")
            .ReplaceLineEndings("\n");

    [Fact]
    public void TheTabReadsGetFinOpsWithTheHighImpactViewWindowAndLimit()
    {
        Assert.Contains("readTool(\"get_finops\", { server, view: \"high_impact\", hours: HOURS, limit: LIMIT }, ctx && ctx.signal)", Tab());
    }

    [Fact]
    public void EveryShownColumnKeyIsEmittedByTheTool()
    {
        var keys = Regex.Matches(Tab(), "\\bkey: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(keys);
        var source = ToolSource();
        foreach (var key in keys)
            Assert.Matches("(?m)^\\s+" + Regex.Escape(key) + " = ", source);
    }

    [Fact]
    public void TheNoticeCountsTheRowsReturnedAndTakesTheWindowFromTheAnswer()
    {
        var tab = Tab();
        Assert.DoesNotContain("\"Top \" + LIMIT", tab);
        Assert.Contains("queries in the top ", tab);
        Assert.Contains("data.hours_back", tab);
        Assert.Contains("(data.rows || []).length", tab);
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
    public void TheTabNeverComparesTheImpactScoreWithANumber()
    {
        Assert.False(Regex.IsMatch(Tab(), "impact_score\\s*(>=|<=|>|<)"), "impact_score is compared in the browser");
    }
}
