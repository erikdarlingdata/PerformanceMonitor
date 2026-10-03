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

/// <summary>Source pins for the FinOps Database Resources tab: it reads get_finops with view database_resources and shows only columns that read emits.</summary>
public sealed class FinOpsTabDatabaseResourcesPageTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "database-resources.js")
            .ReplaceLineEndings("\n");

    private static string ToolSource() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFinOpsTools.DatabaseResources.cs")
            .ReplaceLineEndings("\n");

    [Fact]
    public void TheTabReadsGetFinOpsWithTheDatabaseResourcesViewWindowAndLimit()
    {
        Assert.Contains("readTool(\"get_finops\", { server, view: \"database_resources\", hours: HOURS, limit: LIMIT }, ctx && ctx.signal)", Tab());
    }

    [Fact]
    public void TheWindowIs24HoursAndTheLimitIsTheToolMaximumOf50()
    {
        var tab = Tab();
        Assert.Matches("(?m)^const HOURS = 24;$", tab);
        Assert.Matches("(?m)^const LIMIT = 50;$", tab);
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
        Assert.Contains("(data.rows || []).length", tab);
        Assert.Contains("data.hours_back", tab);
        Assert.Contains("\" databases\"", tab);
        Assert.Contains("\"1 database\"", tab);
    }

    [Fact]
    public void TheNoticeReadsTheTruncatedFlagAndTheDatabaseCount()
    {
        var tab = Tab();
        Assert.Contains("data.truncated", tab);
        Assert.Contains("data.database_count", tab);
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
    public void TheErrorAndRenderFailureStatesAreHandled()
    {
        var tab = Tab();
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"error\"\\) return mount\\(body, readErrorStrip\\(res\\.message\\)\\);$", tab);
        Assert.Contains("Could not render this tab: ", tab);
    }
}
