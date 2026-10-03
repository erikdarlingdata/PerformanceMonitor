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

/// <summary>Source pins for the FinOps Database Sizes tab: it reads get_database_sizes and shows only columns that read emits.</summary>
public sealed class FinOpsTabDatabaseSizesPageTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "database-sizes.js")
            .ReplaceLineEndings("\n");

    private static string ToolSource() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpObjectStatsTools.cs")
            .ReplaceLineEndings("\n");

    [Fact]
    public void TheTabReadsGetDatabaseSizesForTheServer()
    {
        Assert.Contains("readTool(\"get_database_sizes\", { server }", Tab());
    }

    [Fact]
    public void EveryShownColumnKeyIsEmittedByTheTool()
    {
        var keys = Regex.Matches(Tab(), "\\{ key: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(keys);
        var source = ToolSource();
        foreach (var key in keys)
            Assert.Contains("\"" + key + "\"", source);
    }

    [Fact]
    public void TheTabImportsOnlyTheSharedHelpers()
    {
        var imports = Regex.Matches(Tab(), "from \"([^\"]+)\";").Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(imports);
        Assert.All(imports, i => Assert.Contains(i, new[] { "../../panels.js", "../../charts.js", "../../util.js" }));
    }

    [Fact]
    public void TheTabIsNoLongerTheStub()
    {
        Assert.DoesNotContain("Not on the web yet", Tab());
    }
}
