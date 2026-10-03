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

/// <summary>Source pins for the FinOps Locking &amp; Contention tab (#4843): it reads get_object_locking and
/// shows only keys that read emits.</summary>
public sealed class FinOpsTabLockingPageTests
{
    private static string Tab() => ReadRepoFileLf(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "locking.js");

    private static string ToolSource() => ReadRepoFileLf(
        "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpObjectStatsTools.cs");

    [Fact]
    public void Tab_CallsGetObjectLockingWithTheDesktopRowLimit()
    {
        var js = Tab();
        Assert.Contains("read: \"get_object_locking\"", js);
        Assert.Contains("params: { server, limit: 200 }", js);
        Assert.Contains("rowsKey: \"objects\"", js);
    }

    [Fact]
    public void Tab_EveryColumnKeyIsEmittedByTheRead()
    {
        var js = Tab();
        var keys = Regex.Matches(js, "\\{ key: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToList();
        Assert.Equal(14, keys.Count);
        var src = ToolSource();
        foreach (var k in keys)
            Assert.Contains("                " + k + " = r.", src);
    }

    [Fact]
    public void Tab_ImportsOnlyFromTheSharedModules()
    {
        var imports = Regex.Matches(Tab(), "from \"([^\"]+)\"").Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(imports);
        Assert.All(imports, i => Assert.Contains(i, new[] { "../../panels.js", "../../charts.js", "../../util.js" }));
    }

    [Fact]
    public void Tab_IsNoLongerTheShellStub() =>
        Assert.DoesNotContain("Not on the web yet", Tab());
}
