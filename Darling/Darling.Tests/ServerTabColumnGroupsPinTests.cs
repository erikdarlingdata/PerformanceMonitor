/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The two System Events grids with the most columns on the web dashboard use the shared column groups (#4843): every
/// group named in a grid's group list marks at least one column, every column group is named in the list, and the
/// default groups are among the listed ones.
/// </summary>
public sealed class ServerTabColumnGroupsPinTests
{
    private static string Js() => File.ReadAllText(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"));

    [Theory]
    [InlineData("MEMORY_CONDITION_COLUMNS", "MEMORY_CONDITION_GROUPS")]
    [InlineData("MEMORY_OOM_COLUMNS", "MEMORY_OOM_GROUPS")]
    public void TheWideGrid_GroupsItsColumns_AndItsGroupListMatches(string columns, string groups)
    {
        var js = Js();
        var cols = Regex.Match(js, "const " + columns + @" = \[(.*?)\n\];", RegexOptions.Singleline).Groups[1].Value;
        var used = Regex.Matches(cols, "group: \"([^\"]+)\"").Select(m => m.Groups[1].Value).Distinct().ToArray();
        var decl = Regex.Match(js, "const " + groups + @" = \{ groups: \[([^\]]*)\], defaultGroups: \[([^\]]*)\] \};");
        Assert.True(decl.Success, groups + " must declare groups and defaultGroups.");
        var listed = Regex.Matches(decl.Groups[1].Value, "\"([^\"]+)\"").Select(m => m.Groups[1].Value).ToArray();
        var defaults = Regex.Matches(decl.Groups[2].Value, "\"([^\"]+)\"").Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(listed.OrderBy(x => x), used.OrderBy(x => x));
        Assert.NotEmpty(defaults);
        Assert.All(defaults, d => Assert.Contains(d, listed));
        Assert.Contains("group: ", cols);
        Assert.Contains(", " + groups, js);
    }
}
