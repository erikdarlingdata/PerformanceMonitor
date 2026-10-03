/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
        var keys = Regex.Matches(Tab(), "\\bkey: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(keys);
        var source = ToolSource();
        var fileSlice = source[source.IndexOf("FilePayload(DarlingObjectStatsReader.DatabaseSizeRow", StringComparison.Ordinal)..];
        var rowNoteKey = Regex.Match(
            ReadRepoFile("PerformanceMonitor.Common", "AzureSiblingDatabaseSize.cs"),
            "const string RowNoteKey = \"([a-z_]+)\"").Groups[1].Value;
        Assert.NotEmpty(rowNoteKey);
        foreach (var key in keys)
        {
            if (key == "database_name")
                Assert.Contains("\"database_name\"", source[source.IndexOf("DatabaseSizesPayload(string", StringComparison.Ordinal)..]);
            else if (key == rowNoteKey)
                Assert.Contains("[AzureSiblingDatabaseSize.RowNoteKey]", fileSlice);
            else
                Assert.Contains("\"" + key + "\"", fileSlice);
        }
        Assert.Contains(rowNoteKey, keys);
    }

    [Fact]
    public void AnAbortedReadReturnsBeforeAnythingIsMounted()
    {
        var tab = Tab();
        Assert.Contains("readTool(\"get_database_sizes\", { server }, ctx && ctx.signal)", tab);
        Assert.Contains("if (res.kind === \"aborted\" || res.kind === \"auth\") return;", tab);
    }

    [Fact]
    public void AMaxSizeOfMinusOneShowsUnlimitedAndTheCaptureTimeIsFormatted()
    {
        var tab = Tab();
        Assert.Contains("row.max_size_mb === -1 ? \"Unlimited\"", tab);
        Assert.DoesNotContain("auto_growth_mb", tab);
        Assert.Contains("relTime(data.captured_at)", tab);
        Assert.Contains("localTime(data.captured_at)", tab);
        Assert.Contains("e?.name !== \"AbortError\"", tab);
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
