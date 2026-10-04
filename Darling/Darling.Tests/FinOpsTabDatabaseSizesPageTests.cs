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

/// <summary>Source pins for the FinOps Database Sizes tab: it reads the get_finops database_sizes view and shows only columns that view emits.</summary>
public sealed class FinOpsTabDatabaseSizesPageTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "database-sizes.js")
            .ReplaceLineEndings("\n");

    private static string ViewSource() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFinOpsTools.DatabaseSizes.cs")
            .ReplaceLineEndings("\n");

    [Fact]
    public void TheTabReadsTheDatabaseSizesViewOfGetFinOpsForTheServer()
    {
        var tab = Tab();
        Assert.Contains("readTool(\"get_finops\", { server, view: \"database_sizes\" }, ctx && ctx.signal)", tab);
        Assert.DoesNotContain("get_database_sizes", tab);
        Assert.Contains("DatabaseSizesView = \"database_sizes\"", ViewSource());
    }

    [Fact]
    public void EveryShownColumnKeyIsEmittedByTheViewRow()
    {
        var keys = Regex.Matches(Tab(), "\\bkey: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(keys);
        var source = ViewSource();
        var rowSlice = source[source.IndexOf("DatabaseSizesRow(DatabaseSizeFileDto", StringComparison.Ordinal)..];
        var rowNoteKey = Regex.Match(
            ReadRepoFile("PerformanceMonitor.Common", "AzureSiblingDatabaseSize.cs"),
            "const string RowNoteKey = \"([a-z_]+)\"").Groups[1].Value;
        Assert.NotEmpty(rowNoteKey);
        foreach (var key in keys)
        {
            if (key == rowNoteKey)
                Assert.Contains("[AzureSiblingDatabaseSize.RowNoteKey]", rowSlice);
            else
                Assert.Contains("[\"" + key + "\"]", rowSlice);
        }
        foreach (var added in new[] { "auto_growth_mb", "free_space_mb", "used_pct", "recovery_model", "vlf_count", "monthly_cost_usd" })
            Assert.Contains(added, keys);
    }

    [Fact]
    public void AnAbortedReadReturnsBeforeAnythingIsMounted()
    {
        var tab = Tab();
        Assert.Contains("if (res.kind === \"aborted\" || res.kind === \"auth\") return;", tab);
    }

    [Fact]
    public void AMaxSizeOfMinusOneShowsUnlimitedAndTheCaptureTimeIsFormatted()
    {
        var tab = Tab();
        Assert.Contains("row.max_size_mb === -1 ? \"Unlimited\"", tab);
        Assert.Contains("\"Disabled\"", tab);
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
