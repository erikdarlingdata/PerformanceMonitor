/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>#5234: source pins for the Query Store grid's History column.</summary>
public sealed class WebQueryStoreHistoryColumnTests
{
    private static string Js(params string[] path) =>
        ReadRepoFile(["Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", .. path]).ReplaceLineEndings("\n");

    [Fact]
    public void TheQueryStoreTable_EndsWithThePlanAndHistoryColumns()
    {
        var tab = Js("pages", "server-tabs.js");
        Assert.Contains("import { queryStoreHistoryColumn } from \"./query-store-history.js\";", tab, StringComparison.Ordinal);
        Assert.Contains("[...QUERY_STORE_COLUMNS, queryStorePlanColumn(server), queryStoreHistoryColumn(server, ctx.hours)],", tab, StringComparison.Ordinal);
    }

    [Fact]
    public void TheHistoryColumn_IsNeverSortedFilteredExportedOrCopied()
    {
        var src = Js("pages", "query-store-history.js");
        var factory = src[src.IndexOf("export function queryStoreHistoryColumn", StringComparison.Ordinal)..];
        foreach (var flag in new[] { "sortable", "filter", "csv", "copy" })
            Assert.Matches(new Regex($@"\b{flag}: false,"), factory);
        Assert.Contains("key: \"query_store_history\"", factory, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePanel_DrawsEveryStringThroughEl_AndReadsOnlyTheStore()
    {
        var src = Js("pages", "query-store-history.js");
        Assert.DoesNotContain("innerHTML", src, StringComparison.Ordinal);
        Assert.DoesNotContain("html:", src, StringComparison.Ordinal);
        Assert.Contains("readTool(\"get_query_store_query_history\"", src, StringComparison.Ordinal);
        Assert.DoesNotContain("actual", src.Replace("actual_", ""), StringComparison.OrdinalIgnoreCase);
    }
}
