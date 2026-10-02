/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Runtime.CompilerServices;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4198: the web viewer's <c>/api/read</c> row for <c>get_collection_log</c> must keep passing today's
/// behavior explicitly now that the MCP tool previews <c>error_message</c> by default — the same "pass the
/// old default through the row" contract #3897's trend tools pin for <c>TrendBudget.Chart</c>. No rig: a
/// source-text pin, like <c>PgCappedReadSurfaceTests</c>.
/// </summary>
public sealed class CollectionLogWebRowPreservesFullTextPinTests
{
    private const string WebEndpoints =
        "Darling/PerformanceMonitor.Darling.Service/DarlingWebEndpoints.cs";

    [Fact]
    public void GetCollectionLog_WebRow_PassesFullTextTrue_AndTheOldRowLimit()
    {
        var path = Path.Combine(RepoRoot(), WebEndpoints);
        Assert.True(File.Exists(path), $"web endpoints source not found: {path}");
        var source = File.ReadAllText(path);

        var at = source.IndexOf("[\"get_collection_log\"] = (c, pg, an) =>", System.StringComparison.Ordinal);
        Assert.True(at >= 0, "get_collection_log's /api/read row was not found — has it been renamed or moved?");

        var stop = source.IndexOf("[\"get_current_waits_trend\"]", at, System.StringComparison.Ordinal);
        Assert.True(stop > at, "could not bound get_collection_log's row against its next sibling");
        var row = source[at..stop];

        Assert.Contains("full_text: true", row);
        Assert.Contains("Rows(c, \"limit\", 200)", row);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (!File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir)!;
        }
        return dir;
    }
}
