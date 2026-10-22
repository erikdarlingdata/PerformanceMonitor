/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Source-position pins for the per-database Query Store watermark cache (#4661). The cache is exact only
/// while it advances strictly AFTER the item's COPY transaction commits,.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class QueryStoreDatabaseWatermarkWiringPinTests
{
    private static string RunnerSource() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");

    /// <summary>
    /// #5514: the Azure SQL Database per-database loop took query_store's watermark straight from the store, bounded
    /// but uncached, and invalidated the cache key first, so every database paid one store read per cycle (181,050
    /// calls in 13 days on a 43-server store). It resolves through the cache like the enumerated arm, stages the
    /// batch after the fetch, lands it only after the flush returned, and drops the key when a staged batch faults.
    /// </summary>
    [Fact]
    public void TheAzureArm_ResolvesThroughTheCache_StagesBeforeTheFlush_CommitsAfterIt_AndDropsTheKeyOnAFault()
    {
        var src = RunnerSource();

        var armStart = src.IndexOf("var azureReadFloor =", StringComparison.Ordinal);
        Assert.True(armStart > 0, "the Azure arm's read floor is missing");

        /* The old shape: invalidate the key, then read the store, on every pass. */
        Assert.DoesNotContain("per-database cache; the invalidate is only defensive", src);

        var resolve = src.IndexOf("ResolveQueryStoreDatabaseWatermarkAsync(", armStart, StringComparison.Ordinal);
        var plainRead = src.IndexOf("GetLastCollectedTimeForDatabaseAsync(", armStart, StringComparison.Ordinal);
        Assert.True(resolve > armStart && plainRead > resolve,
            "the Azure arm must resolve query_store's watermark through the cache, keeping the plain read only for the collectors with no read floor");
        Assert.Contains("azureReadFloor is DateTime azureCacheFloor", src);

        var fetch = src.IndexOf("dbConnection, pgConnection, server, databaseName, definition.Name", armStart, StringComparison.Ordinal);
        var stage = src.IndexOf("stagedDatabaseWatermark = StageQueryStoreDatabaseWatermark(batch, databaseName", armStart, StringComparison.Ordinal);
        var flush = src.IndexOf("rowsWritten += await WriteBatchAsync(pgConnection, definition, batch, server, collectionTime, context", armStart, StringComparison.Ordinal);
        var commit = src.IndexOf("CommitQueryStoreDatabaseWatermark(server, databaseName, landedWatermark)", armStart, StringComparison.Ordinal);
        Assert.True(fetch > 0 && stage > fetch && flush > stage && commit > flush,
            "stage after the fetch, before the flush; commit only after the flush");

        /* Both fault arms drop the key, and only for a batch that was staged. */
        var drops = Regex.Matches(src,
            @"if \(stagedDatabaseWatermark is not null\)\s*\{\s*_databaseWatermarkCache\.Invalidate\(server\.ServerId, databaseName\);").Count;
        Assert.Equal(2, drops);
    }
}
