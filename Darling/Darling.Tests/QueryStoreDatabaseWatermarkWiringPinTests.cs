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
/// The writer set is pinned in <see cref="QueryStoreWriterSetPinTests"/>.
/// </summary>
public sealed class QueryStoreDatabaseWatermarkWiringPinTests
{
    private static string RunnerSource() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");

    [Fact]
    public void TheCommit_IsCalledOnce_AndOnlyFromOnItemComplete_AndTheStageOnlyFromReadItem()
    {
        var src = RunnerSource();

        var calls = Regex.Matches(src, @"CommitQueryStoreDatabaseWatermark\(").Count;
        /* one declaration + one call */
        Assert.Equal(2, calls);

        var call = src.IndexOf("CommitQueryStoreDatabaseWatermark(server, item", StringComparison.Ordinal);
        Assert.True(call > 0, "the commit call is missing");

        var driver = src.IndexOf("EnumeratedCollectorDriver.RunAsync<TRow>(", StringComparison.Ordinal);
        var readItem = src.IndexOf("readItem:", driver, StringComparison.Ordinal);
        var writeBatch = src.IndexOf("writeBatch:", driver, StringComparison.Ordinal);
        var onComplete = src.IndexOf("onItemComplete:", driver, StringComparison.Ordinal);
        var onError = src.IndexOf("onItemError:", driver, StringComparison.Ordinal);
        Assert.True(driver > 0 && readItem > driver && writeBatch > readItem && onComplete > writeBatch && onError > onComplete);

        Assert.InRange(call, onComplete, onError);

        var stage = src.IndexOf("stagedDatabaseWatermarks[item] =", StringComparison.Ordinal);
        Assert.InRange(stage, readItem, writeBatch);
    }
}
