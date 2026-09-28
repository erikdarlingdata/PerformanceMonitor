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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Source-position pins for the per-database Query Store watermark cache (#4661). The cache is exact only
/// while it advances strictly AFTER the item's COPY transaction commits, and while nothing but the known
/// writers touches <c>query_store_stats</c>; both are properties of where code sits, not of what exists.
/// </summary>
public sealed class QueryStoreDatabaseWatermarkWiringPinTests
{
    private static string RunnerSource() =>
        File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"));

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

    [Fact]
    public void OnlyTheKnownWriters_TouchQueryStoreStats()
    {
        var root = Path.Combine(RepoRoot(), "Darling");
        var writer = new Regex(@"(INSERT\s+INTO|DELETE\s+FROM|UPDATE|COPY|TRUNCATE)\s+(collect\.)?query_store_stats\b", RegexOptions.IgnoreCase);
        var offenders = Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains(Path.DirectorySeparatorChar + "Darling.Tests" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                        && !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                        && !f.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            .Where(f => writer.IsMatch(File.ReadAllText(f)))
            .Select(f => Path.GetRelativePath(root, f))
            .ToList();

        Assert.True(offenders.Count == 0,
            "a new writer of query_store_stats must invalidate DatabaseWatermarkCache (see its remarks): " + string.Join(", ", offenders));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        return dir ?? throw new InvalidOperationException("repo root not found");
    }
}
