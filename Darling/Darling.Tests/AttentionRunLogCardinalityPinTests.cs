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
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5640: the attention collector runs per database on Azure SQL Database, and the Client Timeouts rate divides a count by the span between the
/// run times in <c>collection_log</c> (<c>AttentionEvaluator.OldestTaken</c>). That only holds while a collector run writes ONE blended
/// <c>collection_log</c> row however many databases it fanned out over. If a per-database loop logged its own row, the run times would differ by
/// the loop's seconds, the span would collapse to seconds and the rate would inflate on every Azure SQL DB server. <c>AttentionWindowSql.Runs</c>
/// does not guard this (its DISTINCT removes nothing when each run has one row), so this pins the shape on both runners: the fan-out code writes no
/// <c>collection_log</c> row of its own, and the one place each product logs a finished run is called once.
/// </summary>
[Trait("Stage", "Guard")]
[Trait("Reads", "Lite")]
public sealed class AttentionRunLogCardinalityPinTests
{
    /// <summary>The code of a source file with comments and literal text blanked, so prose and strings are not mistaken for calls.</summary>
    private static string Code(params string[] segments) =>
        CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile(segments));

    private static int Count(string code, string needle) =>
        code.Split(needle).Length - 1;

    [Fact]
    public void TheDarlingFanOutRunner_WritesNoCollectionLogRowOfItsOwn_AndTheWorkerLogsTheRollupOnce()
    {
        var runner = Code("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");
        Assert.True(runner.Length > 100_000, "the scan read no runner source - the pin is broken, not the surface");
        Assert.Contains("RunsPerDatabase", runner, StringComparison.Ordinal);
        Assert.Equal(0, Count(runner, "LogCollectionAsync("));

        /* The worker logs the whole run's one result, with the per-database cost as the fan-out rollup riding on that row. */
        var worker = Code("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        Assert.Equal(1, Count(worker, "result.Fanout,"));
    }

    [Fact]
    public void TheLiteDefinitionRunner_WritesNoCollectionLogRowOfItsOwn_AndTheCollectorRunLogsOnce()
    {
        var runner = Code("Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs");
        Assert.True(runner.Length > 20_000, "the scan read no runner source - the pin is broken, not the surface");
        Assert.Equal(0, Count(runner, "LogCollectionAsync("));

        /* Across every partial of the service, the log is written by exactly one call (the end of the per-collector run), not one per database. */
        var calls = Directory.EnumerateFiles(Path.Combine(RepoFile.Root, "Lite", "Services"), "RemoteCollectorService*.cs")
            .Sum(f => Count(Code("Lite", "Services", Path.GetFileName(f)), "await LogCollectionAsync("));
        Assert.Equal(1, calls);
    }

}
