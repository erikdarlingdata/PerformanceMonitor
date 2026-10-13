/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5633 item 19 review (Medium-2): a trace_flags read that FAILED must never be logged SUCCESS. The change diff, get_trace_flags
/// and the analysis attribution read a SUCCESS run with <c>rows_collected = 0</c> as "no flag is enabled", so the worker's old
/// tolerant wrapper (every SqlException became a 0-row result, logged SUCCESS) turned a denied DBCC or a timeout into "every flag
/// turned off". trace_flags now dispatches through the plain runner like every other collector, so <c>RunOneAsync</c> records
/// PERMISSIONS for a denied permission and ERROR otherwise (Azure SQL Database, where DBCC TRACESTATUS is unsupported, is skipped
/// by the collector's AppliesTo gate and never reaches the run). A source pin, because the failure needs a live monitored server.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class TraceFlagsFailedRunPinTests
{
    [Fact]
    public void TraceFlags_DispatchesThroughThePlainRunner_WithNoSqlExceptionSwallow()
    {
        Assert.Contains("trace_flags", DarlingWorker.DispatchedCollectorNames);

        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        Assert.Contains("[\"trace_flags\"] = (r, s, ct) => r.RunAsync(TraceFlagsCollector.Instance, s, ct),", source);
        Assert.DoesNotContain("RunTraceFlagsTolerantAsync", source);
        /* Nothing else in the worker wraps TraceFlagsCollector in a catch. */
        Assert.Single(Regex.Matches(source, @"TraceFlagsCollector\.Instance"));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
