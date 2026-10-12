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

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5633 item 19 review (Medium-2): a trace_flags read that FAILED must never be logged SUCCESS. The change diff, get_trace_flags
/// and the analysis attribution read a SUCCESS run with <c>rows_collected = 0</c> as "no flag is enabled", so the collector's old
/// catch (every SqlException became 0 rows, status left SUCCESS) turned a denied DBCC or a timeout into "every flag turned off".
/// The collector now has no catch: <c>RunCollectorAsync</c> records PERMISSIONS for a denied permission and ERROR otherwise, like
/// every other collector (Azure SQL Database, where DBCC TRACESTATUS is unsupported, is skipped by the collector's AppliesTo gate).
/// A source pin, because the failure needs a live monitored server to provoke.
/// </summary>
public sealed class TraceFlagsFailedRunPinTests
{
    [Fact]
    public void CollectTraceFlagsAsync_HasNoCatch_SoAFailedReadIsClassifiedByRunCollectorAsync()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Services", "RemoteCollectorService.ServerConfig.cs"));
        var start = source.IndexOf("Task<int> CollectTraceFlagsAsync(", System.StringComparison.Ordinal);
        Assert.True(start >= 0, "could not find CollectTraceFlagsAsync");
        var body = source.Substring(start);
        Assert.Contains("TraceFlagsCollector.Instance", body);
        Assert.DoesNotContain("catch", body);
        Assert.DoesNotContain("return 0", body);
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
