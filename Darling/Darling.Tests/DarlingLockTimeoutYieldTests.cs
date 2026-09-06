/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #1805, Darling half: a SQL error 1222 from the lock-timeout-guarded collector (query_snapshots)
/// records as YIELDED, not ERROR — same classification Lite applies; parity is the point. The shared
/// flag itself is pinned in Lite.Tests (<c>LockTimeoutYieldTests</c>) against the one catalog both
/// runners read; these pin Darling's own two seams: the worker's catch (source-level — the
/// classification is try/catch glue around a live SqlException) and the daily-health collErrors
/// predicate staying exact-match, so YIELDED can never re-enter the Critical band through a
/// broadened not-success form.
/// </summary>
public sealed class DarlingLockTimeoutYieldTests
{
    [Fact]
    public void Worker_Classifies_1222_Through_The_Catalog_Flag()
    {
        var source = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));

        Assert.Contains("ex.Number == 1222 && CollectorCatalog.YieldsOnLockTimeout(collectorName)", source);
        Assert.Contains("\"YIELDED\"", source);
    }

    [Fact]
    public void DailySummary_Error_Count_Stays_ExactMatch_On_ERROR()
    {
        var source = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Storage", "DailySummarySql.cs"));
        Assert.Contains("FILTER (WHERE status = 'ERROR')", source);
    }

}
