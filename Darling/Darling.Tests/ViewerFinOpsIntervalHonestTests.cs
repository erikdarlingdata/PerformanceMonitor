/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4394 part B2 shape pin: every FinOps raw <c>v_query_stats</c> sum must exclude a plan's first-collection
/// (zero-interval) row, the same way the hourly successors already do via
/// <see cref="TimescaleSupport.IntervalHonestSourceFilter"/>. Each of the checked constants must contain that
/// filter's exact text somewhere in its SQL. This is a text pin, not a live pin — the live behavior is
/// covered separately in <c>FinOpsMergedReadsLiveTests.cs</c>.
/// </summary>
public sealed class ViewerFinOpsIntervalHonestTests
{
    private static IEnumerable<(string Name, string Sql)> RawQueryStatsSums()
    {
        yield return (nameof(ViewerDataService.DatabaseResourceUsageSql), ViewerDataService.DatabaseResourceUsageSql);
        yield return (nameof(ViewerDataService.TopResourceConsumersSql), ViewerDataService.TopResourceConsumersSql);
        yield return (nameof(ViewerDataService.ExpensiveQueriesSql), ViewerDataService.ExpensiveQueriesSql);
        yield return (nameof(ViewerDataService.HighImpactQueriesSql), ViewerDataService.HighImpactQueriesSql);
        yield return (nameof(ViewerDataService.IdleDatabasesSql), ViewerDataService.IdleDatabasesSql);
    }

    [Fact]
    public void EveryRawQueryStatsSum_ContainsTheIntervalHonestFilter()
    {
        foreach (var (name, sql) in RawQueryStatsSums())
        {
            Assert.Contains(TimescaleSupport.IntervalHonestSourceFilter, sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void IdleDatabasesSql_FiltersOnlyTheExecutionCountSum_NotTheActivityTimestamp()
    {
        var sql = ViewerDataService.IdleDatabasesSql;

        /* The idleness test (COALESCE(a.total_executions, 0) = 0) must not flip because a real sighting
           was dropped from evidence of activity — MAX(last_execution_time) stays over ALL rows. */
        Assert.Contains(
            $"SUM(delta_execution_count) FILTER (WHERE {TimescaleSupport.IntervalHonestSourceFilter}) AS total_executions",
            sql, StringComparison.Ordinal);
        Assert.Contains("MAX(last_execution_time) AS last_execution", sql, StringComparison.Ordinal);
    }
}
