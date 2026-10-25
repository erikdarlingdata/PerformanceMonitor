/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5809: the table form of the Query Store regressions read, without a database. The first form shipped with its two
/// table arms declared below the field that concatenated them, and static field initializers run in textual order, so
/// both copies (service and viewer) began at <c>baseline_performance</c>: every call failed with 42601 and quietly fell
/// back to raw. These pins hold the composed text, and the fault log that would have shown it.
/// </summary>
public sealed class QueryStoreRegressionsTableReadTests
{
    [Fact]
    public void ServiceTableSql_StartsWithItsTwoTableArms()
    {
        AssertTableShape(DarlingQueryStoreRegressionReader.QueryStoreRegressionsTableSql);
    }

    [Fact]
    public void ViewerTableSql_StartsWithItsTwoTableArms()
    {
        AssertTableShape(ViewerDataService.QueryStoreRegressionsTableSql);
    }

    private static void AssertTableShape(string sql)
    {
        Assert.StartsWith("WITH deduped_baseline AS (", sql.TrimStart(), StringComparison.Ordinal);
        Assert.Contains("deduped_recent AS (", sql, StringComparison.Ordinal);
        Assert.Contains("FROM query_store_interval_wide", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_store_stats", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ATableReadFault_WarnsOncePerProcessWithItsMessage_ThenLogsAtDebug()
    {
        var logger = new CapturingTestLogger();
        DarlingQueryStoreRegressionReader.ResetTableFaultWarningForTests();
        try
        {
            DarlingQueryStoreRegressionReader.LogTableFault(logger, new InvalidOperationException("first fault text"));
            DarlingQueryStoreRegressionReader.LogTableFault(logger, new InvalidOperationException("second fault text"));
            DarlingQueryStoreRegressionReader.LogTableFault(logger, new InvalidOperationException("third fault text"));

            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
            Assert.Equal(2, logger.CountAtLevel(LogLevel.Debug));
            Assert.Contains(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains("first fault text", StringComparison.Ordinal));
            Assert.DoesNotContain(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains("second fault text", StringComparison.Ordinal));
        }
        finally
        {
            DarlingQueryStoreRegressionReader.ResetTableFaultWarningForTests();
        }
    }

    [Fact]
    public void ATableReadFault_WithNoLogger_LeavesTheOnceFlagForTheFirstLoggedFault()
    {
        var logger = new CapturingTestLogger();
        DarlingQueryStoreRegressionReader.ResetTableFaultWarningForTests();
        try
        {
            DarlingQueryStoreRegressionReader.LogTableFault(null, new InvalidOperationException("unscoped fault"));
            DarlingQueryStoreRegressionReader.LogTableFault(logger, new InvalidOperationException("scoped fault"));

            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
            Assert.Contains("scoped fault", logger.Joined, StringComparison.Ordinal);
        }
        finally
        {
            DarlingQueryStoreRegressionReader.ResetTableFaultWarningForTests();
        }
    }

    /// <summary>#5506: the Regular arms may read <c>query_store_interval_latest</c> only where its coverage starts at or before
    /// the baseline start and no batch is pending; anything else keeps the wide table.</summary>
    [Fact]
    public void TheLatestGate_AllowsOnlyCoverageFromTheBaselineStartWithNoPendingBatch()
    {
        var baselineStart = new DateTime(2026, 10, 21, 12, 0, 0);
        Assert.True(QueryStoreRegressionBaseline.LatestCoversRegularArms(baselineStart.AddDays(-20), hasPending: false, baselineStart));
        Assert.True(QueryStoreRegressionBaseline.LatestCoversRegularArms(baselineStart, hasPending: false, baselineStart));
        Assert.False(QueryStoreRegressionBaseline.LatestCoversRegularArms(baselineStart.AddTicks(1), hasPending: false, baselineStart));
        Assert.False(QueryStoreRegressionBaseline.LatestCoversRegularArms(baselineStart.AddDays(-20), hasPending: true, baselineStart));
        Assert.False(QueryStoreRegressionBaseline.LatestCoversRegularArms(null, hasPending: false, baselineStart));
    }

    /// <summary>#5506: each reader runs the latest form where the plan allows it, and the wide form otherwise, with and
    /// without run days. The wide form is today's statement.</summary>
    [Fact]
    public void EachReader_PicksTheLatestForm_OnlyWhenThePlanAllowsIt()
    {
        var windowStart = new DateTime(2026, 10, 24, 18, 0, 0);
        var live = QueryStoreRegressionBaseline.AllLive(windowStart);
        var days = new QueryStoreRegressionBaseline.Plan([new DateOnly(2026, 10, 22)], new DateTime(2026, 10, 22), new DateTime(2026, 10, 22));
        foreach (var plan in new[] { live, days })
        {
            var daily = plan.BuiltDayCount > 0;
            Assert.Equal(DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: daily), DarlingQueryStoreRegressionReader.TableSqlFor(plan));
            Assert.Equal(DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: daily, regularFromLatest: true),
                DarlingQueryStoreRegressionReader.TableSqlFor(plan with { RegularFromLatest = true }));
            Assert.Equal(ViewerDataService.BuildQueryStoreRegressionsSql(wide: true, useDailyTotals: daily), ViewerDataService.QueryStoreRegressionsTableSqlFor(plan));
            Assert.Equal(ViewerDataService.BuildQueryStoreRegressionsSql(wide: true, useDailyTotals: daily, regularFromLatest: true),
                ViewerDataService.QueryStoreRegressionsTableSqlFor(plan with { RegularFromLatest = true }));
            Assert.NotEqual(DarlingQueryStoreRegressionReader.TableSqlFor(plan), DarlingQueryStoreRegressionReader.TableSqlFor(plan with { RegularFromLatest = true }));
        }

        Assert.False(live.RegularFromLatest);
    }

    /// <summary>#5506: the latest form's high range starts at the next midnight minus the measured margin, never below the
    /// last run day's midnight, and the wide form and the all-live plan keep their bounds.</summary>
    [Fact]
    public void TheHighStart_IsRaisedByTheMeasuredMargin_OnTheLatestFormWithRunDaysOnly()
    {
        var lastRunDay = new DateTime(2026, 10, 23);
        var days = new QueryStoreRegressionBaseline.Plan([new DateOnly(2026, 10, 22), new DateOnly(2026, 10, 23)], new DateTime(2026, 10, 22), lastRunDay);
        var latest = days with { RegularFromLatest = true };

        Assert.Equal(new DateTime(2026, 10, 23, 21, 0, 0), QueryStoreRegressionBaseline.WithMeasuredHighStart(latest, TimeSpan.FromHours(3)).HighStart);
        Assert.Equal(lastRunDay, QueryStoreRegressionBaseline.WithMeasuredHighStart(latest, QueryStoreIntervalWide.PurgeEdgeMargin).HighStart);
        Assert.Equal(lastRunDay, QueryStoreRegressionBaseline.WithMeasuredHighStart(days, TimeSpan.FromHours(3)).HighStart);
        var live = QueryStoreRegressionBaseline.AllLive(new DateTime(2026, 10, 24, 18, 0, 0)) with { RegularFromLatest = true };
        Assert.Equal(live, QueryStoreRegressionBaseline.WithMeasuredHighStart(live, TimeSpan.FromHours(3)));
    }
}
