/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static PerformanceMonitor.Ui.WaitDrillDownHelper;

namespace Darling.Tests;

/// <summary>
/// Pins the drill-down / chart-navigation parity restored from Lite (Items 1-5): the +/-30-minute window
/// computation, the slicer-overlay metric selection, the Wait Stats "Show Queries With &lt;wait&gt;"
/// classification + sort, and the new store reads' SQL shape. Pure logic + SQL pins only (no live Postgres).
/// </summary>
/* Serialized: these classes flip the process-wide ViewerTimeHelper.CurrentDisplayMode static (each
   restores in finally, but xUnit runs CLASSES in parallel — two flippers racing corrupts the mode a
   third class reads). One shared collection serializes them. */
[Collection("viewer-time-statics")]
public sealed class ViewerDrillDownTests
{
    // ── +/-30-minute drill window (Items 1-3) ──

    [Fact]
    public void DrillWindowUtc_IsA60MinuteSpanCenteredOnTheClickedInstant()
    {
        /* The chart X is the naive-UTC instant (#4766), so the clicked time IS the centre: no zone is applied
           on the way back, whatever display mode/offset the process statics currently hold. */
        var clicked = new DateTime(2026, 7, 6, 14, 20, 0);
        var (fromUtc, toUtc) = ViewerServerTab.DrillWindowUtc(clicked);

        Assert.Equal(clicked.AddMinutes(-30), fromUtc);
        Assert.Equal(clicked.AddMinutes(30), toUtc);
        Assert.Equal(TimeSpan.FromMinutes(60), toUtc - fromUtc);
    }

    [Theory]
    [InlineData(TimeDisplayMode.UTC, 0)]
    [InlineData(TimeDisplayMode.ServerTime, 330)]   // e.g. IST (+5:30)
    [InlineData(TimeDisplayMode.ServerTime, -480)]  // e.g. PST (-8:00)
    [InlineData(TimeDisplayMode.LocalTime, 0)]
    public void DrillWindowUtc_IsExactlyPlusMinus30OfTheClickedInstant_InEveryDisplayMode(TimeDisplayMode mode, int offsetMinutes)
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedClock = ViewerTimeHelper.ActiveServerClock;
        try
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
            ViewerTimeHelper.UtcOffsetMinutes = offsetMinutes;
            var clicked = new DateTime(2026, 7, 6, 9, 0, 0);
            var (fromUtc, toUtc) = ViewerServerTab.DrillWindowUtc(clicked);

            Assert.Equal(new DateTime(2026, 7, 6, 8, 30, 0), fromUtc);
            Assert.Equal(new DateTime(2026, 7, 6, 9, 30, 0), toUtc);
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.ActiveServerClock = savedClock;
        }
    }

    [Theory]
    [InlineData(TimeDisplayMode.UTC, 0)]
    [InlineData(TimeDisplayMode.ServerTime, 330)]   // e.g. IST (+5:30)
    [InlineData(TimeDisplayMode.ServerTime, -480)]  // e.g. PST (-8:00)
    public void ChartX_RoundTripsTheStoredInstant_WhateverTheDisplayZone(TimeDisplayMode mode, int offsetMinutes)
    {
        /* The chart plots the stored naive UTC as its X and a click reads it back with FromOADate; nothing in
           between depends on the display mode, so the drill centres on the stored instant and reads the rows
           it means. (Before #4766 the chart plotted ForDisplay(naiveUtc) and the drill had to undo the shift.) */
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedClock = ViewerTimeHelper.ActiveServerClock;
        try
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
            ViewerTimeHelper.UtcOffsetMinutes = offsetMinutes;
            var storedUtc = new DateTime(2026, 7, 6, 3, 15, 0);
            var clickedX = DateTime.FromOADate(storedUtc.ToOADate());
            var (fromUtc, toUtc) = ViewerServerTab.DrillWindowUtc(clickedX);

            Assert.Equal(storedUtc, clickedX);
            Assert.Equal(storedUtc.AddMinutes(-30), fromUtc);
            Assert.Equal(storedUtc.AddMinutes(30), toUtc);
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.ActiveServerClock = savedClock;
        }
    }

    // ── Slicer overlay metric selection (Item 5) ──

    private static readonly List<ViewerDataService.ItemTimelinePoint> Timeline = new()
    {
        new(new DateTime(2026, 7, 6, 10, 0, 0), CpuMs: 10, ElapsedMs: 100, Reads: 5, Writes: 2, PhysicalReads: 1),
        new(new DateTime(2026, 7, 6, 10, 1, 0), CpuMs: 0,  ElapsedMs: 0,   Reads: 0, Writes: 0, PhysicalReads: 0),
        new(new DateTime(2026, 7, 6, 10, 2, 0), CpuMs: 20, ElapsedMs: 200, Reads: 7, Writes: 3, PhysicalReads: 4),
    };

    [Theory]
    [InlineData("TotalCpu", 10.0, 20.0)]
    [InlineData("AvgCpu", 10.0, 20.0)]
    [InlineData("TotalElapsed", 100.0, 200.0)]
    [InlineData("TotalReads", 5.0, 7.0)]
    [InlineData("TotalWrites", 2.0, 3.0)]
    [InlineData("TotalPhysReads", 1.0, 4.0)]
    public void ComputeOverlayPoints_PicksTheFieldMatchingTheSlicerMetric_AndDropsZeroCycles(
        string metric, double firstValue, double secondValue)
    {
        var points = ViewerServerTab.ComputeOverlayPoints(Timeline, metric);

        /* The middle (all-zero) cycle is dropped so an idle stretch draws no flat baseline. */
        Assert.Equal(2, points.Count);
        Assert.Equal(firstValue, points[0].Value, precision: 3);
        Assert.Equal(secondValue, points[1].Value, precision: 3);
        Assert.Equal(new DateTime(2026, 7, 6, 10, 0, 0), points[0].TimeUtc);
        Assert.Equal(new DateTime(2026, 7, 6, 10, 2, 0), points[1].TimeUtc);
    }

    [Fact]
    public void ComputeOverlayPoints_UnknownMetric_FallsBackToElapsed()
    {
        /* Query Store's "Sessions" (executions) sort has no dedicated column — it falls to elapsed, like Lite. */
        var points = ViewerServerTab.ComputeOverlayPoints(Timeline, "Sessions");
        Assert.Equal(new[] { 100.0, 200.0 }, points.Select(p => p.Value));
    }

    // ── Wait Stats classification + sort (Item 4) ──

    [Theory]
    [InlineData("SOS_SCHEDULER_YIELD", WaitCategory.Correlated, "CpuTimeMs")]
    [InlineData("WRITELOG", WaitCategory.Correlated, "Writes")]
    [InlineData("RESOURCE_SEMAPHORE", WaitCategory.Correlated, "GrantedQueryMemoryGb")]
    [InlineData("PAGEIOLATCH_SH", WaitCategory.Correlated, "Reads")]
    [InlineData("THREADPOOL", WaitCategory.Uncapturable, "CpuTimeMs")]
    [InlineData("LCK_M_X", WaitCategory.Chain, "")]
    [InlineData("SOME_RANDOM_WAIT", WaitCategory.Filtered, "WaitTimeMs")]
    public void Classify_SelectsTheBranchTheWaitDrillDownWindowRoutesOn(
        string waitType, WaitCategory expectedCategory, string expectedSort)
    {
        var classification = Classify(waitType);
        Assert.Equal(expectedCategory, classification.Category);
        Assert.Equal(expectedSort, classification.SortProperty);
    }

    [Fact]
    public void SortByProperty_OrdersDescendingByTheClassifiedMetric()
    {
        var rows = new List<ViewerQuerySnapshotRow>
        {
            new() { SessionId = 1, CpuTimeMs = 10, WaitTimeMs = 300 },
            new() { SessionId = 2, CpuTimeMs = 90, WaitTimeMs = 100 },
            new() { SessionId = 3, CpuTimeMs = 50, WaitTimeMs = 200 },
        };

        var byCpu = WaitDrillDownWindow.SortByProperty(rows, "CpuTimeMs");
        Assert.Equal(new[] { 2, 3, 1 }, byCpu.Select(r => r.SessionId));

        var byWait = WaitDrillDownWindow.SortByProperty(rows, "WaitTimeMs");
        Assert.Equal(new[] { 1, 3, 2 }, byWait.Select(r => r.SessionId));
    }

    [Fact]
    public void SortByProperty_UnknownProperty_LeavesOrderUnchanged()
    {
        var rows = new List<ViewerQuerySnapshotRow>
        {
            new() { SessionId = 7 },
            new() { SessionId = 3 },
        };
        Assert.Same(rows, WaitDrillDownWindow.SortByProperty(rows, "")); /* the "" (chain) case: untouched */
    }

    // ── New store reads' SQL shape (Items 4-5) ──

    [Fact]
    public void QuerySnapshotsByWaitTypeSql_FiltersByWaitTypeOverTheWindow()
    {
        var sql = ViewerDataService.QuerySnapshotsByWaitTypeSql;
        Assert.Contains("FROM query_snapshots", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("wait_type = $4", sql, StringComparison.Ordinal);
        AssertPgPositionalDialect(sql);
    }

    [Fact]
    public void QueryStatsItemTimelineSql_FiltersOneQueryHashOverTheWindow()
    {
        var sql = ViewerDataService.QueryStatsItemTimelineSql;
        Assert.Contains("FROM query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("database_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("query_hash = $3", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $4", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $5", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time", sql, StringComparison.Ordinal);
        AssertPgPositionalDialect(sql);
    }

    [Fact]
    public void ProcStatsItemTimelineSql_FiltersOneSchemaObjectOverTheWindow()
    {
        var sql = ViewerDataService.ProcStatsItemTimelineSql;
        Assert.Contains("FROM procedure_stats", sql, StringComparison.Ordinal);
        Assert.Contains("database_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("schema_name = $3", sql, StringComparison.Ordinal);
        Assert.Contains("object_name = $4", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $5", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $6", sql, StringComparison.Ordinal);
        AssertPgPositionalDialect(sql);
    }

    [Fact]
    public void QueryStoreItemTimelineSql_FiltersOneQueryPlanAndScalesByExecutionCount()
    {
        var sql = ViewerDataService.QueryStoreItemTimelineSql;
        Assert.Contains("FROM query_store_stats", sql, StringComparison.Ordinal);
        Assert.Contains("query_id = $3", sql, StringComparison.Ordinal);
        Assert.Contains("plan_id = $4", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $5", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $6", sql, StringComparison.Ordinal);
        /* Query Store stores per-execution averages; the per-interval total scales by execution_count. */
        Assert.Contains("execution_count", sql, StringComparison.Ordinal);
        AssertPgPositionalDialect(sql);
    }

    [Fact]
    public void QueryStoreItemTimelineSql_DedupsPerInterval_SoTheOverlayAgreesWithTheSlicerBars()
    {
        /* #1841. The WHERE narrows to query_id + plan_id but NOT to an interval, and the rows are
           CUMULATIVE per-interval snapshots re-collected every cycle — so un-deduped this series draws one
           interval as a rising staircase of restatements, and can emit several points at the SAME
           collection_time. It is drawn OVER QueryStoreSlicerSql's bars, which ARE deduped, so leaving it
           raw would make the overlay contradict the bars it annotates. */
        var sql = ViewerDataService.QueryStoreItemTimelineSql;
        Assert.Contains(
            "PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc",
            sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE rn = 1", sql, StringComparison.Ordinal);
    }

    private static void AssertPgPositionalDialect(string sql)
    {
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);   // no T-SQL named params
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);  // no T-SQL unicode literals
        Assert.Contains("$1", sql, StringComparison.Ordinal);
    }
}
