/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the per-row double-click execution-history drill-down windows (parity-gap "Per-row double-click history
/// windows missing on all 3 query grids" + port-candidate "Per-object execution-history drill-downs"): the
/// three fuller history SQL reads' shape + parameterization, the row models' delta / average / total
/// derivations, the metric-selector chart projection, and the row → window-parameter mapping (which fields
/// identify the double-clicked item, and when a row can't open a window). Pure logic + SQL pins only — no live
/// Postgres, and nothing here mutates <see cref="ViewerTimeHelper"/>'s process-wide statics (the execution-stamp
/// display pin READS them, deriving its expected through the row's own renderer — the unserialized display
/// classes' pattern).
/// </summary>
public sealed class ViewerHistoryWindowTests
{
    // ── History reads' SQL shape (both-sides window-bound, positional PG params) ──

    [Fact]
    public void QueryStatsHistorySql_FiltersOneQueryHashOverTheWindow_OrderedByTime()
    {
        var sql = ViewerDataService.QueryStatsHistorySql;
        Assert.Contains("FROM query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("database_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("query_hash = $3", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $4", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $5", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time", sql, StringComparison.Ordinal);
        /* The window needs the delta, cumulative, and min/max columns the narrow overlay read lacks. */
        Assert.Contains("delta_worker_time", sql, StringComparison.Ordinal);
        Assert.Contains("total_worker_time", sql, StringComparison.Ordinal);
        Assert.Contains("min_grant_kb", sql, StringComparison.Ordinal);
        Assert.Contains("sample_interval_seconds", sql, StringComparison.Ordinal);
        AssertPgPositionalDialect(sql);
    }

    [Fact]
    public void ProcStatsHistorySql_FiltersOneSchemaObjectOverTheWindow_AndDerivesTheInterval()
    {
        var sql = ViewerDataService.ProcStatsHistorySql;
        Assert.Contains("FROM procedure_stats", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("database_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("schema_name = $3", sql, StringComparison.Ordinal);
        Assert.Contains("object_name = $4", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $5", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $6", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time", sql, StringComparison.Ordinal);
        /* procedure_stats has no sample_interval_seconds column — it's derived from the previous row's gap. */
        Assert.Contains("LAG(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("total_spills", sql, StringComparison.Ordinal);
        AssertPgPositionalDialect(sql);
    }

    [Fact]
    public void QueryStoreHistorySql_IsQueryScoped_AcrossEveryPlan_OverTheWindow()
    {
        var sql = ViewerDataService.QueryStoreHistorySql;
        Assert.Contains("FROM query_store_stats", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("database_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("query_id = $3", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $4", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $5", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time", sql, StringComparison.Ordinal);
        /* Query-scoped, NOT plan-filtered — plan_id rides back so the chart draws one series per plan. */
        Assert.DoesNotContain("plan_id = $", sql, StringComparison.Ordinal);
        Assert.Contains("plan_id", sql, StringComparison.Ordinal);
        Assert.Contains("is_forced_plan", sql, StringComparison.Ordinal);
        AssertPgPositionalDialect(sql);
    }

    // ── Row model derivations (delta µs → ms, per-execution averages, cumulative totals) ──

    [Fact]
    public void QueryStatsHistoryRow_ConvertsMicrosecondsAndDividesByExecutionDelta()
    {
        var row = new ViewerQueryStatsHistoryRow
        {
            DeltaExecutions = 10,
            DeltaCpuUs = 5_000,           // 5 ms across 10 execs → 0.5 ms avg
            DeltaElapsedUs = 20_000,      // 20 ms across 10 execs → 2 ms avg
            DeltaLogicalReads = 500,      // 50 avg
            MinCpuUs = 2_000,             // 2 ms
            MaxCpuUs = 9_000,             // 9 ms
            TotalCpuUs = 100_000,         // 100 ms
            TotalClrTimeUs = 3_000,       // 3 ms
        };

        Assert.Equal(5.0, row.DeltaCpuMs, precision: 6);
        Assert.Equal(20.0, row.DeltaElapsedMs, precision: 6);
        Assert.Equal(0.5, row.AvgCpuMs, precision: 6);
        Assert.Equal(2.0, row.AvgElapsedMs, precision: 6);
        Assert.Equal(50.0, row.AvgReads, precision: 6);
        Assert.Equal(2.0, row.MinCpuMs, precision: 6);
        Assert.Equal(9.0, row.MaxCpuMs, precision: 6);
        Assert.Equal(100.0, row.TotalCpuMs, precision: 6);
        Assert.Equal(3.0, row.TotalClrMs, precision: 6);
    }

    [Fact]
    public void QueryStatsHistoryRow_ZeroExecutionDelta_YieldsZeroAverages_NoDivideByZero()
    {
        var row = new ViewerQueryStatsHistoryRow { DeltaExecutions = 0, DeltaCpuUs = 5_000, DeltaLogicalReads = 99 };
        Assert.Equal(0.0, row.AvgCpuMs);
        Assert.Equal(0.0, row.AvgReads);
        Assert.Equal(5.0, row.DeltaCpuMs, precision: 6); // the delta itself is still surfaced
    }

    [Fact]
    public void ProcedureHistoryRow_DerivesMinCpuFromWorkerTime_AndAvgSpills()
    {
        var row = new ViewerProcedureStatsHistoryRow
        {
            DeltaExecutions = 4,
            DeltaSpills = 8,               // 2 avg
            MinWorkerTimeUs = 1_500,      // 1.5 ms
            MaxWorkerTimeUs = 6_000,      // 6 ms
            TotalCpuUs = 40_000,          // 40 ms
        };

        Assert.Equal(2.0, row.AvgSpills, precision: 6);
        Assert.Equal(1.5, row.MinCpuMs, precision: 6);
        Assert.Equal(6.0, row.MaxCpuMs, precision: 6);
        Assert.Equal(40.0, row.TotalCpuMs, precision: 6);
    }

    [Fact]
    public void QueryStoreHistoryRow_TotalsScalePerExecutionAveragesByExecutionCount()
    {
        var row = new ViewerQueryStoreHistoryRow { ExecutionCount = 25, AvgDurationMs = 4, AvgCpuTimeMs = 2 };
        Assert.Equal(100.0, row.TotalDurationMs, precision: 6);
        Assert.Equal(50.0, row.TotalCpuMs, precision: 6);
    }

    [Fact]
    public void QueryStoreHistoryRow_ExecutionTimesLocal_ConvertStoredNaiveUtc_EmptyForNull()
    {
        /* The drill-down history row carries the same naive-UTC query_store_stats stamps as the Query
           Store tab row and renders them through the same stored-UTC conversion (#3207) — pinned the same
           way (#3253): expected derived through the same ForDisplay the renderer uses, because a pinned
           literal only holds where the conversion is the identity (a UTC machine). Distinct inputs, so
           First wired to Last (or the reverse) fails. */
        var firstUtc = new DateTime(2026, 7, 1, 9, 30, 15, DateTimeKind.Unspecified);
        var lastUtc = new DateTime(2026, 7, 2, 21, 5, 59, DateTimeKind.Unspecified);
        var row = new ViewerQueryStoreHistoryRow { FirstExecutionTime = firstUtc, LastExecutionTime = lastUtc };
        Assert.Equal(ViewerTimeHelper.ForDisplay(firstUtc).ToString("yyyy-MM-dd HH:mm:ss"), row.FirstExecutionTimeLocal);
        Assert.Equal(ViewerTimeHelper.ForDisplay(lastUtc).ToString("yyyy-MM-dd HH:mm:ss"), row.LastExecutionTimeLocal);
        Assert.Equal("", new ViewerQueryStoreHistoryRow().FirstExecutionTimeLocal);
        Assert.Equal("", new ViewerQueryStoreHistoryRow().LastExecutionTimeLocal);
    }

    // ── Metric-selector chart projection (each window's GetMetricValue switch) ──

    [Fact]
    public void QueryStatsGetMetricValue_MapsEachSelectorTag_UnknownFallsBackToAvgCpu()
    {
        var row = new ViewerQueryStatsHistoryRow
        {
            DeltaExecutions = 2, DeltaCpuUs = 4_000, DeltaLogicalReads = 6, DeltaSpills = 3,
        };
        Assert.Equal(row.AvgCpuMs, QueryStatsHistoryWindow.GetMetricValue(row, "AvgCpuMs"), precision: 6);
        Assert.Equal(row.AvgReads, QueryStatsHistoryWindow.GetMetricValue(row, "AvgReads"), precision: 6);
        Assert.Equal(row.DeltaExecutions, QueryStatsHistoryWindow.GetMetricValue(row, "DeltaExecutions"), precision: 6);
        Assert.Equal(row.DeltaCpuMs, QueryStatsHistoryWindow.GetMetricValue(row, "DeltaCpuMs"), precision: 6);
        Assert.Equal(row.DeltaSpills, QueryStatsHistoryWindow.GetMetricValue(row, "DeltaSpills"), precision: 6);
        Assert.Equal(row.AvgCpuMs, QueryStatsHistoryWindow.GetMetricValue(row, "not-a-metric"), precision: 6);
    }

    [Fact]
    public void QueryStoreGetMetricValue_MapsTotalsAndExecutionCount()
    {
        var row = new ViewerQueryStoreHistoryRow { ExecutionCount = 7, AvgDurationMs = 3, AvgCpuTimeMs = 1.5 };
        Assert.Equal(row.TotalDurationMs, QueryStoreHistoryWindow.GetMetricValue(row, "TotalDurationMs"), precision: 6);
        Assert.Equal(row.TotalCpuMs, QueryStoreHistoryWindow.GetMetricValue(row, "TotalCpuMs"), precision: 6);
        Assert.Equal(row.ExecutionCount, QueryStoreHistoryWindow.GetMetricValue(row, "ExecutionCount"), precision: 6);
        Assert.Equal(row.AvgCpuTimeMs, QueryStoreHistoryWindow.GetMetricValue(row, "unknown"), precision: 6);
    }

    // ── Row → window-parameter mapping (the fields that identify the double-clicked item) ──

    [Fact]
    public void MapQueryStatsHistoryKey_CarriesDbHashAndText_WhenBothKeyFieldsPresent()
    {
        var key = ViewerServerTab.MapQueryStatsHistoryKey(new ViewerQueryStatsRow
        {
            DatabaseName = "Sales", QueryHash = "0xABCD", QueryText = "SELECT 1",
        });
        Assert.NotNull(key);
        Assert.Equal("Sales", key!.DatabaseName);
        Assert.Equal("0xABCD", key.QueryHash);
        Assert.Equal("SELECT 1", key.QueryText);
    }

    [Theory]
    [InlineData("", "0xABCD")]  // no database
    [InlineData("Sales", "")]   // no query hash
    public void MapQueryStatsHistoryKey_ReturnsNull_WhenAKeyFieldIsMissing(string db, string hash)
        => Assert.Null(ViewerServerTab.MapQueryStatsHistoryKey(
            new ViewerQueryStatsRow { DatabaseName = db, QueryHash = hash }));

    [Fact]
    public void MapProcedureHistoryKey_CarriesDbSchemaObject_WhenDbAndObjectPresent()
    {
        var key = ViewerServerTab.MapProcedureHistoryKey(new ViewerProcedureStatsRow
        {
            DatabaseName = "Sales", SchemaName = "dbo", ObjectName = "usp_Get",
        });
        Assert.NotNull(key);
        Assert.Equal("Sales", key!.DatabaseName);
        Assert.Equal("dbo", key.SchemaName);
        Assert.Equal("usp_Get", key.ObjectName);
    }

    [Theory]
    [InlineData("", "usp_Get")]  // no database
    [InlineData("Sales", "")]    // no object name
    public void MapProcedureHistoryKey_ReturnsNull_WhenAKeyFieldIsMissing(string db, string obj)
        => Assert.Null(ViewerServerTab.MapProcedureHistoryKey(
            new ViewerProcedureStatsRow { DatabaseName = db, ObjectName = obj }));

    [Fact]
    public void MapQueryStoreHistoryKey_CarriesDbQueryIdPlanIdText_WhenDbAndQueryIdPresent()
    {
        var key = ViewerServerTab.MapQueryStoreHistoryKey(new ViewerQueryStoreRow
        {
            DatabaseName = "Sales", QueryId = 42, PlanId = 7, QueryText = "SELECT 2",
        });
        Assert.NotNull(key);
        Assert.Equal("Sales", key!.DatabaseName);
        Assert.Equal(42, key.QueryId);
        Assert.Equal(7, key.PlanId);
        Assert.Equal("SELECT 2", key.QueryText);
    }

    [Theory]
    [InlineData("", 42)]      // no database
    [InlineData("Sales", 0)]  // no query_id
    public void MapQueryStoreHistoryKey_ReturnsNull_WhenAKeyFieldIsMissing(string db, long queryId)
        => Assert.Null(ViewerServerTab.MapQueryStoreHistoryKey(
            new ViewerQueryStoreRow { DatabaseName = db, QueryId = queryId }));

    [Fact]
    public void MapQueryStoreRegressionHistoryKey_CarriesDbQueryIdText_WithZeroPlanId_WhenDbAndQueryIdPresent()
    {
        /* The regression row aggregates over plans (no single plan_id), so the query-scoped history window
           opens with plan_id 0 — the same "no specific plan" value the Dashboard passes. */
        var key = ViewerServerTab.MapQueryStoreRegressionHistoryKey(new ViewerQueryStoreRegressionRow
        {
            DatabaseName = "Sales", QueryId = 42, QueryTextSample = "SELECT 3",
        });
        Assert.NotNull(key);
        Assert.Equal("Sales", key!.DatabaseName);
        Assert.Equal(42, key.QueryId);
        Assert.Equal(0, key.PlanId);
        Assert.Equal("SELECT 3", key.QueryText);
    }

    [Theory]
    [InlineData("", 42)]      // no database
    [InlineData("Sales", 0)]  // no query_id
    public void MapQueryStoreRegressionHistoryKey_ReturnsNull_WhenAKeyFieldIsMissing(string db, long queryId)
        => Assert.Null(ViewerServerTab.MapQueryStoreRegressionHistoryKey(
            new ViewerQueryStoreRegressionRow { DatabaseName = db, QueryId = queryId }));

    // ── Shared PG positional-dialect assertion (mirrors ViewerDrillDownTests) ──

    private static void AssertPgPositionalDialect(string sql)
    {
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);   // no T-SQL named params
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);  // no T-SQL unicode literals
        Assert.Contains("$1", sql, StringComparison.Ordinal);
    }
}
