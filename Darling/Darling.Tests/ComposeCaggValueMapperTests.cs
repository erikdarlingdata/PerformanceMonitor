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
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins <see cref="ComposeCaggValueMapper"/> — the raw-delta → CAGG-column rewrite, the single place a wrong column
/// would mean WRONG DATA on a panel (not just a truncation), so every emission is asserted exactly. Pure string
/// production, no DB — ungated.
/// </summary>
public sealed class ComposeCaggValueMapperTests
{
    private static ComposeMeasure Measure(string key) =>
        MeasureCatalog.Measures.First(m => string.Equals(m.Key, key, StringComparison.Ordinal));

    private static PanelPlan Plan(string measureKey, ComposeAggregate aggregate = ComposeAggregate.Sum) =>
        new()
        {
            Measure = Measure(measureKey),
            Aggregate = aggregate,
            Unit = "ms",
            Filters = Array.Empty<ComposeFilter>(),
            GroupBy = Array.Empty<ComposeDimension>(),
            Viz = "line",
        };

    /* ---------------- CanRemap gate ---------------- */

    [Theory]
    [InlineData("query_worker_us", ComposeAggregate.Sum, true)]
    [InlineData("query_worker_us", ComposeAggregate.Avg, true)]
    [InlineData("query_worker_us", ComposeAggregate.Min, true)]
    [InlineData("query_worker_us", ComposeAggregate.Max, true)]
    [InlineData("proc_executions", ComposeAggregate.Sum, true)]
    [InlineData("query_avg_elapsed_us", ComposeAggregate.Sum, true)]      /* delta Sum-ratio */
    [InlineData("proc_avg_cpu_us", ComposeAggregate.Sum, true)]           /* delta Sum-ratio */
    [InlineData("qs_executions", ComposeAggregate.Sum, true)]             /* QS -> execution_count_sum */
    [InlineData("qs_executions", ComposeAggregate.Avg, true)]             /* QS -> sum / sample_count */
    [InlineData("qs_executions", ComposeAggregate.Max, false)]            /* QS kept no per-row max column */
    [InlineData("qs_avg_duration_us", ComposeAggregate.Sum, true)]        /* QS execution-weighted mean */
    [InlineData("qs_total_duration_us", ComposeAggregate.Sum, true)]      /* QS total -> *_weighted_sum (#2732) */
    [InlineData("qs_total_cpu_us", ComposeAggregate.Sum, true)]           /* QS total -> *_weighted_sum (#2732) */
    [InlineData("qs_max_duration_us", ComposeAggregate.Max, true)]        /* QS peak -> _max */
    [InlineData("qs_max_duration_us", ComposeAggregate.Avg, false)]       /* only MAX kept */
    [InlineData("wait_time_ms", ComposeAggregate.Sum, false)]             /* no CAGG */
    [InlineData("sqlserver_cpu_utilization", ComposeAggregate.Avg, false)] /* gauge, no CAGG */
    public void CanRemap_GatesToSupportedCaggCases(string measureKey, ComposeAggregate aggregate, bool expected) =>
        Assert.Equal(expected, ComposeCaggValueMapper.CanRemap(Measure(measureKey), aggregate));

    /* ---------------- scalar remap (query_worker_us => worker_time_*) ---------------- */

    [Fact]
    public void Scalar_Sum_ReadsSumColumn()
    {
        Assert.Equal(
            "CAST(SUM(f.worker_time_sum) AS double precision)",
            ComposeCaggValueMapper.BuildCaggNativeExpr(Measure("query_worker_us"), ComposeAggregate.Sum));
    }

    [Fact]
    public void Scalar_Min_ReadsMinColumn()
    {
        Assert.Equal(
            "CAST(MIN(f.worker_time_min) AS double precision)",
            ComposeCaggValueMapper.BuildCaggNativeExpr(Measure("query_worker_us"), ComposeAggregate.Min));
    }

    [Fact]
    public void Scalar_Max_ReadsMaxColumn()
    {
        Assert.Equal(
            "CAST(MAX(f.worker_time_max) AS double precision)",
            ComposeCaggValueMapper.BuildCaggNativeExpr(Measure("query_worker_us"), ComposeAggregate.Max));
    }

    [Fact]
    public void Scalar_Avg_IsSumOverSampleCount_NotAvgOfSums()
    {
        /* The exact reconstruction: AVG(delta) == SUM(x_sum)/SUM(sample_count), never AVG(x_sum). */
        Assert.Equal(
            "(CAST(SUM(f.worker_time_sum) AS double precision) / NULLIF(SUM(f.sample_count), 0))",
            ComposeCaggValueMapper.BuildCaggNativeExpr(Measure("query_worker_us"), ComposeAggregate.Avg));
    }

    [Fact]
    public void Scalar_ProcedureStats_MapsSameConvention()
    {
        Assert.Equal(
            "CAST(SUM(f.elapsed_time_sum) AS double precision)",
            ComposeCaggValueMapper.BuildCaggNativeExpr(Measure("proc_elapsed_us"), ComposeAggregate.Sum));
    }

    /* ---------------- Sum-ratio remap (both operands => *_sum) ---------------- */

    [Fact]
    public void SumRatio_MapsBothOperandsToSumColumns()
    {
        /* query_avg_elapsed_us = query_elapsed_us (delta_elapsed_time) / query_executions (delta_execution_count). */
        Assert.Equal(
            "(CAST(SUM(f.elapsed_time_sum) AS double precision) / NULLIF(SUM(f.execution_count_sum), 0))",
            ComposeCaggValueMapper.BuildCaggNativeExpr(Measure("query_avg_elapsed_us"), Measure("query_avg_elapsed_us").DefaultTimeAgg));
    }

    [Fact]
    public void SumRatio_Cpu_MapsWorkerOverExecutions()
    {
        Assert.Equal(
            "(CAST(SUM(f.worker_time_sum) AS double precision) / NULLIF(SUM(f.execution_count_sum), 0))",
            ComposeCaggValueMapper.BuildCaggNativeExpr(Measure("proc_avg_cpu_us"), Measure("proc_avg_cpu_us").DefaultTimeAgg));
    }

    /* ---------------- Query Store remap (execution-weighted mean, executions, peaks) ---------------- */

    [Fact]
    public void QueryStore_WeightedRatio_Duration_UsesWeightedSumOverExecutionCount()
    {
        /* SUM(avg_duration_us * execution_count) / SUM(execution_count) reconstructs from the reshaped columns. */
        Assert.Equal(
            "(CAST(SUM(f.duration_us_weighted_sum) AS double precision) / NULLIF(SUM(f.execution_count_sum), 0))",
            ComposeCaggValueMapper.BuildCaggNativeExpr(Measure("qs_avg_duration_us"), Measure("qs_avg_duration_us").DefaultTimeAgg));
    }

    [Fact]
    public void QueryStore_WeightedRatio_Cpu_UsesCpuWeightedSum()
    {
        Assert.Equal(
            "(CAST(SUM(f.cpu_us_weighted_sum) AS double precision) / NULLIF(SUM(f.execution_count_sum), 0))",
            ComposeCaggValueMapper.BuildCaggNativeExpr(Measure("qs_avg_cpu_us"), Measure("qs_avg_cpu_us").DefaultTimeAgg));
    }

    [Fact]
    public void QueryStore_WeightedSum_Cpu_ReadsTheWeightedSumColumn_WithNoDenominator()
    {
        /* #2732: the total is the CAGG's pre-multiplied product-sum ITSELF — no execution_count_sum divisor,
           and no NULLIF (there is no division; SUM over zero rows is already NULL). */
        Assert.Equal(
            "CAST(SUM(f.cpu_us_weighted_sum) AS double precision)",
            ComposeCaggValueMapper.BuildCaggNativeExpr(Measure("qs_total_cpu_us"), Measure("qs_total_cpu_us").DefaultTimeAgg));
    }

    [Fact]
    public void QueryStore_WeightedSum_Duration_ReadsTheWeightedSumColumn_WithNoDenominator()
    {
        Assert.Equal(
            "CAST(SUM(f.duration_us_weighted_sum) AS double precision)",
            ComposeCaggValueMapper.BuildCaggNativeExpr(Measure("qs_total_duration_us"), Measure("qs_total_duration_us").DefaultTimeAgg));
    }

    [Fact]
    public void QueryStore_Executions_Sum_ReadsExecutionCountSum()
    {
        Assert.Equal(
            "CAST(SUM(f.execution_count_sum) AS double precision)",
            ComposeCaggValueMapper.BuildCaggNativeExpr(Measure("qs_executions"), ComposeAggregate.Sum));
    }

    [Fact]
    public void QueryStore_Executions_Avg_IsSumOverSampleCount()
    {
        Assert.Equal(
            "(CAST(SUM(f.execution_count_sum) AS double precision) / NULLIF(SUM(f.sample_count), 0))",
            ComposeCaggValueMapper.BuildCaggNativeExpr(Measure("qs_executions"), ComposeAggregate.Avg));
    }

    [Fact]
    public void QueryStore_PeakDuration_Max_ReadsMaxColumn()
    {
        Assert.Equal(
            "CAST(MAX(f.max_duration_us_max) AS double precision)",
            ComposeCaggValueMapper.BuildCaggNativeExpr(Measure("qs_max_duration_us"), ComposeAggregate.Max));
    }
}
