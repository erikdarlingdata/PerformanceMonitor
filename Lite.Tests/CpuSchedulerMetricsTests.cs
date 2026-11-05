/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Behavioral pin for the shared <see cref="CpuSchedulerMetrics"/> — the CPU Scheduler latest-snapshot metric
/// projection + install/47 <c>report.cpu_scheduler_pressure</c> banding that used to be duplicated as
/// <c>BuildCpuSchedulerMetrics</c> / <c>ClassifyCpuPressure</c> in Lite (<c>ServerTab.CpuScheduler.cs</c>) and
/// the Darling viewer (<c>ViewerServerTab.CpuScheduler.cs</c>). Drives the shared entry points directly over a
/// stub <see cref="ICpuSchedulerSnapshot"/> — proving the collapsed logic still lands warning flags on the
/// right rows, computes worker-utilization, degrades nullable columns to "N/A", and bands pressure. Pure — no
/// DuckDB, no WPF.
/// </summary>
public class CpuSchedulerMetricsTests
{
    /// <summary>A calm-by-default snapshot; each test overrides only the fields it exercises.</summary>
    private sealed class StubSnapshot : ICpuSchedulerSnapshot
    {
        public int MaxWorkersCount { get; init; } = 512;
        public int SchedulerCount { get; init; } = 8;
        public int CpuCount { get; init; } = 8;
        public int TotalRunnableTasksCount { get; init; }
        public long TotalWorkQueueCount { get; init; }
        public int TotalCurrentWorkersCount { get; init; } = 128;
        public double AvgRunnableTasksCount { get; init; }
        public int TotalActiveRequestCount { get; init; }
        public int TotalQueuedRequestCount { get; init; }
        public int TotalBlockedTaskCount { get; init; }
        public long TotalActiveParallelThreadCount { get; init; }
        public int? RunnableRequestCount { get; init; } = 3;
        public int? TotalRequestCount { get; init; } = 20;
        public double? RunnablePercent { get; init; } = 12.5;
        public bool WorkerThreadExhaustionWarning { get; init; }
        public bool RunnableTasksWarning { get; init; }
        public bool BlockedTasksWarning { get; init; }
        public bool QueuedRequestsWarning { get; init; }
        public long TotalPhysicalMemoryKb { get; init; } = 67_108_864; // 64 GB
        public long AvailablePhysicalMemoryKb { get; init; } = 8_388_608; // 8 GB
        public string? SystemMemoryStateDesc { get; init; } = "Available physical memory is high";
        public bool PhysicalMemoryPressureWarning { get; init; }
        public int TotalNodeCount { get; init; } = 2;
        public int NodesOnlineCount { get; init; } = 2;
        public int OfflineCpuCount { get; init; }
        public bool OfflineCpuWarning { get; init; }
    }

    [Fact]
    public void BuildMetrics_NullSnapshot_YieldsEmptyGrid()
        => Assert.Empty(CpuSchedulerMetrics.BuildMetrics(null));

    [Fact]
    public void BuildMetrics_LeadsWithPressureRows_AndProjectsScalars()
    {
        var rows = CpuSchedulerMetrics.BuildMetrics(new StubSnapshot
        {
            MaxWorkersCount = 400,
            TotalCurrentWorkersCount = 100,
            AvgRunnableTasksCount = 0.5,
            RunnablePercent = 12.5,
            RunnableTasksWarning = true,
        });

        /* The rolled-up pressure badge + recommendation lead the grid. */
        Assert.Equal("Pressure Level", rows[0].Metric);
        Assert.Equal("Recommendation", rows[1].Metric);

        /* 100 / 400 * 100 = 25.0 %, and the two averaged/percentage columns format to F2. */
        Assert.Equal("25.0", rows.Single(r => r.Metric == "Worker Utilization %").Value);
        Assert.Equal("0.50", rows.Single(r => r.Metric == "Avg Runnable / Scheduler").Value);
        Assert.Equal("12.50", rows.Single(r => r.Metric == "Runnable %").Value);

        /* A collector warning flag highlights its own row; a flag that is off does not. */
        Assert.True(rows.Single(r => r.Metric == "Runnable Tasks").IsWarning);
        Assert.False(rows.Single(r => r.Metric == "Blocked Tasks").IsWarning);
    }

    [Fact]
    public void BuildMetrics_NullableColumns_DegradeToNa()
    {
        var rows = CpuSchedulerMetrics.BuildMetrics(new StubSnapshot
        {
            RunnableRequestCount = null,
            TotalRequestCount = null,
            RunnablePercent = null,
        });

        Assert.Equal("N/A", rows.Single(r => r.Metric == "Runnable Requests").Value);
        Assert.Equal("N/A", rows.Single(r => r.Metric == "Total Requests").Value);
        Assert.Equal("0.00", rows.Single(r => r.Metric == "Runnable %").Value);
        Assert.Equal(CpuSchedulerMetrics.RunnablePercentHint, rows.Single(r => r.Metric == "Runnable %").Hint);
    }

    [Theory]
    [InlineData(60, "CRITICAL - High runnable task queue")]
    [InlineData(25, "HIGH - Moderate runnable task queue")]
    [InlineData(15, "MEDIUM - Some runnable tasks queued")]
    [InlineData(5, "NORMAL")]
    public void ClassifyCpuPressure_BandsOnRunnableTaskQueue(int runnable, string expectedLevel)
        => Assert.Equal(expectedLevel,
            CpuSchedulerMetrics.ClassifyCpuPressure(new StubSnapshot { TotalRunnableTasksCount = runnable }).Level);

    [Fact]
    public void ClassifyCpuPressure_WorkerUtilizationOver90_IsHigh_WhenRunnableIsLow()
        => Assert.Equal("HIGH - Worker thread exhaustion",
            CpuSchedulerMetrics.ClassifyCpuPressure(
                new StubSnapshot { MaxWorkersCount = 100, TotalCurrentWorkersCount = 95 }).Level);

    [Fact]
    public void ClassifyCpuPressure_ZeroMaxWorkers_DoesNotDivideByZero_StaysNormal()
        => Assert.Equal("NORMAL",
            CpuSchedulerMetrics.ClassifyCpuPressure(
                new StubSnapshot { MaxWorkersCount = 0, TotalCurrentWorkersCount = 0 }).Level);
}
