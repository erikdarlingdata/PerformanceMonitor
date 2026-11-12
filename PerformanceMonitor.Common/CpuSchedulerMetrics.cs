/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>
/// The shared CPU Scheduler latest-snapshot projection + pressure classification — previously duplicated
/// (byte-for-byte for the classification, behaviorally identical for the projection) as
/// <c>BuildCpuSchedulerMetrics</c> / <c>ClassifyCpuPressure</c> in both Lite
/// (<c>ServerTab.CpuScheduler.cs</c>) and the Darling viewer (<c>ViewerServerTab.CpuScheduler.cs</c>). Both
/// read a single <c>cpu_scheduler_stats</c> row through <see cref="ICpuSchedulerSnapshot"/>, so the warning-flag
/// row projection, the worker-utilization math, the memory formatting, and the install/47
/// <c>report.cpu_scheduler_pressure</c> banding all live here once. Pure (no WPF, no time helpers) — each app
/// keeps only its own data read and chart render.
/// </summary>
public static class CpuSchedulerMetrics
{
    /// <summary>
    /// Projects the latest snapshot into the metric grid's labelled rows (with the collector's pressure flags
    /// driving the row highlight). A null snapshot (an empty window) yields an empty grid.
    /// </summary>
    public static List<CpuSchedulerMetricRow> BuildMetrics(ICpuSchedulerSnapshot? s)
    {
        if (s is null)
        {
            return new List<CpuSchedulerMetricRow>();
        }

        double workerUtil = s.MaxWorkersCount > 0
            ? (double)s.TotalCurrentWorkersCount / s.MaxWorkersCount * 100.0
            : 0;

        /* Rolled-up pressure badge + recommendation (install/47 report.cpu_scheduler_pressure parity): the
           raw warning booleans are kept below, but these two headline rows give the one-glance severity +
           next step. The level row highlights whenever it is not NORMAL. */
        var pressure = ClassifyCpuPressure(s);

        var rows = new List<CpuSchedulerMetricRow>
        {
            new("Pressure Level", pressure.Level, !pressure.Level.StartsWith("NORMAL", StringComparison.Ordinal)),
            new("Recommendation", pressure.Recommendation, false),
            new("Schedulers", FormatInt(s.SchedulerCount), false),
            new("Logical CPUs", FormatInt(s.CpuCount), false),
            new("NUMA Nodes (online / total)", $"{s.NodesOnlineCount} / {s.TotalNodeCount}", false),
            new("Offline CPUs", FormatInt(s.OfflineCpuCount), s.OfflineCpuWarning),
            new("Max Worker Threads", FormatInt(s.MaxWorkersCount), false),
            new("Current Workers", FormatInt(s.TotalCurrentWorkersCount), s.WorkerThreadExhaustionWarning),
            new("Worker Utilization %", workerUtil.ToString("F1"), s.WorkerThreadExhaustionWarning),
            new("Runnable Tasks", FormatInt(s.TotalRunnableTasksCount), s.RunnableTasksWarning),
            new("Avg Runnable / Scheduler", s.AvgRunnableTasksCount.ToString("F2"), s.RunnableTasksWarning),
            new("Work Queue Length", FormatLong(s.TotalWorkQueueCount), false),
            new("Active Requests", FormatInt(s.TotalActiveRequestCount), false),
            new("Queued Requests", FormatInt(s.TotalQueuedRequestCount), s.QueuedRequestsWarning),
            new("Blocked Tasks", FormatInt(s.TotalBlockedTaskCount), s.BlockedTasksWarning),
            new("Active Parallel Threads", FormatLong(s.TotalActiveParallelThreadCount), false),
            new("Runnable Requests", FormatNullableInt(s.RunnableRequestCount), false),
            new("Total Requests", FormatNullableInt(s.TotalRequestCount), false),
            new("Runnable %", s.RunnablePercent.HasValue ? s.RunnablePercent.Value.ToString("F2") : "0.00", false, RunnablePercentHint),
            new("Total Physical Memory", FormatKbAsGb(s.TotalPhysicalMemoryKb), false),
            new("Available Physical Memory", FormatKbAsGb(s.AvailablePhysicalMemoryKb), s.PhysicalMemoryPressureWarning),
            new("System Memory State", string.IsNullOrEmpty(s.SystemMemoryStateDesc) ? "N/A" : s.SystemMemoryStateDesc, s.PhysicalMemoryPressureWarning),
            new("Physical Memory Pressure", s.PhysicalMemoryPressureWarning ? "Yes" : "No", s.PhysicalMemoryPressureWarning),
        };

        return rows;
    }

    /// <summary>
    /// The rolled-up CPU-scheduler pressure classification, a pure client-side derivation of install/47's
    /// <c>report.cpu_scheduler_pressure</c> (pressure_level + recommendation). The banding is the proc's CASE with one
    /// deliberate difference in order: runnable task queue &gt; 50 CRITICAL / &gt; 20 HIGH / &gt; 10 MEDIUM, then the
    /// collector's worker-exhaustion flag CRITICAL, then worker-utilization &gt; 90% HIGH, then the runnable-tasks and
    /// queued-requests warning flags. The proc tests the 90% arm before the flag; because the collector raises the flag
    /// at 90% of max worker threads, that order makes the proc's CRITICAL reachable only at exactly 90%, and a server
    /// at 95% reads HIGH there. Here the flag comes first, so above 90% the level is CRITICAL.
    /// </summary>
    public static CpuPressure ClassifyCpuPressure(ICpuSchedulerSnapshot s)
    {
        double workerUtil = s.MaxWorkersCount > 0
            ? (double)s.TotalCurrentWorkersCount / s.MaxWorkersCount * 100.0
            : 0;

        var level =
            s.TotalRunnableTasksCount > 50 ? "CRITICAL - High runnable task queue" :
            s.TotalRunnableTasksCount > 20 ? "HIGH - Moderate runnable task queue" :
            s.TotalRunnableTasksCount > 10 ? "MEDIUM - Some runnable tasks queued" :
            s.WorkerThreadExhaustionWarning ? "CRITICAL - Worker thread exhaustion warning" :
            workerUtil > 90 ? "HIGH - Worker thread exhaustion" :
            s.RunnableTasksWarning ? "HIGH - Runnable tasks warning" :
            s.QueuedRequestsWarning ? "MEDIUM - Queued requests warning" :
            "NORMAL";

        var recommendation =
            s.TotalRunnableTasksCount > 20 ? "CPU pressure detected - check for CPU-intensive queries, consider adding CPU cores" :
            s.WorkerThreadExhaustionWarning ? "Worker thread exhaustion - check max worker threads setting" :
            s.TotalQueuedRequestCount > 0 ? "Requests queued for execution - CPU or worker thread pressure" :
            "No CPU scheduler pressure detected";

        return new CpuPressure(level, recommendation);
    }

    private static string FormatInt(int value) => value.ToString("N0");

    private static string FormatLong(long value) => value.ToString("N0");

    /// <summary>
    /// #7578: the collector stores no Runnable % when no other request was running, so a missing share is 0.00 and not "N/A".
    /// The words match the web UI's tooltip on the same tile.
    /// </summary>
    public const string RunnablePercentHint = "The share of the other requests running at that moment that were waiting for a CPU. 0.0 means no other request was running.";

    private static string FormatNullableInt(int? value) => value.HasValue ? value.Value.ToString("N0") : "N/A";

    private static string FormatKbAsGb(long kb)
    {
        double gb = kb / 1024.0 / 1024.0;
        return gb >= 1 ? $"{gb:F1} GB" : $"{kb / 1024.0:F0} MB";
    }
}

/// <summary>One row of the CPU Scheduler latest-snapshot metric grid: a labelled scalar from the most recent
/// cpu_scheduler_stats row, plus whether the collector flagged it as pressure (drives the row highlight). A
/// single-row point-in-time collector has no natural top-N, so its "latest snapshot" is presented as this
/// metric/value list.</summary>
public sealed record CpuSchedulerMetricRow(string Metric, string Value, bool IsWarning, string? Hint = null);

/// <summary>The rolled-up CPU-scheduler pressure classification for the metric grid's headline rows: the
/// banded pressure level plus its paired recommendation, derived from install/47's
/// report.cpu_scheduler_pressure.</summary>
public sealed record CpuPressure(string Level, string Recommendation);
