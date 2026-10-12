/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The scheduler run-queue window the Runnable Queue alert (#5641) judges: what
/// <see cref="IAlertReadAdapter.GetRunnableQueueWindowAsync"/> hands the engine from the
/// <c>cpu_scheduler_stats</c> rows the collector already stores.
/// </summary>
/// <param name="SampleCount">How many samples with a non-NULL <c>avg_runnable_tasks_count</c> fell inside the
/// window. Zero means the collector delivered nothing usable in the window.</param>
/// <param name="AverageRunnableTasksPerScheduler">The AVERAGE of <c>avg_runnable_tasks_count</c> over those samples.
/// That column is the collector's own <c>AVG(runnable_tasks_count)</c> over <c>sys.dm_os_schedulers WHERE status =
/// 'VISIBLE ONLINE'</c> (runnable tasks per visible online scheduler), which is the number the bars were measured
/// on. Never the node or offline-CPU columns (#5656). 0 when <see cref="SampleCount"/> is 0.</param>
/// <param name="LatestSchedulerCount">The newest sample's <c>scheduler_count</c> (visible online schedulers), for the
/// summary text. Null when the window is empty or the column was NULL.</param>
/// <param name="LatestTotalRunnableTasks">The newest sample's <c>total_runnable_tasks_count</c> (the SUM across
/// schedulers). Null when the window is empty or the column was NULL. Carried for the detail block the alert
/// context builder adds (Lane 2 of #5641).</param>
/// <param name="NewestCollectionTime">The newest sample's own <c>collection_time</c> (naive UTC), so the engine can
/// refuse to re-fire on a window it already reported and stamp the sample age. <see cref="DateTime.MinValue"/> when
/// the window is empty.</param>
public sealed record RunnableQueueWindow(
    long SampleCount,
    double AverageRunnableTasksPerScheduler,
    int? LatestSchedulerCount,
    long? LatestTotalRunnableTasks,
    DateTime NewestCollectionTime)
{
    /// <summary>The window with no usable sample in it.</summary>
    public static RunnableQueueWindow Empty { get; } = new(0, 0, null, null, DateTime.MinValue);
}

/// <summary>
/// Grades the Runnable Queue window (#5641): runnable tasks per visible online scheduler, averaged over
/// <see cref="WindowMinutes"/> minutes of the stored <c>cpu_scheduler_stats</c> samples.
///
/// <para><b>Why the bars are 8 and 16.</b> Measured read-only on the production store: 43 healthy servers,
/// 4 days, 1-minute samples of <c>avg_runnable_tasks_count</c>, 5-minute average. No server reached 16. Two
/// servers reached 8 (21 and 4 samples, maxima 13.7 and 9.7). Eight servers reached 4, which is too noisy to
/// page on. The incident that asked for the alert ran at about 57 per scheduler (228 runnable tasks on 4
/// schedulers), and another server's incident-window peak was about 25. So Warning at 8 is a rare, real queue
/// and Critical at 16 is a queue no healthy server in the sample ever showed.</para>
///
/// <para><b>Why at least <see cref="MinimumSamples"/> samples.</b> A collection gap must not page on one busy
/// sample. With fewer samples in the window the check does not evaluate: it neither fires nor clears.</para>
/// </summary>
public static class RunnableQueueEvaluator
{
    /// <summary>The window the average is taken over, in minutes. The read's cutoff and the summary text use it.</summary>
    public const int WindowMinutes = 5;

    /// <summary>The fewest samples that make the window evaluable. At the shipped 1-minute cadence a full window holds 5.</summary>
    public const int MinimumSamples = 3;

    /// <summary>Shipped Warning bar: runnable tasks per scheduler, window average.</summary>
    public const int DefaultWarningPerScheduler = 8;

    /// <summary>Shipped Critical bar: runnable tasks per scheduler, window average.</summary>
    public const int DefaultCriticalPerScheduler = 16;

    /// <summary>The metric name on the alert row, the mute rule, the cooldown and the history grids.</summary>
    public const string MetricName = "Runnable Queue";

    /// <summary>
    /// The clamp both stores apply on read: the Warning bar is at least 1, and the Critical bar is never below
    /// the Warning bar (a Critical below Warning would make Warning unreachable). Returns the pair to use.
    /// </summary>
    public static (int Warning, int Critical) ClampBars(int warning, int critical)
    {
        var w = Math.Max(1, warning);
        var c = Math.Max(w, critical);
        return (w, c);
    }

    /// <summary>
    /// True when the window holds enough samples to judge. False is "not observed": the engine holds a standing
    /// alert where it is rather than clearing it, and fires nothing.
    /// </summary>
    public static bool IsEvaluable(RunnableQueueWindow? window) =>
        window is not null && window.SampleCount >= MinimumSamples;

    /// <summary>
    /// Grades the window's average against the two bars. Null when under the Warning bar or when the window is
    /// not evaluable (use <see cref="IsEvaluable"/> to tell those apart). The average is compared at the
    /// collector's own two-decimal precision, so a window that sits exactly at a bar is not lost to floating
    /// point error.
    /// </summary>
    public static Finding? Evaluate(RunnableQueueWindow? window, int warningPerScheduler, int criticalPerScheduler)
    {
        if (!IsEvaluable(window))
        {
            return null;
        }

        var (warning, critical) = ClampBars(warningPerScheduler, criticalPerScheduler);
        var average = Math.Round(window!.AverageRunnableTasksPerScheduler, 2, MidpointRounding.AwayFromZero);

        AlertSeverityLevel severity;
        int bar;
        if (average >= critical)
        {
            severity = AlertSeverityLevel.Critical;
            bar = critical;
        }
        else if (average >= warning)
        {
            severity = AlertSeverityLevel.Warning;
            bar = warning;
        }
        else
        {
            return null;
        }

        return new Finding(severity, average, bar, window.LatestSchedulerCount, window.LatestTotalRunnableTasks, window.NewestCollectionTime);
    }

    /// <summary>
    /// The summary sentence, plain and concrete: "Runnable tasks averaged 23.4 per scheduler over 5 minutes (4
    /// schedulers)." The scheduler clause is left out when the newest sample carried no scheduler count.
    /// </summary>
    public static string Summary(double averagePerScheduler, int? schedulerCount)
    {
        var scheduler = schedulerCount is > 0
            ? string.Create(CultureInfo.InvariantCulture, $" ({schedulerCount} scheduler{(schedulerCount == 1 ? "" : "s")})")
            : "";
        return string.Create(CultureInfo.InvariantCulture,
            $"Runnable tasks averaged {averagePerScheduler:N1} per scheduler over {WindowMinutes} minutes{scheduler}.");
    }

    /// <summary>One window over a bar.</summary>
    /// <param name="Severity">Warning, or Critical when the average reached the Critical bar.</param>
    /// <param name="AveragePerScheduler">The window average (two decimals).</param>
    /// <param name="BarCrossed">The bar of <paramref name="Severity"/>: the threshold the alert row stores.</param>
    /// <param name="SchedulerCount">The newest sample's scheduler count.</param>
    /// <param name="LatestTotalRunnableTasks">The newest sample's total runnable tasks.</param>
    /// <param name="NewestCollectionTime">The newest sample's collection time (naive UTC).</param>
    public sealed record Finding(
        AlertSeverityLevel Severity,
        double AveragePerScheduler,
        int BarCrossed,
        int? SchedulerCount,
        long? LatestTotalRunnableTasks,
        DateTime NewestCollectionTime)
    {
        /// <summary>The summary sentence for the alert.</summary>
        public string Summary => RunnableQueueEvaluator.Summary(AveragePerScheduler, SchedulerCount);

        /// <summary>The "current value" text the alert row shows.</summary>
        public string CurrentValueText =>
            string.Create(CultureInfo.InvariantCulture, $"{AveragePerScheduler:N1} per scheduler");

        /// <summary>The "threshold" text the alert row shows.</summary>
        public string ThresholdText =>
            string.Create(CultureInfo.InvariantCulture, $"{BarCrossed} per scheduler");
    }
}
