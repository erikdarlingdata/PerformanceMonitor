/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// One COLLECTION of the Runnable Queue alert's read (#5641): everything <c>cpu_scheduler_stats</c> holds for one
/// <c>collection_time</c>, folded to one number. The collector can write SEVERAL rows for one collection_time, each
/// covering only a subset of the schedulers while every row still says the full <c>scheduler_count</c> (#5710; old
/// rows stay split), so averaging ROWS is wrong. The read folds them: one row uses its own
/// <c>avg_runnable_tasks_count</c>; several rows use <c>SUM(total_runnable_tasks_count) / MAX(scheduler_count)</c>.
/// </summary>
/// <param name="CollectionTime">The collection's <c>collection_time</c> (naive UTC, microseconds kept).</param>
/// <param name="PerScheduler">Runnable tasks per visible online scheduler for the whole collection. The number the
/// bars were measured on. Never the node or offline-CPU columns (#5656).</param>
/// <param name="TotalRunnableTasks">The collection's <c>SUM(total_runnable_tasks_count)</c> across its rows. Null when
/// the column was NULL. Carried for the detail block the alert context builder adds (Lane 2 of #5641).</param>
/// <param name="SchedulerCount">The collection's <c>MAX(scheduler_count)</c> (visible online schedulers), for the
/// summary text. Null when the column was NULL.</param>
public sealed record RunnableQueueSample(
    DateTime CollectionTime,
    double PerScheduler,
    long? TotalRunnableTasks,
    int? SchedulerCount);

/// <summary>
/// The collections <see cref="RunnableQueueEvaluator.Read"/> judged at one evaluation time: the average, how many
/// collections it covers and how long they span, plus the newest collection's details for the summary, the
/// detail block and the engine's fresh-sample guard.
/// </summary>
/// <param name="AveragePerScheduler">The average of the covered collections' <see cref="RunnableQueueSample.PerScheduler"/>.</param>
/// <param name="SampleCount">How many collections the average covers (3 or more in the 5-minute rule, exactly 3 in the fallback).</param>
/// <param name="SpanMinutes">Whole minutes from the oldest covered collection to the evaluation time (at least 1), for the summary.</param>
/// <param name="SchedulerCount">The newest collection's scheduler count.</param>
/// <param name="LatestTotalRunnableTasks">The newest collection's total runnable tasks.</param>
/// <param name="NewestCollectionTime">The newest collection's time (naive UTC).</param>
public sealed record RunnableQueueReading(
    double AveragePerScheduler,
    int SampleCount,
    int SpanMinutes,
    int? SchedulerCount,
    long? LatestTotalRunnableTasks,
    DateTime NewestCollectionTime);

/// <summary>
/// Grades the Runnable Queue alert (#5641): runnable tasks per visible online scheduler, averaged over the stored
/// <c>cpu_scheduler_stats</c> collections.
///
/// <para><b>The rule, at evaluation time t.</b> If <see cref="MinimumSamples"/> (3) or more collections fall at or
/// after t - <see cref="WindowMinutes"/> (5) minutes, the value is their average. Otherwise, if the newest 3
/// collections all fall at or after t - <see cref="FallbackWindowMinutes"/> (10 minutes, or 4 times the collector's
/// cadence when that is longer) minutes, the value is the average of those 3. Otherwise there is no evaluation: the
/// alert neither fires nor clears, and its state holds. A standing alert with no evaluation for
/// <see cref="StaleIncidentMinutes"/> (30) minutes is resolved by the engine, with the reason stated.</para>
///
/// <para><b>Why the fallback.</b> Under load the collector ran late (gaps of 2.3, 4 and 5 minutes on the 2026-10-09
/// incident), so "at least 3 collections in 5 minutes" went blind exactly when the alert mattered. Three collections
/// that are still fresh within 10 minutes are enough to judge.</para>
///
/// <para><b>Why the window scales with the cadence (round 1 of #5709).</b> The shipped Low-Impact preset collects
/// <c>cpu_scheduler_stats</c> every 5 minutes, and at that cadence the newest three collections are a, a+5 and a+10
/// minutes old, so a fixed 10-minute window almost never held them and the alert sat blind while settings showed it
/// on. The cadence is taken from the samples' own spacing (the median gap over the
/// <see cref="ReadLookbackMinutes"/>-minute read), not from the configured schedule: the shared engine has no
/// schedule seam in either app, and the spacing is also what the collector ACTUALLY delivered (a disabled, late or
/// failing collector shows up in it too). At the 1-minute default the window stays 10 minutes, exactly as before.</para>
///
/// <para><b>Why the bars are 8 and 16.</b> Measured read-only on the production store: 43 healthy servers,
/// 4 days, 1-minute samples of <c>avg_runnable_tasks_count</c>, 5-minute average. No server reached 16. Two
/// servers reached 8 (21 and 4 samples, maxima 13.7 and 9.7). Eight servers reached 4, which is too noisy to
/// page on. The incident that asked for the alert ran at about 57 per scheduler (228 runnable tasks on 4
/// schedulers), and another server's incident-window peak was about 25. So Warning at 8 is a rare, real queue
/// and Critical at 16 is a queue no healthy server in the sample ever showed.</para>
///
/// <para><b>Why at least <see cref="MinimumSamples"/> collections.</b> A collection gap must not page on one busy
/// collection.</para>
/// </summary>
public static class RunnableQueueEvaluator
{
    /// <summary>The primary window the average is taken over, in minutes.</summary>
    public const int WindowMinutes = 5;

    /// <summary>The fallback freshness floor, in minutes: the newest <see cref="MinimumSamples"/> collections must all
    /// fall inside <see cref="FallbackWindowMinutes"/> (this, or <see cref="CadenceMultiplier"/> times the cadence when
    /// that is longer) when the primary window holds too few.</summary>
    public const int FallbackMinutes = 10;

    /// <summary>How the fallback window scales with the collector's cadence: 4 gaps, so the newest three collections
    /// (two gaps apart, the newest up to one gap old) still fit with slack for one late collection.</summary>
    public const int CadenceMultiplier = 4;

    /// <summary>How far back the read looks, in minutes: <see cref="CadenceMultiplier"/> times the slowest shipped
    /// cadence (15 minutes), which is also the widest fallback window. The longer read is what the cadence is derived
    /// from. At the 1-minute default it holds about 60 collections, from the index's newest range.</summary>
    public const int ReadLookbackMinutes = 60;

    /// <summary>A standing Runnable Queue alert with no evaluation for this many minutes is resolved by the engine
    /// ("no fresh scheduler data"), so a collector that was disabled, slowed or broken mid-incident cannot hold it open
    /// forever.</summary>
    public const int StaleIncidentMinutes = 30;

    /// <summary>The fewest collections that make an evaluation. At the shipped 1-minute cadence a full primary window holds 5.</summary>
    public const int MinimumSamples = 3;

    /// <summary>Shipped Warning bar: runnable tasks per scheduler, collection average.</summary>
    public const int DefaultWarningPerScheduler = 8;

    /// <summary>Shipped Critical bar: runnable tasks per scheduler, collection average.</summary>
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

    /// <summary>How many of the newest gaps the cadence is the median of (6 samples). Round 2 of #5709: a median over
    /// the whole 60-minute read took about 50 minutes to register a switch from 1-minute to 5-minute collection, so the
    /// alert was blind for that long and an open incident was falsely resolved as stale. Five gaps register the switch
    /// after three slower ones, and a single outage gap is still one of five and cannot move the median.</summary>
    public const int CadenceGapCount = 5;

    /// <summary>
    /// The collector's cadence as the samples show it: the median of the newest <see cref="CadenceGapCount"/> gaps
    /// between consecutive collections, in minutes (fewer when fewer exist; the lower of the two middle gaps when the
    /// count is even, so with only two gaps one outage cannot double the cadence). Null with fewer than 3 samples
    /// (two gaps). The median ignores one outage gap or a few late collections, so a busy host does not widen the
    /// window, and only the newest gaps count, so a changed cadence is picked up within three collections.
    /// </summary>
    public static double? MedianGapMinutes(IReadOnlyList<RunnableQueueSample>? samples)
    {
        if (samples is null || samples.Count < MinimumSamples)
        {
            return null;
        }

        var times = samples.Select(s => s.CollectionTime).OrderByDescending(t => t).Take(CadenceGapCount + 1).ToList();
        var gaps = new List<double>(times.Count - 1);
        for (var i = 1; i < times.Count; i++)
        {
            gaps.Add((times[i - 1] - times[i]).TotalMinutes);
        }

        gaps.Sort();
        return gaps[(gaps.Count - 1) / 2];
    }

    /// <summary>
    /// The fallback window in minutes: <see cref="FallbackMinutes"/> (10), or <see cref="CadenceMultiplier"/> (4) times
    /// the median collection gap when that is longer, never more than <see cref="ReadLookbackMinutes"/>. Samples that
    /// give no cadence (fewer than 3) get the 10-minute floor.
    /// </summary>
    public static double FallbackWindowMinutes(IReadOnlyList<RunnableQueueSample>? samples)
    {
        var gap = MedianGapMinutes(samples);
        return gap is null
            ? FallbackMinutes
            : Math.Min(ReadLookbackMinutes, Math.Max(FallbackMinutes, CadenceMultiplier * gap.Value));
    }

    /// <summary>
    /// Applies the window rule to the collections at evaluation time <paramref name="now"/> (on the same clock as
    /// the collections' <c>collection_time</c>, naive UTC). Null is "not observed": fewer than 3 collections in the
    /// primary window and the newest 3 not all inside the fallback (10 minutes, or 4 times the cadence when longer). The engine then holds a standing alert where it
    /// is rather than clearing it, and fires nothing. The samples may arrive in any order.
    /// </summary>
    public static RunnableQueueReading? Read(IReadOnlyList<RunnableQueueSample>? samples, DateTime now)
    {
        if (samples is null || samples.Count < MinimumSamples)
        {
            return null;
        }

        var newestFirst = samples.OrderByDescending(s => s.CollectionTime).ToList();
        var primaryFloor = now.AddMinutes(-WindowMinutes);
        var inPrimary = newestFirst.Where(s => s.CollectionTime >= primaryFloor).ToList();

        List<RunnableQueueSample> used;
        if (inPrimary.Count >= MinimumSamples)
        {
            used = inPrimary;
        }
        else
        {
            used = newestFirst.Take(MinimumSamples).ToList();
            if (used[^1].CollectionTime < now.AddMinutes(-FallbackWindowMinutes(samples)))
            {
                return null;
            }
        }

        var newest = used[0];
        var span = (int)Math.Round((now - used[^1].CollectionTime).TotalMinutes, MidpointRounding.AwayFromZero);
        return new RunnableQueueReading(
            used.Average(s => s.PerScheduler),
            used.Count,
            Math.Max(1, span),
            newest.SchedulerCount,
            newest.TotalRunnableTasks,
            newest.CollectionTime);
    }

    /// <summary>
    /// Grades the reading's average against the two bars. Null when under the Warning bar or when there is no
    /// reading (use <see cref="Read"/> to tell those apart). The average is compared at the collector's own
    /// two-decimal precision, so an average that sits exactly at a bar is not lost to floating point error.
    /// </summary>
    public static Finding? Evaluate(RunnableQueueReading? reading, int warningPerScheduler, int criticalPerScheduler)
    {
        if (reading is null)
        {
            return null;
        }

        var (warning, critical) = ClampBars(warningPerScheduler, criticalPerScheduler);
        var average = Math.Round(reading.AveragePerScheduler, 2, MidpointRounding.AwayFromZero);

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

        return new Finding(severity, average, bar, reading.SampleCount, reading.SpanMinutes,
            reading.SchedulerCount, reading.LatestTotalRunnableTasks, reading.NewestCollectionTime);
    }

    /// <summary>
    /// The summary sentence, plain and concrete: "Runnable tasks averaged 23.6 per scheduler over the last 3
    /// collections (5 minutes, 4 schedulers)." The scheduler clause is left out when the newest collection carried
    /// no scheduler count.
    /// </summary>
    public static string Summary(double averagePerScheduler, int sampleCount, int spanMinutes, int? schedulerCount)
    {
        var scheduler = schedulerCount is > 0
            ? string.Create(CultureInfo.InvariantCulture, $", {schedulerCount} scheduler{(schedulerCount == 1 ? "" : "s")}")
            : "";
        return string.Create(CultureInfo.InvariantCulture,
            $"Runnable tasks averaged {averagePerScheduler:N1} per scheduler over the last {sampleCount} collections ({spanMinutes} minute{(spanMinutes == 1 ? "" : "s")}{scheduler}).");
    }

    /// <summary>One reading over a bar.</summary>
    /// <param name="Severity">Warning, or Critical when the average reached the Critical bar.</param>
    /// <param name="AveragePerScheduler">The collections' average (two decimals).</param>
    /// <param name="BarCrossed">The bar of <paramref name="Severity"/>: the threshold the alert row stores.</param>
    /// <param name="SampleCount">How many collections the average covers.</param>
    /// <param name="SpanMinutes">Whole minutes from the oldest covered collection to the evaluation.</param>
    /// <param name="SchedulerCount">The newest collection's scheduler count.</param>
    /// <param name="LatestTotalRunnableTasks">The newest collection's total runnable tasks.</param>
    /// <param name="NewestCollectionTime">The newest collection's time (naive UTC).</param>
    public sealed record Finding(
        AlertSeverityLevel Severity,
        double AveragePerScheduler,
        int BarCrossed,
        int SampleCount,
        int SpanMinutes,
        int? SchedulerCount,
        long? LatestTotalRunnableTasks,
        DateTime NewestCollectionTime)
    {
        /// <summary>The summary sentence for the alert.</summary>
        public string Summary => RunnableQueueEvaluator.Summary(AveragePerScheduler, SampleCount, SpanMinutes, SchedulerCount);

        /// <summary>The "current value" text the alert row shows.</summary>
        public string CurrentValueText =>
            string.Create(CultureInfo.InvariantCulture, $"{AveragePerScheduler:N1} per scheduler");

        /// <summary>The "threshold" text the alert row shows.</summary>
        public string ThresholdText =>
            string.Create(CultureInfo.InvariantCulture, $"{BarCrossed} per scheduler");
    }
}
