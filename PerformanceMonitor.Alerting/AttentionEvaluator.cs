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

/// <summary>One <c>count</c> row of <c>attention_events</c>: the exact number of attentions the event_counter saw since the
/// prior count row (#5640). The collector writes one only for a run whose delta is above zero.</summary>
/// <param name="CollectionTime">The run's <c>collection_time</c> (naive UTC, microseconds kept).</param>
/// <param name="Count">The row's <c>attention_count</c>.</param>
public sealed record AttentionCountRow(DateTime CollectionTime, long Count);

/// <summary>Sampled attention events (the ring buffer's kept events, one <c>sample</c> row each) of one run, for one
/// (database, client application). A sample is detail, never the total (#5640).</summary>
/// <param name="CollectionTime">The collection time of the run that kept the samples.</param>
/// <param name="DatabaseName">The event's database, or null when the row carries none.</param>
/// <param name="ClientAppName">The event's client application, or null when the row carries none.</param>
/// <param name="Samples">How many sample rows this group holds.</param>
public sealed record AttentionSampleGroup(DateTime CollectionTime, string? DatabaseName, string? ClientAppName, long Samples);

/// <summary>One entry of a top-5 list in the read: a statement fingerprint, a database or a client application, ranked by
/// its sampled event count over the range the read covers (the alert card's covered span, or the whole lookback when the span kept
/// no samples; see <see cref="AttentionWindow.TopsFromLookback"/>).</summary>
/// <param name="Name">The fingerprint, database name or application name. Rows that carry none are not listed.</param>
/// <param name="Samples">Sampled events carrying it in the lookback.</param>
/// <param name="SampleText">For a statement fingerprint, one sample's statement text (already through the statement filter);
/// null for the other two lists.</param>
public sealed record AttentionTopItem(string Name, long Samples, string? SampleText = null);

/// <summary>
/// The Client Timeouts alert's whole read (#5640), as plain data with no judgment (the adapter contract,
/// <see cref="IAlertReadAdapter.GetAttentionWindowAsync"/>).
/// </summary>
/// <param name="RunTimes">Every SUCCESSFUL attention collector run in the lookback, from the collection log, newest first. A
/// quiet run writes no row to <c>attention_events</c>, so the log is the only proof the collector ran.</param>
/// <param name="Counts">The <c>count</c> rows in the lookback, each with its own time.</param>
/// <param name="SampleGroups">The <c>sample</c> rows in the lookback grouped by (run, database, application).</param>
/// <param name="TopStatements">The top 5 statement fingerprints by sampled events, each with one sample text.</param>
/// <param name="TopDatabases">The top 5 databases by sampled events.</param>
/// <param name="TopApplications">The top 5 client applications by sampled events.</param>
/// <param name="TopsFromLookback">False (the default): the three top lists cover the samples inside the covered span
/// (<see cref="AttentionEvaluator.SpanBoundary"/> onward), the alert card's reading. True: the span kept no samples, so the lists
/// cover the whole lookback and the card says so in its headings (round 2 of #5714, L6).</param>
public sealed record AttentionWindow(
    IReadOnlyList<DateTime> RunTimes,
    IReadOnlyList<AttentionCountRow> Counts,
    IReadOnlyList<AttentionSampleGroup> SampleGroups,
    IReadOnlyList<AttentionTopItem> TopStatements,
    IReadOnlyList<AttentionTopItem> TopDatabases,
    IReadOnlyList<AttentionTopItem> TopApplications,
    bool TopsFromLookback = false)
{
    /// <summary>No runs, no rows: what a read of a server that never ran the collector returns.</summary>
    public static AttentionWindow Empty { get; } = new(
        Array.Empty<DateTime>(), Array.Empty<AttentionCountRow>(), Array.Empty<AttentionSampleGroup>(),
        Array.Empty<AttentionTopItem>(), Array.Empty<AttentionTopItem>(), Array.Empty<AttentionTopItem>());
}

/// <summary>Sampled events of one (database, client application) inside the covered span.</summary>
public sealed record AttentionGroup(string? DatabaseName, string? ClientAppName, long Samples);

/// <summary>
/// What <see cref="AttentionEvaluator.Read"/> took from the window: the covered span, the exact count in it and the
/// sampled events by group.
/// </summary>
/// <param name="TotalCount">The exact number of attentions in the span (the sum of the count rows of the taken runs).</param>
/// <param name="CoveredMinutes">Minutes from the run BEFORE the oldest taken run to the newest run (at least 1).</param>
/// <param name="RunCount">How many successful runs the span takes.</param>
/// <param name="NewestRunTime">The newest successful run (naive UTC): the fresh-sample marker.</param>
/// <param name="Groups">Sampled events in the span by (database, application); empty when the span kept no samples.</param>
public sealed record AttentionReading(
    long TotalCount,
    double CoveredMinutes,
    int RunCount,
    DateTime NewestRunTime,
    IReadOnlyList<AttentionGroup> Groups)
{
    /// <summary>All sampled events in the span.</summary>
    public long SampleTotal => Groups.Sum(g => g.Samples);
}

/// <summary>
/// Grades the Client Timeouts alert (#5640): client attentions (a client cancelled or timed out a query) per minute, from the
/// <c>attention_events</c> rows the attention collector writes.
///
/// <para><b>The count model.</b> The collector writes one <c>count</c> row per run, only when the session's event_counter
/// moved, holding the exact number of attentions since the prior run. The other rows are SAMPLES, one per kept ring-buffer
/// event, with database, application and statement; they are never the total. A quiet run writes nothing, so "no rows" never
/// means the collector ran: freshness and the minutes covered come from the collection log's successful runs.</para>
///
/// <para><b>The rate rule.</b> Take the successful runs newest first, and add runs until the span from the run BEFORE the
/// oldest taken run to the newest run reaches <see cref="WindowMinutes"/> (3) minutes, or the history runs out. The rate is
/// the sum of the taken runs' count rows divided by that span in minutes (at least 1). That is exact: each count row covers
/// the time since the run before it, so the taken rows cover exactly the span, and a failed run inside it only widens the
/// span of the next success, whose counter delta spans both. A storm at three times the bar still fires on the first run after
/// it starts, as the same averaging over three minutes does for #5641.</para>
///
/// <para><b>Not observed.</b> With no successful run within max(10 minutes, 4 times the median of the newest 5 gaps between
/// runs), capped at the lookback, or with fewer than two runs (no span), the alert neither fires nor clears. The window
/// rule is <see cref="RunnableQueueEvaluator.FallbackWindowOfTimes(IReadOnlyList{DateTime})"/>: one definition for both alerts.
/// A standing alert unevaluated for <see cref="StaleIncidentMinutes"/> is resolved by the engine, with the reason stated.</para>
///
/// <para><b>Known edge, deliberate.</b> The first run after a service restart stages the counter and writes no count row, so
/// the span before it is counted as quiet. That under-counts, which is the safe direction for a paging alert; nothing more
/// is built for it.</para>
///
/// <para><b>Mute is proportional.</b> A mute rule matches a (database, application) group, but the exact count has no groups,
/// only the samples do. The judged count is the exact count times the unmuted share of the sampled events in the span.
/// With no samples in the span the full count is judged. When the full count crosses a bar and the judged count does not, the
/// fire is recorded Muted. When the proportion changed the count, the detail says it is an estimate from N sampled
/// events.</para>
///
/// <para><b>The bars.</b> Warning at 10 per minute, Critical at 60 per minute, OFF by default. These are a guess from "a
/// healthy application times out rarely", not a measured normal rate: none exists yet. The alert stays off until the bars are
/// checked against captured data.</para>
/// </summary>
public static class AttentionEvaluator
{
    /// <summary>The metric name on the alert row, the mute rule, the cooldown and the history grids. Declared once, in
    /// <see cref="AlertFamily.ClientTimeoutsMetric"/>, because the assemblies below this one read it too.</summary>
    public const string MetricName = AlertFamily.ClientTimeoutsMetric;

    /// <summary>The span the rate is averaged over, in minutes.</summary>
    public const int WindowMinutes = 3;

    /// <summary>How far back the read looks, in minutes. Equal to <see cref="RunnableQueueEvaluator.ReadLookbackMinutes"/>
    /// (a test pins it): the shared freshness rule caps its window there.</summary>
    public const int ReadLookbackMinutes = 60;

    /// <summary>How far past the newest successful run a row may sit and still belong to the span, in seconds. The collection
    /// log stamps a run when it starts and the rows carry the time the runner took a moment later; a row further out than this
    /// belongs to a run the log has not recorded yet (the log row is written after the data rows), so it is left for the next
    /// evaluation instead of counted in a span that does not cover it.</summary>
    public const int RowSkewSeconds = 30;

    /// <summary>A standing alert with no evaluation for this many minutes is resolved by the engine ("no fresh collector
    /// runs"), so a collector that was disabled, slowed or broken mid-incident cannot hold it open forever.</summary>
    public const int StaleIncidentMinutes = 30;

    /// <summary>Shipped Warning bar: attentions per minute.</summary>
    public const int DefaultWarningPerMinute = 10;

    /// <summary>Shipped Critical bar: attentions per minute.</summary>
    public const int DefaultCriticalPerMinute = 60;

    /// <summary>The clamp both stores apply on read: the Warning bar is at least 1 and the Critical bar is never below it.
    /// The same rule as the Runnable Queue pair.</summary>
    public static (int Warning, int Critical) ClampBars(int warning, int critical) =>
        RunnableQueueEvaluator.ClampBars(warning, critical);

    /// <summary>
    /// Applies the window and rate rules to <paramref name="window"/> at evaluation time <paramref name="now"/> (naive UTC,
    /// the clock of the collector's times). Null is "not observed" (see the class remarks).
    /// </summary>
    public static AttentionReading? Read(AttentionWindow? window, DateTime now)
    {
        if (window is null || window.RunTimes.Count == 0)
        {
            return null;
        }

        var runs = window.RunTimes.Distinct().OrderByDescending(t => t).ToList();

        /* The freshness rule is #5709's, applied to the collector's runs: the newest run must be inside
           max(10 minutes, 4 x the median of the newest 5 run gaps), capped at the lookback. */
        if (runs[0] < now.AddMinutes(-RunnableQueueEvaluator.FallbackWindowOfTimes(runs)))
        {
            return null;
        }

        /* The span needs a run BEFORE the oldest taken run, so one run alone is no observation. */
        if (runs.Count < 2)
        {
            return null;
        }

        return Take(window, runs, OldestTaken(runs));
    }

    /// <summary>The index of the oldest run <see cref="Read"/> takes: runs are added newest first until the span from the run before
    /// the oldest taken run to the newest run reaches <see cref="WindowMinutes"/>, or the history runs out. Needs at least two runs.</summary>
    private static int OldestTaken(List<DateTime> runs)
    {
        var newest = runs[0];
        var oldestTaken = 0;
        for (var k = 0; k < runs.Count - 1; k++)
        {
            oldestTaken = k;
            if ((newest - runs[k + 1]).TotalMinutes >= WindowMinutes)
            {
                break;
            }
        }

        return oldestTaken;
    }

    /// <summary>
    /// The instant after which a row belongs to the span <see cref="Read"/> judges (round 2 of #5714, L6): the midpoint between the
    /// base run and the oldest taken run. The alert reads' adapters start the card's top lists here, so the top statements,
    /// databases and applications cover the alert's own few minutes and not the whole lookback. Null when fewer than two runs
    /// make no span. Freshness is not asked: a stale window is "not observed" and never reaches a card.
    /// </summary>
    public static DateTime? SpanBoundary(IReadOnlyList<DateTime> runTimes)
    {
        var runs = runTimes.Distinct().OrderByDescending(t => t).ToList();
        return runs.Count < 2 ? null : Boundary(runs, OldestTaken(runs));
    }

    /// <summary>
    /// Where the alert read's three top lists start (round 2 of #5714, L6), and whether they are the lookback's. The span's boundary
    /// when the span kept sampled events; otherwise (no span, or a span with no samples) the whole lookback, flagged so the card
    /// says its real range. Both alert adapters call this once, after the runs and sample groups are read.
    /// </summary>
    public static (DateTime Floor, bool FromLookback) TopListFloor(
        IReadOnlyList<DateTime> runTimes, IReadOnlyList<AttentionSampleGroup> sampleGroups, DateTime lookbackFloor)
    {
        if (SpanBoundary(runTimes) is { } boundary && sampleGroups.Any(g => g.CollectionTime > boundary))
        {
            return (boundary, false);
        }

        return (lookbackFloor, true);
    }

    private static DateTime Boundary(List<DateTime> runs, int oldestTaken)
    {
        var baseRun = runs[oldestTaken + 1];
        return baseRun + ((runs[oldestTaken] - baseRun) / 2);
    }

    /// <summary>
    /// The read surface's reading (get_client_timeouts, #5640): the SAME freshness and span rules as <see cref="Read"/>, but the span
    /// takes EVERY successful run in the window (from the oldest run in it to the newest) instead of stopping at
    /// <see cref="WindowMinutes"/>, so the rate is the exact count over the whole range asked for. Null when the evaluator would say
    /// "not observed" (no fresh run, or fewer than two runs). The first run in the window is the BASE, not a taken run: its count row
    /// covers the time before the window.
    /// </summary>
    public static AttentionReading? ReadSpan(AttentionWindow? window, DateTime now)
    {
        if (window is null || window.RunTimes.Count == 0)
        {
            return null;
        }

        var runs = window.RunTimes.Distinct().OrderByDescending(t => t).ToList();
        if (runs[0] < now.AddMinutes(-RunnableQueueEvaluator.FallbackWindowOfTimes(runs)) || runs.Count < 2)
        {
            return null;
        }

        return Take(window, runs, runs.Count - 2);
    }

    private static AttentionReading Take(AttentionWindow window, List<DateTime> runs, int oldestTaken)
    {
        var newest = runs[0];
        var baseRun = runs[oldestTaken + 1];
        var covered = Math.Max(1d, (newest - baseRun).TotalMinutes);

        /* A row belongs to the span when it sits after the midpoint between the base run and the oldest taken run. The
           collection log stamps a run when it starts and the rows carry the time the runner took a moment later, so the
           two clocks differ by seconds; the midpoint keeps the base run's own row out and the oldest taken run's row in
           for any cadence. */
        var boundary = Boundary(runs, oldestTaken);

        var ceiling = newest.AddSeconds(RowSkewSeconds);

        var total = window.Counts.Where(c => c.CollectionTime > boundary && c.CollectionTime <= ceiling).Sum(c => c.Count);
        var groups = window.SampleGroups
            .Where(g => g.CollectionTime > boundary && g.CollectionTime <= ceiling)
            .GroupBy(g => (g.DatabaseName, g.ClientAppName))
            .Select(g => new AttentionGroup(g.Key.DatabaseName, g.Key.ClientAppName, g.Sum(x => x.Samples)))
            .ToList();

        return new AttentionReading(total, covered, oldestTaken + 1, newest, groups);
    }

    /// <summary>
    /// Grades the reading against the two bars. Null when no reading, or when the EXACT count's rate is under the Warning
    /// bar. A rate is compared at two decimals, so a rate sitting exactly at a bar is not lost to floating point.
    /// <paramref name="isGroupMuted"/> is asked once per sampled (database, application) group, and only when the exact
    /// rate crossed a bar.
    /// </summary>
    public static Finding? Evaluate(
        AttentionReading? reading, int warningPerMinute, int criticalPerMinute, Func<string?, string?, bool>? isGroupMuted = null)
    {
        if (reading is null)
        {
            return null;
        }

        var (warning, critical) = ClampBars(warningPerMinute, criticalPerMinute);
        var fullRate = Math.Round(reading.TotalCount / reading.CoveredMinutes, 2, MidpointRounding.AwayFromZero);
        if (!Grade(fullRate, warning, critical, out var fullSeverity, out var fullBar))
        {
            return null;
        }

        var sampleTotal = reading.SampleTotal;
        var judgedCount = (double)reading.TotalCount;
        var estimated = false;
        if (sampleTotal == 0 && isGroupMuted is not null && isGroupMuted(null, null))
        {
            /* Round 2 of #5714 (M1): a span that kept no samples is judged at the full count, and the server-wide mute still
               applies. A rule with a database or app pattern never matches a null database and app, so only a rule on the
               server and metric alone is asked here and can mute. The finding is Muted at the full count's grade. */
            return new Finding(fullSeverity, fullRate, fullBar, reading.TotalCount, judgedCount, reading.CoveredMinutes,
                reading.RunCount, sampleTotal, estimated, Muted: true, reading.NewestRunTime);
        }

        if (sampleTotal > 0 && isGroupMuted is not null)
        {
            var unmuted = reading.Groups.Where(g => !isGroupMuted(g.DatabaseName, g.ClientAppName)).Sum(g => g.Samples);
            if (unmuted < sampleTotal)
            {
                judgedCount = reading.TotalCount * (double)unmuted / sampleTotal;
                estimated = true;
            }
        }

        var judgedRate = Math.Round(judgedCount / reading.CoveredMinutes, 2, MidpointRounding.AwayFromZero);
        if (Grade(judgedRate, warning, critical, out var judgedSeverity, out var judgedBar))
        {
            return new Finding(judgedSeverity, judgedRate, judgedBar, reading.TotalCount, judgedCount, reading.CoveredMinutes,
                reading.RunCount, sampleTotal, estimated, Muted: false, reading.NewestRunTime);
        }

        /* The exact count crossed a bar, the unmuted share did not: the fire is recorded Muted at the exact count's grade. */
        return new Finding(fullSeverity, fullRate, fullBar, reading.TotalCount, judgedCount, reading.CoveredMinutes,
            reading.RunCount, sampleTotal, estimated, Muted: true, reading.NewestRunTime);
    }

    private static bool Grade(double rate, int warning, int critical, out AlertSeverityLevel severity, out int bar)
    {
        if (rate >= critical)
        {
            severity = AlertSeverityLevel.Critical;
            bar = critical;
            return true;
        }

        if (rate >= warning)
        {
            severity = AlertSeverityLevel.Warning;
            bar = warning;
            return true;
        }

        severity = AlertSeverityLevel.Warning;
        bar = 0;
        return false;
    }

    /// <summary>
    /// The summary sentence: "412 client timeouts in the last 3 minutes (137.3 per minute)." and, when the mute proportion
    /// was applied, "Estimated from 20 sampled events: 31.2 per minute is unmuted."
    /// </summary>
    public static string Summary(Finding finding)
    {
        var minutes = Math.Max(1, (int)Math.Round(finding.CoveredMinutes, MidpointRounding.AwayFromZero));
        var text = string.Create(CultureInfo.InvariantCulture,
            $"{finding.TotalCount:N0} client timeout{(finding.TotalCount == 1 ? "" : "s")} in the last {minutes} minute{(minutes == 1 ? "" : "s")} ({finding.TotalCount / finding.CoveredMinutes:N1} per minute).");
        if (finding.Estimated)
        {
            text += string.Create(CultureInfo.InvariantCulture,
                $" Estimated from {finding.SampledEvents:N0} sampled events: {finding.JudgedCount / finding.CoveredMinutes:N1} per minute is not muted.");
        }

        return text;
    }

    /// <summary>One reading over a bar.</summary>
    /// <param name="Severity">Warning, or Critical when the rate reached the Critical bar.</param>
    /// <param name="RatePerMinute">The rate the grade rests on, two decimals: the unmuted (judged) rate, or the exact rate for a Muted finding.</param>
    /// <param name="BarCrossed">The bar of <paramref name="Severity"/>: the threshold the alert row stores.</param>
    /// <param name="TotalCount">The exact attentions in the span.</param>
    /// <param name="JudgedCount">The count after the mute proportion (equal to <paramref name="TotalCount"/> when nothing was muted).</param>
    /// <param name="CoveredMinutes">The span, in minutes.</param>
    /// <param name="RunCount">Successful runs the span takes.</param>
    /// <param name="SampledEvents">Sampled events in the span.</param>
    /// <param name="Estimated">True when the mute proportion changed the count: the judged count is an estimate from the samples.</param>
    /// <param name="Muted">True when the exact count crossed a bar and the judged count did not: record the fire as Muted.</param>
    /// <param name="NewestRunTime">The newest run's time (naive UTC).</param>
    public sealed record Finding(
        AlertSeverityLevel Severity,
        double RatePerMinute,
        int BarCrossed,
        long TotalCount,
        double JudgedCount,
        double CoveredMinutes,
        int RunCount,
        long SampledEvents,
        bool Estimated,
        bool Muted,
        DateTime NewestRunTime)
    {
        /// <summary>The summary sentence for the alert.</summary>
        public string Summary => AttentionEvaluator.Summary(this);

        /// <summary>The "current value" text the alert row shows.</summary>
        public string CurrentValueText =>
            string.Create(CultureInfo.InvariantCulture, $"{RatePerMinute:N1} per minute");

        /// <summary>The "threshold" text the alert row shows.</summary>
        public string ThresholdText =>
            string.Create(CultureInfo.InvariantCulture, $"{BarCrossed} per minute");
    }
}
