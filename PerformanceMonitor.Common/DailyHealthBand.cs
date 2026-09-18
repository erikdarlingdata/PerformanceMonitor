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

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// The composite health tier for a single day, used to color a Performance Calendar day cell at a
    /// glance. This is deliberately a <b>composite</b> band (folding deadlocks, collection failures, CPU,
    /// blocking, memory pressure and alerts into one verdict) rather than any single metric, so a month of
    /// cells reads as green / amber / red days.
    /// </summary>
    public enum DailyHealthBand
    {
        /// <summary>No collection happened that day — rendered neutral grey (the muted theme tier).</summary>
        NoData = 0,

        /// <summary>Collected, nothing elevated — green.</summary>
        Healthy = 1,

        /// <summary>Elevated but not critical (moderate CPU, some blocking, memory pressure, or alerts) — amber.</summary>
        Warning = 2,

        /// <summary>A serious day (a critical deadlock rate, collection failures, sustained high CPU, heavy
        /// blocking, or severe memory pressure) — red.</summary>
        Critical = 3,
    }

    /// <summary>
    /// The drill targets a Performance Calendar day-detail panel can offer. Which targets are shown for a
    /// given day is decided by <see cref="DailyHealthBandCalculator.AvailableDrills"/> (shared, testable);
    /// how each one navigates — which inner tab, and scoping the toolbar to the clicked day's window — is
    /// app-specific and handled by each host (Lite's <c>ServerTab</c>, the Darling <c>ViewerServerTab</c>),
    /// reusing the same tab-switch + time-window mechanism as the per-chart "Show Active Queries at This
    /// Time" drill.
    /// </summary>
    public enum DayDrillTarget
    {
        /// <summary>Jump to the Deadlocks grid scoped to the day — offered only when the day had deadlocks.</summary>
        Deadlocks,

        /// <summary>Jump to the Blocked Process Reports grid scoped to the day — offered only when the day had blocking.</summary>
        Blocking,

        /// <summary>Jump to the Top Queries grid scoped to the day — offered on any collected day.</summary>
        TopQueries,
    }

    /// <summary>
    /// The per-day signal counts that feed <see cref="DailyHealthBandCalculator.Classify"/>. Every field is
    /// a whole-day roll-up over the same source views the Daily Summary already aggregates. A day with no
    /// collection at all sets <see cref="HasData"/> = false (all other fields ignored → <see cref="DailyHealthBand.NoData"/>).
    /// </summary>
    public readonly record struct DailyHealthSignals
    {
        /// <summary>True when any collection ran that day. When false the band is always <see cref="DailyHealthBand.NoData"/>.</summary>
        public bool HasData { get; init; }

        /// <summary>Deadlocks captured in the window. Banded as a RATE over <see cref="Window"/> through the
        /// card band's own tiers (#3525) — see <see cref="DailyHealthBandCalculator.Classify"/>.</summary>
        public long Deadlocks { get; init; }

        /// <summary>
        /// How long the window these counts cover (#3525) — the denominator the deadlock rate is computed
        /// over. A calendar day is 24 hours; a fleet-sweep span is previous-sweep-to-now (sub-day).
        ///
        /// <para><b>A measurement property, so its <c>default</c> has to be unusable</b> — the
        /// <see cref="ServerHealthMetrics.DeadlockWindow"/> discipline, verbatim: <see cref="TimeSpan.Zero"/>
        /// means no window was declared, and the deadlock band then declines to compute a rate rather than
        /// dividing by zero. An undeclared (or sub-hour) window fails away from Healthy, never into
        /// Critical: a non-zero count reads Warning, a zero count simply stays out of the deadlock
        /// trigger.</para>
        /// </summary>
        public TimeSpan Window { get; init; }

        /// <summary>Collector runs that ended in ERROR that day. Any (&gt; 0) is Critical — a monitoring blind spot is itself serious.</summary>
        public long CollectionErrors { get; init; }

        /// <summary>CPU samples where total host CPU (SQL + other-process) was ≥ 80%. Sustained (see
        /// <see cref="DailyHealthThresholds.HighCpuCriticalSamples"/>) is Critical; a few is Warning.</summary>
        public long HighCpuEvents { get; init; }

        /// <summary>Blocking events that day (blocked-process reports, falling back to DMV blocking snapshots).
        /// Past <see cref="DailyHealthThresholds.BlockingCriticalEvents"/> is Critical; some is Warning.</summary>
        public long BlockingEvents { get; init; }

        /// <summary>Memory-pressure events (a process or system indicator ≥ 2) that were not severe. Any is Warning.</summary>
        public long MemoryPressureEvents { get; init; }

        /// <summary>Severe memory-pressure events (a process indicator ≥ 3) that day. Any (&gt; 0) is Critical.</summary>
        public long MemoryCriticalEvents { get; init; }

        /// <summary>Actionable (non-resolution) alerts raised that day. At/over
        /// <see cref="DailyHealthThresholds.AlertWarningCount"/> is Warning.</summary>
        public long AlertCount { get; init; }
    }

    /// <summary>
    /// The one, documented, tunable place for the Performance Calendar's day-banding thresholds. If a month
    /// of cells reads too red or too green in practice, adjust these — nothing else needs to change. The
    /// defaults are chosen to be continuous with the Daily Summary's long-standing single-day banding
    /// (high-CPU &gt; 5 and blocking &gt; 10 were the original Critical triggers).
    /// </summary>
    public sealed record DailyHealthThresholds
    {
        /// <summary>High-CPU samples (≥ 80% total host) at or above which the day is Critical ("sustained high CPU").
        /// Default 6 preserves the original "high_cpu_events &gt; 5" Critical rule.</summary>
        public int HighCpuCriticalSamples { get; init; } = 6;

        /// <summary>High-CPU samples at or above which the day is at least Warning ("moderate CPU"). Default 1:
        /// any 80%+ total-host-CPU moment is worth a second look, short of the sustained (Critical) count.</summary>
        public int HighCpuWarningSamples { get; init; } = 1;

        /// <summary>Blocking events at or above which the day is Critical. Default 11 preserves the original
        /// "blocking_events &gt; 10" Critical rule.</summary>
        public int BlockingCriticalEvents { get; init; } = 11;

        /// <summary>Blocking events at or above which the day is at least Warning ("some blocking"). Default 1.</summary>
        public int BlockingWarningEvents { get; init; } = 1;

        /// <summary>Actionable alerts at or above which the day is at least Warning. Default 1 (any alert).</summary>
        public int AlertWarningCount { get; init; } = 1;

        /// <summary>
        /// The deadlock RATE tiers the day bands with (#3525) — the SAME store-backed pair the Overview
        /// card's deadlock dot reads (#3368, V120), so a day cell and the card cannot disagree about what a
        /// deadlock count over a window means. Defaults to the shipped pair
        /// (<see cref="DeadlockRateThresholds.Default"/>); a Darling caller with a store hands the
        /// <c>config_alert_settings</c> pair in, and Lite — which has no such knobs — bands on the
        /// default, which is its store's own future value should it ever grow them.
        /// </summary>
        public DeadlockRateThresholds DeadlockRates { get; init; } = DeadlockRateThresholds.Default;

        /// <summary>The shipped defaults. Use this everywhere unless a caller has an explicit reason to override.</summary>
        public static DailyHealthThresholds Default { get; } = new();
    }

    /// <summary>
    /// The single, app-agnostic source of truth for the Performance Calendar's per-day composite health band.
    /// Both apps (Lite over DuckDB, the Darling viewer over Postgres) roll each day's signals into a
    /// <see cref="DailyHealthSignals"/> and call <see cref="Classify"/>, so the month heatmap bands identically
    /// no matter which store fed it — replacing the previously twinned-and-drifted single-day "OverallHealth"
    /// string logic (Lite's inline copy lacked the memory-critical escalation the viewer had).
    /// </summary>
    public static class DailyHealthBandCalculator
    {
        /// <summary>
        /// Folds a day's signals into a single <see cref="DailyHealthBand"/>. Severity is first-match-wins:
        /// no-data → critical checks → warning checks → healthy.
        /// </summary>
        /// <param name="signals">The day's rolled-up signal counts.</param>
        /// <param name="thresholds">Banding thresholds; <see cref="DailyHealthThresholds.Default"/> when null.</param>
        public static DailyHealthBand Classify(in DailyHealthSignals signals, DailyHealthThresholds? thresholds = null)
        {
            if (!signals.HasData)
                return DailyHealthBand.NoData;

            var t = thresholds ?? DailyHealthThresholds.Default;

            /* Deadlocks band as a RATE over the window, through the SAME band the Overview card's deadlock
               dot reads (#3525) — not the "any deadlock is Critical" count trigger this replaced, #3368's
               un-fixed twin. Measured on the same 43-server fleet: count > 0 read 13.4% of 1-hour windows
               Critical and 87.9% of 24-hour windows Critical, and the calendar IS a 24-hour window, so ~7 of
               8 day cells painted red from deadlocks alone and the label stopped discriminating. Delegating
               to DeadlockSeverity (rather than re-stating its ladder) is what keeps the day cell, the card
               dot and the fleet sweep one banding: Critical/Warning fold into the day's matching tier, and
               its unrateable-window arm (a sub-hour or undeclared span) is exactly the fallback this
               classifier wants — a non-zero count reads Warning, a zero count (Unknown) stays out of the
               deadlock trigger entirely. */
            var deadlockSeverity = ServerHealthClassifier.DeadlockSeverity(
                signals.Deadlocks, signals.Window, t.DeadlockRates);

            // Critical: anything that makes the day genuinely serious. A critical deadlock rate, a
            // monitoring gap (collection errors), severe memory pressure, sustained high CPU, or heavy
            // blocking.
            if (deadlockSeverity == HealthSeverity.Critical
                || signals.CollectionErrors > 0
                || signals.MemoryCriticalEvents > 0
                || signals.HighCpuEvents >= t.HighCpuCriticalSamples
                || signals.BlockingEvents >= t.BlockingCriticalEvents)
            {
                return DailyHealthBand.Critical;
            }

            // Warning: elevated but not critical — an elevated deadlock rate, moderate CPU, some blocking,
            // (non-severe) memory pressure, or any actionable alert fired that day.
            if (deadlockSeverity == HealthSeverity.Warning
                || signals.HighCpuEvents >= t.HighCpuWarningSamples
                || signals.BlockingEvents >= t.BlockingWarningEvents
                || signals.MemoryPressureEvents > 0
                || signals.AlertCount >= t.AlertWarningCount)
            {
                return DailyHealthBand.Warning;
            }

            return DailyHealthBand.Healthy;
        }

        /// <summary>A short human label for the band ("No Data" / "Healthy" / "Warning" / "Critical").</summary>
        public static string Label(DailyHealthBand band) => band switch
        {
            DailyHealthBand.Healthy => "Healthy",
            DailyHealthBand.Warning => "Warning",
            DailyHealthBand.Critical => "Critical",
            _ => "No Data",
        };

        /// <summary>
        /// The theme resource key for the band's fill brush. Resolved by each app's theme via
        /// <c>DynamicResource</c> — the shared calendar control has no theme of its own. The muted tier
        /// (<c>ForegroundMutedBrush</c>) is the neutral grey used for days with no collection.
        /// </summary>
        public static string BrushKey(DailyHealthBand band) => band switch
        {
            DailyHealthBand.Healthy => "SuccessBrush",
            DailyHealthBand.Warning => "WarningBrush",
            DailyHealthBand.Critical => "ErrorBrush",
            _ => "ForegroundMutedBrush",
        };

        /// <summary>
        /// Builds a multi-line tooltip summarizing the day's signals — one line per non-zero signal, or a
        /// terse "No data collected." / "No issues detected." when appropriate. Suitable for a cell ToolTip.
        /// </summary>
        public static string Describe(in DailyHealthSignals signals)
        {
            if (!signals.HasData)
                return "No data collected.";

            var lines = BuildSignalLines(signals, peakBlockMs: 0);
            return lines.Count == 0 ? "No issues detected." : string.Join(Environment.NewLine, lines);
        }

        /// <summary>
        /// The day-detail "Why this day is &lt;band&gt;" reasons: one line per non-zero signal that drove the
        /// band, using the SAME per-signal wording as the calendar tooltip (<see cref="Describe"/>) so the two
        /// can't drift, plus — when <paramref name="peakBlockMs"/> is supplied (&gt; 0) — the day's peak block
        /// duration appended to the blocking line. A collected-but-quiet day returns a single "No issues
        /// detected."; a day with no collection returns "No collection this day." (the day-detail phrasing,
        /// distinct from the tooltip's "No data collected.").
        /// </summary>
        /// <param name="signals">The day's rolled-up signal counts.</param>
        /// <param name="peakBlockMs">The day's peak/max block wait in ms (0 = unknown / not carried), shown on the blocking line.</param>
        public static IReadOnlyList<string> BuildReasons(in DailyHealthSignals signals, long peakBlockMs = 0)
        {
            if (!signals.HasData)
                return new[] { "No collection this day." };

            var lines = BuildSignalLines(signals, peakBlockMs);
            return lines.Count == 0 ? new List<string> { "No issues detected." } : lines;
        }

        /// <summary>
        /// Which day-detail drill buttons to offer for a day, in panel order. A day with no collection offers
        /// none (there is nothing to drill into). Otherwise Top Queries is always offered (you can always
        /// inspect what ran), Deadlocks is offered only when the day had any deadlock, and Blocking only when it
        /// had any blocking event. Pure + static so both hosts and the tests share one decision.
        /// </summary>
        public static IReadOnlyList<DayDrillTarget> AvailableDrills(in DailyHealthSignals signals)
        {
            var drills = new List<DayDrillTarget>();
            if (!signals.HasData)
                return drills;

            if (signals.Deadlocks > 0)
                drills.Add(DayDrillTarget.Deadlocks);
            if (signals.BlockingEvents > 0)
                drills.Add(DayDrillTarget.Blocking);
            drills.Add(DayDrillTarget.TopQueries);
            return drills;
        }

        /// <summary>
        /// The [start, next-day-start) window for a clicked calendar day as naive-UTC bounds — the calendar
        /// buckets days in UTC day-space (<c>date_trunc('day', collection_time)</c> over naive-UTC collection
        /// times), so a day drill scopes to exactly that UTC day. Each host converts these bounds into its own
        /// time space (Lite adds the server offset for its server-time reads; the viewer reads naive UTC
        /// directly). Pure + static so the window computation is unit-testable.
        /// </summary>
        public static (DateTime StartUtc, DateTime EndUtc) DayWindowUtc(DateTime date)
        {
            var start = date.Date;
            return (start, start.AddDays(1));
        }

        /// <summary>
        /// The day-detail key-metrics one-liner from a day's roll-up: top wait type, high-CPU sample count,
        /// total wait time, and unique query count. Formatting lives here (shared by both apps, unit-testable)
        /// so the two hosts render the line identically.
        /// </summary>
        public static string BuildKeyMetricsLine(string? topWaitType, long highCpuEvents, decimal totalWaitSeconds, long uniqueQueries)
        {
            var wait = string.IsNullOrWhiteSpace(topWaitType) ? "none" : topWaitType;
            var totalWait = totalWaitSeconds < 1000
                ? totalWaitSeconds.ToString("N1", CultureInfo.InvariantCulture) + " s"
                : (totalWaitSeconds / 60).ToString("N1", CultureInfo.InvariantCulture) + " min";
            return string.Format(
                CultureInfo.InvariantCulture,
                "Top wait: {0}     High-CPU samples: {1:N0}     Total wait: {2}     Unique queries: {3:N0}",
                wait, highCpuEvents, totalWait, uniqueQueries);
        }

        /// <summary>The non-empty per-signal lines for a collected day (no terminal "quiet"/"no-data" handling —
        /// callers add their own). The order matches the tooltip; the blocking line carries the peak block
        /// duration when one is supplied.</summary>
        private static List<string> BuildSignalLines(in DailyHealthSignals signals, long peakBlockMs)
        {
            var lines = new List<string>();
            AppendDeadlocks(lines, signals);
            AppendCount(lines, signals.CollectionErrors, "collection error", "collection errors");
            AppendCount(lines, signals.HighCpuEvents, "high-CPU sample", "high-CPU samples");
            AppendBlocking(lines, signals.BlockingEvents, peakBlockMs);
            AppendCount(lines, signals.MemoryCriticalEvents, "severe memory-pressure event", "severe memory-pressure events");
            // MemoryPressureEvents is the full count (medium + severe); severe is a subset already listed
            // above, so only the non-severe remainder is shown here to avoid double-counting.
            var nonSevereMemory = Math.Max(0, signals.MemoryPressureEvents - signals.MemoryCriticalEvents);
            AppendCount(lines, nonSevereMemory, "memory-pressure event", "memory-pressure events");
            AppendCount(lines, signals.AlertCount, "alert", "alerts");
            return lines;
        }

        private static void AppendCount(List<string> lines, long count, string singular, string plural)
        {
            if (count <= 0)
                return;

            var noun = count == 1 ? singular : plural;
            lines.Add(count.ToString("N0", CultureInfo.InvariantCulture) + " " + noun);
        }

        /// <summary>The deadlock line — like <see cref="AppendCount"/> but appends the per-hour rate the
        /// band evaluated when the window is rateable, e.g. "120 deadlocks (5.0/hr)" — the card reason's own
        /// format (#3368/#3525): the count is the countable fact, the rate is the banded one, and a line
        /// that shows only the count cannot say which tier was crossed. An unrateable window prints the
        /// count alone, which is exactly what the band had to go on.</summary>
        private static void AppendDeadlocks(List<string> lines, in DailyHealthSignals signals)
        {
            if (signals.Deadlocks <= 0)
                return;

            var noun = signals.Deadlocks == 1 ? "deadlock" : "deadlocks";
            var line = signals.Deadlocks.ToString("N0", CultureInfo.InvariantCulture) + " " + noun;
            var rate = ServerHealthClassifier.DeadlockRatePerHour(signals.Deadlocks, signals.Window);
            if (rate.HasValue)
                line += " (" + rate.Value.ToString("0.0", CultureInfo.InvariantCulture) + "/hr)";
            lines.Add(line);
        }

        /// <summary>The blocking line — like <see cref="AppendCount"/> but appends the day's peak block
        /// duration when one is known (&gt; 0), e.g. "42 blocking events (peak block 12.5 s)".</summary>
        private static void AppendBlocking(List<string> lines, long count, long peakBlockMs)
        {
            if (count <= 0)
                return;

            var noun = count == 1 ? "blocking event" : "blocking events";
            var line = count.ToString("N0", CultureInfo.InvariantCulture) + " " + noun;
            if (peakBlockMs > 0)
                line += " (peak block " + FormatDurationMs(peakBlockMs) + ")";
            lines.Add(line);
        }

        /// <summary>Formats a millisecond duration compactly ("800 ms" / "12.5 s" / "3.2 min").</summary>
        internal static string FormatDurationMs(long ms)
        {
            if (ms < 1000)
                return ms.ToString("N0", CultureInfo.InvariantCulture) + " ms";

            var seconds = ms / 1000.0;
            return seconds < 60
                ? seconds.ToString("N1", CultureInfo.InvariantCulture) + " s"
                : (seconds / 60).ToString("N1", CultureInfo.InvariantCulture) + " min";
        }
    }
}
