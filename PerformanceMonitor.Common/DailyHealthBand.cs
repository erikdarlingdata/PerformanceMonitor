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

        /// <summary>Elevated but not critical (moderate CPU, some blocking, memory pressure, a fifth of the
        /// day's collection failing, or alerts) — amber.</summary>
        Warning = 2,

        /// <summary>A serious day (a critical deadlock or blocking rate, a 60-second block, sustained high
        /// CPU, or severe memory pressure) — red.</summary>
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

        /// <summary>Collector runs that ended in ERROR in the window. Banded as a SHARE of
        /// <see cref="CollectionRuns"/> (#3539 A2): past the collector-health classifier's own 20% bar the
        /// window is Warning; below it the errors are disclosed but do not band — see
        /// <see cref="DailyHealthBandCalculator.CollectionErrorSeverity"/>.</summary>
        public long CollectionErrors { get; init; }

        /// <summary>
        /// Collector runs of EVERY status in the window (#3539 A2) — the denominator of the collection
        /// error share. Zero means no denominator was declared: a non-zero error count then bands Warning
        /// on presence (fail away from Healthy, never into a share nobody computed), and a zero error
        /// count stays out of the trigger. The shared daily SQL has always computed this (<c>coll.runs</c>
        /// marks the day collected); it is now projected so the band can divide by it.
        /// </summary>
        public long CollectionRuns { get; init; }

        /// <summary>CPU samples where total host CPU (SQL + other-process) was at or above
        /// <see cref="ServerHealthThresholds.CpuWarningPercent"/>. Banded against a bar that SCALES with
        /// <see cref="Window"/> (#3539 A2): the greater of an excursion-scale minimum and a sustained-heat
        /// rate — see <see cref="DailyHealthThresholds.HighCpuCriticalSamplesFor"/>.</summary>
        public long HighCpuEvents { get; init; }

        /// <summary>Blocking events in the window (blocked-process reports, falling back to DMV blocking
        /// snapshots). Banded as a RATE over <see cref="Window"/> through the card band's own
        /// <see cref="ServerHealthClassifier.BlockingSeverity"/> (#3539 A2/A3), together with
        /// <see cref="PeakBlockWaitMs"/>.</summary>
        public long BlockingEvents { get; init; }

        /// <summary>The longest single block in the window, in milliseconds (#3539 A2) — the blocking
        /// band's rate-independent wait arm: a 60-second block is Critical and a 10-second one Warning
        /// whatever the window. Zero when no blocking, or when the blocking source carries no wait time,
        /// which leaves the count arm alone to decide.</summary>
        public long PeakBlockWaitMs { get; init; }

        /// <summary>Memory-pressure events (a process or system indicator ≥ 2) that were not severe. Any is Warning.</summary>
        public long MemoryPressureEvents { get; init; }

        /// <summary>Severe memory-pressure events (a process indicator ≥ 3) that day. Any (&gt; 0) is Critical.</summary>
        public long MemoryCriticalEvents { get; init; }

        /// <summary>Actionable (non-resolution) alerts raised that day. At/over
        /// <see cref="DailyHealthThresholds.AlertWarningCount"/> is Warning.</summary>
        public long AlertCount { get; init; }
    }

    /// <summary>
    /// The one, documented place for the Performance Calendar's day-banding thresholds — and, since the
    /// same classifier scores fleet-sweep spans of 15 minutes to a day, for every window the composite band
    /// is applied to.
    ///
    /// <para><b>Why the count triggers became window-scaled (#3539 A2).</b> The originals were the Daily
    /// Summary's single-day rules (<c>high_cpu_events &gt; 5</c>, <c>blocking_events &gt; 10</c>), carried
    /// here as constants and then applied unchanged to sweep spans 96x shorter than the day they were
    /// written for. Measured on the 43-server dogfood fleet over 14 days, six high-CPU samples reddened 5.9%
    /// of server-days and eleven blocking events 5.1% — and the same numbers on a 15-minute span meant
    /// something else entirely. The blocking trigger is now the card band's rate (its constants live on
    /// <see cref="ServerHealthThresholds"/>); the CPU trigger scales as documented on its members below;
    /// the collection-error trigger is a share of runs.</para>
    /// </summary>
    public sealed record DailyHealthThresholds
    {
        /// <summary>
        /// The sustained-heat RATE of the CPU Critical arm: high-CPU samples per hour of window. 1.25 per
        /// hour is 30 samples over a 24-hour day.
        ///
        /// <para><b>Measured.</b> 645 server-days of <c>cpu_utilization_stats</c> on the dogfood fleet
        /// (≈1,977 samples per server-day): 80.8% of server-days hold no sample at or above 80% total host
        /// CPU at all; p90 is 3 hot samples, p99 is 33.7. Thirty or more hot samples marks 9 server-days
        /// (1.4%), against 38 (5.9%) for the six the day used to redden on — and a day that reaches 30 has
        /// spent roughly half an hour hot, which is sustained heat rather than two routine excursions.</para>
        /// </summary>
        public double HighCpuCriticalSamplesPerHour { get; init; } = 1.25;

        /// <summary>
        /// The excursion-scale MINIMUM of the CPU Critical arm: the count a window has to reach before the
        /// rate above is a claim about sustained heat rather than about one excursion, so short windows
        /// band on this count and long ones on the rate — <see cref="HighCpuCriticalSamplesFor"/> takes the
        /// greater of the two.
        ///
        /// <para><b>Measured.</b> 758 consecutive-sample excursions over the same 14 days: p50 run length 1
        /// sample, p90 2, p99 7, max 24 (≈17 minutes); 95.8% of excursions are three samples or fewer and
        /// six samples is the 97.8th percentile of excursion LENGTH (#3282's finding, confirmed). So a
        /// window holding fewer than six hot samples holds at most a routine excursion or two, and six in
        /// one 15-minute sweep span (34 of 57,820 buckets, 0.06%) or one hour is genuinely sustained for
        /// that span. Below a window of 4.8 hours this minimum dominates and every sweep span bands exactly
        /// as it did before #3539; above it the rate takes over, and the 24-hour day needs 30.</para>
        /// </summary>
        public int HighCpuCriticalSamplesMinimum { get; init; } = 6;

        /// <summary>The Warning arm's rate: 0.25 samples per hour is six over a day — the count the day used
        /// to redden on becomes the count it turns amber on (5.9% of measured server-days), and the 1–5
        /// hot-sample days that made 19.2% of server-days amber (two routine excursions, on the excursion
        /// measurement above) now band Healthy.</summary>
        public double HighCpuWarningSamplesPerHour { get; init; } = 0.25;

        /// <summary>The Warning arm's minimum: one hot sample. On any window under four hours a single
        /// 80%+ moment is still worth a second look — exactly the pre-#3539 Warning rule for the sweep.</summary>
        public int HighCpuWarningSamplesMinimum { get; init; } = 1;

        /// <summary>Actionable alerts at or above which the day is at least Warning. Default 1 (any alert).
        ///
        /// <para><b>Kept, on review (#3539 A2).</b> The objection was that the signal couples the historical
        /// record to alert configuration, so retuning would recolour the past. It does not: alert rows are
        /// written once at fire time under the configuration then in force, so a retune changes what future
        /// days record and nothing about days already recorded — the same property every other signal here
        /// has (a collector enabled tomorrow does not recolour yesterday). What the coupling DOES mean is
        /// that an alert is by definition a condition the operator asked to be told about, and a day that
        /// contained one is not a quiet day. It is Warning-only, so it can never redden a cell on its
        /// own, and the count is disclosed on the cell.</para></summary>
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

        /// <summary>The high-CPU sample count at or above which <paramref name="window"/> is Critical: the
        /// greater of <see cref="HighCpuCriticalSamplesMinimum"/> and
        /// <see cref="HighCpuCriticalSamplesPerHour"/> × the window's hours. An undeclared window
        /// (<see cref="TimeSpan.Zero"/>) yields the minimum — the count the pre-#3539 constant applied to
        /// every window, so a producer that declares nothing bands as it always did rather than on a rate
        /// it never supplied.</summary>
        public double HighCpuCriticalSamplesFor(TimeSpan window) =>
            ScaledSamples(HighCpuCriticalSamplesMinimum, HighCpuCriticalSamplesPerHour, window);

        /// <summary>The Warning arm's bar for <paramref name="window"/> — see <see cref="HighCpuCriticalSamplesFor"/>.</summary>
        public double HighCpuWarningSamplesFor(TimeSpan window) =>
            ScaledSamples(HighCpuWarningSamplesMinimum, HighCpuWarningSamplesPerHour, window);

        private static double ScaledSamples(int minimum, double perHour, TimeSpan window) =>
            window > TimeSpan.Zero ? Math.Max(minimum, perHour * window.TotalHours) : minimum;

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

            /* Blocking bands the same way (#3539 A2/A3): through the card band's own BlockingSeverity, over
               the same window, with the day's peak block as the wait arm. The count trigger this replaced
               (BlockingCriticalEvents = 11, the Daily Summary's original "> 10") reddened 5.1% of measured
               server-days and meant something else on every sweep span; the rate tiers and the 60 s wait
               arm are documented on ServerHealthThresholds. Delegating rather than restating is what keeps
               the day cell, the card dot and the sweep one banding, and its unrateable arm (a sub-hour or
               undeclared span) is the fallback this classifier wants: a non-zero count reads Warning, a zero
               count (Unknown) stays out of the blocking trigger. */
            var blockingSeverity = ServerHealthClassifier.BlockingSeverity(
                signals.BlockingEvents, signals.PeakBlockWaitMs / 1000.0, signals.Window);

            var cpuSeverity = HighCpuSeverity(signals.HighCpuEvents, signals.Window, t);
            var collectionSeverity = CollectionErrorSeverity(signals.CollectionErrors, signals.CollectionRuns);

            // Critical: anything that makes the window genuinely serious. A critical deadlock or blocking
            // rate (or a 60-second block), severe memory pressure, or sustained high CPU. Collection
            // errors are deliberately NOT here any more — see CollectionErrorSeverity.
            if (deadlockSeverity == HealthSeverity.Critical
                || blockingSeverity == HealthSeverity.Critical
                || signals.MemoryCriticalEvents > 0
                || cpuSeverity == HealthSeverity.Critical)
            {
                return DailyHealthBand.Critical;
            }

            // Warning: elevated but not critical — an elevated deadlock or blocking rate, moderate CPU,
            // (non-severe) memory pressure, a fifth of the window's collection failing, or any actionable
            // alert fired in the window.
            if (deadlockSeverity == HealthSeverity.Warning
                || blockingSeverity == HealthSeverity.Warning
                || cpuSeverity == HealthSeverity.Warning
                || signals.MemoryPressureEvents > 0
                || collectionSeverity == HealthSeverity.Warning
                || signals.AlertCount >= t.AlertWarningCount)
            {
                return DailyHealthBand.Warning;
            }

            return DailyHealthBand.Healthy;
        }

        /// <summary>
        /// The high-CPU arm of the composite band (#3539 A2): the hot-sample count against a bar that
        /// scales with the window — <see cref="DailyHealthThresholds.HighCpuCriticalSamplesFor"/> /
        /// <see cref="DailyHealthThresholds.HighCpuWarningSamplesFor"/>, each the greater of an
        /// excursion-scale minimum and a sustained-heat rate. Public so the fleet sweep's would-have-paged
        /// ledger can fire on exactly the arm the verdict banded with, and so the tests can state the shape
        /// directly: below ~4.8 hours the minimum decides and every sweep span bands as before; at 24 hours
        /// the day needs 30 hot samples for Critical and 6 for Warning.
        ///
        /// <para><b>Why not a pure rate with a minimum window, as the deadlock and blocking counts use.</b>
        /// Hot CPU samples arrive in excursions whose length has its own measured scale (95.8% are three
        /// samples or fewer), and the day-scale rate divided down to an hour is 1.25 samples — so a pure
        /// rate would band one routine two-sample excursion Critical at the hourly sweep, LOUDER than the
        /// constant it replaced. A rate that refused sub-hour windows would instead leave a 15-minute sweep
        /// unable to band a pinned server Critical at all. The greater-of shape keeps the count's meaning
        /// on short windows and the rate's on long ones, and the two agree exactly at 4.8 hours.</para>
        /// </summary>
        public static HealthSeverity HighCpuSeverity(long highCpuEvents, TimeSpan window, DailyHealthThresholds? thresholds = null)
        {
            var t = thresholds ?? DailyHealthThresholds.Default;

            if (highCpuEvents >= t.HighCpuCriticalSamplesFor(window))
                return HealthSeverity.Critical;

            if (highCpuEvents >= t.HighCpuWarningSamplesFor(window))
                return HealthSeverity.Warning;

            return HealthSeverity.Healthy;
        }

        /// <summary>
        /// The collection-error share, in percent of <paramref name="collectionRuns"/>, or <c>null</c> with
        /// no declared denominator. Public because the day's tooltip and reasons name it beside the count.
        /// </summary>
        public static double? CollectionErrorSharePercent(long collectionErrors, long collectionRuns) =>
            collectionRuns > 0 ? collectionErrors * 100.0 / collectionRuns : null;

        /// <summary>
        /// The collection-error arm of the composite band (#3539 A2): Warning when the ERROR share of the
        /// window's collector runs exceeds <see cref="CollectorHealthClassifier.WarningFailureRatePercent"/>,
        /// otherwise Healthy — never Critical.
        ///
        /// <para><b>The rule this replaces was <c>CollectionErrors &gt; 0 &rarr; Critical</c></b>, and it
        /// disagreed with the product's own collector-health surface twice over on the same evidence: that
        /// surface bands a collector on a RATE (errors over runs, WARNING past 20%, on the reasoning that an
        /// error may be transient and is retried), and its verdict for a collector erroring at any rate
        /// short of total silence is Warning, not Critical. One transient ERROR row among tens of thousands
        /// of runs painted a whole calendar day red; #1805 was one such day, and its fix had to reclassify a
        /// benign lock-timeout yield rather than touch the bar because there was no bar.</para>
        ///
        /// <para><b>The same bar and the same tier, deliberately.</b> A fifth of a window's runs erroring is
        /// not a transient — it is a fifth of the collectors dark all window or every collector dark a fifth
        /// of it — and the surface that owns that verdict already calls it Warning. Making the day cell say
        /// Critical at the same bar would recreate the tier mismatch this arm exists to remove; putting a
        /// Warning tier on mere presence would recreate the presence-flat reading. Below the bar the errors
        /// still appear on the cell, with their share, so they are disclosed rather than banded. The loud
        /// verdicts for a dark collection — FAILING and STOPPED rows, Collection Stopped alerts, the fleet
        /// card's graded collector dot (#3539 A8d) — belong to the surfaces that measure collection; the
        /// calendar is a performance record.</para>
        ///
        /// <para>With no denominator declared a non-zero error count fails away from Healthy into Warning
        /// (the unrateable-window discipline); a zero count bands Healthy whatever the denominator.</para>
        /// </summary>
        public static HealthSeverity CollectionErrorSeverity(long collectionErrors, long collectionRuns)
        {
            if (collectionErrors <= 0)
                return HealthSeverity.Healthy;

            var share = CollectionErrorSharePercent(collectionErrors, collectionRuns);
            if (!share.HasValue)
                return HealthSeverity.Warning;

            return share.Value > CollectorHealthClassifier.WarningFailureRatePercent
                ? HealthSeverity.Warning
                : HealthSeverity.Healthy;
        }

        /// <summary>
        /// The window a CALENDAR-DAY cell bands its counts over: a finished day is its full 24 hours, but
        /// the still-forming day is only the portion that has elapsed — otherwise an active storm dilutes
        /// against hours that have not happened yet (60 deadlocks in the last hour ÷ 24h reads 2.5/hr and
        /// Healthy while the true in-progress rate is 60/hr; the pre-#3525 any-deadlock trigger could not
        /// under-read this way, so the clamp is part of the rate change, per its review). The reference
        /// clock is the CALLER's: an anchored (as_of) read hands its window end so a backdated read clamps
        /// against its own "now", and the live calendars hand the wall clock. An elapsed portion under an
        /// hour lands in <see cref="ServerHealthClassifier.DeadlockSeverity"/>'s unrateable-window arm
        /// (Warning-not-rate), which is exactly right for a day cell minutes old; a reference before the
        /// day starts (a future cell) returns zero for the same reason.
        /// </summary>
        public static TimeSpan CalendarDayWindow(DateTime summaryDate, DateTime referenceUtc)
        {
            var dayStart = summaryDate.Date;
            if (referenceUtc >= dayStart.AddDays(1))
                return TimeSpan.FromDays(1);

            var elapsed = referenceUtc - dayStart;
            return elapsed > TimeSpan.Zero ? elapsed : TimeSpan.Zero;
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
            AppendCollectionErrors(lines, signals);
            AppendCount(lines, signals.HighCpuEvents, "high-CPU sample", "high-CPU samples");
            /* The peak handed in by the caller wins when it has one (the sweep and the day-detail panel
               carry it beside the signals); otherwise the signal's own, which the band itself read. */
            AppendBlocking(lines, signals, peakBlockMs > 0 ? peakBlockMs : signals.PeakBlockWaitMs);
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

        /// <summary>The collection-error line — the count plus the share of the window's runs the band read
        /// (#3539 A2), e.g. "12 collection errors (0.1% of 9,800 runs)"; the count alone when no denominator
        /// was declared, which is exactly what the band had to go on.</summary>
        private static void AppendCollectionErrors(List<string> lines, in DailyHealthSignals signals)
        {
            if (signals.CollectionErrors <= 0)
                return;

            var noun = signals.CollectionErrors == 1 ? "collection error" : "collection errors";
            var line = signals.CollectionErrors.ToString("N0", CultureInfo.InvariantCulture) + " " + noun;
            var share = CollectionErrorSharePercent(signals.CollectionErrors, signals.CollectionRuns);
            if (share.HasValue)
            {
                line += " (" + share.Value.ToString("0.0", CultureInfo.InvariantCulture) + "% of "
                    + signals.CollectionRuns.ToString("N0", CultureInfo.InvariantCulture) + " runs)";
            }

            lines.Add(line);
        }

        /// <summary>The blocking line — the count, then the per-hour rate the band evaluated when the window
        /// is rateable (#3539 A3, the deadlock line's rule), then the peak block duration when one is known,
        /// e.g. "42 blocking events (1.8/hr, peak block 12.5 s)". An unrateable window prints no rate.</summary>
        private static void AppendBlocking(List<string> lines, in DailyHealthSignals signals, long peakBlockMs)
        {
            var count = signals.BlockingEvents;
            if (count <= 0)
                return;

            var noun = count == 1 ? "blocking event" : "blocking events";
            var line = count.ToString("N0", CultureInfo.InvariantCulture) + " " + noun;

            var qualifiers = new List<string>(2);
            var rate = ServerHealthClassifier.BlockingRatePerHour(count, signals.Window);
            if (rate.HasValue)
                qualifiers.Add(rate.Value.ToString("0.0", CultureInfo.InvariantCulture) + "/hr");
            if (peakBlockMs > 0)
                qualifiers.Add("peak block " + FormatDurationMs(peakBlockMs));
            if (qualifiers.Count > 0)
                line += " (" + string.Join(", ", qualifiers) + ")";

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
