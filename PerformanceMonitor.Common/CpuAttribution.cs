/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// The attributed-CPU denominator for the top-queries/procedures MCP reads (#2320, split from #2235) —
    /// how much of the instance's actually-consumed CPU the returned ranking explains. Pre-#2290 the reads
    /// explained ~10% of the box and nothing said so; a caller chased the visible 10% assuming it was
    /// everything. And the number catches impossible claims at a glance: an external comparison died the
    /// moment someone divided its worker_time sum by the box's available CPU-seconds and got 137%. Both
    /// SKUs' tools hand the caller numerator, denominator, and ratio instead of leaving the division to be
    /// re-derived — ONE computation here, so the two cannot disagree.
    ///
    /// <para>The denominator is measured, not theoretical: the SQL process's average CPU%% over the window
    /// (the cpu_utilization series both stores already collect) × core count (server_properties) × window
    /// seconds. When a piece is missing — no CPU samples, no properties snapshot, or the series covers too
    /// little of the window — the ratio is OMITTED, never invented (#2320's explicit degrade rule).</para>
    /// </summary>
    public static class CpuAttribution
    {
        /// <summary>The CPU series must span at least this fraction of the requested window for the
        /// denominator to be honest — below it a server added (or monitoring resumed) mid-window would
        /// deflate measured CPU-seconds and inflate the ratio.</summary>
        public const double MinimumCoverageFraction = 0.9;

        /// <summary>Below this ratio the result carries the "not the whole story" note — the #2235 history
        /// says real post-fix rankings explain roughly a third, so half is a generous line between "normal
        /// plan-cache attribution loss" and "worth saying out loud".</summary>
        public const double LowRatioThreshold = 0.5;

        /// <summary>Above this ratio the returned rows claim more CPU than the process measurably consumed —
        /// the impossible-claim marker (137% is how the Datadog comparison died). Slack above 1.0 covers
        /// sampling noise between the two series.</summary>
        public const double OverAttributionThreshold = 1.1;

        /// <summary>
        /// A null <see cref="AttributedCpuRatio"/> always comes with a <see cref="Note"/> saying why.
        /// <see cref="SqlCpuSecondsInWindow"/> is usually null alongside it (the denominator could not be
        /// measured) — EXCEPT the measured-zero case, where the zero is reported and only the ratio is
        /// omitted, so the caller sees WHY dividing was refused. When the ratio is present the note is
        /// null unless the ratio is low or impossible.
        /// </summary>
        public sealed record Result(
            double RankedCpuSeconds,
            double? SqlCpuSecondsInWindow,
            double? AttributedCpuRatio,
            string? Note);

        /// <summary>
        /// <paramref name="rankedCpuSeconds"/> is the summed windowed CPU of the rows the tool RETURNS
        /// (post top-N, post filters) — the ratio answers "what does the caller-visible ranking explain",
        /// not "what does the whole table hold". The sample aggregate (count / first / last /
        /// <paramref name="avgSqlCpuPercent"/>) comes from the store's cpu_utilization series windowed on
        /// the SAME collection_time bounds as the ranking, so numerator and denominator share gaps.
        /// </summary>
        public static Result Compute(
            double rankedCpuSeconds,
            DateTime windowStartUtc,
            DateTime windowEndUtc,
            int sampleCount,
            DateTime? firstSampleUtc,
            DateTime? lastSampleUtc,
            double? avgSqlCpuPercent,
            int cpuCount)
        {
            var ranked = Math.Round(rankedCpuSeconds, 1);
            var windowSeconds = (windowEndUtc - windowStartUtc).TotalSeconds;

            if (windowSeconds <= 0)
            {
                return new Result(ranked, null, null,
                    "the requested window is empty; ratio omitted");
            }

            if (sampleCount == 0 || avgSqlCpuPercent is null || firstSampleUtc is null || lastSampleUtc is null)
            {
                return new Result(ranked, null, null,
                    "no cpu_utilization samples in the window, so measured CPU-seconds cannot be computed; ratio omitted rather than invented");
            }

            if (cpuCount <= 0)
            {
                return new Result(ranked, null, null,
                    "core count unavailable (no server_properties snapshot), so measured CPU-seconds cannot be computed; ratio omitted rather than invented");
            }

            var coverageStart = firstSampleUtc.Value > windowStartUtc ? firstSampleUtc.Value : windowStartUtc;
            var coverageEnd = lastSampleUtc.Value < windowEndUtc ? lastSampleUtc.Value : windowEndUtc;
            var coverageFraction = Math.Max(0, (coverageEnd - coverageStart).TotalSeconds) / windowSeconds;
            if (coverageFraction < MinimumCoverageFraction)
            {
                return new Result(ranked, null, null,
                    $"cpu_utilization covers only {Math.Round(coverageFraction * 100).ToString(CultureInfo.InvariantCulture)}% of the window; ratio omitted rather than computed against a partial denominator");
            }

            var sqlCpuSeconds = avgSqlCpuPercent.Value / 100.0 * cpuCount * windowSeconds;
            if (sqlCpuSeconds <= 0)
            {
                return new Result(ranked, Math.Round(sqlCpuSeconds, 1), null,
                    "the SQL process's measured CPU in the window is zero; ratio omitted");
            }

            var ratio = rankedCpuSeconds / sqlCpuSeconds;
            var pct = Math.Round(ratio * 100).ToString(CultureInfo.InvariantCulture);

            string? note = null;
            if (ratio > OverAttributionThreshold)
            {
                note = $"the returned rows' CPU is {pct}% of the SQL process's measured CPU-seconds — more than the process consumed. Treat this as an impossible-claim marker: suspect double-counted deltas, clock skew between the two series, or a CPU-series gap before trusting the ranking's absolute numbers.";
            }
            else if (ratio < LowRatioThreshold)
            {
                note = $"the returned rows explain {pct}% of the SQL process's measured CPU-seconds in this window. The remainder is plans evicted between snapshots, statements outside the top-N or filters, zero-cost rows, and non-query CPU — a low ratio means the visible ranking is not the whole story.";
            }

            return new Result(ranked, Math.Round(sqlCpuSeconds, 1), Math.Round(ratio, 3), note);
        }
    }
}
