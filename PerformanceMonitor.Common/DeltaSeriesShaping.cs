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

namespace PerformanceMonitor.Common;

/// <summary>What a differenced counter's plotted value is a rate OF (#3653 A7).</summary>
public enum DeltaBasis
{
    /// <summary>The per-interval delta divided by the stored <c>sample_interval_seconds</c> — the value an
    /// operator expects from a counter whose name says <c>/sec</c>.</summary>
    PerSecond,

    /// <summary>The per-interval delta as stored. Honest only when the axis says so.</summary>
    PerInterval,
}

/// <summary>
/// One collected point of a differenced series, as the store hands it to a chart: when it was collected,
/// the delta the collector stored for the interval that ended there, and the seconds that interval covered
/// under the three-state rule (#2234 / #3540): a positive <paramref name="IntervalSeconds"/> is the measured
/// sweep gap; <c>0</c> is the calculator's "no delta knowable" marker (first sighting, counter reset, a gap
/// past the measured 3600 s policy — in practice a restart), and the <c>Delta</c> beside it is a fabricated
/// zero, not a measurement; <c>null</c> is a row from before the family stored an interval at all.
/// </summary>
/// <param name="Time">The collection time, in whatever clock the caller's X axis uses.</param>
/// <param name="Delta">The stored per-interval delta (already summed across instances where the read does that).</param>
/// <param name="IntervalSeconds">The stored interval under the three-state rule; see the type remarks.</param>
public readonly record struct DeltaSample(DateTime Time, long Delta, long? IntervalSeconds);

/// <summary>
/// The shaping every chart of a DIFFERENCED series does before it hands arrays to ScottPlot (#3653 A7, the
/// viewer half of #3540's keystone), in one place both viewers call so the two SKUs cannot drift on it and
/// so it can be executed on a Mac: this type has no <c>System.Windows</c> and no ScottPlot dependency, so
/// the mactest harness runs its pins against the built <c>PerformanceMonitor.Common.dll</c>.
///
/// <para><b>What it fixes.</b> The perfmon chart in both viewers plotted <c>delta_cntr_value</c> under a Y
/// axis labelled "Value" while the reader had fetched <c>MAX(sample_interval_seconds)</c> onto the same row
/// and left it unused — so <c>Batch Requests/sec</c> drew the number of batches in a ~300 s sweep, not a
/// per-second rate, and a restart's fabricated (0, 0) drew as a genuine trough. <see cref="Shape"/> divides
/// a rate counter's delta by the stored interval, leaves a non-rate counter's delta alone but says so in
/// the label, and renders the unknowable point as <see cref="double.NaN"/> — which ScottPlot 5 draws as a
/// line break, so a restart reads as absence rather than as zero, exactly as #1944's cadence-derived gap
/// markers already do for a collection gap (<c>PerformanceMonitor.Ui.TimeSeriesGaps</c>; that rule works
/// on X spacing, this one on the stored interval, and the two compose because <c>TimeSeriesGaps</c> never
/// looks at Y).</para>
///
/// <para><b>The rate-vs-gauge classification is a PROXY, and it is named as one.</b> <c>cntr_type</c> is
/// not stored (the rung that adds it is queued under #3653 and is the fix for everything this paragraph
/// concedes), so the row cannot say whether the counter is a <c>PERF_COUNTER_BULK_COUNT</c> (a rate, whose
/// delta over the interval is the value) or a <c>PERF_COUNTER_LARGE_RAWCOUNT</c> gauge (whose delta is
/// noise). Until the rung lands, <see cref="BasisFor"/> reads the counter NAME: a name ending in
/// <c>/sec</c> (Batch Requests/sec, Log Bytes Flushed/sec, Transactions/sec, ...) or in <c>/s)</c> (the
/// <c>Version Cleanup rate (KB/s)</c> pair) is a rate counter, and everything else is plotted as its raw
/// per-interval delta under an axis that says "Δ per interval". Known mis-classes the proxy accepts:
/// <c>Temp Tables Creation Rate</c> and <c>Lock Wait Time (ms)</c> are bulk counts without the suffix and
/// plot per interval (an honest label on a less useful number); the wait-statistics object's
/// <c>Lock waits</c> / <c>Page latch waits</c> family are per-second in one instance and averages in another,
/// and plot per interval. None of them is plotted under a lying label; the rung, not a longer suffix list,
/// is where the classification becomes the engine's.</para>
///
/// <para><b>What it does not do.</b> It does not touch a series that is already rated in SQL (the wait,
/// latch, spinlock, file-I/O and query trends divide by the stored interval in their reads and DROP the
/// unknowable row, #3540/#3595), does not decide the cadence gap rule (that stays <c>TimeSeriesGaps</c>),
/// and does not correct a gauge's delta into a level — that needs <c>cntr_type</c>.</para>
/// </summary>
public static class DeltaSeriesShaping
{
    /// <summary>The name suffix the proxy reads as "a rate counter" (ordinal, case-insensitive, trimmed).</summary>
    public const string RateSuffix = "/sec";

    /// <summary>The second spelling the fleet's collected set uses: <c>Version Cleanup rate (KB/s)</c> and
    /// <c>Version Generation rate (KB/s)</c> are <c>PERF_COUNTER_BULK_COUNT</c> and end in <c>/s)</c>.</summary>
    public const string RateSuffixParenthesised = "/s)";

    /// <summary>Appended to a non-rate counter's legend entry so the reader knows the plotted number is the
    /// stored delta, not a per-second value. Short, because it rides in a 12-entry legend.</summary>
    public const string PerIntervalLegendSuffix = " (Δ/interval)";

    /// <summary>The Y label when every plotted series is a rate.</summary>
    public const string PerSecondAxisLabel = "per second";

    /// <summary>The Y label when every plotted series is a raw per-interval delta.</summary>
    public const string PerIntervalAxisLabel = "Δ per interval";

    /// <summary>The Y label when the picker mixes the two — the one honest label for one axis carrying both.</summary>
    public const string MixedAxisLabel = "per second (…/sec) · Δ per interval (others)";

    /// <summary>The Y label when nothing is plotted (every selected counter's trend came back empty) — the
    /// label the chart carried before #3653, kept so an empty chart looks like it always did.</summary>
    public const string EmptyAxisLabel = "Value";

    /// <summary>How a grid spells a stored interval of 0 — the calculator's marker, in the words #3540's
    /// documentation and #3595's rung use for it. Not "0": a zero here would be read as "zero seconds".</summary>
    public const string UnknowableIntervalDisplay = "restart / first sample";

    /// <summary>How a grid spells a NULL interval — a row from before the family stored one (pre-V127 /
    /// pre-v60 for latch and spinlock stats). Its deltas are real; only the denominator was never recorded.</summary>
    public const string UnrecordedIntervalDisplay = "not stored";

    /// <summary>
    /// The rate-vs-gauge proxy: <see cref="DeltaBasis.PerSecond"/> when the counter's trimmed name ends in
    /// <see cref="RateSuffix"/> or <see cref="RateSuffixParenthesised"/>, else <see cref="DeltaBasis.PerInterval"/>.
    /// A null or empty name is per interval — there is no evidence for a rate.
    /// </summary>
    public static DeltaBasis BasisFor(string? counterName)
    {
        if (string.IsNullOrWhiteSpace(counterName))
        {
            return DeltaBasis.PerInterval;
        }

        var name = counterName.Trim();
        return name.EndsWith(RateSuffix, StringComparison.OrdinalIgnoreCase)
            || name.EndsWith(RateSuffixParenthesised, StringComparison.OrdinalIgnoreCase)
            ? DeltaBasis.PerSecond
            : DeltaBasis.PerInterval;
    }

    /// <summary>
    /// The plotted Y for every sample, parallel to the input, under the three-state interval rule:
    /// <list type="bullet">
    /// <item>interval <c>0</c> → <see cref="double.NaN"/> for BOTH bases. The delta beside a 0 is the
    /// calculator's fabricated zero, so it is not a value at all — and NaN is what breaks the line there.</item>
    /// <item>interval <c>n &gt; 0</c> → <c>delta / n</c> for <see cref="DeltaBasis.PerSecond"/>, <c>delta</c>
    /// for <see cref="DeltaBasis.PerInterval"/>.</item>
    /// <item>interval <c>null</c> (a pre-column row) → for <see cref="DeltaBasis.PerInterval"/> the raw delta
    /// (the interval does not enter); for <see cref="DeltaBasis.PerSecond"/> the delta over the seconds to
    /// the PREVIOUS sample in this list — the same LAG fallback every SQL-side reader in the repo uses for a
    /// pre-column row — and NaN for the first sample, which has no previous (the SQL readers' LAG is NULL
    /// there and they drop the row; a fabricated first-point rate is #3642's class). A non-positive spacing
    /// (duplicate timestamps) is likewise NaN rather than a division by zero.</item>
    /// </list>
    /// Callers hand samples in ascending time order, the order every trend read here returns. For the perfmon
    /// family the NULL arm is the rule's completeness, not a population: <c>perfmon_stats</c> has carried the
    /// column since its first schema (pre-#2234 rows carried a fabricated 60 — the configured cadence — which
    /// no row-level rule can detect, and which raw retention has aged out).
    /// </summary>
    public static double[] Shape(IReadOnlyList<DeltaSample> samples, DeltaBasis basis)
    {
        ArgumentNullException.ThrowIfNull(samples);

        var ys = new double[samples.Count];
        for (var i = 0; i < samples.Count; i++)
        {
            var s = samples[i];
            if (s.IntervalSeconds == 0)
            {
                ys[i] = double.NaN;
                continue;
            }

            if (basis == DeltaBasis.PerInterval)
            {
                ys[i] = s.Delta;
                continue;
            }

            double seconds;
            if (s.IntervalSeconds is long stored)
            {
                /* A negative stored interval is not a state the calculator produces; treat it as unrated rather
                   than flip the sign of a rate. */
                if (stored < 0)
                {
                    ys[i] = double.NaN;
                    continue;
                }

                seconds = stored;
            }
            else if (i == 0)
            {
                ys[i] = double.NaN;
                continue;
            }
            else
            {
                seconds = (s.Time - samples[i - 1].Time).TotalSeconds;
            }

            ys[i] = seconds > 0 ? s.Delta / seconds : double.NaN;
        }

        return ys;
    }

    /// <summary>The legend entry: the counter's own name for a rate (its name already says <c>/sec</c>),
    /// the name plus <see cref="PerIntervalLegendSuffix"/> for a per-interval delta.</summary>
    public static string LegendLabel(string counterName, DeltaBasis basis) =>
        basis == DeltaBasis.PerInterval ? counterName + PerIntervalLegendSuffix : counterName;

    /// <summary>
    /// The one Y-axis label for a chart carrying the given bases: <see cref="PerSecondAxisLabel"/> when all
    /// are rates, <see cref="PerIntervalAxisLabel"/> when all are deltas, <see cref="MixedAxisLabel"/> when
    /// both are present, <see cref="EmptyAxisLabel"/> when nothing was plotted.
    /// </summary>
    public static string YAxisLabel(IEnumerable<DeltaBasis> plottedBases)
    {
        ArgumentNullException.ThrowIfNull(plottedBases);

        var anyRate = false;
        var anyDelta = false;
        foreach (var basis in plottedBases)
        {
            if (basis == DeltaBasis.PerSecond) anyRate = true;
            else anyDelta = true;
        }

        return (anyRate, anyDelta) switch
        {
            (true, true) => MixedAxisLabel,
            (true, false) => PerSecondAxisLabel,
            (false, true) => PerIntervalAxisLabel,
            _ => EmptyAxisLabel,
        };
    }

    /// <summary>
    /// The largest finite value in a shaped series, or <paramref name="fallback"/> when there is none —
    /// for the chart's Y ceiling. Spelled out rather than <c>Enumerable.Max</c> because a NaN's ordering
    /// under <c>Max</c> is an implementation detail this code should not lean on, and an all-NaN series
    /// (a window holding only a restart) must still give the axis a number.
    /// </summary>
    public static double MaxFinite(IEnumerable<double> values, double fallback)
    {
        ArgumentNullException.ThrowIfNull(values);

        double? max = null;
        foreach (var v in values)
        {
            if (double.IsNaN(v) || double.IsInfinity(v)) continue;
            if (max is null || v > max.Value) max = v;
        }

        return max ?? fallback;
    }

    /// <summary>
    /// A snapshot grid's delta cell under the same rule: <c>null</c> when the row's stored interval is the
    /// 0 marker (the delta beside it is fabricated and the cell must not read "0"), the stored delta
    /// otherwise — including for a NULL interval, whose delta is real.
    /// </summary>
    public static long? ReadableDelta(long delta, int? intervalSeconds) =>
        intervalSeconds == 0 ? null : delta;

    /// <summary>
    /// A snapshot grid's interval cell: <see cref="UnknowableIntervalDisplay"/> for the 0 marker,
    /// <see cref="UnrecordedIntervalDisplay"/> for NULL, the seconds (invariant, no grouping) otherwise.
    /// </summary>
    public static string IntervalDisplay(int? intervalSeconds) => intervalSeconds switch
    {
        null => UnrecordedIntervalDisplay,
        0 => UnknowableIntervalDisplay,
        var seconds => seconds.Value.ToString(CultureInfo.InvariantCulture),
    };
}
