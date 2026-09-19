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

/// <summary>What a plotted perfmon point's number IS (#3653 A7).</summary>
public enum DeltaBasis
{
    /// <summary>The per-interval delta divided by the stored <c>sample_interval_seconds</c> — the value an
    /// operator expects from a rate counter (<c>PERF_COUNTER_BULK_COUNT</c>; under the name proxy, a counter
    /// whose name says <c>/sec</c>).</summary>
    PerSecond,

    /// <summary>The per-interval delta as stored. Honest only when the axis says so.</summary>
    PerInterval,

    /// <summary>The raw counter value as stored — a GAUGE's reading (<c>PERF_COUNTER_LARGE_RAWCOUNT</c>: a level
    /// such as <c>Total Server Memory (KB)</c>), which has no delta and plots as itself. Only a stored
    /// <c>cntr_type</c> can produce this basis; the name proxy never does (V132 / v62).</summary>
    Level,
}

/// <summary>
/// One collected point of a perfmon series, as the store hands it to a chart: when it was collected, the delta
/// the collector stored for the interval that ended there (NULL on a gauge row since V132 / v62, which stores
/// no delta), the seconds that interval covered under the three-state rule (#2234 / #3540): a positive
/// <paramref name="IntervalSeconds"/> is the measured sweep gap; <c>0</c> is the calculator's "no delta
/// knowable" marker (first sighting, counter reset, a gap past the measured 3600 s policy — in practice a
/// restart), and the <c>Delta</c> beside it is a fabricated zero, not a measurement; <c>null</c> is a row from
/// before the family stored an interval at all, or a gauge row that has none — and the raw counter value,
/// which is what a <see cref="DeltaBasis.Level"/> series plots.
/// </summary>
/// <param name="Time">The collection time, in whatever clock the caller's X axis uses.</param>
/// <param name="Delta">The stored per-interval delta (already summed across instances where the read does that);
/// null on a gauge row, which stores none.</param>
/// <param name="IntervalSeconds">The stored interval under the three-state rule; see the type remarks.</param>
/// <param name="Value">The stored raw <c>cntr_value</c> (summed across instances where the read does that) — the
/// reading a gauge plots. Optional because the latch/spinlock grids that reuse this helper's rules have no such
/// column; a <see cref="DeltaBasis.Level"/> series handed null here plots NaN rather than inventing a level.</param>
public readonly record struct DeltaSample(DateTime Time, long? Delta, long? IntervalSeconds, long? Value = null);

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
/// <para><b>The classification is the STORED TYPE, and the name-suffix proxy is only its NULL fallback</b>
/// (Darling V132 / Lite v62; before the rung the proxy was the whole rule). <see cref="BasisFor(string?, int?)"/>
/// reads the row's <c>cntr_type</c> through <see cref="PerfmonCounterTypes"/>: a rate id
/// (<c>PERF_COUNTER_BULK_COUNT</c>) is <see cref="DeltaBasis.PerSecond"/>, a gauge id
/// (<c>PERF_COUNTER_LARGE_RAWCOUNT</c>) is <see cref="DeltaBasis.Level"/> — the raw value plots as the level
/// it is, which the proxy could never do — and every other id (the average/fraction/base family, or one this
/// vocabulary has never seen) is <see cref="DeltaBasis.PerInterval"/>. Only when the type is NULL — a row
/// written before the rung, or a point whose instance rows disagree on type (see the readers) — does
/// <see cref="BasisFor(string?)"/> read the counter NAME: a name ending in <c>/sec</c> (Batch Requests/sec,
/// Log Bytes Flushed/sec, Transactions/sec, ...) or in <c>/s)</c> (the <c>Version Cleanup rate (KB/s)</c>
/// pair) is a rate counter, and everything else is plotted as its raw per-interval delta under an axis that
/// says "Δ per interval". The proxy's known mis-classes stand exactly where the proxy still applies:
/// <c>Temp Tables Creation Rate</c> and <c>Lock Wait Time (ms)</c> are bulk counts without the suffix and a
/// pre-rung row of theirs plots per interval (an honest label on a less useful number); a pre-rung gauge
/// row plots its meaningless delta per interval, as it did the day #3702 shipped — the fallback is chosen
/// so that history renders as the operator last saw it, and the type decides the moment a row carries one.
/// The wait-statistics object's <c>Lock waits</c> / <c>Page latch waits</c> family mixes a rate, a gauge and
/// an average across its INSTANCES; the trend reads sum those instances into one point and report a type
/// only when the instances agree, so that family stays on the proxy and plots per interval — the honest
/// end state for it is one series per instance, a picker change and its own lane.</para>
///
/// <para><b>What it does not do.</b> It does not touch a series that is already rated in SQL (the wait,
/// latch, spinlock, file-I/O and query trends divide by the stored interval in their reads and DROP the
/// unknowable row, #3540/#3595), does not decide the cadence gap rule (that stays <c>TimeSeriesGaps</c>),
/// and does not divide a <c>PERF_AVERAGE_BULK</c> numerator by its base — the store holds no join to the
/// base row, so that family's delta is plotted per interval and named as such.</para>
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

    /// <summary>The Y label when every plotted series is a gauge's level (V132 / v62).</summary>
    public const string LevelAxisLabel = "value (gauge)";

    /// <summary>The Y label when the picker mixes bases — the one honest label for one axis carrying more than
    /// one kind of number: the parts present, in this order, joined by <see cref="MixedAxisSeparator"/>. The
    /// full three-way spelling is <c>per second (rates) · Δ per interval (others) · value (gauges)</c>; a
    /// two-way mix carries only its two parts. Composed by <see cref="YAxisLabel"/>, never read as a whole.</summary>
    public static readonly IReadOnlyDictionary<DeltaBasis, string> MixedAxisParts = new Dictionary<DeltaBasis, string>
    {
        [DeltaBasis.PerSecond] = "per second (rates)",
        [DeltaBasis.PerInterval] = "Δ per interval (others)",
        [DeltaBasis.Level] = "value (gauges)",
    };

    /// <summary>What separates the parts of a mixed axis label.</summary>
    public const string MixedAxisSeparator = " · ";

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
    /// The classification a chart uses: the STORED type when the row has one, the name proxy when it does not.
    /// <see cref="PerfmonCounterKind.Rate"/> → <see cref="DeltaBasis.PerSecond"/>; <see cref="PerfmonCounterKind.Gauge"/>
    /// → <see cref="DeltaBasis.Level"/>; <see cref="PerfmonCounterKind.Other"/> → <see cref="DeltaBasis.PerInterval"/>;
    /// a NULL type → <see cref="BasisFor(string?)"/>. Callers hand the SERIES' type — the trend reads report a
    /// type per point only where the counter's instance rows agree, and a counter's type does not change, so any
    /// point's non-null type is the series' type and a gauge's whole history plots as a level the moment one row
    /// carries the type (its pre-rung rows stored the value too).
    /// </summary>
    public static DeltaBasis BasisFor(string? counterName, int? cntrType) => PerfmonCounterTypes.Kind(cntrType) switch
    {
        PerfmonCounterKind.Rate => DeltaBasis.PerSecond,
        PerfmonCounterKind.Gauge => DeltaBasis.Level,
        PerfmonCounterKind.Other => DeltaBasis.PerInterval,
        _ => BasisFor(counterName),
    };

    /// <summary>
    /// The rate-vs-not proxy, now the NULL-type fallback only: <see cref="DeltaBasis.PerSecond"/> when the
    /// counter's trimmed name ends in <see cref="RateSuffix"/> or <see cref="RateSuffixParenthesised"/>, else
    /// <see cref="DeltaBasis.PerInterval"/>. Never <see cref="DeltaBasis.Level"/> — a name is no evidence of a
    /// gauge, and a pre-rung gauge row's stored delta plots per interval as it did before the rung. A null or
    /// empty name is per interval — there is no evidence for a rate.
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
    /// The plotted Y for every sample, parallel to the input. A <see cref="DeltaBasis.Level"/> series plots each
    /// sample's <see cref="DeltaSample.Value"/> — the gauge's reading — and reads neither the delta nor the
    /// interval: a gauge row stores no delta (NULL since V132 / v62), and a pre-rung gauge row's (0, 0) marker was
    /// a claim about a delta nobody should have taken, not about the level, so it does not break the line. A
    /// null value plots NaN. The two delta bases follow the three-state interval rule:
    /// <list type="bullet">
    /// <item>delta <c>null</c> → <see cref="double.NaN"/>: a row with no delta (a gauge row summed into a
    /// delta series, which the readers' type rule should not let happen) has nothing to plot.</item>
    /// <item>interval <c>0</c> → <see cref="double.NaN"/> for BOTH delta bases. The delta beside a 0 is the
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
            if (basis == DeltaBasis.Level)
            {
                ys[i] = s.Value is long level ? level : double.NaN;
                continue;
            }

            if (s.Delta is not long delta || s.IntervalSeconds == 0)
            {
                ys[i] = double.NaN;
                continue;
            }

            if (basis == DeltaBasis.PerInterval)
            {
                ys[i] = delta;
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

            ys[i] = seconds > 0 ? delta / seconds : double.NaN;
        }

        return ys;
    }

    /// <summary>The legend entry: the counter's own name for a rate (its name already says <c>/sec</c>) and for
    /// a gauge (its name is its unit: <c>Total Server Memory (KB)</c>), the name plus
    /// <see cref="PerIntervalLegendSuffix"/> for a per-interval delta.</summary>
    public static string LegendLabel(string counterName, DeltaBasis basis) =>
        basis == DeltaBasis.PerInterval ? counterName + PerIntervalLegendSuffix : counterName;

    /// <summary>
    /// The one Y-axis label for a chart carrying the given bases: <see cref="PerSecondAxisLabel"/> when all
    /// are rates, <see cref="PerIntervalAxisLabel"/> when all are deltas, <see cref="LevelAxisLabel"/> when all
    /// are gauges, the <see cref="MixedAxisParts"/> present joined by <see cref="MixedAxisSeparator"/> when
    /// more than one basis is plotted, <see cref="EmptyAxisLabel"/> when nothing was plotted.
    /// </summary>
    public static string YAxisLabel(IEnumerable<DeltaBasis> plottedBases)
    {
        ArgumentNullException.ThrowIfNull(plottedBases);

        var present = new HashSet<DeltaBasis>(plottedBases);
        if (present.Count == 0) return EmptyAxisLabel;
        if (present.Count == 1)
        {
            return present.Contains(DeltaBasis.PerSecond) ? PerSecondAxisLabel
                : present.Contains(DeltaBasis.Level) ? LevelAxisLabel
                : PerIntervalAxisLabel;
        }

        var parts = new List<string>(3);
        foreach (var basis in new[] { DeltaBasis.PerSecond, DeltaBasis.PerInterval, DeltaBasis.Level })
        {
            if (present.Contains(basis)) parts.Add(MixedAxisParts[basis]);
        }

        return string.Join(MixedAxisSeparator, parts);
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
