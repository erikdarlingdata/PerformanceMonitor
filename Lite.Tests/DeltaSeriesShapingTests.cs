/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3653 A7 — the shaping both viewers' perfmon charts and both latch/spinlock snapshot grids do through
/// <see cref="DeltaSeriesShaping"/>, pinned value-by-value. The perfmon chart plotted <c>delta_cntr_value</c>
/// under "Value" with the fetched interval unused, drew a restart's fabricated (0, 0) as a real trough, and the
/// snapshot grids rendered that same (0, 0) as "Δ 0". These pins are the three-state interval rule
/// (#2234 / #3540: 0 = unknowable, NULL = never stored, n = measured) applied to a plotted point and to a
/// grid cell, the stored-type classification that arrived with Darling V132 / Lite v62 (a gauge plots as its
/// level; a rate per second; everything else per interval), and the name-suffix rate proxy that is now the
/// NULL-type fallback only.
///
/// <para>The helper lives in <c>PerformanceMonitor.Common</c> (no WPF, no ScottPlot) precisely so this file
/// runs outside Windows: it was executed on the Mac through the net10.0 mactest harness against the built
/// <c>PerformanceMonitor.Common.dll</c> before the PR opened, so the values below are observed, not
/// assumed. The rendering that consumes these arrays (ScottPlot's NaN line break, WPF's
/// <c>TargetNullValue</c>) is first seen in CI / at install.</para>
/// </summary>
public sealed class DeltaSeriesShapingTests
{
    private static readonly DateTime T0 = new(2026, 9, 18, 12, 0, 0, DateTimeKind.Unspecified);

    private static DeltaSample At(int minutes, long delta, long? interval) =>
        new(T0.AddMinutes(minutes), delta, interval);

    /* ---- the stored type decides (V132 / v62) ------------------------------------------------------------- */

    /// <summary>A stored type wins over the name, whatever the name says: a gauge id makes a <c>/sec</c>-named
    /// counter a level, a rate id makes a name without the suffix per second, and the average/fraction/base
    /// family is per interval however it is named. The proxy's stated mis-classes are corrected by exactly
    /// this arm — <c>Temp Tables Creation Rate</c> and <c>Lock Wait Time (ms)</c> are bulk counts, and their
    /// stored type says so.</summary>
    [Theory]
    [InlineData("Total Server Memory (KB)", PerfmonCounterTypes.PerfCounterLargeRawCount, DeltaBasis.Level)]
    [InlineData("Memory Grants Pending", PerfmonCounterTypes.PerfCounterRawCount, DeltaBasis.Level)]
    [InlineData("Batch Requests/sec", PerfmonCounterTypes.PerfCounterLargeRawCount, DeltaBasis.Level)]   // the type, not the name
    [InlineData("Batch Requests/sec", PerfmonCounterTypes.PerfCounterBulkCount, DeltaBasis.PerSecond)]
    [InlineData("Temp Tables Creation Rate", PerfmonCounterTypes.PerfCounterBulkCount, DeltaBasis.PerSecond)]
    [InlineData("Lock Wait Time (ms)", PerfmonCounterTypes.PerfCounterCounter, DeltaBasis.PerSecond)]
    [InlineData("Lock waits", PerfmonCounterTypes.PerfAverageBulk, DeltaBasis.PerInterval)]
    [InlineData("Buffer cache hit ratio", PerfmonCounterTypes.PerfLargeRawFraction, DeltaBasis.PerInterval)]
    [InlineData("Buffer cache hit ratio base", PerfmonCounterTypes.PerfLargeRawBase, DeltaBasis.PerInterval)]
    [InlineData("Something/sec", 424242, DeltaBasis.PerInterval)]   // an id the vocabulary has never seen is Other, never a rate
    public void BasisFor_AStoredType_DecidesOverTheName(string counterName, int cntrType, DeltaBasis expected)
    {
        Assert.Equal(expected, DeltaSeriesShaping.BasisFor(counterName, cntrType));
    }

    /// <summary>A NULL type is the proxy, byte for byte: the one-argument overload's verdict, which never says
    /// Level — so a pre-rung gauge row plots its stored delta per interval, as it did before the rung.</summary>
    [Theory]
    [InlineData("Batch Requests/sec", DeltaBasis.PerSecond)]
    [InlineData("Version Cleanup rate (KB/s)", DeltaBasis.PerSecond)]
    [InlineData("Total Server Memory (KB)", DeltaBasis.PerInterval)]
    [InlineData("Temp Tables Creation Rate", DeltaBasis.PerInterval)]
    [InlineData(null, DeltaBasis.PerInterval)]
    public void BasisFor_ANullType_IsTheNameProxy(string? counterName, DeltaBasis expected)
    {
        Assert.Equal(expected, DeltaSeriesShaping.BasisFor(counterName, null));
        Assert.Equal(DeltaSeriesShaping.BasisFor(counterName), DeltaSeriesShaping.BasisFor(counterName, null));
    }

    /* ---- the rate proxy (the NULL-type fallback) ---------------------------------------------------------- */

    [Theory]
    [InlineData("Batch Requests/sec")]
    [InlineData("Log Bytes Flushed/sec")]
    [InlineData("Transactions/sec")]
    [InlineData("Number of Deadlocks/sec")]
    [InlineData("Version Cleanup rate (KB/s)")]
    [InlineData("Version Generation rate (KB/s)")]
    [InlineData("Batch Requests/SEC ")]   // case and the RTRIM the collector already applies
    public void BasisFor_ReadsThePerSecondSuffixes_AsARate(string counterName)
    {
        Assert.Equal(DeltaBasis.PerSecond, DeltaSeriesShaping.BasisFor(counterName));
    }

    /// <summary>The proxy's stated limits, pinned as limits: these are per-interval under the proxy even where
    /// the engine's cntr_type would say otherwise (Temp Tables Creation Rate and Lock Wait Time (ms) are bulk
    /// counts without the suffix). The stored type, not a longer suffix list, is the fix — it applies the
    /// moment a row carries one (<see cref="BasisFor_AStoredType_DecidesOverTheName"/>) — so a "helpful"
    /// widening that starts classifying the FALLBACK by anything other than the suffix fails here and has to
    /// say why.</summary>
    [Theory]
    [InlineData("Page life expectancy")]
    [InlineData("Memory Grants Pending")]
    [InlineData("Lock waits")]
    [InlineData("Temp Tables Creation Rate")]
    [InlineData("Lock Wait Time (ms)")]
    [InlineData("Total Server Memory (KB)")]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void BasisFor_EverythingElse_IsPerInterval(string? counterName)
    {
        Assert.Equal(DeltaBasis.PerInterval, DeltaSeriesShaping.BasisFor(counterName));
    }

    /* ---- shaping a rate series ---------------------------------------------------------------------------- */

    [Fact]
    public void PerSecond_DividesTheDeltaByTheStoredInterval()
    {
        var ys = DeltaSeriesShaping.Shape(new[] { At(0, 1500, 300), At(5, 600, 120) }, DeltaBasis.PerSecond);

        Assert.Equal(new[] { 5.0, 5.0 }, ys);
    }

    /// <summary>(0, n) is a MEASUREMENT — an idle interval — and plots 0; (0, 0) is the marker and plots NaN.
    /// The two zeros are the whole point of #2234's interval column, and a chart that cannot tell them apart
    /// is the lie this fixes.</summary>
    [Fact]
    public void PerSecond_IdleIsZero_UnknowableIsNaN()
    {
        var ys = DeltaSeriesShaping.Shape(new[] { At(0, 0, 300), At(5, 0, 0), At(10, 900, 300) }, DeltaBasis.PerSecond);

        Assert.Equal(0.0, ys[0]);
        Assert.True(double.IsNaN(ys[1]));
        Assert.Equal(3.0, ys[2]);
    }

    /// <summary>The NULL arm: a pre-column row divides by the seconds to the previous sample — the LAG fallback
    /// every SQL-side reader uses — and the first sample, with no previous, is unrated rather than fabricated
    /// (#3642's first-point class).</summary>
    [Fact]
    public void PerSecond_NullInterval_FallsBackToRowSpacing_FirstPointUnrated()
    {
        var ys = DeltaSeriesShaping.Shape(new[] { At(0, 1500, null), At(5, 1500, null), At(15, 3000, null) }, DeltaBasis.PerSecond);

        Assert.True(double.IsNaN(ys[0]));
        Assert.Equal(5.0, ys[1]);   // 1500 over the 300 s to the previous sample
        Assert.Equal(5.0, ys[2]);   // 3000 over 600 s
    }

    /// <summary>A stored interval wins over the spacing when the row has one — the spacing here says 300 s,
    /// the store says 120 s, and 120 s is the sweep the delta actually accrued over.</summary>
    [Fact]
    public void PerSecond_StoredIntervalWinsOverRowSpacing()
    {
        var ys = DeltaSeriesShaping.Shape(new[] { At(0, 100, 300), At(5, 600, 120) }, DeltaBasis.PerSecond);

        Assert.Equal(5.0, ys[1]);
    }

    [Fact]
    public void PerSecond_NullInterval_DuplicateTimestamp_IsUnratedNotDivisionByZero()
    {
        var ys = DeltaSeriesShaping.Shape(new[] { At(0, 10, null), At(0, 10, null) }, DeltaBasis.PerSecond);

        Assert.True(double.IsNaN(ys[0]));
        Assert.True(double.IsNaN(ys[1]));
    }

    [Fact]
    public void PerSecond_NegativeStoredInterval_IsUnrated()
    {
        var ys = DeltaSeriesShaping.Shape(new[] { At(0, 10, -5) }, DeltaBasis.PerSecond);

        Assert.True(double.IsNaN(ys[0]));
    }

    /* ---- shaping a per-interval series -------------------------------------------------------------------- */

    /// <summary>A non-rate counter keeps its raw delta whatever the interval says — measured or never stored —
    /// EXCEPT the marker, whose delta is the calculator's fabricated zero and plots NaN like any other
    /// unknowable point.</summary>
    [Fact]
    public void PerInterval_KeepsTheRawDelta_ExceptOnTheMarker()
    {
        var ys = DeltaSeriesShaping.Shape(new[] { At(0, 42, 300), At(5, 7, null), At(10, 0, 0), At(15, 0, 300) }, DeltaBasis.PerInterval);

        Assert.Equal(42.0, ys[0]);
        Assert.Equal(7.0, ys[1]);
        Assert.True(double.IsNaN(ys[2]));
        Assert.Equal(0.0, ys[3]);
    }

    [Fact]
    public void Shape_IsParallelToItsInput_AndEmptyForEmpty()
    {
        Assert.Empty(DeltaSeriesShaping.Shape(Array.Empty<DeltaSample>(), DeltaBasis.PerSecond));
        Assert.Equal(3, DeltaSeriesShaping.Shape(new[] { At(0, 1, 1), At(1, 1, 1), At(2, 1, 1) }, DeltaBasis.PerInterval).Length);
    }

    /// <summary>A row with no delta — a gauge row that reached a delta series, which the readers' type rule should
    /// not allow — plots NaN on both delta bases rather than a manufactured 0 (#3642's class).</summary>
    [Fact]
    public void DeltaBases_NullDelta_IsNaN_NotZero()
    {
        var samples = new[] { new DeltaSample(T0, null, null, 100), new DeltaSample(T0.AddMinutes(5), null, 300, 100) };

        Assert.All(DeltaSeriesShaping.Shape(samples, DeltaBasis.PerSecond), y => Assert.True(double.IsNaN(y)));
        Assert.All(DeltaSeriesShaping.Shape(samples, DeltaBasis.PerInterval), y => Assert.True(double.IsNaN(y)));
    }

    /* ---- shaping a gauge series (V132 / v62) -------------------------------------------------------------- */

    /// <summary>A gauge plots its stored value as the level it is and reads NEITHER the delta nor the interval:
    /// the post-rung row (NULL delta, NULL interval) and the pre-rung row (a meaningless delta, a measured
    /// interval) plot the same way, and a pre-rung (0, 0) marker — the calculator's verdict on a delta nobody
    /// should have taken, in practice a FALLING level read as a reset — does not break the line. That is the
    /// correction: the store held the level all along. A null value plots NaN rather than inventing one.</summary>
    [Fact]
    public void Level_PlotsTheStoredValue_AndIgnoresDeltaAndInterval()
    {
        var samples = new[]
        {
            new DeltaSample(T0, null, null, 8_388_608),                 // post-rung gauge row
            new DeltaSample(T0.AddMinutes(5), 12_345, 300, 8_400_953),   // pre-rung row: a delta nobody should read
            new DeltaSample(T0.AddMinutes(10), 0, 0, 8_000_000),         // pre-rung marker: the level FELL, and that is the reading
            new DeltaSample(T0.AddMinutes(15), null, null, null),        // no value at all
        };

        var ys = DeltaSeriesShaping.Shape(samples, DeltaBasis.Level);

        Assert.Equal(8_388_608.0, ys[0]);
        Assert.Equal(8_400_953.0, ys[1]);
        Assert.Equal(8_000_000.0, ys[2]);
        Assert.True(double.IsNaN(ys[3]));
    }

    /* ---- labels ------------------------------------------------------------------------------------------- */

    [Fact]
    public void LegendLabel_RateAndGaugeKeepTheirNames_DeltaSaysSo()
    {
        Assert.Equal("Batch Requests/sec", DeltaSeriesShaping.LegendLabel("Batch Requests/sec", DeltaBasis.PerSecond));
        Assert.Equal("Lock waits (Δ/interval)", DeltaSeriesShaping.LegendLabel("Lock waits", DeltaBasis.PerInterval));
        Assert.Equal("Total Server Memory (KB)", DeltaSeriesShaping.LegendLabel("Total Server Memory (KB)", DeltaBasis.Level));   // its name is its unit
    }

    /// <summary>One basis → its own label; more than one → the parts present, in the fixed order rate · delta ·
    /// gauge, each with its qualifier — so a mixed axis names every kind of number on it and only those.</summary>
    [Fact]
    public void YAxisLabel_NamesWhatIsPlotted()
    {
        Assert.Equal("per second", DeltaSeriesShaping.YAxisLabel(new[] { DeltaBasis.PerSecond, DeltaBasis.PerSecond }));
        Assert.Equal("Δ per interval", DeltaSeriesShaping.YAxisLabel(new[] { DeltaBasis.PerInterval }));
        Assert.Equal("value (gauge)", DeltaSeriesShaping.YAxisLabel(new[] { DeltaBasis.Level, DeltaBasis.Level }));

        Assert.Equal("per second (rates) · Δ per interval (others)",
            DeltaSeriesShaping.YAxisLabel(new[] { DeltaBasis.PerInterval, DeltaBasis.PerSecond }));
        Assert.Equal("per second (rates) · value (gauges)",
            DeltaSeriesShaping.YAxisLabel(new[] { DeltaBasis.Level, DeltaBasis.PerSecond }));
        Assert.Equal("Δ per interval (others) · value (gauges)",
            DeltaSeriesShaping.YAxisLabel(new[] { DeltaBasis.Level, DeltaBasis.PerInterval }));
        Assert.Equal("per second (rates) · Δ per interval (others) · value (gauges)",
            DeltaSeriesShaping.YAxisLabel(new[] { DeltaBasis.Level, DeltaBasis.PerInterval, DeltaBasis.PerSecond, DeltaBasis.Level }));
    }

    /// <summary>Nothing plotted → the label the chart carried before #3653, so an empty chart is unchanged.</summary>
    [Fact]
    public void YAxisLabel_EmptyIsTheOldLabel()
    {
        Assert.Equal("Value", DeltaSeriesShaping.YAxisLabel(Array.Empty<DeltaBasis>()));
    }

    /* ---- the Y ceiling ------------------------------------------------------------------------------------ */

    [Fact]
    public void MaxFinite_IgnoresNaN_AndFallsBackWhenNothingIsFinite()
    {
        Assert.Equal(3.0, DeltaSeriesShaping.MaxFinite(new[] { 1.0, double.NaN, 3.0 }, fallback: 0));
        Assert.Equal(0.0, DeltaSeriesShaping.MaxFinite(new[] { double.NaN, double.NaN }, fallback: 0));
        Assert.Equal(0.0, DeltaSeriesShaping.MaxFinite(Array.Empty<double>(), fallback: 0));
        Assert.Equal(-2.0, DeltaSeriesShaping.MaxFinite(new[] { -7.0, -2.0 }, fallback: 0));   // a real max, even below the fallback
        Assert.Equal(1.0, DeltaSeriesShaping.MaxFinite(new[] { 1.0, double.PositiveInfinity }, fallback: 0));
    }

    /* ---- the grid cells ----------------------------------------------------------------------------------- */

    /// <summary>The interval decides, not the delta's value: a 5 beside a 0 interval is as fabricated as a 0
    /// beside one (the calculator never writes it, and the rule should not depend on that).</summary>
    [Fact]
    public void ReadableDelta_NullOnTheMarker_StoredDeltaOtherwise()
    {
        Assert.Null(DeltaSeriesShaping.ReadableDelta(0, 0));
        Assert.Null(DeltaSeriesShaping.ReadableDelta(5, 0));
        Assert.Equal(5L, DeltaSeriesShaping.ReadableDelta(5, 60));
        Assert.Equal(0L, DeltaSeriesShaping.ReadableDelta(0, 60));   // idle is a measurement
        Assert.Equal(5L, DeltaSeriesShaping.ReadableDelta(5, null)); // pre-column row: the delta stands
    }

    [Fact]
    public void IntervalDisplay_SpellsTheThreeStates()
    {
        Assert.Equal("restart / first sample", DeltaSeriesShaping.IntervalDisplay(0));
        Assert.Equal("not stored", DeltaSeriesShaping.IntervalDisplay(null));
        Assert.Equal("60", DeltaSeriesShaping.IntervalDisplay(60));
        Assert.Equal("1234", DeltaSeriesShaping.IntervalDisplay(1234));   // invariant, no grouping separator
    }

    /* ---- the type stays free of the UI stack -------------------------------------------------------------- */

    /// <summary>The reason this file can run on a Mac: the helper's assembly must not reference WPF or
    /// ScottPlot. A future "convenience" overload taking a <c>Scatter</c> would drag <c>PerformanceMonitor.Common</c>
    /// onto <c>net10.0-windows</c> for every consumer, and the mactest harness would stop being able to load it.</summary>
    [Fact]
    public void TheHelperAssembly_ReferencesNeitherWpfNorScottPlot()
    {
        var referenced = typeof(DeltaSeriesShaping).Assembly.GetReferencedAssemblies();
        foreach (var name in referenced)
        {
            Assert.DoesNotContain("PresentationFramework", name.Name, StringComparison.Ordinal);
            Assert.DoesNotContain("PresentationCore", name.Name, StringComparison.Ordinal);
            Assert.DoesNotContain("WindowsBase", name.Name, StringComparison.Ordinal);
            Assert.DoesNotContain("ScottPlot", name.Name, StringComparison.Ordinal);
        }
    }
}
