/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Decision-table pins for the shared <see cref="CpuAttribution"/> (#2320) — the denominator the
/// top-CPU rankings never handed the caller. This SAME table is pinned identically in Darling.Tests so
/// the two SKUs cannot drift. The load-bearing cases are #2235's field numbers: the pre-fix reads
/// explained ~10% of one box and nothing said so, and the Datadog disagreement died the moment its
/// worker_time sum was divided by the box's consumed CPU-seconds and produced 137%.
/// </summary>
public sealed class CpuAttributionTests
{
    /* The #2235 window: 8 vCPU, 2 hours, RDS CPU averaging 18% → 10,368 measured core-seconds. */
    private const double FieldAvgPct = 18.0;
    private const int FieldCores = 8;
    private const double FieldHours = 2.0;
    private const int CoveredSamples = 120;
    private const double FullSpan = 2.0;

    [Fact]
    public void TheFieldWindowComputesTheMeasuredDenominator()
    {
        var window = CpuAttribution.Compute(
            attributedCpuMs: 3_000_000, FieldAvgPct, CoveredSamples, FullSpan, FieldCores, FieldHours);

        Assert.NotNull(window);
        Assert.Equal(10_368, window.Value.MeasuredSqlCpuSeconds, precision: 0);
        Assert.Equal(3_000, window.Value.AttributedCpuSeconds, precision: 0);
        Assert.Equal(0.289, window.Value.AttributedRatio, precision: 3);
    }

    /// <summary>Under half explained → the note says where the rest lives.</summary>
    [Fact]
    public void ALowRatioCarriesTheRemainderNote()
    {
        var window = CpuAttribution.Compute(3_000_000, FieldAvgPct, CoveredSamples, FullSpan, FieldCores, FieldHours);

        Assert.NotNull(window!.Value.Note);
        Assert.Contains("below the ranking cut", window.Value.Note, System.StringComparison.Ordinal);
    }

    /// <summary>
    /// The impossibility detector: attributed exceeding measured by more than sampling skew could
    /// explain gets the skew note — the exact division that settled #2235's Datadog claim (137%).
    /// </summary>
    [Fact]
    public void AttributedBeyondMeasuredCarriesTheSkewNote()
    {
        var window = CpuAttribution.Compute(14_229_000, FieldAvgPct, CoveredSamples, FullSpan, FieldCores, FieldHours);

        Assert.NotNull(window);
        Assert.True(window.Value.AttributedRatio > 1.3);
        Assert.Contains("exceeds the measured total", window.Value.Note, System.StringComparison.Ordinal);
    }

    /// <summary>An ordinary healthy ratio says nothing — notes are for the two failure directions.</summary>
    [Fact]
    public void AMidRatioCarriesNoNote()
    {
        var window = CpuAttribution.Compute(8_000_000, FieldAvgPct, CoveredSamples, FullSpan, FieldCores, FieldHours);

        Assert.NotNull(window);
        Assert.Null(window.Value.Note);
    }

    /// <summary>
    /// Every way the denominator can be untrustworthy omits the window rather than fabricating one:
    /// no average, no samples, unknown or nonsensical cores, a degenerate window, thin coverage,
    /// and a hard-zero average (the ratio would divide by zero).
    /// </summary>
    [Fact]
    public void AnUnsupportableDenominatorIsOmittedNeverFabricated()
    {
        Assert.Null(CpuAttribution.Compute(1_000, null, CoveredSamples, FullSpan, FieldCores, FieldHours));
        Assert.Null(CpuAttribution.Compute(1_000, FieldAvgPct, 0, FullSpan, FieldCores, FieldHours));
        Assert.Null(CpuAttribution.Compute(1_000, FieldAvgPct, CoveredSamples, FullSpan, null, FieldHours));
        Assert.Null(CpuAttribution.Compute(1_000, FieldAvgPct, CoveredSamples, FullSpan, 0, FieldHours));
        Assert.Null(CpuAttribution.Compute(1_000, FieldAvgPct, CoveredSamples, FullSpan, FieldCores, 0));
        /* A 24h window whose samples span only 5 hours: the average extrapolates a fragment —
           span-based on purpose, so a SLOW but steady cadence still qualifies (the review catch). */
        Assert.Null(CpuAttribution.Compute(1_000, FieldAvgPct, 300, 5.0, FieldCores, 24));
        /* Two lonely points can bracket a wide span — the sample floor rejects them. */
        Assert.Null(CpuAttribution.Compute(1_000, FieldAvgPct, 2, FullSpan, FieldCores, FieldHours));
        Assert.Null(CpuAttribution.Compute(1_000, 0.0, CoveredSamples, FullSpan, FieldCores, FieldHours));
    }

    /// <summary>
    /// The span floor's boundary: samples spanning exactly half the window qualify — and cadence
    /// never enters it, so a server whose cpu_utilization schedule was slowed to 5 minutes (24
    /// samples in 2 hours) qualifies exactly like a 1-minute one (the review catch: a
    /// count-per-minute expectation would have disqualified it permanently).
    /// </summary>
    [Fact]
    public void SpanAtTheFloorQualifies_AtAnyCadence()
    {
        Assert.NotNull(CpuAttribution.Compute(1_000, FieldAvgPct, 60, FieldHours * 0.5, FieldCores, FieldHours));
        Assert.NotNull(CpuAttribution.Compute(1_000, FieldAvgPct, 24, FullSpan, FieldCores, FieldHours));
    }
}
