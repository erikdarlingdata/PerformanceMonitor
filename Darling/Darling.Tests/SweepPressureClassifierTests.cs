/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Decision-table pins for the shared <see cref="SweepPressureClassifier"/> (#2296) — the roll-up both
/// SKUs' get_collection_health serve so half-rate collection stops being visible only as a service-log
/// warning. This SAME table is pinned identically in Lite.Tests so the two SKUs cannot drift.
///
/// <para>The load-bearing case is the motivating measurement: prod-sql-use2-multi-01's four heavy
/// collectors averaged 22,141 + 16,590 + 13,544 + 8,437 ms against a 60s cadence — the body could not
/// fit, every relaunch was skipped (~50 warnings/hour), the server collected at half rate, and all 40
/// collectors read HEALTHY, because from each one's own seat nothing was wrong.</para>
/// </summary>
public sealed class SweepPressureClassifierTests
{
    private static (string, double, int) C(string name, double avgMs, int freqMin) => (name, avgMs, freqMin);

    /// <summary>The #2296 measurement verbatim: ~101% of the minute — SATURATED, not a warning-log easter egg.</summary>
    [Fact]
    public void TheMotivatingServerReadsSaturated()
    {
        var pressure = SweepPressureClassifier.Compute(new[]
        {
            C("procedure_stats", 22_141, 1),
            C("query_store", 16_590, 1),
            C("plan_correction", 13_544, 1),
            C("query_stats", 8_437, 1),
        });

        Assert.Equal(SweepPressureClassifier.Saturated, pressure.Verdict);
        Assert.Equal(60_712, pressure.BusyMsPerMinute, 3);
        Assert.True(pressure.BusyPercent > 100.0);
    }

    /// <summary>An ordinary in-region profile sits far below every threshold.</summary>
    [Fact]
    public void AHealthyProfileReadsOk()
    {
        var pressure = SweepPressureClassifier.Compute(new[]
        {
            C("wait_stats", 180, 1),
            C("cpu_utilization", 95, 1),
            C("query_stats", 2_400, 1),
            C("database_size_stats", 1_200, 60),
        });

        Assert.Equal(SweepPressureClassifier.Ok, pressure.Verdict);
        Assert.True(pressure.BusyPercent < 5.0);
    }

    /// <summary>
    /// The band edges, both inclusive: 45,000 ms/min is exactly 75% (AT_RISK), 60,000 exactly 100%
    /// (SATURATED). Inclusive because the average already smooths spikes — a body that AVERAGES the
    /// boundary is over it half the time.
    /// </summary>
    [Fact]
    public void TheBandEdgesAreInclusive()
    {
        Assert.Equal(SweepPressureClassifier.Ok,
            SweepPressureClassifier.Compute(new[] { C("a", 44_999, 1) }).Verdict);
        Assert.Equal(SweepPressureClassifier.AtRisk,
            SweepPressureClassifier.Compute(new[] { C("a", 45_000, 1) }).Verdict);
        Assert.Equal(SweepPressureClassifier.AtRisk,
            SweepPressureClassifier.Compute(new[] { C("a", 59_999, 1) }).Verdict);
        Assert.Equal(SweepPressureClassifier.Saturated,
            SweepPressureClassifier.Compute(new[] { C("a", 60_000, 1) }).Verdict);
    }

    /// <summary>
    /// A non-recurring collector (frequency 0: on-load, unknown name) contributes nothing however long it
    /// runs — it does not compete for the sweep. A zero-duration entry likewise adds nothing.
    /// </summary>
    [Fact]
    public void OnLoadAndZeroDurationCollectorsAreExcluded()
    {
        var pressure = SweepPressureClassifier.Compute(new[]
        {
            C("database_config", 500_000, 0),
            C("trace_flags", 0, 1),
            C("wait_stats", 300, 1),
        });

        Assert.Equal(SweepPressureClassifier.Ok, pressure.Verdict);
        Assert.Equal(300, pressure.BusyMsPerMinute, 3);
    }

    /// <summary>
    /// Amortization is by each collector's OWN cadence: an hourly collector averaging 30s costs 500 ms of
    /// every minute, not 30,000 — the mistake this pin forbids is charging slow collectors at the fast
    /// cadence, which would flag every server with a heavy daily job.
    /// </summary>
    [Fact]
    public void SlowCollectorsAreAmortizedByTheirOwnCadence()
    {
        var pressure = SweepPressureClassifier.Compute(new[] { C("index_object_stats", 30_000, 60) });

        Assert.Equal(500, pressure.BusyMsPerMinute, 3);
        Assert.Equal(SweepPressureClassifier.Ok, pressure.Verdict);
    }

    /// <summary>No collectors — a server before first collection — is OK with zero demand, never a verdict from nothing.</summary>
    [Fact]
    public void AnEmptyWindowReadsOkWithZeroDemand()
    {
        var pressure = SweepPressureClassifier.Compute(Array.Empty<(string, double, int)>());

        Assert.Equal(SweepPressureClassifier.Ok, pressure.Verdict);
        Assert.Equal(0, pressure.BusyMsPerMinute);
        Assert.Equal(0, pressure.BusyPercent);
    }
}
