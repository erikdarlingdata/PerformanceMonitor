/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// One subject's persisted <see cref="AlertPersistenceGate"/> state, as the built-in alert catalog stores it
/// through <see cref="IAlertStateStore"/> (#3282). The gate itself is pure and owns no storage; this is the
/// row shape the caller writes between observations.
///
/// <para><b>Why the sample instant rides along.</b> The gate counts OBSERVATIONS, and for a gauge read out of
/// the collected store an observation is one collected SAMPLE — not one alert sweep. The sweep runs every 30
/// seconds while a CPU sample lands about once a minute, so a sweep that re-reads the row it read last time is
/// looking at the same observation twice. Counting sweeps would therefore satisfy "three consecutive breaches"
/// inside a 90-second excursion, which is shorter than the excursions #3282 measured and would leave the
/// defect in place while appearing to fix it. <see cref="LastObservedSampleUtc"/> is what makes the count mean
/// samples: an observation whose sample instant has not advanced past it is not counted at all, and the streak
/// simply holds.</para>
///
/// <para>A record struct so the whole answer moves as ONE value — a caller cannot advance the counters and
/// forget the sample instant, which would make every subsequent re-read look fresh.</para>
/// </summary>
/// <param name="State">The gate's counters plus whether an incident is currently open.</param>
/// <param name="LastObservedSampleUtc">
/// The sample instant of the newest observation already counted into <paramref name="State"/>, or null when
/// this subject has counted none yet (a fresh subject, or a host that supplies no sample instant — see
/// <see cref="AlertServerSnapshot.CpuSampleTimeUtc"/> for what that degrades to).
/// </param>
public readonly record struct AlertPersistenceRecord(PersistenceState State, DateTime? LastObservedSampleUtc)
{
    /// <summary>The record of a subject never observed: no streak, not firing, nothing counted.</summary>
    public static AlertPersistenceRecord Initial => new(PersistenceState.Initial, null);
}
