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
/// samples: an observation whose sample instant IS it is the same sample seen again, is not counted at all,
/// and the streak simply holds. Sameness is decided by equality, not by order (#3744): the instant is an
/// IDENTITY, and a different one is a different sample whichever clock stamped it — see
/// <c>AlertEngine.ObservePersistenceAsync</c> for why "strictly greater" was the wrong test (it read every
/// sample in a fall-back's repeated hour, and the first sample after a clock-frame change, as stale).</para>
///
/// <para>A record struct so the whole answer moves as ONE value — a caller cannot advance the counters and
/// forget the sample instant, which would make every subsequent re-read look fresh.</para>
/// </summary>
/// <param name="State">The gate's counters plus whether an incident is currently open.</param>
/// <param name="LastObservedSampleUtc">
/// The sample instant of the newest observation already counted into <paramref name="State"/>, or null when
/// this subject has counted none yet (a fresh subject, or a host that supplies no sample instant — see
/// <see cref="AlertServerSnapshot.CpuSampleTimeUtc"/> for what that degrades to).
/// <para>Naive UTC, as every gated arm's identity is — with one stated exception the name does not show: the
/// CPU arm's identity is the row's <c>sample_time_utc</c> twin only where the store has one (every row written
/// since Darling V134 / Lite v63), and the target's LOCAL <c>sample_time</c> on a row written before that, so
/// a record persisted by a pre-#3744 build, or off a pre-rung row, holds a local instant under this name. The
/// store cannot say which (<c>config.alert_persistence_state</c> has no frame column and this wave added no
/// rung), and it does not need to: the value is only ever compared for EQUALITY against the next read's
/// identity, so the frame is not load-bearing and a frame change reads as one new sample, once.</para>
/// </param>
public readonly record struct AlertPersistenceRecord(PersistenceState State, DateTime? LastObservedSampleUtc)
{
    /// <summary>The record of a subject never observed: no streak, not firing, nothing counted.</summary>
    public static AlertPersistenceRecord Initial => new(PersistenceState.Initial, null);
}
