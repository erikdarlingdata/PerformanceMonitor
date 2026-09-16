/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The fleet sweep's cadence knob constants (#3466, lane 2): the shipped default and both write
/// bounds, named once here so the V124 column default, the darling.json seed, the
/// <c>update_alert_settings</c> write bound, the Viewer's Settings gate and Restore-Defaults button,
/// and the worker's read-side clamp all resolve to the same figures — the #3060 discipline every
/// knob rung since V119 has followed, and the "setting did not stick" parity the write bounds exist
/// for: no value any writer accepts is a value the read-side clamp then rewrites.
///
/// <para><b>Why this lives in Storage rather than beside the engine.</b> The engine is in the
/// Service assembly, which the Viewer cannot reference — and the Viewer's save gate and
/// Restore-Defaults button both need the constants, the same reach problem V119 solved by putting
/// the Retention Held bounds on <see cref="TimescaleSupport"/>. These are bounds on a
/// <c>config_alert_settings</c> column's values, so the store assembly is where every writer can
/// see them.</para>
/// </summary>
public static class FleetSweepCadence
{
    /// <summary>The shipped cadence: hourly, the spec's own default and the grain the external
    /// evidence-day sweep ran at. Also the V124 column default (restated as a literal there because
    /// a rung is a SQL string, and pinned equal by <c>FleetSweepCadenceKnobRungTests</c>).</summary>
    public const int DefaultIntervalMinutes = 60;

    /// <summary>
    /// The floor. A sweep bands on signal counts aggregated over its span, and the collectors that
    /// feed those counts run on per-minute cadences — below ~15 minutes a span holds a handful of
    /// samples per source, and a verdict over a handful of samples is exactly the single-sample
    /// reading the spec's own hysteresis requirement exists to reject. The watch-item bars are
    /// stated in SWEEPS (two consecutive to open, two quiet to close), so a faster cadence also
    /// tightens the confirmation window the two-sweep bars were calibrated for at hourly; 15 minutes
    /// is the fastest cadence at which "two consecutive sweeps" still describes a sustained
    /// condition rather than one burst read twice.
    /// </summary>
    public const int IntervalMinutesFloor = 15;

    /// <summary>
    /// The ceiling: one sweep a day. The daily channel rollup (lane 4) is a digest OF sweeps at a
    /// daily ceiling per the owner's cadence ruling, and a sweep rarer than daily leaves that digest
    /// with nothing to roll up; it is also the span cap the engine applies when recovering from
    /// downtime, so no sweep ever claims to summarize a window longer than the slowest cadence an
    /// operator can configure.
    /// </summary>
    public const int IntervalMinutesCeiling = 1440;
}
