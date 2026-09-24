/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Analysis.Baselines;

/// <summary>
/// #3653 A8 option B (lane L1a): the precomputed (hour, dow) baseline map bundled with the window
/// clock it is looked up against — the two things <c>AnomalyGate.EvaluateTiles</c> needs to score a
/// window that has been split into per-target-local-hour tiles, per the design's §1/§3.
///
/// <para><b>Why the clock rides along.</b> The map's keys are LOCAL (hour, dow) — the same key the
/// provider's baseline SQL wrote (<c>BaselineLocalClock</c>'s class remarks: hour-of-week baselines key
/// on the target's local clock, not UTC). A tile's own <c>WindowTile.LocalHour</c> is already local (the
/// window SQL groups by <c>date_trunc('hour', BaselineLocalClock.LocalCollectionTimeSql)</c>), so
/// <see cref="For"/> takes the tile's own (hour, dow) directly and does not resolve anything itself —
/// <see cref="WindowClock"/> is carried here for a caller that needs to convert a tile's local hour back
/// to a UTC instant (<c>tile_start_ticks</c> in the design's §1) or that has only a UTC instant to key
/// with; it is not consulted by <see cref="For"/>.</para>
///
/// <para><b>Where it comes from.</b> The provider accessor (<c>PgBaselineProvider.GetBucketMapAsync</c>,
/// <c>Lite/Analysis/BaselineProvider.cs</c> — lane L1b) reuses the SAME cached baseline compute
/// (<c>GetOrComputeBaselinesAsync</c>) and re-resolves <see cref="BaselineLocalClock.Resolve"/> over the
/// ANALYSIS window rather than the cached 30-day baseline window, because a DST step can fall inside a
/// short analysis window without falling inside — or falling at a different instant within — the longer
/// cached one (design §1). This type carries the result of that re-resolution; it does not perform it.</para>
/// </summary>
public sealed record BaselineBucketMap(
    IReadOnlyDictionary<(int HourOfDay, int DayOfWeek), BaselineBucket> Buckets,
    LocalClockWindow WindowClock)
{
    /// <summary>
    /// The bucket for one tile's (local hour, local day-of-week) key, through the same Full →
    /// HourOnly → Flat selection every other caller of <see cref="BaselineMath.SelectBucket"/> gets —
    /// each tile is looked up independently, so each tile can land on a different tier and carry its
    /// own trust/zero-history state (design §1: "each tile gets its own trust and zero-history state").
    /// </summary>
    public BaselineBucket For(int hour, int dow) => BaselineMath.SelectBucket(Buckets, hour, dow);

    /// <summary>
    /// The empty map — no buckets, UTC keying — for a caller with nothing computed yet (mirrors
    /// <c>BaselineBucket.Empty</c>'s role for a single bucket). <paramref name="end"/> is the window
    /// clock's anchor, exactly as <c>LocalClockWindow.Utc</c> takes it.
    /// </summary>
    public static BaselineBucketMap Empty(DateTime end) =>
        new(new Dictionary<(int, int), BaselineBucket>(), LocalClockWindow.Utc(end));
}

/// <summary>
/// #3653 A8 option B (lane L1a): one target-local hour's slice of an analysis window, as the tiled
/// window SQL returns it (design §1: one row per tile — <c>local_hour, peak, mean, samples</c>, plus
/// <c>peak_time</c> where the family has one). <see cref="LocalHour"/> is the tile's key instant, Kind
/// Unspecified like every other local time this codebase carries (<c>LocalClockWindow.ToLocal</c>) — its
/// <c>Hour</c> and <c>DayOfWeek</c> are the <c>(HourOfDay, DayOfWeek)</c> pair <see cref="BaselineBucketMap.For"/>
/// is keyed with.
/// </summary>
/// <param name="LocalHour">The tile's target-local hour boundary (<c>date_trunc('hour', …)</c> over the
/// window's local clock). Kind Unspecified.</param>
/// <param name="Peak">The tile's window MAX for the family's statistic — the value <c>AnomalyGate</c>'s
/// peak clause judges.</param>
/// <param name="Mean">The tile's window MEAN for the family's statistic — the value the tile-mode mean
/// clause judges under the SAME corrected cutoff as the peak (design §1: "tile mode corrects the mean
/// too").</param>
/// <param name="Samples">Samples observed in this tile. Below <c>AnomalyThresholds.MinTileSamples</c>
/// the tile is not scored (design §1's minimum-samples edge rule).</param>
/// <param name="PeakTimeUtc">The UTC instant the peak was observed at, for families that track it
/// (<c>peak_time</c> in the design). <c>null</c> for families with no per-tile peak time.</param>
public readonly record struct WindowTile(
    DateTime LocalHour,
    double Peak,
    double Mean,
    long Samples,
    DateTime? PeakTimeUtc = null);
