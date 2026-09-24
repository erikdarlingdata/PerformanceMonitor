/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Analysis.Baselines;

/// <summary>
/// #3653 A8 option B (lane L1c): the shared per-hour tile helpers every detector call site (24 of them,
/// across the SQL Server store, the PostgreSQL target and Lite's DuckDB — design §2) uses, written ONCE
/// here so the three surfaces cannot drift on the tile key expression, the whole-window fallback, the
/// local-hour → UTC inversion, or the new fired-fact metadata keys.
/// </summary>
public static class WindowTiles
{
    /// <summary>
    /// The tile key expression: the window's local hour boundary. Runs byte-identical on PostgreSQL and
    /// DuckDB, because <c>date_trunc('hour', timestamp)</c> is supported by both, and
    /// <see cref="BaselineLocalClock.LocalCollectionTimeSql"/> already is (its own doc comment).
    ///
    /// <para>Binds <c>$4..$6</c> exactly as <see cref="BaselineLocalClock.LocalCollectionTimeSql"/>
    /// documents: the caller binds them from <see cref="BaselineBucketMap.WindowClock"/> — the ANALYSIS
    /// window's clock — NEVER from the baseline's own cached clock, because a DST step can fall inside a
    /// short analysis window without falling inside, or falling at the same instant within, the longer
    /// cached 30-day baseline window (design §1).</para>
    /// </summary>
    public const string LocalHourSql = "date_trunc('hour', " + BaselineLocalClock.LocalCollectionTimeSql + ")";

    /// <summary>
    /// The whole-window aggregate the never-blind fallback feeds to today's <c>EvaluateZScore</c> when no
    /// tile clears <c>AnomalyThresholds.MinTileSamples</c> (design §1's "Never-blind rule"): peak is the
    /// max of the tiles' peaks, mean is the samples-weighted mean, samples is the sum, <see cref="WindowTile.PeakTimeUtc"/>
    /// is the max-peak tile's (a tie goes to the LATER tile), and <see cref="WindowTile.LocalHour"/> is the
    /// earliest tile's.
    ///
    /// <para>An empty list, or a list whose samples sum to zero, returns <c>default</c> with
    /// <c>Samples == 0</c> — the caller treats that exactly as today's early "no data" return.</para>
    /// </summary>
    public static WindowTile WholeWindow(IReadOnlyList<WindowTile> tiles)
    {
        if (tiles is null || tiles.Count == 0)
        {
            return default;
        }

        double peak = double.NegativeInfinity;
        double weightedMeanSum = 0;
        long totalSamples = 0;
        DateTime? peakTimeUtc = null;
        DateTime earliestLocalHour = default;
        var haveEarliest = false;

        foreach (var tile in tiles)
        {
            if (!haveEarliest || tile.LocalHour < earliestLocalHour)
            {
                earliestLocalHour = tile.LocalHour;
                haveEarliest = true;
            }

            weightedMeanSum += tile.Mean * tile.Samples;
            totalSamples += tile.Samples;

            // Later tile wins a tie: >= so the last tile seen with the max peak sticks.
            if (tile.Peak >= peak)
            {
                peak = tile.Peak;
                peakTimeUtc = tile.PeakTimeUtc;
            }
        }

        if (totalSamples == 0)
        {
            return default;
        }

        return new WindowTile(
            earliestLocalHour,
            peak,
            weightedMeanSum / totalSamples,
            totalSamples,
            peakTimeUtc);
    }

    /// <summary>
    /// Inverts <see cref="BaselineLocalClock.LocalCollectionTimeSql"/>: the local hour, mapped back to the UTC instant it
    /// came from. Tries <c>local − OffsetBeforeMinutes</c> first, and accepts it when the result is before
    /// <see cref="LocalClockWindow.TransitionAtUtc"/>; otherwise uses <c>local − OffsetAfterMinutes</c>.
    ///
    /// <para><b>Fall-back ambiguity.</b> A repeated local hour (the fall-back DST transition) maps to its
    /// FIRST occurrence — the earlier, before-offset instant — because that candidate is tried first and
    /// accepted whenever it lands before the transition.</para>
    ///
    /// <para>Returns Kind Unspecified, as naive UTC — the same convention every other UTC instant in this
    /// codebase carries.</para>
    /// </summary>
    public static DateTime LocalHourToUtc(DateTime localHour, LocalClockWindow clock)
    {
        var beforeCandidate = DateTime.SpecifyKind(localHour.AddMinutes(-clock.OffsetBeforeMinutes), DateTimeKind.Unspecified);
        if (beforeCandidate < clock.TransitionAtUtc)
        {
            return beforeCandidate;
        }

        return DateTime.SpecifyKind(localHour.AddMinutes(-clock.OffsetAfterMinutes), DateTimeKind.Unspecified);
    }

    /// <summary>
    /// Adds exactly the seven tile keys the design's §1 "New keys" list names to a fired fact's metadata:
    /// <c>tile_local_hour</c>, <c>tile_day_of_week</c>, <c>tile_start_ticks</c>, <c>tiles_scored</c>,
    /// <c>tiles_fired</c>, <c>window_peak</c> and <c>window_samples_total</c>. Does NOT write the existing
    /// keys (<c>peak_*</c>, <c>baseline_*</c>, <c>deviation_sigma</c> and so on) — the detectors keep
    /// writing those, from the worst tile and its bucket.
    /// </summary>
    public static void AddTileMetadata(
        IDictionary<string, double> metadata,
        AnomalyGate.TileVerdict verdict,
        IReadOnlyList<WindowTile> tiles,
        LocalClockWindow clock)
    {
        var wholeWindow = WholeWindow(tiles);

        metadata["tile_local_hour"] = verdict.Tile.LocalHour.Hour;
        metadata["tile_day_of_week"] = (int)verdict.Tile.LocalHour.DayOfWeek;
        metadata["tile_start_ticks"] = LocalHourToUtc(verdict.Tile.LocalHour, clock).Ticks;
        metadata["tiles_scored"] = verdict.TilesScored;
        metadata["tiles_fired"] = verdict.TilesFired;
        metadata["window_peak"] = wholeWindow.Peak;
        metadata["window_samples_total"] = wholeWindow.Samples;
    }

    /// <summary>
    /// Builds one <see cref="WindowTile"/> from a data row. DBNull peak or mean becomes 0; DBNull samples
    /// becomes 0; a negative <paramref name="peakTimeOrdinal"/>, or a DBNull cell there, means no peak
    /// time. Uses <see cref="Convert.ToDouble(object)"/> and <see cref="Convert.ToInt64(object)"/> because
    /// Npgsql and DuckDB hand back different numeric CLR types for the same SQL numeric column — this
    /// works for both <c>NpgsqlDataReader</c> and <c>DuckDBDataReader</c>.
    /// </summary>
    public static WindowTile ReadTile(
        IDataRecord r,
        int localHourOrdinal,
        int peakOrdinal,
        int meanOrdinal,
        int samplesOrdinal,
        int peakTimeOrdinal = -1)
    {
        var localHour = r.GetDateTime(localHourOrdinal);

        var peak = r.IsDBNull(peakOrdinal) ? 0 : Convert.ToDouble(r.GetValue(peakOrdinal));
        var mean = r.IsDBNull(meanOrdinal) ? 0 : Convert.ToDouble(r.GetValue(meanOrdinal));
        var samples = r.IsDBNull(samplesOrdinal) ? 0L : Convert.ToInt64(r.GetValue(samplesOrdinal));

        DateTime? peakTimeUtc = null;
        if (peakTimeOrdinal >= 0 && !r.IsDBNull(peakTimeOrdinal))
        {
            peakTimeUtc = r.GetDateTime(peakTimeOrdinal);
        }

        return new WindowTile(localHour, peak, mean, samples, peakTimeUtc);
    }
}
