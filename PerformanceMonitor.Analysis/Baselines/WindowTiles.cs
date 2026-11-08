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

    /// <summary>#8293: the column every tiled statement returns the tile's distinct collection instants under.</summary>
    public const string CollectionsColumn = "tile_collections";

    /// <summary>#8293: the column every tiled statement returns the seconds from the tile's first collection instant
    /// to its last under.</summary>
    public const string CollectionSpanSecondsColumn = "tile_span_seconds";

    /// <summary>
    /// #8293: the two coverage columns every tiled statement appends after its own columns — the tile's distinct
    /// collection instants and the seconds from its first instant to its last, both measured from the rows' own
    /// <c>collection_time</c> — so <see cref="ReadTile"/> fills <see cref="WindowTile.Collections"/> and
    /// <see cref="WindowTile.CollectionSpan"/> for every family without moving any family's ordinal contract.
    /// Counts instants, not rows, because the I/O families carry several file rows per collection. Runs on
    /// PostgreSQL and DuckDB alike. Every tiled statement's FROM exposes <c>collection_time</c>, since the tile
    /// key <see cref="LocalHourSql"/> is built over it, and sees every collection the collector wrote: a statement
    /// that filters rows on activity keeps that filter on its value aggregates, or joins the collections it would
    /// drop back in, so an idle hour the collector wrote every minute is still a covered hour and only the active
    /// rows are samples.
    /// </summary>
    public const string CoverageColumnsSql =
        "COUNT(DISTINCT collection_time) AS " + CollectionsColumn + @",
       extract(epoch FROM (date_trunc('second', MAX(collection_time)) - date_trunc('second', MIN(collection_time)))) AS " + CollectionSpanSecondsColumn;

    /// <summary>
    /// The whole-window aggregate the never-blind fallback feeds to today's <c>EvaluateZScore</c> when no
    /// tile can be scored (design §1's "Never-blind rule", <c>AnomalyGate.EvaluateTiles</c> null): peak is the
    /// max of the tiles' peaks, mean is the samples-weighted mean, samples is the sum, <see cref="WindowTile.PeakTimeUtc"/>
    /// is the max-peak tile's (a tie goes to the LATER tile), and <see cref="WindowTile.LocalHour"/> is the
    /// earliest tile's.
    ///
    /// <para>An empty list, or a list whose samples sum to zero, returns <c>default</c> with
    /// <c>Samples == 0</c> — the caller treats that exactly as today's early "no data" return.</para>
    ///
    /// <para>The aggregate carries no coverage (<see cref="WindowTile.Collections"/> 0): it is judged by the
    /// whole-window gate, which the tile gate's coverage rule decides whether to reach.</para>
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
    /// <para>#8293: the two coverage columns (<see cref="CollectionsColumn"/>, <see cref="CollectionSpanSecondsColumn"/>)
    /// are read by NAME, because <see cref="CoverageColumnsSql"/> appends them after each family's own columns and
    /// no family's ordinal contract moves. A row without them, or with DBNull there, leaves
    /// <see cref="WindowTile.Collections"/> at 0, and the gate does not score such a tile.</para>
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

        long collections = 0;
        var collectionSpan = TimeSpan.Zero;
        for (var i = 0; i < r.FieldCount; i++)
        {
            var name = r.GetName(i);
            if (name == CollectionsColumn && !r.IsDBNull(i))
            {
                collections = Convert.ToInt64(r.GetValue(i));
            }
            else if (name == CollectionSpanSecondsColumn && !r.IsDBNull(i))
            {
                collectionSpan = TimeSpan.FromSeconds(Convert.ToDouble(r.GetValue(i)));
            }
        }

        return new WindowTile(localHour, peak, mean, samples, peakTimeUtc, collections, collectionSpan);
    }

    /// <summary>
    /// #8293: the collector's cadence as the window's own tiles measure it — the mean gap between consecutive
    /// collection instants in the FULLEST tile, the one holding the most distinct instants (at least
    /// <paramref name="minCollections"/>, never fewer than two), ties to the shorter gap. The fullest, for two
    /// reasons: a gap inside a tile can only stretch that tile's mean gap, and the fullest tile has the fewest gaps;
    /// and a sparse tile's instants can sit seconds apart (a collector restart, a run-now) without saying anything
    /// about the cadence — read from such a tile, a few seconds would price every full hour as uncovered and leave
    /// the window blind. <c>null</c> when no tile holds enough instants to measure one, which leaves every tile's
    /// coverage unknown.
    /// </summary>
    public static TimeSpan? CollectionCadence(IReadOnlyList<WindowTile> tiles, int minCollections)
    {
        TimeSpan? cadence = null;
        long fullest = 0;
        var floor = Math.Max(2, minCollections);
        foreach (var tile in tiles)
        {
            if (tile.Collections < floor || tile.CollectionSpan <= TimeSpan.Zero)
            {
                continue;
            }

            var gap = TimeSpan.FromTicks(tile.CollectionSpan.Ticks / (tile.Collections - 1));
            if (cadence is null || tile.Collections > fullest || (tile.Collections == fullest && gap < cadence.Value))
            {
                cadence = gap;
                fullest = tile.Collections;
            }
        }

        return cadence;
    }

    /// <summary>#8293: how much of its hour a tile's collection covers — its collection instants, each worth one
    /// <paramref name="cadence"/>. A gap leaves instants out, and so leaves that time uncovered.</summary>
    public static TimeSpan CoveredTime(WindowTile tile, TimeSpan cadence)
        => TimeSpan.FromTicks(tile.Collections * cadence.Ticks);

    /// <summary>#8293: how much of the window the tiles' collection covers in all — the per-tile figures summed.</summary>
    public static TimeSpan CoveredTime(IReadOnlyList<WindowTile> tiles, TimeSpan cadence)
    {
        long ticks = 0;
        foreach (var tile in tiles)
        {
            ticks += tile.Collections * cadence.Ticks;
        }

        return TimeSpan.FromTicks(ticks);
    }
}
