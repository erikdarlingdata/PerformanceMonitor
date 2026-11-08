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
using PerformanceMonitor.Analysis.Baselines;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8293: <c>AnomalyGate.EvaluateTiles</c> scores a target-local hour tile only once its collection covers at
/// least half of its hour, measured from the samples' own timestamps against the collector's cadence, so the
/// verdict on a short burst does not depend on how soon after it the analysis pass runs. Every tile here is
/// built the way the detectors build theirs, through <see cref="WindowTiles.ReadTile"/> over a tiled-statement
/// row, so the cases observe the verdict and never the gate's internals. A trustworthy classical bucket (mean 0,
/// stddev 1) answers every hour, so the peak and mean values below are sigmas.
/// </summary>
public class AnomalyGateTileCoverageTests
{
    private const double ClassicalThreshold = AnomalyThresholds.DefaultDeviationThreshold; // 2.0
    private const double ModifiedZThreshold = AnomalyThresholds.ModifiedZThreshold;         // 3.5
    private const double MagnitudeFloor = 0.0;
    private const double AbsoluteFallbackBar = 1_000_000.0;
    private const double SigmaCap = AnomalyThresholds.SigmaDisplayCap;
    private static readonly TimeSpan FourHours = TimeSpan.FromHours(4);
    private static readonly TimeSpan OneHour = TimeSpan.FromHours(1);

    /// <summary>The tiles' day (a Wednesday); the window clock is UTC, so local hours and UTC hours agree.</summary>
    private static readonly DateTime Day = new(2026, 9, 23, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>One trustworthy null bucket (mean 0, stddev 1) for every (hour, dow) key, so each tile is judged
    /// on its own coverage and statistics alone.</summary>
    private static BaselineBucketMap Map()
    {
        var dict = new Dictionary<(int, int), BaselineBucket>();
        for (var h = 0; h < 24; h++)
        for (var d = 0; d < 7; d++)
            dict[(h, d)] = new BaselineBucket
            {
                HourOfDay = h, DayOfWeek = d, Tier = BaselineTier.Full,
                Mean = 0, StdDev = 1, SampleCount = 100, DistinctDays = 10, AbsStdDevFloor = 0,
            };
        return new BaselineBucketMap(dict, LocalClockWindow.Utc(Day.AddDays(1)));
    }

    /// <summary>
    /// A tile as the tiled statements return it: <c>local_hour, peak, mean, samples</c>, then the two coverage
    /// columns every tiled statement appends, <c>tile_collections</c> (distinct collection instants in the tile)
    /// and <c>tile_span_seconds</c> (first instant to last).
    /// </summary>
    private static WindowTile Tile(int hour, double peak, double mean, long samples, long collections, double spanMinutes)
        => WindowTiles.ReadTile(new TileRow(Day.AddHours(hour), peak, mean, samples, collections, spanMinutes * 60.0), 0, 1, 2, 3);

    /// <summary>A tile at a one-minute cadence holding <paramref name="minutes"/> consecutive minutes of samples,
    /// every sample at <paramref name="value"/>.</summary>
    private static WindowTile MinuteTile(int hour, int minutes, double value)
        => Tile(hour, peak: value, mean: value, samples: minutes, collections: minutes, spanMinutes: minutes - 1);

    /// <summary>The trailing tile of a pass that ends <paramref name="minutesIntoHour"/> minutes into an hour whose
    /// first five minutes were a burst at <paramref name="burst"/> sigmas, at a one-minute cadence, with every
    /// later minute at baseline.</summary>
    private static WindowTile BurstThenQuietTile(int hour, int minutesIntoHour, double burst)
    {
        var burstMinutes = Math.Min(minutesIntoHour, 5);
        return Tile(hour, peak: burst, mean: burst * burstMinutes / minutesIntoHour,
            samples: minutesIntoHour, collections: minutesIntoHour, spanMinutes: minutesIntoHour - 1);
    }

    private static AnomalyGate.TileVerdict? Evaluate(TimeSpan window, params WindowTile[] tiles)
        => AnomalyGate.EvaluateTiles(
            tiles, Map(), ClassicalThreshold, ModifiedZThreshold, MagnitudeFloor, AbsoluteFallbackBar, SigmaCap, window);

    [Fact]
    public void TrailingThreeMinuteTile_EntirelyBurst_DoesNotFire()
    {
        // A 4-hour pass ending three minutes into hour 13: three full quiet hours and a trailing tile that is all burst.
        var verdict = Evaluate(FourHours,
            MinuteTile(10, 60, 0.0), MinuteTile(11, 60, 0.0), MinuteTile(12, 60, 0.0),
            MinuteTile(13, 3, 10.0));

        Assert.NotNull(verdict);
        Assert.False(verdict!.Value.Decision.Fire, "a three-minute sliver covers a twentieth of its hour and is not a sustained rise");
        Assert.Equal(3, verdict.Value.TilesScored);
        Assert.NotEqual(13, verdict.Value.Tile.LocalHour.Hour);
    }

    [Fact]
    public void FiveMinuteBurstAtTheTopOfTheHour_SameVerdict_WhateverThePassPhase()
    {
        // The same five-minute burst, seen by passes ending 3, 7, 20 and 50 minutes into its hour.
        var verdicts = new List<bool>();
        foreach (var minutesIntoHour in new[] { 3, 7, 20, 50 })
        {
            var verdict = Evaluate(FourHours,
                MinuteTile(10, 60, 0.0), MinuteTile(11, 60, 0.0), MinuteTile(12, 60, 0.0),
                BurstThenQuietTile(13, minutesIntoHour, 10.0));
            Assert.NotNull(verdict);
            verdicts.Add(verdict!.Value.Decision.Fire);
        }

        Assert.All(verdicts, fired => Assert.False(fired, "a five-minute burst is not a sustained rise at any pass phase"));
    }

    [Fact]
    public void FullTile_SustainedRise_StillFires()
    {
        var verdict = Evaluate(FourHours,
            MinuteTile(10, 60, 0.0), MinuteTile(11, 60, 0.0), MinuteTile(12, 60, 5.0));

        Assert.NotNull(verdict);
        Assert.True(verdict!.Value.Decision.Fire);
        Assert.Equal(12, verdict.Value.Tile.LocalHour.Hour);
        Assert.Equal(3, verdict.Value.TilesScored);
    }

    [Fact]
    public void LeadingTenMinuteTile_OfAnHourWindowEndingAtFifty_IsSkippedLikeATrailingOne()
    {
        // A 1-hour window from 12:50 to 13:50: the first tile holds ten minutes of burst, the second fifty quiet minutes.
        var verdict = Evaluate(OneHour,
            MinuteTile(12, 10, 10.0), MinuteTile(13, 50, 0.0));

        Assert.NotNull(verdict);
        Assert.False(verdict!.Value.Decision.Fire);
        Assert.Equal(1, verdict.Value.TilesScored);
        Assert.Equal(13, verdict.Value.Tile.LocalHour.Hour);
    }

    [Fact]
    public void TileWithGaps_LeavingItUnderHalfCovered_IsNotScored()
    {
        // Hour 12 holds twenty one-minute samples scattered across the whole hour: forty minutes of gaps.
        var verdict = Evaluate(FourHours,
            MinuteTile(10, 60, 0.0), MinuteTile(11, 60, 0.0),
            Tile(12, peak: 10.0, mean: 10.0, samples: 20, collections: 20, spanMinutes: 59));

        Assert.NotNull(verdict);
        Assert.False(verdict!.Value.Decision.Fire);
        Assert.Equal(2, verdict.Value.TilesScored);
    }

    [Fact]
    public void FifteenMinuteCadence_TwoCollectionsAreHalfAnHour_OneIsNot()
    {
        // A full hour at a 15-minute cadence sets the cadence; the I/O families count several file rows per collection.
        var fullHour = Tile(10, peak: 0.0, mean: 0.0, samples: 12, collections: 4, spanMinutes: 45);

        var halfHour = Evaluate(FourHours, fullHour,
            Tile(11, peak: 10.0, mean: 10.0, samples: 6, collections: 2, spanMinutes: 15));
        Assert.NotNull(halfHour);
        Assert.True(halfHour!.Value.Decision.Fire, "two 15-minute collections cover half the hour and are scored");
        Assert.Equal(11, halfHour.Value.Tile.LocalHour.Hour);

        var quarterHour = Evaluate(FourHours, fullHour,
            Tile(11, peak: 10.0, mean: 10.0, samples: 3, collections: 1, spanMinutes: 0));
        Assert.NotNull(quarterHour);
        Assert.False(quarterHour!.Value.Decision.Fire, "one 15-minute collection covers a quarter of the hour and is not scored");
        Assert.Equal(1, quarterHour.Value.TilesScored);
    }

    [Fact]
    public void WindowWhoseOnlyTileIsASliver_DoesNotFireThroughTheWholeWindowPathEither()
    {
        // The collector was away for the whole window and returned three minutes before the pass, into a burst.
        var verdict = Evaluate(FourHours, MinuteTile(13, 3, 10.0));

        Assert.NotNull(verdict);
        Assert.False(verdict!.Value.Decision.Fire, "three minutes of a four-hour window cannot fire through the whole-window mean");
        Assert.Equal(0, verdict.Value.TilesScored);
        Assert.Equal(0, verdict.Value.TilesFired);
    }

    [Fact]
    public void CadenceTooSparseToMeasureCoverage_StillFallsBackToTheWholeWindowPath()
    {
        // Two collections an hour, under MinTileSamples in every tile: no tile can be scored, and the caller's
        // whole-window path still gets its turn (the never-blind rule).
        var verdict = Evaluate(FourHours,
            Tile(10, peak: 6.0, mean: 6.0, samples: 2, collections: 2, spanMinutes: 30),
            Tile(11, peak: 6.0, mean: 6.0, samples: 2, collections: 2, spanMinutes: 30),
            Tile(12, peak: 6.0, mean: 6.0, samples: 2, collections: 2, spanMinutes: 30),
            Tile(13, peak: 6.0, mean: 6.0, samples: 2, collections: 2, spanMinutes: 30));

        Assert.Null(verdict);
    }

    [Fact]
    public void SparseTileOfInstantsSecondsApart_DoesNotSetTheCadence_FullHoursStillScore()
    {
        // Three full hours at a one-minute cadence, a sustained rise in hour 12, and a tile of three instants five
        // seconds apart (a collector restart). The fullest tile sets the cadence, so the full hours are still scored.
        var verdict = Evaluate(FourHours,
            MinuteTile(10, 60, 0.0), MinuteTile(11, 60, 0.0), MinuteTile(12, 60, 5.0),
            Tile(13, peak: 0.0, mean: 0.0, samples: 3, collections: 3, spanMinutes: 10.0 / 60.0));

        Assert.NotNull(verdict);
        Assert.True(verdict!.Value.Decision.Fire, "a sustained rise in a full hour fires whatever a sparse tile's instants say about the cadence");
        Assert.Equal(12, verdict.Value.Tile.LocalHour.Hour);
        Assert.Equal(3, verdict.Value.TilesScored);
    }

    [Fact]
    public void HourMissingFortyOfItsSixtyCollections_IsNotScored_TheFullestTileSetsTheCadence()
    {
        // Hours 10 and 12 are full at a one-minute cadence; hour 11 holds twenty instants spread over the hour, a burst
        // among them. Its own mean gap is three minutes, which would call twenty instants a covered hour; the fullest
        // tile's one-minute cadence prices them at twenty minutes.
        var verdict = Evaluate(FourHours,
            MinuteTile(10, 60, 0.0),
            Tile(11, peak: 10.0, mean: 10.0, samples: 20, collections: 20, spanMinutes: 57),
            MinuteTile(12, 60, 0.0));

        Assert.NotNull(verdict);
        Assert.False(verdict!.Value.Decision.Fire);
        Assert.Equal(2, verdict.Value.TilesScored);
        Assert.NotEqual(11, verdict.Value.Tile.LocalHour.Hour);
    }

    [Fact]
    public void FullyCollectedHour_WhoseQueriesRanTwentyFiveMinutes_IsScoredOnItsCollectorInstants()
    {
        // Sixty collector instants, twenty-five of them carrying work: coverage is the collector's, the samples are the work's.
        var verdict = Evaluate(FourHours,
            MinuteTile(10, 60, 0.0), MinuteTile(11, 60, 0.0), MinuteTile(12, 60, 0.0),
            Tile(13, peak: 10.0, mean: 10.0, samples: 25, collections: 60, spanMinutes: 59));

        Assert.NotNull(verdict);
        Assert.True(verdict!.Value.Decision.Fire, "an idle hour the collector wrote every minute is a covered hour");
        Assert.Equal(13, verdict.Value.Tile.LocalHour.Hour);
        Assert.Equal(4, verdict.Value.TilesScored);
    }

    /// <summary>One row of a tiled window statement, with the column names the statements use.</summary>
    private sealed class TileRow : IDataRecord
    {
        private static readonly string[] Names = { "local_hour", "peak", "mean", "samples", "tile_collections", "tile_span_seconds" };
        private readonly object[] _values;

        public TileRow(DateTime localHour, double peak, double mean, long samples, long collections, double spanSeconds)
            => _values = new object[] { localHour, peak, mean, samples, collections, spanSeconds };

        public int FieldCount => _values.Length;
        public object this[int i] => _values[i];
        public object this[string name] => _values[GetOrdinal(name)];
        public string GetName(int i) => Names[i];
        public int GetOrdinal(string name) => Array.IndexOf(Names, name);
        public object GetValue(int i) => _values[i];
        public int GetValues(object[] values) { _values.CopyTo(values, 0); return _values.Length; }
        public bool IsDBNull(int i) => _values[i] is DBNull;
        public DateTime GetDateTime(int i) => (DateTime)_values[i];
        public double GetDouble(int i) => Convert.ToDouble(_values[i]);
        public long GetInt64(int i) => Convert.ToInt64(_values[i]);
        public int GetInt32(int i) => Convert.ToInt32(_values[i]);
        public string GetDataTypeName(int i) => _values[i].GetType().Name;
        public Type GetFieldType(int i) => _values[i].GetType();
        public bool GetBoolean(int i) => throw new NotSupportedException();
        public byte GetByte(int i) => throw new NotSupportedException();
        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
        public char GetChar(int i) => throw new NotSupportedException();
        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
        public IDataReader GetData(int i) => throw new NotSupportedException();
        public decimal GetDecimal(int i) => throw new NotSupportedException();
        public float GetFloat(int i) => throw new NotSupportedException();
        public Guid GetGuid(int i) => throw new NotSupportedException();
        public short GetInt16(int i) => throw new NotSupportedException();
        public string GetString(int i) => throw new NotSupportedException();
    }
}
