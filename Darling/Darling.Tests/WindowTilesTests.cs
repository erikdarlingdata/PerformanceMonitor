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
/// #3653 A8 option B (lane L1c): <c>WindowTiles</c>, the shared per-hour tile helpers every detector call
/// site uses — design-3653-A8-B.md §1.
/// </summary>
public class WindowTilesTests
{
    private static WindowTile Tile(int localHour, double peak, double mean, long samples, DateTime? peakTimeUtc = null)
        => new(new DateTime(2026, 9, 24, localHour, 0, 0, DateTimeKind.Unspecified), peak, mean, samples, peakTimeUtc);

    // ── LocalHourSql ────────────────────────────────────────────────────────────────────────

    [Fact]
    public void LocalHourSql_Contains_LocalCollectionTimeSql_Verbatim()
    {
        Assert.Contains(BaselineLocalClock.LocalCollectionTimeSql, WindowTiles.LocalHourSql);
    }

    // ── WholeWindow ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void WholeWindow_WeightedMean_And_MaxPeak_LaterTileWinsTie()
    {
        var earlyPeakTime = new DateTime(2026, 9, 24, 1, 30, 0, DateTimeKind.Utc);
        var latePeakTime = new DateTime(2026, 9, 24, 2, 30, 0, DateTimeKind.Utc);

        // Both tiles peak at 10.0 (a tie): the later tile (hour 2) must win, contributing its own peak time.
        var tiles = new List<WindowTile>
        {
            Tile(1, peak: 10.0, mean: 4.0, samples: 12, peakTimeUtc: earlyPeakTime),
            Tile(2, peak: 10.0, mean: 1.0, samples: 3, peakTimeUtc: latePeakTime),
        };

        var whole = WindowTiles.WholeWindow(tiles);

        Assert.Equal(10.0, whole.Peak);
        Assert.Equal(latePeakTime, whole.PeakTimeUtc);
        Assert.Equal(15L, whole.Samples);
        // Weighted mean: (4.0*12 + 1.0*3) / 15 = 51/15 = 3.4
        Assert.Equal(3.4, whole.Mean, 10);
        // LocalHour is the EARLIEST tile's, regardless of which tile had the peak.
        Assert.Equal(Tile(1, 0, 0, 0).LocalHour, whole.LocalHour);
    }

    [Fact]
    public void WholeWindow_EmptyList_ReturnsDefault_ZeroSamples()
    {
        var whole = WindowTiles.WholeWindow(Array.Empty<WindowTile>());
        Assert.Equal(0L, whole.Samples);
        Assert.Equal(default, whole);
    }

    [Fact]
    public void WholeWindow_AllTilesZeroSamples_ReturnsDefault_ZeroSamples()
    {
        var tiles = new List<WindowTile> { Tile(1, 10.0, 5.0, 0), Tile(2, 20.0, 6.0, 0) };
        var whole = WindowTiles.WholeWindow(tiles);
        Assert.Equal(0L, whole.Samples);
        Assert.Equal(default, whole);
    }

    // ── LocalHourToUtc ──────────────────────────────────────────────────────────────────────

    [Fact]
    public void LocalHourToUtc_FixedOffset_MapsLocalToUtc()
    {
        var end = new DateTime(2026, 9, 24, 0, 0, 0, DateTimeKind.Unspecified);
        var clock = LocalClockWindow.FixedOffset(end, -240);

        var local = new DateTime(2026, 9, 24, 10, 0, 0, DateTimeKind.Unspecified);
        var utc = WindowTiles.LocalHourToUtc(local, clock);

        Assert.Equal(DateTimeKind.Unspecified, utc.Kind);
        Assert.Equal(new DateTime(2026, 9, 24, 14, 0, 0, DateTimeKind.Unspecified), utc);
    }

    [Fact]
    public void LocalHourToUtc_SpringForward_MapsBothSidesOfTheTransition()
    {
        // before −300 (UTC−5), after −240 (UTC−4), transition 2026-03-08 07:00Z.
        var clock = new LocalClockWindow(
            new DateTime(2026, 3, 8, 7, 0, 0, DateTimeKind.Unspecified), -300, -240);

        var beforeLocal = new DateTime(2026, 3, 8, 1, 0, 0, DateTimeKind.Unspecified);
        var afterLocal = new DateTime(2026, 3, 8, 3, 0, 0, DateTimeKind.Unspecified);

        Assert.Equal(new DateTime(2026, 3, 8, 6, 0, 0, DateTimeKind.Unspecified), WindowTiles.LocalHourToUtc(beforeLocal, clock));
        Assert.Equal(new DateTime(2026, 3, 8, 7, 0, 0, DateTimeKind.Unspecified), WindowTiles.LocalHourToUtc(afterLocal, clock));
    }

    [Fact]
    public void LocalHourToUtc_FallBack_AmbiguousHourMapsToFirstOccurrence()
    {
        // before −240 (UTC−4), after −300 (UTC−5), transition 2026-11-01 06:00Z.
        var clock = new LocalClockWindow(
            new DateTime(2026, 11, 1, 6, 0, 0, DateTimeKind.Unspecified), -240, -300);

        var ambiguousLocal = new DateTime(2026, 11, 1, 1, 0, 0, DateTimeKind.Unspecified);

        // FIRST occurrence: local 01:00 − (−240) = 05:00Z, which is before the 06:00Z transition, so it's accepted.
        Assert.Equal(new DateTime(2026, 11, 1, 5, 0, 0, DateTimeKind.Unspecified), WindowTiles.LocalHourToUtc(ambiguousLocal, clock));
    }

    // ── AddTileMetadata ─────────────────────────────────────────────────────────────────────

    [Fact]
    public void AddTileMetadata_AddsExactlySevenKeys()
    {
        var bucket = new BaselineBucket
        {
            HourOfDay = 3,
            DayOfWeek = 2,
            Tier = BaselineTier.Full,
            Mean = 0.0,
            StdDev = 1.0,
            SampleCount = 100,
            DistinctDays = 10,
            AbsStdDevFloor = 0,
        };

        var tile = Tile(3, peak: 12.0, mean: 4.0, samples: 50);
        var decision = new AnomalyGate.ZDecision(true, 6.0, false, 0.0, 3.5);
        var verdict = new AnomalyGate.TileVerdict(decision, tile, bucket, TilesScored: 4, TilesFired: 1);

        var tiles = new List<WindowTile> { tile, Tile(4, peak: 8.0, mean: 3.0, samples: 25) };
        var clock = LocalClockWindow.FixedOffset(new DateTime(2026, 9, 24, 0, 0, 0, DateTimeKind.Unspecified), -240);

        var metadata = new Dictionary<string, double>();
        WindowTiles.AddTileMetadata(metadata, verdict, tiles, clock);

        Assert.Equal(7, metadata.Count);
        Assert.Equal(3.0, metadata["tile_local_hour"]);
        Assert.Equal((double)(int)tile.LocalHour.DayOfWeek, metadata["tile_day_of_week"]);
        Assert.Equal((double)WindowTiles.LocalHourToUtc(tile.LocalHour, clock).Ticks, metadata["tile_start_ticks"]);
        Assert.Equal(4.0, metadata["tiles_scored"]);
        Assert.Equal(1.0, metadata["tiles_fired"]);
        Assert.Equal(WindowTiles.WholeWindow(tiles).Peak, metadata["window_peak"]);
        Assert.Equal(WindowTiles.WholeWindow(tiles).Samples, metadata["window_samples_total"]);
    }

    // ── ReadTile ────────────────────────────────────────────────────────────────────────────

    private static DataTable BuildTable()
    {
        var table = new DataTable();
        table.Columns.Add("local_hour", typeof(DateTime));
        table.Columns.Add("peak", typeof(double));
        table.Columns.Add("mean", typeof(double));
        table.Columns.Add("samples", typeof(long));
        table.Columns.Add("peak_time", typeof(DateTime));
        return table;
    }

    [Fact]
    public void ReadTile_ReadsAllColumns_WhenPresent()
    {
        var table = BuildTable();
        var localHour = new DateTime(2026, 9, 24, 5, 0, 0, DateTimeKind.Unspecified);
        var peakTime = new DateTime(2026, 9, 24, 5, 30, 0, DateTimeKind.Utc);
        table.Rows.Add(localHour, 42.5, 10.25, 100L, peakTime);

        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());

        var tile = WindowTiles.ReadTile(reader, 0, 1, 2, 3, 4);

        Assert.Equal(localHour, tile.LocalHour);
        Assert.Equal(42.5, tile.Peak);
        Assert.Equal(10.25, tile.Mean);
        Assert.Equal(100L, tile.Samples);
        Assert.Equal(peakTime, tile.PeakTimeUtc);
    }

    [Fact]
    public void ReadTile_DBNullCells_BecomeZeroOrNoPeakTime()
    {
        var table = BuildTable();
        var localHour = new DateTime(2026, 9, 24, 6, 0, 0, DateTimeKind.Unspecified);
        table.Rows.Add(localHour, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);

        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());

        var tile = WindowTiles.ReadTile(reader, 0, 1, 2, 3, 4);

        Assert.Equal(localHour, tile.LocalHour);
        Assert.Equal(0.0, tile.Peak);
        Assert.Equal(0.0, tile.Mean);
        Assert.Equal(0L, tile.Samples);
        Assert.Null(tile.PeakTimeUtc);
    }

    [Fact]
    public void ReadTile_NegativePeakTimeOrdinal_MeansNoPeakTime()
    {
        var table = BuildTable();
        var localHour = new DateTime(2026, 9, 24, 7, 0, 0, DateTimeKind.Unspecified);
        var peakTime = new DateTime(2026, 9, 24, 7, 30, 0, DateTimeKind.Utc);
        table.Rows.Add(localHour, 5.0, 2.0, 10L, peakTime);

        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());

        var tile = WindowTiles.ReadTile(reader, 0, 1, 2, 3);

        Assert.Null(tile.PeakTimeUtc);
    }
}
