/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9965: the query heat map keeps the newest cells, so it reads the newest bins first and reads the whole window only when those answer too few
/// cells. The whole-window read (<c>boundTheRead: false</c>) is the oracle: both reads must give the same cells. <c>readStartedFrom</c> reports the
/// lower bound of each statement the read ran, which is what the read touches: a slice starts at its bin boundary, a whole-window read at the window start.
/// </summary>
[Collection("server-time-helper")]
public sealed class QueryHeatmapNewestBinsTests : IDisposable
{
    private const int ServerId = 9965;
    private const string ServerName = "NewestBinsServer";
    private const int Bin = 5;
    private static readonly DateTime End = new(2026, 6, 1, 12, 0, 0, DateTimeKind.Unspecified);

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly PendingSeedSession _seed;
    private long _nextId = 1;

    public QueryHeatmapNewestBinsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "HeatmapNewestBins_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _seed = new PendingSeedSession(_duckDb);
    }

    private LocalDataService Service()
    {
        _seed.Flush();
        return new LocalDataService(_duckDb);
    }

    public void Dispose()
    {
        _seed.Dispose();
        _duckDb.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    /// <summary>
    /// <paramref name="hashes"/> queries in every 5-minute step of [from, to), one minute into the step. Query k runs 1 + k times (the most-executed query
    /// of a cell is the highest k in it, with no ties) at 2 * 10^(k mod 4) ms an execution, so a step holds four cells.
    /// </summary>
    private async Task SeedAsync(DateTime from, DateTime to, int hashes, string database)
    {
        await _duckDb.InitializeFromTemplateAsync();
        var connection = await _seed.ConnectionAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle, last_execution_time,
     delta_execution_count, delta_worker_time, delta_elapsed_time, query_text)
SELECT $1 + row_number() OVER (), g.t + INTERVAL 1 MINUTE, $2, $3, $7,
       '0xHM' || lpad(CAST(k AS VARCHAR), 3, '0'), '0xSH' || lpad(CAST(k AS VARCHAR), 3, '0'), g.t - INTERVAL 30 DAY,
       1 + k,
       (1 + k) * CAST(2 * pow(10, k % 4) * 1000 AS BIGINT),
       (1 + k) * CAST(2 * pow(10, k % 4) * 1000 AS BIGINT),
       'SELECT ' || CAST(k AS VARCHAR)
FROM generate_series($4::TIMESTAMP, $5::TIMESTAMP - INTERVAL 5 MINUTE, INTERVAL 5 MINUTE) AS g(t)
CROSS JOIN generate_series(0, $6::BIGINT - 1) AS h(k)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = from });
        cmd.Parameters.Add(new DuckDBParameter { Value = to });
        cmd.Parameters.Add(new DuckDBParameter { Value = (long)hashes });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        _nextId += 1_000_000;
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task DeleteAfterAsync(DateTime after)
    {
        var connection = await _seed.ConnectionAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "DELETE FROM query_stats WHERE server_id = $1 AND collection_time > $2";
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = after });
        await cmd.ExecuteNonQueryAsync();
    }

    private sealed record Answer(List<(DateTime Bin, int Bucket, long Count, string Hash, string Text, bool Cut)> Cells, List<DateTime> ReadStarts);

    private async Task<Answer> ReadAsync(int hoursBack, int cap, bool bound, IReadOnlyList<string>? databases = null)
    {
        var starts = new List<DateTime>();
        var rows = await Service().GetQueryHeatmapCellsAsync(
            ServerId, HeatmapMetric.Duration, hoursBack, Bin, cap, databases, asOfUtc: End, previewLength: 80, boundTheRead: bound, readStartedFrom: starts.Add);
        return new Answer(rows.Select(r => (r.TimeBucket, r.BucketIndex, r.QueryCount, r.TopQueryHash, r.TopQueryText, r.TopQueryTextTruncated)).ToList(), starts);
    }

    /// <summary>Every bin filled: the newest 41 bins answer the cap, so one statement runs, starting at the bin boundary 40 bins back, and the cells are the whole-window cells.</summary>
    [Fact]
    public async Task EveryBinFilled_ReadsOnlyTheNewestBins_AndGivesTheWholeWindowCells()
    {
        await SeedAsync(End.AddHours(-167), End, hashes: 8, "Alpha");

        var bounded = await ReadAsync(168, cap: 41, bound: true);
        var whole = await ReadAsync(168, cap: 41, bound: false);

        Assert.Equal(41, bounded.Cells.Count);
        Assert.Equal(whole.Cells, bounded.Cells);
        Assert.Equal(new[] { End.AddMinutes(-200).AddTicks(-10) }, bounded.ReadStarts);
        Assert.Equal(new[] { End.AddHours(-168) }, whole.ReadStarts);
    }

    /// <summary>Empty bins fill the newest of the slice: it answers fewer cells than the cap, the read runs again over the whole window, and the cells are the whole-window cells.</summary>
    [Fact]
    public async Task EmptyBinsInTheSlice_FallBackToTheWholeWindow_WithTheSameCells()
    {
        await SeedAsync(End.AddHours(-167), End, hashes: 8, "Alpha");
        await DeleteAfterAsync(End.AddMinutes(-190));

        var bounded = await ReadAsync(168, cap: 41, bound: true);
        var whole = await ReadAsync(168, cap: 41, bound: false);

        Assert.Equal(41, bounded.Cells.Count);
        Assert.Equal(whole.Cells, bounded.Cells);
        Assert.Equal(new[] { End.AddMinutes(-200).AddTicks(-10), End.AddHours(-168) }, bounded.ReadStarts);
    }

    /// <summary>A database filter that matches only old bins leaves the slice empty: the read falls back and the filtered cells are the whole-window cells.</summary>
    [Fact]
    public async Task ADatabaseFilter_MatchingOnlyOldBins_FallsBack_WithTheSameCells()
    {
        await SeedAsync(End.AddHours(-167), End, hashes: 8, "Alpha");
        await SeedAsync(End.AddHours(-30), End.AddHours(-20), hashes: 8, "Beta");

        var bounded = await ReadAsync(168, cap: 41, bound: true, new[] { "Beta" });
        var whole = await ReadAsync(168, cap: 41, bound: false, new[] { "Beta" });

        Assert.Equal(41, bounded.Cells.Count);
        Assert.Equal(whole.Cells, bounded.Cells);
        Assert.Equal(2, bounded.ReadStarts.Count);
    }

    /// <summary>Five bins of four cells (20 cells): a cap of 20, 21 and 22 rows read the same cells bounded and whole, and a cap of 20 is the 20 cells there are.</summary>
    [Fact]
    public async Task TheCapEdge_ReadsTheSameCells_AtExactlyTheCellCountAndOneEitherSide()
    {
        await SeedAsync(End.AddMinutes(-25), End, hashes: 4, "Alpha");

        foreach (var cap in new[] { 19, 20, 21, 22 })
        {
            var bounded = await ReadAsync(168, cap, bound: true);
            var whole = await ReadAsync(168, cap, bound: false);
            Assert.Equal(whole.Cells, bounded.Cells);
            Assert.Equal(Math.Min(cap, 20), bounded.Cells.Count);
        }
    }

    /// <summary>A window that starts inside the newest bins is read whole, in one statement.</summary>
    [Fact]
    public async Task AWindowStartingInsideTheSlice_IsReadWhole_InOneStatement()
    {
        await SeedAsync(End.AddHours(-3), End, hashes: 8, "Alpha");

        var bounded = await ReadAsync(2, cap: 500, bound: true);
        var whole = await ReadAsync(2, cap: 500, bound: false);

        Assert.Equal(whole.Cells, bounded.Cells);
        Assert.Equal(new[] { End.AddHours(-2) }, bounded.ReadStarts);
    }

    /// <summary>The slice boundary sits on the bin edge of the epoch origin, for bin widths that do not divide the hour.</summary>
    [Theory]
    [InlineData(5)]
    [InlineData(7)]
    [InlineData(70)]
    [InlineData(1440)]
    public void TheSliceFloor_IsABinEdgeOnTheEpochOrigin(int bin)
    {
        var end = new DateTime(2026, 11, 27, 13, 17, 41, DateTimeKind.Unspecified);
        var start = end.AddDays(-400);
        var floor = LocalDataService.NewestHeatmapBinsFloor(start, end, bin, 30);
        Assert.NotNull(floor);
        var sinceEpoch = (floor!.Value - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified)).Ticks;
        Assert.Equal(0, sinceEpoch % TimeSpan.FromMinutes(bin).Ticks);
        Assert.True(floor.Value <= end);
        Assert.Equal(29, (long)Math.Floor((end - floor.Value).TotalMinutes / bin));

        Assert.Null(LocalDataService.NewestHeatmapBinsFloor(end.AddMinutes(-bin * 3), end, bin, 30));
        Assert.Null(LocalDataService.NewestHeatmapBinsFloor(start, end, bin, 0));
    }
}
