/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9862: a query tag sighting must outlive the oldest archived usage row that can still exist, or the last days of an
/// archive file read with no tag. Both ends are built from the real constants and the real archive cutoff, on a fake clock.
/// </summary>
public sealed class QueryTagSightingWindowTests : IClassFixture<SharedDuckDbFixture>
{
    /// <summary>
    /// The latest instant an archived row can still be read, given when it was collected. The row stays hot for
    /// <see cref="ArchiveService.HotDataDays"/>, so the first hourly archive run after that writes it, into a file
    /// named for that run's date. <see cref="RetentionService.CleanupOldArchives"/> then deletes the file at the first
    /// daily run after the real <see cref="RetentionService.ArchiveCutoff"/> passes the file's date. Both runs are taken
    /// at their worst phase: the archive run just missed, and the cleanup run just missed.
    /// </summary>
    private static DateTime LastInstantAnArchivedRowCanBeRead(DateTime collectedUtc)
    {
        var written = collectedUtc.AddDays(ArchiveService.HotDataDays) + CollectionBackgroundService.ArchiveInterval;
        var fileDate = written.Date;

        /* The cutoff is calendar months, so the crossing is found by stepping, not by adding a fixed day count. */
        var crossing = fileDate.AddDays(RetentionService.ArchiveRetentionMonths * 28);
        while (!(fileDate < RetentionService.ArchiveCutoff(crossing)))
        {
            crossing = crossing.AddHours(1);
        }

        return crossing + CollectionBackgroundService.RetentionInterval;
    }

    [Fact]
    public void ASighting_OutlivesTheOldestArchivedRowItTags_OnEveryDayOfTheYear()
    {
        var sightingDays = LocalDataService.QueryTagSightingRetentionDays;
        var worstShortfall = TimeSpan.Zero;
        DateTime? worstCollected = null;

        /* Two years of hourly collection times, so every month-length combination is crossed, the long July to September
           span included. A sighting is last seen when its row is collected, and goes at last_seen + the window. */
        for (var collected = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
             collected < new DateTime(2028, 1, 1, 0, 0, 0, DateTimeKind.Utc);
             collected = collected.AddHours(1))
        {
            var rowGoneAt = LastInstantAnArchivedRowCanBeRead(collected);
            var sightingGoneAt = collected.AddDays(sightingDays);
            var shortfall = rowGoneAt - sightingGoneAt;
            if (shortfall > worstShortfall)
            {
                worstShortfall = shortfall;
                worstCollected = collected;
            }
        }

        Assert.True(worstShortfall <= TimeSpan.Zero,
            $"The sighting window ({sightingDays} days) is shorter than the archive's longest life: a row collected {worstCollected:u} is still archived {worstShortfall.TotalHours:F0} hours after its sighting is gone.");
    }

    private static readonly DateTime Now = new(2026, 9, 1, 12, 0, 0, DateTimeKind.Utc);

    private readonly SharedDuckDbFixture _fixture;
    private readonly LocalDataService _service;

    public QueryTagSightingWindowTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
        _service = new LocalDataService(fixture.DuckDb);
        foreach (var file in Directory.Exists(fixture.DuckDb.ArchivePath) ? Directory.GetFiles(fixture.DuckDb.ArchivePath) : [])
        {
            File.Delete(file);
        }

        Directory.CreateDirectory(fixture.DuckDb.ArchivePath);
    }

    private static string Ts(DateTime t) => $"TIMESTAMP '{t:yyyy-MM-dd HH:mm:ss}'";

    private async Task ExecAsync(params string[] statements)
    {
        using var connection = _fixture.DuckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private async Task<long> SightingCountAsync(string key)
    {
        using var connection = _fixture.DuckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM query_tag_sightings WHERE key = '{key}'";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    private Task SeedStatsRowAsync(DateTime collected) => ExecAsync($@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     max_dop, min_dop, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_spills, query_text)
VALUES (1, {Ts(collected)}, 1, 'srv', 'SalesDb', '0xOLD', '0xPLAN', 1, 1, 1, 1, 1, 0, 'SELECT 1')");

    private Task SeedSightingAsync(string key, DateTime lastSeen) => ExecAsync($@"
INSERT INTO query_tag_sightings (server_id, key_kind, key, first_seen, last_seen)
VALUES (1, 'hash', '{key}', {Ts(lastSeen)}, {Ts(lastSeen)})");

    /// <summary>Moves every hot query_stats row into an archive file named for <paramref name="fileDate"/>, as the archive run does.</summary>
    private async Task<string> ArchiveStatsRowsAsync(DateTime fileDate)
    {
        var path = Path.Combine(_fixture.DuckDb.ArchivePath, $"{fileDate:yyyyMMdd}_0000_query_stats.parquet").Replace("\\", "/");
        await ExecAsync(
            $"COPY (SELECT * FROM query_stats) TO '{DuckDbInitializer.EscapeSqlPath(path)}' (FORMAT PARQUET)",
            "DELETE FROM query_stats");
        return path;
    }

    /// <summary>
    /// A row is collected, Lite is shut for 60 days, and on restart the row is archived into a file named for the restart day
    /// (the file's name is the run's clock, not the row's). The row now lives well past the window counted from its collection,
    /// so the purge keeps its sighting until the file is gone, and drops it as soon as no readable row is older than the window.
    /// </summary>
    [Fact]
    public async Task APurge_KeepsTheSightingOfARowCollectedBeforeAShutdown_UntilItsArchiveFileIsGone()
    {
        var collected = Now.AddDays(-(LocalDataService.QueryTagSightingRetentionDays + 8));
        var restart = collected.AddDays(60);
        await SeedStatsRowAsync(collected);
        var file = await ArchiveStatsRowsAsync(restart);
        await SeedSightingAsync("tagged", collected);
        await SeedSightingAsync("unrelated", collected.AddDays(-5));

        Assert.Equal(1, await _service.PurgeOldQueryTagSightingsAsync(utcNow: Now));
        Assert.Equal(1L, await SightingCountAsync("tagged"));
        Assert.Equal(0L, await SightingCountAsync("unrelated"));

        File.Delete(file);

        Assert.Equal(1, await _service.PurgeOldQueryTagSightingsAsync(utcNow: Now));
        Assert.Equal(0L, await SightingCountAsync("tagged"));
    }

    /// <summary>The same shutdown before any archive run: the row is still hot, and past the window.</summary>
    [Fact]
    public async Task APurge_KeepsTheSightingOfAHotRowOlderThanTheWindow()
    {
        var collected = Now.AddDays(-(LocalDataService.QueryTagSightingRetentionDays + 8));
        await SeedStatsRowAsync(collected);
        await SeedSightingAsync("tagged", collected);

        Assert.Equal(0, await _service.PurgeOldQueryTagSightingsAsync(utcNow: Now));
        Assert.Equal(1L, await SightingCountAsync("tagged"));

        await ExecAsync("DELETE FROM query_stats");

        Assert.Equal(1, await _service.PurgeOldQueryTagSightingsAsync(utcNow: Now));
    }

    /// <summary>An archive file that cannot be read means the oldest row is unknown: every sighting is kept, never purged on a guess.</summary>
    [Fact]
    public async Task APurge_KeepsEverySighting_WhenAnArchiveFileCannotBeRead()
    {
        var path = Path.Combine(_fixture.DuckDb.ArchivePath, $"{Now.AddDays(-30):yyyyMMdd}_0000_query_stats.parquet");
        File.WriteAllText(path, "not a parquet file");
        await SeedSightingAsync("ancient", Now.AddDays(-400));
        await SeedSightingAsync("old", Now.AddDays(-200));

        Assert.Equal(0, await _service.PurgeOldQueryTagSightingsAsync(utcNow: Now));
        Assert.Equal(1L, await SightingCountAsync("ancient"));
        Assert.Equal(1L, await SightingCountAsync("old"));

        File.Delete(path);

        Assert.Equal(2, await _service.PurgeOldQueryTagSightingsAsync(utcNow: Now));
    }
}
