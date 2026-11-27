/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8882 on Lite's DuckDB store: the read behind the TempDB Growth Won't Fit alert returns tempdb's files from the newest
/// <c>database_size_stats</c> collection only, with the new <c>is_autogrow_all_files</c> column (v84), and the growth arithmetic gives
/// the verdict the alert fires on. The same scenario table runs on the Darling store in <c>TempDbGrowthFitReadLiveTests</c>.
/// </summary>
public sealed class TempDbGrowthFitReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 8882;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;
    private int _minutesAgo = 600;

    public TempDbGrowthFitReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private sealed record Row(
        int FileId, string Type, double Size, double? FixedStep, bool Percent, double? Pct, double Max,
        string? Volume, double? VolumeFree, bool? AllFiles, int DatabaseId = 2);

    private static Row Data(int id, double size, double? step, double volumeFree, bool? allFiles = null, bool percent = false, double? pct = null, double max = -1, string? volume = "T:\\")
        => new(id, "ROWS", size, step, percent, pct, max, volume, volume is null ? null : volumeFree, allFiles);

    private static Row Log(int id, double size, double step, double volumeFree)
        => new(id, "LOG", size, step, false, null, -1, "L:\\", volumeFree, null);

    /* Whole minutes: DuckDB TIMESTAMP is microsecond-resolution. Kind Unspecified, the store's naive-UTC convention. */
    private static DateTime Clock(int minutesAgo)
    {
        var t = DateTime.UtcNow.AddMinutes(-minutesAgo);
        return DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);
    }

    private async Task SeedAsync(DateTime collectionTime, params Row[] rows)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        foreach (var r in rows)
        {
            using var cmd = _seedConn.CreateCommand();
            cmd.CommandText = @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name, physical_name,
     total_size_mb, used_size_mb, auto_growth_mb, max_size_mb, volume_mount_point, volume_total_mb, volume_free_mb,
     is_percent_growth, growth_pct, is_autogrow_all_files)
VALUES ($1, $2, $3, 'srv', $4, $5, $6, $7, $8, $9, $10, 1, $11, $12, $13, $14, $15, $16, $17, $18)";
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
            cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = r.DatabaseId == 2 ? "tempdb" : "userdb" });
            cmd.Parameters.Add(new DuckDBParameter { Value = r.DatabaseId });
            cmd.Parameters.Add(new DuckDBParameter { Value = r.FileId });
            cmd.Parameters.Add(new DuckDBParameter { Value = r.Type });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"file{r.FileId}" });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"T:\\file{r.FileId}" });
            cmd.Parameters.Add(new DuckDBParameter { Value = r.Size });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)r.FixedStep ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = r.Max });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)r.Volume ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = r.Volume is null ? DBNull.Value : 100_000d });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)r.VolumeFree ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = r.Percent });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)r.Pct ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)r.AllFiles ?? DBNull.Value });
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task<TempDbGrowthFitResult> JudgeAsync(params Row[] rows)
    {
        _minutesAgo -= 10;
        await SeedAsync(Clock(_minutesAgo), rows);
        var files = await new LocalDataService(_duckDb).GetTempDbFilesAsync(ServerId);
        return TempDbGrowthFit.Evaluate(files);
    }

    [Fact]
    public async Task TheRead_ReturnsTheNewestTempDbCollectionOnly_WithTheFlagAndTheVolume()
    {
        var older = Clock(120);
        var newer = Clock(60);
        await SeedAsync(older, Data(1, 1_000, 64, 90_000), Data(2, 1_000, 64, 90_000));
        await SeedAsync(newer,
            Data(1, 2_000, 128, 80_000, allFiles: true), Data(2, 2_000, 128, 80_000, allFiles: true), Log(3, 512, 64, 70_000),
            /* Another database's file in the same collection is not tempdb's. */
            new Row(1, "ROWS", 9_000, 1, false, null, -1, "T:\\", 80_000, null, DatabaseId: 7));

        var files = await new LocalDataService(_duckDb).GetTempDbFilesAsync(ServerId);

        Assert.Equal(3, files.Count);
        Assert.Equal(new[] { 1, 2, 3 }, files.Select(f => f.FileId).ToArray());
        Assert.All(files, f => Assert.Equal((DateTime?)newer, f.CollectionTimeUtc));
        Assert.All(files, f => Assert.Equal(DateTimeKind.Utc, f.CollectionTimeUtc!.Value.Kind));
        Assert.Equal(2_000d, files[0].TotalSizeMb);
        Assert.Equal(128d, files[0].AutoGrowthMb);
        Assert.Equal(true, files[0].IsAutogrowAllFiles);
        Assert.Equal("T:\\", files[0].VolumeMountPoint);
        Assert.Equal(80_000d, files[0].VolumeFreeMb);
        Assert.True(files[2].IsLog);
        Assert.Null(files[2].IsAutogrowAllFiles);
        Assert.Equal("L:\\", files[2].VolumeMountPoint);
    }

    [Fact]
    public async Task TheRead_IsEmptyWhenTheStoreHasNoTempDbRows_AndReadsNullForAnUnreadableVolumeAndAnUnknownFlag()
    {
        var service = new LocalDataService(_duckDb);
        Assert.Empty(await service.GetTempDbFilesAsync(ServerId));

        await SeedAsync(Clock(30), Data(1, 4_096, 64, 0, volume: null));
        var file = Assert.Single(await service.GetTempDbFilesAsync(ServerId));
        Assert.Null(file.VolumeMountPoint);
        Assert.Null(file.VolumeFreeMb);
        Assert.Null(file.IsAutogrowAllFiles);
    }

    [Fact]
    public async Task TheGrowthArithmetic_OnTheNewestCollection_GivesTheVerdictEachRuleExpects()
    {
        /* Fits: no fire. */
        Assert.Empty((await JudgeAsync(Data(1, 20_000, 64, 10_000))).Shortfalls);

        /* A fixed step bigger than the free space fires, and says what it needs. */
        var fixedStep = Assert.Single((await JudgeAsync(Data(1, 20_000, 4_096, 1_000))).Shortfalls);
        Assert.Equal(4_096d, fixedStep.NeededMb);
        Assert.Equal(1_000d, fixedStep.FreeMb);
        Assert.False(fixedStep.IsLog);

        /* A percent step is computed from the size: 10 percent of 20,000 is 2,000. */
        var percent = Assert.Single((await JudgeAsync(Data(1, 20_000, null, 1_500, percent: true, pct: 10))).Shortfalls);
        Assert.Equal(2_000d, percent.NeededMb);

        /* The ceiling cuts the step to the room under it: 500 MB left, so 4,096 MB is 500 MB and it fits in 1,000 MB. */
        Assert.Empty((await JudgeAsync(Data(1, 9_000, 4_096, 1_000, max: 9_500))).Shortfalls);
        var capped = Assert.Single((await JudgeAsync(Data(1, 9_000, 4_096, 400, max: 9_500))).Shortfalls);
        Assert.Equal(500d, capped.NeededMb);

        /* Autogrow-all-files sums the steps where the largest alone would fit. */
        var together = Assert.Single((await JudgeAsync(
            Data(1, 20_000, 512, 1_500, allFiles: true), Data(2, 20_000, 512, 1_500, allFiles: true),
            Data(3, 20_000, 512, 1_500, allFiles: true), Data(4, 20_000, 512, 1_500, allFiles: true))).Shortfalls);
        Assert.Equal(2_048d, together.NeededMb);
        Assert.Equal(4, together.FileNames.Count);
        Assert.Empty((await JudgeAsync(
            Data(1, 20_000, 512, 1_500, allFiles: false), Data(2, 20_000, 512, 1_500, allFiles: false),
            Data(3, 20_000, 512, 1_500, allFiles: false), Data(4, 20_000, 512, 1_500, allFiles: false))).Shortfalls);

        /* A log file that will not fit fires. */
        var log = Assert.Single((await JudgeAsync(Data(1, 20_000, 64, 50_000), Log(2, 8_192, 2_048, 1_000))).Shortfalls);
        Assert.True(log.IsLog);
        Assert.Equal(2_048d, log.NeededMb);

        /* Growth 0 does not fire and shows in the count. */
        var cannotGrow = await JudgeAsync(Data(1, 20_000, 0, 1_000), Data(2, 20_000, 0, 1_000));
        Assert.Empty(cannotGrow.Shortfalls);
        Assert.Equal(2, cannotGrow.FilesUnableToGrow.Count);
        Assert.True(cannotGrow.NoDataFileCanGrow);

        /* No readable volume: nothing to judge. */
        var unreadable = await JudgeAsync(Data(1, 20_000, 4_096, 0, volume: null));
        Assert.Empty(unreadable.Shortfalls);
        Assert.Equal(0, unreadable.JudgedFileCount);
    }
}
