/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #8882 on a live store: the read behind the TempDB Growth Won't Fit alert returns tempdb's files from the newest
/// <c>database_size_stats</c> collection only, with the new <c>is_autogrow_all_files</c> column, and the growth arithmetic gives the
/// verdict the alert fires on. The same scenario table runs on Lite's DuckDB store in <c>TempDbGrowthFitReadTests</c>.
/// </summary>
public sealed class TempDbGrowthFitReadLiveTests
{
    private const int ServerId = -888201;
    private static readonly string ServerKey = ServerId.ToString(CultureInfo.InvariantCulture);

    private sealed record Row(
        int FileId, string Type, double Size, double? FixedStep, bool Percent, double? Pct, double Max,
        string? Volume, double? VolumeFree, bool? AllFiles, int DatabaseId = 2);

    private static Row Data(int id, double size, double? step, double volumeFree, bool? allFiles = null, bool percent = false, double? pct = null, double max = -1, string? volume = "T:\\")
        => new(id, "ROWS", size, step, percent, pct, max, volume, volume is null ? null : volumeFree, allFiles);

    private static Row Log(int id, double size, double step, double volumeFree)
        => new(id, "LOG", size, step, false, null, -1, "L:\\", volumeFree, null);

    private static async Task SeedAsync(NpgsqlConnection connection, long collectionId, DateTime collectionTime, IEnumerable<Row> rows, CancellationToken ct)
    {
        foreach (var r in rows)
        {
            using var command = new NpgsqlCommand(@"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name, physical_name,
     total_size_mb, used_size_mb, auto_growth_mb, max_size_mb, volume_mount_point, volume_total_mb, volume_free_mb,
     is_percent_growth, growth_pct, is_autogrow_all_files)
VALUES ($1, $2, $3, 'srv', $4, $5, $6, $7, $8, $9, $10, 1, $11, $12, $13, $14, $15, $16, $17, $18)", connection);
            command.Parameters.AddWithValue(collectionId);
            command.Parameters.AddWithValue(collectionTime);
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(r.DatabaseId == 2 ? "tempdb" : "userdb");
            command.Parameters.AddWithValue(r.DatabaseId);
            command.Parameters.AddWithValue(r.FileId);
            command.Parameters.AddWithValue(r.Type);
            command.Parameters.AddWithValue($"file{r.FileId}");
            command.Parameters.AddWithValue($"T:\\file{r.FileId}");
            command.Parameters.AddWithValue(r.Size);
            command.Parameters.AddWithValue(r.FixedStep.HasValue ? r.FixedStep.Value : DBNull.Value);
            command.Parameters.AddWithValue(r.Max);
            command.Parameters.AddWithValue(r.Volume is null ? DBNull.Value : r.Volume);
            command.Parameters.AddWithValue(r.Volume is null ? DBNull.Value : 100_000d);
            command.Parameters.AddWithValue(r.VolumeFree.HasValue ? r.VolumeFree.Value : DBNull.Value);
            command.Parameters.AddWithValue(r.Percent);
            command.Parameters.AddWithValue(r.Pct.HasValue ? r.Pct.Value : DBNull.Value);
            command.Parameters.AddWithValue(r.AllFiles.HasValue ? r.AllFiles.Value : DBNull.Value);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private async Task RunAsync(Func<NpgsqlConnection, DarlingAlertReadAdapter, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the tempdb growth-fit live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            await body(connection, new DarlingAlertReadAdapter(dataSource), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static DateTime Clock(int minutesAgo)
    {
        var raw = DateTime.UtcNow.AddMinutes(-minutesAgo);
        return DateTime.SpecifyKind(new DateTime(raw.Ticks - (raw.Ticks % 10)), DateTimeKind.Unspecified);
    }

    [Fact]
    public async Task TheRead_ReturnsTheNewestTempDbCollectionOnly_WithTheFlagAndTheVolume()
    {
        await RunAsync(async (connection, adapter, ct) =>
        {
            var older = Clock(120);
            var newer = Clock(60);
            await SeedAsync(connection, 1, older, new[] { Data(1, 1_000, 64, 90_000), Data(2, 1_000, 64, 90_000) }, ct);
            await SeedAsync(connection, 2, newer, new[]
            {
                Data(1, 2_000, 128, 80_000, allFiles: true), Data(2, 2_000, 128, 80_000, allFiles: true), Log(3, 512, 64, 70_000),
                /* Another database's file in the same collection is not tempdb's. */
                new Row(1, "ROWS", 9_000, 1, false, null, -1, "T:\\", 80_000, null, DatabaseId: 7),
            }, ct);

            var files = await adapter.GetTempDbFilesAsync(ServerKey, ct);

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
        });
    }

    [Fact]
    public async Task TheRead_IsEmptyWhenTheStoreHasNoTempDbRows_AndReadsNullForAnUnreadableVolumeAndAnUnknownFlag()
    {
        await RunAsync(async (connection, adapter, ct) =>
        {
            Assert.Empty(await adapter.GetTempDbFilesAsync(ServerKey, ct));

            await SeedAsync(connection, 1, Clock(30), new[] { Data(1, 4_096, 64, 0, volume: null) }, ct);
            var file = Assert.Single(await adapter.GetTempDbFilesAsync(ServerKey, ct));
            Assert.Null(file.VolumeMountPoint);
            Assert.Null(file.VolumeFreeMb);
            Assert.Null(file.IsAutogrowAllFiles);
        });
    }

    [Fact]
    public async Task TheGrowthArithmetic_OnTheNewestCollection_GivesTheVerdictEachRuleExpects()
    {
        await RunAsync(async (connection, adapter, ct) =>
        {
            long id = 0;
            var minutesAgo = 600;

            async Task<TempDbGrowthFitResult> JudgeAsync(params Row[] rows)
            {
                id++;
                minutesAgo -= 10;
                await SeedAsync(connection, id, Clock(minutesAgo), rows, ct);
                return TempDbGrowthFit.Evaluate(await adapter.GetTempDbFilesAsync(ServerKey, ct));
            }

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
        });
    }
}
