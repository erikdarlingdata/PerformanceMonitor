/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9596: the size reset archives every collector table to Parquet and recreates the store, and the object usage chain is derived data that is not
/// carried across it. This runs the real <see cref="ArchiveService.ArchiveAllAndResetAsync"/> on a store holding a built chain and checks the three
/// things the design says: the chain is empty after the reset, <c>get_object_usage</c>'s read still answers from the archive, and the next pass rebuilds
/// the chain from the archived snapshots plus the first new one and gives the same answer as the fold.
/// </summary>
/* ArchiveAllAndResetAsync touches CollectionResetGate and ArchiveService's static archive lock, both process-wide, so this class joins the serialized
   collection the other reset tests use. */
[Collection("CollectionResetGate")]
public sealed class ObjectUsageChainResetTests : IDisposable
{
    private const int ServerId = 4242;
    private const string Database = "Sales";
    private static readonly DateTime Now = ObjectUsageChainHistory.Now;

    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;

    public ObjectUsageChainResetTests()
    {
        CollectionResetGate.ResetForTests();
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        CollectionResetGate.ResetForTests();
        try
        {
            if (Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, recursive: true);
            }
        }
        catch (IOException)
        {
            /* Best-effort cleanup */
        }
    }

    private static DateTime Start(int daysBack) => DateTime.SpecifyKind(Now.AddDays(-daysBack), DateTimeKind.Unspecified);

    private static string Json(ObjectUsageWindowResult result) => JsonSerializer.Serialize(result);

    private async Task<long> CountAsync(string table)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT count(*) FROM {table}";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken), System.Globalization.CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task AfterTheSizeReset_TheChainIsEmpty_TheFoldAnswersFromTheArchive_AndTheNextPassRebuildsTheChainFromIt()
    {
        await _duckDb.InitializeFromTemplateAsync();
        var rows = ObjectUsageChainHistory.Generate();
        var beforeNewest = rows.Where(r => r.CollectionTime < Now).ToList();
        var newest = rows.Where(r => r.CollectionTime >= Now).ToList();
        Assert.NotEmpty(beforeNewest);
        Assert.NotEmpty(newest);

        await ObjectUsageChainHistory.SeedAsync(_duckDb, ServerId, "ResetSrv", Database, beforeNewest);
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now);
        var snapshotsBefore = ObjectUsageChainHistory.SnapshotDays().Count() - 1;
        Assert.Equal(snapshotsBefore, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(snapshotsBefore, await CountAsync("object_usage_snapshots"));
        var service = new LocalDataService(_duckDb);
        var foldBefore = Json(await service.GetObjectUsageWindowFullAsync(ServerId, Database, Start(90)));

        Assert.True(await new ArchiveService(_duckDb, _archiveDir, NullLogger<ArchiveService>.Instance).ArchiveAllAndResetAsync());

        /* The reset left the raw rows in the archive only, and the derived chain behind. */
        Assert.Equal(0L, await CountAsync("index_object_stats"));
        Assert.Equal(beforeNewest.Count, await CountAsync("v_index_object_stats"));
        Assert.Equal(0L, await CountAsync("object_usage_snapshots"));
        Assert.Equal(0L, await CountAsync("object_usage_deltas"));
        Assert.Equal(0L, await CountAsync("object_usage_runs"));
        Assert.Equal(foldBefore, Json(await service.GetObjectUsageWindowAsync(ServerId, Database, Start(90))));

        /* A database with no hot snapshot is not listed, so a pass has nothing to do until the next collection writes one. */
        Assert.Equal(0, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));

        await ObjectUsageChainHistory.SeedAsync(_duckDb, ServerId, "ResetSrv", Database, newest, firstId: -1_000_000);
        Assert.Equal(snapshotsBefore + 1, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        foreach (var daysBack in new[] { 7, 90 })
        {
            var full = await service.GetObjectUsageWindowFullAsync(ServerId, Database, Start(daysBack));
            var (persisted, reason) = await service.TryReadObjectUsageChainAsync(ServerId, Database, Start(daysBack));
            Assert.Null(reason);
            Assert.NotNull(persisted);
            Assert.Equal(Json(full), Json(persisted));
        }
    }

    [Fact]
    public async Task AResetBetweenTwoStepsOfOnePass_StopsThePair_AndTheNextPassRebuildsTheChainWhole()
    {
        await _duckDb.InitializeFromTemplateAsync();
        var rows = ObjectUsageChainHistory.Generate();
        var beforeNewest = rows.Where(r => r.CollectionTime < Now).ToList();
        var newest = rows.Where(r => r.CollectionTime >= Now).ToList();
        await ObjectUsageChainHistory.SeedAsync(_duckDb, ServerId, "ResetSrv", Database, beforeNewest);
        var snapshots = ObjectUsageChainHistory.SnapshotDays().Count() - 1;

        /* The size reset runs between the 40th and the 41st step of one pass, the way the housekeeping reset can during a backfill of minutes. */
        const int ResetAfterStep = 40;
        var completed = 0;
        var resets = 0;
        var archive = new ArchiveService(_duckDb, _archiveDir, NullLogger<ArchiveService>.Instance);
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now)
        {
            StepCompletedForTests = async _ =>
            {
                if (++completed == ResetAfterStep)
                {
                    Assert.True(await archive.ArchiveAllAndResetAsync());
                    resets++;
                }
            },
        };
        Assert.Equal(ResetAfterStep, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(1, resets);

        /* The 41st step found the chain cleared and wrote nothing, so no step of the pair sits in the new store without the runs it needs. */
        Assert.Equal(0L, await CountAsync("object_usage_snapshots"));
        Assert.Equal(0L, await CountAsync("object_usage_deltas"));
        Assert.Equal(0L, await CountAsync("object_usage_runs"));

        builder.StepCompletedForTests = null;
        await ObjectUsageChainHistory.SeedAsync(_duckDb, ServerId, "ResetSrv", Database, newest, firstId: -1_000_000);
        Assert.Equal(snapshots + 1, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        var service = new LocalDataService(_duckDb);
        foreach (var daysBack in new[] { 7, 90 })
        {
            var full = await service.GetObjectUsageWindowFullAsync(ServerId, Database, Start(daysBack));
            var (persisted, reason) = await service.TryReadObjectUsageChainAsync(ServerId, Database, Start(daysBack));
            Assert.Null(reason);
            Assert.NotNull(persisted);
            Assert.Equal(Json(full), Json(persisted));
        }
    }

    /// <summary>The horizon the whole log gives, read the way the builder read it before it took the hot table first: every archived month included.</summary>
    private async Task<DateTime?> WholeLogHorizonAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"""
            SELECT max(collection_time + to_milliseconds(COALESCE(duration_ms, 0) + 1000))
            FROM v_collection_log
            WHERE server_id = {ServerId}
            AND   collector_name = 'index_object_stats'
            """;
        return await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken) as DateTime?;
    }

    private async Task<DateTime?> NewestBuiltAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT max(collection_time) FROM object_usage_snapshots";
        return await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken) as DateTime?;
    }

    [Fact]
    public async Task TheRunHorizon_ComesFromTheArchiveWhenTheHotLogHoldsNoRun_AndFromTheHotLogWhenItDoes_AsTheWholeLogsMaximumDoes()
    {
        await _duckDb.InitializeFromTemplateAsync();
        var rows = ObjectUsageChainHistory.Generate();
        var beforeNewest = rows.Where(r => r.CollectionTime < Now).ToList();
        var newest = rows.Where(r => r.CollectionTime >= Now).ToList();
        var newestTime = DateTime.SpecifyKind(Now, DateTimeKind.Unspecified);
        await ObjectUsageChainHistory.SeedAsync(_duckDb, ServerId, "ResetSrv", Database, beforeNewest);
        Assert.True(await new ArchiveService(_duckDb, _archiveDir, NullLogger<ArchiveService>.Instance).ArchiveAllAndResetAsync());
        Assert.Equal(0L, await CountAsync("collection_log"));
        Assert.True(await CountAsync("v_collection_log") > 0);

        /* The newest snapshot is hot and its run is not logged yet. The hot log holds no run, so the archived runs answer: every archived snapshot
           is folded and the newest is not, as under the whole log's maximum. */
        await ObjectUsageChainHistory.SeedAsync(_duckDb, ServerId, "ResetSrv", Database, newest, firstId: -1_000_000, logRuns: false);
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now);
        var snapshotsBefore = ObjectUsageChainHistory.SnapshotDays().Count() - 1;
        Assert.True(await WholeLogHorizonAsync() < newestTime);
        Assert.Equal(snapshotsBefore, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(ObjectUsageChainHistory.Day(1), await NewestBuiltAsync());

        /* A hot run that ends a millisecond short of the newest snapshot, with the slack: the hot log answers and the snapshot is still not whole. */
        await ObjectUsageChainHistory.SeedLogAsync(_duckDb, ServerId, "ResetSrv", [newestTime.AddMilliseconds(-3001)]);
        Assert.Equal(newestTime.AddMilliseconds(-1), await WholeLogHorizonAsync());
        Assert.Equal(0, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(ObjectUsageChainHistory.Day(1), await NewestBuiltAsync());

        /* A hot run that ends exactly at it: the snapshot is whole and the next pass folds it. */
        await ObjectUsageChainHistory.SeedLogAsync(_duckDb, ServerId, "ResetSrv", [newestTime.AddMilliseconds(-3000)]);
        Assert.Equal(newestTime, await WholeLogHorizonAsync());
        Assert.Equal(1, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(newestTime, await NewestBuiltAsync());

        var service = new LocalDataService(_duckDb);
        foreach (var daysBack in new[] { 7, 90 })
        {
            var (persisted, reason) = await service.TryReadObjectUsageChainAsync(ServerId, Database, Start(daysBack));
            Assert.Null(reason);
            Assert.NotNull(persisted);
            Assert.Equal(Json(await service.GetObjectUsageWindowFullAsync(ServerId, Database, Start(daysBack))), Json(persisted));
        }
    }
}
