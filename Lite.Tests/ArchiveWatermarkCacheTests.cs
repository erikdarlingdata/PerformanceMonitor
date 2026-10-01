/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins the archive watermark cache rule: an entry is valid only for the generation it was read in, and
/// every path that removes rows from a live table bumps that generation inside the write lock.
/// </summary>
public sealed class ArchiveWatermarkCacheTests : IDisposable
{
    private readonly string _tempDir;

    public ArchiveWatermarkCacheTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    [Fact]
    public async Task SameGeneration_ReadsOnce_BumpedGeneration_ReadsAgain()
    {
        var cache = new ArchiveWatermarkCache();
        var calls = 0;
        Task<object?> Read() { calls++; return Task.FromResult<object?>(calls); }

        await cache.GetOrReadAsync("k", 1, Read);
        await cache.GetOrReadAsync("k", 1, Read);
        Assert.Equal(1, calls);

        var after = await cache.GetOrReadAsync("k", 2, Read);
        Assert.Equal(2, calls);
        Assert.Equal(2, after);
    }

    [Fact]
    public async Task NullResult_IsCached()
    {
        var cache = new ArchiveWatermarkCache();
        var calls = 0;
        Task<object?> Read() { calls++; return Task.FromResult<object?>(null); }

        Assert.Null(await cache.GetOrReadAsync("k", 1, Read));
        Assert.Null(await cache.GetOrReadAsync("k", 1, Read));
        Assert.Equal(1, calls);
    }

    /* Calls the real private DeleteArchivedRowsAsync (the periodic DELETE under the write lock) by reflection;
       the periodic export itself needs a full cycle and is not reachable without one. */
    [Fact]
    public async Task PeriodicDelete_BumpsGeneration_OnlyWhenRowsAreDeleted()
    {
        var dbPath = Path.Combine(_tempDir, "test.duckdb");
        var archivePath = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(archivePath);
        var initializer = new DuckDbInitializer(dbPath);
        await initializer.InitializeAsync();

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type)
VALUES (1, now() - INTERVAL 30 HOUR, 1, 'S1', 'SOS_SCHEDULER_YIELD')";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var service = new ArchiveService(initializer, archivePath);
        var delete = typeof(ArchiveService).GetMethod("DeleteArchivedRowsAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;

        var before = initializer.ArchiveViewGeneration;
        var none = await (Task<int>)delete.Invoke(service, ["wait_stats", "collection_time", DateTime.UtcNow.AddDays(-30)])!;
        Assert.Equal(0, none);
        Assert.Equal(before, initializer.ArchiveViewGeneration);

        var deleted = await (Task<int>)delete.Invoke(service, ["wait_stats", "collection_time", DateTime.Now])!;
        Assert.True(deleted > 0);
        Assert.True(initializer.ArchiveViewGeneration > before);
    }

    [Fact]
    public async Task BumpArchiveViewGeneration_RaisesGeneration()
    {
        var initializer = new DuckDbInitializer(Path.Combine(_tempDir, "bump.duckdb"));
        var before = initializer.ArchiveViewGeneration;
        initializer.BumpArchiveViewGeneration();
        Assert.Equal(before + 1, initializer.ArchiveViewGeneration);
        await Task.CompletedTask;
    }

    [Fact]
    public void DeleteAndReset_PathsCallBump_SourcePin()
    {
        var source = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Services", "ArchiveService.cs")));

        var del = Between(source, "private async Task<int> DeleteArchivedRowsCoreAsync", "private string PendingArchivePath");
        Assert.Contains("BumpArchiveViewGeneration()", del);

        var reset = Between(source, "await _duckDb.ResetDatabaseCoreAsync();", "AfterDatabaseResetForTests?.Invoke();");
        Assert.Contains("BumpArchiveViewGeneration()", reset);
    }

    private static string Between(string s, string start, string end)
    {
        var a = s.IndexOf(start, StringComparison.Ordinal);
        Assert.True(a >= 0, start);
        var b = s.IndexOf(end, a, StringComparison.Ordinal);
        Assert.True(b > a, end);
        return s.Substring(a, b - a);
    }

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
                return candidate;
            dir = Path.GetDirectoryName(dir);
        }

        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
