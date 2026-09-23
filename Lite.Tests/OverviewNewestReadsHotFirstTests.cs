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
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3895, the Lite twin of Darling's per-server fleet probes: the Overview card's three newest-row reads — CPU,
/// memory, last collection — answer from the HOT table when it holds the server's rows, and reach for the
/// archive view (hot UNION every archived parquet month) only when it does not.
///
/// <para><b>The defect.</b> They read the <c>v_</c> views outright, so every Overview refresh scanned every
/// archived month of three tables for every server to find values the hot table already held: 5-28 ms a read
/// against about 1 ms hot on a store holding four months of archives, three reads per server, growing with
/// the archive.</para>
///
/// <para><b>Why the hot answer is the same answer.</b> <c>ArchiveService</c> archives only rows older than its
/// cutoff, so a server's newest row is hot whenever the server has any hot row at all. The archive stays the
/// fallback, so a server that has nothing hot still reads its newest archived row. Both halves are pinned
/// below — and the first is pinned by REMOVING the archive: a read that touched the view would fail on the
/// missing parquet, so a summary that still comes back whole never opened it.</para>
/// </summary>
public sealed class OverviewNewestReadsHotFirstTests : IDisposable
{
    private const int HotServer = 3895001;
    private const int ArchiveOnlyServer = 3895002;

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archivePath;
    private long _nextId = 1;

    public OverviewNewestReadsHotFirstTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteHotFirst_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archivePath = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archivePath);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, recursive: true);
            }
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    [Fact]
    public async Task ANewestHotRow_IsReadWithoutOpeningTheArchive_AndAnArchiveOnlyServer_StillReadsItsNewestArchivedRow()
    {
        var ct = TestContext.Current.CancellationToken;
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var hotNewest = new DateTime(2026, 9, 22, 21, 0, 0);
        var archivedNewest = new DateTime(2026, 8, 31, 23, 0, 0);

        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(ct);

            /* The archived month: both servers, older rows — the hot server's must be outranked by its hot
               rows, the archive-only server's must still be found. */
            foreach (var server in new[] { HotServer, ArchiveOnlyServer })
            {
                await SeedAsync(connection, server, archivedNewest.AddHours(-1), cpu: 10, memoryMb: 1000);
                await SeedAsync(connection, server, archivedNewest, cpu: 20, memoryMb: 2000);
            }

            foreach (var table in new[] { "cpu_utilization_stats", "memory_stats", "collection_log" })
            {
                var parquet = Path.Combine(_archivePath, $"202608_{table}.parquet").Replace("\\", "/");
                await ExecAsync(connection, $"COPY {table} TO '{parquet}' (FORMAT PARQUET)");
                await ExecAsync(connection, $"DELETE FROM {table}");
            }

            /* The hot tier: only the hot server has rows. */
            await SeedAsync(connection, HotServer, hotNewest.AddMinutes(-1), cpu: 41, memoryMb: 4100);
            await SeedAsync(connection, HotServer, hotNewest, cpu: 42, memoryMb: 4200);
        }

        await initializer.CreateArchiveViewsAsync();
        var data = new LocalDataService(initializer);

        var hot = await data.GetServerSummaryAsync(HotServer, "hot", registeredAtUtc: null);
        Assert.NotNull(hot);
        Assert.Equal(42, hot!.CpuPercent);
        Assert.Equal(4200, hot.MemoryMb);
        Assert.Equal(hotNewest, hot.MemoryCollectionTime);
        Assert.Equal(hotNewest, hot.LastCollectionTime);

        /* The fallback: nothing hot, so the archive view answers — with the archived NEWEST row. */
        var archived = await data.GetServerSummaryAsync(ArchiveOnlyServer, "archived", registeredAtUtc: null);
        Assert.NotNull(archived);
        Assert.Equal(20, archived!.CpuPercent);
        Assert.Equal(2000, archived.MemoryMb);
        Assert.Equal(archivedNewest, archived.MemoryCollectionTime);
        Assert.Equal(archivedNewest, archived.LastCollectionTime);

        /* Now take the archive away. The views still name it, so any read of them fails from here on — the
           precondition, asserted so the next step cannot pass by accident. */
        foreach (var file in Directory.GetFiles(_archivePath, "*.parquet"))
        {
            File.Delete(file);
        }

        using (var check = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await check.OpenAsync(ct);
            using var cmd = check.CreateCommand();
            cmd.CommandText = "SELECT count(*) FROM v_memory_stats";
            await Assert.ThrowsAnyAsync<Exception>(() => cmd.ExecuteScalarAsync(ct));
        }

        /* The hot server's card is untouched by that: none of its three newest-row reads opened the archive. */
        var again = await data.GetServerSummaryAsync(HotServer, "hot", registeredAtUtc: null);
        Assert.NotNull(again);
        Assert.Equal(42, again!.CpuPercent);
        Assert.Equal(4200, again.MemoryMb);
        Assert.Equal(hotNewest, again.LastCollectionTime);
    }

    /// <summary>One collection's rows for one server: a CPU sample, a memory snapshot, a collection-log run, all
    /// at <paramref name="at"/>.</summary>
    private async Task SeedAsync(DuckDBConnection connection, int serverId, DateTime at, int cpu, decimal memoryMb)
    {
        await ExecAsync(connection,
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, 's', $2, $4, 1)",
            _nextId++, at, serverId, cpu);
        await ExecAsync(connection,
            "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_server_memory_mb, buffer_pool_mb) VALUES ($1, $2, $3, 's', $4, 1)",
            _nextId++, at, serverId, memoryMb);
        await ExecAsync(connection,
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, 's', 'memory_stats', $3, 'SUCCESS')",
            _nextId++, serverId, at);
    }

    private static async Task ExecAsync(DuckDBConnection connection, string sql, params object[] args)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var arg in args)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = arg });
        }

        await cmd.ExecuteNonQueryAsync();
    }
}
