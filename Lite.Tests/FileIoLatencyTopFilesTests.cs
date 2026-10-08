/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Release walk V9: the File I/O Read Latency chart and the Overview "I/O Latency ms" lane were blank on a server whose log files
/// carry the most operations. The latency read took the ten files with the most reads + writes, which were all log files (a log has
/// writes and no reads), so no data file reached the read chart and the lane averaged zeros. Each chart now ranks its own subject:
/// the ten busiest files by reads feed the read chart and the ten busiest by writes feed the write chart.
/// </summary>
public sealed class FileIoLatencyTopFilesTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 548900;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private DuckDBConnection? _conn;
    private long _nextId = 5489000;

    public FileIoLatencyTopFilesTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _service = new LocalDataService(_duckDb);
    }

    public void Dispose() => _conn?.Dispose();

    private readonly DateTime _anchor = new(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, 0, DateTimeKind.Utc);

    private async Task SeedAsync(string db, string file, string fileType, int minutesAgo, long reads, long writes, long stallReadMs, long stallWriteMs)
    {
        if (_conn is null)
        {
            _conn = _duckDb.CreateConnection();
            await _conn.OpenAsync();
        }

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO file_io_stats
            (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
             delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms,
             sample_interval_seconds)
            VALUES ($1, $2, $3, 'Srv', $4, $5, $6, '', 100, $7, $8, $9, $10, $11, $12, 60)";
        foreach (var v in new object[] { _nextId++, _anchor.AddMinutes(-minutesAgo), ServerId, db, file, fileType, reads, writes, reads * 8192, writes * 8192, stallReadMs, stallWriteMs })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Twelve databases whose log files write far more than anything else on the server and never read, and two data files with real reads.</summary>
    private async Task SeedLogHeavyServerAsync()
    {
        foreach (var minutes in new[] { 20, 10 })
        {
            for (var i = 0; i < 12; i++)
            {
                await SeedAsync($"Db{i}", $"Db{i}_log", "LOG", minutes, 0, 100_000, 0, 200_000);
            }

            await SeedAsync("Db0", "Db0_data", "ROWS", minutes, 500, 20, 4_000, 60);
            await SeedAsync("Db1", "Db1_data", "ROWS", minutes, 300, 10, 1_500, 30);
        }
    }

    [Fact]
    public async Task ReadChart_LogFilesWithTheLargestTotalsAndNoReads_DoNotCrowdOutTheDataFiles()
    {
        await SeedLogHeavyServerAsync();

        var points = await _service.GetFileIoLatencyTrendAsync(ServerId, 1);

        var files = points.Select(p => p.FileName).Distinct().ToList();
        Assert.Contains("Db0_data", files);
        Assert.Contains("Db1_data", files);

        /* 4,000 ms of stall over 500 reads. */
        Assert.Contains(points, p => p.FileName == "Db0_data" && Math.Abs(p.AvgReadLatencyMs - 8.0) < 0.001);
        Assert.Contains(points, p => p.FileName == "Db1_data" && Math.Abs(p.AvgReadLatencyMs - 5.0) < 0.001);
    }

    [Fact]
    public async Task WriteChart_StillGetsTheTenBusiestWriters()
    {
        await SeedLogHeavyServerAsync();

        var points = await _service.GetFileIoLatencyTrendAsync(ServerId, 1);

        /* Twelve log files write 100,000 each; the ten the write ranking keeps are the first ten by name, and they chart their 2 ms writes. */
        var logs = points.Where(p => p.FileName.EndsWith("_log", StringComparison.Ordinal)).Select(p => p.FileName).Distinct().ToList();
        Assert.Equal(10, logs.Count);
        Assert.All(points.Where(p => p.FileName.EndsWith("_log", StringComparison.Ordinal)), p => Assert.Equal(2.0, p.AvgWriteLatencyMs, 3));
    }
}
