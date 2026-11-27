/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9195: the anomaly detector's window statements read a table's hot rows alone when its archive holds nothing at or after
/// the window start, so a 4 h read no longer opens every Parquet file's footer. The answer must not change: a window that
/// reaches into the archive still reads the archived rows.
///
/// <para>The first test proves the read leaves the archive alone by taking the archive's file away after the baselines are
/// cached: a statement over the <c>v_</c> view then fails to bind, the detector logs it and emits no fact, and only a window
/// read of the hot table still fires. The second proves the other direction, a window that sits inside the archive.</para>
/// </summary>
public sealed class AnomalyDetectorArchiveWindowTests : IDisposable
{
    private const int ServerId = 7;
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly string _dbPath;
    private readonly string _archiveDir;
    private long _nextId = 1;

    public AnomalyDetectorArchiveWindowTests()
    {
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
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

    /// <summary>The start of the baseline day <paramref name="daysBack"/> before <paramref name="windowStart"/>, floored to the
    /// hour so the seeded rows stay in the window's hour-of-week bucket (the discipline of AnomalyDetectorTests.SeedDayStart).</summary>
    private static DateTime BaselineDay(DateTime windowStart, int daysBack)
    {
        var day = windowStart.AddDays(-daysBack);
        return day.Date.AddHours(day.Hour);
    }

    private static async Task ExecuteAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task SeedIoAsync(DuckDBConnection connection, DateTime time, long stallReadMs, long reads)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO file_io_stats
            (collection_id, collection_time, server_id, server_name,
             database_name, file_name, file_type, physical_name, size_mb,
             delta_reads, delta_writes, delta_read_bytes, delta_write_bytes,
             delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
            VALUES ($1, $2, $3, 'TestServer', 'AppDb', 'AppDb_data', 'ROWS', 'D:\AppDb.mdf', 100,
                    $4, 0, 81920, 0, $5, 0, 60)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = reads });
        cmd.Parameters.Add(new DuckDBParameter { Value = stallReadMs });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task SeedCpuAsync(DuckDBConnection connection, DateTime time, int cpu)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time,
             sqlserver_cpu_utilization, other_process_cpu_utilization)
            VALUES ($1, $2, $3, 'TestServer', $2, $4, 2)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpu });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>14 baseline days of 2 ms read latency in the window's hour, a quiet CPU canary for the baseline gate, and a
    /// window of 40 ms reads (800 ms of stall over 20 reads each, 16 rows: past the read-volume floor).</summary>
    private async Task SeedAsync(DuckDBConnection connection, DateTime windowStart)
    {
        for (var day = 1; day <= 14; day++)
        {
            var baseDay = BaselineDay(windowStart, day);
            for (var i = 0; i < 4; i++)
            {
                await SeedIoAsync(connection, baseDay.AddMinutes(i * 3), stallReadMs: 20, reads: 10);
                await SeedCpuAsync(connection, baseDay.AddMinutes(i * 3), 10);
            }
        }

        for (var i = 0; i < 16; i++)
            await SeedIoAsync(connection, windowStart.AddMinutes(i * 15), stallReadMs: 800, reads: 20);
    }

    /// <summary>Moves the file_io_stats rows before <paramref name="cutoff"/> into one Parquet file in the archive folder and
    /// out of the hot table, as the archive move does, and rebuilds the views over it.</summary>
    private async Task ArchiveIoBeforeAsync(DuckDBConnection connection, DateTime cutoff)
    {
        Directory.CreateDirectory(_archiveDir);
        var file = Path.Combine(_archiveDir, "20200101_0000_file_io_stats.parquet").Replace("\\", "/");
        await ExecuteAsync(connection, $"COPY (SELECT * FROM file_io_stats WHERE collection_time < '{cutoff:yyyy-MM-dd HH:mm:ss}') TO '{file}' (FORMAT PARQUET)");
        await ExecuteAsync(connection, $"DELETE FROM file_io_stats WHERE collection_time < '{cutoff:yyyy-MM-dd HH:mm:ss}'");
    }

    private static AnalysisContext ContextFor(DateTime start, DateTime end) => new()
    {
        ServerId = ServerId,
        ServerName = "TestServer",
        TimeRangeStart = start,
        TimeRangeEnd = end
    };

    [Fact]
    public async Task Window_ReadsTheHotTableAlone_WhenTheArchiveHoldsNothingFromTheWindowStart()
    {
        var end = DateTime.UtcNow;
        var start = end.AddHours(-4);
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await ExecuteAsync(connection, "BEGIN TRANSACTION");
            await SeedAsync(connection, start);
            await ExecuteAsync(connection, "COMMIT");
            /* Everything older than a week is archived, the way the hourly archive leaves a store. */
            await ArchiveIoBeforeAsync(connection, end.AddDays(-7));
        }

        await initializer.CreateArchiveViewsAsync();
        Assert.NotEmpty(Directory.GetFiles(_archiveDir, "*_file_io_stats.parquet"));

        var provider = new BaselineProvider(initializer);
        var first = await new AnomalyDetector(initializer, provider).DetectAnomaliesAsync(ContextFor(start, end));
        Assert.Contains(first, f => f.Key == "ANOMALY_READ_LATENCY");

        /* The baselines are cached for the day now. The window reads are the only reads left, and with the archive's
           file gone a statement over v_file_io_stats cannot bind: only a read of the hot table still answers. */
        foreach (var file in Directory.GetFiles(_archiveDir, "*_file_io_stats.parquet"))
            File.Delete(file);

        var second = await new AnomalyDetector(initializer, provider).DetectAnomaliesAsync(ContextFor(start, end));
        Assert.Contains(second, f => f.Key == "ANOMALY_READ_LATENCY");
    }

    [Fact]
    public async Task Window_StillReadsTheArchive_WhenTheWindowSitsInsideIt()
    {
        var end = DateTime.UtcNow.AddDays(-10);
        var start = end.AddHours(-4);
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await ExecuteAsync(connection, "BEGIN TRANSACTION");
            await SeedAsync(connection, start);
            await ExecuteAsync(connection, "COMMIT");
            /* The window and its baseline are all older than a week: every file_io_stats row lives in Parquet. */
            await ArchiveIoBeforeAsync(connection, DateTime.UtcNow.AddDays(-7));
        }

        await initializer.CreateArchiveViewsAsync();
        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM file_io_stats";
            Assert.Equal(0L, Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken)));
        }

        var facts = await new AnomalyDetector(initializer, new BaselineProvider(initializer)).DetectAnomaliesAsync(ContextFor(start, end));

        Assert.Contains(facts, f => f.Key == "ANOMALY_READ_LATENCY");
    }
}
