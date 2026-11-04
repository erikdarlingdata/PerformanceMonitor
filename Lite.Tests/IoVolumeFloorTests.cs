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
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7417: the read and write latency facts and anomalies need <see cref="IoVolumeFloor"/> I/Os behind them in the window.
/// An idle server's two reads at 57 ms used to fire a WARNING and a CRITICAL. Each case is run on both sides of the floor
/// (249 and 250 I/Os) and a sustained problem with real volume still fires. The write side counts log files and tempdb only.
/// </summary>
public class IoVolumeFloorTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -7417_01;
    private static readonly DateTime WindowStart = new(2026, 3, 4, 10, 0, 0);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(4);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _conn;
    private long _nextId = -1;

    public IoVolumeFloorTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _conn?.Dispose();

    private async Task ExecAsync(string sql, params object[] args)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_conn is null)
        {
            _conn = _duckDb.CreateConnection();
            await _conn.OpenAsync();
        }
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var arg in args)
            cmd.Parameters.Add(new DuckDBParameter { Value = arg });
        await cmd.ExecuteNonQueryAsync();
    }

    private Task SeedCollectedHistoryAsync() => ExecAsync(
        @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time,
            sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, 'TestServer', $2, 10, 2)",
        _nextId--, WindowEnd.AddDays(-1), ServerId);

    private Task SeedRowAsync(DateTime at, string fileType, string database, long reads, long stallReadMs, long writes, long stallWriteMs) => ExecAsync(
        @"INSERT INTO file_io_stats
            (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
             delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
          VALUES ($1, $2, $3, 'TestServer', $4, $5, $6, 'D:\f.mdf', 100, $7, $8, 81920, 81920, $9, $10, 60)",
        _nextId--, at, ServerId, database, "f" + (-_nextId), fileType, reads, writes, stallReadMs, stallWriteMs);

    /// <summary>Fourteen days of quiet 2 ms reads and 1 ms log writes in the window's own hour, so the baseline is trustworthy.</summary>
    private async Task SeedQuietBaselineAsync()
    {
        await ExecAsync("BEGIN TRANSACTION");
        for (var day = 1; day <= 14; day++)
        {
            for (var i = 0; i < 4; i++)
            {
                var at = WindowStart.AddDays(-day).AddMinutes(i * 3);
                await SeedRowAsync(at, "ROWS", "AppDb", 10, 20, 0, 0);
                await SeedRowAsync(at, "LOG", "AppDb", 0, 0, 10, 10);
            }
        }
        await ExecAsync("COMMIT");
    }

    /// <summary>Four files in one collection that sum to <paramref name="total"/> I/Os. Read rows run at 40 ms a read, write rows at 60 ms.</summary>
    private async Task SeedWindowAsync(bool reads, long total, string fileType = "LOG", string database = "AppDb")
    {
        var at = WindowStart.AddHours(1);
        for (var i = 0; i < 4; i++)
        {
            var share = (total / 4) + (i < total % 4 ? 1 : 0);
            if (reads)
                await SeedRowAsync(at, "ROWS", database, share, share * 40, 0, 0);
            else
                await SeedRowAsync(at, fileType, database, 0, 0, share, share * 60);
        }
    }

    private AnalysisContext Context() => new()
    {
        ServerId = ServerId,
        ServerName = "TestServer",
        TimeRangeStart = WindowStart,
        TimeRangeEnd = WindowEnd,
    };

    private Task<List<Fact>> FactsAsync() => new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());

    private async Task<List<Fact>> AnomaliesAsync()
    {
        await SeedCollectedHistoryAsync();
        return await new AnomalyDetector(_duckDb, new BaselineProvider(_duckDb)).DetectAnomaliesAsync(Context());
    }

    [Theory]
    [InlineData(2, false)]
    [InlineData(249, false)]
    [InlineData(250, true)]
    [InlineData(100_000, true)]
    public async Task ReadLatencyFact_FiresOnlyWithTheFloorsReads(int reads, bool fires)
    {
        await SeedWindowAsync(reads: true, total: reads);

        var fact = (await FactsAsync()).FirstOrDefault(f => f.Key == "IO_READ_LATENCY_MS");

        Assert.Equal(fires, fact is not null);
        if (fact is not null)
        {
            Assert.Equal(40.0, fact.Value, 6);
            Assert.Equal(reads, fact.Metadata["total_reads"]);
        }
    }

    [Theory]
    [InlineData(249, false)]
    [InlineData(250, true)]
    [InlineData(100_000, true)]
    public async Task WriteLatencyFact_FiresOnlyWithTheFloorsWrites(int writes, bool fires)
    {
        await SeedWindowAsync(reads: false, total: writes);

        var fact = (await FactsAsync()).FirstOrDefault(f => f.Key == "IO_WRITE_LATENCY_MS");

        Assert.Equal(fires, fact is not null);
        if (fact is not null)
            Assert.Equal(60.0, fact.Value, 6);
    }

    [Fact]
    public async Task WriteLatencyFact_UserDataFileWritesDoNotLendVolumeToAQuietLog()
    {
        await SeedWindowAsync(reads: false, total: 10);
        await SeedWindowAsync(reads: false, total: 100_000, fileType: "ROWS");

        Assert.DoesNotContain(await FactsAsync(), f => f.Key == "IO_WRITE_LATENCY_MS");
    }

    [Theory]
    [InlineData(2, false)]
    [InlineData(249, false)]
    [InlineData(250, true)]
    [InlineData(100_000, true)]
    public async Task ReadLatencyAnomaly_FiresOnlyWithTheFloorsReads(int reads, bool fires)
    {
        await SeedQuietBaselineAsync();
        await SeedWindowAsync(reads: true, total: reads);

        var anomaly = (await AnomaliesAsync()).FirstOrDefault(f => f.Key == "ANOMALY_READ_LATENCY");

        Assert.Equal(fires, anomaly is not null);
        if (anomaly is not null)
            Assert.Equal(reads, anomaly.Metadata["window_io_ops"]);
    }

    [Theory]
    [InlineData(249, false)]
    [InlineData(250, true)]
    [InlineData(100_000, true)]
    public async Task WriteLatencyAnomaly_FiresOnlyWithTheFloorsWrites(int writes, bool fires)
    {
        await SeedQuietBaselineAsync();
        await SeedWindowAsync(reads: false, total: writes);

        var anomaly = (await AnomaliesAsync()).FirstOrDefault(f => f.Key == "ANOMALY_WRITE_LATENCY");

        Assert.Equal(fires, anomaly is not null);
        if (anomaly is not null)
            Assert.Equal(writes, anomaly.Metadata["window_io_ops"]);
    }
}
