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
using PerformanceMonitor.Ui;
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
[Trait("Reads", "Darling")]
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

    /// <summary>
    /// Release walk V9b: the Overview "I/O Latency ms" lane drew one point per time as the plain average of every charted file's
    /// read latency, so ten log files with no reads counted as ten 0 ms reads and pulled the figure down to a fraction of the data files'.
    /// The points now carry the read count, and the lane's figure is total stall over total reads across the files that had reads.
    /// </summary>
    [Fact]
    public async Task OverviewLaneFigure_IsWeightedByReads_AndIgnoresLogFilesWithNoReads()
    {
        await SeedLogHeavyServerAsync();

        var points = await _service.GetFileIoLatencyTrendAsync(ServerId, 1);

        Assert.Contains(points, p => p.FileName == "Db0_data" && p.Reads > 0);
        Assert.All(points.Where(p => p.FileName.EndsWith("_log", StringComparison.Ordinal)), p => Assert.Equal(0, p.Reads));

        /* (4,000 + 1,500) ms of stall over (500 + 300) reads at each time the data files were charted. */
        var perTime = points.GroupBy(p => p.CollectionTime)
            .Where(g => g.Any(p => p.Reads > 0))
            .Select(g => IoLatencyWeighting.Weighted(g.Select(x => (x.AvgReadLatencyMs, x.Reads))))
            .ToList();
        Assert.NotEmpty(perTime);
        Assert.All(perTime, v => Assert.Equal(6.875, v, 3));

        /* The old plain average of the same rows is far below it: the lane understated read latency. */
        var plain = points.GroupBy(p => p.CollectionTime).Select(g => g.Average(x => x.AvgReadLatencyMs)).Max();
        Assert.True(plain < 2.0, $"plain average {plain}");
    }

    [Theory]
    [InlineData("Lite/Controls/CorrelatedTimelineLanesControl.xaml.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/CorrelatedTimelineLanesControl.xaml.cs")]
    public void OverviewLane_AveragesReadLatencyByReads_NotPerFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !System.IO.File.Exists(System.IO.Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = System.IO.Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        var source = System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, relativePath.Replace('/', System.IO.Path.DirectorySeparatorChar)));
        Assert.Contains("IoLatencyWeighting.Weighted(g.Select(x => (x.AvgReadLatencyMs, x.Reads)))", source, StringComparison.Ordinal);
        Assert.DoesNotContain("g.Average(x => x.AvgReadLatencyMs)", source, StringComparison.Ordinal);
    }

    [Fact]
    public void IoLatencyWeighting_FilesWithNoOperationsDoNotTakePart()
    {
        Assert.Equal(6.875, IoLatencyWeighting.Weighted(new[] { (8.0, 500L), (5.0, 300L), (0.0, 0L), (0.0, 0L) }), 3);
        Assert.Equal(0, IoLatencyWeighting.Weighted(new[] { (0.0, 0L) }));
        Assert.Equal(0, IoLatencyWeighting.Weighted(System.Array.Empty<(double, long)>()));
    }

    /// <summary>
    /// File I/O ranking, NULL and tie order: PostgreSQL sorts a NULL first under DESC and DuckDB last, so a file whose reads were all
    /// NULL (writes only) took a read-ranking slot in PostgreSQL alone. The ranking sums are COALESCEd to 0 and every key is NULLS
    /// LAST, and equally busy files tie-break on the database and file name in the byte order DuckDB uses (PostgreSQL pins
    /// COLLATE "C"), so both stores keep the same ten.
    /// </summary>
    [Fact]
    public async Task Ranking_NullReadsAndTiedFiles_KeepTheSameTenInEveryStore()
    {
        /* Eleven data files tied on reads and writes; one more with NULL reads and some writes. The byte order drops "a9". */
        var names = new[] { "C_data", "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9" };
        foreach (var name in names)
        {
            await SeedAsync("Db", name, "ROWS", 10, 100, 5, 500, 10);
        }

        if (_conn is null)
        {
            _conn = _duckDb.CreateConnection();
            await _conn.OpenAsync();
        }

        using (var cmd = _conn.CreateCommand())
        {
            cmd.CommandText = @"INSERT INTO file_io_stats
                (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
                 delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
                VALUES ($1, $2, $3, 'Srv', 'Db', 'nullreads_log', 'LOG', '', 100, NULL, 7, NULL, 7, NULL, 7, 60)";
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
            cmd.Parameters.Add(new DuckDBParameter { Value = _anchor.AddMinutes(-10) });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            await cmd.ExecuteNonQueryAsync();
        }

        var points = await _service.GetFileIoLatencyTrendAsync(ServerId, 1);
        var read = points.Where(p => p.Reads > 0).Select(p => p.FileName).Distinct().ToList();

        Assert.Equal(10, read.Count);
        Assert.Contains("C_data", read);
        Assert.DoesNotContain("a9", read);
        Assert.DoesNotContain("nullreads_log", read);
    }

    [Fact]
    public void RankingStatements_SortNullsLastWithANameTieBreaker()
    {
        Assert.Contains("COALESCE(SUM(delta_reads), 0) AS total_reads", LocalDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);
        Assert.Contains("(ORDER BY total_reads DESC NULLS LAST, total_writes DESC NULLS LAST, database_name NULLS LAST, file_name NULLS LAST) AS read_rank", LocalDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);
        Assert.Contains("(ORDER BY total_writes DESC NULLS LAST, total_reads DESC NULLS LAST, database_name NULLS LAST, file_name NULLS LAST) AS write_rank", LocalDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY SUM(delta_read_bytes + delta_write_bytes) DESC NULLS LAST, database_name NULLS LAST, file_name NULLS LAST", LocalDataService.FileIoThroughputTrendSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The charts take the files the read gives them. Twelve log files at 3 ms writes and two data files at 1 ms reads: the read
    /// chart must show the two data files (the old chart cut the combined list to ten by summed latency and kept only logs), and the
    /// write chart the ten busiest writers, each file ranked on its own count.
    /// </summary>
    [Fact]
    public void ChartFiles_EachChartTakesItsOwnTen_NotASharedTenBySummedLatency()
    {
        var t = DateTime.UtcNow;
        var points = new System.Collections.Generic.List<FileIoTrendPoint>();
        for (var i = 0; i < 12; i++)
        {
            points.Add(new FileIoTrendPoint { CollectionTime = t, DatabaseName = $"Db{i:00}", FileName = "log", AvgWriteLatencyMs = 3.0, Writes = 1000 + i });
        }

        points.Add(new FileIoTrendPoint { CollectionTime = t, DatabaseName = "DbA", FileName = "data", AvgReadLatencyMs = 1.0, Reads = 50, Writes = 1 });
        points.Add(new FileIoTrendPoint { CollectionTime = t, DatabaseName = "DbB", FileName = "data", AvgReadLatencyMs = 1.0, Reads = 40, Writes = 1 });

        var read = FileIoChartFiles.ReadChartFiles(points, p => $"{p.DatabaseName}.{p.FileName}", p => p.Reads, p => p.Writes);
        var write = FileIoChartFiles.WriteChartFiles(points, p => $"{p.DatabaseName}.{p.FileName}", p => p.Reads, p => p.Writes);

        Assert.Equal(new[] { "DbA.data", "DbB.data" }, read.Select(g => g.Key));

        /* The ten log files with the most writes (Db11 first) fill the write chart; the data files' 1 write each cannot reach the ten. */
        Assert.Equal(10, write.Count);
        Assert.Equal("Db11.log", write[0].Key);
        Assert.All(write, g => Assert.EndsWith(".log", g.Key, StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("Lite/Controls/ServerTab.Charts.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.FileIo.cs")]
    public void FileIoCharts_UseTheRankedFileLists_NotASummedLatencyCut(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !System.IO.File.Exists(System.IO.Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = System.IO.Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        var source = System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, relativePath.Replace('/', System.IO.Path.DirectorySeparatorChar)));
        Assert.Contains("FileIoChartFiles.ReadChartFiles(data,", source, StringComparison.Ordinal);
        Assert.Contains("FileIoChartFiles.WriteChartFiles(data,", source, StringComparison.Ordinal);
        Assert.DoesNotContain("limit to top 10 by total stall", source, StringComparison.Ordinal);
    }
}
