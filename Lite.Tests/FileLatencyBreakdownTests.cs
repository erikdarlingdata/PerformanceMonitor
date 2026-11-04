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
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7449: the per-file latency drill-down on an I/O latency finding. The finding states a latency over the window as stall
/// divided by operations. The drill-down averaged each collection interval's own ratio, so one slow write in a quiet minute
/// outweighed a thousand fast ones in a busy minute, and it ordered by read latency on a write finding, so the file with the
/// worst writes could fall off the ten-row page. A row's latency times its count must be its stall, so the rows can be
/// added up against the figure the finding states.
/// </summary>
public sealed class FileLatencyBreakdownTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 7449;

    private readonly DuckDbInitializer _duckDb;

    public FileLatencyBreakdownTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task AWriteFinding_ListsLogAndTempdbFiles_ByWriteStall_AndTheRowsAddUpToTheHeadline()
    {
        var end = DateTime.UtcNow;
        end = new DateTime(end.Ticks - (end.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
        await SeedWritePictureAsync(end);

        var context = Context(end);
        var headline = await HeadlineAsync(context, "IO_WRITE_LATENCY_MS");
        var rows = await DrillDownAsync(context, "IO_WRITE_LATENCY_MS");

        /* tempdb: one write at 500 ms in a quiet interval, 1,000 at 1 ms in a busy one: 1,500 ms over 1,001 writes. The mean of
           the two interval ratios said 250.5. The log file is 4,000 ms over 200 writes. User data files are not in the headline,
           so they are not in the rows, and the twelve of them that read slowly no longer push the two files that matter off the page. */
        Assert.Equal(["SalesDb", "tempdb"], rows.Select(r => r.Database).ToArray());
        var log = rows[0];
        var tempdb = rows[1];
        Assert.Equal(20.0, log.AvgWriteMs, precision: 2);
        Assert.Equal(200, log.TotalWrites);
        Assert.Equal(1.5, tempdb.AvgWriteMs, precision: 2);
        Assert.Equal(1001, tempdb.TotalWrites);

        /* The headline's stall is the rows' stall: latency times count, per row, to the rounding the row carries. */
        Assert.Equal(5500, headline.Metadata["total_stall_write_ms"]);
        Assert.Equal(1201, headline.Metadata["total_writes"]);
        Assert.InRange(log.AvgWriteMs * log.TotalWrites, 4000 - 0.005 * log.TotalWrites, 4000 + 0.005 * log.TotalWrites);
        Assert.InRange(tempdb.AvgWriteMs * tempdb.TotalWrites, 1500 - 0.005 * tempdb.TotalWrites, 1500 + 0.005 * tempdb.TotalWrites);
    }

    [Fact]
    public async Task AReadFinding_WeighsEachFileByItsReads_AndOrdersByReadStall()
    {
        var end = DateTime.UtcNow;
        end = new DateTime(end.Ticks - (end.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
        await SeedReadPictureAsync(end);

        var context = Context(end);
        var headline = await HeadlineAsync(context, "IO_READ_LATENCY_MS");
        var rows = await DrillDownAsync(context, "IO_READ_LATENCY_MS");

        /* BusyDb: one 900 ms read in a quiet interval, 100,000 reads at 1 ms in a busy one: 100,900 ms of stall over 100,001 reads.
           OneOffDb: a single 900 ms read. The old mean put OneOffDb (900) ahead of BusyDb (450.5); by stall BusyDb is the file
           that cost the most. */
        Assert.Equal(["BusyDb", "OneOffDb"], rows.Select(r => r.Database).ToArray());
        Assert.Equal(1.01, rows[0].AvgReadMs, precision: 2);
        Assert.Equal(100_001, rows[0].TotalReads);
        Assert.Equal(900.0, rows[1].AvgReadMs, precision: 2);

        Assert.Equal(101_800, headline.Metadata["total_stall_read_ms"]);
        Assert.Equal(100_002, headline.Metadata["total_reads"]);
    }

    private static AnalysisContext Context(DateTime end) => new()
    {
        ServerId = ServerId,
        ServerName = "file-latency-breakdown",
        TimeRangeStart = end.AddHours(-4),
        TimeRangeEnd = end,
    };

    private async Task<Fact> HeadlineAsync(AnalysisContext context, string key)
    {
        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context);
        return Assert.Single(facts, f => f.Key == key);
    }

    private sealed record Row(string Database, double AvgReadMs, double AvgWriteMs, long TotalReads, long TotalWrites);

    private async Task<List<Row>> DrillDownAsync(AnalysisContext context, string pathKey)
    {
        var finding = new AnalysisFinding
        {
            ServerId = context.ServerId,
            RootFactKey = pathKey,
            StoryPath = pathKey,
            PathKeys = [pathKey],
            Severity = 0.6,
        };

        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], context);

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown!.TryGetValue("file_latency_breakdown", out var raw), "no file_latency_breakdown drill-down");
        return JsonSerializer.SerializeToElement(raw).EnumerateArray()
            .Select(e => new Row(
                e.GetProperty("database").GetString()!,
                e.GetProperty("avg_read_latency_ms").GetDouble(),
                e.GetProperty("avg_write_latency_ms").GetDouble(),
                e.GetProperty("total_reads").GetInt64(),
                e.GetProperty("total_writes").GetInt64()))
            .ToList();
    }

    private async Task SeedWritePictureAsync(DateTime end)
    {
        /* tempdb data file: a quiet interval with one slow write, then a busy one with a thousand fast ones. */
        await InsertAsync(end.AddMinutes(-30), "tempdb", "tempdev", "ROWS", reads: 0, stallReadMs: 0, writes: 1, stallWriteMs: 500);
        await InsertAsync(end.AddMinutes(-20), "tempdb", "tempdev", "ROWS", reads: 0, stallReadMs: 0, writes: 1000, stallWriteMs: 1000);
        await InsertAsync(end.AddMinutes(-25), "SalesDb", "SalesDb_log", "LOG", reads: 0, stallReadMs: 0, writes: 200, stallWriteMs: 4000);

        /* Twelve user data files that read slowly: they filled the old read-ordered page and are not in the write headline. */
        for (var i = 1; i <= 12; i++)
        {
            await InsertAsync(end.AddMinutes(-15), $"Data{i:00}", $"Data{i:00}_data", "ROWS", reads: 100, stallReadMs: 100_000, writes: 50, stallWriteMs: 100);
        }
    }

    private async Task SeedReadPictureAsync(DateTime end)
    {
        await InsertAsync(end.AddMinutes(-30), "BusyDb", "BusyDb_data", "ROWS", reads: 1, stallReadMs: 900, writes: 0, stallWriteMs: 0);
        await InsertAsync(end.AddMinutes(-20), "BusyDb", "BusyDb_data", "ROWS", reads: 100_000, stallReadMs: 100_000, writes: 0, stallWriteMs: 0);
        await InsertAsync(end.AddMinutes(-25), "OneOffDb", "OneOffDb_data", "ROWS", reads: 1, stallReadMs: 900, writes: 0, stallWriteMs: 0);
    }

    private static long s_nextId;

    private async Task InsertAsync(DateTime at, string database, string file, string fileType,
        long reads, long stallReadMs, long writes, long stallWriteMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
     delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
VALUES ($1, $2, $3, 'file-latency-breakdown', $4, $5, $6, $7, 100, $8, $9, 0, 0, $10, $11, 60)";
        cmd.Parameters.Add(new DuckDBParameter { Value = -System.Threading.Interlocked.Increment(ref s_nextId) - 7_449_000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = file });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileType });
        cmd.Parameters.Add(new DuckDBParameter { Value = "D:\\" + file });
        cmd.Parameters.Add(new DuckDBParameter { Value = reads });
        cmd.Parameters.Add(new DuckDBParameter { Value = writes });
        cmd.Parameters.Add(new DuckDBParameter { Value = stallReadMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = stallWriteMs });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
