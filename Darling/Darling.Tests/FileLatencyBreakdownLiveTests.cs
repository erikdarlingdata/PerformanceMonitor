/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7449, Darling's half of Lite's <c>FileLatencyBreakdownTests</c>: the per-file latency drill-down on an I/O latency
/// finding states a file's latency as stall over operations, so a row's latency times its count is its stall and the rows
/// add up to the figure the finding states. A write finding lists the log and tempdb files its figure covers, by write
/// stall, and a read finding lists every file by read stall. The statement is the one the Lite twin runs on DuckDB, so this
/// pins that PostgreSQL gives that text the same answer, including the boolean parameter that picks the fact.
/// </summary>
[Collection("live-postgres")]
public sealed class FileLatencyBreakdownLiveTests
{
    /// <summary>A sentinel id: a real server_id is a storage-name hash, never this.</summary>
    private const int WriteServerId = -744_901;
    private const int ReadServerId = -744_902;

    private static readonly int[] s_serverIds = [WriteServerId, ReadServerId];

    [Fact]
    public async Task AWriteFinding_ListsLogAndTempdbFiles_ByWriteStall_AndTheRowsAddUpToTheHeadline_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live file latency breakdown test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);

            var end = TruncateToSeconds(DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc));
            /* tempdb: one write at 500 ms in a quiet interval, 1,000 at 1 ms in a busy one: 1,500 ms over 1,001 writes (the mean of
               the two interval ratios said 250.5). The log file is 4,000 ms over 200 writes. Twelve user data files read slowly:
               they filled the old read-ordered page and are not part of the write headline. */
            await InsertAsync(connection, WriteServerId, end.AddMinutes(-30), "tempdb", "tempdev", "ROWS", 0, 0, 1, 500, ct);
            await InsertAsync(connection, WriteServerId, end.AddMinutes(-20), "tempdb", "tempdev", "ROWS", 0, 0, 1000, 1000, ct);
            await InsertAsync(connection, WriteServerId, end.AddMinutes(-25), "SalesDb", "SalesDb_log", "LOG", 0, 0, 200, 4000, ct);
            for (var i = 1; i <= 12; i++)
            {
                await InsertAsync(connection, WriteServerId, end.AddMinutes(-15), $"Data{i:00}", $"Data{i:00}_data", "ROWS", 100, 100_000, 50, 100, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var context = Context(WriteServerId, end);
            var headline = Assert.Single(await new PgFactCollector(postgres).CollectFactsAsync(context), f => f.Key == "IO_WRITE_LATENCY_MS");
            var rows = await DrillDownAsync(postgres, context, "IO_WRITE_LATENCY_MS");

            Assert.Equal(["SalesDb", "tempdb"], rows.Select(r => r.Database).ToArray());
            Assert.Equal(20.0, rows[0].AvgWriteMs, precision: 2);
            Assert.Equal(200, rows[0].TotalWrites);
            Assert.Equal(1.5, rows[1].AvgWriteMs, precision: 2);
            Assert.Equal(1001, rows[1].TotalWrites);

            Assert.Equal(5500, headline.Metadata["total_stall_write_ms"]);
            Assert.Equal(1201, headline.Metadata["total_writes"]);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    [Fact]
    public async Task AReadFinding_WeighsEachFileByItsReads_AndOrdersByReadStall_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live file latency breakdown test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);

            var end = TruncateToSeconds(DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc));
            /* BusyDb: one 900 ms read in a quiet interval, 100,000 reads at 1 ms in a busy one: 100,900 ms over 100,001 reads.
               OneOffDb: a single 900 ms read. The old mean put OneOffDb (900) ahead of BusyDb (450.5). */
            await InsertAsync(connection, ReadServerId, end.AddMinutes(-30), "BusyDb", "BusyDb_data", "ROWS", 1, 900, 0, 0, ct);
            await InsertAsync(connection, ReadServerId, end.AddMinutes(-20), "BusyDb", "BusyDb_data", "ROWS", 100_000, 100_000, 0, 0, ct);
            await InsertAsync(connection, ReadServerId, end.AddMinutes(-25), "OneOffDb", "OneOffDb_data", "ROWS", 1, 900, 0, 0, ct);

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var context = Context(ReadServerId, end);
            var headline = Assert.Single(await new PgFactCollector(postgres).CollectFactsAsync(context), f => f.Key == "IO_READ_LATENCY_MS");
            var rows = await DrillDownAsync(postgres, context, "IO_READ_LATENCY_MS");

            Assert.Equal(["BusyDb", "OneOffDb"], rows.Select(r => r.Database).ToArray());
            Assert.Equal(1.01, rows[0].AvgReadMs, precision: 2);
            Assert.Equal(100_001, rows[0].TotalReads);
            Assert.Equal(900.0, rows[1].AvgReadMs, precision: 2);

            Assert.Equal(101_800, headline.Metadata["total_stall_read_ms"]);
            Assert.Equal(100_002, headline.Metadata["total_reads"]);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    private sealed record Row(string Database, double AvgReadMs, double AvgWriteMs, long TotalReads, long TotalWrites);

    private static AnalysisContext Context(int serverId, DateTime end) => new()
    {
        ServerId = serverId,
        ServerName = "file-latency-breakdown-e2e",
        TimeRangeStart = end.AddHours(-4),
        TimeRangeEnd = end,
        ServerUtcOffset = TimeSpan.Zero,
    };

    private static async Task<List<Row>> DrillDownAsync(NpgsqlDataSource postgres, AnalysisContext context, string pathKey)
    {
        var finding = new AnalysisFinding
        {
            ServerId = context.ServerId,
            RootFactKey = pathKey,
            StoryPath = pathKey,
            PathKeys = [pathKey],
            Severity = 0.6,
        };

        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

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

    private static async Task InsertAsync(NpgsqlConnection connection, int serverId, DateTime at, string database, string file,
        string fileType, long reads, long stallReadMs, long writes, long stallWriteMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
     delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
VALUES ($1, $2, $3, 'file-latency-breakdown-e2e', $4, $5, $6, $7, 100, $8, $9, 0, 0, $10, $11, 60)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(file);
        command.Parameters.AddWithValue(fileType);
        command.Parameters.AddWithValue("D:\\" + file);
        command.Parameters.AddWithValue(reads);
        command.Parameters.AddWithValue(writes);
        command.Parameters.AddWithValue(stallReadMs);
        command.Parameters.AddWithValue(stallWriteMs);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteSentinelsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", s_serverIds.Select(id => id.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        using var cleanup = new NpgsqlCommand($"DELETE FROM file_io_stats WHERE server_id IN ({ids})", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
}
