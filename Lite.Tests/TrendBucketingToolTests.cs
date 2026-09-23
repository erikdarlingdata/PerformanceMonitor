/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3897 on Lite: the bucketed reads behind get_file_io_trend and the duration trends roll their collections up
/// the way their notes say, on planted rows whose figures are worked by hand. The SQL is DuckDB's, written apart
/// from Darling's, so the Darling live tests (TrendBucketingLiveTests) prove nothing about it — these plant the
/// same rows and assert the same numbers.
/// </summary>
public sealed class TrendBucketingToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TrendBucketSrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 870000;

    public TrendBucketingToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-trendbucket-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    /// <summary>
    /// Seven (database, file type) series in the 10:00 ten-minute bucket plus one write at 10:12, ranked on stall:
    /// A-ROWS 800, A-LOG 500, B 450, C 300, D 250, then E 150 and F 90 folded into "(other)".
    /// </summary>
    [Fact]
    public async Task FileIoTrend_RanksByStall_FoldsPerCollection_AndRecomputesLatencyFromSums()
    {
        var service = new LocalDataService(_duckDb);
        var t1 = new DateTime(2026, 3, 4, 10, 1, 0);
        var t2 = t1.AddMinutes(1);
        var t3 = new DateTime(2026, 3, 4, 10, 12, 0);

        /* dbA's data files are two files pooled into one line: at t1, 200 reads with 600 ms of stall — 3 ms each. */
        await SeedFileAsync(t1, "dbA", "a1.mdf", "ROWS", reads: 100, writes: 0, stallRead: 500, stallWrite: 0);
        await SeedFileAsync(t1, "dbA", "a2.ndf", "ROWS", reads: 100, writes: 0, stallRead: 100, stallWrite: 0);
        await SeedFileAsync(t2, "dbA", "a1.mdf", "ROWS", reads: 200, writes: 0, stallRead: 200, stallWrite: 0);
        await SeedFileAsync(t1, "dbA", "a.ldf", "LOG", reads: 0, writes: 100, stallRead: 0, stallWrite: 500);
        await SeedFileAsync(t1, "dbB", "b.mdf", "ROWS", reads: 100, writes: 0, stallRead: 400, stallWrite: 0);
        await SeedFileAsync(t1, "dbC", "c.mdf", "ROWS", reads: 100, writes: 0, stallRead: 300, stallWrite: 0);
        await SeedFileAsync(t1, "dbD", "d.mdf", "ROWS", reads: 100, writes: 0, stallRead: 250, stallWrite: 0);

        /* The fold, pooled per collection: at t1 E's 10 reads at 10 ms and F's 90 at 1 ms are 190 ms over 100
           reads, 1.9 ms — the (other) line's peak, not the 10 ms one member hit alone. */
        await SeedFileAsync(t1, "dbE", "e.mdf", "ROWS", reads: 10, writes: 0, stallRead: 100, stallWrite: 0);
        await SeedFileAsync(t1, "dbF", "f.mdf", "ROWS", reads: 90, writes: 0, stallRead: 90, stallWrite: 0);
        await SeedFileAsync(t2, "dbE", "e.mdf", "ROWS", reads: 50, writes: 0, stallRead: 50, stallWrite: 0);

        /* A write-only bucket: a write latency and a NULL read latency, never 0.00. */
        await SeedFileAsync(t3, "dbB", "b.mdf", "ROWS", reads: 0, writes: 10, stallRead: 0, stallWrite: 50);

        /* A restart row (a stored interval of 0, #3540) with a garbage delta: ranks nothing, charts nothing. */
        await SeedFileAsync(t2, "dbZ", "z.mdf", "ROWS", reads: 1_000_000, writes: 0, stallRead: 99_000_000, stallWrite: 0, interval: 0);

        var root = Root(await McpIoTools.GetFileIoTrend(service, _serverManager, ServerName, 1, "2026-03-04T11:00:00Z", bucket_minutes: 10));
        Assert.Equal("database_file_type", root.GetProperty("series_grain").GetString());
        Assert.Equal(7, root.GetProperty("series_active").GetInt32());
        Assert.Equal(5, root.GetProperty("series_charted").GetInt32());
        Assert.Equal(2, root.GetProperty("series_folded").GetInt32());

        var legend = root.GetProperty("series").EnumerateArray().ToArray();
        Assert.Equal(
            new[] { "dbA|ROWS", "dbA|LOG", "dbB|ROWS", "dbC|ROWS", "dbD|ROWS", "(other)|(other)" },
            legend.Select(s => s.GetProperty("database_name").GetString() + "|" + s.GetProperty("file_type").GetString()).ToArray());
        Assert.Equal(2, legend[0].GetProperty("files").GetInt32());
        Assert.Equal(2.0, legend[0].GetProperty("avg_read_latency_ms").GetDouble(), 6);
        Assert.Equal(3.0, legend[0].GetProperty("peak_read_latency_ms").GetDouble(), 6);
        Assert.Equal(1.6, legend[5].GetProperty("avg_read_latency_ms").GetDouble(), 6);
        Assert.Equal(1.9, legend[5].GetProperty("peak_read_latency_ms").GetDouble(), 6);

        var trend = root.GetProperty("trend").EnumerateArray().ToArray();
        Assert.Equal(7, trend.Length);
        var bWrites = Assert.Single(trend, p => p.GetProperty("database_name").GetString() == "dbB"
                                               && p.GetProperty("time").GetString()!.StartsWith("2026-03-04T10:10:00", StringComparison.Ordinal));
        Assert.Equal(JsonValueKind.Null, bWrites.GetProperty("avg_read_latency_ms").ValueKind);
        Assert.Equal(5.0, bWrites.GetProperty("avg_write_latency_ms").GetDouble(), 6);
        Assert.DoesNotContain(trend, p => p.GetProperty("database_name").GetString() == "dbZ");

        /* Scoped to one database: its files, one line each, in stall order. */
        var scoped = Root(await McpIoTools.GetFileIoTrend(service, _serverManager, ServerName, 1, "2026-03-04T11:00:00Z", bucket_minutes: 10, database_name: "dbA"));
        Assert.Equal("file", scoped.GetProperty("series_grain").GetString());
        Assert.Equal(
            new[] { "a1.mdf", "a.ldf", "a2.ndf" },
            scoped.GetProperty("series").EnumerateArray().Select(s => s.GetProperty("file_name").GetString()).ToArray());
        var a1 = Assert.Single(scoped.GetProperty("trend").EnumerateArray(), p => p.GetProperty("file_name").GetString() == "a1.mdf");
        Assert.Equal(5.0, a1.GetProperty("peak_read_latency_ms").GetDouble(), 6);

        var missing = Root(await McpIoTools.GetFileIoTrend(service, _serverManager, ServerName, 1, "2026-03-04T11:00:00Z", database_name: "dbNope"));
        Assert.Equal("empty", missing.GetProperty("status").GetString());
        Assert.Contains("'dbNope'", missing.GetProperty("message").GetString()!, StringComparison.Ordinal);

        /* An unusable width is refused before anything is read; a width over the cap names the width that fits
           the lines the ranking found. */
        Assert.True(McpHelpers.IsRefusalEnvelope(await McpIoTools.GetFileIoTrend(service, _serverManager, ServerName, 1, "2026-02-01T00:00:00Z", bucket_minutes: 0)));
        var overCap = await McpIoTools.GetFileIoTrend(service, _serverManager, ServerName, 24, "2026-03-04T11:00:00Z", bucket_minutes: 1);
        Assert.Contains("across 6 series", McpHelpers.ErrorMessageOf(overCap), StringComparison.Ordinal);
    }

    /// <summary>
    /// The duration trend, time-weighted: in the 10:00 bucket a 60-second collection at 100 ms/s, a 120-second one
    /// at 10 ms/s and a restart (stored 0) — (6,000 + 1,200) ms over 180 s = 40 ms/s, the restart left out and
    /// counted. The 10:10 bucket holds only a restart (null rates); the 10:20 one a pre-v61 collection rated over
    /// the ten minutes since the one before.
    /// </summary>
    [Fact]
    public async Task DurationTrend_IsTimeWeighted_LeavesTheUnknowableOut_AndStampsTheFirstPointAtTheWindowStart()
    {
        var service = new LocalDataService(_duckDb);
        var ten = new DateTime(2026, 3, 4, 10, 0, 0);

        await SeedQueryAsync(ten.AddMinutes(-3), executions: 1, elapsedMs: 60, interval: 60);
        await SeedQueryAsync(ten.AddMinutes(2), executions: 60, elapsedMs: 6_000, interval: 60);
        await SeedQueryAsync(ten.AddMinutes(4), executions: 12, elapsedMs: 1_200, interval: 120);
        await SeedQueryAsync(ten.AddMinutes(5), executions: 99_999, elapsedMs: 999_000, interval: 0);
        await SeedQueryAsync(ten.AddMinutes(15), executions: 5, elapsedMs: 500, interval: 0);
        await SeedQueryAsync(ten.AddMinutes(25), executions: 6, elapsedMs: 600, interval: null);

        var root = Root(await McpQueryTools.GetQueryDurationTrend(service, _serverManager, ServerName, 1, "2026-03-04T11:00:00Z", bucket_minutes: 10));
        Assert.Equal("10 minutes", root.GetProperty("bucket").GetString());

        var trend = root.GetProperty("trend").EnumerateArray().ToArray();
        Assert.Equal(3, trend.Length);
        Assert.StartsWith("2026-03-04T10:00:00", trend[0].GetProperty("time").GetString()!, StringComparison.Ordinal);
        Assert.Equal(40.0, trend[0].GetProperty("elapsed_ms_per_second").GetDouble(), 9);
        Assert.Equal(0.4, trend[0].GetProperty("executions_per_second").GetDouble(), 9);
        Assert.Equal(100.0, trend[0].GetProperty("peak_elapsed_ms_per_second").GetDouble(), 9);
        Assert.Equal(JsonValueKind.Null, trend[1].GetProperty("elapsed_ms_per_second").ValueKind);
        Assert.Equal(1.0, trend[2].GetProperty("elapsed_ms_per_second").GetDouble(), 9);
        Assert.Equal(1, root.GetProperty("unrated_points").GetInt32());
        Assert.Equal(2, root.GetProperty("unrated_collections").GetInt32());
        Assert.StartsWith("2026-03-04T10:02:00", root.GetProperty("effective_start").GetString()!, StringComparison.Ordinal);

        /* A window starting mid-bucket: the 09:57 collection's bucket began at 09:50, so its point is stamped at
           the window's 09:55 start, and effective_start is still the collection. */
        var shifted = Root(await McpQueryTools.GetQueryDurationTrend(service, _serverManager, ServerName, 1, "2026-03-04T10:55:00Z", bucket_minutes: 10));
        var first = shifted.GetProperty("trend")[0];
        Assert.StartsWith("2026-03-04T09:55:00", first.GetProperty("time").GetString()!, StringComparison.Ordinal);
        Assert.Equal(1.0, first.GetProperty("elapsed_ms_per_second").GetDouble(), 9);
        Assert.StartsWith("2026-03-04T09:57:00", shifted.GetProperty("effective_start").GetString()!, StringComparison.Ordinal);
    }

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedFileAsync(
        DateTime collectionTime, string database, string file, string fileType,
        long reads, long writes, long stallRead, long stallWrite, int? interval = 60)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, file_name, file_type, size_mb,
     num_of_reads, num_of_writes, read_bytes, write_bytes,
     io_stall_read_ms, io_stall_write_ms,
     delta_reads, delta_writes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, 0, 0, 0, 0, 0, 0, 0, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = file });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileType });
        cmd.Parameters.Add(new DuckDBParameter { Value = reads });
        cmd.Parameters.Add(new DuckDBParameter { Value = writes });
        cmd.Parameters.Add(new DuckDBParameter { Value = stallRead });
        cmd.Parameters.Add(new DuckDBParameter { Value = stallWrite });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)interval ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedQueryAsync(DateTime collectionTime, long executions, long elapsedMs, int? interval)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, query_text, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'Sales', '0xBUCKETHASH', '0xSQLH', $2, $5, 0, $6, 'SELECT 1', $7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = elapsedMs * 1000 });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)interval ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }
}
