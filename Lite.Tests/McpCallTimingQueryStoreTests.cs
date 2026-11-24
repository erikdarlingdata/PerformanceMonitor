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
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9423: the done-when of the issue. A real get_query_store_top call over a lowered threshold writes a row with every phase filled:
/// the lock wait, the hot store time, the archive time, the archive files its span read opened, the rows it returned and the reply bytes.
/// The store holds hot rows and a Parquet archive file, so the archive span read really opens a file.
/// </summary>
public sealed class McpCallTimingQueryStoreTests : IAsyncDisposable
{
    private const string ServerName = "TimingQsSrv";

    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly ServerConnection _server;
    private readonly LocalDataService _data;
    private McpCallTimingTests.ServerHost? _host;

    public McpCallTimingQueryStoreTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _duckDb.InitializeFromTemplateAsync().GetAwaiter().GetResult();
        Directory.CreateDirectory(Path.Combine(_tempDir, "config"));
        _serverManager = new ServerManager(Path.Combine(_tempDir, "config"));
        _server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, IsEnabled = true };
        _serverManager.AddServer(_server);
        _data = new LocalDataService(_duckDb);
    }

    public async ValueTask DisposeAsync()
    {
        if (_host is not null) await _host.DisposeAsync();
        _duckDb.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    private static string Ts(DateTime t) => $"TIMESTAMP '{t:yyyy-MM-dd HH:mm:ss}'";

    [Fact]
    public async Task AQueryStoreTopCall_OverALoweredThreshold_WritesARowWithEveryPhaseFilled()
    {
        var serverId = RemoteCollectorService.GetServerId(_server);
        var now = DateTime.UtcNow;
        var id = 1;
        string Interval(DateTime collected, int queryId) =>
            $"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, " +
            $"first_execution_time, last_execution_time, query_hash, execution_count, avg_cpu_time_us, avg_duration_us, query_plan_hash, is_forced_plan, " +
            $"force_failure_count, runtime_stats_interval_id, interval_start_time_utc) VALUES ({id++}, {Ts(collected)}, {serverId}, '{ServerName}', 'SalesDb', " +
            $"{queryId}, {queryId * 10}, 'Regular', {Ts(collected.AddMinutes(-30))}, {Ts(collected.AddMinutes(-5))}, '0xQH{queryId}', 10, 1000, 1000, '0xPH{queryId}', false, 0, " +
            $"{queryId}, {Ts(collected.AddMinutes(-30))})";

        /* Three queries stay in the hot table. Two are moved to a Parquet archive file inside the same 24-hour window, so the read opens it. */
        await ExecuteAsync(
            Interval(now.AddMinutes(-20), 1), Interval(now.AddMinutes(-25), 2), Interval(now.AddMinutes(-30), 3),
            Interval(now.AddHours(-8), 4), Interval(now.AddHours(-9), 5));
        var archivePath = Path.Combine(_archiveDir, "20260101_000000_query_store_stats.parquet").Replace("\\", "/");
        await ExecuteAsync(
            $"COPY (SELECT * FROM query_store_stats WHERE collection_time < {Ts(now.AddHours(-2))}) TO '{DuckDbInitializer.EscapeSqlPath(archivePath)}' (FORMAT PARQUET)",
            $"DELETE FROM query_store_stats WHERE collection_time < {Ts(now.AddHours(-2))}");
        await _duckDb.CreateArchiveViewsAsync();

        var recorder = new McpCallTimingRecorder(_data) { SlowThreshold = TimeSpan.Zero };
        _host = await McpCallTimingTests.ServerHost.StartAsync(_data, _serverManager, recorder);
        var result = await _host.Client.CallToolAsync("get_query_store_top",
            new System.Collections.Generic.Dictionary<string, object?> { ["server_name"] = ServerName, ["hours_back"] = 24 },
            cancellationToken: TestContext.Current.CancellationToken);
        await recorder.WhenWritesDoneAsync();

        var reply = string.Concat(result.Content.OfType<ModelContextProtocol.Protocol.TextContentBlock>().Select(b => b.Text));
        var row = Assert.Single(await _data.GetMcpSlowCallsAsync(DateTime.UtcNow.AddHours(-1), 50, "get_query_store_top"));
        Assert.Equal("ok", row.Outcome);
        Assert.Equal(ServerName, row.ServerName);
        Assert.Equal(5, row.RowsReturned);
        Assert.Equal(System.Text.Encoding.UTF8.GetByteCount(reply), row.ResponseBytes);
        Assert.NotNull(row.LockWaitMs);
        Assert.NotNull(row.HotMs);
        Assert.NotNull(row.ArchiveMs);
        Assert.True(row.ArchiveFiles >= 1, $"archive_files was {row.ArchiveFiles}");
        Assert.True(row.HotMs + row.ArchiveMs <= row.TotalMs + 1);
    }

    private async Task ExecuteAsync(params string[] statements)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        foreach (var sql in statements)
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }
}
