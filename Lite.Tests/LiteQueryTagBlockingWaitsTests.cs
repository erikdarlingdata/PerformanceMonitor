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
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #7163: the blocking_reports section of the query-tag roll-up counts WAITS. SQL Server writes a blocked-process report every threshold
/// interval, each carrying the wait so far, so one 60 s wait at a 5 s threshold is twelve reports. The section reads the longest report
/// of each wait (<c>BlockingWaits.WaitsCteSql</c>) and leaves out a parallel query's self-wait, as every other blocking surface does.
/// </summary>
public sealed class LiteQueryTagBlockingWaitsTests : IDisposable
{
    private const string ServerName = "BlockingWaitsServer";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;

    public LiteQueryTagBlockingWaitsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "QueryTagBlockingWaits_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "waits.duckdb"));
        _duckDb.InitializeAsync().GetAwaiter().GetResult();
        _service = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(_tempDir);
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private async Task ReportAsync(long id, DateTime eventTime, long waitMs, int blockedSpid, int blockingSpid)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, wait_time_ms, blocked_spid, blocking_spid, database_name) "
            + "VALUES ($1, $2, $2, $3, 'x', $4, $5, $6, 'db1')";
        foreach (var v in new object[] { id, eventTime, _serverId, waitMs, blockedSpid, blockingSpid })
        {
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task SnapshotAsync(long id, long waitMs, int blockedSpid, int blockingSpid)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, event_time, server_id, server_name, database_name, monitor_loop, wait_time_ms, blocked_spid, blocking_spid) "
            + "VALUES ($1, $2, $2, $3, 'x', 'db1', 1, $4, $5, $6)";
        foreach (var v in new object[] { id, DateTime.UtcNow.AddMinutes(-20), _serverId, waitMs, blockedSpid, blockingSpid })
        {
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task BlockingReportsOfOneWait_CountOnce_AtTheLongestReport_AndSelfWaitsAddNothing()
    {
        var start = DateTime.UtcNow.AddMinutes(-30);
        long id = 1;
        for (var k = 1; k <= 12; k++)
        {
            await ReportAsync(id++, start.AddSeconds(5 * k), 5_000L * k, 70, 60);
        }

        /* A second, separate wait of 8 s (two reports), and a self-wait the way a parallel query writes it. */
        var second = start.AddMinutes(10);
        await ReportAsync(id++, second.AddSeconds(5), 5_000, 71, 60);
        await ReportAsync(id++, second.AddSeconds(8), 8_000, 71, 60);
        await ReportAsync(id++, second.AddSeconds(5), 5_000, 90, 90);

        /* The always-on DMV sample: one row per sample, so an ordinary row counts and the self-wait row adds nothing. */
        await SnapshotAsync(id++, 3_000, 70, 60);
        await SnapshotAsync(id++, 9_000, 90, 90);

        var json = await McpQueryTagUsageTools.GetResourceUseByTag(_service, _serverManager, ServerName, 24);
        using var doc = JsonDocument.Parse(json);
        var blocking = doc.RootElement.GetProperty("sections").EnumerateArray().Single(s => s.GetProperty("section").GetString() == "blocking_reports");
        var total = blocking.GetProperty("total");

        Assert.Equal(68_000, total.GetProperty("blocked_ms").GetDouble());
        Assert.Equal(68_000, total.GetProperty("caused_ms").GetDouble());
        Assert.Equal(2, total.GetProperty("blocked_reports").GetDouble());
        Assert.Equal(2, total.GetProperty("caused_reports").GetDouble());

        var snapshots = doc.RootElement.GetProperty("sections").EnumerateArray().Single(s => s.GetProperty("section").GetString() == "blocking_snapshots").GetProperty("total");
        Assert.Equal(3_000, snapshots.GetProperty("blocked_ms").GetDouble());
        Assert.Equal(3_000, snapshots.GetProperty("caused_ms").GetDouble());
        Assert.Equal(1, snapshots.GetProperty("blocked_snapshots").GetDouble());
        Assert.Equal(1, snapshots.GetProperty("caused_snapshots").GetDouble());
    }
}
