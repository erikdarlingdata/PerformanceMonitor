/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's get_blocking_trend / get_deadlock_trend (#2485), twins of Darling's. Both used to serialize
/// <c>{ server, hours_back, trend }</c> unconditionally, so a server that never blocked and a server that never
/// collected produced the SAME bytes — and an MCP client, which has only the JSON, can reasonably read the
/// first as "there is no data" when the true answer is "no, and that is good news".
///
/// <para>The denominator is what makes the empty answer honest, and it cannot come from the read itself: these
/// are EDGE tables, where an absent capture and a capture that found nothing are both an absence of rows. It
/// comes from <c>collection_log</c>, which records a SUCCESS with zero rows for a collector that ran and stored
/// nothing.</para>
///
/// <para>The messages are Darling's word for word. A user moving between the SKUs must not be told a different
/// story about the same state — the parity half of #2485.</para>
/// </summary>
public sealed class BlockingTrendEmptyToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "BlockingTrendSrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 810000;

    public BlockingTrendEmptyToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-blocktrend-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);

        /* Derived, not stored -- seeding under a hardcoded id would write rows the tool looks past. */
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task NeverCollected_IsNotReportedAsAnAllClear()
    {
        var service = new LocalDataService(_duckDb);

        var root = JsonDocument.Parse(
            await McpBlockingTools.GetBlockingTrend(service, _serverManager, ServerName, 4)).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("NOT an all-clear", text, StringComparison.Ordinal);
        Assert.Contains("EVER", text, StringComparison.Ordinal);
        Assert.Equal(0, root.GetProperty("hints").GetProperty("capture_count").GetInt64());
    }

    [Fact]
    public async Task CollectedBeforeButNotInTheWindow_IsAGap_NotANeverCollected()
    {
        var service = new LocalDataService(_duckDb);
        await SeedRunAsync("blocked_process_report", DateTime.UtcNow.AddHours(-48));

        var root = JsonDocument.Parse(
            await McpBlockingTools.GetBlockingTrend(service, _serverManager, ServerName, 1)).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("NOT an all-clear", text, StringComparison.Ordinal);
        Assert.Contains("widen hours_back", text, StringComparison.Ordinal);

        /* Same status as the never-collected case and it must not reach for the same word. */
        Assert.DoesNotContain("EVER", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CapturesRanAndSawNothing_IsAGenuineAllClear_AndSaysSoDifferently()
    {
        var service = new LocalDataService(_duckDb);
        await SeedRunAsync("blocked_process_report", DateTime.UtcNow.AddMinutes(-10));
        await SeedRunAsync("dmv_blocking_snapshot", DateTime.UtcNow.AddMinutes(-10));

        var root = JsonDocument.Parse(
            await McpBlockingTools.GetBlockingTrend(service, _serverManager, ServerName, 4)).RootElement;

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("genuine all-clear", text, StringComparison.Ordinal);

        /* Same zero rows as both other cases, and it must share wording with neither. */
        Assert.DoesNotContain("EVER", text, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT an all-clear", text, StringComparison.Ordinal);

        /* "No blocking" means something different across two captures than across two hundred, and the
           caller cannot supply that number itself. */
        var hints = root.GetProperty("hints");
        Assert.Equal(2, hints.GetProperty("capture_count").GetInt64());
        Assert.Equal(2, hints.GetProperty("captures").GetArrayLength());
    }

    /// <summary>
    /// The two-capture-path guard. An RDS instance cannot run the blocked-process-report XE session, so its
    /// blocking arrives entirely through the DMV snapshot the trend falls back to — and a probe counting only
    /// <c>blocked_process_report</c> runs would tell that server its blocking has never been captured.
    /// </summary>
    [Fact]
    public async Task DmvOnlyCapture_StillCountsAsHavingLooked()
    {
        var service = new LocalDataService(_duckDb);
        await SeedRunAsync("dmv_blocking_snapshot", DateTime.UtcNow.AddMinutes(-10));

        var root = JsonDocument.Parse(
            await McpBlockingTools.GetBlockingTrend(service, _serverManager, ServerName, 4)).RootElement;

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Contains("genuine all-clear", root.GetProperty("message").GetString()!, StringComparison.Ordinal);
        Assert.Equal(1, root.GetProperty("hints").GetProperty("capture_count").GetInt64());
    }

    /// <summary>
    /// A blocking capture is NOT a deadlock capture. Without this the deadlock probe would be satisfied by the
    /// neighbouring collector's runs, and a server with the deadlocks collector switched off would be told its
    /// empty deadlock trend is an all-clear.
    /// </summary>
    [Fact]
    public async Task DeadlockTrend_IsNotSatisfiedByABlockingCapture()
    {
        var service = new LocalDataService(_duckDb);
        await SeedRunAsync("blocked_process_report", DateTime.UtcNow.AddMinutes(-10));

        var root = JsonDocument.Parse(
            await McpBlockingTools.GetDeadlockTrend(service, _serverManager, ServerName, 4)).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        Assert.Contains("EVER", root.GetProperty("message").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DeadlockTrend_WithItsOwnCapture_IsAGenuineAllClear()
    {
        var service = new LocalDataService(_duckDb);
        await SeedRunAsync("deadlocks", DateTime.UtcNow.AddMinutes(-10));

        var root = JsonDocument.Parse(
            await McpBlockingTools.GetDeadlockTrend(service, _serverManager, ServerName, 4)).RootElement;

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("genuine all-clear", text, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", text, StringComparison.Ordinal);
        Assert.Equal(1, root.GetProperty("hints").GetProperty("capture_count").GetInt64());
    }

    /// <summary>
    /// A run that FAILED is not a run that looked. Counting a PERMISSIONS row would manufacture an all-clear
    /// out of a collector that never saw the window — the failure this whole change exists to prevent, arrived
    /// at from the other direction.
    /// </summary>
    [Fact]
    public async Task AFailedRunDoesNotCountAsHavingLooked()
    {
        var service = new LocalDataService(_duckDb);
        await SeedRunAsync("blocked_process_report", DateTime.UtcNow.AddMinutes(-10), status: "PERMISSIONS");

        var root = JsonDocument.Parse(
            await McpBlockingTools.GetBlockingTrend(service, _serverManager, ServerName, 4)).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        Assert.Contains("EVER", root.GetProperty("message").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARealEventStillReturnsTheTrendPayload()
    {
        var service = new LocalDataService(_duckDb);
        await SeedRunAsync("blocked_process_report", DateTime.UtcNow.AddMinutes(-10));
        await SeedBlockedProcessReportAsync(DateTime.UtcNow.AddMinutes(-5));

        var root = JsonDocument.Parse(
            await McpBlockingTools.GetBlockingTrend(service, _serverManager, ServerName, 4)).RootElement;

        Assert.False(root.TryGetProperty("status", out _));
        Assert.Equal(1, root.GetProperty("trend").GetArrayLength());
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    /// <summary>A collector run that SUCCEEDED and stored nothing — the row that makes an empty trend
    /// interpretable, and the one the edge tables can never produce.</summary>
    private async Task SeedRunAsync(string collector, DateTime collectionTimeUtc, string status = "SUCCESS")
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = 100 });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 80 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 20 });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedBlockedProcessReportAsync(DateTime eventTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text,
     blocked_process_report_xml, contentious_object)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)";
        var naive = DateTime.SpecifyKind(eventTimeUtc, DateTimeKind.Unspecified);
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = naive });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = naive });
        cmd.Parameters.Add(new DuckDBParameter { Value = "AppDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = 55 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 60 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 8000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "X" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT 1" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "UPDATE Orders SET Total = 1" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "<blocked-process-report/>" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "dbo.Orders" });
        await cmd.ExecuteNonQueryAsync();
    }
}
