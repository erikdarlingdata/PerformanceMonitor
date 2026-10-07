/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
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
/// <c>get_object_locking</c> carries <c>optimized_locking_note</c> (the shared <see cref="OptimizedLockingNote.Text"/>)
/// only when the newest stored database-config snapshot has <c>is_optimized_locking_on</c> true for some database;
/// a false or unknown flag gives JSON null. Darling's twin is <c>OptimizedLockingNoteLivePostgresTests</c>.
/// </summary>
public sealed class OptimizedLockingNoteTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "OptimizedLockingNoteSrv";

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = -1;

    public OptimizedLockingNoteTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-optlock-note-" + Guid.NewGuid().ToString("N"));
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

    [Fact]
    public async Task TheNote_RidesOnlyWhenTheNewestSnapshotHasTheFlagOnForSomeDatabase()
    {
        var t = DateTime.UtcNow.AddMinutes(-30);
        await SeedContendedIndexAsync(t);

        await SeedConfigAsync(t, "Sales", null);
        await SeedConfigAsync(t, "Billing", null);
        Assert.Null(await NoteAsync());

        await SeedConfigAsync(t.AddMinutes(5), "Sales", false);
        await SeedConfigAsync(t.AddMinutes(5), "Billing", true);
        Assert.Equal(OptimizedLockingNote.Text, await NoteAsync());

        await SeedConfigAsync(t.AddMinutes(10), "Sales", false);
        await SeedConfigAsync(t.AddMinutes(10), "Billing", false);
        Assert.Null(await NoteAsync());
    }

    /// <summary>
    /// #5372 M2: with a database chosen, only that database's flag counts (Darling's twin does the same), so the call never
    /// carries a note about a database it does not show, and the `empty` status for a database with no contention carries
    /// the note when THAT database has the flag on.
    /// </summary>
    [Fact]
    public async Task WithADatabaseChosen_TheNoteFollowsThatDatabasesFlagOnly()
    {
        var t = DateTime.UtcNow.AddMinutes(-30);
        await SeedContendedIndexAsync(t);
        await SeedConfigAsync(t, "Sales", false);
        await SeedConfigAsync(t, "Billing", true);

        using (var sales = JsonDocument.Parse(await McpObjectStatsTools.GetObjectLocking(_dataService, _serverManager, ServerName, database_name: "Sales")))
            Assert.Equal(JsonValueKind.Null, sales.RootElement.GetProperty("optimized_locking_note").ValueKind);

        using (var all = JsonDocument.Parse(await McpObjectStatsTools.GetObjectLocking(_dataService, _serverManager, ServerName)))
            Assert.Equal(OptimizedLockingNote.Text, all.RootElement.GetProperty("optimized_locking_note").GetString());

        using var billing = JsonDocument.Parse(await McpObjectStatsTools.GetObjectLocking(_dataService, _serverManager, ServerName, database_name: "Billing"));
        Assert.Equal("empty", billing.RootElement.GetProperty("status").GetString());
        Assert.Equal(OptimizedLockingNote.Text,
            billing.RootElement.GetProperty("hints").GetProperty("optimized_locking_note").GetString());
    }

    [Fact]
    public void OptimizedLockingDisplay_NullIsUnknown_NotNo()
    {
        Assert.Equal("Unknown", new DatabaseConfigRow { IsOptimizedLockingOn = null }.OptimizedLockingDisplay);
        Assert.Equal("Yes", new DatabaseConfigRow { IsOptimizedLockingOn = true }.OptimizedLockingDisplay);
        Assert.Equal("No", new DatabaseConfigRow { IsOptimizedLockingOn = false }.OptimizedLockingDisplay);
    }

    [Fact]
    public async Task GetDatabaseConfig_UnknownFlag_IsJsonNull_AndEmptyLockingCarriesTheNote()
    {
        var t = DateTime.UtcNow.AddMinutes(-30);
        await SeedConfigAsync(t, "Sales", null);

        using (var doc = JsonDocument.Parse(await McpConfigTools.GetDatabaseConfig(_dataService, _serverManager, ServerName)))
        {
            var text = doc.RootElement.GetRawText();
            Assert.Contains("\"optimized_locking\":null", text.Replace(" ", ""), StringComparison.Ordinal);
        }

        /* No contended index rows, flag on: the empty status carries the note. */
        await SeedConfigAsync(t.AddMinutes(5), "Sales", true);
        using var empty = JsonDocument.Parse(await McpObjectStatsTools.GetObjectLocking(_dataService, _serverManager, ServerName));
        Assert.Equal("unavailable", empty.RootElement.GetProperty("status").GetString());
        Assert.Equal(OptimizedLockingNote.Text,
            empty.RootElement.GetProperty("hints").GetProperty("optimized_locking_note").GetString());
    }

    private async Task<string?> NoteAsync()
    {
        var json = await McpObjectStatsTools.GetObjectLocking(_dataService, _serverManager, ServerName);
        Assert.False(McpHelpers.IsErrorEnvelope(json), $"tool returned an error: {json}");
        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("optimized_locking_note", out var note),
            "the payload has no optimized_locking_note field");
        Assert.Equal("Complete: every index with lock/latch contention at the latest snapshot is included.",
            doc.RootElement.GetProperty("note").GetString());
        return note.ValueKind == JsonValueKind.Null ? null : note.GetString();
    }

    private async Task SeedContendedIndexAsync(DateTime capture)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO index_object_stats
            (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
             schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
             row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms,
             index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms)
            VALUES ($1,$2,$3,$4,$5,'Sales',7,'dbo',1000,'Orders',1,'PK_Orders','CLUSTERED',100,100,10000,5,500,1,100,0,10,10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = capture });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = capture.AddDays(-10) });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedConfigAsync(DateTime capture, string database, bool? optimizedLockingOn)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO database_config
            (config_id, capture_time, server_id, server_name, database_name, state_desc, compatibility_level, is_optimized_locking_on)
            VALUES ($1,$2,$3,$4,$5,'ONLINE',170,$6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = capture });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)optimizedLockingOn ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
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
}
