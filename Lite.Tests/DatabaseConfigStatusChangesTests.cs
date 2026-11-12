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

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7416 item 2: <c>log_reuse_wait_desc</c> is one of the 27 <c>database_config</c> columns, but it is the engine's live reason the
/// log cannot truncate (NOTHING, ACTIVE_TRANSACTION, LOG_BACKUP) and flips with no operator action. Change Watch and the analysis
/// engine already drop it. The change grid must list settings only, and <c>get_database_config_changes</c> reports those rows apart
/// as <c>status_changes</c> so the history is not lost. Darling twin: <c>DarlingMcpConfigHistoryStatusChangesLiveTests</c>.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class DatabaseConfigStatusChangesTests : IDisposable
{
    private const string ServerName = "StatusChangeServer";
    private static readonly DateTime Anchor = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);

    private readonly int _serverId;
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly PendingSeedSession _seed;
    private readonly ServerManager _serverManager;
    private long _nextId = 1;

    public DatabaseConfigStatusChangesTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "DbConfigStatus_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(_tempDir, "config"));
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _seed = new PendingSeedSession(_duckDb);

        _serverManager = new ServerManager(Path.Combine(_tempDir, "config"));
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seed.Dispose();
        _duckDb.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private Task SeedCaptureAsync(DateTime at, string recoveryModel, string logReuse) => _seed.ExecuteAsync(@"
INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, recovery_model, log_reuse_wait_desc)
VALUES ($1, $2, $3, $4, 'Sales', $5, $6)",
        new object?[] { _nextId++, DateTime.SpecifyKind(at, DateTimeKind.Unspecified), _serverId, ServerName, recoveryModel, logReuse });

    private async Task<JsonElement> CallToolAsync()
    {
        _seed.Flush();
        var json = await McpConfigHistoryTools.GetDatabaseConfigChanges(
            new LocalDataService(_duckDb), _serverManager, ServerName, 168, "2026-09-10T12:00:00Z");
        return JsonDocument.Parse(json).RootElement;
    }

    [Fact]
    public async Task TheGrid_ListsSettingsOnly_AndTheToolReportsTheStatusRowsApart()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await SeedCaptureAsync(Anchor.AddHours(-30), "FULL", "NOTHING");
        await SeedCaptureAsync(Anchor.AddHours(-20), "FULL", "ACTIVE_TRANSACTION");
        await SeedCaptureAsync(Anchor.AddHours(-10), "SIMPLE", "ACTIVE_TRANSACTION");
        _seed.Flush();

        var grid = await new LocalDataService(_duckDb).GetDatabaseConfigChangesAsync(_serverId, 168, asOfUtc: Anchor);
        Assert.Equal(new[] { "recovery_model" }, grid.Select(r => r.SettingName));

        var root = await CallToolAsync();
        Assert.Equal(1, root.GetProperty("change_count").GetInt32());
        Assert.Equal("recovery_model", root.GetProperty("changes")[0].GetProperty("setting_name").GetString());
        Assert.Equal(1, root.GetProperty("status_change_count").GetInt32());
        var status = root.GetProperty("status_changes")[0];
        Assert.Equal("log_reuse_wait_desc", status.GetProperty("setting_name").GetString());
        Assert.Equal("NOTHING", status.GetProperty("old_value").GetString());
        Assert.Equal("ACTIVE_TRANSACTION", status.GetProperty("new_value").GetString());
    }

    [Fact]
    public async Task WhenOnlyTheStatusColumnMoved_TheGridIsEmpty_AndTheToolKeepsTheHistory()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await SeedCaptureAsync(Anchor.AddHours(-30), "FULL", "NOTHING");
        await SeedCaptureAsync(Anchor.AddHours(-20), "FULL", "ACTIVE_TRANSACTION");
        await SeedCaptureAsync(Anchor.AddHours(-10), "FULL", "NOTHING");
        _seed.Flush();

        Assert.Empty(await new LocalDataService(_duckDb).GetDatabaseConfigChangesAsync(_serverId, 168, asOfUtc: Anchor));

        var root = await CallToolAsync();
        Assert.Equal(0, root.GetProperty("change_count").GetInt32());
        Assert.Equal(0, root.GetProperty("changes").GetArrayLength());
        Assert.Equal(2, root.GetProperty("status_change_count").GetInt32());
        Assert.All(root.GetProperty("status_changes").EnumerateArray(),
            row => Assert.Equal("log_reuse_wait_desc", row.GetProperty("setting_name").GetString()));
    }
}
