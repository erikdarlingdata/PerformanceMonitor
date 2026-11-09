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
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's get_table_index_sizes names the snapshot each nominal-window baseline was read from in its history block
/// (<c>baseline_7d_snapshot</c>, <c>baseline_30d_snapshot</c>), null when the store holds no snapshot that old. With a
/// snapshot every day the two stamps are a week and a month before the latest snapshot, so two figures that agree are
/// visibly two measurements; a store with a gap names the one snapshot both cutoffs resolved to; a store younger than
/// the windows carries both keys as null. Darling's twin is pinned by its live test.
/// </summary>
public sealed class ObjectSizeGrowthBaselineSnapshotTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "GrowthBaselineSnapshotSrv";
    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public ObjectSizeGrowthBaselineSnapshotTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-growth-baseline-snapshot-" + Guid.NewGuid().ToString("N"));
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
    public async Task DailySnapshots_NameTwoDifferentBaselineSnapshots_AWeekAndAMonthBeforeTheLatest()
    {
        var latest = Truncate(DateTime.UtcNow.AddHours(-2));
        foreach (var age in Enumerable.Range(0, 35))
            await SeedTableAsync("Hist", "TableA", latest.AddDays(-age), 100m + 3m * (34 - age));

        using var doc = JsonDocument.Parse(await McpObjectStatsTools.GetTableIndexSizes(_dataService, _serverManager, ServerName));
        var history = doc.RootElement.GetProperty("history");
        Assert.True(history.GetProperty("covers_7d").GetBoolean());
        Assert.True(history.GetProperty("covers_30d").GetBoolean());
        Assert.Equal(latest.AddDays(-7).ToString("o"), BaselineSnapshot(history, "baseline_7d_snapshot"));
        Assert.Equal(latest.AddDays(-30).ToString("o"), BaselineSnapshot(history, "baseline_30d_snapshot"));

        var a = Assert.Single(doc.RootElement.GetProperty("tables").EnumerateArray());
        Assert.Equal(21, a.GetProperty("growth_7d_mb").GetDecimal());
        Assert.Equal(90, a.GetProperty("growth_30d_mb").GetDecimal());
    }

    [Fact]
    public async Task AGapBetweenTheRecentSnapshotsAndTheRest_NamesTheOneSnapshotBothCutoffsResolvedTo()
    {
        /* The last five days and from 30 days back; nothing between, so both cutoffs resolve to the 30-day-old snapshot. */
        var latest = Truncate(DateTime.UtcNow.AddHours(-2));
        foreach (var age in Enumerable.Range(0, 35).Where(age => age < 5 || age >= 30))
            await SeedTableAsync("Hist", "TableA", latest.AddDays(-age), 100m + 3m * (34 - age));

        using var doc = JsonDocument.Parse(await McpObjectStatsTools.GetTableIndexSizes(_dataService, _serverManager, ServerName));
        var history = doc.RootElement.GetProperty("history");
        Assert.False(history.GetProperty("covers_7d").GetBoolean());
        Assert.True(history.GetProperty("covers_30d").GetBoolean());
        Assert.Equal(latest.AddDays(-30).ToString("o"), BaselineSnapshot(history, "baseline_7d_snapshot"));
        Assert.Equal(latest.AddDays(-30).ToString("o"), BaselineSnapshot(history, "baseline_30d_snapshot"));
        Assert.Contains(latest.AddDays(-30).ToString("o"), history.GetProperty("note").GetString(), StringComparison.Ordinal);

        var a = Assert.Single(doc.RootElement.GetProperty("tables").EnumerateArray());
        Assert.Equal(JsonValueKind.Null, a.GetProperty("growth_7d_mb").ValueKind);
        Assert.Equal(90, a.GetProperty("growth_30d_mb").GetDecimal());
    }

    [Fact]
    public async Task AStoreYoungerThanTheWindows_CarriesBothBaselineSnapshotsAsNull()
    {
        var latest = Truncate(DateTime.UtcNow.AddHours(-2));
        foreach (var age in Enumerable.Range(0, 3))
            await SeedTableAsync("Hist", "TableA", latest.AddDays(-age), 100m + 3m * (2 - age));

        using var doc = JsonDocument.Parse(await McpObjectStatsTools.GetTableIndexSizes(_dataService, _serverManager, ServerName));
        var history = doc.RootElement.GetProperty("history");
        Assert.False(history.GetProperty("covers_7d").GetBoolean());
        Assert.False(history.GetProperty("covers_30d").GetBoolean());
        Assert.Null(BaselineSnapshot(history, "baseline_7d_snapshot"));
        Assert.Null(BaselineSnapshot(history, "baseline_30d_snapshot"));
    }

    /// <summary>The named baseline snapshot stamp from the history block, which must carry the key even when its value is null.</summary>
    private static string? BaselineSnapshot(JsonElement history, string key)
    {
        Assert.True(history.TryGetProperty(key, out var value), $"history has no {key}");
        return value.ValueKind == JsonValueKind.Null ? null : value.GetString();
    }

    private static DateTime Truncate(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    private async Task ExecAsync(string sql, params object[] args)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var a in args)
            cmd.Parameters.Add(new DuckDBParameter { Value = a });
        await cmd.ExecuteNonQueryAsync();
    }

    private Task SeedTableAsync(string databaseName, string tableName, DateTime collectionTime, decimal reservedMb) =>
        ExecAsync(@"
INSERT INTO index_object_stats
    (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
     schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
     user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_in_ms, index_lock_promotion_count)
VALUES ($1, $2, $3, $4, $5, $6, 7, 'dbo', 100, $7, 1, 'IX', 'NONCLUSTERED', $8, $8, 10, 0, 0, 0, 0, 0, 0)",
            _nextId++, collectionTime, _serverId, ServerName, Truncate(DateTime.UtcNow.AddDays(-60)), databaseName, tableName, reservedMb);
}
