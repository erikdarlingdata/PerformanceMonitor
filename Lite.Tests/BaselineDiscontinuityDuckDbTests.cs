/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3653 A5: the DuckDB execution of the shared discontinuity read, against a real Lite store — the two SQL
/// texts <see cref="BaselineDiscontinuities"/> spells once for both engines run here on DuckDB (their
/// PostgreSQL execution is <c>DarlingMcpTrendToolsLivePostgresTests</c>' arm), the window bounds both sides, the
/// persisted pair reaches the reason word, and a Lite trend TOOL carries the block as its trailing key with an
/// empty array on a continuous window. Seeded the way the carriers write: a <c>collection_log</c> row per carrier
/// run whose <c>error_message</c> is the #3161 measurement note, and the pair under the carrier's name in
/// <c>collector_state</c>.
/// </summary>
public sealed class BaselineDiscontinuityDuckDbTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "DiscontinuitySrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 910000;

    public BaselineDiscontinuityDuckDbTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-discontinuity-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    /// <summary>The marker read as it was before it named the collectors (#8570): no collector filter.</summary>
    private const string ReferenceMarkerRowsSql =
        "SELECT collection_time, collector_name, error_message FROM collection_log "
        + "WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3 AND error_message IS NOT NULL "
        + "AND (error_message LIKE '%" + ServerEpoch.IdentityChangesMeasurement + "=%' "
        + "OR error_message LIKE '%" + ServerEpoch.StatementsChangesMeasurement + "=%' "
        + "OR error_message LIKE '%" + ServerEpoch.PostmasterChangesMeasurement + "=%') "
        + "ORDER BY collection_time, collector_name";

    private async Task<List<(DateTime At, string Collector, string? Note)>> ReadMarkerRowsAsync(string sql, int serverId, DateTime from, DateTime to)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = from });
        cmd.Parameters.Add(new DuckDBParameter { Value = to });
        var rows = new List<(DateTime, string, string?)>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetDateTime(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
        }

        return rows;
    }

    [Fact]
    public async Task MarkerRead_ReturnsWhatTheReadBeforeItReturned_ForEveryWriter()
    {
        var day = new DateTime(2026, 1, 10, 0, 0, 0, DateTimeKind.Unspecified);
        var otherServerId = _serverId + 1;

        /* Noise on two servers: a row per collector per minute for three hours, most notes NULL and one in thirty-seven a short note. */
        using (var readLock = _duckDb.AcquireReadLock())
        {
            var connection = await SeedConnectionAsync();
            foreach (var id in new[] { _serverId, otherServerId })
            {
                using var noise = connection.CreateCommand();
                noise.CommandText = $@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected)
SELECT {id}::BIGINT * 1000000 + c.idx * 100000 + r.m, {id}, 'noise', c.name, TIMESTAMP '2026-01-10' + r.m * INTERVAL 1 MINUTE, 50, 'SUCCESS',
       CASE WHEN c.name = '{WaitStatsCollector.Instance.Name}' AND r.m % 13 = 0 THEN 'the {ServerEpoch.IdentityChangesMeasurement} marker is prose here'
            WHEN c.name = '{PgStatementStatsCollector.Instance.Name}' AND r.m % 11 = 0 THEN '{ServerEpoch.StatementsDeallocMeasurement}=3'
            WHEN r.m % 37 = 0 THEN 'short note ' || r.m END, 10
FROM (VALUES (1, '{CpuUtilizationCollector.Instance.Name}'), (2, '{WaitStatsCollector.Instance.Name}'),
             (3, '{PgStatementStatsCollector.Instance.Name}'), (4, '{PgWaitStatsCollector.Instance.Name}'),
             (5, 'query_stats'), (6, 'latch_stats')) AS c(idx, name),
     range(0, 180) AS r(m)";
                await noise.ExecuteNonQueryAsync();
            }
        }

        /* Markers from each of the four writers, on both servers, with a host note in front of some. */
        foreach (var id in new[] { _serverId, otherServerId })
        {
            await SeedLogAsync(CpuUtilizationCollector.Instance.Name, day.AddMinutes(30).AddSeconds(40), ServerEpoch.IdentityChangesMeasurement + "=1", id);
            await SeedLogAsync(WaitStatsCollector.Instance.Name, day.AddMinutes(30), ServerEpoch.IdentityChangesMeasurement + "=1", id);
            await SeedLogAsync(PgStatementStatsCollector.Instance.Name, day.AddMinutes(90), "wall-clock budget (120s) reached; " + ServerEpoch.StatementsChangesMeasurement + "=1", id);
            await SeedLogAsync(PgWaitStatsCollector.Instance.Name, day.AddMinutes(150), "cycle abandoned; " + ServerEpoch.PostmasterChangesMeasurement + "=1", id);
        }

        var end = day.AddHours(3);
        foreach (var (server, from, to, expected) in new[]
        {
            (_serverId, day, end, 4),
            (otherServerId, day, end, 4),
            (_serverId, day.AddMinutes(30), day.AddMinutes(30), 1),
            (_serverId, day.AddMinutes(100), day.AddMinutes(120), 0),
            (_serverId + 7, day, end, 0),
        })
        {
            var reference = await ReadMarkerRowsAsync(ReferenceMarkerRowsSql, server, from, to);
            var read = await ReadMarkerRowsAsync(BaselineDiscontinuities.MarkerRowsSql, server, from, to);
            Assert.Equal(expected, reference.Count);
            Assert.Equal(reference, read);
        }
    }

    [Fact]
    public async Task Reader_ReturnsTheWindowsMarkers_WithTheReasonFromThePersistedPair()
    {
        var service = new LocalDataService(_duckDb);
        var epoch = Truncate(DateTime.UtcNow.AddHours(-2));
        var oldStart = new DateTime(2026, 9, 1, 8, 0, 0);
        var newStart = new DateTime(2026, 9, 20, 3, 11, 40);

        /* Two carriers on the epoch pass (the CPU carrier runs tenth, here 40 s later), an ordinary quiet
           run between them, a marker outside the window, and a row whose note mentions the label in prose. */
        await SeedLogAsync("wait_stats", epoch, "identity_epoch_changes=1");
        await SeedLogAsync("latch_stats", epoch.AddSeconds(5), null);
        await SeedLogAsync("cpu_utilization", epoch.AddSeconds(40), "identity_epoch_changes=1");
        await SeedLogAsync("wait_stats", epoch.AddHours(-30), "identity_epoch_changes=1");
        await SeedLogAsync("pg_statement_stats", epoch.AddMinutes(20), "wall-clock budget (120s) reached; cycle abandoned; statements_epoch_changes=1");
        await SeedLogAsync("query_stats", epoch.AddMinutes(30), "the identity_epoch_changes marker is prose here");
        await SeedStateAsync("wait_stats", ServerEpoch.IdentityStateKey, ServerEpoch.Serialize(new(newStart, "NODE1")), epoch.AddSeconds(1));
        await SeedStateAsync("wait_stats", ServerEpoch.IdentityPreviousStateKey, ServerEpoch.Serialize(new(oldStart, "NODE1")), epoch.AddSeconds(1));
        await SeedStateAsync("cpu_utilization", ServerEpoch.IdentityStateKey, ServerEpoch.Serialize(new(newStart, "NODE1")), epoch.AddSeconds(41));
        await SeedStateAsync("cpu_utilization", ServerEpoch.IdentityPreviousStateKey, ServerEpoch.Serialize(new(oldStart, "NODE1")), epoch.AddSeconds(41));

        var result = await service.GetBaselineDiscontinuitiesAsync(_serverId, hoursBack: 24);

        Assert.Equal(2, result.Count);
        Assert.Equal(epoch.Ticks, result[0].At.Ticks);
        Assert.Equal(BaselineDiscontinuities.RestartReason, result[0].Reason);
        Assert.Contains("2026-09-01 08:00:00 → 2026-09-20 03:11:40", result[0].Detail, StringComparison.Ordinal);
        Assert.Contains("observed by wait_stats, cpu_utilization", result[0].Detail, StringComparison.Ordinal);
        Assert.Equal(epoch.AddMinutes(20).Ticks, result[1].At.Ticks);
        Assert.Equal(BaselineDiscontinuities.StatsResetReason, result[1].Reason);

        /* A window that ends before the epoch holds neither. */
        Assert.Empty(await service.GetBaselineDiscontinuitiesAsync(_serverId, hoursBack: 4, asOfUtc: epoch.AddMinutes(-5)));

        /* The window the 30-hour-old marker falls in reaches it, and that older event has no pair of its own. */
        var wide = await service.GetBaselineDiscontinuitiesAsync(_serverId, hoursBack: 48);
        Assert.Equal(3, wide.Count);
        Assert.Equal(BaselineDiscontinuities.IdentityReason, wide[0].Reason);
    }

    [Fact]
    public async Task ATrendTool_CarriesTheBlockAsItsTrailingKey_AndAnEmptyArrayOnAContinuousWindow()
    {
        var service = new LocalDataService(_duckDb);
        var now = DateTime.UtcNow;

        await SeedMemoryAsync(now.AddMinutes(-30));
        await SeedMemoryAsync(now.AddMinutes(-10));

        var continuous = await McpMemoryTools.GetMemoryTrend(service, _serverManager, ServerName, 4);
        using (var doc = JsonDocument.Parse(continuous))
        {
            var block = doc.RootElement.GetProperty(BaselineDiscontinuities.PayloadKey);
            Assert.Equal(JsonValueKind.Array, block.ValueKind);
            Assert.Equal(0, block.GetArrayLength());
            AssertTrailing(doc.RootElement);
        }

        await SeedLogAsync("wait_stats", Truncate(now.AddMinutes(-20)), "identity_epoch_changes=1");

        var marked = await McpMemoryTools.GetMemoryTrend(service, _serverManager, ServerName, 4);
        using (var doc = JsonDocument.Parse(marked))
        {
            var block = doc.RootElement.GetProperty(BaselineDiscontinuities.PayloadKey);
            var one = Assert.Single(block.EnumerateArray());
            Assert.Equal(BaselineDiscontinuities.IdentityReason, one.GetProperty("reason").GetString());
            Assert.DoesNotContain("Z", one.GetProperty("at").GetString(), StringComparison.Ordinal);
            Assert.Contains("observed by wait_stats", one.GetProperty("detail").GetString(), StringComparison.Ordinal);
            AssertTrailing(doc.RootElement);
        }
    }

    private static void AssertTrailing(JsonElement root)
    {
        string? last = null;
        foreach (var property in root.EnumerateObject())
        {
            last = property.Name;
        }

        Assert.Equal(BaselineDiscontinuities.PayloadKey, last);
    }

    private static DateTime Truncate(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private async Task SeedLogAsync(string collector, DateTime collectionTimeUtc, string? note, int? serverId = null)
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
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId ?? _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = 100 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SUCCESS" });
        cmd.Parameters.Add(new DuckDBParameter { Value = note is null ? DBNull.Value : note });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 80 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 20 });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedStateAsync(string collector, string key, string value, DateTime updatedAtUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT OR REPLACE INTO collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = key });
        cmd.Parameters.Add(new DuckDBParameter { Value = value });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(updatedAtUtc, DateTimeKind.Unspecified) });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedMemoryAsync(DateTime collectionTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb, plan_cache_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = 65536.0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 8192.0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 49152.0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 40000.0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 35000.0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5000.0 });
        await cmd.ExecuteNonQueryAsync();
    }
}
