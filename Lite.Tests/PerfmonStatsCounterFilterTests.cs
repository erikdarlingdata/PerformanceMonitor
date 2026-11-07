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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7903 item 1: <c>get_perfmon_stats</c>'s <c>counter_name</c> and <c>instance_name</c> match the whole name first,
/// ignoring case, and fall back to a substring match only when no name equals the filter. A caller narrowing to
/// 'Transactions/sec' and summing the page gets that one counter, not Write Transactions/sec and Tracked
/// transactions/sec beside it; a partial name still finds its counters. Darling's twin is
/// <c>PerfmonStatsCounterFilterLiveTests</c>.
/// </summary>
public sealed class PerfmonStatsCounterFilterTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "PerfmonFilterSrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public PerfmonStatsCounterFilterTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-perfmon-filter-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, IsEnabled = true };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    private static string[] CounterNames(JsonDocument doc) =>
        doc.RootElement.GetProperty("counters").EnumerateArray().Select(c => c.GetProperty("counter_name").GetString()!).OrderBy(n => n, StringComparer.Ordinal).ToArray();

    [Fact]
    public async Task CounterName_MatchesTheWholeNameFirst_AndFallsBackToSubstringOnlyWhenNothingEqualsIt()
    {
        var service = new LocalDataService(_duckDb);
        var at = DateTime.UtcNow.AddMinutes(-2);
        await SeedPerfmonAsync(at, "SQLServer:Databases", "Transactions/sec", "UserDb1", 1000);
        await SeedPerfmonAsync(at, "SQLServer:Databases", "Write Transactions/sec", "UserDb1", 400);
        await SeedPerfmonAsync(at, "SQLServer:Databases", "Tracked transactions/sec", "UserDb1", 7);
        await SeedPerfmonAsync(at, "SQLServer:Databases", "Transactions/sec", "UserDb10", 2000);

        /* The exact name, in the stored case and in another: that counter only, on both instances. */
        using (var exact = JsonDocument.Parse(await McpPerfmonTools.GetPerfmonStats(service, _serverManager, ServerName, counter_name: "Transactions/sec")))
        {
            Assert.Equal(new[] { "Transactions/sec", "Transactions/sec" }, CounterNames(exact));
        }
        using (var exactOtherCase = JsonDocument.Parse(await McpPerfmonTools.GetPerfmonStats(service, _serverManager, ServerName, counter_name: "transactions/SEC")))
        {
            Assert.Equal(new[] { "Transactions/sec", "Transactions/sec" }, CounterNames(exactOtherCase));
        }

        /* A partial name equals nothing, so it is a substring match: every counter carrying it. */
        using (var partial = JsonDocument.Parse(await McpPerfmonTools.GetPerfmonStats(service, _serverManager, ServerName, counter_name: "transactions")))
        {
            Assert.Equal(4, partial.RootElement.GetProperty("counters").GetArrayLength());
        }

        /* instance_name follows the same rule: UserDb1 is one instance, not UserDb1 and UserDb10. */
        using (var instance = JsonDocument.Parse(await McpPerfmonTools.GetPerfmonStats(service, _serverManager, ServerName, counter_name: "Transactions/sec", instance_name: "UserDb1")))
        {
            var rows = instance.RootElement.GetProperty("counters").EnumerateArray().ToList();
            Assert.Single(rows);
            Assert.Equal("UserDb1", rows[0].GetProperty("instance_name").GetString());
        }
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

    private async Task SeedPerfmonAsync(DateTime timeUtc, string objectName, string counter, string instance, long value)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO perfmon_stats
    (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name,
     cntr_value, delta_cntr_value, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(timeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = objectName });
        cmd.Parameters.Add(new DuckDBParameter { Value = counter });
        cmd.Parameters.Add(new DuckDBParameter { Value = instance });
        cmd.Parameters.Add(new DuckDBParameter { Value = value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 60L });
        await cmd.ExecuteNonQueryAsync();
    }
}
