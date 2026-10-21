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
/// #6254: Lite's collection health and collection log show where the Query Store collector's newest run read its rows from
/// (<c>read_source</c>, DuckDB v78), and only when it has one. With NULL on every run, which is every server until an operator
/// sets the Query Store replica, the MCP replies, the health row and its Note column are exactly what they were.
/// </summary>
public sealed class CollectionHealthReadSourceTests : IDisposable
{
    private const string Replica = "replica 12, primary 2";

    private readonly string _tempDir;
    private readonly string _dbPath;
    private long _nextId = 1;
    private DuckDbInitializer? _initializer;

    public CollectionHealthReadSourceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        _initializer?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* Best-effort cleanup */ }
    }

    private async Task RunAsync(ServerConnection server, string collector, TimeSpan age, string? readSource)
    {
        var serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected, sql_duration_ms, duckdb_duration_ms, read_source)
                            VALUES ($1, $2, $3, $4, $5, 100, 'SUCCESS', 5, 80, 20, $6)";
        void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        P(_nextId++); P(serverId); P(server.ServerName); P(collector); P(DateTime.UtcNow - age); P(readSource);
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<(LocalDataService Service, ServerManager Servers, ServerConnection Server)> StoreAsync(string name)
    {
        var initializer = _initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        var servers = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = name, DisplayName = name };
        servers.AddServer(server);
        return (new LocalDataService(initializer), servers, server);
    }

    private static JsonElement CollectorRow(JsonElement health, string collector) =>
        health.GetProperty("collectors").EnumerateArray().Single(r => r.GetProperty("collector").GetString() == collector);

    /// <summary>The newest Query Store run's source is the one shown, on that row only, in the tool, the row and its Note.</summary>
    [Fact]
    public async Task Health_ShowsTheNewestQueryStoreRunsReadSource_OnThatRowOnly()
    {
        var (service, servers, server) = await StoreAsync("rs-a");
        await RunAsync(server, "query_store", TimeSpan.FromMinutes(20), "replica 3, primary 1");
        await RunAsync(server, "query_store", TimeSpan.FromMinutes(5), Replica);
        await RunAsync(server, "wait_stats", TimeSpan.FromMinutes(2), null);

        var health = JsonDocument.Parse(await McpHealthTools.GetCollectionHealth(service, servers, server.ServerName)).RootElement;
        Assert.Equal(Replica, CollectorRow(health, "query_store").GetProperty("read_source").GetString());
        Assert.False(CollectorRow(health, "wait_stats").TryGetProperty("read_source", out _));

        var rows = await service.GetCollectionHealthAsync(RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server)));
        Assert.Equal("read source: " + Replica, rows.Single(r => r.CollectorName == "query_store").NoteFormatted);
        Assert.Equal("", rows.Single(r => r.CollectorName == "wait_stats").NoteFormatted);
    }

    /// <summary>A server whose setting was turned off stops showing a source as soon as its runs stop writing one.</summary>
    [Fact]
    public async Task Health_NewestRunWithNoSource_ShowsNoSource()
    {
        var (service, servers, server) = await StoreAsync("rs-b");
        await RunAsync(server, "query_store", TimeSpan.FromMinutes(20), "replica 3, primary 1");
        await RunAsync(server, "query_store", TimeSpan.FromMinutes(5), null);

        var json = await McpHealthTools.GetCollectionHealth(service, servers, server.ServerName);
        Assert.DoesNotContain("read_source", json, StringComparison.Ordinal);
    }

    /// <summary>With NULL on every run, which is every server with the setting off, neither tool names read_source anywhere.</summary>
    [Fact]
    public async Task WithNoReadSourceAnywhere_NeitherToolCarriesTheKey()
    {
        var (service, servers, server) = await StoreAsync("rs-c");
        await RunAsync(server, "query_store", TimeSpan.FromMinutes(5), null);
        await RunAsync(server, "wait_stats", TimeSpan.FromMinutes(2), null);

        var health = await McpHealthTools.GetCollectionHealth(service, servers, server.ServerName);
        var log = await McpHealthTools.GetCollectionLog(service, servers, server.ServerName);
        Assert.DoesNotContain("read_source", health, StringComparison.Ordinal);
        Assert.DoesNotContain("read_source", log, StringComparison.Ordinal);
    }

    /// <summary>The per-run Collection Log grids' two reads fill the run's read source, and the message cell shows it only on that run.</summary>
    [Fact]
    public async Task CollectionLogGrids_ShowTheReadSourceOnTheRunThatHasOne()
    {
        var (service, _, server) = await StoreAsync("rs-e");
        await RunAsync(server, "query_store", TimeSpan.FromMinutes(5), Replica);
        await RunAsync(server, "query_store", TimeSpan.FromMinutes(9), null);
        await RunAsync(server, "wait_stats", TimeSpan.FromMinutes(2), null);
        var serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));

        var grid = await service.GetRecentCollectionLogAsync(serverId, hoursBack: 1, cancellationToken: TestContext.Current.CancellationToken);
        var drill = await service.GetCollectionLogByCollectorAsync(serverId, "query_store", DateTime.UtcNow.AddHours(-1), DateTime.UtcNow.AddMinutes(1));

        Assert.Equal(3, grid.Count);
        Assert.Equal("read source: " + Replica, grid.Single(r => r.ReadSource != null).MessageFormatted);
        Assert.All(grid.Where(r => r.ReadSource == null), r => Assert.Equal("", r.MessageFormatted));
        Assert.Equal(2, drill.Count);
        Assert.Equal("read source: " + Replica, drill.Single(r => r.ReadSource != null).MessageFormatted);
        Assert.Equal("", drill.Single(r => r.ReadSource == null).MessageFormatted);

        Assert.Equal("a note; read source: " + Replica, new CollectionLogRow { ErrorMessage = "a note", ReadSource = Replica }.MessageFormatted);
        Assert.Equal("a note", new CollectionLogRow { ErrorMessage = "a note", ReadSource = " " }.MessageFormatted);
    }

    /// <summary>The log carries the key on the run that has one, per server and fleet-wide, and on no other run.</summary>
    [Fact]
    public async Task Log_CarriesTheReadSourceOnTheRunsThatHaveOne()
    {
        var (service, servers, server) = await StoreAsync("rs-d");
        await RunAsync(server, "query_store", TimeSpan.FromMinutes(5), Replica);
        await RunAsync(server, "wait_stats", TimeSpan.FromMinutes(2), null);

        foreach (var serverName in new string?[] { server.ServerName, "*" })
        {
            var log = JsonDocument.Parse(await McpHealthTools.GetCollectionLog(service, servers, serverName)).RootElement;
            var runs = log.GetProperty("runs").EnumerateArray().ToList();
            Assert.Equal(2, runs.Count);
            var withSource = runs.Single(r => r.GetProperty("collector").GetString() == "query_store");
            Assert.Equal(Replica, withSource.GetProperty("read_source").GetString());
            Assert.False(runs.Single(r => r.GetProperty("collector").GetString() == "wait_stats").TryGetProperty("read_source", out _));
        }
    }
}
