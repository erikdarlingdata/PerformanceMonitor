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
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;
using static PerformanceMonitor.Collectors.CollectorRuntimePrecondition;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7813: Lite's get_plan_corrections says which case an empty answer is: the server's version has no automatic tuning, no database has
/// Query Store on in the newest health capture, or nothing has been recommended so far. The version comes from the connection status the
/// way the PVS sentence reads it, the Query Store state from the DuckDB store.
/// </summary>
public sealed class AutomaticTuningEmptyTextTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "AutoTuningEmptySrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly ServerConnection _server;
    private readonly int _serverId;
    private readonly LocalDataService _service;
    private DuckDBConnection? _seedConn;
    private long _nextId = 781300;

    public AutomaticTuningEmptyTextTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _service = new LocalDataService(_duckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-autotuningempty-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        _server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(_server);

        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(_server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    private void SetServerMajorVersion(int major) =>
        _serverManager.GetConnectionStatus(_server.Id).SqlMajorVersion = major;

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    private static string Message(JsonElement root) => root.GetProperty("message").GetString()!;

    private async Task SeedQueryStoreHealthAsync(params (string Database, string ActualState)[] rows)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        var at = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-5), DateTimeKind.Unspecified);
        foreach (var (database, actualState) in rows)
        {
            using var cmd = _seedConn.CreateCommand();
            cmd.CommandText = @"
INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state)
VALUES ($1, $2, $3, $4, $5, $6, $7)";
            foreach (var value in new object[] { _nextId++, at, _serverId, ServerName, database, actualState, "READ_WRITE" })
                cmd.Parameters.Add(new DuckDBParameter { Value = value });
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private Task<string> AskAsync() => McpPlanCorrectionTools.GetPlanCorrections(_service, _serverManager, ServerName);

    [Fact]
    public async Task AServerBefore2017_HearsThatItsVersionHasNoAutomaticTuning()
    {
        SetServerMajorVersion(13);

        var root = Parse(await AskAsync());

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Equal(PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 13 }, null), Message(root));
        Assert.Contains("has no automatic tuning", Message(root), StringComparison.Ordinal);
    }

    [Fact]
    public async Task AServerWithQueryStoreOffEverywhere_HearsThatNoDatabaseHasItOn()
    {
        SetServerMajorVersion(15);
        await SeedQueryStoreHealthAsync(("DbA", "OFF"), ("DbB", "OFF"));

        var root = Parse(await AskAsync());

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Equal(
            PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 15 }, new[] { new QueryStoreDatabaseState("DbA", "OFF"), new QueryStoreDatabaseState("DbB", "OFF") }),
            Message(root));
        Assert.Contains("no database has Query Store on", Message(root), StringComparison.Ordinal);
    }

    [Fact]
    public async Task AServerWithQueryStoreOn_HearsThatNothingHasBeenRecommended()
    {
        SetServerMajorVersion(15);
        await SeedQueryStoreHealthAsync(("DbA", "OFF"), ("DbB", "READ_WRITE"));

        var root = Parse(await AskAsync());

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Contains("Query Store is on", Message(root), StringComparison.Ordinal);
        Assert.Contains("nothing has been recommended", Message(root), StringComparison.Ordinal);
    }

    /// <summary>A filtered empty answer is a verdict about the chosen database only and keeps its own sentence.</summary>
    [Fact]
    public async Task AFilteredEmptyAnswer_KeepsTheChosenDatabaseSentence()
    {
        SetServerMajorVersion(13);

        var root = Parse(await McpPlanCorrectionTools.GetPlanCorrections(_service, _serverManager, ServerName, database_name: "NoSuchDb"));

        Assert.Equal("No plan correction data found for the database NoSuchDb.", Message(root));
    }
}
