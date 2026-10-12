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
/// #5633 items 3, 19 and 9 on Lite. An empty read must say WHY from the collector's own <c>collection_log</c> history:
/// the "two cycles" reason only while <c>procedure_stats</c> is new, "no trace flag is enabled" when the
/// <c>trace_flags</c> collector ran and stored nothing, and a configured 0 beside an in-use 16 for
/// <c>min server memory (MB)</c> is a match. Each fails on the old shape: the old procedure text was returned for every
/// empty page, the old trace-flag text claimed too few snapshots whatever the log said, and the old match flag was plain
/// equality.
/// </summary>
public sealed class CollectorRunEmptyReasonTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "LiteCollectorRunReason";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextLogId = 1_000_000;

    public CollectorRunEmptyReasonTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-runreason-" + Guid.NewGuid().ToString("N"));
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

    private static string Field(string json, string name) =>
        JsonDocument.Parse(json).RootElement.GetProperty(name).GetString()!;

    private async Task LogRunAsync(string collector, DateTime at, int rows, string status = "SUCCESS")
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected)
VALUES ($1, $2, $3, $4, $5, $6, $7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextLogId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = rows });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task EmptyProcedures_OnANewCollector_KeepTheTwoCyclesReason()
    {
        await LogRunAsync("procedure_stats", DateTime.UtcNow.AddMinutes(-5), 0);
        var service = new LocalDataService(_duckDb);

        var json = await McpQueryTools.GetTopProceduresByCpu(service, _serverManager, ServerName);

        Assert.Equal("unavailable", Field(json, "status"));
        Assert.Contains("two collection cycles", Field(json, "message"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task EmptyProcedures_AfterTheCollectorHasRun_SaysNoProcedureExecuted()
    {
        await LogRunAsync("procedure_stats", DateTime.UtcNow.AddMinutes(-40), 0);
        await LogRunAsync("procedure_stats", DateTime.UtcNow.AddMinutes(-25), 0);
        await LogRunAsync("procedure_stats", DateTime.UtcNow.AddMinutes(-10), 0);
        var service = new LocalDataService(_duckDb);

        var json = await McpQueryTools.GetTopProceduresByCpu(service, _serverManager, ServerName);

        Assert.Equal("empty", Field(json, "status"));
        var message = Field(json, "message");
        Assert.Contains("No procedure executed", message, StringComparison.Ordinal);
        Assert.DoesNotContain("two collection cycles", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EmptyProcedures_WithRunsOnlyOutsideTheWindow_AreAGapNotAnAllClear()
    {
        await LogRunAsync("procedure_stats", DateTime.UtcNow.AddHours(-30), 0);
        await LogRunAsync("procedure_stats", DateTime.UtcNow.AddHours(-29), 0);
        var service = new LocalDataService(_duckDb);

        var json = await McpQueryTools.GetTopProceduresByCpu(service, _serverManager, ServerName, hours_back: 1);

        Assert.Equal("unavailable", Field(json, "status"));
        Assert.Contains("no successful run inside", Field(json, "message"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task TraceFlags_AfterARunThatFoundNone_SayNoFlagIsEnabled()
    {
        await LogRunAsync("trace_flags", DateTime.UtcNow.AddMinutes(-20), 0);
        var service = new LocalDataService(_duckDb);

        var current = await McpConfigTools.GetTraceFlags(service, _serverManager, ServerName);
        var changes = await McpConfigHistoryTools.GetTraceFlagChanges(service, _serverManager, ServerName);

        foreach (var json in new[] { current, changes })
        {
            Assert.Equal("empty", Field(json, "status"));
            Assert.Contains("No trace flags are enabled as of", Field(json, "message"), StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task TraceFlagChanges_WithNoSuccessfulRun_KeepTheTwoSnapshotsText()
    {
        await LogRunAsync("trace_flags", DateTime.UtcNow.AddMinutes(-20), 0, status: "ERROR");
        var service = new LocalDataService(_duckDb);

        var changes = await McpConfigHistoryTools.GetTraceFlagChanges(service, _serverManager, ServerName);

        Assert.Equal("empty", Field(changes, "status"));
        Assert.Contains("at least two snapshots", Field(changes, "message"), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("min server memory (MB)", 0, 16, true)]
    [InlineData("min server memory (MB)", 0, 0, true)]
    [InlineData("min server memory (MB)", 0, 32, false)]
    [InlineData("min server memory (MB)", 16, 0, false)]
    [InlineData("max server memory (MB)", 0, 16, false)]
    [InlineData("max degree of parallelism", 8, 4, false)]
    public void MinServerMemoryZeroRunsAsSixteen_OnlyForThatSetting(string name, long configured, long inUse, bool expected)
    {
        var row = new ServerConfigRow { ConfigurationName = name, ValueConfigured = configured, ValueInUse = inUse };

        Assert.Equal(expected, row.ValuesMatch);
    }
}
