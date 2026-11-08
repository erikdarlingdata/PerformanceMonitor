/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A count in a Lite tool answer or a service message reads the way a person says it: "1 collector" and "3 collectors",
/// "1 hour" and "24 hours", never "N collector(s)". Each fact calls the real tool, or the method that builds the message,
/// with a count of one and a count above one and reads the realized words, so a count that drifted back to the "(s)" form,
/// or a verb left disagreeing with its count, fails here.
/// </summary>
public sealed class LiteMcpAnswerCountTextTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "CountTextSrv";
    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public LiteMcpAnswerCountTextTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-counttext-" + Guid.NewGuid().ToString("N"));
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

    [Fact]
    public async Task CollectionHealthNote_OneHealthyCollector_ReadsSingular()
    {
        await SeedHealthyRunsAsync("wait_stats");

        var note = (await HealthAsync()).GetProperty("collector_detail_note").GetString()!;

        Assert.StartsWith("1 of 1 collector below is HEALTHY", note, StringComparison.Ordinal);
        Assert.DoesNotContain("(s)", note, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CollectionHealthNote_ThreeHealthyCollectors_ReadsPlural()
    {
        await SeedHealthyRunsAsync("wait_stats", "memory_stats", "cpu_utilization_stats");

        var note = (await HealthAsync()).GetProperty("collector_detail_note").GetString()!;

        Assert.Matches(new Regex(@"^\d+ of 3 collectors below (is|are) HEALTHY"), note);
        Assert.DoesNotContain("(s)", note, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1, "in the last 1 hour.")]
    [InlineData(24, "in the last 24 hours.")]
    public async Task CollectionLogQuietWindow_CountsHoursAsAPersonWould(int hoursBack, string expected)
    {
        /* Collected once, outside the asked-for window: the answer is the quiet-window one, with the window's length in it. */
        await SeedLogAsync("query_store", DateTime.UtcNow.AddHours(-72), "SUCCESS", 10, null);

        var json = await McpHealthTools.GetCollectionLog(new LocalDataService(_duckDb), _serverManager, ServerName, hoursBack, 200);
        var root = JsonDocument.Parse(json).RootElement;

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.StartsWith($"No collector runs recorded for {ServerName} {expected}", message, StringComparison.Ordinal);
        Assert.DoesNotContain("(s)", message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1, "Collection cycle took 130s, so 1 collection slot was skipped.")]
    [InlineData(4, "Collection cycle took 130s, so 4 collection slots were skipped.")]
    public void CycleSkipMessage_AgreesWithItsSlotCount(int slots, string expected)
    {
        var steps = new List<CycleStep> { new("collectors", 61_000), new("archival", 60_000), new("retention", 9_000) };

        var report = CollectionCycleSkip.Build(slots, TimeSpan.FromSeconds(130), TimeSpan.FromSeconds(1), steps, TimeSpan.FromMinutes(1));

        Assert.NotNull(report);
        Assert.StartsWith(expected, report!.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("(s)", report.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1, "(searched 1 server that has a list in the last 7 days)")]
    [InlineData(3, "(searched 3 servers that have a list in the last 7 days)")]
    public void FindDatabaseMiss_AgreesWithItsServerCount(int searched, string expected)
    {
        var message = McpDiscoveryTools.NoMatchMessage("appdb", searched, 7);

        Assert.StartsWith("No monitored server's newest database list names 'appdb' " + expected, message, StringComparison.Ordinal);
        Assert.DoesNotContain("(s)", message, StringComparison.Ordinal);
    }

    private async Task<JsonElement> HealthAsync()
    {
        var json = await McpHealthTools.GetCollectionHealth(new LocalDataService(_duckDb), _serverManager, ServerName);
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    private async Task SeedHealthyRunsAsync(params string[] collectors)
    {
        var now = DateTime.UtcNow;
        for (var i = 0; i < 6; i++)
        {
            foreach (var collector in collectors)
            {
                await SeedLogAsync(collector, now.AddMinutes(-i * 30 - 2), "SUCCESS", 40, null);
            }
        }
    }

    private async Task SeedLogAsync(string collector, DateTime collectionTimeUtc, string status, int? rowsCollected, string? errorMessage)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, 50, $6, $7, $8, 40, 10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)errorMessage ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rowsCollected ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = new DuckDBConnection(_duckDb.ConnectionString);
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }
}
