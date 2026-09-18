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
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's half of #3541 A12 ("zero is a measurement"), against a real DuckDB, through the real tool methods —
/// the twin of Darling's <c>McpZeroIsAMeasurementLivePostgresTests</c>. The source-level census of both SKUs
/// lives in Darling.Tests (which reads Lite's files); this file is the one that EXECUTES Lite's ladder.
///
/// <para>The health-parser family is the weight-bearing case: eight of its nine reads answered a
/// never-read <c>system_health</c> session with the same <c>empty</c> a healthy quiet hour earns. The four
/// rungs are walked on <c>get_health_parser_memory_node_oom</c> — the rarest category, and therefore the one
/// where "never captured while the session IS being read" is most obviously the healthy measurement and most
/// obviously NOT <c>unavailable</c>.</para>
/// </summary>
public sealed class McpZeroIsAMeasurementTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "ZeroMeasureSrv";

    /* Lite derives the server id from the storage name (see SignificantWaitsToolTests), so seeded rows must
       be written under the same derived value the tool resolves to. */
    private readonly int _serverId;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public McpZeroIsAMeasurementTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-zeromeasure-" + Guid.NewGuid().ToString("N"));
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

    private static string LoadFixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", name));

    [Fact]
    public async Task TheHealthParserLadder_FourRungs_OnlyTheDeadSessionIsUnavailable()
    {
        var service = new LocalDataService(_duckDb);

        /* Rung 4 — nothing of any type, ever: NOT a clean bill. This is the rung the other eight tools never
           had; before #3541 A12 this read answered "empty" here. */
        var dead = JsonDocument.Parse(await McpHealthParserTools.GetMemoryNodeOOM(service, _serverManager, ServerName, 24, 50)).RootElement;
        Assert.Equal("unavailable", dead.GetProperty("status").GetString());
        Assert.False(dead.GetProperty("source_observed").GetBoolean());
        Assert.Equal(JsonValueKind.Null, dead.GetProperty("last_captured_at").ValueKind);
        Assert.Equal(0, dead.GetProperty("events_in_window").GetInt32());
        Assert.Contains("NOT an all-clear", dead.GetProperty("message").GetString()!, StringComparison.Ordinal);
        Assert.Contains("system_health session is started", dead.GetProperty("message").GetString()!, StringComparison.Ordinal);

        /* Rung 3 — the session IS being read (another category was stored), this category never: the
           healthy measurement. An OOM that never happened is not a blind spot. */
        var stored = Truncate(DateTime.UtcNow.AddMinutes(-30));
        await SeedEventAsync(SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_system.xml"), Truncate(DateTime.UtcNow.AddMinutes(-31)), stored);

        var never = JsonDocument.Parse(await McpHealthParserTools.GetMemoryNodeOOM(service, _serverManager, ServerName, 24, 50)).RootElement;
        Assert.Equal("empty", never.GetProperty("status").GetString());
        Assert.True(never.GetProperty("source_observed").GetBoolean());
        Assert.Equal(stored.ToString("o"), never.GetProperty("last_captured_at").GetString());
        Assert.Equal(JsonValueKind.Null, never.GetProperty("last_captured_of_type_at").ValueKind);
        Assert.Contains("the absence is a measurement", never.GetProperty("message").GetString()!, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", never.GetProperty("message").GetString()!, StringComparison.Ordinal);
        Assert.DoesNotContain("widen", never.GetProperty("message").GetString()!, StringComparison.Ordinal);

        /* Rung 2 — captured before, outside this window: quiet, and widening reaches it. */
        var oldOom = Truncate(DateTime.UtcNow.AddHours(-48));
        await SeedEventAsync(SystemHealthParser.MemoryNodeOomEvent, LoadFixture("memory_node_oom.xml"), oldOom, oldOom);

        var quiet = JsonDocument.Parse(await McpHealthParserTools.GetMemoryNodeOOM(service, _serverManager, ServerName, 1, 50)).RootElement;
        Assert.Equal("empty", quiet.GetProperty("status").GetString());
        Assert.True(quiet.GetProperty("source_observed").GetBoolean());
        Assert.Equal(oldOom.ToString("o"), quiet.GetProperty("last_captured_of_type_at").GetString());
        Assert.Contains("widen hours_back", quiet.GetProperty("message").GetString()!, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", quiet.GetProperty("message").GetString()!, StringComparison.Ordinal);

        /* The data envelope carries the same witness pair beside its rows. */
        var hit = JsonDocument.Parse(await McpHealthParserTools.GetMemoryNodeOOM(service, _serverManager, ServerName, 72, 50)).RootElement;
        Assert.Equal(ServerName, hit.GetProperty("server").GetString());
        Assert.True(hit.GetProperty("source_observed").GetBoolean());
        Assert.Equal(stored.ToString("o"), hit.GetProperty("last_captured_at").GetString());
        Assert.Equal(1, hit.GetProperty("event_count").GetInt32());

        /* Rung 1 — captured IN the window and gated out: the memory-conditions read over the same
           sp_server_diagnostics event, whose SYSTEM component is not a low-memory notification. Healthy, and
           the count of what WAS captured is published so the caller can see the gate did the work. */
        var gated = JsonDocument.Parse(await McpHealthParserTools.GetMemoryConditions(service, _serverManager, ServerName, 1, 50)).RootElement;
        Assert.Equal("empty", gated.GetProperty("status").GetString());
        Assert.Equal(1, gated.GetProperty("events_in_window").GetInt32());
        Assert.Contains("Events ARE being captured", gated.GetProperty("message").GetString()!, StringComparison.Ordinal);
        Assert.DoesNotContain("widen", gated.GetProperty("message").GetString()!, StringComparison.Ordinal);
    }

    private static DateTime Truncate(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedEventAsync(string eventType, string eventXml, DateTime eventTimeUtc, DateTime collectionTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO system_health_events
    (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventType });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventXml });
        await cmd.ExecuteNonQueryAsync();
    }
}
