/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
/// Lite's get_health_parser_significant_waits (#2484), the twin of Darling's. The tool landed on both SKUs
/// in one change rather than on the divergence ratchet, so the promises the SKUs make to each other are
/// pinned here: the same three kinds of empty, in words that cannot be mistaken for one another.
///
/// <para>Written at the TOOL level, not the reader level. The reader is the easy half; the three-way empty
/// branch and the cap contract both live in the tool, and a reader-level test would see neither.</para>
/// </summary>
public sealed class SignificantWaitsToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "SigWaitsSrv";

    /*
        Lite does not store a server id -- ServerResolver DERIVES it from the storage name, so seeded rows
        have to be written under the same derived value the tool resolves to. A hardcoded id would seed
        rows the tool looks straight past, and the never-captured assertion would pass for the wrong reason.
    */
    private readonly int _serverId;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public SignificantWaitsToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-sigwaits-" + Guid.NewGuid().ToString("N"));
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

    private static string LoadFixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", name));

    [Fact]
    public async Task ThreeKindsOfNothing_AreThreeDifferentAnswers()
    {
        var service = new LocalDataService(_duckDb);

        /* Registered, nothing ever captured: NOT an all-clear, and it must refuse to read as one. */
        var never = await McpHealthParserTools.GetSignificantWaits(service, _serverManager, ServerName, 24, 50);
        var neverRoot = JsonDocument.Parse(never).RootElement;
        Assert.Equal("unavailable", neverRoot.GetProperty("status").GetString());
        var neverText = neverRoot.GetProperty("message").GetString()!;
        Assert.Contains("EVER", neverText, StringComparison.Ordinal);
        Assert.Contains("NOT an all-clear", neverText, StringComparison.Ordinal);
        Assert.DoesNotContain("widen", neverText, StringComparison.OrdinalIgnoreCase);

        /* Captured, but outside the asked-for window: a quiet window, and widening IS the move. */
        await SeedWaitAsync(LoadFixture("wait_info.xml"), Truncate(DateTime.UtcNow.AddHours(-48)));

        var quiet = await McpHealthParserTools.GetSignificantWaits(service, _serverManager, ServerName, 1, 50);
        var quietRoot = JsonDocument.Parse(quiet).RootElement;
        Assert.Equal("empty", quietRoot.GetProperty("status").GetString());
        var quietText = quietRoot.GetProperty("message").GetString()!;
        Assert.Contains("widen", quietText, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("EVER", quietText, StringComparison.Ordinal);

        /*
            Captured IN the window and gated out: the healthy answer, and the one the other two must never
            be confused with. Same fixture, duration dropped under the 500 ms bar.
        */
        var tooShort = LoadFixture("wait_info.xml").Replace("<value>1500</value>", "<value>100</value>", StringComparison.Ordinal);
        Assert.DoesNotContain("<value>1500</value>", tooShort, StringComparison.Ordinal);
        await SeedWaitAsync(tooShort, Truncate(DateTime.UtcNow.AddMinutes(-10)));

        var gated = await McpHealthParserTools.GetSignificantWaits(service, _serverManager, ServerName, 4, 50);
        var gatedRoot = JsonDocument.Parse(gated).RootElement;
        Assert.Equal("empty", gatedRoot.GetProperty("status").GetString());
        var gatedText = gatedRoot.GetProperty("message").GetString()!;
        Assert.Contains("none was significant", gatedText, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", gatedText, StringComparison.Ordinal);
        Assert.DoesNotContain("widen", gatedText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ThePayloadCarriesTheStatementThatPaidForTheWait()
    {
        var service = new LocalDataService(_duckDb);

        /* One significant wait and one gated-out event, both inside the window. */
        await SeedWaitAsync(LoadFixture("wait_info.xml"), Truncate(DateTime.UtcNow.AddMinutes(-9)));
        await SeedWaitAsync(
            LoadFixture("wait_info.xml").Replace("<value>1500</value>", "<value>100</value>", StringComparison.Ordinal),
            Truncate(DateTime.UtcNow.AddMinutes(-10)));

        var hit = await McpHealthParserTools.GetSignificantWaits(service, _serverManager, ServerName, 4, 50);
        var root = JsonDocument.Parse(hit).RootElement;
        Assert.Equal(ServerName, root.GetProperty("server").GetString());
        Assert.Equal(1, root.GetProperty("wait_count").GetInt32());

        var wait = root.GetProperty("waits")[0];
        Assert.Equal("PAGEIOLATCH_SH", wait.GetProperty("wait_type").GetString());
        Assert.Equal(1500, wait.GetProperty("duration_ms").GetInt64());
        Assert.Equal(12, wait.GetProperty("signal_duration_ms").GetInt64());
        Assert.Equal(57, wait.GetProperty("session_id").GetInt32());

        /*
            The SQL text is the half get_wait_stats can never give: the instance-wide totals name a wait
            type and never the statement that paid for it. Both SKUs advertise the same field name.
        */
        Assert.Contains("dbo.big_table", wait.GetProperty("query_text").GetString()!, StringComparison.Ordinal);

        /* The event time comes back as the raw naive-UTC XE @timestamp, not the grid's local render. */
        Assert.StartsWith("2026-07-05T12:04:30", wait.GetProperty("event_time").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnOutOfRangeCap_IsRefused_NotSilentlyClamped()
    {
        var service = new LocalDataService(_duckDb);
        await SeedWaitAsync(LoadFixture("wait_info.xml"), Truncate(DateTime.UtcNow.AddMinutes(-9)));

        var tooBig = await McpHealthParserTools.GetSignificantWaits(service, _serverManager, ServerName, 24, 5000);
        Assert.Contains("exceeds maximum of", tooBig, StringComparison.Ordinal);
        Assert.Contains("1000", tooBig, StringComparison.Ordinal);
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

    private async Task SeedWaitAsync(string eventXml, DateTime eventTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO system_health_events
    (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = SystemHealthParser.WaitInfoEvent });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventXml });
        await cmd.ExecuteNonQueryAsync();
    }
}
