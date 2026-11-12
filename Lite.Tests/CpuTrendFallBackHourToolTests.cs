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
/// #7908: Lite's <c>get_cpu_utilization</c> buckets a sample that has a <c>sample_time_utc</c> on that instant, as the
/// chart read does, so the two passes through a server-local fall-back hour are two points instead of one averaged
/// point; a sample collected before that column existed is still bucketed on its wall time. Points keep the
/// server-local stamp the tool publishes. Run through the real tool over a real DuckDB, on a server whose clock is a
/// zone that falls back.
/// </summary>
public sealed class CpuTrendFallBackHourToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "FallBackSrv";

    /* The first Sunday of November 2026: clocks go from 02:00 EDT back to 01:00 EST at 06:00 UTC. */
    private static readonly DateTime AsOfUtc = new(2026, 11, 1, 8, 0, 0, DateTimeKind.Utc);

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 790800;

    public CpuTrendFallBackHourToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-cpudst-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);

        /* Derived, not stored -- seeding under a hardcoded id would write rows the tool looks past. */
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task TheTwoPassesThroughTheRepeatedHour_AreTwoPoints_AndAnOlderWallTimeSampleKeepsItsBucket()
    {
        await SeedServerClockAsync();
        /* 00:30 EDT, then 01:30 EDT and 01:30 EST (the same wall time an hour apart), then a sample with no UTC stamp at 02:30 EST. */
        await SeedCpuAsync(sampleTimeLocal: new DateTime(2026, 11, 1, 0, 30, 0), sampleTimeUtc: new DateTime(2026, 11, 1, 4, 30, 0), sqlCpu: 10);
        await SeedCpuAsync(sampleTimeLocal: new DateTime(2026, 11, 1, 1, 30, 0), sampleTimeUtc: new DateTime(2026, 11, 1, 5, 30, 0), sqlCpu: 20);
        await SeedCpuAsync(sampleTimeLocal: new DateTime(2026, 11, 1, 1, 30, 0), sampleTimeUtc: new DateTime(2026, 11, 1, 6, 30, 0), sqlCpu: 80);
        await SeedCpuAsync(sampleTimeLocal: new DateTime(2026, 11, 1, 2, 30, 0), sampleTimeUtc: null, sqlCpu: 40);

        var service = new LocalDataService(_duckDb);
        var clock = await service.GetServerClockAsync(_serverId);
        Assert.NotNull(clock);
        Assert.Equal(-240, clock!.OffsetMinutesAt(new DateTime(2026, 11, 1, 5, 30, 0)));
        Assert.Equal(-300, clock.OffsetMinutesAt(new DateTime(2026, 11, 1, 6, 30, 0)));

        var root = JsonDocument.Parse(await McpCpuTools.GetCpuUtilization(service, _serverManager, ServerName, 4, AsOfUtc.ToString("o"), 60)).RootElement;
        Assert.False(root.TryGetProperty("status", out _), root.GetRawText());
        var points = root.GetProperty("samples").EnumerateArray().ToList();

        Assert.True(points.Count == 4, "the repeated hour's two samples were averaged into one point: " + root.GetProperty("samples").GetRawText());
        Assert.All(points, p => Assert.Equal(1, p.GetProperty("samples_in_bucket").GetInt64()));

        /* In instant order, each stamped in the server's clock: the two repeated-hour points share the wall-clock hour. */
        Assert.Equal(
            new[] { "2026-11-01T00:00:00", "2026-11-01T01:00:00", "2026-11-01T01:00:00", "2026-11-01T02:00:00" },
            points.Select(p => p.GetProperty("sample_time").GetString()!.Substring(0, 19)).ToArray());
        Assert.Equal(new[] { 10, 20, 80, 40 }, points.Select(p => p.GetProperty("peak_sql_server_cpu").GetInt32()).ToArray());
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

    /// <summary>One server_properties snapshot naming a zone that falls back, with the offset in force when it was taken.</summary>
    private async Task SeedServerClockAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO server_properties
            (collection_id, collection_time, server_id, server_name,
             edition, product_version, product_level, engine_edition,
             cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes, time_zone_id)
            VALUES ($1, $2, $3, $4, 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, -300, 'Eastern Standard Time')";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId++);
        P(DateTime.SpecifyKind(AsOfUtc.AddMinutes(-1), DateTimeKind.Unspecified));
        P(_serverId);
        P(ServerName);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedCpuAsync(DateTime sampleTimeLocal, DateTime? sampleTimeUtc, int sqlCpu)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time,
             sqlserver_cpu_utilization, other_process_cpu_utilization, sample_time_utc)
            VALUES ($1, $2, $3, $4, $5, $6, 5, $7)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId++);
        P(DateTime.SpecifyKind((sampleTimeUtc ?? sampleTimeLocal.AddHours(5)).AddMinutes(1), DateTimeKind.Unspecified));
        P(_serverId);
        P(ServerName);
        P(DateTime.SpecifyKind(sampleTimeLocal, DateTimeKind.Unspecified));
        P(sqlCpu);
        P(sampleTimeUtc is { } utc ? DateTime.SpecifyKind(utc, DateTimeKind.Unspecified) : DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
    }
}
