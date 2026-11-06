/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7896: the CPU spike drill-down's <c>queries_at_spike</c> lists each request once. A request alive across several snapshots in the
/// four minutes around the peak is sampled once per snapshot with a climbing cumulative CPU, so one long request could fill every slot
/// and hide the second and third consumers. The seed is one request (session 70) in four snapshots plus two other requests: three
/// entries come back, the long one at its longest sample.
/// </summary>
public sealed class QueriesAtSpikeDistinctRequestsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TestServer";
    private readonly DuckDbInitializer _duckDb;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public QueriesAtSpikeDistinctRequestsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task OneLongRequestInFourSnapshots_TakesOneSlot_AtItsLongestSample()
    {
        var now = DateTime.UtcNow;
        var peak = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc).AddMinutes(-10);

        await ExecAsync(@"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time, sample_time_utc, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $2, $5, 95, 0)",
            _nextId--, Naive(peak), _serverId, ServerName, peak);

        // Session 70 runs one request across four snapshots, 90 s and 30 s either side of the peak, its cumulative CPU climbing.
        var cpu = 1000L;
        foreach (var offset in new[] { -90, -30, 30, 90 })
        {
            await SnapshotAsync(peak.AddSeconds(offset), sessionId: 70, cpuTimeMs: cpu, "SELECT long");
            cpu += 1000;
        }
        // Two other requests, each sampled once.
        await SnapshotAsync(peak.AddSeconds(-30), sessionId: 71, cpuTimeMs: 500, "SELECT second");
        await SnapshotAsync(peak.AddSeconds(30), sessionId: 72, cpuTimeMs: 300, "SELECT third");

        var context = new AnalysisContext
        {
            ServerId = _serverId,
            ServerName = ServerName,
            TimeRangeStart = peak.AddMinutes(-5),
            TimeRangeEnd = peak.AddMinutes(5),
        };
        var finding = new AnalysisFinding { RootFactKey = "CPU_SPIKE", StoryPath = "CPU_SPIKE", PathKeys = ["CPU_SPIKE"], Severity = 1.0 };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], context);

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown!.TryGetValue("queries_at_spike", out var raw), "no queries_at_spike drill-down");
        var entries = JsonSerializer.SerializeToElement(raw).EnumerateArray()
            .Select(e => (Session: e.GetProperty("session_id").GetInt32(), Cpu: e.GetProperty("cpu_time_ms").GetInt64()))
            .ToList();

        Assert.Equal(3, entries.Count);
        Assert.Equal(new[] { 70, 71, 72 }, entries.Select(e => e.Session).ToArray());
        Assert.Equal(4000L, entries[0].Cpu);
    }

    private Task SnapshotAsync(DateTime at, int sessionId, long cpuTimeMs, string text) => ExecAsync(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status,
     blocking_session_id, wait_type, cpu_time_ms, total_elapsed_time_ms, program_name, request_id, query_hash)
VALUES ($1, $2, $3, $4, $5, 'AppDb', $6, 'running', 0, NULL, $7, $8, 'Orders.Api', 0, '0xABC')",
        _nextId--, Naive(at), _serverId, ServerName, sessionId, text, cpuTimeMs, cpuTimeMs * 60);

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private async Task ExecAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
            cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }
}
