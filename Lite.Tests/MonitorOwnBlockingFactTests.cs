using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9340: the BLOCKING_EVENTS and BLOCKING_CHAIN facts leave out a pair whose two sessions are both the monitor's own
/// (the monitor's collectors queued behind each other on a compile or memory problem). A pair with a user's session on either side, or with
/// no program name recorded, is still real blocking evidence.
/// </summary>
public class MonitorOwnBlockingFactTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -9340_01;
    private static readonly DateTime WindowStart = new(2026, 3, 4, 10, 0, 0);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(4);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _conn;
    private long _nextId = -1;

    public MonitorOwnBlockingFactTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _conn?.Dispose();

    private async Task ExecAsync(string sql, params object?[] args)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _conn ??= _duckDb.CreateConnection();
        if (_conn.State != System.Data.ConnectionState.Open) await _conn.OpenAsync();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var arg in args)
            cmd.Parameters.Add(new DuckDBParameter { Value = arg ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    /* Wait-stats deltas give the fact collector its observed time; four hours, one collection every 15 minutes. */
    private async Task SeedObservedWindowAsync()
    {
        for (var i = 0; i <= 16; i++)
            await ExecAsync(
                @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type,
                    wait_time_ms, signal_wait_time_ms, waiting_tasks_count,
                    delta_wait_time_ms, delta_signal_wait_time_ms, delta_waiting_tasks)
                  VALUES ($1, $2, $3, 'TestServer', 'CXPACKET', 1000, 0, 1, $4, 0, $4)",
                _nextId--, WindowStart.AddMinutes(15 * i), ServerId, i == 0 ? 0L : 1000L);
    }

    /* One wait of its own per call (blocked spid 100 + n), blocked by spid 50, with the two program names given. */
    private async Task SeedPairAsync(int n, string? blockedApp, string? blockingApp) =>
        await ExecAsync(
            @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, wait_time_ms, blocked_spid, blocking_spid, blocked_client_app, blocking_client_app)
              VALUES ($1,$2,$2,$3,'TestServer',12000,$4,50,$5,$6)",
            _nextId--, WindowStart.AddMinutes(30 + n), ServerId, 100 + n, blockedApp, blockingApp);

    private async Task<List<Fact>> FactsAsync() =>
        await new DuckDbFactCollector(_duckDb).CollectFactsAsync(new AnalysisContext
        {
            ServerId = ServerId, ServerName = "TestServer", TimeRangeStart = WindowStart, TimeRangeEnd = WindowEnd,
        });

    [Fact]
    public async Task OnlyTheMonitorsOwnBlocking_ProducesNoBlockingFacts()
    {
        await SeedObservedWindowAsync();
        await SeedPairAsync(0, "PerformanceMonitorDarling", "PerformanceMonitorDarling");
        await SeedPairAsync(1, "PerformanceMonitorDarling-Remediation", "PerformanceMonitorDarling");
        await SeedPairAsync(2, "PerformanceMonitorLite", "PerformanceMonitorLite");

        var facts = await FactsAsync();

        Assert.DoesNotContain(facts, f => f.Key == "BLOCKING_EVENTS");
        Assert.DoesNotContain(facts, f => f.Key == "BLOCKING_CHAIN");
    }

    [Fact]
    public async Task BlockingWithAUserSessionOnEitherSide_OrNoProgramName_StillCounts()
    {
        await SeedObservedWindowAsync();
        await SeedPairAsync(0, "PerformanceMonitorDarling", "PerformanceMonitorDarling");   // left out
        await SeedPairAsync(1, "PerformanceMonitorDarling", "OrderEntry");                 // monitor blocked by a user
        await SeedPairAsync(2, "OrderEntry", "PerformanceMonitorDarling");                 // user blocked by the monitor
        await SeedPairAsync(3, "OrderEntry", "OrderEntry");
        await SeedPairAsync(4, null, null);                                               // no name recorded: never hidden
        await SeedPairAsync(5, "PerformanceMonitorDarling", null);
        await SeedPairAsync(6, "MyPerformanceMonitorTool", "MyPerformanceMonitorTool");    // contains the words, does not begin with them

        var facts = await FactsAsync();

        Assert.Equal(6.0, facts.Single(f => f.Key == "BLOCKING_EVENTS").Metadata["event_count"]);
        Assert.Contains(facts, f => f.Key == "BLOCKING_CHAIN");
    }

    [Fact]
    public async Task OneRealPair_BesideTheMonitorsOwn_IsTheOnlyBlockingEvidence()
    {
        await SeedObservedWindowAsync();
        for (var n = 0; n < 4; n++)
            await SeedPairAsync(n, "PerformanceMonitorDarling", "PerformanceMonitorDarling");
        await SeedPairAsync(9, "OrderEntry", "OrderEntry");

        var facts = await FactsAsync();

        Assert.Equal(1.0, facts.Single(f => f.Key == "BLOCKING_EVENTS").Metadata["event_count"]);
        Assert.Equal(1.0, facts.Single(f => f.Key == "BLOCKING_CHAIN").Metadata["worst_chain_victim_count"]);
    }
}
