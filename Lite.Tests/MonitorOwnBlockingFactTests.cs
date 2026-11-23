using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
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

    /* One wait of its own per call (blocked spid 100 + n), blocked by spid `blockingSpid`, with the two program names given. */
    private async Task SeedPairAsync(int n, string? blockedApp, string? blockingApp, int blockingSpid = 50) =>
        await ExecAsync(
            @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, wait_time_ms, blocked_spid, blocking_spid, blocked_client_app, blocking_client_app)
              VALUES ($1,$2,$2,$3,'TestServer',12000,$4,$5,$6,$7)",
            _nextId--, WindowStart.AddMinutes(30 + n), ServerId, 100 + n, blockingSpid, blockedApp, blockingApp);

    /* The snapshot arm's twin of SeedPairAsync (blocked spid 200 + n); a snapshot's event time is its collection time. */
    private async Task SeedSnapshotPairAsync(int n, string? blockedApp, string? blockingApp, int blockingSpid = 50) =>
        await ExecAsync(
            @"INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, event_time, server_id, server_name, wait_time_ms, blocked_spid, blocking_spid, blocked_client_app, blocking_client_app, monitor_loop)
              VALUES ($1,$2,$2,$3,'TestServer',12000,$4,$5,$6,$7,1)",
            _nextId--, WindowStart.AddMinutes(30 + n), ServerId, 200 + n, blockingSpid, blockedApp, blockingApp);

    /* More monitor-only pairs than a read's 5,000-row cap, every one NEWER than the real pairs the tests seed (a read keeps the newest rows
       first, so a filter applied after the cap would keep only these). Each is a distinct wait: blocked spid 1000 + i, blocked by spid 60. */
    private const int PastTheCap = 5100;

    /* With floodDatabase set the flood is user blocking inside that database (as a database monitored as its own target would have), not the monitor's own. */
    private async Task SeedMonitorOnlyFloodAsync(bool snapshots, string? floodDatabase = null)
    {
        var apps = floodDatabase is null ? "'PerformanceMonitorDarling', 'PerformanceMonitorDarling'" : "'OrderEntry', 'OrderEntry'";
        var sql = snapshots
            ? $@"INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, event_time, server_id, server_name, wait_time_ms, blocked_spid, blocking_spid, blocked_client_app, blocking_client_app, monitor_loop, database_name)
                SELECT -1000000 - i, $1::TIMESTAMP + to_seconds(i), $1::TIMESTAMP + to_seconds(i), $2, 'TestServer', 12000, 1000 + i, 60, {apps}, 1, $4
                FROM range($3) AS t(i)"
            : $@"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, wait_time_ms, blocked_spid, blocking_spid, blocked_client_app, blocking_client_app, database_name)
                SELECT -1000000 - i, $1::TIMESTAMP + to_seconds(i), $1::TIMESTAMP + to_seconds(i), $2, 'TestServer', 12000, 1000 + i, 60, {apps}, $4
                FROM range($3) AS t(i)";
        await ExecAsync(sql, WindowStart.AddMinutes(60), ServerId, PastTheCap, floodDatabase);
    }

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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MoreMonitorOnlyPairsThanTheRowCap_DoNotPushTheRealChainOutOfTheRead(bool snapshots)
    {
        await SeedObservedWindowAsync();
        if (snapshots) await SeedSnapshotPairAsync(1, "OrderEntry", "OrderEntry");
        else await SeedPairAsync(1, "OrderEntry", "OrderEntry");
        await SeedMonitorOnlyFloodAsync(snapshots);

        var facts = await FactsAsync();

        Assert.Equal(1.0, facts.Single(f => f.Key == "BLOCKING_CHAIN").Metadata["worst_chain_victim_count"]);
    }

    private const string SeparateDatabase = "ShardDb";

    /* #9346: a master target leaves out the pairs of a database monitored as its own target. 5,100 newer pairs of that database must not push
       the master's own pair (no database recorded) out of either read, so the chain still forms and the drill-down shows it. */
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MoreSeparatelyMonitoredPairsThanTheRowCap_DoNotPushTheMastersChainOutOfTheFact(bool snapshots)
    {
        await SeedObservedWindowAsync();
        if (snapshots) await SeedSnapshotPairAsync(1, "OrderEntry", "OrderEntry");
        else await SeedPairAsync(1, "OrderEntry", "OrderEntry");
        await SeedMonitorOnlyFloodAsync(snapshots, SeparateDatabase);

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(new AnalysisContext
        {
            ServerId = ServerId, ServerName = "TestServer", TimeRangeStart = WindowStart, TimeRangeEnd = WindowEnd,
            SeparatelyMonitoredDatabases = [SeparateDatabase.ToLowerInvariant()]
        });

        Assert.Equal(1.0, facts.Single(f => f.Key == "BLOCKING_CHAIN").Metadata["worst_chain_victim_count"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MoreSeparatelyMonitoredPairsThanTheRowCap_DoNotPushTheMastersChainOutOfTheDrillDown(bool snapshots)
    {
        if (snapshots) await SeedSnapshotPairAsync(1, "OrderEntry", "OrderEntry");
        else await SeedPairAsync(1, "OrderEntry", "OrderEntry");
        await SeedMonitorOnlyFloodAsync(snapshots, SeparateDatabase);

        var finding = await DrillDownAsync("BLOCKING_CHAIN", [SeparateDatabase.ToLowerInvariant()]);

        var apexes = Section(finding, "reconstructed_blocking_chains").Select(c => c!["apex_spid"]!.GetValue<int>()).ToArray();
        Assert.Equal([50], apexes);
    }

    private async Task<AnalysisFinding> DrillDownAsync(string pathKey, IReadOnlyList<string>? separatelyMonitored = null)
    {
        var finding = new AnalysisFinding { RootFactKey = pathKey, StoryPath = pathKey, PathKeys = [pathKey], Severity = 1.0 };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], new AnalysisContext
        {
            ServerId = ServerId, ServerName = "TestServer", TimeRangeStart = WindowStart, TimeRangeEnd = WindowEnd,
            SeparatelyMonitoredDatabases = separatelyMonitored
        });
        return finding;
    }

    private static JsonArray Section(AnalysisFinding finding, string section) =>
        JsonSerializer.SerializeToNode(finding.DrillDown![section])!.AsArray();

    [Fact]
    public async Task TheTopBlockingList_ShowsTheRealPair_NotTheMonitorsOwn()
    {
        await SeedPairAsync(1, "OrderEntry", "OrderEntry");
        await SeedPairAsync(2, "PerformanceMonitorDarling", "PerformanceMonitorDarling", blockingSpid: 60);
        await SeedSnapshotPairAsync(1, "OrderEntry", "OrderEntry");
        await SeedSnapshotPairAsync(2, "PerformanceMonitorLite", "PerformanceMonitorLite", blockingSpid: 60);

        var finding = await DrillDownAsync("BLOCKING_EVENTS");

        var spids = Section(finding, "top_blocking_chains").Select(r => r!["blocked_spid"]!.GetValue<int>()).Order().ToArray();
        Assert.Equal([101, 201], spids);
    }

    [Fact]
    public async Task TheReconstructedChains_ShowTheRealChain_NotTheMonitorsOwn()
    {
        await SeedPairAsync(1, "OrderEntry", "OrderEntry");
        await SeedPairAsync(2, "PerformanceMonitorDarling", "PerformanceMonitorDarling", blockingSpid: 60);
        await SeedSnapshotPairAsync(3, "OrderEntry", "OrderEntry", blockingSpid: 70);
        await SeedSnapshotPairAsync(4, "PerformanceMonitorLite", "PerformanceMonitorLite", blockingSpid: 80);

        var finding = await DrillDownAsync("BLOCKING_CHAIN");

        var apexes = Section(finding, "reconstructed_blocking_chains").Select(c => c!["apex_spid"]!.GetValue<int>()).Order().ToArray();
        Assert.Equal([50, 70], apexes);
    }
}
