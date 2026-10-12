using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5644 W1: Lite's DuckDB form of the blocking-wait rule (<see cref="BlockingWaits.WaitsCteSql"/> with
/// <c>epoch_ms</c>) run for real against seeded blocked-process reports, and checked against the C# rule
/// (<see cref="BlockingWaits.Collapse{T}"/>) over the very same rows. Two readers use the SQL: the analysis
/// BLOCKING_EVENTS fact and the overview card's last-hour count (which Lite's MCP summary reads).
/// One wait is one blocked session, blocker and wait start; SQL Server writes a report for a still-blocked
/// session every threshold seconds, so a one-minute wait is about eleven report rows.
/// </summary>
[Collection(SeparatelyMonitoredProviderCollection.Name)]
public class BlockingWaitDuckDbTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -5644_11;

    private readonly DuckDbInitializer _duckDb;
    private readonly DateTime _windowEnd = DateTime.UtcNow;
    private readonly DateTime _windowStart;
    private DuckDBConnection? _conn;
    private long _nextId = -1;
    private readonly List<Report> _seeded = new();

    private sealed record Report(int Blocked, int Blocker, DateTime EventTime, long WaitMs);

    public BlockingWaitDuckDbTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _windowStart = _windowEnd.AddHours(-4);
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

    /* Wait-stats deltas give the fact collector its observed time: four hours, a collection every 15 minutes. */
    private async Task SeedObservedWindowAsync()
    {
        for (var i = 0; i <= 16; i++)
            await ExecAsync(
                @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type,
                    wait_time_ms, signal_wait_time_ms, waiting_tasks_count,
                    delta_wait_time_ms, delta_signal_wait_time_ms, delta_waiting_tasks)
                  VALUES ($1, $2, $3, 'TestServer', 'CXPACKET', 1000, 0, 1, $4, 0, $4)",
                _nextId--, _windowStart.AddMinutes(15 * i), ServerId, i == 0 ? 0L : 1000L);
    }

    /// <summary>
    /// One wait as SQL Server reports it: <paramref name="reports"/> rows, one per threshold interval, the k-th
    /// at <c>start + (k + 1) * interval</c> carrying wait_time_ms = (k + 1) * interval. <paramref name="jitterMs"/>
    /// moves each report's wait time off the exact value, alternately up and down, so consecutive derived starts differ by twice that.
    /// </summary>
    private async Task SeedWaitAsync(
        int blocked, int blocker, DateTime start, int reports, int intervalSeconds = 5, int jitterMs = 0, bool dmv = false)
    {
        for (var k = 0; k < reports; k++)
        {
            var waitMs = ((k + 1) * intervalSeconds * 1000L) + (k % 2 == 0 ? jitterMs : -jitterMs);
            var eventTime = start.AddSeconds((k + 1) * intervalSeconds);
            _seeded.Add(new Report(blocked, blocker, eventTime, waitMs));
            if (dmv)
                await ExecAsync(
                    "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, event_time, server_id, server_name, database_name, monitor_loop, wait_time_ms, blocked_spid, blocking_spid) VALUES ($1,$2,$2,$3,'TestServer','db',1,$4,$5,$6)",
                    _nextId--, eventTime, ServerId, waitMs, blocked, blocker);
            else
                await ExecAsync(
                    "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, database_name, wait_time_ms, blocked_spid, blocking_spid) VALUES ($1,$2,$2,$3,'TestServer','db',$4,$5,$6)",
                    _nextId--, eventTime, ServerId, waitMs, blocked, blocker);
        }
    }

    /* The C# rule over exactly the rows that were seeded. */
    private int CSharpWaitCount() => BlockingWaits.Collapse(
        _seeded, _ => "xe", r => r.Blocked, r => r.Blocker, r => (DateTime?)r.EventTime, r => r.WaitMs).Count;

    private async Task<Fact> BlockingFactAsync()
    {
        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = "TestServer",
            TimeRangeStart = _windowStart,
            TimeRangeEnd = _windowEnd,
        };
        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context);
        return facts.First(f => f.Key == "BLOCKING_EVENTS");
    }

    private async Task<int> OverviewCountAsync() =>
        (await new LocalDataService(_duckDb).GetServerSummaryAsync(ServerId, "TestServer", registeredAtUtc: null))!.BlockingCount;

    /* Seed times sit 5-50 minutes back, inside both the analysis window and the overview's last hour. */
    private DateTime Ago(int minutes) => _windowEnd.AddMinutes(-minutes);

    private async Task AssertAllThreeAgreeAsync(int expectedWaits)
    {
        await SeedObservedWindowAsync();
        Assert.Equal(expectedWaits, CSharpWaitCount());
        Assert.Equal(expectedWaits, (int)(await BlockingFactAsync()).Metadata["event_count"]);
        Assert.Equal(expectedWaits, await OverviewCountAsync());
    }

    [Fact]
    public async Task SeveralReportsOfOneWait_CountOnce()
    {
        await SeedWaitAsync(52, 51, Ago(30), reports: 11);

        await AssertAllThreeAgreeAsync(1);

        /* The longest wait time is the one kept: the 11th report, 55 s. */
        Assert.Equal(55_000.0, (await BlockingFactAsync()).Metadata["max_wait_time_ms"]);
    }

    [Fact]
    public async Task TheAuditsElevenNineNineAndSixReports_AreFourWaits()
    {
        await SeedWaitAsync(52, 50, Ago(45), reports: 11);
        await SeedWaitAsync(53, 50, Ago(40), reports: 9);
        await SeedWaitAsync(54, 50, Ago(35), reports: 9);
        await SeedWaitAsync(52, 50, Ago(20), reports: 6);   /* the same pair again, a later wait */

        await AssertAllThreeAgreeAsync(4);
    }

    [Fact]
    public async Task ReportsWhoseDerivedStartsJitterWithinTwoSeconds_AreOneWait()
    {
        await SeedWaitAsync(52, 51, Ago(30), reports: 9, jitterMs: 900);

        await AssertAllThreeAgreeAsync(1);
    }

    [Fact]
    public async Task ReportsWhoseDerivedStartsDriftMoreThanTwoSeconds_AreTwoWaits()
    {
        /* Same pair, the second wait starting 6 s after the first (the threshold is at least 5 s). */
        await SeedWaitAsync(52, 51, Ago(30), reports: 4);
        await SeedWaitAsync(52, 51, Ago(30).AddSeconds(6), reports: 4);

        await AssertAllThreeAgreeAsync(2);
    }

    [Fact]
    public async Task OneBlockerWithTwoBlockedSessions_IsTwoWaits_AndTwoBlockersOneBlocked_IsTwoToo()
    {
        await SeedWaitAsync(52, 51, Ago(30), reports: 6);
        await SeedWaitAsync(53, 51, Ago(30), reports: 6);
        await SeedWaitAsync(52, 60, Ago(30), reports: 6);

        await AssertAllThreeAgreeAsync(3);
    }

    [Fact]
    public async Task TheDmvSnapshotFallback_CountsWaitsToo()
    {
        /* No blocked-process reports at all: the overview falls back to the always-on DMV snapshots. */
        await SeedWaitAsync(52, 51, Ago(30), reports: 11, dmv: true);
        await SeedWaitAsync(53, 51, Ago(20), reports: 6, dmv: true);

        Assert.Equal(2, CSharpWaitCount());
        Assert.Equal(2, await OverviewCountAsync());
    }
}
