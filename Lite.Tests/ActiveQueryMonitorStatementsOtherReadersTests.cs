/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
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
/// #7573: the collector stores the monitor's own sessions again so Active Queries can show them behind its switch. Every OTHER read of the
/// query snapshot rows (the wait drill-down, the long-running query alert, the who-drove-it reads, the same-statement pile-up reader) has
/// to leave them out the way it did while the collector dropped them (#7416). The seed is six snapshot rows sharing one query hash, three
/// the monitor's and three a user's (a similar name, an ordinary application, a NULL program name); each read must show only the three.
/// </summary>
public sealed class ActiveQueryMonitorStatementsOtherReadersTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TestServer";
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public ActiveQueryMonitorStatementsOtherReadersTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose() => _seedConn?.Dispose();

    private static readonly string?[] Apps =
    {
        "PerformanceMonitorDarling", "PerformanceMonitorLite", "PerformanceMonitorDarling-Remediation",
        "MyPerformanceMonitorTool", "Orders.Api", null,
    };

    private DateTime _at;

    private async Task SeedAsync()
    {
        var now = DateTime.UtcNow;
        _at = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc).AddSeconds(-30);
        for (var i = 0; i < Apps.Length; i++)
        {
            await ExecAsync(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status,
     blocking_session_id, wait_type, cpu_time_ms, total_elapsed_time_ms, program_name, request_id, query_hash)
VALUES ($1, $2, $3, $4, $5, 'TestDb', $6, 'running', 0, 'LCK_M_X', 1000, 600000, $7, 0, '0xABC')",
                _nextId--, DateTime.SpecifyKind(_at, DateTimeKind.Unspecified), _serverId, ServerName, 60 + i, "SELECT 1", Apps[i]);
        }
    }

    /* The old reads had no program predicate and returned all six rows, so each assertion below fails on them. */
    [Fact]
    public async Task TheWaitDrillDownReads_LeaveTheMonitorOut()
    {
        await SeedAsync();

        var byWait = await _dataService.GetQuerySnapshotsByWaitTypeAsync(_serverId, "LCK_M_X", hoursBack: 1);
        Assert.Equal(3, byWait.Count);
        Assert.DoesNotContain(byWait, r => r.ProgramName.StartsWith("PerformanceMonitor", StringComparison.Ordinal));

        var inRange = await _dataService.GetAllQuerySnapshotsInRangeAsync(_serverId, hoursBack: 1);
        Assert.Equal(3, inRange.Count);
        Assert.DoesNotContain(inRange, r => r.ProgramName.StartsWith("PerformanceMonitor", StringComparison.Ordinal));
    }

    /* The wait drill-down's chain view walks GetAllQuerySnapshotsInRangeAsync to find each waiter's head blocker. The monitor's session 60 heads the chain and
       the application's session 61 waits on it. The grid read leaves 60 out, so the walk finds no head; the chain walk passes includeMonitor and reaches 60. */
    [Fact]
    public async Task TheWaitDrillDownChainWalk_ReachesAHeadBlockerThatIsAMonitorSession()
    {
        var at = DateTime.SpecifyKind(DateTime.UtcNow.AddSeconds(-30), DateTimeKind.Unspecified);
        await ExecAsync(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status,
     blocking_session_id, wait_type, cpu_time_ms, total_elapsed_time_ms, program_name, request_id, query_hash)
VALUES ($1, $2, $3, $4, 60, 'TestDb', 'SELECT head', 'running', 0, NULL, 1000, 600000, 'PerformanceMonitorLite', 0, '0xABC')",
            _nextId--, at, _serverId, ServerName);
        await ExecAsync(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status,
     blocking_session_id, wait_type, cpu_time_ms, total_elapsed_time_ms, program_name, request_id, query_hash)
VALUES ($1, $2, $3, $4, 61, 'TestDb', 'SELECT victim', 'running', 60, 'LCK_M_X', 1000, 600000, 'Orders.Api', 0, '0xABD')",
            _nextId--, at, _serverId, ServerName);

        static PerformanceMonitor.Ui.WaitDrillDownHelper.SnapshotInfo Info(QuerySnapshotRow r) => new()
        {
            SessionId = r.SessionId,
            BlockingSessionId = r.BlockingSessionId,
            CollectionTime = r.CollectionTime,
        };

        var waiters = (await _dataService.GetQuerySnapshotsByWaitTypeAsync(_serverId, "LCK_M_X", hoursBack: 1)).Select(Info).ToList();
        Assert.Equal(61, Assert.Single(waiters).SessionId);

        var grid = (await _dataService.GetAllQuerySnapshotsInRangeAsync(_serverId, hoursBack: 1)).Select(Info);
        Assert.Empty(PerformanceMonitor.Ui.WaitDrillDownHelper.WalkBlockingChains(waiters, grid));

        var all = (await _dataService.GetAllQuerySnapshotsInRangeAsync(_serverId, hoursBack: 1, includeMonitor: true)).Select(Info);
        var head = Assert.Single(PerformanceMonitor.Ui.WaitDrillDownHelper.WalkBlockingChains(waiters, all));
        Assert.Equal(60, head.SessionId);
        Assert.Equal(1, head.BlockedSessionCount);
    }

    [Fact]
    public async Task TheLongRunningQueryAlertRead_LeavesTheMonitorOut()
    {
        await SeedAsync();

        var result = await _dataService.GetLongRunningQueriesAsync(_serverId, thresholdMinutes: 1, maxResults: 50);
        Assert.Equal(3, result.Sessions.Count);
        Assert.DoesNotContain(result.Sessions, s => s.ProgramName.StartsWith("PerformanceMonitor", StringComparison.Ordinal));
    }

    [Fact]
    public async Task TheWhoDroveItReads_LeaveTheMonitorOut()
    {
        await SeedAsync();
        var start = _at.AddMinutes(-5);
        var end = _at.AddMinutes(5);

        var callers = await _dataService.GetLoadByCallerAsync(_serverId, start, end, groups: 20, statements: 3);
        Assert.Equal(3, callers.Count);
        Assert.DoesNotContain(callers, c => c.ProgramName.StartsWith("PerformanceMonitor", StringComparison.Ordinal));

        var pileups = await _dataService.GetQueryPileupsAsync(_serverId, start, end, minSessions: 2, limit: 10);
        Assert.Equal(3, Assert.Single(pileups).Sessions);
    }

    [Fact]
    public async Task ThePileupSnapshotReader_LeavesTheMonitorOut()
    {
        await SeedAsync();

        var rows = await new PileupSnapshotReader(_duckDb).ReadWindowAsync(_serverId, _at.AddMinutes(-5), CancellationToken.None);
        Assert.Equal(3, rows.Count);
    }

    /* The ACTIVE_QUERIES fact and the CPU spike drill-down's queries count only the application's rows. The old reads had no program
       predicate and counted all six. */
    [Fact]
    public async Task TheAnalysisFactAndTheSpikeDrillDown_CountOnlyTheApplicationsRows()
    {
        await SeedAsync();
        await ExecAsync(@"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time, sample_time_utc, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $2, $5, 95, 0)",
            _nextId--, DateTime.SpecifyKind(_at, DateTimeKind.Unspecified), _serverId, ServerName, _at);

        var context = new AnalysisContext
        {
            ServerId = _serverId,
            ServerName = ServerName,
            TimeRangeStart = _at.AddMinutes(-5),
            TimeRangeEnd = _at.AddMinutes(5),
        };

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context);
        var fact = Assert.Single(facts, f => f.Key == "ACTIVE_QUERIES");
        Assert.Equal(3, fact.Metadata["total_snapshots"]);
        Assert.Equal(3, fact.Metadata["distinct_sessions"]);

        var finding = new AnalysisFinding { RootFactKey = "CPU_SPIKE", StoryPath = "CPU_SPIKE", PathKeys = ["CPU_SPIKE"], Severity = 1.0 };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], context);
        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown!.TryGetValue("queries_at_spike", out var raw), "no queries_at_spike drill-down");
        var sessions = System.Text.Json.JsonSerializer.SerializeToElement(raw).EnumerateArray()
            .Select(e => e.GetProperty("session_id").GetInt32()).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 63, 64, 65 }, sessions);
    }

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
