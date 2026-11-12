using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7892: Lite's <c>top_blocking_chains</c> drill-down lists WAITS, the unit the BLOCKING_EVENTS fact counts, not the blocked-process
/// reports SQL Server writes for a still-blocked session every threshold seconds. A wait reported eight times is one row, carrying its
/// longest report's wait time and the number of reports it collapsed, and the DMV snapshot arm folds its samples the same way.
/// </summary>
public class TopBlockingChainsWaitsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -7892_02;
    private static readonly DateTime WindowStart = new(2026, 3, 4, 10, 0, 0);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(4);
    private static readonly string[] ScopeGp = ["GP"];

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _conn;
    private long _nextId = -1;

    public TopBlockingChainsWaitsTests(SharedDuckDbFixture fixture)
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

    private Task BprAsync(DateTime eventTime, long waitMs, int blocked, int blocker, string database) => ExecAsync(
        "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, database_name, wait_time_ms, blocked_spid, blocking_spid, lock_mode) VALUES ($1,$2,$2,$3,'TestServer',$4,$5,$6,$7,'X')",
        _nextId--, eventTime, ServerId, database, waitMs, blocked, blocker);

    private Task DmvAsync(DateTime eventTime, long waitMs, int blocked, int blocker, string database) => ExecAsync(
        "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, event_time, server_id, server_name, database_name, monitor_loop, wait_time_ms, blocked_spid, blocking_spid, lock_mode) VALUES ($1,$2,$2,$3,'TestServer',$4,1,$5,$6,$7,'X')",
        _nextId--, eventTime, ServerId, database, waitMs, blocked, blocker);

    /// <summary>Thirteen rows of three waits. Wait A: session 51 blocked by 60 in GP, reported eight times five seconds apart, each report five
    /// seconds longer than the last, so every report derives the same wait start. Wait B: session 52 blocked by 60 in HS, reported twice the
    /// same way. Wait C: a DMV snapshot sees session 53 blocked by 61 in HS on three consecutive one-minute samples.</summary>
    private async Task SeedAsync()
    {
        var startA = WindowStart.AddHours(1);
        for (var i = 1; i <= 8; i++)
            await BprAsync(startA.AddSeconds(5 * i), 5000L * i, blocked: 51, blocker: 60, "GP");
        var startB = WindowStart.AddHours(2);
        for (var i = 1; i <= 2; i++)
            await BprAsync(startB.AddSeconds(5 * i), 5000L * i, blocked: 52, blocker: 60, "HS");
        var startC = WindowStart.AddHours(3);
        for (var i = 1; i <= 3; i++)
            await DmvAsync(startC.AddMinutes(i), 60000L * i, blocked: 53, blocker: 61, "HS");
    }

    private async Task<List<JsonElement>> DrillAsync(IReadOnlyList<string>? scope)
    {
        var context = new AnalysisContext
        {
            SeparatelyMonitoredDatabases = scope, ServerId = ServerId, ServerName = "TestServer", TimeRangeStart = WindowStart, TimeRangeEnd = WindowEnd,
        };
        var finding = new AnalysisFinding { RootFactKey = "BLOCKING_EVENTS", StoryPath = "BLOCKING_EVENTS", PathKeys = ["BLOCKING_EVENTS"], Severity = 1.0 };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], context);
        Assert.NotNull(finding.DrillDown);
        return JsonSerializer.SerializeToElement(finding.DrillDown["top_blocking_chains"]).EnumerateArray().ToList();
    }

    [Fact]
    public async Task TopBlockingChains_ListEachWaitOnce_WithItsLongestReportAndItsReportCount()
    {
        await SeedAsync();

        var rows = await DrillAsync(null);

        /* Three waits, worst first: a list of reports would be five escalating reports of waits C and A. */
        Assert.Equal([180000L, 40000L, 10000L], rows.Select(r => r.GetProperty("wait_time_ms").GetInt64()).ToArray());
        Assert.Equal([53, 51, 52], rows.Select(r => r.GetProperty("blocked_spid").GetInt32()).ToArray());
        Assert.Equal([3L, 8L, 2L], rows.Select(r => r.GetProperty("report_count").GetInt64()).ToArray());
    }

    /// <summary>The Azure SQL Database master form: the waits of a database monitored as its own target are left out, and the rest still
    /// fold to one row per wait.</summary>
    [Fact]
    public async Task TopBlockingChains_SkippingSeparatelyMonitored_ListEachRemainingWaitOnce()
    {
        await SeedAsync();

        var rows = await DrillAsync(ScopeGp);

        Assert.Equal([180000L, 10000L], rows.Select(r => r.GetProperty("wait_time_ms").GetInt64()).ToArray());
        Assert.Equal([53, 52], rows.Select(r => r.GetProperty("blocked_spid").GetInt32()).ToArray());
        Assert.Equal([3L, 2L], rows.Select(r => r.GetProperty("report_count").GetInt64()).ToArray());
    }
}
