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
        int blocked, int blocker, DateTime start, int reports, int intervalSeconds = 5, int jitterMs = 0, bool dmv = false, int intervalMs = 0)
    {
        for (var k = 0; k < reports; k++)
        {
            var step = intervalMs > 0 ? intervalMs : intervalSeconds * 1000;
            var waitMs = ((k + 1) * step * 1L) + (k % 2 == 0 ? jitterMs : -jitterMs);
            var eventTime = start.AddMilliseconds((k + 1) * step);
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

    /* ── #5644 final review: the server tab badge and the daily summary count waits too ── */

    private async Task<int> BadgeBlockingCountAsync() =>
        (await new LocalDataService(_duckDb).GetAlertCountsAsync(ServerId, hoursBack: 2, fromDate: null, toDate: null)).blockingCount;

    /* The day buckets are UTC days of the wait's longest report; a run across midnight is two buckets, so sum them. */
    private async Task<long> DailySummaryBlockingEventsAsync()
    {
        var today = DateTime.UtcNow.Date;
        var rows = await new LocalDataService(_duckDb).GetDailySummaryRangeAsync(ServerId, today.AddDays(-1), today.AddDays(2));
        return rows.Sum(r => r.BlockingEvents);
    }

    [Fact]
    public async Task TheServerTabBadgeAndTheDailySummary_CountWaitsNotReports()
    {
        /* Nine reports of one wait, then six of another: 15 report rows, 2 waits. */
        await SeedWaitAsync(52, 51, Ago(30), reports: 9);
        await SeedWaitAsync(53, 51, Ago(20), reports: 6);

        Assert.Equal(2, CSharpWaitCount());
        Assert.Equal(2, await BadgeBlockingCountAsync());
        Assert.Equal(2, await DailySummaryBlockingEventsAsync());
        Assert.Equal(2, await OverviewCountAsync());
    }

    [Fact]
    public async Task TheServerTabBadgeAndTheDailySummary_CountWaitsOnTheDmvFallbackToo()
    {
        await SeedWaitAsync(52, 51, Ago(30), reports: 11, dmv: true);
        await SeedWaitAsync(53, 51, Ago(20), reports: 6, dmv: true);

        Assert.Equal(2, await BadgeBlockingCountAsync());
        Assert.Equal(2, await DailySummaryBlockingEventsAsync());
    }

    /// <summary>#5644 review: one long wait written as 130 reports (a report every 50 ms, so the longest block is 6.5 s and
    /// the wait arm stays out of it) is ONE blocking event, which the overview card, the badge and the MCP counts show,
    /// but the band's count tiers (5 and 20 per server-hour) were measured in reports, so the day bands Warning on
    /// 130 / 24 h = 5.4 per hour. On the waits-banded head the day read 1 event over 24 h and came out Healthy. The
    /// overview card's last-hour count stays one wait, and that card does not band by the tiers at all (it is elevated
    /// on any blocking).</summary>
    [Fact]
    public async Task TheDailyBand_ReadsTheReportCount_WhileTheDisplayedCountsStayWaits()
    {
        var yesterday = DateTime.UtcNow.Date.AddDays(-1);
        await SeedWaitAsync(52, 51, yesterday.AddHours(2), reports: 130, intervalMs: 50);

        var row = Assert.Single(await new LocalDataService(_duckDb).GetDailySummaryRangeAsync(ServerId, yesterday, yesterday.AddDays(1)));

        Assert.Equal(1, row.BlockingEvents);
        Assert.Equal(130, row.BlockingReports);
        Assert.Equal(6_500, row.MaxBlockDurationMs);
        Assert.Equal(DailyHealthBand.Warning, row.HealthBand);
        Assert.Equal(1, CSharpWaitCount());
    }

    /// <summary>The report count follows the DMV snapshot fallback too (snapshot rows are what the band counted before waits).</summary>
    [Fact]
    public async Task TheDailyReportCount_FollowsTheDmvFallback()
    {
        var yesterday = DateTime.UtcNow.Date.AddDays(-1);
        await SeedWaitAsync(52, 51, yesterday.AddHours(2), reports: 11, dmv: true);
        await SeedWaitAsync(53, 51, yesterday.AddHours(3), reports: 6, dmv: true);

        var row = Assert.Single(await new LocalDataService(_duckDb).GetDailySummaryRangeAsync(ServerId, yesterday, yesterday.AddDays(1)));

        Assert.Equal(2, row.BlockingEvents);
        Assert.Equal(17, row.BlockingReports);
    }

    /* ── #5644 wave-1 review: sub-millisecond starts and NULL times ── */

    private sealed record Edge(int Blocked, int Blocker, DateTime? Time, long? Wait);

    /* The same edge cases Darling.Tests BlockingWaitCountTests.EdgeCaseReports runs on PostgreSQL: pair 1/2 two waits
       (derived starts 2000.4 ms apart), 3/4 one (exactly 2000.0 ms), 5/6 four (NULL event time, NULL wait time and
       both NULL are waits of their own), 7/8 one (a tie on wait time keeps the later report). */
    private static List<Edge> EdgeReports()
    {
        var t0 = new DateTime(2026, 10, 9, 9, 0, 0, DateTimeKind.Unspecified);
        DateTime At(long ticks) => t0.AddTicks(ticks);
        const long Second = TimeSpan.TicksPerSecond;
        return new List<Edge>
        {
            new(1, 2, At(10 * Second), 10000), new(1, 2, At((12 * Second) + 4000), 10000),
            new(3, 4, At(10 * Second), 10000), new(3, 4, At(12 * Second), 10000),
            new(5, 6, At(10 * Second), 10000), new(5, 6, null, 5000), new(5, 6, At(20 * Second), null), new(5, 6, null, null),
            new(7, 8, At(10 * Second), 10000), new(7, 8, At((10 * Second) + 4000000), 10000),
        };
    }

    private static string EdgeKey(int blocked, int blocker, DateTime? time, long? wait) =>
        string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}|{1}|{2:yyyy-MM-dd HH:mm:ss.ffffff}|{3}", blocked, blocker, time, wait);

    [Fact]
    public async Task DuckDbSql_AgreesWithTheCSharpRule_OnSubMillisecondStartsAndNullTimes()
    {
        var rows = EdgeReports();
        var source = "SELECT v.blocked_spid, v.blocking_spid, v.event_time, v.wait_time_ms FROM (VALUES "
            + string.Join(",", rows.Select(r => string.Format(
                System.Globalization.CultureInfo.InvariantCulture, "({0},{1},CAST({2} AS TIMESTAMP),CAST({3} AS BIGINT))",
                r.Blocked, r.Blocker,
                r.Time is null ? "NULL" : $"'{r.Time:yyyy-MM-dd HH:mm:ss.ffffff}'",
                r.Wait is null ? "NULL" : r.Wait.Value.ToString(System.Globalization.CultureInfo.InvariantCulture))))
            + ") AS v(blocked_spid, blocking_spid, event_time, wait_time_ms)";

        var fromSql = new List<string>();
        using (var readLock = _duckDb.AcquireReadLock())
        {
            _conn ??= _duckDb.CreateConnection();
            if (_conn.State != System.Data.ConnectionState.Open) await _conn.OpenAsync();
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = "WITH " + BlockingWaits.WaitsCteSql(BlockingWaits.SqlDialect.DuckDb, "waits", source)
                + " SELECT blocked_spid, blocking_spid, event_time, wait_time_ms FROM waits";
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                fromSql.Add(EdgeKey(
                    Convert.ToInt32(reader.GetValue(0)), Convert.ToInt32(reader.GetValue(1)),
                    reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                    reader.IsDBNull(3) ? null : Convert.ToInt64(reader.GetValue(3))));
        }

        var fromCSharp = BlockingWaits.Collapse(rows, _ => "xe", r => r.Blocked, r => r.Blocker, r => r.Time, r => r.Wait)
            .Select(w => EdgeKey(w.Blocked, w.Blocker, w.Time, w.Wait));
        Assert.Equal(fromCSharp.OrderBy(k => k, StringComparer.Ordinal), fromSql.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(8, fromSql.Count);
    }
}
