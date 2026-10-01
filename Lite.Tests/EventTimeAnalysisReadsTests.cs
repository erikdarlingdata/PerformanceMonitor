using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's analysis reads of blocked-process reports and deadlocks window on when the EVENT happened
/// (<c>event_time</c>, <c>deadlock_time</c>), not on when the collector stored it, so the facts count the same events
/// the grids show. Three rows per table: (a) the event happened 2 h before the window and was collected inside it,
/// (b) event and collection both inside, (c) the event inside the window and collected 30 min after its end. Only b and
/// c count. Covers the BLOCKING_EVENTS and DEADLOCKS facts, the drill-down's top deadlocks and top chains, the
/// anomaly detector's current counts, and the baseline bucket count (including a collection lag that crosses an hour).
/// </summary>
public class EventTimeAnalysisReadsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -4894_11;
    private static readonly DateTime WindowStart = new(2026, 3, 4, 10, 0, 0);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(4);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _conn;
    private long _nextId = -1;

    public EventTimeAnalysisReadsTests(SharedDuckDbFixture fixture)
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

    /// <summary>One event: its time and its collection time. The wait time is distinct per row so a list can be told apart.</summary>
    private Task BprAsync(DateTime eventTime, DateTime collected, long waitMs) => ExecAsync(
        @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, database_name)
          VALUES ($1,$2,$3,'TestServer',$4,$5,$6)",
        _nextId--, collected, ServerId, eventTime, waitMs, _seedDatabase);

    private Task DeadlockAsync(DateTime eventTime, DateTime collected, string victim) => ExecAsync(
        @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, database_name)
          VALUES ($1,$2,$3,'TestServer',$4,$5,$6)",
        _nextId--, collected, ServerId, eventTime, victim, _seedDatabase);

    /// <summary>The database the seeded rows carry: HS in the scoped cases (outside the GP scope, so they still count), NULL otherwise.</summary>
    private string? _seedDatabase;
    private static readonly string[] ScopeGp = ["GP"];

    /// <summary>The three rows, in BOTH tables: (a) happened before the window and was collected inside it,
    /// (b) happened and was collected inside it, (c) happened inside it and was collected after it ended.
    /// Waits 100 / 200 / 300, victims a / b / c.</summary>
    private async Task SeedAbcAsync()
    {
        var a = (Event: WindowStart.AddHours(-2), Collected: WindowStart.AddMinutes(30));
        var b = (Event: WindowStart.AddHours(1), Collected: WindowStart.AddHours(1).AddMinutes(5));
        var c = (Event: WindowEnd.AddMinutes(-10), Collected: WindowEnd.AddMinutes(30));
        await BprAsync(a.Event, a.Collected, 100);
        await BprAsync(b.Event, b.Collected, 200);
        await BprAsync(c.Event, c.Collected, 300);
        await DeadlockAsync(a.Event, a.Collected, "a");
        await DeadlockAsync(b.Event, b.Collected, "b");
        await DeadlockAsync(c.Event, c.Collected, "c");
    }

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

    private AnalysisContext Context(IReadOnlyList<string>? scope = null) => new()
    {
        SeparatelyMonitoredDatabases = scope,
        ServerId = ServerId,
        ServerName = "TestServer",
        TimeRangeStart = WindowStart,
        TimeRangeEnd = WindowEnd,
    };

    [Fact]
    public async Task BlockingEventsFact_CountsTheEventsThatHappenedInTheWindow_NotTheOnesCollectedInIt()
    {
        await SeedObservedWindowAsync();
        await SeedAbcAsync();
        /* A second late-collected report, so the event-time count (b + 2c = 3) differs from the collection-time
           count the old read gave (a + b = 2): with one row of each, both windows count two. */
        await BprAsync(WindowEnd.AddMinutes(-5), WindowEnd.AddMinutes(40), 400);

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());

        Assert.Equal(3.0, facts.First(f => f.Key == "BLOCKING_EVENTS").Metadata["event_count"]);
    }

    [Fact]
    public async Task DeadlocksFact_CountsTheEventsThatHappenedInTheWindow_NotTheOnesCollectedInIt()
    {
        await SeedObservedWindowAsync();
        await SeedAbcAsync();
        /* A second late-collected deadlock, for the same reason as the blocking fact: b + 2c = 3 by event time,
           a + b = 2 by collection time. */
        await DeadlockAsync(WindowEnd.AddMinutes(-5), WindowEnd.AddMinutes(40), "d");

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());

        Assert.Equal(3.0, facts.First(f => f.Key == "DEADLOCKS").Metadata["deadlock_count"]);
    }

    [Fact]
    public async Task DeadlocksFact_Scoped_CountsTheEventsThatHappenedInTheWindow_NotTheOnesCollectedInIt()
    {
        /* Rows are in HS, outside the GP scope, so they count; the scoped read still windows on the event time. */
        _seedDatabase = "HS";
        await SeedObservedWindowAsync();
        await SeedAbcAsync();
        await DeadlockAsync(WindowEnd.AddMinutes(-5), WindowEnd.AddMinutes(40), "d");

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context(ScopeGp));

        Assert.Equal(3.0, facts.First(f => f.Key == "DEADLOCKS").Metadata["deadlock_count"]);
    }

    [Fact]
    public async Task TopDeadlocks_Scoped_ListTheEventsThatHappenedInTheWindow()
    {
        _seedDatabase = "HS";
        await SeedAbcAsync();

        var rows = await DrillAsync("DEADLOCKS", "top_deadlocks", ScopeGp);

        Assert.Equal(["c", "b"], rows.EnumerateArray().Select(r => r.GetProperty("victim").GetString()).OrderByDescending(v => v).ToArray());
    }

    private async Task<JsonElement> DrillAsync(string factKey, string section, IReadOnlyList<string>? scope = null)
    {
        var finding = new AnalysisFinding { RootFactKey = factKey, StoryPath = factKey, PathKeys = [factKey], Severity = 1.0 };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context(scope));
        Assert.NotNull(finding.DrillDown);
        return JsonSerializer.SerializeToElement(finding.DrillDown[section]);
    }

    [Fact]
    public async Task TopDeadlocks_ListTheEventsThatHappenedInTheWindow()
    {
        await SeedAbcAsync();

        var rows = await DrillAsync("DEADLOCKS", "top_deadlocks");

        Assert.Equal(["c", "b"], rows.EnumerateArray().Select(r => r.GetProperty("victim").GetString()).OrderByDescending(v => v).ToArray());
        Assert.Equal(2, rows.GetArrayLength());
    }

    [Fact]
    public async Task TopBlockingChains_ListTheBlockedProcessReportsThatHappenedInTheWindow()
    {
        await SeedAbcAsync();

        var rows = await DrillAsync("BLOCKING_EVENTS", "top_blocking_chains");

        Assert.Equal([300L, 200L], rows.EnumerateArray().Select(r => r.GetProperty("wait_time_ms").GetInt64()).ToArray());
    }

    /* The detector fires on 5 or more: one a row and three each of b and c are 6 by event time but only 4 by collection time. */
    [Fact]
    public async Task AnomalyDetector_CurrentCounts_AreEventsThatHappenedInTheWindow()
    {
        await ExecAsync(
            @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time,
                sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, 'TestServer', $2, 10, 2)",
            _nextId--, WindowEnd.AddDays(-1), ServerId);
        for (var i = 0; i < 3; i++)
        {
            if (i == 0) await BprAsync(WindowStart.AddHours(-2).AddMinutes(i), WindowStart.AddMinutes(30 + i), 100);
            await BprAsync(WindowStart.AddHours(1).AddMinutes(i), WindowStart.AddHours(1).AddMinutes(5 + i), 200);
            await BprAsync(WindowEnd.AddMinutes(-10 - i), WindowEnd.AddMinutes(30 + i), 300);
            if (i == 0) await DeadlockAsync(WindowStart.AddHours(-2).AddMinutes(i), WindowStart.AddMinutes(30 + i), "a");
            await DeadlockAsync(WindowStart.AddHours(1).AddMinutes(i), WindowStart.AddHours(1).AddMinutes(5 + i), "b");
            await DeadlockAsync(WindowEnd.AddMinutes(-10 - i), WindowEnd.AddMinutes(30 + i), "c");
        }

        var anomalies = await new AnomalyDetector(_duckDb, new BaselineProvider(_duckDb)).DetectAnomaliesAsync(Context());

        foreach (var key in new[] { "ANOMALY_BLOCKING_SPIKE", "ANOMALY_DEADLOCK_SPIKE" })
        {
            var spike = anomalies.FirstOrDefault(f => f.Key == key);
            Assert.True(spike is not null,
                $"no {key} was raised: the current-window count stayed under the spike floor of 5 (a collection-time read counts 4)");
            Assert.Equal(6.0, spike!.Value);
        }
    }

    private static readonly DateTime AnalysisTime = new(2026, 3, 4, 14, 0, 0);

    private async Task<IReadOnlyDictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>> BaselineAsync(string metric)
    {
        var map = await new BaselineProvider(_duckDb).GetBucketMapAsync(ServerId, metric, AnalysisTime, AnalysisTime.AddHours(1));
        return map.Buckets;
    }

    private async Task SeedLogAsync(string collector) => await ExecAsync(
        @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
          SELECT -9000000000 - CAST(row_number() OVER () AS BIGINT), $1, 'TestServer', $2, t, 'SUCCESS'
          FROM generate_series($3::TIMESTAMP, $4::TIMESTAMP, INTERVAL 15 MINUTE) AS g(t)",
        ServerId, collector, AnalysisTime.AddDays(-35), AnalysisTime.AddMinutes(-15));

    /// <summary>
    /// The window is [Feb 2 00:00, Mar 4 00:00). (a) happened Sunday Feb 1 22:00 and was collected Monday Feb 2 00:10,
    /// inside; (b) Tuesday Feb 10 14:20 collected 14:25; (c) Tuesday Mar 3 23:30 collected Mar 4 00:10, outside. Only b
    /// and c fall in the window by event time: (14, Tue) and (23, Tue) each hold one event over five covered Tuesdays.
    /// (d) Tuesday Feb 17 10:58, collected 11:03, lands in the 10:00 bucket, not the 11:00 one.
    /// </summary>
    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task BaselineBuckets_CountEventsByWhenTheyHappened(string family)
    {
        var (metric, collector) = family == "blocking" ? (MetricNames.Blocking, "blocked_process_report") : (MetricNames.Deadlock, "deadlocks");
        await SeedLogAsync(collector);
        var events = new[]
        {
            (new DateTime(2026, 2, 1, 22, 0, 0), new DateTime(2026, 2, 2, 0, 10, 0)),
            (new DateTime(2026, 2, 10, 14, 20, 0), new DateTime(2026, 2, 10, 14, 25, 0)),
            (new DateTime(2026, 3, 3, 23, 30, 0), new DateTime(2026, 3, 4, 0, 10, 0)),
            (new DateTime(2026, 2, 17, 10, 58, 0), new DateTime(2026, 2, 17, 11, 3, 0)),
        };
        foreach (var (happened, collected) in events)
            if (family == "blocking") await BprAsync(happened, collected, 1000);
            else await DeadlockAsync(happened, collected, "v");

        var buckets = await BaselineAsync(metric);
        var tuesday = (int)DayOfWeek.Tuesday;

        Assert.Equal(0.2, buckets[(14, tuesday)].Mean, 6);
        Assert.Equal(0.2, buckets[(23, tuesday)].Mean, 6);
        Assert.Equal(0.2, buckets[(10, tuesday)].Mean, 6);
        Assert.Equal(0.0, buckets[(11, tuesday)].Mean, 6);
        Assert.Equal(0.0, buckets[(0, (int)DayOfWeek.Monday)].Mean, 6);
        Assert.Equal(0.0, buckets[(22, (int)DayOfWeek.Sunday)].Mean, 6);
    }
}
