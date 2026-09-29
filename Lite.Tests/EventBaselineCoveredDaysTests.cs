using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4731: the blocking and deadlock baselines count the hours collection covered. A slot is one local (date, hour);
/// it is covered when the event's OWN collector logged a SUCCESS run in it, or when it holds events. A bucket's mean
/// is its events over its covered days, so a quiet hour of a quiet month is a row with mean 0 (a measured zero) and
/// not a missing row, and an hour with events on two of five covered days divides by five, not two.
///
/// <para>The window is [Feb 2 14:00, Mar 4 14:00) for the analysis time Wed Mar 4 14:00 (Feb 2 is a Monday). The
/// bucket under test is Tuesday 14:00: Feb 3, 10, 17, 24 and Mar 3 make five covered days; the whole hour of day
/// 14 makes thirty, which is what the HourOnly tier pools and what the detector judges the analysis hour against.</para>
/// </summary>
public class EventBaselineCoveredDaysTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -4731_02;
    private const int Tuesday = (int)DayOfWeek.Tuesday;
    private const int Hour = 14;
    private static readonly DateTime AnalysisTime = new(2026, 3, 4, 14, 0, 0);
    private static readonly DateTime[] Tuesdays =
    [
        new(2026, 2, 3, 14, 0, 0), new(2026, 2, 10, 14, 0, 0), new(2026, 2, 17, 14, 0, 0),
        new(2026, 2, 24, 14, 0, 0), new(2026, 3, 3, 14, 0, 0),
    ];

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _conn;
    private long _nextId = -1;
    private long _nextLogBase = -1_000_000_000;

    public EventBaselineCoveredDaysTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _conn?.Dispose();

    private static (string Metric, string Collector, string Key, string Noun) Family(string name) => name == "blocking"
        ? (MetricNames.Blocking, "blocked_process_report", "ANOMALY_BLOCKING_SPIKE", "blocking events")
        : (MetricNames.Deadlock, "deadlocks", "ANOMALY_DEADLOCK_SPIKE", "deadlocks");

    /* ───────────────────────── seeding ───────────────────────── */

    private async Task ExecAsync(string sql, params object[] args)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_conn is null)
        {
            _conn = _duckDb.CreateConnection();
            await _conn.OpenAsync();
        }
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var arg in args)
            cmd.Parameters.Add(new DuckDBParameter { Value = arg });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One log row every 15 minutes from <paramref name="from"/> to <paramref name="to"/> inclusive.</summary>
    private Task SeedLogAsync(string collector, DateTime from, DateTime to, string status = "SUCCESS")
    {
        _nextLogBase -= 1_000_000;
        return ExecAsync(
            @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
              SELECT $1 - CAST(row_number() OVER () AS BIGINT), $2, 'TestServer', $3, t, $4
              FROM generate_series($5::TIMESTAMP, $6::TIMESTAMP, INTERVAL 15 MINUTE) AS g(t)",
            _nextLogBase, ServerId, collector, status, from, to);
    }

    /// <summary>Five weeks of SUCCESS runs every 15 minutes, ending just before the analysis hour: every slot of the window is covered.</summary>
    private Task SeedQuietMonthAsync(string collector) =>
        SeedLogAsync(collector, AnalysisTime.AddDays(-35), AnalysisTime.AddMinutes(-15));

    private Task SeedEventAsync(string family, DateTime at) => family == "blocking"
        ? ExecAsync(
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, wait_time_ms) VALUES ($1,$2,$3,'TestServer',1000)",
            _nextId--, at, ServerId)
        : ExecAsync(
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name) VALUES ($1,$2,$3,'TestServer')",
            _nextId--, at, ServerId);

    private async Task SeedEventsAsync(string family, DateTime at, int count)
    {
        for (var i = 0; i < count; i++)
            await SeedEventAsync(family, at.AddMinutes(10 + i));
    }

    private Task SeedServerClockAsync(int utcOffsetMinutes) => ExecAsync(
        @"INSERT INTO server_properties
            (collection_id, collection_time, server_id, server_name, edition, product_version, product_level,
             engine_edition, cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes, time_zone_id)
          VALUES ($1, $2, $3, 'TestServer', 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, $4, NULL)",
        _nextId--, new DateTime(2026, 1, 1), ServerId, utcOffsetMinutes);

    /* ───────────────────────── reading ───────────────────────── */

    private async Task<IReadOnlyDictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>> BucketsAsync(string metric)
    {
        var map = await new BaselineProvider(_duckDb).GetBucketMapAsync(ServerId, metric, AnalysisTime, AnalysisTime.AddHours(1));
        return map.Buckets;
    }

    private async Task<BaselineBucket?> RowAsync(string metric, int hour, int dow) =>
        (await BucketsAsync(metric)).TryGetValue((hour, dow), out var bucket) ? bucket : null;

    /// <summary>Spikes the analysis hour and runs the real detector; the gate wants any CPU row in the month.</summary>
    private async Task<Fact> SpikeAsync(string family, int events)
    {
        var (_, _, key, _) = Family(family);
        await ExecAsync(
            @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time,
                sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, 'TestServer', $2, 10, 2)",
            _nextId--, AnalysisTime.AddDays(-1), ServerId);
        await SeedEventsAsync(family, AnalysisTime, events);

        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = "TestServer",
            TimeRangeStart = AnalysisTime,
            TimeRangeEnd = AnalysisTime.AddHours(1),
        };
        var facts = await new AnomalyDetector(_duckDb, new BaselineProvider(_duckDb)).DetectAnomaliesAsync(context);
        return Assert.Single(facts, f => f.Key == key);
    }

    private static AdviceBlock Compose(Fact fact) =>
        FactAdvice.Compose(fact.Key, new Dictionary<string, Fact> { [fact.Key] = fact })!;

    /* ───────────────────────── (a) a quiet server ───────────────────────── */

    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task QuietServer_EveryHourOfWeek_IsARowWithMeanZero_NotAMissingRow(string family)
    {
        var (metric, collector, _, _) = Family(family);
        await SeedQuietMonthAsync(collector);

        var buckets = await BucketsAsync(metric);

        Assert.Equal(24 * 7, buckets.Count);
        var tuesday = buckets[(Hour, Tuesday)];
        Assert.Equal(0.0, tuesday.Mean);
        Assert.Equal(0.0, tuesday.StdDev);
        Assert.Equal(5L, tuesday.SampleCount);
        Assert.Equal(5L, tuesday.DistinctDays);
        Assert.Equal(4L, buckets[(Hour, (int)DayOfWeek.Wednesday)].SampleCount);
    }

    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task QuietServer_ASpikeIntoTheHour_IsAMeasuredZero_NotAFirstOccurrence(string family)
    {
        var (_, collector, _, noun) = Family(family);
        await SeedQuietMonthAsync(collector);

        var fact = await SpikeAsync(family, 12);

        Assert.Equal(1.0, fact.Metadata["baseline_zero_history"]);
        Assert.Equal(1.0, fact.Metadata["is_new"]);
        Assert.Equal(30.0, fact.Metadata["baseline_samples"]);
        var block = Compose(fact);
        Assert.Equal($"12 {noun} this window — against a month in which this hour saw none", block.Headline);
        Assert.Contains("a measured ZERO: 30 baseline samples across 30 distinct days", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("first occurrence", block.Headline + block.Investigation + block.Remediation, StringComparison.Ordinal);
    }

    /* ───────────────────────── (b) events on every covered day: unchanged ───────────────────────── */

    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task EventsOnEveryCoveredDay_KeepTheirPerDayMean(string family)
    {
        var (metric, collector, _, _) = Family(family);
        await SeedQuietMonthAsync(collector);
        foreach (var tuesday in Tuesdays)
            await SeedEventsAsync(family, tuesday, 3);

        var row = await RowAsync(metric, Hour, Tuesday);

        Assert.NotNull(row);
        Assert.Equal(3.0, row!.Mean);
        Assert.Equal(5L, row.SampleCount);
        Assert.Equal(5L, row.DistinctDays);
    }

    [Fact]
    public async Task SteadyEventsEveryDay_StayTrusted_AndTheSpikeReadsAsARatio()
    {
        await SeedQuietMonthAsync("blocked_process_report");
        for (var day = 0; day < 30; day++)
            await SeedEventsAsync("blocking", AnalysisTime.AddDays(-30 + day), 4);

        var fact = await SpikeAsync("blocking", 20);

        Assert.Equal(0.0, fact.Metadata["baseline_zero_history"]);
        Assert.Equal(0.0, fact.Metadata["is_new"]);
        Assert.Equal(4.0, fact.Metadata["baseline_rate"], 6);
        Assert.Equal(5.0, fact.Metadata["ratio"], 6);
        Assert.Contains("spiked to 5× its baseline", Compose(fact).Headline, StringComparison.Ordinal);
    }

    /* ───────────────────────── (c) events on 2 of 5 covered days: the divisor is 5 ───────────────────────── */

    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task EventsOnTwoOfFiveCoveredDays_DivideByFive_NotByTwo(string family)
    {
        var (metric, collector, _, _) = Family(family);
        await SeedQuietMonthAsync(collector);
        await SeedEventsAsync(family, Tuesdays[0], 3);
        await SeedEventsAsync(family, Tuesdays[3], 3);

        var row = await RowAsync(metric, Hour, Tuesday);

        Assert.NotNull(row);
        Assert.Equal(6.0 / 5.0, row!.Mean, 9);
        Assert.Equal(5L, row.SampleCount);
        Assert.Equal(5L, row.DistinctDays);
    }

    [Fact]
    public async Task RareEvents_NoLongerInflateTheBaseline_TheSpikeIsJudgedAgainstThirtyCoveredDays()
    {
        await SeedQuietMonthAsync("blocked_process_report");
        await SeedEventsAsync("blocking", Tuesdays[0], 8);
        await SeedEventsAsync("blocking", Tuesdays[3], 8);

        var fact = await SpikeAsync("blocking", 12);

        Assert.Equal(16.0 / 30.0, fact.Metadata["baseline_rate"], 9);
        Assert.Equal(12.0 / (16.0 / 30.0), fact.Metadata["ratio"], 9);
        Assert.Equal(0.0, fact.Metadata["is_new"]);
        Assert.Equal(0.0, fact.Metadata["baseline_zero_history"]);
    }

    /* ───────────────────────── (d), (e) which slots are covered ───────────────────────── */

    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task ASlotWithEventsButNoLogRow_StillCounts_AsCovered(string family)
    {
        var (metric, collector, _, _) = Family(family);
        foreach (var tuesday in Tuesdays.Take(4))
            await SeedLogAsync(collector, tuesday, tuesday.AddMinutes(45));
        await SeedEventsAsync(family, Tuesdays[4], 2); // the collector ran on Mar 3, its log row is gone

        var row = await RowAsync(metric, Hour, Tuesday);

        Assert.NotNull(row);
        Assert.Equal(5L, row!.SampleCount);
        Assert.Equal(2.0 / 5.0, row.Mean, 9);
    }

    [Fact]
    public async Task WithNoLogAtAll_OnlyTheEventSlotsAreCovered_AsBeforeTheLogWasConsulted()
    {
        await SeedEventsAsync("blocking", Tuesdays[0], 3);
        await SeedEventsAsync("blocking", Tuesdays[3], 3);

        var buckets = await BucketsAsync(MetricNames.Blocking);

        var row = Assert.Single(buckets).Value;
        Assert.Equal((Hour, Tuesday), (row.HourOfDay, row.DayOfWeek));
        Assert.Equal(3.0, row.Mean);
        Assert.Equal(2L, row.SampleCount);
    }

    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task ASlotWhereTheCollectorOnlyFailed_IsNotCovered(string family)
    {
        var (metric, collector, _, _) = Family(family);
        foreach (var tuesday in Tuesdays.Take(4))
            await SeedLogAsync(collector, tuesday, tuesday.AddMinutes(45));
        await SeedLogAsync(collector, Tuesdays[4], Tuesdays[4].AddMinutes(45), status: "ERROR");

        var row = await RowAsync(metric, Hour, Tuesday);

        Assert.NotNull(row);
        Assert.Equal(4L, row!.SampleCount);
        Assert.Equal(0.0, row.Mean);
    }

    [Theory]
    [InlineData("blocking", "deadlocks")]
    [InlineData("deadlock", "blocked_process_report")]
    public async Task AnotherCollectorsSuccessfulRuns_DoNotCoverTheEventsSlots(string family, string otherCollector)
    {
        var (metric, _, _, _) = Family(family);
        await SeedQuietMonthAsync(otherCollector);
        await SeedQuietMonthAsync("wait_stats");

        Assert.Empty(await BucketsAsync(metric));
    }

    /* ───────────────────────── the local clock ───────────────────────── */

    [Fact]
    public async Task LogSlots_KeyOnTheTargetsLocalClock_LikeTheEventsDo()
    {
        await SeedServerClockAsync(-300);
        var utc = new DateTime(2026, 2, 3, 19, 0, 0); // 14:00 local at UTC-5
        await SeedLogAsync("blocked_process_report", utc, utc.AddMinutes(45));

        var buckets = await BucketsAsync(MetricNames.Blocking);

        var row = Assert.Single(buckets).Value;
        Assert.Equal((Hour, Tuesday), (row.HourOfDay, row.DayOfWeek));
        Assert.Equal(1L, row.SampleCount);
    }
}
