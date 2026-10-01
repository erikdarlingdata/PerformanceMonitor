using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4731: the blocking and deadlock baselines count the hours collection covered. A slot is one local (date, hour);
/// it is covered when the event's OWN collector logged a SUCCESS run in it, or when it holds events. A bucket's mean
/// is its events over its covered days, so a quiet hour of a quiet month is a row with mean 0 (a measured zero) and
/// not a missing row, and an hour with events on two of five covered days divides by five, not two. The logged slots
/// count only on a server whose source holds at least one event in the window: a successful run proves the collector
/// ran, not that its source could see events (a blocked process threshold of 0 logs SUCCESS every hour over an empty
/// ring buffer), so a server with none gets no bucket, as it did before covered days.
///
/// <para>Both families are raw-table arms (<see cref="BaselineProvider.IsDailyCacheMetric"/>, #4731 after #4248), so
/// the window ends at the analysis day's UTC midnight: [Feb 2 00:00, Mar 4 00:00) for the analysis time Wed Mar 4
/// 14:00 (Feb 2 is a Monday). The bucket under test is Tuesday 14:00: Feb 3, 10, 17, 24 and Mar 3 make five covered
/// days; the whole hour of day 14 makes thirty, which is what the HourOnly tier pools and what the detector judges
/// the analysis hour against.</para>
/// </summary>
public class EventBaselineCoveredDaysTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -4731_02;
    private const int Tuesday = (int)DayOfWeek.Tuesday;
    private const int Hour = 14;
    private static readonly DateTime AnalysisTime = new(2026, 3, 4, 14, 0, 0);

    /// <summary>The window's end: the analysis day's UTC midnight (the family's cache key, <see cref="BaselineProvider.RoundedDay"/>).</summary>
    private static readonly DateTime WindowEnd = BaselineProvider.RoundedDay(AnalysisTime);

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

    /// <summary>One log row every 15 minutes from <paramref name="from"/> to <paramref name="to"/> inclusive.
    /// <paramref name="rowsCollected"/> and <paramref name="errorMessage"/> are the run's own columns (NULL when not given).</summary>
    private Task SeedLogAsync(
        string collector, DateTime from, DateTime to, string status = "SUCCESS", int? rowsCollected = null, string? errorMessage = null)
    {
        _nextLogBase -= 1_000_000;
        return ExecAsync(
            @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected, error_message)
              SELECT $1 - CAST(row_number() OVER () AS BIGINT), $2, 'TestServer', $3, t, $4, $7::INTEGER, $8::VARCHAR
              FROM generate_series($5::TIMESTAMP, $6::TIMESTAMP, INTERVAL 15 MINUTE) AS g(t)",
            _nextLogBase, ServerId, collector, status, from, to,
            rowsCollected is int rows ? (object)rows : DBNull.Value, errorMessage is string message ? (object)message : DBNull.Value);
    }

    /// <summary>Five weeks of SUCCESS runs every 15 minutes, ending just before the analysis hour: every slot of the window is covered.</summary>
    private Task SeedQuietMonthAsync(string collector) =>
        SeedLogAsync(collector, AnalysisTime.AddDays(-35), AnalysisTime.AddMinutes(-15));

    private Task SeedEventAsync(string family, DateTime at) => family == "blocking"
        ? ExecAsync(
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, wait_time_ms) VALUES ($1,$2,$2,$3,'TestServer',1000)",
            _nextId--, at, ServerId)
        : ExecAsync(
            "INSERT INTO deadlocks (deadlock_id, collection_time, deadlock_time, server_id, server_name) VALUES ($1,$2,$2,$3,'TestServer')",
            _nextId--, at, ServerId);

    private async Task SeedEventsAsync(string family, DateTime at, int count)
    {
        for (var i = 0; i < count; i++)
            await SeedEventAsync(family, at.AddMinutes(10 + i));
    }

    /// <summary>The one event that lets a quiet server's covered hours count (#4731): inside the window, on Sunday
    /// Feb 8 at 09:10, which is neither hour 14 nor the bucket under test (Tuesday 14:00).</summary>
    private static readonly DateTime ElsewhereEvent = new(2026, 2, 8, 9, 10, 0);

    private Task SeedAnEventInAnotherHourAsync(string family) => SeedEventAsync(family, ElsewhereEvent);

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

    /* ───────────────────────── (a) a quiet hour on a server whose source has captured events ───────────────────────── */

    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task QuietHours_OnAServerThatHasCapturedEvents_AreRowsWithMeanZero_NotMissingRows(string family)
    {
        var (metric, collector, _, _) = Family(family);
        await SeedQuietMonthAsync(collector);
        await SeedAnEventInAnotherHourAsync(family);

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
    public async Task ASpikeIntoAQuietHour_OnAServerThatHasCapturedEvents_IsAMeasuredZero_NotAFirstOccurrence(string family)
    {
        var (_, collector, _, noun) = Family(family);
        await SeedQuietMonthAsync(collector);
        await SeedAnEventInAnotherHourAsync(family);

        var fact = await SpikeAsync(family, 12);

        Assert.Equal(1.0, fact.Metadata["baseline_zero_history"]);
        Assert.Equal(1.0, fact.Metadata["is_new"]);
        Assert.Equal(30.0, fact.Metadata["baseline_samples"]);
        var block = Compose(fact);
        Assert.Equal($"12 {noun} this window — against a month in which this hour saw none", block.Headline);
        Assert.Contains("a measured ZERO: 30 baseline samples across 30 distinct days", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("first occurrence", block.Headline + block.Investigation + block.Remediation, StringComparison.Ordinal);
    }

    /* ───────────────────────── (a2) a source that has captured nothing: no baseline, not a measured zero ───────────────────────── */

    /// <summary>
    /// A successful run proves the collector ran, not that its source could see events: with a blocked process
    /// threshold of 0 (RDS, Azure, or a login that cannot set it) the collector reads an empty ring buffer and logs
    /// SUCCESS every hour. Thirty days of those, and no event, must not read as a trusted mean of zero.
    /// </summary>
    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task ACollectorThatLoggedSuccessEveryHour_ForAServerWhoseSourceHoldsNoEvent_HasNoBaseline(string family)
    {
        var (metric, collector, _, _) = Family(family);
        await SeedQuietMonthAsync(collector);

        Assert.Empty(await BucketsAsync(metric));
    }

    /// <summary>The same server when it spikes: no baseline to judge against, so a first occurrence, and the events
    /// of the spike's own hour (after the window closes) do not open the gate.</summary>
    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task ASpike_OnAServerWhoseSourceHoldsNoEventInTheWindow_IsAFirstOccurrence_NotAMeasuredZero(string family)
    {
        var (_, collector, _, _) = Family(family);
        await SeedQuietMonthAsync(collector);

        var fact = await SpikeAsync(family, 12);

        Assert.Equal(0.0, fact.Metadata["baseline_zero_history"]);
        Assert.Equal(1.0, fact.Metadata["is_new"]);
        Assert.Equal(0.0, fact.Metadata["baseline_samples"]);
        var block = Compose(fact);
        Assert.DoesNotContain("measured ZERO", block.Headline + block.Investigation + block.Remediation, StringComparison.Ordinal);
    }

    /// <summary>An event just outside the window (before its start) is not one the window's source holds.</summary>
    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task AnEventBeforeTheWindow_DoesNotOpenTheGate(string family)
    {
        var (metric, collector, _, _) = Family(family);
        await SeedQuietMonthAsync(collector);
        await SeedEventAsync(family, WindowEnd.AddDays(-30).AddMinutes(-1));

        Assert.Empty(await BucketsAsync(metric));
    }

    /// <summary>The window ends at the analysis day's UTC midnight (#4731: a raw-table arm keys on the day, like Cpu
    /// and IoLatency since #4248), so an event earlier the same day, before the analysis hour, is not one it holds.</summary>
    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task AnEventOnTheAnalysisDay_IsAfterTheWindowsEnd_AndDoesNotOpenTheGate(string family)
    {
        var (metric, collector, _, _) = Family(family);
        await SeedQuietMonthAsync(collector);
        await SeedEventAsync(family, WindowEnd.AddHours(9).AddMinutes(10));

        Assert.Empty(await BucketsAsync(metric));
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
        await SeedAnEventInAnotherHourAsync(family); // the source has captured events, so the covered slots count

        var row = await RowAsync(metric, Hour, Tuesday);

        Assert.NotNull(row);
        Assert.Equal(4L, row!.SampleCount);
        Assert.Equal(0.0, row.Mean);
    }

    /// <summary>
    /// A cycle the whole-cycle budget abandoned stored nothing. The rows written before abandonment had its own status
    /// carry <c>SUCCESS</c> beside <c>rows_collected = 0</c> and the budget note, so the status alone would count that
    /// slot as one the collector covered; the collection-health rollup already excludes them
    /// (<see cref="EnumeratedCollectorDriver.AbandonedByNotePredicateSql"/>) and the coverage read now does too.
    /// </summary>
    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task ASlotWhoseOnlySuccessRunWasAbandonedByTheBudget_IsNotCovered(string family)
    {
        var (metric, collector, _, _) = Family(family);
        foreach (var tuesday in Tuesdays.Take(4))
            await SeedLogAsync(collector, tuesday, tuesday.AddMinutes(45));
        await SeedLogAsync(collector, Tuesdays[4], Tuesdays[4].AddMinutes(45),
            rowsCollected: 0, errorMessage: EnumeratedCollectorDriver.WholeCycleBudgetNote(120));
        await SeedAnEventInAnotherHourAsync(family); // the source has captured events, so the covered slots count

        var row = await RowAsync(metric, Hour, Tuesday);

        Assert.NotNull(row);
        Assert.Equal(4L, row!.SampleCount);
        Assert.Equal(0.0, row.Mean);
    }

    /// <summary>
    /// The exclusion is the abandonment's own two facts and nothing wider: a quiet run (<c>rows_collected = 0</c>, no
    /// note) is exactly what a collector that found no events writes, and a note beside stored rows is not an
    /// abandonment. Both still cover their slot. The first row is the one a bare <c>LIKE</c> without the shared
    /// predicate's <c>COALESCE</c> would lose: <c>NULL LIKE</c> is NULL, and <c>NOT NULL</c> drops the row.
    /// </summary>
    [Theory]
    [InlineData("blocking", 0, false)]
    [InlineData("deadlock", 0, false)]
    [InlineData("blocking", 7, true)]
    [InlineData("deadlock", 7, true)]
    public async Task ARunThatIsNotAnAbandonment_StillCoversItsSlot(string family, int rowsCollected, bool withBudgetNote)
    {
        var (metric, collector, _, _) = Family(family);
        foreach (var tuesday in Tuesdays.Take(4))
            await SeedLogAsync(collector, tuesday, tuesday.AddMinutes(45));
        await SeedLogAsync(collector, Tuesdays[4], Tuesdays[4].AddMinutes(45),
            rowsCollected: rowsCollected, errorMessage: withBudgetNote ? EnumeratedCollectorDriver.WholeCycleBudgetNote(120) : null);
        await SeedAnEventInAnotherHourAsync(family);

        var row = await RowAsync(metric, Hour, Tuesday);

        Assert.NotNull(row);
        Assert.Equal(5L, row!.SampleCount);
        Assert.Equal(0.0, row.Mean);
    }

    /// <summary>The exclusion is the shared predicate interpolated into the <c>logged</c> CTE, right after the success
    /// filter, and not a copy of it: re-wording the abandonment's note cannot leave this read on a stale sentence.</summary>
    [Theory]
    [InlineData(MetricNames.Blocking)]
    [InlineData(MetricNames.Deadlock)]
    public void TheLoggedCte_ExcludesBudgetAbandonedRuns_WithTheSharedPredicate(string metric)
    {
        var sql = BaselineProvider.GetBaselineQuery(metric)!.Replace("\r\n", "\n", StringComparison.Ordinal);
        var logged = sql[..sql.IndexOf("events AS (", StringComparison.Ordinal)];

        Assert.Contains(
            "AND   status = 'SUCCESS'\n    AND   NOT " + EnumeratedCollectorDriver.AbandonedByNotePredicateSql + "\n",
            logged,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("blocking", "deadlocks")]
    [InlineData("deadlock", "blocked_process_report")]
    public async Task AnotherCollectorsSuccessfulRuns_DoNotCoverTheEventsSlots(string family, string otherCollector)
    {
        var (metric, _, _, _) = Family(family);
        await SeedQuietMonthAsync(otherCollector);
        await SeedQuietMonthAsync("wait_stats");
        await SeedAnEventInAnotherHourAsync(family); // the gate is open, so only the slots the family's OWN runs cover could add rows

        var row = Assert.Single(await BucketsAsync(metric)).Value;

        Assert.Equal((ElsewhereEvent.Hour, (int)ElsewhereEvent.DayOfWeek), (row.HourOfDay, row.DayOfWeek));
        Assert.Equal(1L, row.SampleCount);
        Assert.Equal(1.0, row.Mean);
    }

    /* ───────────────────────── the cache grain: the UTC day (#4731, as #4248 for Cpu and IoLatency) ───────────────────────── */

    /// <summary>
    /// Both families read the collection log at full grain over the 30-day window, so they are raw-table arms: a
    /// provider computes each once per UTC day, not once per hour, and the window ends at the day's midnight. The
    /// second analysis time is twenty hours after the first on the same UTC day, so a recompute would count the event
    /// seeded between them (Tuesday Feb 10, 14:10); the next day's time is a new key and does. A provider built fresh
    /// at the second instant is the control that the seeded event is visible to a compute.
    /// </summary>
    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task TwoAnalysisTimesOnOneUtcDay_ShareOneCompute_AndTheNextDayComputesAgain(string family)
    {
        var (metric, collector, _, _) = Family(family);
        await SeedQuietMonthAsync(collector);
        await SeedAnEventInAnotherHourAsync(family);
        var provider = new BaselineProvider(_duckDb);
        var morning = WindowEnd.AddHours(1);
        var evening = WindowEnd.AddHours(21);

        var first = await provider.GetBucketMapAsync(ServerId, metric, morning, morning.AddHours(1));
        Assert.Equal(24 * 7, first.Buckets.Count);
        Assert.Equal(0.0, first.Buckets[(Hour, Tuesday)].Mean);

        await SeedEventAsync(family, Tuesdays[1].AddMinutes(10));

        var second = await provider.GetBucketMapAsync(ServerId, metric, evening, evening.AddHours(1));
        Assert.Same(first.Buckets, second.Buckets);
        Assert.Equal(0.0, second.Buckets[(Hour, Tuesday)].Mean);

        var fresh = await new BaselineProvider(_duckDb).GetBucketMapAsync(ServerId, metric, evening, evening.AddHours(1));
        Assert.Equal(1.0 / 5.0, fresh.Buckets[(Hour, Tuesday)].Mean, 9);

        var nextDay = evening.AddHours(4);
        var third = await provider.GetBucketMapAsync(ServerId, metric, nextDay, nextDay.AddHours(1));
        Assert.NotSame(first.Buckets, third.Buckets);
        Assert.Equal(1.0 / 5.0, third.Buckets[(Hour, Tuesday)].Mean, 9);
    }

    /// <summary>The family arms are on the day key beside Cpu and IoLatency: the key time is the day's midnight, the entry
    /// carries the 24-hour backstop, and the arms that read an already-aggregated table keep the hour.</summary>
    [Fact]
    public void TheFamilies_AreDailyCacheArms_BesideCpuAndIoLatency_AndTheOthersKeepTheHour()
    {
        var early = new DateTime(2026, 3, 4, 1, 0, 0);
        var late = new DateTime(2026, 3, 4, 23, 0, 0);

        foreach (var metric in new[] { MetricNames.Blocking, MetricNames.Deadlock, MetricNames.Cpu, MetricNames.IoLatency })
        {
            Assert.True(BaselineProvider.IsDailyCacheMetric(metric), metric);
            Assert.Equal(WindowEnd, BaselineProvider.RoundedKeyTime(metric, early));
            Assert.Equal(WindowEnd, BaselineProvider.RoundedKeyTime(metric, late));
        }

        Assert.False(BaselineProvider.IsDailyCacheMetric(MetricNames.BatchRequests));
        Assert.NotEqual(BaselineProvider.RoundedKeyTime(MetricNames.BatchRequests, early), BaselineProvider.RoundedKeyTime(MetricNames.BatchRequests, late));
    }

    /* ───────────────────────── the local clock ───────────────────────── */

    [Fact]
    public async Task LogSlots_KeyOnTheTargetsLocalClock_LikeTheEventsDo()
    {
        await SeedServerClockAsync(-300);
        var utc = new DateTime(2026, 2, 3, 19, 0, 0); // 14:00 local at UTC-5
        await SeedLogAsync("blocked_process_report", utc, utc.AddMinutes(45));
        await SeedEventAsync("blocking", utc.AddHours(7)); // 21:00 local the same Tuesday: the source has captured events

        var buckets = await BucketsAsync(MetricNames.Blocking);

        Assert.Equal([(Hour, Tuesday), (21, Tuesday)], buckets.Keys.OrderBy(k => k.HourOfDay).ToArray());
        var row = buckets[(Hour, Tuesday)];
        Assert.Equal(1L, row.SampleCount);
        Assert.Equal(0.0, row.Mean);
        Assert.Equal(1.0, buckets[(21, Tuesday)].Mean);
    }
}
