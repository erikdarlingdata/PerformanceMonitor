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
/// Tests for BaselineProvider: time-bucketed baseline computation, bucket collapse
/// with hysteresis, restart poisoning exclusion, and division-by-zero handling.
/// </summary>
public class BaselineProviderTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private readonly BaselineProvider _provider;
    private DuckDBConnection? _seedConn;

    private const int ServerId = -999;

    // Analysis time is pinned to a known hour+dow for deterministic bucket matching.
    // Wednesday 14:00 UTC (dow=3 in DuckDB where Sunday=0)
    private static readonly DateTime AnalysisTime = new(2026, 4, 1, 14, 0, 0, DateTimeKind.Utc);

    private long _nextId = -1;

    public BaselineProviderTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _provider = new BaselineProvider(_duckDb);
        // Use very short TTL so cache doesn't interfere between tests
        BaselineProvider.CacheTtl = TimeSpan.FromMilliseconds(1);
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>
    /// One connection reused for every seeded row — opening a fresh connection per
    /// single-row INSERT measured ~90ms/row and dominated this class's runtime.
    /// </summary>
    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    // ── Full bucket: enough samples in one hour+dow ──

    [Fact]
    public async Task GetBaseline_FullBucket_ReturnsMeanAndStdDev()
    {
        // Seed 20 CPU samples on Wednesdays at 14:xx over 4 weeks (well above RestoreThreshold=15)
        for (int week = 0; week < 4; week++)
        {
            var wednesday = AnalysisTime.AddDays(-7 * (week + 1)); // Previous Wednesdays
            for (int i = 0; i < 5; i++)
            {
                await SeedCpuAsync(wednesday.AddMinutes(i * 10), 50 + i * 2); // 50,52,54,56,58
            }
        }

        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, AnalysisTime);

        Assert.True(baseline.SampleCount >= 15); // Full bucket
        Assert.Equal(BaselineTier.Full, baseline.Tier);
        Assert.InRange(baseline.Mean, 50, 58); // Mean of 50,52,54,56,58 repeated
        Assert.True(baseline.StdDev > 0);
    }

    // ── Bucket collapse: hour-only fallback ──

    [Fact]
    public async Task GetBaseline_SparseBucket_CollapsesToHourOnly()
    {
        // Seed only 5 samples on Wednesday 14:xx (below CollapseThreshold=10)
        var wednesday = AnalysisTime.AddDays(-7);
        for (int i = 0; i < 5; i++)
            await SeedCpuAsync(wednesday.AddMinutes(i * 10), 40 + i);

        // Seed 2 samples each on 9 OTHER distinct calendar days at 14:xx — enough sample count for
        // hour-only (23 total, >= CollapseThreshold) AND, since #3653 A8 option B narrowed the tier
        // walk to walk coarser only on a YOUNG (too-few-distinct-days) bucket, enough distinct days
        // (10 total, clearing HourOnly's own 10-day floor) that the pooled hour-only tier this test
        // pins is itself trustworthy — not walked past to Flat.
        for (int d = 0; d < 9; d++)
        {
            var day = AnalysisTime.AddDays(-14 - d); // 9 further distinct days, same hour
            for (int i = 0; i < 2; i++)
                await SeedCpuAsync(day.AddMinutes(i * 10), 60 + i + d);
        }

        _provider.ClearCache();
        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, AnalysisTime);

        Assert.True(baseline.SampleCount >= 10);
        Assert.Equal(BaselineTier.HourOnly, baseline.Tier);
        Assert.Equal(-1, baseline.DayOfWeek); // Indicates hour-only
    }

    // ── Bucket collapse: flat fallback ──

    [Fact]
    public async Task GetBaseline_VerySparseBucket_CollapsesToFlat()
    {
        // Seed only 2 samples at 14:xx (below threshold for hour-only)
        var day = AnalysisTime.AddDays(-7);
        await SeedCpuAsync(day.AddMinutes(0), 30);
        await SeedCpuAsync(day.AddMinutes(15), 35);

        // Seed 5 samples at other hours (enough for flat but not hour-only)
        for (int h = 0; h < 5; h++)
            await SeedCpuAsync(day.AddHours(-h - 1), 50 + h);

        _provider.ClearCache();
        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, AnalysisTime);

        // Should fall through to flat (7 samples total, >= 3 minimum viable)
        Assert.True(baseline.SampleCount >= 3);
        Assert.Equal(BaselineTier.Flat, baseline.Tier);
    }

    // ── DistinctDays (baseline quality gate, change 2) ──

    [Fact]
    public async Task GetBaseline_FullBucket_PopulatesDistinctDaysAndIsTrustworthy()
    {
        // 20 samples across 4 distinct Wednesdays at 14:xx.
        for (int week = 0; week < 4; week++)
        {
            var wednesday = AnalysisTime.AddDays(-7 * (week + 1));
            for (int i = 0; i < 5; i++)
                await SeedCpuAsync(wednesday.AddMinutes(i * 10), 50 + i * 2);
        }

        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, AnalysisTime);

        Assert.Equal(BaselineTier.Full, baseline.Tier);
        Assert.Equal(4, baseline.DistinctDays);   // 4 distinct Wednesdays
        Assert.True(baseline.IsTrustworthy);        // Full needs >= 3 distinct days; 4 clears it
    }

    [Fact]
    public async Task GetBaseline_HourOnly_SumsDistinctDaysAcrossDowBuckets()
    {
        // Same hour (14:00) across 10 distinct calendar days of DIFFERENT days-of-week, 4 samples
        // each (below Full's restore) → collapses to hour-only. Each calendar day lands in exactly
        // one day-of-week bucket, so summing distinct-days across those buckets is exact (= 10).
        // 10 distinct days, not 4: since #3653 A8 option B narrowed the tier walk to walk coarser
        // only on a YOUNG (too-few-distinct-days) bucket, the pooled hour-only tier this test pins
        // must itself clear HourOnly's own 10-day floor — otherwise it is young and gets walked past
        // to Flat before this assertion ever sees it.
        for (int d = 0; d < 10; d++)
        {
            var day = AnalysisTime.AddDays(-7 - d);
            for (int i = 0; i < 4; i++)
                await SeedCpuAsync(day.AddMinutes(i * 10), 40 + i);
        }

        _provider.ClearCache();
        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, AnalysisTime);

        Assert.Equal(BaselineTier.HourOnly, baseline.Tier);
        Assert.Equal(10, baseline.DistinctDays);   // SUM across the 10 dow buckets
    }

    [Fact]
    public async Task GetBaseline_Flat_TakesMaxDistinctDaysAcrossBuckets()
    {
        // Everything on ONE calendar day (2 samples at 14:xx + 5 samples at other hours) → collapses to
        // flat. A calendar day recurs across hour buckets, so DistinctDays is the conservative MAX (= 1),
        // not the sum.
        var day = AnalysisTime.AddDays(-7);
        await SeedCpuAsync(day.AddMinutes(0), 30);
        await SeedCpuAsync(day.AddMinutes(15), 35);
        for (int h = 0; h < 5; h++)
            await SeedCpuAsync(day.AddHours(-h - 1), 50 + h);

        _provider.ClearCache();
        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, AnalysisTime);

        Assert.Equal(BaselineTier.Flat, baseline.Tier);
        Assert.Equal(1, baseline.DistinctDays);
    }

    // ── Empty baseline ──

    [Fact]
    public async Task GetBaseline_NoData_ReturnsEmpty()
    {
        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, AnalysisTime);

        Assert.Equal(0, baseline.SampleCount);
    }

    // ── Hysteresis: between collapse and restore thresholds ──

    [Fact]
    public async Task GetBaseline_BetweenThresholds_UsesFullBucket()
    {
        // Seed exactly 12 samples on Wednesday 14:xx (between 10 and 15)
        for (int week = 0; week < 3; week++)
        {
            var wednesday = AnalysisTime.AddDays(-7 * (week + 1));
            for (int i = 0; i < 4; i++)
                await SeedCpuAsync(wednesday.AddMinutes(i * 10), 45 + i);
        }

        _provider.ClearCache();
        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, AnalysisTime);

        // 12 samples >= CollapseThreshold(10), so full bucket is used (hysteresis)
        Assert.Equal(12, baseline.SampleCount);
        Assert.Equal(BaselineTier.Full, baseline.Tier);
    }

    // ── Division by zero: proportional floor ──

    [Fact]
    public void EffectiveStdDev_ZeroStdDev_UsesProportionalFloor()
    {
        // All identical values → stddev = 0, mean = 50
        var bucket = new BaselineBucket
        {
            HourOfDay = 14, DayOfWeek = 3,
            Mean = 50.0, StdDev = 0.0, SampleCount = 20,
            Tier = BaselineTier.Full
        };

        // Should be max(0, 50 * 0.01) = 0.5
        Assert.Equal(0.5, bucket.EffectiveStdDev);
    }

    [Fact]
    public void EffectiveStdDev_ZeroMeanAndZeroStdDev_ReturnsZero()
    {
        // Zero activity → skip scoring
        var bucket = new BaselineBucket
        {
            HourOfDay = 14, DayOfWeek = 3,
            Mean = 0.0, StdDev = 0.0, SampleCount = 20,
            Tier = BaselineTier.Full
        };

        Assert.Equal(0.0, bucket.EffectiveStdDev);
    }

    [Fact]
    public void EffectiveStdDev_NormalStdDev_ReturnsActual()
    {
        var bucket = new BaselineBucket
        {
            HourOfDay = 14, DayOfWeek = 3,
            Mean = 50.0, StdDev = 5.0, SampleCount = 20,
            Tier = BaselineTier.Full
        };

        // StdDev (5.0) > Mean * 0.01 (0.5), so return actual
        Assert.Equal(5.0, bucket.EffectiveStdDev);
    }

    // ── Restart poisoning: cumulative counter drop excluded ──

    [Fact]
    public async Task GetBaseline_BatchRequests_ExcludesRestartDrop()
    {
        // Seed batch requests with a restart in the middle — as the collector actually writes one (#3653):
        // the counter went backwards, so the delta is unknowable and the row carries delta 0 WITH interval 0.
        // Every row sits INSIDE the analysed hour (5-minute spacing): the earlier shape of this test put the
        // restart at +60 and +70 minutes, one bucket over, where nothing it asserted could see it.
        var baseDay = AnalysisTime.AddDays(-7);
        var normalValues = new[] { 5000, 5100, 4900, 5200, 5050, 4950 };

        for (int i = 0; i < normalValues.Length; i++)
            await SeedPerfmonAsync(baseDay.AddMinutes(i * 5), "Batch Requests/sec", normalValues[i]);

        // Restart: value falls to 0 over NO measured interval, then recovers
        await SeedPerfmonAsync(baseDay.AddMinutes(30), "Batch Requests/sec", 0, intervalSeconds: 0);     // Restart
        await SeedPerfmonAsync(baseDay.AddMinutes(35), "Batch Requests/sec", 5100);  // Recovery

        // Add enough more samples on other days to reach threshold
        for (int d = 2; d <= 4; d++)
        {
            var day = AnalysisTime.AddDays(-7 * d);
            for (int i = 0; i < 5; i++)
                await SeedPerfmonAsync(day.AddMinutes(i * 10), "Batch Requests/sec", 5000 + i * 50);
        }

        _provider.ClearCache();
        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.BatchRequests, AnalysisTime);

        // #3527: the baseline is per-second now — deltas of ~5000 over 10s intervals are ~500/sec.
        // The restart (0 over interval 0) is excluded by the knowability filter, so the mean sits near 500
        // and the sample count is every row but that one: 7 + 15 = 22.
        Assert.Equal(22, baseline.SampleCount);
        var expectedMean = (normalValues.Sum() + 5100 + Enumerable.Range(0, 5).Sum(i => 5000 + i * 50) * 3) / 10.0 / 22.0;
        Assert.Equal(expectedMean, baseline.Mean, 3);
        Assert.InRange(baseline.Mean, 400, 600);
    }

    /// <summary>
    /// #3653 (A10, the mechanical half): a zero over a MEASURED interval is a real idle sample and stays in the
    /// population whatever the sample before it read. The retired QUALIFY heuristic dropped exactly this row
    /// (delta 0 after a prior > 1000) as a "restart signature", but the collector writes a restart as interval
    /// 0, which the WHERE already removes — so the only rows the heuristic ever reached were genuine idle
    /// minutes after busy ones, and it was biasing the baseline upward by removing them. Same fixture as the
    /// restart test, the zero now carrying its 10 s interval: one more sample, and the mean moves toward zero by
    /// exactly that sample's weight.
    /// </summary>
    [Fact]
    public async Task GetBaseline_BatchRequests_KeepsAMeasuredIdleZero_AfterABusySample()
    {
        var baseDay = AnalysisTime.AddDays(-7);
        var normalValues = new[] { 5000, 5100, 4900, 5200, 5050, 4950 };

        for (int i = 0; i < normalValues.Length; i++)
            await SeedPerfmonAsync(baseDay.AddMinutes(i * 5), "Batch Requests/sec", normalValues[i]);

        // A measured idle interval: nothing ran for 10 s right after a 495/sec sample. Real, and kept.
        await SeedPerfmonAsync(baseDay.AddMinutes(30), "Batch Requests/sec", 0, intervalSeconds: 10);
        await SeedPerfmonAsync(baseDay.AddMinutes(35), "Batch Requests/sec", 5100);

        for (int d = 2; d <= 4; d++)
        {
            var day = AnalysisTime.AddDays(-7 * d);
            for (int i = 0; i < 5; i++)
                await SeedPerfmonAsync(day.AddMinutes(i * 10), "Batch Requests/sec", 5000 + i * 50);
        }

        _provider.ClearCache();
        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.BatchRequests, AnalysisTime);

        Assert.Equal(23, baseline.SampleCount);
        var expectedMean = (normalValues.Sum() + 0 + 5100 + Enumerable.Range(0, 5).Sum(i => 5000 + i * 50) * 3) / 10.0 / 23.0;
        Assert.Equal(expectedMean, baseline.Mean, 3);
    }

    [Fact]
    public async Task GetBaseline_BatchRequests_IsPerSecond_AndSkipsIntervalZeroRows()
    {
        // #3527 fixture: deltas of 6000 over measured 60s intervals are 100 requests/sec — the
        // baseline population must be that division, in the same unit as the detector's window read.
        // Interspersed interval-0 rows (unknowable delta) must not enter the population at all:
        // read as raw deltas they would inflate the mean; read as 0 they would drag it down.
        for (int d = 1; d <= 4; d++)
        {
            var day = AnalysisTime.AddDays(-7 * d);
            for (int i = 0; i < 5; i++)
                await SeedPerfmonAsync(day.AddMinutes(i * 10), "Batch Requests/sec", 6000, intervalSeconds: 60);

            // One unknowable-delta row per day, wedged between the usable samples.
            await SeedPerfmonAsync(day.AddMinutes(55), "Batch Requests/sec", 0, intervalSeconds: 0);
        }

        _provider.ClearCache();
        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.BatchRequests, AnalysisTime);

        Assert.Equal(20, baseline.SampleCount); // 5 usable rows x 4 days; interval-0 rows contribute nothing
        Assert.Equal(100.0, baseline.Mean, 3);
    }

    // ── Wait stats: per-collection aggregation ──

    [Fact]
    public async Task GetBaseline_WaitStats_AggregatesPerCollection()
    {
        // Seed multiple wait types at each collection time — baseline should aggregate to total
        for (int week = 0; week < 4; week++)
        {
            var day = AnalysisTime.AddDays(-7 * (week + 1));
            for (int i = 0; i < 5; i++)
            {
                var t = day.AddMinutes(i * 10);
                await SeedWaitStatAsync(t, "SOS_SCHEDULER_YIELD", 100);
                await SeedWaitStatAsync(t, "WRITELOG", 50);
                await SeedWaitStatAsync(t, "PAGEIOLATCH_SH", 30);
            }
        }

        _provider.ClearCache();
        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.WaitStats, AnalysisTime);

        Assert.True(baseline.SampleCount > 0);
        // Mean should be ~180 (100+50+30 per collection)
        Assert.InRange(baseline.Mean, 150, 210);
    }

    /// <summary>
    /// #3653 (A10, the mechanical half) — the three interval states a wait-stats collection can be in, and what
    /// the WaitStats baseline does with a ZERO total in each: a pre-v60 collection (NULL interval) keeps the
    /// magnitude heuristic, so its zero after a busy collection is dropped as the restart signature it might
    /// be; a collection with a MEASURED interval is authoritative, so its zero after a busy collection is a real
    /// idle sample and stays; a restart as the collector writes it (every row 0 over interval 0) forms no
    /// per-collection row at all. Forty plain collections carry the bucket past the Full-tier threshold; the
    /// three planted pairs then move the count by exactly the rows each rule admits.
    /// </summary>
    [Fact]
    public async Task GetBaseline_WaitStats_ThreeIntervalStates_HeuristicOnlyWhereTheIntervalIsUnknown()
    {
        for (int week = 0; week < 4; week++)
        {
            var day = AnalysisTime.AddDays(-7 * (week + 1));
            for (int i = 0; i < 10; i++)
            {
                var t = day.AddMinutes(i * 5);
                await SeedWaitStatAsync(t, "SOS_SCHEDULER_YIELD", 150);
                await SeedWaitStatAsync(t, "WRITELOG", 50);
            }
        }

        var week1 = AnalysisTime.AddDays(-7);
        var week2 = AnalysisTime.AddDays(-14);
        var week3 = AnalysisTime.AddDays(-21);

        // Pre-v60: a busy collection then a zero — only the heuristic can judge it, and it drops it.
        await SeedWaitStatAsync(week1.AddMinutes(50), "SOS_SCHEDULER_YIELD", 60000);
        await SeedWaitStatAsync(week1.AddMinutes(55), "SOS_SCHEDULER_YIELD", 0);

        // Measured: the same shape with the interval recorded — the zero is a real idle collection and stays.
        await SeedWaitStatAsync(week2.AddMinutes(50), "SOS_SCHEDULER_YIELD", 60000, intervalSeconds: 300);
        await SeedWaitStatAsync(week2.AddMinutes(55), "SOS_SCHEDULER_YIELD", 0, intervalSeconds: 300);

        // Restart: the collector's own verdict (interval 0) on every row — no per-collection row forms.
        await SeedWaitStatAsync(week3.AddMinutes(50), "SOS_SCHEDULER_YIELD", 60000, intervalSeconds: 300);
        await SeedWaitStatAsync(week3.AddMinutes(55), "SOS_SCHEDULER_YIELD", 0, intervalSeconds: 0);
        await SeedWaitStatAsync(week3.AddMinutes(55), "WRITELOG", 0, intervalSeconds: 0);

        _provider.ClearCache();
        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.WaitStats, AnalysisTime);

        // 40 plain + 3 busy + the one measured idle zero = 44; the heuristic-dropped zero and the restart are out.
        Assert.Equal(BaselineTier.Full, baseline.Tier);
        Assert.Equal(44, baseline.SampleCount);
        Assert.Equal((40 * 200.0 + 3 * 60000.0 + 0) / 44.0, baseline.Mean, 2);

        // The rate arm over the same rows: pre-v60 collections rate off the LAG gap, measured ones off their
        // stored interval; the same three verdicts hold there (week 1's zero after 200 ms/s drops on the
        // heuristic, week 2's measured zero stays, week 3's restart never forms a row). Only the window's very
        // FIRST collection lacks a prior and drops; each later week's first collection rates off its 7-day gap.
        var rate = await _provider.GetBaselineAsync(ServerId, MetricNames.WaitMsPerSec, AnalysisTime);
        Assert.Equal(BaselineTier.Full, rate.Tier);
        Assert.Equal(43, rate.SampleCount);
    }

    // ── Session count: per-collection aggregation ──

    [Fact]
    public async Task GetBaseline_SessionCount_AggregatesPerCollection()
    {
        // Seed multiple program_name rows per collection
        for (int week = 0; week < 4; week++)
        {
            var day = AnalysisTime.AddDays(-7 * (week + 1));
            for (int i = 0; i < 5; i++)
            {
                var t = day.AddMinutes(i * 10);
                await SeedSessionStatAsync(t, "App1", 10);
                await SeedSessionStatAsync(t, "App2", 5);
            }
        }

        _provider.ClearCache();
        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.SessionCount, AnalysisTime);

        Assert.True(baseline.SampleCount > 0);
        // Mean should be ~15 (10+5 per collection)
        Assert.InRange(baseline.Mean, 12, 18);
    }

    // ── Cache behavior ──

    [Fact]
    public async Task GetBaseline_CacheHit_ReturnsSameResult()
    {
        for (int i = 0; i < 20; i++)
            await SeedCpuAsync(AnalysisTime.AddDays(-7).AddMinutes(i * 10), 50);

        BaselineProvider.CacheTtl = TimeSpan.FromMinutes(5);
        _provider.ClearCache();

        var first = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, AnalysisTime);
        var second = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, AnalysisTime);

        Assert.Equal(first.Mean, second.Mean);
        Assert.Equal(first.SampleCount, second.SampleCount);

        // Restore short TTL
        BaselineProvider.CacheTtl = TimeSpan.FromMilliseconds(1);
    }

    [Fact]
    public async Task InvalidateCache_ClearsServerEntries()
    {
        for (int i = 0; i < 20; i++)
            await SeedCpuAsync(AnalysisTime.AddDays(-7).AddMinutes(i * 10), 50);

        BaselineProvider.CacheTtl = TimeSpan.FromMinutes(5);
        _provider.ClearCache();

        await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, AnalysisTime);
        _provider.InvalidateCache(ServerId);

        // After invalidation, should recompute (no error, same result)
        var after = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, AnalysisTime);
        Assert.True(after.SampleCount > 0);

        BaselineProvider.CacheTtl = TimeSpan.FromMilliseconds(1);
    }

    // ── Server isolation: no cross-contamination ──

    [Fact]
    public async Task GetBaseline_DifferentServers_NoCrossContamination()
    {
        int server1 = -998, server2 = -997;

        // Seed different CPU values for two servers
        for (int i = 0; i < 20; i++)
        {
            await SeedCpuAsync(AnalysisTime.AddDays(-7).AddMinutes(i * 10), 80, server1);
            await SeedCpuAsync(AnalysisTime.AddDays(-7).AddMinutes(i * 10), 20, server2);
        }

        _provider.ClearCache();
        var baseline1 = await _provider.GetBaselineAsync(server1, MetricNames.Cpu, AnalysisTime);
        var baseline2 = await _provider.GetBaselineAsync(server2, MetricNames.Cpu, AnalysisTime);

        Assert.InRange(baseline1.Mean, 75, 85);
        Assert.InRange(baseline2.Mean, 15, 25);
    }

    // ── Memory metric (Lite-only) ──

    [Fact]
    public async Task GetBaseline_Memory_ComputesPressurePercent()
    {
        // 80% memory pressure: 80GB used of 100GB target
        for (int week = 0; week < 4; week++)
        {
            var day = AnalysisTime.AddDays(-7 * (week + 1));
            for (int i = 0; i < 5; i++)
                await SeedMemoryStatAsync(day.AddMinutes(i * 10), totalServerMb: 80_000, targetMb: 100_000);
        }

        _provider.ClearCache();
        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.Memory, AnalysisTime);

        Assert.True(baseline.SampleCount > 0);
        Assert.InRange(baseline.Mean, 78, 82); // ~80%
    }

    // ── Helpers ──

    private async Task SeedCpuAsync(DateTime time, int cpuValue, int serverId = ServerId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time,
             sqlserver_cpu_utilization, other_process_cpu_utilization)
            VALUES ($1, $2, $3, 'TestServer', $4, $5, 2)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuValue });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Seeds one perfmon row. deltaValue is the PER-INTERVAL delta; the baseline divides it by
    /// intervalSeconds (#3527), so at the default 10s interval the per-second value is deltaValue / 10.
    /// intervalSeconds = 0 plants the unknowable-delta marker the baseline must skip.</summary>
    private async Task SeedPerfmonAsync(DateTime time, string counterName, long deltaValue, int intervalSeconds = 10)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO perfmon_stats
            (collection_id, collection_time, server_id, server_name,
             object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds)
            VALUES ($1, $2, $3, 'TestServer', 'SQLServer:SQL Statistics', $4, '', $5, $5, $6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = counterName });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaValue });
        cmd.Parameters.Add(new DuckDBParameter { Value = intervalSeconds });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>intervalSeconds null plants a pre-v60 row (interval never recorded); 0 plants the collector's
    /// unknowable-delta marker (a restart); n plants a measured interval (#3653).</summary>
    private async Task SeedWaitStatAsync(DateTime time, string waitType, long deltaWaitMs, int? intervalSeconds = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO wait_stats
            (collection_id, collection_time, server_id, server_name, wait_type,
             waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
             delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
            VALUES ($1, $2, $3, 'TestServer', $4, 0, 0, 0, 0, $5, 0, $6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitType });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaWaitMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = intervalSeconds.HasValue ? intervalSeconds.Value : DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedSessionStatAsync(DateTime time, string programName, long connectionCount)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO session_stats
            (collection_id, collection_time, server_id, server_name, program_name,
             connection_count, running_count, sleeping_count, dormant_count)
            VALUES ($1, $2, $3, 'TestServer', $4, $5, 0, 0, 0)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = programName });
        cmd.Parameters.Add(new DuckDBParameter { Value = connectionCount });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedMemoryStatAsync(DateTime time, double totalServerMb, double targetMb)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO memory_stats
            (collection_id, collection_time, server_id, server_name,
             total_physical_memory_mb, available_physical_memory_mb,
             target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
            VALUES ($1, $2, $3, 'TestServer', $4, $5, $6, $7, $7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = targetMb * 1.2 }); // total physical > target
        cmd.Parameters.Add(new DuckDBParameter { Value = targetMb * 0.2 }); // some available
        cmd.Parameters.Add(new DuckDBParameter { Value = targetMb });
        cmd.Parameters.Add(new DuckDBParameter { Value = totalServerMb });
        await cmd.ExecuteNonQueryAsync();
    }
}
