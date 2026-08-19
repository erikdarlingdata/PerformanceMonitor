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
/// Tests for the upgraded AnomalyDetector: time-bucketed baselines, new detection
/// methods (batch requests, sessions, query duration, memory), per-metric thresholds,
/// and baseline context metadata.
/// </summary>
public class AnomalyDetectorTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private readonly BaselineProvider _baselineProvider;
    private readonly AnomalyDetector _detector;
    private DuckDBConnection? _seedConn;

    private const int ServerId = -999;
    private const string ServerName = "TestServer";

    // Fixed timestamps for deterministic testing
    private static readonly DateTime _now = DateTime.UtcNow;
    private static readonly DateTime _analysisEnd = _now;
    private static readonly DateTime _analysisStart = _now.AddHours(-4);

    /* #2177: the start of a seeded baseline day, floored to the HOUR.

       Every seed helper below writes several samples spanning ~21 minutes from this point. They used to
       start at whatever time-of-day _analysisStart inherited from the wall clock, so when a CI run put
       _analysisStart within 21 minutes of midnight the span crossed a date boundary and each intended
       'day' contributed TWO distinct dates — doubling the distinct-day count the baseline-quality gate
       counts, which flipped a deliberately-thin (2-day) baseline into a trustworthy one and sent the
       detector down the z-path instead of the absolute fallback. Deterministic failure for runs between
       03:39 and 04:00 UTC.

       Flooring to the hour rather than midday-anchoring (#1972's discipline elsewhere) is deliberate:
       the Full baseline tier buckets by hour AND day-of-week, so the seeds must keep _analysisStart's
       hour and weekday to land in the same bucket the analysis window reads. Starting at :00 keeps both
       while making a 21-minute span unable to leave the hour, let alone the date. */
    private static DateTime SeedDayStart(int daysBack)
    {
        var day = _analysisStart.AddDays(-daysBack);
        return day.Date.AddHours(day.Hour);
    }

    private long _nextId = -1;

    public AnomalyDetectorTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _baselineProvider = new BaselineProvider(_duckDb);
        _detector = new AnomalyDetector(_duckDb, _baselineProvider);
        BaselineProvider.CacheTtl = TimeSpan.FromMilliseconds(1);
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>
    /// One connection reused for every seeded row. Opening a fresh connection and
    /// auto-committing a single INSERT per row measured ~90ms/row, which made this
    /// class's 100-200-row seeds the critical path of the whole analysis-heavy filter.
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

    private async Task ExecuteSeedAsync(string sql)
    {
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private AnalysisContext CreateContext() => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = _analysisStart,
        TimeRangeEnd = _analysisEnd
    };

    // ── Batch Requests ──

    [Fact]
    public async Task DetectBatchRequestAnomalies_Spike_DetectsAnomaly()
    {
        // Baseline: normal batch requests (~5000)
        await SeedBaselinePerfmon("Batch Requests/sec", 5000, variance: 200);

        // Analysis window: spike to 15000
        for (int i = 0; i < 16; i++)
            await SeedPerfmonAsync(_analysisStart.AddMinutes(i * 15), "Batch Requests/sec", 15000);

        // Need wait/cpu data for HasBaselineDataAsync
        await SeedBaselineCpu(10, variance: 2);

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        Assert.Contains(anomalies, f => f.Key == "ANOMALY_BATCH_REQUESTS");
        var fact = anomalies.First(f => f.Key == "ANOMALY_BATCH_REQUESTS");
        Assert.True(fact.Metadata["deviation_sigma"] >= 2.0);
        Assert.True(fact.Metadata.ContainsKey("baseline_hour"));
        Assert.True(fact.Metadata.ContainsKey("baseline_dow"));
        Assert.True(fact.Metadata.ContainsKey("baseline_tier"));
    }

    [Fact]
    public async Task DetectBatchRequestAnomalies_Normal_NoAnomaly()
    {
        await SeedBaselinePerfmon("Batch Requests/sec", 5000, variance: 200);

        // Analysis window: same as baseline
        for (int i = 0; i < 16; i++)
            await SeedPerfmonAsync(_analysisStart.AddMinutes(i * 15), "Batch Requests/sec", 5000);

        await SeedBaselineCpu(10, variance: 2);

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        Assert.DoesNotContain(anomalies, f => f.Key == "ANOMALY_BATCH_REQUESTS");
    }

    [Fact]
    public async Task DetectBatchRequestAnomalies_LowVolumeSpike_NoAnomaly()
    {
        // Low-throughput server: a 6x relative spike but the peak stays at 300/sec
        // — below the 500/sec BatchRequestFloor — must NOT be flagged.
        await SeedBaselinePerfmon("Batch Requests/sec", 50, variance: 5);
        for (int i = 0; i < 16; i++)
            await SeedPerfmonAsync(_analysisStart.AddMinutes(i * 15), "Batch Requests/sec", 300);

        await SeedBaselineCpu(10, variance: 2);

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        Assert.DoesNotContain(anomalies, f => f.Key == "ANOMALY_BATCH_REQUESTS");
    }

    // ── Session Count ──

    [Fact]
    public async Task DetectSessionAnomalies_Spike_DetectsAnomaly()
    {
        // Baseline: ~20 connections
        await SeedBaselineSessions(20, variance: 2);

        // Analysis window: spike to 200 connections
        for (int i = 0; i < 16; i++)
        {
            var t = _analysisStart.AddMinutes(i * 15);
            await SeedSessionStatAsync(t, "App1", 150);
            await SeedSessionStatAsync(t, "App2", 50);
        }

        await SeedBaselineCpu(10, variance: 2);
        // CPU data in analysis window (needed for HasBaselineDataAsync and CPU detector to not exit early)
        for (int i = 0; i < 4; i++)
            await SeedCpuAsync(_analysisStart.AddMinutes(i * 15), 10);

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        Assert.Contains(anomalies, f => f.Key == "ANOMALY_SESSION_SPIKE");
    }

    [Fact]
    public async Task DetectSessionAnomalies_Normal_NoAnomaly()
    {
        await SeedBaselineSessions(20, variance: 2);

        // Analysis window: same as baseline
        for (int i = 0; i < 16; i++)
        {
            var t = _analysisStart.AddMinutes(i * 15);
            await SeedSessionStatAsync(t, "App1", 15);
            await SeedSessionStatAsync(t, "App2", 5);
        }

        await SeedBaselineCpu(10, variance: 2);

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        Assert.DoesNotContain(anomalies, f => f.Key == "ANOMALY_SESSION_SPIKE");
    }

    [Fact]
    public async Task DetectSessionAnomalies_LowCountSpike_NoAnomaly()
    {
        // Low-connection server: an 8x relative spike but the peak stays at 40
        // connections — below the 50 SessionCountFloor — must NOT be flagged.
        await SeedBaselineSessions(5, variance: 1);
        for (int i = 0; i < 16; i++)
        {
            var t = _analysisStart.AddMinutes(i * 15);
            await SeedSessionStatAsync(t, "App1", 30);
            await SeedSessionStatAsync(t, "App2", 10);
        }

        await SeedBaselineCpu(10, variance: 2);
        for (int i = 0; i < 4; i++)
            await SeedCpuAsync(_analysisStart.AddMinutes(i * 15), 10);

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        Assert.DoesNotContain(anomalies, f => f.Key == "ANOMALY_SESSION_SPIKE");
    }

    // ── Query Duration ──

    [Fact]
    public async Task DetectQueryDurationAnomalies_Spike_DetectsAnomaly()
    {
        // Baseline: ~10000 microseconds total elapsed per collection
        await SeedBaselineQueryStats(10_000, variance: 1000);

        // Analysis window: spike to 5,000,000 microseconds (5s) — above the 1s
        // QueryDurationFloorUs so a genuinely slow query is still flagged
        for (int i = 0; i < 16; i++)
            await SeedQueryStatAsync(_analysisStart.AddMinutes(i * 15), 5_000_000, 100);

        await SeedBaselineCpu(10, variance: 2);
        await SeedBaselineWaits();

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        Assert.Contains(anomalies, f => f.Key == "ANOMALY_QUERY_DURATION");
    }

    [Fact]
    public async Task DetectQueryDurationAnomalies_SubFloorSpike_NoAnomaly()
    {
        // A huge *relative* spike (50x baseline) whose peak stays at 500,000 us
        // (0.5s) — below the 1s QueryDurationFloorUs — must NOT be flagged. Guards
        // against alarming on trivially small absolute query durations, the noise
        // this floor was added to suppress.
        await SeedBaselineQueryStats(10_000, variance: 1000);
        for (int i = 0; i < 16; i++)
            await SeedQueryStatAsync(_analysisStart.AddMinutes(i * 15), 500_000, 100);

        await SeedBaselineCpu(10, variance: 2);
        await SeedBaselineWaits();

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        Assert.DoesNotContain(anomalies, f => f.Key == "ANOMALY_QUERY_DURATION");
    }

    // ── Memory Pressure ──

    [Fact]
    public async Task DetectMemoryAnomalies_HighPressure_DetectsAnomaly()
    {
        // Baseline: ~70% memory pressure
        await SeedBaselineMemory(70_000, 100_000);

        // Analysis window: spike to 99%
        for (int i = 0; i < 16; i++)
            await SeedMemoryStatAsync(_analysisStart.AddMinutes(i * 15), 99_000, 100_000);

        await SeedBaselineCpu(10, variance: 2);

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        Assert.Contains(anomalies, f => f.Key == "ANOMALY_MEMORY_PRESSURE");
    }

    [Fact]
    public async Task DetectMemoryAnomalies_SubFloorSpike_NoAnomaly()
    {
        // Pressure climbs sharply (40% -> 85%) but the peak stays below the 90%
        // MemoryPressureFloorPct — must NOT be flagged.
        await SeedBaselineMemory(40_000, 100_000);
        for (int i = 0; i < 16; i++)
            await SeedMemoryStatAsync(_analysisStart.AddMinutes(i * 15), 85_000, 100_000);

        await SeedBaselineCpu(10, variance: 2);

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        Assert.DoesNotContain(anomalies, f => f.Key == "ANOMALY_MEMORY_PRESSURE");
    }

    [Fact]
    public async Task DetectMemoryAnomalies_Normal_NoAnomaly()
    {
        await SeedBaselineMemory(70_000, 100_000);

        // Analysis window: same as baseline
        for (int i = 0; i < 16; i++)
            await SeedMemoryStatAsync(_analysisStart.AddMinutes(i * 15), 70_000, 100_000);

        await SeedBaselineCpu(10, variance: 2);

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        Assert.DoesNotContain(anomalies, f => f.Key == "ANOMALY_MEMORY_PRESSURE");
    }

    // ── Per-metric threshold ──

    [Fact]
    public async Task SetDeviationThreshold_HigherThreshold_SuppressesAnomaly()
    {
        // Baseline: CPU ~10%
        await SeedBaselineCpu(10, variance: 2);

        // Analysis window: CPU spike to 60% (would normally be >2σ)
        for (int i = 0; i < 16; i++)
            await SeedCpuAsync(_analysisStart.AddMinutes(i * 15), 60);

        // Default threshold (2σ) should detect it
        var anomalies1 = await _detector.DetectAnomaliesAsync(CreateContext());
        var hasCpu1 = anomalies1.Any(f => f.Key == "ANOMALY_CPU_SPIKE");

        // Set very high threshold — should suppress it
        _detector.SetDeviationThreshold(MetricNames.Cpu, 100.0);
        _baselineProvider.ClearCache();
        var anomalies2 = await _detector.DetectAnomaliesAsync(CreateContext());
        var hasCpu2 = anomalies2.Any(f => f.Key == "ANOMALY_CPU_SPIKE");

        // Reset
        _detector.SetDeviationThreshold(MetricNames.Cpu, 2.0);

        Assert.False(hasCpu2, "High threshold should suppress CPU anomaly");
    }

    // ── Baseline context metadata ──

    [Fact]
    public async Task AnomalyFacts_ContainBaselineContextMetadata()
    {
        await SeedBaselineCpu(10, variance: 2);

        // Spike to trigger anomaly
        for (int i = 0; i < 16; i++)
            await SeedCpuAsync(_analysisStart.AddMinutes(i * 15), 90);

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());
        var cpuAnomaly = anomalies.FirstOrDefault(f => f.Key == "ANOMALY_CPU_SPIKE");

        if (cpuAnomaly != null)
        {
            Assert.True(cpuAnomaly.Metadata.ContainsKey("baseline_hour"), "Missing baseline_hour");
            Assert.True(cpuAnomaly.Metadata.ContainsKey("baseline_dow"), "Missing baseline_dow");
            Assert.True(cpuAnomaly.Metadata.ContainsKey("baseline_tier"), "Missing baseline_tier");
            Assert.True(cpuAnomaly.Metadata.ContainsKey("baseline_mean"), "Missing baseline_mean");
            Assert.True(cpuAnomaly.Metadata.ContainsKey("deviation_sigma"), "Missing deviation_sigma");
        }
    }

    // ── Baseline quality gate + interaction trap (change 2) ──

    [Fact]
    public async Task DetectCpuAnomalies_ThinBaseline_AbsoluteFallbackFires_NotSilent()
    {
        // Thin baseline: only 2 distinct days (below the Full-tier 3-day minimum) → NOT trustworthy,
        // so the z-path is suppressed. But 95% CPU clears the absolute-fallback bar (90%) — the
        // detector must still fire (the interaction trap: a young store fires on the higher bar, not
        // silence).
        await SeedThinBaselineCpu(avgCpu: 10, variance: 2);
        for (int i = 0; i < 16; i++)
            await SeedCpuAsync(_analysisStart.AddMinutes(i * 15), 95);

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        var cpu = anomalies.FirstOrDefault(f => f.Key == "ANOMALY_CPU_SPIKE");
        Assert.NotNull(cpu);
        Assert.Equal(1.0, cpu!.Metadata["baseline_low_quality"]); // fired via the absolute fallback
        // The exceedance the scorer grades off (finding 1): peak 95% ÷ the 90% CpuFallbackPct bar (>= 1.0
        // on a fire). Without this, the scorer's 2σ gate would zero the small fallback z and drop the finding.
        Assert.Equal(95.0 / 90.0, cpu.Metadata["fallback_exceedance"], precision: 4);
    }

    [Fact]
    public async Task DetectCpuAnomalies_ThinBaseline_SuppressesPureZSpikeBelowAbsoluteBar()
    {
        // Same thin (untrustworthy) baseline. 60% CPU is a large z-spike over a 10% baseline, but it is
        // BELOW the 90% absolute-fallback bar — a thin baseline must NOT let a pure z-spike fire.
        await SeedThinBaselineCpu(avgCpu: 10, variance: 2);
        for (int i = 0; i < 16; i++)
            await SeedCpuAsync(_analysisStart.AddMinutes(i * 15), 60);

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        Assert.DoesNotContain(anomalies, f => f.Key == "ANOMALY_CPU_SPIKE");
    }

    [Fact]
    public async Task DetectCpuAnomalies_HealthyBaseline_TrustsZPath_NotFallback()
    {
        // Healthy baseline: 14 distinct days → trustworthy. A 70% spike over a 10% baseline clears the
        // 50% magnitude floor and fires on the TRUSTED z-path (not the fallback), with a real sigma.
        await SeedBaselineCpu(10, variance: 2);
        for (int i = 0; i < 16; i++)
            await SeedCpuAsync(_analysisStart.AddMinutes(i * 15), 70);

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        var cpu = anomalies.FirstOrDefault(f => f.Key == "ANOMALY_CPU_SPIKE");
        Assert.NotNull(cpu);
        Assert.Equal(0.0, cpu!.Metadata["baseline_low_quality"]); // trusted z-path
        Assert.True(cpu.Metadata["deviation_sigma"] >= 2.0);
        Assert.Equal(0.0, cpu.Metadata["fallback_exceedance"]); // trusted path carries no fallback exceedance
    }

    // ── Wait profile (change 1): one ANOMALY_WAIT_PROFILE, minority-but-real wait captured ──

    [Fact]
    public async Task DetectWaitAnomalies_EmitsOneProfile_CapturingMinorityButRealWait()
    {
        // Trustworthy WaitMsPerSec baseline: 14 distinct days of modest SOS.
        await SeedBaselineWaits();
        await SeedBaselineCpu(10, variance: 2); // HasBaselineData canary

        // Window: SOS dominates the totals, but a real RESOURCE_SEMAPHORE (the kind of minority wait
        // the old per-type detector would compare to the all-types baseline and MISS) is also present.
        // The all-types profile fires once and names BOTH as contributors.
        for (int i = 0; i < 16; i++)
        {
            var t = _analysisStart.AddMinutes(i * 15);
            await SeedWaitStatAsync(t, "SOS_SCHEDULER_YIELD", 300_000);
            await SeedWaitStatAsync(t, "RESOURCE_SEMAPHORE", 50_000);
        }

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        var profiles = anomalies.Where(f => f.Key == "ANOMALY_WAIT_PROFILE").ToList();
        var profile = Assert.Single(profiles); // ONE fact, not one-per-type
        Assert.Equal(0.0, profile.Metadata["is_new"]); // trustworthy baseline → real ratio path
        Assert.True(profile.Metadata.ContainsKey("contrib_SOS_SCHEDULER_YIELD"));
        Assert.True(profile.Metadata.ContainsKey("contrib_RESOURCE_SEMAPHORE"),
            "the minority-but-real RESOURCE_SEMAPHORE must be captured as a contributor");
        Assert.True(profile.Metadata["ratio"] >= 4.0);
    }

    // ── No baseline = no anomalies ──

    [Fact]
    public async Task DetectAnomalies_NoBaselineData_ReturnsEmpty()
    {
        // Only analysis window data, no baseline
        for (int i = 0; i < 16; i++)
            await SeedCpuAsync(_analysisStart.AddMinutes(i * 15), 90);

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        // Should not fire — no baseline to compare against
        Assert.Empty(anomalies);
    }

    // ── Helpers: seed baseline data in the 30-day window before analysis ──

    /// <summary>
    /// Seeds baseline data across 14 days, keeping all samples within the same hour
    /// as the analysis start so they land in the same time bucket. Uses 3-minute
    /// intervals to stay within one hour (14 days × 4 samples = 56 total, enough
    /// for flat/hour-only collapse).
    /// </summary>
    private async Task SeedBaselineCpu(int avgCpu, int variance)
    {
        await ExecuteSeedAsync("BEGIN TRANSACTION");
        var rng = new Random(42);
        for (int day = 1; day <= 14; day++)
        {
            var baseDay = SeedDayStart(day);
            for (int i = 0; i < 4; i++)
            {
                var cpu = Math.Clamp(avgCpu + rng.Next(-variance, variance + 1), 0, 100);
                await SeedCpuAsync(baseDay.AddMinutes(i * 3), cpu);
            }
        }
        await ExecuteSeedAsync("COMMIT");
    }

    /// <summary>
    /// Seeds a THIN CPU baseline: many samples but only 2 distinct calendar days (7 and 14 days back,
    /// the same day-of-week as the analysis start, so they land in the analysis-hour's full bucket).
    /// Enough samples to be selected as a Full-tier baseline, but below the 3-distinct-day trust floor
    /// — exercises the low-quality-baseline path.
    /// </summary>
    private async Task SeedThinBaselineCpu(int avgCpu, int variance)
    {
        await ExecuteSeedAsync("BEGIN TRANSACTION");
        var rng = new Random(42);
        foreach (var day in new[] { 7, 14 })
        {
            var baseDay = SeedDayStart(day);
            for (int i = 0; i < 8; i++)
            {
                var cpu = Math.Clamp(avgCpu + rng.Next(-variance, variance + 1), 0, 100);
                await SeedCpuAsync(baseDay.AddMinutes(i * 3), cpu);
            }
        }
        await ExecuteSeedAsync("COMMIT");
    }

    private async Task SeedBaselinePerfmon(string counterName, long avgValue, int variance)
    {
        await ExecuteSeedAsync("BEGIN TRANSACTION");
        var rng = new Random(42);
        for (int day = 1; day <= 14; day++)
        {
            var baseDay = SeedDayStart(day);
            for (int i = 0; i < 4; i++)
            {
                var value = Math.Max(0, avgValue + rng.Next(-variance, variance + 1));
                await SeedPerfmonAsync(baseDay.AddMinutes(i * 3), counterName, value);
            }
        }
        await ExecuteSeedAsync("COMMIT");
    }

    private async Task SeedBaselineSessions(int avgConnections, int variance)
    {
        await ExecuteSeedAsync("BEGIN TRANSACTION");
        var rng = new Random(42);
        for (int day = 1; day <= 14; day++)
        {
            var baseDay = SeedDayStart(day);
            for (int i = 0; i < 4; i++)
            {
                var count = Math.Max(1, avgConnections + rng.Next(-variance, variance + 1));
                await SeedSessionStatAsync(baseDay.AddMinutes(i * 3), "App1", count);
            }
        }
        await ExecuteSeedAsync("COMMIT");
    }

    private async Task SeedBaselineQueryStats(long avgElapsed, int variance)
    {
        await ExecuteSeedAsync("BEGIN TRANSACTION");
        var rng = new Random(42);
        for (int day = 1; day <= 14; day++)
        {
            var baseDay = SeedDayStart(day);
            for (int i = 0; i < 4; i++)
            {
                var elapsed = Math.Max(0, avgElapsed + rng.Next(-variance, variance + 1));
                await SeedQueryStatAsync(baseDay.AddMinutes(i * 3), elapsed, 100);
            }
        }
        await ExecuteSeedAsync("COMMIT");
    }

    private async Task SeedBaselineWaits()
    {
        await ExecuteSeedAsync("BEGIN TRANSACTION");
        for (int day = 1; day <= 14; day++)
        {
            var baseDay = SeedDayStart(day);
            for (int i = 0; i < 4; i++)
                await SeedWaitStatAsync(baseDay.AddMinutes(i * 3), "SOS_SCHEDULER_YIELD", 100);
        }
        await ExecuteSeedAsync("COMMIT");
    }

    private async Task SeedBaselineMemory(double avgTotalServerMb, double targetMb)
    {
        await ExecuteSeedAsync("BEGIN TRANSACTION");
        for (int day = 1; day <= 14; day++)
        {
            var baseDay = SeedDayStart(day);
            for (int i = 0; i < 4; i++)
                await SeedMemoryStatAsync(baseDay.AddMinutes(i * 3), avgTotalServerMb, targetMb);
        }
        await ExecuteSeedAsync("COMMIT");
    }

    // ── Helpers: seed individual rows ──

    private async Task SeedCpuAsync(DateTime time, int cpuValue)
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
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuValue });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedPerfmonAsync(DateTime time, string counterName, long deltaValue)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO perfmon_stats
            (collection_id, collection_time, server_id, server_name,
             object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds)
            VALUES ($1, $2, $3, 'TestServer', 'SQLServer:SQL Statistics', $4, '', $5, $5, 10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = counterName });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaValue });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedWaitStatAsync(DateTime time, string waitType, long deltaWaitMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO wait_stats
            (collection_id, collection_time, server_id, server_name, wait_type,
             waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
             delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
            VALUES ($1, $2, $3, 'TestServer', $4, 0, 0, 0, 0, $5, 0)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitType });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaWaitMs });
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

    private async Task SeedQueryStatAsync(DateTime time, long deltaElapsed, long deltaExecCount)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO query_stats
            (collection_id, collection_time, server_id, server_name,
             execution_count, total_elapsed_time, total_worker_time,
             total_logical_reads, total_logical_writes, total_physical_reads,
             delta_execution_count, delta_elapsed_time, delta_worker_time,
             delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills)
            VALUES ($1, $2, $3, 'TestServer', $4, $5, 0, 0, 0, 0, $4, $5, 0, 0, 0, 0, 0, 0)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaExecCount });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaElapsed });
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
        cmd.Parameters.Add(new DuckDBParameter { Value = targetMb * 1.2 });
        cmd.Parameters.Add(new DuckDBParameter { Value = targetMb * 0.2 });
        cmd.Parameters.Add(new DuckDBParameter { Value = targetMb });
        cmd.Parameters.Add(new DuckDBParameter { Value = totalServerMb });
        await cmd.ExecuteNonQueryAsync();
    }
}
