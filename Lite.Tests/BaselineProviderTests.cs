using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests for BaselineProvider: time-bucketed baseline computation, bucket collapse
/// with hysteresis, restart poisoning exclusion, and division-by-zero handling.
/// </summary>
public class BaselineProviderTests : IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly BaselineProvider _provider;

    private const int ServerId = -999;

    // Analysis time is pinned to a known hour+dow for deterministic bucket matching.
    // Wednesday 14:00 UTC (dow=3 in DuckDB where Sunday=0)
    private static readonly DateTime AnalysisTime = new(2026, 4, 1, 14, 0, 0, DateTimeKind.Utc);

    private long _nextId = -1;

    public BaselineProviderTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "BaselineTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        var dbPath = Path.Combine(_tempDir, "test.duckdb");
        _duckDb = new DuckDbInitializer(dbPath);
        _provider = new BaselineProvider(_duckDb);
        // Use very short TTL so cache doesn't interfere between tests
        BaselineProvider.CacheTtl = TimeSpan.FromMilliseconds(1);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch { /* Best-effort cleanup */ }
    }

    // ── Full bucket: enough samples in one hour+dow ──

    [Fact]
    public async Task GetBaseline_FullBucket_ReturnsMeanAndStdDev()
    {
        await _duckDb.InitializeAsync();

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
        await _duckDb.InitializeAsync();

        // Seed only 5 samples on Wednesday 14:xx (below CollapseThreshold=10)
        var wednesday = AnalysisTime.AddDays(-7);
        for (int i = 0; i < 5; i++)
            await SeedCpuAsync(wednesday.AddMinutes(i * 10), 40 + i);

        // Seed 15 samples on other days at 14:xx (enough for hour-only)
        for (int dow = 0; dow < 3; dow++) // Sun, Mon, Tue
        {
            var day = AnalysisTime.AddDays(-7 - dow - 4); // Different days, same hour
            for (int i = 0; i < 5; i++)
                await SeedCpuAsync(day.AddMinutes(i * 10), 60 + i);
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
        await _duckDb.InitializeAsync();

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

    // ── Empty baseline ──

    [Fact]
    public async Task GetBaseline_NoData_ReturnsEmpty()
    {
        await _duckDb.InitializeAsync();

        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, AnalysisTime);

        Assert.Equal(0, baseline.SampleCount);
    }

    // ── Hysteresis: between collapse and restore thresholds ──

    [Fact]
    public async Task GetBaseline_BetweenThresholds_UsesFullBucket()
    {
        await _duckDb.InitializeAsync();

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
        await _duckDb.InitializeAsync();

        // Seed batch requests with a restart-shaped drop in the middle
        var baseDay = AnalysisTime.AddDays(-7);
        var normalValues = new[] { 5000, 5100, 4900, 5200, 5050, 4950 };

        for (int i = 0; i < normalValues.Length; i++)
            await SeedPerfmonAsync(baseDay.AddMinutes(i * 10), "Batch Requests/sec", normalValues[i]);

        // Restart drop: value falls to 0 then recovers
        await SeedPerfmonAsync(baseDay.AddMinutes(60), "Batch Requests/sec", 0);     // Restart
        await SeedPerfmonAsync(baseDay.AddMinutes(70), "Batch Requests/sec", 5100);  // Recovery

        // Add enough more samples on other days to reach threshold
        for (int d = 2; d <= 4; d++)
        {
            var day = AnalysisTime.AddDays(-7 * d);
            for (int i = 0; i < 5; i++)
                await SeedPerfmonAsync(day.AddMinutes(i * 10), "Batch Requests/sec", 5000 + i * 50);
        }

        _provider.ClearCache();
        var baseline = await _provider.GetBaselineAsync(ServerId, MetricNames.BatchRequests, AnalysisTime);

        // The restart drop (0) should be excluded, so mean should be near 5000, not pulled toward 0
        Assert.True(baseline.Mean > 4000, $"Mean {baseline.Mean} should not be poisoned by restart drop");
    }

    // ── Wait stats: per-collection aggregation ──

    [Fact]
    public async Task GetBaseline_WaitStats_AggregatesPerCollection()
    {
        await _duckDb.InitializeAsync();

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

    // ── Session count: per-collection aggregation ──

    [Fact]
    public async Task GetBaseline_SessionCount_AggregatesPerCollection()
    {
        await _duckDb.InitializeAsync();

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
        await _duckDb.InitializeAsync();

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
        await _duckDb.InitializeAsync();

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
        await _duckDb.InitializeAsync();

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
        await _duckDb.InitializeAsync();

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
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();
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

    private async Task SeedPerfmonAsync(DateTime time, string counterName, long deltaValue)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();
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
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();
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
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();
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
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();
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
