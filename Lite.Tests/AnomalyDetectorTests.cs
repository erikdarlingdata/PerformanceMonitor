using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests for the upgraded AnomalyDetector: time-bucketed baselines, new detection
/// methods (batch requests, sessions, query duration, memory), per-metric thresholds,
/// and baseline context metadata.
/// </summary>
public class AnomalyDetectorTests : IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly BaselineProvider _baselineProvider;
    private readonly AnomalyDetector _detector;

    private const int ServerId = -999;
    private const string ServerName = "TestServer";

    // Fixed timestamps for deterministic testing
    private static readonly DateTime _now = DateTime.UtcNow;
    private static readonly DateTime _analysisEnd = _now;
    private static readonly DateTime _analysisStart = _now.AddHours(-4);

    private long _nextId = -1;

    public AnomalyDetectorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "AnomalyTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        var dbPath = Path.Combine(_tempDir, "test.duckdb");
        _duckDb = new DuckDbInitializer(dbPath);
        _baselineProvider = new BaselineProvider(_duckDb);
        _detector = new AnomalyDetector(_duckDb, _baselineProvider);
        BaselineProvider.CacheTtl = TimeSpan.FromMilliseconds(1);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch { }
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
        await _duckDb.InitializeAsync();

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
        await _duckDb.InitializeAsync();

        await SeedBaselinePerfmon("Batch Requests/sec", 5000, variance: 200);

        // Analysis window: same as baseline
        for (int i = 0; i < 16; i++)
            await SeedPerfmonAsync(_analysisStart.AddMinutes(i * 15), "Batch Requests/sec", 5000);

        await SeedBaselineCpu(10, variance: 2);

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        Assert.DoesNotContain(anomalies, f => f.Key == "ANOMALY_BATCH_REQUESTS");
    }

    // ── Session Count ──

    [Fact]
    public async Task DetectSessionAnomalies_Spike_DetectsAnomaly()
    {
        await _duckDb.InitializeAsync();

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
        await _duckDb.InitializeAsync();

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

    // ── Query Duration ──

    [Fact]
    public async Task DetectQueryDurationAnomalies_Spike_DetectsAnomaly()
    {
        await _duckDb.InitializeAsync();

        // Baseline: ~10000 microseconds total elapsed per collection
        await SeedBaselineQueryStats(10_000, variance: 1000);

        // Analysis window: spike to 500000 microseconds
        for (int i = 0; i < 16; i++)
            await SeedQueryStatAsync(_analysisStart.AddMinutes(i * 15), 500_000, 100);

        await SeedBaselineCpu(10, variance: 2);
        await SeedBaselineWaits();

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext());

        Assert.Contains(anomalies, f => f.Key == "ANOMALY_QUERY_DURATION");
    }

    // ── Memory Pressure ──

    [Fact]
    public async Task DetectMemoryAnomalies_HighPressure_DetectsAnomaly()
    {
        await _duckDb.InitializeAsync();

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
    public async Task DetectMemoryAnomalies_Normal_NoAnomaly()
    {
        await _duckDb.InitializeAsync();

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
        await _duckDb.InitializeAsync();

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
        await _duckDb.InitializeAsync();

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

    // ── No baseline = no anomalies ──

    [Fact]
    public async Task DetectAnomalies_NoBaselineData_ReturnsEmpty()
    {
        await _duckDb.InitializeAsync();

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
        var rng = new Random(42);
        for (int day = 1; day <= 14; day++)
        {
            var baseDay = _analysisStart.AddDays(-day);
            for (int i = 0; i < 4; i++)
            {
                var cpu = Math.Clamp(avgCpu + rng.Next(-variance, variance + 1), 0, 100);
                await SeedCpuAsync(baseDay.AddMinutes(i * 3), cpu);
            }
        }
    }

    private async Task SeedBaselinePerfmon(string counterName, long avgValue, int variance)
    {
        var rng = new Random(42);
        for (int day = 1; day <= 14; day++)
        {
            var baseDay = _analysisStart.AddDays(-day);
            for (int i = 0; i < 4; i++)
            {
                var value = Math.Max(0, avgValue + rng.Next(-variance, variance + 1));
                await SeedPerfmonAsync(baseDay.AddMinutes(i * 3), counterName, value);
            }
        }
    }

    private async Task SeedBaselineSessions(int avgConnections, int variance)
    {
        var rng = new Random(42);
        for (int day = 1; day <= 14; day++)
        {
            var baseDay = _analysisStart.AddDays(-day);
            for (int i = 0; i < 4; i++)
            {
                var count = Math.Max(1, avgConnections + rng.Next(-variance, variance + 1));
                await SeedSessionStatAsync(baseDay.AddMinutes(i * 3), "App1", count);
            }
        }
    }

    private async Task SeedBaselineQueryStats(long avgElapsed, int variance)
    {
        var rng = new Random(42);
        for (int day = 1; day <= 14; day++)
        {
            var baseDay = _analysisStart.AddDays(-day);
            for (int i = 0; i < 4; i++)
            {
                var elapsed = Math.Max(0, avgElapsed + rng.Next(-variance, variance + 1));
                await SeedQueryStatAsync(baseDay.AddMinutes(i * 3), elapsed, 100);
            }
        }
    }

    private async Task SeedBaselineWaits()
    {
        for (int day = 1; day <= 14; day++)
        {
            var baseDay = _analysisStart.AddDays(-day);
            for (int i = 0; i < 4; i++)
                await SeedWaitStatAsync(baseDay.AddMinutes(i * 3), "SOS_SCHEDULER_YIELD", 100);
        }
    }

    private async Task SeedBaselineMemory(double avgTotalServerMb, double targetMb)
    {
        for (int day = 1; day <= 14; day++)
        {
            var baseDay = _analysisStart.AddDays(-day);
            for (int i = 0; i < 4; i++)
                await SeedMemoryStatAsync(baseDay.AddMinutes(i * 3), avgTotalServerMb, targetMb);
        }
    }

    // ── Helpers: seed individual rows ──

    private async Task SeedCpuAsync(DateTime time, int cpuValue)
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
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
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

    private async Task SeedQueryStatAsync(DateTime time, long deltaElapsed, long deltaExecCount)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();
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
        cmd.Parameters.Add(new DuckDBParameter { Value = targetMb * 1.2 });
        cmd.Parameters.Add(new DuckDBParameter { Value = targetMb * 0.2 });
        cmd.Parameters.Add(new DuckDBParameter { Value = targetMb });
        cmd.Parameters.Add(new DuckDBParameter { Value = totalServerMb });
        await cmd.ExecuteNonQueryAsync();
    }
}
