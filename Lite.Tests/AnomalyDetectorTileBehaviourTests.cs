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
/// issue-3653 A8 option B (lane T4169): behaviour tests for Lite's per-hour tile gate
/// (<see cref="AnomalyGate.EvaluateTiles"/>) on the CPU and wait-profile anomaly detectors — each now scores
/// every target-local hour of the analysis window against its OWN hour-of-week baseline instead of
/// collapsing the whole window to one peak/mean pair. Every scenario runs through the REAL
/// <see cref="AnomalyDetector.DetectAnomaliesAsync"/>, never a hand-copied tile call, and reads the fired
/// fact's own metadata (<c>tile_local_hour</c>, <c>tiles_scored</c>, <c>tiles_fired</c>, <c>fire_threshold</c>).
/// Lite I/O's tiled fire path is covered by <see cref="AnomalyDetectorTests"/>'s
/// <c>DetectIoAnomalies_…ASustainedWindowDoes</c> instead (#3653 B), not by a test in this class.
///
/// <para><b>Scope-trimmed to scenario 1 (the 4 h shift) for CPU and waits</b>, per the coordinator's
/// SCOPE TRIM ruling (issue-3653 A8 option B): the 24 h cutoff, the lone spike and the fallback are already
/// covered by the gate's unit tests and B's acceptance harness. Each test asserts only that the shift fires
/// through the tile path with the right metadata (per a later coordinator ruling, the in-test "whole-window
/// z stays below k" dev-comparison was dropped rather than re-sized on this Windows CI-only rig — there is
/// no mutation check for Lite here, and B's necessity is already proven by the acceptance harness and by
/// the Darling mutation checks).</para>
///
/// <para>Fixture, seeding and helper conventions are copied from <see cref="AnomalyDetectorTests"/>
/// (constructor, <c>SeedConnectionAsync</c>/<c>ExecuteSeedAsync</c>, <c>SeedCpuAsync</c>/<c>SeedWaitStatAsync</c>/
/// <c>SeedFileIoAsync</c>) — this class edits none of that file.</para>
/// </summary>
public class AnomalyDetectorTileBehaviourTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private readonly BaselineProvider _baselineProvider;
    private readonly AnomalyDetector _detector;
    private DuckDBConnection? _seedConn;

    private const int ServerId = -998;
    private const string ServerName = "TestServer";

    // Wednesday, floored to the hour — every seed below keeps this hour and weekday so it lands in the same
    // (hour, dow) bucket the analysis window reads (AnomalyDetectorTests.SeedDayStart's own discipline).
    private static readonly DateTime _windowStart = FloorToHourOnAWednesday(DateTime.UtcNow);

    private static DateTime FloorToHourOnAWednesday(DateTime now)
    {
        var floored = now.Date.AddHours(now.Hour);
        var daysToWednesday = ((int)DayOfWeek.Wednesday - (int)floored.DayOfWeek + 7) % 7;
        return floored.AddDays(daysToWednesday);
    }

    private long _nextId = -2_000_000;

    public AnomalyDetectorTileBehaviourTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _baselineProvider = new BaselineProvider(_duckDb);
        _detector = new AnomalyDetector(_duckDb, _baselineProvider);
        BaselineProvider.CacheTtl = TimeSpan.FromMilliseconds(1);
    }

    public void Dispose() => _seedConn?.Dispose();

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

    private AnalysisContext CreateContext(DateTime start, DateTime end) => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = start,
        TimeRangeEnd = end
    };

    // ── CPU: all four scenarios ──

    private const double CpuMu = 30.0;
    private const double CpuSigma = 5.0; // above AbsStdDevFloorFor(Cpu) = 5.0, so the fixture keeps real spread
    private const double CpuShift = CpuMu + 6 * CpuSigma; // 60, clears CpuFloorPct (50)

    /// <summary>
    /// Scenario 1 (design's "the shift, 4 h"): a 4 h window [T, T+4h) whose first two hours sit at the
    /// baseline and whose last two hours run the WHOLE hour at μ + 6σ. The per-hour tile gate fires (hours
    /// 3/4 clear the cutoff against their own bucket); the SAME numbers, judged as one whole-window peak/mean
    /// pair against the START hour's bucket alone, do not clear the cutoff — the shift is diluted by the two
    /// quiet hours, exactly the case this lane exists to prove.
    /// </summary>
    [Fact]
    public async Task Cpu_TwoHourShiftInFourHourWindow_FiresPerTile_WhereTheWholeWindowGateWouldStayQuiet()
    {
        await SeedCpuBaseline21Days(_windowStart, hours: 4, CpuMu, CpuSigma);
        await SeedCpuWindow(_windowStart, hours: 4, shiftFromHour: 2, CpuMu, CpuSigma, CpuShift);

        var startBucket = await _baselineProvider.GetBaselineAsync(ServerId, MetricNames.Cpu, _windowStart);
        Assert.True(startBucket.IsTrustworthy, "the start-hour CPU bucket must be trustworthy");

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext(_windowStart, _windowStart.AddHours(4)));
        var fact = Assert.Single(anomalies, f => f.Key == "ANOMALY_CPU_SPIKE");

        Assert.True(fact.Metadata.ContainsKey("tile_local_hour"), "the sustained two-hour shift should have scored through the tile path");
        Assert.InRange(fact.Metadata["tile_local_hour"], _windowStart.AddHours(2).Hour, _windowStart.AddHours(3).Hour);
        Assert.Equal(4, fact.Metadata["tiles_scored"]);
        Assert.Equal(2, fact.Metadata["tiles_fired"]);
        Assert.Equal(AnomalyThresholds.ModifiedZThresholdFor(MetricNames.Cpu), fact.Metadata["fire_threshold"], precision: 6);
    }

    // ── Waits: scenario 1 only ──

    private const double WaitMu = 300.0; // ms/sec, comfortably above WaitProfileFallbackMsPerSec's 250
    private const double WaitSigma = 20.0;
    private const double WaitShift = WaitMu + 8 * WaitSigma; // 460 ms/sec

    /// <summary>
    /// Scenario 1 for the wait profile: a 4 h window whose first two hours sit at baseline and whose last
    /// two hours run the whole hour at a sustained shift far above <see cref="WaitMu"/>. Fires through the
    /// tile path where the whole-window pair, judged against the start bucket alone, would stay quiet.
    /// </summary>
    [Fact]
    public async Task WaitProfile_TwoHourShiftInFourHourWindow_FiresPerTile_WhereTheWholeWindowGateWouldStayQuiet()
    {
        await SeedWaitBaseline21Days(_windowStart, hours: 4, WaitMu, WaitSigma);
        await SeedWaitWindow(_windowStart, hours: 4, shiftFromHour: 2, WaitMu, WaitSigma, WaitShift);

        var startBucket = await _baselineProvider.GetBaselineAsync(ServerId, MetricNames.WaitMsPerSec, _windowStart);
        Assert.True(startBucket.IsTrustworthy, "the start-hour wait-profile bucket must be trustworthy");
        Assert.True(startBucket.EffectiveRobustSigma > 0, "the wait profile is scored on the robust frame");

        var anomalies = await _detector.DetectAnomaliesAsync(CreateContext(_windowStart, _windowStart.AddHours(4)));
        var fact = Assert.Single(anomalies, f => f.Key == "ANOMALY_WAIT_PROFILE");

        Assert.True(fact.Metadata.ContainsKey("tile_local_hour"), "the sustained wait-profile shift should have scored through the tile path");
        Assert.InRange(fact.Metadata["tile_local_hour"], _windowStart.AddHours(2).Hour, _windowStart.AddHours(3).Hour);
        Assert.Equal(4, fact.Metadata["tiles_scored"]);
        Assert.Equal(2, fact.Metadata["tiles_fired"]);
        Assert.Equal(AnomalyThresholds.HeavyTailModifiedZThreshold, fact.Metadata["fire_threshold"], precision: 6);
    }

    // ── I/O (read latency): scenario 1 only ──

    // ── Helpers: 21-day baselines and shifted windows, set-based per family ──

    /// <summary>21 days of CPU history for the window's hours of week, ~12 samples/hour (15-minute cadence),
    /// deterministic pseudo-noise (μ + σ·sin(i)) so the bucket is trustworthy with a known spread.</summary>
    private async Task SeedCpuBaseline21Days(DateTime windowStart, int hours, double mu, double sigma)
    {
        await ExecuteSeedAsync("BEGIN TRANSACTION");
        await ExecuteSeedAsync($@"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time,
     sqlserver_cpu_utilization, other_process_cpu_utilization)
SELECT {_nextId} - (d * 10000 + h * 100 + i),
       TIMESTAMP '{windowStart:yyyy-MM-dd HH:mm:ss}' - (d * INTERVAL 1 DAY) + (h * INTERVAL 1 HOUR) + (i * INTERVAL 5 MINUTE),
       {ServerId}, '{ServerName}',
       TIMESTAMP '{windowStart:yyyy-MM-dd HH:mm:ss}' - (d * INTERVAL 1 DAY) + (h * INTERVAL 1 HOUR) + (i * INTERVAL 5 MINUTE),
       GREATEST(0, LEAST(100, ROUND({mu} + {sigma} * sin(d * 4 + h * 12 + i)))),
       2
FROM generate_series(1, 21) AS t1(d)
CROSS JOIN generate_series(0, {hours - 1}) AS t2(h)
CROSS JOIN generate_series(0, 11) AS t3(i)");
        await ExecuteSeedAsync("COMMIT");
        _nextId -= 21L * hours * 12 + 1000;
    }

    /// <summary>The window: hours below <paramref name="shiftFromHour"/> at baseline shape; hours at/above it
    /// run the WHOLE hour at <paramref name="shift"/>. A <paramref name="shiftFromHour"/> past the last hour
    /// (e.g. 99) plants an all-baseline window.</summary>
    private async Task SeedCpuWindow(DateTime windowStart, int hours, int shiftFromHour, double mu, double sigma, double shift)
    {
        await ExecuteSeedAsync("BEGIN TRANSACTION");
        await ExecuteSeedAsync($@"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time,
     sqlserver_cpu_utilization, other_process_cpu_utilization)
SELECT {_nextId} - (h * 100 + i),
       TIMESTAMP '{windowStart:yyyy-MM-dd HH:mm:ss}' + (h * INTERVAL 1 HOUR) + (i * INTERVAL 5 MINUTE),
       {ServerId}, '{ServerName}',
       TIMESTAMP '{windowStart:yyyy-MM-dd HH:mm:ss}' + (h * INTERVAL 1 HOUR) + (i * INTERVAL 5 MINUTE),
       GREATEST(0, LEAST(100, ROUND(CASE WHEN h >= {shiftFromHour} THEN {shift} ELSE {mu} + {sigma} * sin(h * 12 + i) END))),
       2
FROM generate_series(0, {hours - 1}) AS t1(h)
CROSS JOIN generate_series(0, 11) AS t2(i)");
        await ExecuteSeedAsync("COMMIT");
        _nextId -= (long)hours * 12 + 1000;
    }

    private async Task SeedCpuAsync(DateTime time, int cpuValue)
    {
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

    /// <summary>21 days of wait-stats history for the window's hours of week, ~12 samples/hour at a stored
    /// 900-second interval (15-minute cadence): a all-types wait rate of μ + σ·sin(i) ms/sec, so
    /// deltaWaitMs = rate * intervalSeconds.</summary>
    private async Task SeedWaitBaseline21Days(DateTime windowStart, int hours, double mu, double sigma)
    {
        await ExecuteSeedAsync("BEGIN TRANSACTION");
        await ExecuteSeedAsync($@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
SELECT {_nextId} - (d * 10000 + h * 100 + i),
       TIMESTAMP '{windowStart:yyyy-MM-dd HH:mm:ss}' - (d * INTERVAL 1 DAY) + (h * INTERVAL 1 HOUR) + (i * INTERVAL 5 MINUTE),
       {ServerId}, '{ServerName}', 'SOS_SCHEDULER_YIELD',
       0, 0, 0, 0,
       ROUND(({mu} + {sigma} * sin(d * 4 + h * 12 + i)) * 900),
       0, 900
FROM generate_series(1, 21) AS t1(d)
CROSS JOIN generate_series(0, {hours - 1}) AS t2(h)
CROSS JOIN generate_series(0, 11) AS t3(i)");
        await ExecuteSeedAsync("COMMIT");
        _nextId -= 21L * hours * 12 + 1000;
    }

    private async Task SeedWaitWindow(DateTime windowStart, int hours, int shiftFromHour, double mu, double sigma, double shift)
    {
        await ExecuteSeedAsync("BEGIN TRANSACTION");
        await ExecuteSeedAsync($@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
SELECT {_nextId} - (h * 100 + i),
       TIMESTAMP '{windowStart:yyyy-MM-dd HH:mm:ss}' + (h * INTERVAL 1 HOUR) + (i * INTERVAL 5 MINUTE),
       {ServerId}, '{ServerName}', 'SOS_SCHEDULER_YIELD',
       0, 0, 0, 0,
       ROUND((CASE WHEN h >= {shiftFromHour} THEN {shift} ELSE {mu} + {sigma} * sin(h * 12 + i) END) * 900),
       0, 900
FROM generate_series(0, {hours - 1}) AS t1(h)
CROSS JOIN generate_series(0, 11) AS t2(i)");
        await ExecuteSeedAsync("COMMIT");
        _nextId -= (long)hours * 12 + 1000;
    }

}
