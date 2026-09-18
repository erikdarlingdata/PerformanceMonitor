/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// End-to-end validation of the Performance Calendar's per-day-range aggregate against a real DuckDB:
/// that the grouped range SQL (day spine + per-source CTEs, arg_max top wait, FILTER counts, the
/// blocked-process-report -> DMV-snapshot blocking fallback, the actionable-alert filter) buckets each
/// seeded day correctly and that each day's composite band matches the shared calculator.
/// </summary>
public class PerformanceCalendarDataTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private DuckDBConnection? _seedConn;
    private const int ServerId = -779;
    private const string ServerName = "CalTestServer";
    private long _nextId = -1;

    private static readonly DateTime MonthStart = new(2026, 7, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime MonthEnd = new(2026, 8, 1, 0, 0, 0, DateTimeKind.Utc);

    public PerformanceCalendarDataTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
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

    private async Task ExecAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
            cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private Task SeedCollectionRunAsync(DateTime day, string status = "SUCCESS") =>
        ExecAsync(
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1,$2,$3,'wait_stats',$4,$5)",
            _nextId--, ServerId, ServerName, day.AddHours(1), status);

    private Task SeedWaitAsync(DateTime day, string waitType, long deltaMs) =>
        ExecAsync(
            "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_wait_time_ms) VALUES ($1,$2,$3,$4,$5,$6)",
            _nextId--, day.AddHours(1), ServerId, ServerName, waitType, deltaMs);

    private Task SeedCpuAsync(DateTime day, int sqlCpu, int otherCpu) =>
        ExecAsync(
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1,$2,$3,$4,$2,$5,$6)",
            _nextId--, day.AddHours(1), ServerId, ServerName, sqlCpu, otherCpu);

    private Task SeedDeadlockAsync(DateTime day) =>
        ExecAsync(
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name) VALUES ($1,$2,$3,$4)",
            _nextId--, day.AddHours(1), ServerId, ServerName);

    private Task SeedDmvBlockingAsync(DateTime day) =>
        ExecAsync(
            "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, monitor_loop) VALUES ($1,$2,$3,$4,1)",
            _nextId--, day.AddHours(1), ServerId, ServerName);

    private Task SeedBprAsync(DateTime day, long waitTimeMs) =>
        ExecAsync(
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, wait_time_ms) VALUES ($1,$2,$3,$4,$5)",
            _nextId--, day.AddHours(1), ServerId, ServerName, waitTimeMs);

    private Task SeedMemoryAsync(DateTime day, int process, int system) =>
        ExecAsync(
            "INSERT INTO memory_pressure_events (collection_id, collection_time, server_id, server_name, sample_time, memory_notification, memory_indicators_process, memory_indicators_system) VALUES ($1,$2,$3,$4,$2,'RESOURCE_MEMPHYSICAL_LOW',$5,$6)",
            _nextId--, day.AddHours(1), ServerId, ServerName, process, system);

    private Task SeedAlertAsync(DateTime day, string metric, bool dismissed) =>
        ExecAsync(
            "INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, dismissed) VALUES ($1,$2,$3,$4,1.0,1.0,$5)",
            day.AddHours(1), ServerId, ServerName, metric, dismissed);

    private static DateTime Day(int d) => new(2026, 7, d, 0, 0, 0, DateTimeKind.Utc);

    [Fact]
    public async Task GetDailySummaryRange_BucketsEachDay_AndBandsViaSharedCalculator()
    {
        // 07-02 Healthy despite one deadlock (#3525): deadlocks band as a RATE over the 24-hour day
        // through the card band's tiers, and 1/day is 0.04/hr — far under the 5/hr Warning tier. The
        // count still lands in the row (the drill and tooltip keep it); waits decide the top-wait ranking
        // (CXPACKET should win) but never the band.
        await SeedCollectionRunAsync(Day(2));
        await SeedWaitAsync(Day(2), "CXPACKET", 100_000);
        await SeedWaitAsync(Day(2), "PAGEIOLATCH_SH", 50_000);
        await SeedDeadlockAsync(Day(2));

        // 07-03 Critical: a deadlock STORM — 480 over the day is 20/hr, the Critical tier, proving the
        // rate path end-to-end through the live aggregate rather than through a hand-built signals struct.
        await SeedCollectionRunAsync(Day(3));
        for (var i = 0; i < 480; i++)
            await SeedDeadlockAsync(Day(3));

        // 07-05 Critical: 6 sustained high-CPU samples (>= threshold).
        await SeedCollectionRunAsync(Day(5));
        for (var i = 0; i < 6; i++)
            await SeedCpuAsync(Day(5), 70, 20); // 90 total >= 80

        // 07-08 Warning: no BPR but 3 DMV blocking snapshots (fallback) + 2 moderate high-CPU samples.
        await SeedCollectionRunAsync(Day(8));
        await SeedDmvBlockingAsync(Day(8));
        await SeedDmvBlockingAsync(Day(8));
        await SeedDmvBlockingAsync(Day(8));
        await SeedCpuAsync(Day(8), 85, 0);
        await SeedCpuAsync(Day(8), 82, 0);

        // 07-10 Warning: one actionable alert; a dismissed alert and a resolution notice must NOT count.
        await SeedCollectionRunAsync(Day(10));
        await SeedAlertAsync(Day(10), "High CPU", dismissed: false);
        await SeedAlertAsync(Day(10), "Blocking Detected", dismissed: true);
        await SeedAlertAsync(Day(10), "CPU Resolved", dismissed: false);

        // 07-12 Healthy: collected, quiet.
        await SeedCollectionRunAsync(Day(12));

        // 07-15 Critical: a collection ERROR (monitoring blind spot).
        await SeedCollectionRunAsync(Day(15), status: "SUCCESS");
        await SeedCollectionRunAsync(Day(15), status: "ERROR");

        // 07-18 Critical: severe memory pressure (process indicator >= 3).
        await SeedCollectionRunAsync(Day(18));
        await SeedMemoryAsync(Day(18), process: 3, system: 1);

        // 07-22 Warning via an alert alone (no collection-log run that day) -- the day must still appear
        // (alerts are part of the day spine), banded Warning, not dropped as No-Data.
        await SeedAlertAsync(Day(22), "Blocking Detected", dismissed: false);

        var rows = await _dataService.GetDailySummaryRangeAsync(ServerId, MonthStart, MonthEnd);
        var byDate = rows.ToDictionary(r => r.SummaryDate.Date);

        // Exactly the nine seeded days appear; unseeded days are absent (calendar renders them No-Data).
        Assert.Equal(9, rows.Count);
        Assert.False(byDate.ContainsKey(Day(20)));

        Assert.Equal(DailyHealthBand.Warning, byDate[Day(22)].HealthBand);
        Assert.Equal(1, byDate[Day(22)].AlertCount);

        Assert.Equal(DailyHealthBand.Healthy, byDate[Day(2)].HealthBand);
        Assert.Equal(1, byDate[Day(2)].DeadlockCount);
        Assert.Equal("CXPACKET", byDate[Day(2)].TopWaitType);
        Assert.Equal(150m, byDate[Day(2)].TotalWaitTimeSec);

        Assert.Equal(DailyHealthBand.Critical, byDate[Day(3)].HealthBand);
        Assert.Equal(480, byDate[Day(3)].DeadlockCount);

        Assert.Equal(DailyHealthBand.Critical, byDate[Day(5)].HealthBand);
        Assert.Equal(6, byDate[Day(5)].HighCpuEvents);

        Assert.Equal(DailyHealthBand.Warning, byDate[Day(8)].HealthBand);
        Assert.Equal(3, byDate[Day(8)].BlockingEvents);   // DMV fallback (0 BPR)
        Assert.Equal(2, byDate[Day(8)].HighCpuEvents);

        Assert.Equal(DailyHealthBand.Warning, byDate[Day(10)].HealthBand);
        Assert.Equal(1, byDate[Day(10)].AlertCount);       // dismissed + resolution excluded

        Assert.Equal(DailyHealthBand.Healthy, byDate[Day(12)].HealthBand);
        Assert.True(byDate[Day(12)].HasData);

        Assert.Equal(DailyHealthBand.Critical, byDate[Day(15)].HealthBand);
        Assert.Equal(1, byDate[Day(15)].CollectionErrors);

        Assert.Equal(DailyHealthBand.Critical, byDate[Day(18)].HealthBand);
        Assert.Equal(1, byDate[Day(18)].MemoryCriticalEvents);
        Assert.Equal(1, byDate[Day(18)].MemoryPressureEvents);
    }

    [Fact]
    public async Task GetDailySummaryRange_CarriesPeakBlockDuration_FromBlockedProcessReports()
    {
        // Two blocked-process reports on 07-14 with different wait times; the day's peak is the max, and the
        // BPR-sourced count wins over any DMV fallback (there is none here). Feeds the day-detail blocking
        // reason ("N blocking events (peak block X)").
        await SeedCollectionRunAsync(Day(14));
        await SeedBprAsync(Day(14), 4_200);
        await SeedBprAsync(Day(14), 12_500);

        var rows = await _dataService.GetDailySummaryRangeAsync(ServerId, MonthStart, MonthEnd);
        var row = rows.Single(r => r.SummaryDate.Date == Day(14));

        Assert.Equal(2, row.BlockingEvents);          // BPR-sourced count (preferred over DMV)
        Assert.Equal(12_500, row.MaxBlockDurationMs); // peak = MAX(wait_time_ms)
    }

    [Fact]
    public async Task GetDailySummary_SingleDay_DelegatesToRange_AndReturnsNoDataRowWhenEmpty()
    {
        await SeedCollectionRunAsync(Day(2));
        await SeedDeadlockAsync(Day(2));

        var seeded = await _dataService.GetDailySummaryAsync(ServerId, Day(2));
        Assert.NotNull(seeded);
        Assert.True(seeded!.HasData);
        /* One deadlock in a day is 0.04/hr — Healthy under the rate band (#3525); the count still rides
           the row, which is what distinguishes this from the No-Data arm below. */
        Assert.Equal(1, seeded.DeadlockCount);
        Assert.Equal(DailyHealthBand.Healthy, seeded.HealthBand);
        Assert.Equal("Healthy", seeded.OverallHealth);

        var empty = await _dataService.GetDailySummaryAsync(ServerId, Day(25));
        Assert.NotNull(empty);
        Assert.False(empty!.HasData);
        Assert.Equal(DailyHealthBand.NoData, empty.HealthBand);
        Assert.Equal("No Data", empty.OverallHealth);
    }
}
