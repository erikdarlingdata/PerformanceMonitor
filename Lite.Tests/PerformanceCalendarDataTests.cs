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

    /* The fixture month is the calendar month TWO months before the current one, not a fixed month
       (#3541 A9): the daily summary now judges each day against the store's retention horizon (Lite:
       RetentionService.ArchiveRetentionMonths back from the wall clock), and a fixed July 2026 would have
       drifted past that horizon within weeks of landing, turning every band below into No Data on a date
       nobody changed. Two months back is always inside a three-month horizon and always a finished month,
       so the still-forming-day clamp never applies; the day-number comments below ("07-10") read as
       "day 10 of the fixture month". */
    private static readonly DateTime MonthStart = new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, 1, 0, 0, 0, DateTimeKind.Utc).AddMonths(-2);
    private static readonly DateTime MonthEnd = MonthStart.AddMonths(1);

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

    private static DateTime Day(int d) => MonthStart.AddDays(d - 1);

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

        // 07-05 Critical: SUSTAINED high CPU — 30 hot samples, the day-scale bar the sustained-heat rate
        // (1.25/hr) sets for a finished 24-hour day (#3539 A2). Six used to redden a day and now turns it
        // amber (see 07-08); this proves the rate path through the live aggregate.
        await SeedCollectionRunAsync(Day(5));
        for (var i = 0; i < 30; i++)
            await SeedCpuAsync(Day(5), 70, 20); // 90 total >= 80

        // 07-08 Warning: no BPR but 3 DMV blocking snapshots (the fallback count, 0.125/hr — Healthy by
        // rate, no wait time for the wait arm) + 6 moderate high-CPU samples, which is the day's Warning
        // bar (#3539 A2: the old Critical count is the new Warning count).
        await SeedCollectionRunAsync(Day(8));
        await SeedDmvBlockingAsync(Day(8));
        await SeedDmvBlockingAsync(Day(8));
        await SeedDmvBlockingAsync(Day(8));
        for (var i = 0; i < 6; i++)
            await SeedCpuAsync(Day(8), 85, 0);

        // 07-09 Warning via the blocking WAIT arm alone (#3539 A3): one 15-second block is a Warning day
        // whatever the rate — a per-event magnitude claim, not a frequency one.
        await SeedCollectionRunAsync(Day(9));
        await SeedBprAsync(Day(9), 15_000);

        // 07-10 Warning: one actionable alert; a dismissed alert and a resolution notice must NOT count.
        await SeedCollectionRunAsync(Day(10));
        await SeedAlertAsync(Day(10), "High CPU", dismissed: false);
        await SeedAlertAsync(Day(10), "Blocking Detected", dismissed: true);
        await SeedAlertAsync(Day(10), "CPU Resolved", dismissed: false);

        // 07-11 Warning via the blocking RATE: 120 short blocks over the day is 5.0/hr, the Warning tier
        // exactly (#3539 A3) — each under the 10 s wait bar so the count arm is what decides.
        await SeedCollectionRunAsync(Day(11));
        for (var i = 0; i < 120; i++)
            await SeedBprAsync(Day(11), 6_000);

        // 07-12 Healthy: collected, quiet.
        await SeedCollectionRunAsync(Day(12));

        // 07-13 Critical via a single 60-second block (#3539 A3): the rate-independent Critical arm.
        await SeedCollectionRunAsync(Day(13));
        await SeedBprAsync(Day(13), 60_000);

        // 07-15 Warning: one collector run of two ERRORED — a 50% error share, past the collector-health
        // classifier's 20% bar, which is the share's Warning arm and never Critical (#3539 A2). Under the
        // presence rule this replaced, one ERROR row alone painted the day red.
        await SeedCollectionRunAsync(Day(15), status: "SUCCESS");
        await SeedCollectionRunAsync(Day(15), status: "ERROR");

        // 07-16 Healthy despite an ERROR run: one of eleven runs is a 9% share, under the bar — disclosed
        // on the cell with its share, not banded (#3539 A2).
        for (var i = 0; i < 10; i++)
            await SeedCollectionRunAsync(Day(16), status: "SUCCESS");
        await SeedCollectionRunAsync(Day(16), status: "ERROR");

        // 07-18 Critical: severe memory pressure (process indicator >= 3).
        await SeedCollectionRunAsync(Day(18));
        await SeedMemoryAsync(Day(18), process: 3, system: 1);

        // 07-22 Warning via an alert alone (no collection-log run that day) -- the day must still appear
        // (alerts are part of the day spine), banded Warning, not dropped as No-Data.
        await SeedAlertAsync(Day(22), "Blocking Detected", dismissed: false);

        var rows = await _dataService.GetDailySummaryRangeAsync(ServerId, MonthStart, MonthEnd);
        var byDate = rows.ToDictionary(r => r.SummaryDate.Date);

        // Exactly the thirteen seeded days appear; unseeded days are absent (calendar renders them No-Data).
        Assert.Equal(13, rows.Count);
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
        Assert.Equal(30, byDate[Day(5)].HighCpuEvents);

        Assert.Equal(DailyHealthBand.Warning, byDate[Day(8)].HealthBand);
        Assert.Equal(3, byDate[Day(8)].BlockingEvents);   // DMV fallback (0 BPR)
        Assert.Equal(6, byDate[Day(8)].HighCpuEvents);

        Assert.Equal(DailyHealthBand.Warning, byDate[Day(9)].HealthBand);
        Assert.Equal(15_000, byDate[Day(9)].MaxBlockDurationMs);
        Assert.Contains("1 blocking event (0.0/hr, peak block 15.0 s)", byDate[Day(9)].SignalsTooltip, StringComparison.Ordinal);

        Assert.Equal(DailyHealthBand.Warning, byDate[Day(10)].HealthBand);
        Assert.Equal(1, byDate[Day(10)].AlertCount);       // dismissed + resolution excluded

        Assert.Equal(DailyHealthBand.Warning, byDate[Day(11)].HealthBand);
        Assert.Equal(120, byDate[Day(11)].BlockingEvents);
        Assert.Contains("120 blocking events (5.0/hr, peak block 6.0 s)", byDate[Day(11)].SignalsTooltip, StringComparison.Ordinal);

        Assert.Equal(DailyHealthBand.Healthy, byDate[Day(12)].HealthBand);
        Assert.True(byDate[Day(12)].HasData);

        Assert.Equal(DailyHealthBand.Critical, byDate[Day(13)].HealthBand);
        Assert.Equal(60_000, byDate[Day(13)].MaxBlockDurationMs);

        Assert.Equal(DailyHealthBand.Warning, byDate[Day(15)].HealthBand);
        Assert.Equal(1, byDate[Day(15)].CollectionErrors);
        Assert.Equal(2, byDate[Day(15)].CollectionRuns);
        Assert.Contains("1 collection error (50.0% of 2 runs)", byDate[Day(15)].SignalsTooltip, StringComparison.Ordinal);

        Assert.Equal(DailyHealthBand.Healthy, byDate[Day(16)].HealthBand);
        Assert.Equal(1, byDate[Day(16)].CollectionErrors);
        Assert.Equal(11, byDate[Day(16)].CollectionRuns);

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
        /* #3539 A2: the peak is also the blocking band's wait arm — 12.5 s is past the 10 s Warning bar, so
           the day is Warning on the wait alone (two events over a day is 0.08/hr). */
        Assert.Equal(DailyHealthBand.Warning, row.HealthBand);
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
