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
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7885: the blocking and deadlock spike detectors compare the window's per-hour rate to the bucket's REAL mean. On a
/// server whose trusted baseline sits under one event an hour, which is most servers, "3x baseline" must mean three
/// times that mean, not three events an hour. The count gates (5 blocking events, 3 deadlocks) still bound the
/// numerator. The fixture is <c>EventBaselineCoveredDaysTests</c>'s: a month of SUCCESS runs ending before the analysis
/// day's UTC midnight makes thirty covered days at hour 14, and six events on six of those days make a trusted mean of
/// 0.2 an hour. The analysis window is the default four hours, starting at hour 14. Darling's
/// <c>SpikeBaselineFloorLiveTests</c> is the twin.
/// </summary>
public sealed class SpikeBaselineFloorTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -7885_01;
    private static readonly DateTime AnalysisTime = new(2026, 3, 4, 14, 0, 0);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _conn;
    private long _nextId = -1;

    public SpikeBaselineFloorTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _conn?.Dispose();

    [Fact]
    public async Task FourDeadlocksInFourHours_AgainstATrustedFifthOfOneAnHour_Fire()
    {
        await SeedTrustedBaselineAsync("deadlock", eventsInTheMonth: 6);

        var fact = Assert.Single(await DetectAsync("deadlock", windowEvents: 4), f => f.Key == "ANOMALY_DEADLOCK_SPIKE");

        Assert.Equal(0.0, fact.Metadata["is_new"]);
        Assert.Equal(0.2, fact.Metadata["baseline_rate"], 9);
        Assert.Equal(5.0, fact.Metadata["ratio"], 9);
        Assert.Equal(4.0, fact.Metadata["current_count"]);
    }

    [Fact]
    public async Task OneDeadlockInFourHours_DoesNotPassTheCountGate()
    {
        await SeedTrustedBaselineAsync("deadlock", eventsInTheMonth: 6);

        Assert.DoesNotContain(await DetectAsync("deadlock", windowEvents: 1), f => f.Key == "ANOMALY_DEADLOCK_SPIKE");
    }

    /// <summary>Four deadlocks in four hours is one an hour: twice a trusted 0.5 an hour, under the 3x bar.</summary>
    [Fact]
    public async Task FourDeadlocksAtTwiceATrustedBaseline_DoNotFire()
    {
        await SeedTrustedBaselineAsync("deadlock", eventsInTheMonth: 15);

        Assert.DoesNotContain(await DetectAsync("deadlock", windowEvents: 4), f => f.Key == "ANOMALY_DEADLOCK_SPIKE");
    }

    [Fact]
    public async Task SixBlockedProcessReportsInFourHours_AgainstATrustedFifthOfOneAnHour_Fire()
    {
        await SeedTrustedBaselineAsync("blocking", eventsInTheMonth: 6);

        var fact = Assert.Single(await DetectAsync("blocking", windowEvents: 6), f => f.Key == "ANOMALY_BLOCKING_SPIKE");

        Assert.Equal(0.0, fact.Metadata["is_new"]);
        Assert.Equal(0.2, fact.Metadata["baseline_rate"], 9);
        Assert.Equal(7.5, fact.Metadata["ratio"], 9);
        Assert.Equal(6.0, fact.Metadata["current_count"]);
    }

    [Fact]
    public async Task FourBlockedProcessReportsInFourHours_DoNotPassTheCountGate()
    {
        await SeedTrustedBaselineAsync("blocking", eventsInTheMonth: 6);

        Assert.DoesNotContain(await DetectAsync("blocking", windowEvents: 4), f => f.Key == "ANOMALY_BLOCKING_SPIKE");
    }

    /* ───────────────────────── seeding ───────────────────────── */

    /// <summary>A month of SUCCESS runs every fifteen minutes for the family's collector, ending before the analysis
    /// day's UTC midnight, and <paramref name="eventsInTheMonth"/> events at 14:10 on that many distinct days of it:
    /// thirty covered days at hour 14, so the pooled mean is the count over thirty.</summary>
    private async Task SeedTrustedBaselineAsync(string family, int eventsInTheMonth)
    {
        var collector = family == "blocking" ? "blocked_process_report" : "deadlocks";
        await ExecAsync(
            @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
              SELECT $1 - CAST(row_number() OVER () AS BIGINT), $2, 'TestServer', $3, t, 'SUCCESS'
              FROM generate_series($4::TIMESTAMP, $5::TIMESTAMP, INTERVAL 15 MINUTE) AS g(t)",
            -1_000_000_000L, ServerId, collector, AnalysisTime.AddDays(-35), AnalysisTime.AddMinutes(-15));
        for (var day = 1; day <= eventsInTheMonth; day++)
            await SeedEventAsync(family, AnalysisTime.AddDays(-day).AddMinutes(10));
        /* The detector wants any CPU row in the month before it looks at a family. */
        await ExecAsync(
            @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time,
                sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, 'TestServer', $2, 10, 2)",
            _nextId--, AnalysisTime.AddDays(-1), ServerId);
    }

    private Task SeedEventAsync(string family, DateTime at) => family == "blocking"
        ? ExecAsync(
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, blocked_spid, blocking_spid, wait_time_ms) VALUES ($1,$2,$2,$3,'TestServer',$4,60,1000)",
            _nextId--, at, ServerId, 100 + (int)(-_nextId % 1000))
        : ExecAsync(
            "INSERT INTO deadlocks (deadlock_id, collection_time, deadlock_time, server_id, server_name) VALUES ($1,$2,$2,$3,'TestServer')",
            _nextId--, at, ServerId);

    /// <summary>Plants <paramref name="windowEvents"/> events in the first minutes of the analysis hour and runs the real detector
    /// over the default four-hour window.</summary>
    private async Task<System.Collections.Generic.List<Fact>> DetectAsync(string family, int windowEvents)
    {
        for (var i = 0; i < windowEvents; i++)
            await SeedEventAsync(family, AnalysisTime.AddMinutes(10 + i));

        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = "TestServer",
            TimeRangeStart = AnalysisTime,
            TimeRangeEnd = AnalysisTime.AddHours(4),
        };
        return await new AnomalyDetector(_duckDb, new BaselineProvider(_duckDb)).DetectAnomaliesAsync(context);
    }

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
}
