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
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #7893, the Lite twin of the Darling live tests in <c>AnalysisCoverageLivePostgresTests</c>: the DEADLOCKS value is the
/// busiest one-hour bucket of the window, the server-hour grain the (5, 20) pair was measured on, so the same storm grades
/// the same on a 4 hour pass and a 24 hour read. The whole-window average stays published as <c>deadlocks_per_hour</c>.
/// The wait samples planted here are the coverage witness, a reading every fifteen minutes.
/// </summary>
public sealed class DeadlockPeakHourTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 78930;
    private const string ServerName = "SynthDeadlockPeakSrv";

    private static readonly DateTime WindowEnd = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerMinute * TimeSpan.TicksPerMinute),
        DateTimeKind.Unspecified);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public DeadlockPeakHourTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>Twenty deadlocks inside the hour [End-2h, End-1h): one bucket under the window-start origin on both windows.</summary>
    private async Task PlantStormAsync()
    {
        var stormStart = WindowEnd.AddHours(-2);
        for (var i = 0; i < 20; i++)
            await PlantDeadlockAsync(stormStart.AddMinutes(3 * i));
    }

    [Fact]
    public async Task AStormOfTwentyInOneHour_GradesTheSame_OnAFourHourAndATwentyFourHourWindow()
    {
        await PlantCoverageAsync(WindowEnd.AddHours(-24), WindowEnd);
        await PlantStormAsync();

        var fourHours = await CollectDeadlocksAsync(WindowEnd.AddHours(-4));
        var day = await CollectDeadlocksAsync(WindowEnd.AddHours(-24));

        Assert.Equal(20.0, fourHours.Value, precision: 6);
        Assert.Equal(20.0, day.Value, precision: 6);
        Assert.Equal(FactScorer.ApplyThresholdFormula(20.0, 5, 20), FactScorer.ApplyThresholdFormula(day.Value, 5, 20), precision: 6);

        /* The whole-window average is still published as context, and it is the figure that depends on the window. */
        Assert.Equal(20.0, fourHours.Metadata["deadlock_count"], precision: 6);
        Assert.Equal(5.0, fourHours.Metadata["deadlocks_per_hour"], precision: 6);
        Assert.Equal(20.0 / 24.0, day.Metadata["deadlocks_per_hour"], precision: 6);

        Assert.True(day.Metadata.TryGetValue("deadlocks_per_hour_peak_1h", out var peakRate), "the fact carries the peak hour's rate");
        Assert.Equal(20.0, peakRate, precision: 6);
        Assert.True(day.Metadata.TryGetValue("peak_1h_deadlock_count", out var peakCount), "the fact carries the peak hour's count");
        Assert.Equal(20.0, peakCount, precision: 6);
    }

    [Fact]
    public async Task SteadyDeadlocks_PeakEqualsTheAverage()
    {
        var start = WindowEnd.AddHours(-4);
        await PlantCoverageAsync(start, WindowEnd);
        /* Two deadlocks in every hour of the window: the busiest hour is any hour, and the peak is the average. */
        for (var hour = 0; hour < 4; hour++)
        {
            await PlantDeadlockAsync(start.AddHours(hour).AddMinutes(10));
            await PlantDeadlockAsync(start.AddHours(hour).AddMinutes(40));
        }

        var deadlocks = await CollectDeadlocksAsync(start);

        Assert.Equal(2.0, deadlocks.Value, precision: 6);
        Assert.Equal(2.0, deadlocks.Metadata["deadlocks_per_hour"], precision: 6);
        Assert.True(deadlocks.Metadata.TryGetValue("peak_1h_deadlock_count", out var peakCount), "the fact carries the peak hour's count");
        Assert.Equal(2.0, peakCount, precision: 6);
    }

    /// <summary>The Azure SQL Database master path, which skips deadlocks wholly inside the separately monitored
    /// databases, grades the same peak: twenty deadlocks of another database inside one hour of a 24 hour read.</summary>
    [Fact]
    public async Task TheSeparatelyMonitoredScope_GradesThePeakHourToo()
    {
        await PlantCoverageAsync(WindowEnd.AddHours(-24), WindowEnd);
        await PlantStormAsync();

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = WindowEnd.AddHours(-24),
            TimeRangeEnd = WindowEnd,
            SeparatelyMonitoredDatabases = ["SeparateDb"],
        });
        var deadlocks = Assert.Single(facts, f => f.Key == "DEADLOCKS");

        Assert.Equal(20.0, deadlocks.Metadata["deadlock_count"], precision: 6);
        Assert.Equal(20.0, deadlocks.Value, precision: 6);
    }

    private async Task<Fact> CollectDeadlocksAsync(DateTime start)
    {
        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = start,
            TimeRangeEnd = WindowEnd,
        });
        return Assert.Single(facts, f => f.Key == "DEADLOCKS");
    }

    /// <summary>A wait-stats reading every fifteen minutes from <paramref name="from"/> to <paramref name="to"/> inclusive, the
    /// first with no delta (a baseline), so the coverage witness sees the collector up for exactly that span.</summary>
    private async Task PlantCoverageAsync(DateTime from, DateTime to)
    {
        for (var t = from; t <= to; t = t.AddMinutes(15))
            await PlantWaitAsync(t, t == from ? 0L : 1_000L);
    }

    private async Task PlantWaitAsync(DateTime at, long deltaWaitMs)
    {
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, 'CXPACKET', 10, 10000, 0, $5, $6, 0)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaWaitMs > 0 ? 10L : 0L });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaWaitMs });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One deadlock of the application database, which is never a separately monitored one here.</summary>
    private async Task PlantDeadlockAsync(DateTime at)
    {
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time, database_name)
VALUES ($1, $2, $3, $4, $2, 'AppDb')";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            using var readLock = _duckDb.AcquireReadLock();
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }
}
