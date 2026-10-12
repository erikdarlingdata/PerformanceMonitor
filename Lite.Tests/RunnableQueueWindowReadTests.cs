/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5641: the Runnable Queue alert's DuckDB sample read, <see cref="LocalDataService.GetRunnableQueueSamplesAsync"/>
/// over <c>v_cpu_scheduler_stats</c>, run against a real DuckDB file: one sample per collection_time, the collector's
/// split rows (#5710) folded by SUM(total) / MAX(scheduler_count), a lone row kept at its own average. The twin of
/// Darling's <c>DarlingRunnableQueueReadLiveTests</c>: the same cases, so the two SKUs cannot drift.
///
/// <para>The read binds its floor as <c>DateTime.UtcNow.AddMinutes(-n)</c> (Kind = Utc), exactly as the poison-wait read
/// does, so a DuckDB that shifted that bound by the machine's local time zone would move the window by the offset. Rows
/// are therefore seeded with UTC times relative to the real clock, and every in-lookback row sits at least a minute
/// inside the 10-minute lookback and every out-of-lookback row at least two minutes outside it: a window shifted by
/// hours fails, and a slow test host cannot move a row across the bar.</para>
/// </summary>
public sealed class RunnableQueueWindowReadTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 5641;
    private const int OtherServerId = 5642;
    private const int LookbackMinutes = RunnableQueueEvaluator.FallbackMinutes; // the read takes any lookback; these cases pin its boundary at 10

    private readonly DuckDbInitializer _duckDb;

    public RunnableQueueWindowReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public void RunnableQueueSamplesSql_IsOneRowPerCollectionTimeWithTheSplitRowFold()
    {
        var sql = LocalDataService.RunnableQueueSamplesSql;

        Assert.Contains("FROM v_cpu_scheduler_stats", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("avg_runnable_tasks_count IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_id", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT", sql, StringComparison.Ordinal);
        Assert.Contains("WHEN COUNT(*) = 1", sql, StringComparison.Ordinal);
        Assert.Contains("THEN MAX(avg_runnable_tasks_count)", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(total_runnable_tasks_count) AS DOUBLE) / NULLIF(MAX(scheduler_count), 0)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("node", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("offline", sql, StringComparison.OrdinalIgnoreCase);
        /* The floor is bound from the caller's clock; no engine-side now() to disagree with the evaluator. */
        Assert.DoesNotContain("now()", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("current_timestamp", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task EmptyWindow_ReturnsNoSamples()
    {
        var samples = await new LocalDataService(_duckDb).GetRunnableQueueSamplesAsync(ServerId, LookbackMinutes);

        Assert.Empty(samples);
    }

    [Fact]
    public async Task RowsOnlyBeforeTheFloor_ReturnNoSamples()
    {
        var now = DateTime.UtcNow;
        await InsertAsync(1, now.AddMinutes(-LookbackMinutes - 2), 40.0m, 4, 160);
        await InsertAsync(2, now.AddMinutes(-LookbackMinutes - 9), 50.0m, 4, 200);

        var samples = await new LocalDataService(_duckDb).GetRunnableQueueSamplesAsync(ServerId, LookbackMinutes);

        Assert.Empty(samples);
    }

    /// <summary>
    /// The local-time-zone case. Under a 10-minute lookback a row 3 minutes old comes back and a row 13 minutes old does
    /// not. If DuckDB shifted the Kind = Utc floor by this machine's local offset (this laptop is not on UTC), then on any
    /// machine west of UTC the floor would land hours in the future and the 3-minute row would vanish, and east of UTC it
    /// would land hours in the past and the 13-minute row would join.
    /// </summary>
    [Fact]
    public async Task AThreeMinuteOldRowCounts_AndAThirteenMinuteOldRowDoesNot_WhateverTheMachinesTimeZone()
    {
        var now = DateTime.UtcNow;
        await InsertAsync(1, now.AddMinutes(-3), 6.0m, 4, 24);
        await InsertAsync(2, now.AddMinutes(-13), 600.0m, 4, 2400);

        var samples = await new LocalDataService(_duckDb).GetRunnableQueueSamplesAsync(ServerId, LookbackMinutes);

        var sample = Assert.Single(samples);
        Assert.Equal(6.0, sample.PerScheduler, 6);
        Assert.Equal(24L, sample.TotalRunnableTasks);
        /* The sample's own time comes back as the UTC wall-clock it was stored with. */
        Assert.True(Math.Abs((sample.CollectionTime - now.AddMinutes(-3)).TotalSeconds) < 1,
            $"collection time {sample.CollectionTime:O} should be the stored UTC time {now.AddMinutes(-3):O}");
    }

    [Fact]
    public async Task AMixInsideAndOutsideTheLookback_ReturnsOnlyTheInsideCollections_NewestFirst()
    {
        var now = DateTime.UtcNow;
        var newest = now.AddMinutes(-1);
        await InsertAsync(1, newest.AddMinutes(-2), 9.0m, 4, 36);
        await InsertAsync(2, newest.AddMinutes(-1), 4.0m, 4, 16);
        await InsertAsync(3, newest, 2.0m, 8, 16);
        await InsertAsync(4, now.AddMinutes(-LookbackMinutes - 2), 500.0m, 4, 2000);
        await InsertAsync(5, now.AddMinutes(-LookbackMinutes - 20), 700.0m, 4, 2800);

        var samples = await new LocalDataService(_duckDb).GetRunnableQueueSamplesAsync(ServerId, LookbackMinutes);

        Assert.Equal(3, samples.Count);
        Assert.Equal(new[] { 2.0, 4.0, 9.0 }, samples.Select(s => s.PerScheduler).ToArray());
        Assert.Equal(newest.Ticks / 10, samples[0].CollectionTime.Ticks / 10);
        Assert.Equal(8, samples[0].SchedulerCount);
        Assert.Equal(16L, samples[0].TotalRunnableTasks);
    }

    [Fact]
    public async Task ARowOlderThanFiveMinutes_ButInsideTenMinutes_IsReturned_ForTheFallback()
    {
        var now = DateTime.UtcNow;
        await InsertAsync(1, now.AddMinutes(-1), 20.0m, 4, 80);
        await InsertAsync(2, now.AddMinutes(-4.5), 20.0m, 4, 80);
        await InsertAsync(3, now.AddMinutes(-8), 20.0m, 4, 80);

        var samples = await new LocalDataService(_duckDb).GetRunnableQueueSamplesAsync(ServerId, LookbackMinutes);

        Assert.Equal(3, samples.Count);
    }

    [Fact]
    public async Task NullAverageRows_AreNotSamples_AndNeverCarryTheNewestCollection()
    {
        var now = DateTime.UtcNow;
        await InsertAsync(1, now.AddMinutes(-3), 6.0m, 4, 24);
        await InsertAsync(2, now.AddMinutes(-2), 12.0m, 4, 48);
        /* The NEWEST row has no average: not a sample, so it does not come back and is not the one that rides along. */
        await InsertAsync(3, now.AddMinutes(-1), null, 16, 999);

        var samples = await new LocalDataService(_duckDb).GetRunnableQueueSamplesAsync(ServerId, LookbackMinutes);

        Assert.Equal(2, samples.Count);
        Assert.Equal(new[] { 12.0, 6.0 }, samples.Select(s => s.PerScheduler).ToArray());
        Assert.Equal(4, samples[0].SchedulerCount);
        Assert.Equal(48L, samples[0].TotalRunnableTasks);
        Assert.Equal(now.AddMinutes(-2).Ticks / 10, samples[0].CollectionTime.Ticks / 10);
    }

    [Fact]
    public async Task RowsAtOneCollectionTime_AreOneSample_SummedOverTheHighestSchedulerCount()
    {
        var now = DateTime.UtcNow;
        var split = now.AddMinutes(-1);
        /* The 15:42:43 collection of the 2026-10-09 incident: two rows, each covering a subset of the four schedulers
           (27.67|83 is three of them, 18.00|18 is one), every row saying scheduler_count 4. */
        await InsertAsync(20, split, 18.0m, 4, 18);
        await InsertAsync(10, split, 27.67m, 4, 83);
        await InsertAsync(15, now.AddMinutes(-2), 3.0m, 3, 9);

        var samples = await new LocalDataService(_duckDb).GetRunnableQueueSamplesAsync(ServerId, LookbackMinutes);

        Assert.Equal(2, samples.Count);
        Assert.Equal(split.Ticks / 10, samples[0].CollectionTime.Ticks / 10);
        Assert.Equal(25.25, samples[0].PerScheduler, 9);
        Assert.Equal(101L, samples[0].TotalRunnableTasks);
        Assert.Equal(4, samples[0].SchedulerCount);
        Assert.Equal(3.0, samples[1].PerScheduler, 9);
    }

    [Fact]
    public async Task ASplitCollection_UsesTheTotalsNotTheRowAverages()
    {
        var now = DateTime.UtcNow;
        /* 15:49:01: the mean of the row averages is 53.17; the collection is (60 + 101 + 49) / 4 = 52.5. */
        await InsertAsync(1, now.AddMinutes(-1), 60.0m, 4, 60);
        await InsertAsync(2, now.AddMinutes(-1), 50.5m, 4, 101);
        await InsertAsync(3, now.AddMinutes(-1), 49.0m, 4, 49);

        var samples = await new LocalDataService(_duckDb).GetRunnableQueueSamplesAsync(ServerId, LookbackMinutes);

        var sample = Assert.Single(samples);
        Assert.Equal(52.5, sample.PerScheduler, 9);
        Assert.Equal(210L, sample.TotalRunnableTasks);
    }

    [Fact]
    public async Task ASingleRow_UsesItsOwnAverageExactly_NotTotalOverSchedulerCount()
    {
        var now = DateTime.UtcNow;
        /* The collector's own AVG is 3.33 here; total / scheduler_count would say 2.5. */
        await InsertAsync(1, now.AddMinutes(-1), 3.33m, 4, 10);

        var samples = await new LocalDataService(_duckDb).GetRunnableQueueSamplesAsync(ServerId, LookbackMinutes);

        var sample = Assert.Single(samples);
        Assert.Equal(3.33, sample.PerScheduler, 9);
        Assert.Equal(10L, sample.TotalRunnableTasks);
    }

    [Fact]
    public async Task SplitRowsThatAreAllZero_AreAZeroSample()
    {
        var now = DateTime.UtcNow;
        /* 15:38:28 in the incident: four rows, all zero but one lone runnable task. And a collection of all-zero rows. */
        await InsertAsync(1, now.AddMinutes(-2), 0.0m, 4, 0);
        await InsertAsync(2, now.AddMinutes(-2), 1.0m, 4, 1);
        await InsertAsync(3, now.AddMinutes(-2), 0.0m, 4, 0);
        await InsertAsync(4, now.AddMinutes(-1), 0.0m, 4, 0);
        await InsertAsync(5, now.AddMinutes(-1), 0.0m, 4, 0);

        var samples = await new LocalDataService(_duckDb).GetRunnableQueueSamplesAsync(ServerId, LookbackMinutes);

        Assert.Equal(2, samples.Count);
        Assert.Equal(0.0, samples[0].PerScheduler, 9);
        Assert.Equal(0L, samples[0].TotalRunnableTasks);
        Assert.Equal(0.25, samples[1].PerScheduler, 9);
    }

    [Fact]
    public async Task ANullAverageRowInASplitCollection_IsLeftOutOfTheFold()
    {
        var now = DateTime.UtcNow;
        var instant = now.AddMinutes(-1);
        await InsertAsync(1, instant, 8.0m, 4, 32);
        await InsertAsync(2, instant, null, 4, 9999);
        await InsertAsync(3, instant, 4.0m, 4, 16);

        var samples = await new LocalDataService(_duckDb).GetRunnableQueueSamplesAsync(ServerId, LookbackMinutes);

        var sample = Assert.Single(samples);
        Assert.Equal(12.0, sample.PerScheduler, 9);
        Assert.Equal(48L, sample.TotalRunnableTasks);
    }

    [Fact]
    public async Task ASplitCollectionWithoutASchedulerCount_IsNotASample()
    {
        var now = DateTime.UtcNow;
        await InsertAsync(1, now.AddMinutes(-2), 5.0m, 4, 20);
        /* Two rows, no scheduler_count: nothing to divide by, so the collection has no per-scheduler value. */
        await InsertAsync(2, now.AddMinutes(-1), 5.0m, null, 20);
        await InsertAsync(3, now.AddMinutes(-1), 5.0m, null, 20);

        var samples = await new LocalDataService(_duckDb).GetRunnableQueueSamplesAsync(ServerId, LookbackMinutes);

        var sample = Assert.Single(samples);
        Assert.Equal(now.AddMinutes(-2).Ticks / 10, sample.CollectionTime.Ticks / 10);
    }

    [Fact]
    public async Task AnotherServersRows_AreIgnored()
    {
        var now = DateTime.UtcNow;
        await InsertAsync(1, now.AddMinutes(-2), 4.0m, 4, 16);
        await InsertAsync(2, now.AddMinutes(-1), 400.0m, 32, 12800, OtherServerId);

        var service = new LocalDataService(_duckDb);
        var samples = await service.GetRunnableQueueSamplesAsync(ServerId, LookbackMinutes);

        var sample = Assert.Single(samples);
        Assert.Equal(4.0, sample.PerScheduler, 9);
        Assert.Equal(4, sample.SchedulerCount);

        var other = await service.GetRunnableQueueSamplesAsync(OtherServerId, LookbackMinutes);
        Assert.Equal(400.0, Assert.Single(other).PerScheduler, 9);
    }

    [Fact]
    public async Task SubMillisecondTimesAtTheFloor_KeepMicrosecondPrecision()
    {
        /* The SQL text run directly with a floor the test chooses (the only way to put a row exactly on the floor):
           DuckDB stores microseconds, and a floor or a row truncated to milliseconds would move a row across the bar. */
        var floor = new DateTime(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc).AddTicks(1234560); // .123456 s
        var oneMicrosecond = TimeSpan.FromTicks(10);
        await InsertAsync(1, floor - oneMicrosecond, 100.0m, 4, 400);
        await InsertAsync(2, floor, 4.0m, 4, 16);
        await InsertAsync(3, floor + oneMicrosecond, 8.0m, 4, 32);

        var samples = await ReadWithFloorAsync(floor);

        Assert.Equal(2, samples.Count);
        Assert.Equal(new[] { 8.0, 4.0 }, samples.Select(s => s.PerScheduler).ToArray());
        Assert.Equal((floor + oneMicrosecond).Ticks, samples[0].CollectionTime.Ticks);

        var later = await ReadWithFloorAsync(floor + oneMicrosecond);
        Assert.Equal(8.0, Assert.Single(later).PerScheduler, 9);
    }

    /* ---------------- the 2026-10-09 incident, through the DuckDB read ---------------- */

    /// <summary>
    /// The incident rows seeded split exactly as stored (several rows to a collection_time, every row saying
    /// scheduler_count 4), read through <see cref="LocalDataService.RunnableQueueSamplesSql"/> with the floor each
    /// evaluation time implies (t minus the 10-minute lookback; rows after t do not exist yet, so they are dropped),
    /// and graded by the evaluator. The same expectations as the pure replay in Darling.Tests'
    /// <c>RunnableQueueIncidentReplayTests</c>.
    /// </summary>
    [Fact]
    public async Task TheIncidentReplay_ThroughTheSplitRowRead_GradesAsTheWindowRuleExpects()
    {
        await SeedIncidentAsync();

        async Task<(RunnableQueueEvaluator.Finding? Finding, RunnableQueueReading? Reading)> JudgeAsync(string time)
        {
            var t = IncidentTime(time);
            var written = (await ReadWithFloorAsync(t.AddMinutes(-LookbackMinutes))).Where(s => s.CollectionTime <= t).ToList();
            var reading = RunnableQueueEvaluator.Read(written, t);
            return (RunnableQueueEvaluator.Evaluate(reading, 8, 16), reading);
        }

        /* Under the Warning bar until the oldest near-zero collection falls out of the 5 minutes. */
        var (f0, r0) = await JudgeAsync("15:42:44.000");
        Assert.Null(f0);
        Assert.Equal(7.85, r0!.AveragePerScheduler, 2);

        var (warning, rw) = await JudgeAsync("15:43:29.000");
        Assert.Equal(AlertSeverityLevel.Warning, warning!.Severity);
        Assert.Equal(9.75, rw!.AveragePerScheduler, 2);

        var (critical, rc) = await JudgeAsync("15:45:00.871");
        Assert.Equal(AlertSeverityLevel.Critical, critical!.Severity);
        Assert.Equal(17.94, rc!.AveragePerScheduler, 2);

        /* Held through the sparse stretch by the fallback. */
        foreach (var (time, average) in new[] { ("15:46:30.000", 23.58), ("15:49:02.000", 36.83), ("15:54:01.000", 31.33), ("15:55:02.000", 21.92) })
        {
            var (finding, reading) = await JudgeAsync(time);
            Assert.Equal(AlertSeverityLevel.Critical, finding!.Severity);
            Assert.Equal(average, reading!.AveragePerScheduler, 2);
            Assert.Equal(3, reading.SampleCount);
        }

        /* No evaluation at 15:53:00: the third newest collection is older than 10 minutes. */
        var (blindFinding, blindReading) = await JudgeAsync("15:53:00.000");
        Assert.Null(blindReading);
        Assert.Null(blindFinding);

        var (cleared, rq) = await JudgeAsync("15:55:29.287");
        Assert.Null(cleared);
        Assert.Equal(4.5, rq!.AveragePerScheduler, 2);
    }

    private static readonly (string Time, (decimal Avg, int Total)[] Rows)[] IncidentRows =
    {
        ("15:30:28.573", new[] { (0.00m, 0) }), ("15:31:30.741", new[] { (0.00m, 0) }), ("15:32:31.872", new[] { (2.25m, 9) }),
        ("15:33:28.181", new[] { (0.00m, 0) }), ("15:34:30.404", new[] { (0.00m, 0) }), ("15:35:28.451", new[] { (0.75m, 3) }),
        ("15:36:29.260", new[] { (0.00m, 0) }), ("15:37:29.873", new[] { (0.00m, 0) }),
        ("15:38:28.757", new[] { (0.00m, 0), (1.00m, 1), (0.00m, 0), (0.00m, 0) }),
        ("15:39:30.198", new[] { (0.00m, 0) }), ("15:40:29.006", new[] { (1.00m, 4) }), ("15:41:29.926", new[] { (12.75m, 51) }),
        ("15:42:43.814", new[] { (27.67m, 83), (18.00m, 18) }),
        ("15:45:00.871", new[] { (35.00m, 35), (32.00m, 96) }),
        ("15:49:01.703", new[] { (60.00m, 60), (50.50m, 101), (49.00m, 49) }),
        ("15:54:00.022", new[] { (10.00m, 10), (7.50m, 15), (10.00m, 10) }),
        ("15:55:01.517", new[] { (3.33m, 10), (8.00m, 8) }),
        ("15:55:29.287", new[] { (1.00m, 1), (0.00m, 0), (0.00m, 0) }),
        ("15:56:29.695", new[] { (0.50m, 2) }), ("15:57:31.491", new[] { (0.00m, 0) }),
        ("15:58:29.323", new[] { (0.25m, 1) }), ("15:59:30.398", new[] { (0.00m, 0) }),
    };

    private static DateTime IncidentTime(string timeOfDay) =>
        DateTime.SpecifyKind(
            DateTime.ParseExact("2026-10-09 " + timeOfDay, "yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
            DateTimeKind.Utc);

    private async Task SeedIncidentAsync()
    {
        long id = 1000;
        foreach (var (time, rows) in IncidentRows)
        {
            foreach (var (avg, total) in rows)
            {
                await InsertAsync(id++, IncidentTime(time), avg, 4, total);
            }
        }
    }

    private async Task InsertAsync(
        long collectionId, DateTime collectionTime, decimal? average, int? schedulerCount, int totalRunnable,
        int serverId = ServerId)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO cpu_scheduler_stats
    (collection_id, collection_time, server_id, server_name, scheduler_count, total_runnable_tasks_count, avg_runnable_tasks_count)
VALUES ($1, $2, $3, 'test-server', $4, $5, $6)";
        command.Parameters.Add(new DuckDBParameter { Value = collectionId });
        command.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)schedulerCount ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = totalRunnable });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)average ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>Runs the read's SQL text with a floor the test chooses, mapped exactly as the read maps it.</summary>
    private async Task<List<RunnableQueueSample>> ReadWithFloorAsync(DateTime floor)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = LocalDataService.RunnableQueueSamplesSql;
        command.Parameters.Add(new DuckDBParameter { Value = ServerId });
        command.Parameters.Add(new DuckDBParameter { Value = floor });

        var samples = new List<RunnableQueueSample>();
        using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            if (reader.IsDBNull(1))
            {
                continue;
            }

            samples.Add(new RunnableQueueSample(
                reader.GetDateTime(0),
                reader.GetDouble(1),
                reader.IsDBNull(2) ? null : reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetInt32(3)));
        }

        return samples;
    }
}
