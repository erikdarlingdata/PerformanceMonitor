/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Wait rows stored before the collectors trimmed wait names (<c>WaitTypeName.Trim</c>) keep the DMV's trailing
/// space, for example <c>SQP_STATS_REPORTING </c>, until retention removes them. Every Lite read that filters,
/// groups or looks up by wait name reads the two spellings as one name: the ignore list hides the spaced one, a
/// name stored both ways comes back as one row under the clean name, and a lookup by the clean name finds the
/// spaced history. These run against a real DuckDB through the real reads, with the bundled default ignore list.
/// </summary>
public sealed class WaitNameSpacedHistoryReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -4884;
    private const string ServerName = "spaced-wait-history";

    /* In the bundled default ignore list, and stored with its trailing space before the trim. */
    private const string IgnoredClean = "SQP_STATS_REPORTING";
    private const string IgnoredSpaced = "SQP_STATS_REPORTING ";

    /* Not ignored: one of the four names the DMV reports with a trailing space. */
    private const string KeptClean = "EDC_DOPP_LOCK";
    private const string KeptSpaced = "EDC_DOPP_LOCK ";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public WaitNameSpacedHistoryReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>The pins below lean on the bundled default list naming the clean spelling.</summary>
    [Fact]
    public void TheDefaultIgnoreList_NamesTheCleanSpelling()
    {
        Assert.Contains(IgnoredClean, IgnoredWaitTypes.Load());
    }

    [Fact]
    public async Task WaitStats_TheDefaultIgnoreList_HidesTheSpacedSpelling()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-1));
        await SeedWaitAsync(t1, IgnoredSpaced, deltaMs: 3_600_069, deltaSignal: 0, deltaTasks: 37, interval: 60);
        await SeedWaitAsync(t1, "CXPACKET", deltaMs: 1_000, deltaSignal: 10, deltaTasks: 5, interval: 60);

        var rows = await _dataService.GetWaitStatsAsync(ServerId, hoursBack: 3);

        Assert.DoesNotContain(rows, r => r.WaitType.TrimEnd() == IgnoredClean);
        Assert.Contains(rows, r => r.WaitType == "CXPACKET");
    }

    [Fact]
    public async Task Picker_TheDefaultIgnoreList_HidesTheSpacedSpelling()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-1));
        await SeedWaitAsync(t1, IgnoredSpaced, deltaMs: 3_600_069, deltaSignal: 0, deltaTasks: 37, interval: 60);
        await SeedWaitAsync(t1, "CXPACKET", deltaMs: 1_000, deltaSignal: 10, deltaTasks: 5, interval: 60);

        var names = await _dataService.GetDistinctWaitTypesForPickerAsync(ServerId, hoursBack: 3);

        Assert.DoesNotContain(names, n => n.TrimEnd() == IgnoredClean);
        Assert.Contains("CXPACKET", names);
    }

    [Fact]
    public async Task TotalWaitTrend_TheDefaultIgnoreList_LeavesTheSpacedSpellingOut()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-1));
        await SeedWaitAsync(t1, IgnoredSpaced, deltaMs: 3_600_000, deltaSignal: 0, deltaTasks: 37, interval: 60);
        await SeedWaitAsync(t1, "CXPACKET", deltaMs: 600, deltaSignal: 10, deltaTasks: 5, interval: 60);

        var points = await _dataService.GetTotalWaitTrendAsync(ServerId, hoursBack: 3);

        /* CXPACKET alone: 600 ms over 60 s. */
        Assert.Equal(10.0, Assert.Single(points).WaitTimeMsPerSecond, precision: 6);
    }

    [Fact]
    public async Task WaitStats_ANameStoredBothWays_IsOneRowWithTheSummedValues()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-2));
        var t2 = t1.AddMinutes(5);
        await SeedWaitAsync(t1, KeptSpaced, deltaMs: 1_000, deltaSignal: 100, deltaTasks: 10, interval: 300);
        await SeedWaitAsync(t2, KeptClean, deltaMs: 500, deltaSignal: 50, deltaTasks: 5, interval: 300);

        var rows = await _dataService.GetWaitStatsAsync(ServerId, hoursBack: 3);

        var row = Assert.Single(rows, r => r.WaitType.TrimEnd() == KeptClean);
        Assert.Equal(KeptClean, row.WaitType);
        Assert.Equal(1_500, row.TotalWaitTimeMs);
        Assert.Equal(150, row.TotalSignalWaitTimeMs);
        Assert.Equal(15, row.TotalWaitingTasks);
        Assert.Equal(2, row.SampleCount);
    }

    [Fact]
    public async Task Picker_ANameStoredBothWays_IsOneCleanName()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-2));
        await SeedWaitAsync(t1, KeptSpaced, deltaMs: 1_000, deltaSignal: 100, deltaTasks: 10, interval: 300);
        await SeedWaitAsync(t1.AddMinutes(5), KeptClean, deltaMs: 500, deltaSignal: 50, deltaTasks: 5, interval: 300);

        var names = await _dataService.GetDistinctWaitTypesForPickerAsync(ServerId, hoursBack: 3);

        Assert.Equal(new[] { KeptClean }, names.Where(n => n.TrimEnd() == KeptClean).ToArray());
    }

    [Fact]
    public async Task WaitTrend_ByTheCleanName_IncludesTheSpacedHistory()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-2));
        var t2 = t1.AddMinutes(5);
        await SeedWaitAsync(t1, KeptSpaced, deltaMs: 600, deltaSignal: 60, deltaTasks: 3, interval: 300);
        await SeedWaitAsync(t2, KeptClean, deltaMs: 900, deltaSignal: 90, deltaTasks: 3, interval: 300);

        var points = await _dataService.GetWaitStatsTrendAsync(ServerId, KeptClean, hoursBack: 3);

        Assert.Equal(new[] { t1, t2 }, points.Select(p => p.CollectionTime).ToArray());
        Assert.Equal(2.0, points[0].WaitTimeMsPerSecond, precision: 6);
        Assert.Equal(3.0, points[1].WaitTimeMsPerSecond, precision: 6);
    }

    [Fact]
    public async Task WaitTrends_ByTheCleanName_AreOneSeriesThatIncludesTheSpacedHistory()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-2));
        var t2 = t1.AddMinutes(5);
        await SeedWaitAsync(t1, KeptSpaced, deltaMs: 600, deltaSignal: 60, deltaTasks: 3, interval: 300);
        await SeedWaitAsync(t2, KeptClean, deltaMs: 900, deltaSignal: 90, deltaTasks: 3, interval: 300);

        var series = await _dataService.GetWaitStatsTrendsByTypesAsync(ServerId, new List<string> { KeptClean }, hoursBack: 3);

        var (name, points) = Assert.Single(series);
        Assert.Equal(KeptClean, name);
        Assert.Equal(new[] { t1, t2 }, points.Select(p => p.CollectionTime).ToArray());
    }

    [Fact]
    public async Task WaitingTasks_TheDefaultIgnoreList_HidesTheSpacedSpelling()
    {
        var t1 = Truncate(DateTime.UtcNow.AddMinutes(-20));
        await SeedWaitingTaskAsync(t1, IgnoredSpaced, waitDurationMs: 5_000);
        await SeedWaitingTaskAsync(t1, "LCK_M_X", waitDurationMs: 100);

        var rows = await _dataService.GetWaitingTasksAsync(ServerId, hoursBack: 1);

        Assert.DoesNotContain(rows, r => r.WaitType.TrimEnd() == IgnoredClean);
        Assert.Contains(rows, r => r.WaitType == "LCK_M_X");
    }

    [Fact]
    public async Task WaitingTaskTrend_ANameStoredBothWays_IsOneSeriesUnderTheCleanName()
    {
        var t1 = Truncate(DateTime.UtcNow.AddMinutes(-40));
        var t2 = t1.AddMinutes(5);
        await SeedWaitingTaskAsync(t1, KeptSpaced, waitDurationMs: 200);
        await SeedWaitingTaskAsync(t2, KeptClean, waitDurationMs: 300);

        var points = await _dataService.GetWaitingTaskTrendAsync(ServerId, hoursBack: 1);

        var mine = points.Where(p => p.WaitType.TrimEnd() == KeptClean).ToList();
        Assert.Equal(new[] { KeptClean, KeptClean }, mine.Select(p => p.WaitType).ToArray());
        Assert.Equal(500, mine.Sum(p => p.TotalWaitMs));
    }

    [Fact]
    public async Task QueryDrillDown_ByTheCleanName_FindsASnapshotStoredWithTheSpace()
    {
        var t1 = Truncate(DateTime.UtcNow.AddMinutes(-30));
        await SeedQuerySnapshotAsync(t1, KeptSpaced, waitTimeMs: 1_234);

        var rows = await _dataService.GetQuerySnapshotsByWaitTypeAsync(ServerId, KeptClean, hoursBack: 1);

        Assert.Equal(1_234, Assert.Single(rows).WaitTimeMs);
    }

    /// <summary>The bucketed read behind get_wait_trend.</summary>
    [Fact]
    public async Task WaitBuckets_ByTheCleanName_IncludeTheSpacedHistory()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-2));
        await SeedWaitAsync(t1, KeptSpaced, deltaMs: 600, deltaSignal: 60, deltaTasks: 3, interval: 300);
        await SeedWaitAsync(t1.AddMinutes(5), KeptClean, deltaMs: 900, deltaSignal: 90, deltaTasks: 3, interval: 300);

        var points = await _dataService.GetWaitBucketsAsync(ServerId, KeptClean, hoursBack: 3, asOfUtc: DateTime.UtcNow, bucketMinutes: 1);

        Assert.Equal(2, points.Count);
    }

    /// <summary>A day's top wait is the name with the most wait time, its two spellings summed: 600 + 500
    /// beats CXPACKET's 800, while either spelling alone loses to it.</summary>
    [Fact]
    public async Task DailySummary_TopWait_SumsBothSpellingsUnderTheCleanName()
    {
        var day = DateTime.UtcNow.Date.AddDays(-1);
        await SeedWaitAsync(day.AddHours(10), KeptSpaced, deltaMs: 600, deltaSignal: 0, deltaTasks: 3, interval: 300);
        await SeedWaitAsync(day.AddHours(10).AddMinutes(5), KeptClean, deltaMs: 500, deltaSignal: 0, deltaTasks: 3, interval: 300);
        await SeedWaitAsync(day.AddHours(10).AddMinutes(5), "CXPACKET", deltaMs: 800, deltaSignal: 0, deltaTasks: 3, interval: 300);

        var summary = await _dataService.GetDailySummaryAsync(ServerId, day);

        Assert.NotNull(summary);
        Assert.Equal(KeptClean, summary.TopWaitType);
    }

    /// <summary>The FinOps wait categories: both spellings are one wait in "Other", and they outweigh a
    /// name that beats either one alone.</summary>
    [Fact]
    public async Task WaitCategorySummary_TopWait_SumsBothSpellingsUnderTheCleanName()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-2));
        await SeedWaitAsync(t1, KeptSpaced, deltaMs: 600, deltaSignal: 0, deltaTasks: 3, interval: 300);
        await SeedWaitAsync(t1.AddMinutes(5), KeptClean, deltaMs: 500, deltaSignal: 0, deltaTasks: 3, interval: 300);
        await SeedWaitAsync(t1.AddMinutes(5), "BROKER_TASK_STOP", deltaMs: 800, deltaSignal: 0, deltaTasks: 3, interval: 300);

        var categories = await _dataService.GetWaitCategorySummaryAsync(ServerId, hoursBack: 24);

        var other = Assert.Single(categories, c => c.Category == "Other");
        Assert.Equal(KeptClean, other.TopWaitType);
        Assert.Equal(1_100, other.TopWaitTimeMs);
    }

    /// <summary>The analysis pass's wait facts: one fact under the clean name, with both spellings' time.</summary>
    [Fact]
    public async Task AnalysisWaitFacts_ANameStoredBothWays_IsOneFactUnderTheCleanName()
    {
        var now = Truncate(DateTime.UtcNow);
        for (var minutesAgo = 225; minutesAgo >= 0; minutesAgo -= 15)
        {
            await SeedWaitAsync(now.AddMinutes(-minutesAgo), minutesAgo >= 120 ? KeptSpaced : KeptClean,
                deltaMs: 100, deltaSignal: 0, deltaTasks: 1, interval: 900);
        }

        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = now.AddHours(-4),
            TimeRangeEnd = now,
        };
        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context);

        var fact = Assert.Single(facts, f => f.Key.TrimEnd() == KeptClean);
        Assert.Equal(KeptClean, fact.Key);
        Assert.Equal(1_600, fact.Metadata["wait_time_ms"]);
    }

    private static DateTime Truncate(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private async Task SeedAsync(string sql, params object[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private Task SeedWaitAsync(DateTime at, string waitType, long deltaMs, long deltaSignal, long deltaTasks, int interval) =>
        SeedAsync(@"INSERT INTO wait_stats
            (collection_id, collection_time, server_id, server_name, wait_type,
             waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
             delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, $5, 0, 0, 0, $6, $7, $8, $9)",
            _nextId--, at, ServerId, ServerName, waitType, deltaTasks, deltaMs, deltaSignal, interval);

    private Task SeedWaitingTaskAsync(DateTime at, string waitType, long waitDurationMs) =>
        SeedAsync(@"INSERT INTO waiting_tasks
            (collection_id, collection_time, server_id, server_name,
             session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, '', 'AppDb')",
            _nextId--, at, ServerId, ServerName, 55, waitType, waitDurationMs);

    private Task SeedQuerySnapshotAsync(DateTime at, string waitType, long waitTimeMs) =>
        SeedAsync(@"INSERT INTO query_snapshots
            (collection_id, collection_time, server_id, server_name, session_id, database_name, status, wait_type, wait_time_ms)
            VALUES ($1, $2, $3, $4, $5, 'AppDb', 'suspended', $6, $7)",
            _nextId--, at, ServerId, ServerName, 61, waitType, waitTimeMs);
}
