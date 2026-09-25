/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4234 review pins: the coordinator's two findings on PR #4304's original submission (item 3's
/// raw-timestamp-at-singleton-bucket rule for <c>ViewerDataService.WaitTrendsSql</c> /
/// <c>PerfmonTrendsSql</c>, and <see cref="ViewerNameListCache"/>'s window-end check), plus the #4234
/// ruling's four required tests. No live store needed.
/// </summary>
public sealed class ViewerTrendBucketWidthSqlTests
{
    [Fact]
    public void WaitTrendsSql_CarriesABucketWidth()
    {
        /* server/start/end = $1-$3, 2 wait types = $4-$5, so the width is $6 — the ruling's item 6
           source check. Proven once by hand against the pre-#4234 text rather than automated against git
           history: `git show origin/dev:.../ViewerDataService.Waits.cs` has no `date_bin` anywhere (a
           per-collection read has no bucket width), so this assert fails there. */
        var sql = ViewerDataService.WaitTrendsSql(2);
        Assert.Contains("date_bin(CAST($6 AS integer) * INTERVAL '1 minute'", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBucketSql.OriginSql, sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PerfmonTrendsSql_CarriesABucketWidth()
    {
        var sql = ViewerDataService.PerfmonTrendsSql(2);
        Assert.Contains("date_bin(CAST($6 AS integer) * INTERVAL '1 minute'", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBucketSql.OriginSql, sql, StringComparison.Ordinal);
    }

    [Fact]
    public void WaitTrendsSql_And_PerfmonTrendsSql_ProjectFirstCollectionTimeAndCollectionCount()
    {
        /* The singleton-detection columns the #4234 review's item-3 fix reads: MIN(collection_time) and
           COUNT(*) per bucket, mirroring DurationTrendRouting.BuildBucketedRawTrendSql's own
           first_collection_time column. */
        foreach (var sql in new[] { ViewerDataService.WaitTrendsSql(1), ViewerDataService.PerfmonTrendsSql(1) })
        {
            Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
            Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// #4234 review (defect 2): <see cref="ViewerNameListCache"/>'s hit/miss rules, exercised directly — no
/// live store needed since the cache never touches Postgres itself.
/// </summary>
public sealed class ViewerNameListCacheTests
{
    private static readonly TimeSpan Length = TimeSpan.FromHours(24);

    [Fact]
    public void TryGet_MissesInitially()
    {
        var cache = new ViewerNameListCache();
        var now = DateTime.UtcNow;
        Assert.False(cache.TryGet(1, Length, now, now, out _));
    }

    [Fact]
    public void TryGet_HitsWithinTtl_SameWindowEnd()
    {
        var cache = new ViewerNameListCache();
        var fetchedAt = new DateTime(2026, 3, 1, 12, 0, 0);
        var end = new DateTime(2026, 3, 1, 11, 0, 0);
        var names = new List<string> { "CXPACKET" };
        cache.Set(1, Length, end, names, fetchedAt);

        /* The ruling's own pin: a second refresh inside 15 minutes runs no DISTINCT — this IS that
           refresh, proven at the mechanism that prevents the query: TryGet returns the cached list. */
        Assert.True(cache.TryGet(1, Length, end, fetchedAt.AddMinutes(14), out var cached));
        Assert.Same(names, cached);
    }

    [Fact]
    public void TryGet_MissesAtFifteenMinutes_WallClockExpiry()
    {
        var cache = new ViewerNameListCache();
        var fetchedAt = new DateTime(2026, 3, 1, 12, 0, 0);
        var end = new DateTime(2026, 3, 1, 11, 0, 0);
        cache.Set(1, Length, end, new List<string> { "CXPACKET" }, fetchedAt);

        /* One after 15 minutes DOES run the DISTINCT again — the ruling's other half of the same pin. */
        Assert.False(cache.TryGet(1, Length, end, fetchedAt.AddMinutes(15), out _));
    }

    /// <summary>The bug as filed: keyed on (server, length) alone, a 7-day custom range from last month
    /// answered from this week's cached 7-day list, because both windows share one length-keyed slot. A
    /// fresh fetch timestamp is not enough — the cached window's END must also still be recent.</summary>
    [Fact]
    public void TryGet_Misses_WhenCachedWindowEndIsFarFromRequestedEnd_DespiteFreshFetch()
    {
        var cache = new ViewerNameListCache();
        var now = new DateTime(2026, 3, 1, 12, 0, 0);
        cache.Set(1, TimeSpan.FromDays(7), now, new List<string> { "CXPACKET" }, now);

        var lastMonthsEnd = now.AddDays(-30);
        Assert.False(cache.TryGet(1, TimeSpan.FromDays(7), lastMonthsEnd, now, out _));
    }

    [Fact]
    public void TryGet_Hits_WhenRequestedEndIsSlightlyBeforeCachedEnd_TwoSidedCheck()
    {
        /* A custom range's end can sit BEFORE the cached end too — only a sliding preset's auto-refresh
           always moves forward. */
        var cache = new ViewerNameListCache();
        var now = new DateTime(2026, 3, 1, 12, 0, 0);
        cache.Set(1, Length, now, new List<string> { "CXPACKET" }, now);

        Assert.True(cache.TryGet(1, Length, now.AddMinutes(-5), now, out _));
    }
}

/// <summary>
/// #4234 review, gated (DARLING_TEST_PG) live pins for the ruling's remaining two tests (the 7-day
/// row-budget cap, and point equality when the budget covers every collection) plus an equality pin
/// against the MCP twins at a shared explicit width. Shares the serialized "live-postgres" collection.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerTrendBucketingLiveTests
{
    private const int ServerId = -424242;
    private const string ServerName = "viewer-trend-bucketing-e2e";

    [Fact]
    public async Task WaitTrend_SevenDayWindow_ReturnsAtMostBudgetTimesSeriesRows()
    {
        await RunAsync(async (connection, postgres, viewer, ct) =>
        {
            var end = new DateTime(2026, 3, 10, 0, 0, 0);
            var start = end.AddDays(-7);
            var waitTypes = new List<string> { "CXPACKET", "WRITELOG" };

            var baseId = CollectionIdGenerator.Next() * 1_000_000L;
            foreach (var waitType in waitTypes)
            {
                await BulkSeedWaitAsync(connection, ct, baseId, start, end, waitType);
                baseId += 20_000;
            }

            var trends = await viewer.GetWaitStatsTrendsByTypesAsync(ServerId, waitTypes, start, end, ct);

            var totalRows = trends.Values.Sum(list => list.Count);
            var budget = TrendBudget.Chart.AutoPoints * waitTypes.Count;
            Assert.True(totalRows > 0 && totalRows <= budget, $"{totalRows} rows over a {waitTypes.Count}-series budget of {budget}");
        });
    }

    [Fact]
    public async Task PerfmonTrend_SevenDayWindow_ReturnsAtMostBudgetTimesSeriesRows()
    {
        await RunAsync(async (connection, postgres, viewer, ct) =>
        {
            var end = new DateTime(2026, 3, 10, 0, 0, 0);
            var start = end.AddDays(-7);
            var counters = new List<string> { "Batch Requests/sec", "Page life expectancy" };

            var baseId = CollectionIdGenerator.Next() * 1_000_000L;
            foreach (var counter in counters)
            {
                await BulkSeedPerfmonAsync(connection, ct, baseId, start, end, counter);
                baseId += 20_000;
            }

            var trends = await viewer.GetPerfmonTrendsByCountersAsync(ServerId, counters, start, end, ct);

            var totalRows = trends.Values.Sum(list => list.Count);
            var budget = TrendBudget.Chart.AutoPoints * counters.Count;
            Assert.True(totalRows > 0 && totalRows <= budget, $"{totalRows} rows over a {counters.Count}-series budget of {budget}");
        });
    }

    [Fact]
    public async Task WaitTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsAndValuesUnchanged()
    {
        await RunAsync(async (connection, postgres, viewer, ct) =>
        {
            /* Off the minute grid (:37 seconds) — on the pre-fix code every point would have been
               floored to the date_bin grid line, losing the seconds. */
            var t1 = new DateTime(2026, 3, 10, 9, 0, 37);
            var t2 = t1.AddMinutes(5);
            var t3 = t2.AddMinutes(5);

            await InsertWaitRowAsync(connection, ct, t1, "CXPACKET", deltaWait: 300, deltaSignal: 30, deltaTasks: 10, sampleIntervalSeconds: 300);
            await InsertWaitRowAsync(connection, ct, t2, "CXPACKET", deltaWait: 600, deltaSignal: 60, deltaTasks: 20, sampleIntervalSeconds: 300);
            await InsertWaitRowAsync(connection, ct, t3, "CXPACKET", deltaWait: 900, deltaSignal: 90, deltaTasks: 30, sampleIntervalSeconds: 300);

            var trends = await viewer.GetWaitStatsTrendsByTypesAsync(
                ServerId, new List<string> { "CXPACKET" }, t1.AddMinutes(-1), t3.AddMinutes(1), ct);

            var points = trends["CXPACKET"];
            Assert.Equal(new[] { t1, t2, t3 }, points.Select(p => p.CollectionTime).ToArray());
            Assert.Equal(1.0, points[0].WaitTimeMsPerSecond, precision: 6);
            Assert.Equal(2.0, points[1].WaitTimeMsPerSecond, precision: 6);
            Assert.Equal(3.0, points[2].WaitTimeMsPerSecond, precision: 6);
            Assert.Equal(30.0, points[0].AvgMsPerWait, precision: 6);
            Assert.Equal(30.0, points[1].AvgMsPerWait, precision: 6);
        });
    }

    [Fact]
    public async Task PerfmonTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsAndValuesUnchanged()
    {
        await RunAsync(async (connection, postgres, viewer, ct) =>
        {
            var t1 = new DateTime(2026, 3, 10, 9, 0, 37);
            var t2 = t1.AddMinutes(5);
            var t3 = t2.AddMinutes(5);

            await InsertPerfmonRowAsync(connection, ct, t1, "Batch Requests/sec", cntrValue: 1000, deltaValue: 300, sampleIntervalSeconds: 300);
            await InsertPerfmonRowAsync(connection, ct, t2, "Batch Requests/sec", cntrValue: 1300, deltaValue: 600, sampleIntervalSeconds: 300);
            await InsertPerfmonRowAsync(connection, ct, t3, "Batch Requests/sec", cntrValue: 1900, deltaValue: 900, sampleIntervalSeconds: 300);

            var trends = await viewer.GetPerfmonTrendsByCountersAsync(
                ServerId, new List<string> { "Batch Requests/sec" }, t1.AddMinutes(-1), t3.AddMinutes(1), ct);

            var points = trends["Batch Requests/sec"];
            Assert.Equal(new[] { t1, t2, t3 }, points.Select(p => p.CollectionTime).ToArray());
            Assert.Equal(new long[] { 1000, 1300, 1900 }, points.Select(p => p.Value).ToArray());
            Assert.Equal(new long?[] { 300, 600, 900 }, points.Select(p => p.DeltaValue).ToArray());
            Assert.Equal(new long?[] { 300, 300, 300 }, points.Select(p => p.SampleIntervalSeconds).ToArray());
        });
    }

    /// <summary>
    /// #4234 review: "the WPF read returns the same bucket starts and values as its MCP twin at the same
    /// width" — run at an EXPLICIT width where buckets hold multiple collections (60 minutes over 90
    /// one-minute collections), so the comparison exercises the shared aggregation math
    /// (<see cref="ViewerDataService.WaitTrendsSql"/> vs <see cref="DarlingDataReader.WaitTrendBucketedSql"/>),
    /// not the singleton-collapsing wrapper the two point-equality tests above already cover.
    /// </summary>
    [Fact]
    public async Task WaitTrend_AtASharedExplicitWidth_MatchesTheMcpTwinsBucketStartsAndValues()
    {
        await RunAsync(async (connection, postgres, viewer, ct) =>
        {
            var start = new DateTime(2026, 3, 10, 8, 0, 0);
            var baseId = CollectionIdGenerator.Next() * 1_000_000L;
            await BulkSeedWaitAsync(connection, ct, baseId, start, start.AddMinutes(89), "CXPACKET");
            var end = start.AddMinutes(90);
            const int width = 60;

            var wpfBuckets = new List<(DateTime Start, double Rate)>();
            await using (var command = postgres.CreateCommand(ViewerDataService.WaitTrendsSql(1)))
            {
                command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "CXPACKET" });
                command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = width });
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    wpfBuckets.Add((reader.GetDateTime(1), reader.GetDouble(2)));
                }
            }

            var mcpBuckets = await DarlingDataReader.GetWaitBucketsAsync(postgres, ServerId, "CXPACKET", start, end, width, ct);

            Assert.True(wpfBuckets.Count >= 2, "the window must straddle more than one 60-minute bucket");
            Assert.Equal(mcpBuckets.Count, wpfBuckets.Count);
            for (var i = 0; i < wpfBuckets.Count; i++)
            {
                Assert.Equal(mcpBuckets[i].BucketStart, wpfBuckets[i].Start);
                Assert.Equal(mcpBuckets[i].WaitTimeMsPerSecond, wpfBuckets[i].Rate, precision: 6);
            }
        });
    }

    [Fact]
    public async Task PerfmonTrend_AtASharedExplicitWidth_MatchesTheMcpTwinsBucketStartsAndRates()
    {
        await RunAsync(async (connection, postgres, viewer, ct) =>
        {
            var start = new DateTime(2026, 3, 10, 8, 0, 0);
            var baseId = CollectionIdGenerator.Next() * 1_000_000L;
            await BulkSeedPerfmonAsync(connection, ct, baseId, start, start.AddMinutes(89), "Batch Requests/sec");
            var end = start.AddMinutes(90);
            const int width = 60;

            var wpfBuckets = new List<(DateTime Start, long? Delta, long? Seconds)>();
            await using (var command = postgres.CreateCommand(ViewerDataService.PerfmonTrendsSql(1)))
            {
                command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "Batch Requests/sec" });
                command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = width });
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    wpfBuckets.Add((
                        reader.GetDateTime(1),
                        reader.IsDBNull(3) ? null : reader.GetInt64(3),
                        reader.IsDBNull(4) ? null : reader.GetInt64(4)));
                }
            }

            var mcpBuckets = await DarlingTrendReader.GetPerfmonBucketsAsync(postgres, ServerId, "Batch Requests/sec", start, end, width, ct);

            Assert.True(wpfBuckets.Count >= 2, "the window must straddle more than one 60-minute bucket");
            Assert.Equal(mcpBuckets.Count, wpfBuckets.Count);
            for (var i = 0; i < wpfBuckets.Count; i++)
            {
                Assert.Equal(mcpBuckets[i].BucketStart, wpfBuckets[i].Start);
                /* delta_cntr_value / sample_interval_seconds are the same SUM(...) FILTER (...) expression
                   in both statements — RatedDelta/RatedSeconds on the MCP side, DeltaValue/SampleIntervalSeconds
                   on the WPF side — so these must match exactly, unlike the rounded gauge average. */
                Assert.Equal(mcpBuckets[i].RatedDelta, wpfBuckets[i].Delta);
                Assert.Equal(mcpBuckets[i].RatedSeconds, wpfBuckets[i].Seconds);
            }
        });
    }

    /* ───────────────────────────── plumbing ───────────────────────────── */

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, ViewerDataService, System.Threading.CancellationToken, Task> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live trend-bucketing review tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        await using var viewer = new ViewerDataService(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await body(connection, postgres, viewer, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task InsertWaitRowAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime t, string waitType,
        long deltaWait, long deltaSignal, long deltaTasks, int sampleIntervalSeconds) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 0, 0, 0, $6, $7, $8, $9)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, waitType, deltaTasks, deltaWait, deltaSignal, sampleIntervalSeconds);

    private static async Task InsertPerfmonRowAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime t, string counterName,
        long cntrValue, long deltaValue, int sampleIntervalSeconds) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO perfmon_stats
    (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name,
     cntr_value, delta_cntr_value, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'SQLServer:SQL Statistics', $5, '', $6, $7, $8)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, counterName, cntrValue, deltaValue, sampleIntervalSeconds);

    /// <summary>One row per minute from <paramref name="start"/> to <paramref name="end"/> inclusive, generated
    /// server-side — the real wait_stats cadence (<c>CollectorScheduleDefaults.All["wait_stats"]</c> is 1
    /// minute) without a per-row round trip. <paramref name="baseId"/> is pre-multiplied by the caller so this
    /// block of sequential ids cannot collide with another concurrent test's single <c>CollectionIdGenerator.Next()</c> call.</summary>
    private static async Task BulkSeedWaitAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, long baseId, DateTime start, DateTime end, string waitType) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
SELECT $1 + row_number() OVER (), g, $2, $3, $4, 0, 0, 0, 10, 100, 10, 60
FROM generate_series($5::timestamp, $6::timestamp, interval '1 minute') AS g",
            baseId, ServerId, ServerName, waitType, start, end);

    /// <summary>Perfmon twin of <see cref="BulkSeedWaitAsync"/> — the real perfmon_stats cadence is also 1
    /// minute.</summary>
    private static async Task BulkSeedPerfmonAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, long baseId, DateTime start, DateTime end, string counterName) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO perfmon_stats
    (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name,
     cntr_value, delta_cntr_value, sample_interval_seconds)
SELECT $1 + row_number() OVER (), g, $2, $3, 'SQLServer:SQL Statistics', $4, '', 1000, 100, 60
FROM generate_series($5::timestamp, $6::timestamp, interval '1 minute') AS g",
            baseId, ServerId, ServerName, counterName, start, end);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        foreach (var table in new[] { "wait_stats", "perfmon_stats", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", ServerId);
        }
    }
}
