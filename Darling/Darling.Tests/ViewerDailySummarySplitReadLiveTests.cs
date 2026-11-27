/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: the Viewer reads a cold Daily Summary range as up to four day-aligned sub-ranges side by side, each older one a day past its
/// end, and keeps each sub-range's own days. The rows are the rows the one statement returns for the same store, every field of every
/// day: for a blocked session whose reports cross a split midnight, a wait that runs more than a day past one with reports throughout,
/// DMV snapshot waits beside and across one, a deadlock, a wait and a query on each side of each, a scoped Azure SQL Database master,
/// and the hourly rollup tier. A one-statement read that stops at a split midnight counts the day before it differently, which is what
/// the extra day is read for. Gated on DARLING_TEST_PG.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE and drop its
   own scratch database through ScratchPostgres. */
public sealed class ViewerDailySummarySplitReadLiveTests
{
    private const int PlainId = 8561;
    private const int MasterId = 8562;
    private const int GpId = 8563;

    /* The window: 20 closed days and the day the cache's clock sits in. Split in four, its sub-ranges start at B1, B2 and B3. */
    private static readonly DateTime LastDay = LiveClock.Now().Date.AddDays(-2);
    private static readonly DateTime From = LastDay.AddDays(-20);
    private static readonly DateTime To = LastDay.AddDays(1);
    private static readonly DateTime CacheNow = LastDay.AddHours(12);
    private static readonly DateTime B1 = LastDay.AddDays(-5);
    private static readonly DateTime B2 = LastDay.AddDays(-10);
    private static readonly DateTime B3 = LastDay.AddDays(-15);

    [Fact]
    public async Task AColdRangeReadAsSubRanges_ReturnsTheOneStatementsRows_ForWaitsThatCrossTheSplitMidnights() => await WithStoreAsync(timescale: false, async (scratch, connection, ct) =>
    {
        await SeedPlainAsync(connection, ct);
        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        var one = await ReadAsync(viewer, PlainId, From, To, parts: 1, cold: true, ct);
        Assert.Equal(new List<(DateTime, DateTime)> { (From, To) }, one.Reads);
        Assert.Equal(21, one.Rows.Count);

        /* B1: the report wait's longest report is at 00:00:10, so it is the new day's wait and the old day's 23:00 hour holds no report
           wait: that hour counts its two DMV waits. The DMV wait that crosses the midnight ends in the new day's 01:00 hour. */
        AssertBlocking(one.Rows, B1.AddDays(-1), events: 2, peakMs: 120_000);
        AssertBlocking(one.Rows, B1, events: 2, peakMs: 4_800_000);

        /* B2: one wait reported every 30 minutes from 23:30 to 02:00 two days on. It is one wait, on the day of its longest report. */
        AssertBlocking(one.Rows, B2.AddDays(-1), events: 0, peakMs: 0);
        AssertBlocking(one.Rows, B2, events: 0, peakMs: 0);
        AssertBlocking(one.Rows, B2.AddDays(1), events: 1, peakMs: 97_200_000);

        /* B3: a DMV wait sampled every minute from 23:51 to 00:09 is the new day's wait. */
        AssertBlocking(one.Rows, B3.AddDays(-1), events: 0, peakMs: 0);
        AssertBlocking(one.Rows, B3, events: 1, peakMs: 1_140_000);
        Assert.True(Day(one.Rows, B3).PeakSnapshotWaitMs > 0, "the sampled wait passes the snapshot gate on the day it ends");

        foreach (var midnight in new[] { B1, B2, B3 })
        {
            foreach (var day in new[] { midnight.AddDays(-1), midnight })
            {
                Assert.Equal(1, Day(one.Rows, day).DeadlockCount);
                Assert.Equal(QueriesOn(day) + 1, Day(one.Rows, day).UniqueQueries);
            }
        }

        /* Four sub-ranges, newest first, each older one read a day past its end. */
        var split = await ReadAsync(viewer, PlainId, From, To, parts: 4, cold: true, ct);
        Assert.Equal(
            new List<(DateTime, DateTime)> { (B1, To), (B2, B1.AddDays(1)), (B3, B2.AddDays(1)), (From, B3.AddDays(1)) },
            split.Reads);
        AssertSameRows(one.Rows, split.Rows);

        /* The block the split stored serves the next read, which reads only the open day. */
        var hit = await ReadAsync(viewer, PlainId, From, To, parts: 4, cold: false, ct);
        Assert.Equal(new List<(DateTime, DateTime)> { (LastDay, To) }, hit.Reads);
        AssertSameRows(one.Rows, hit.Rows);

        /* Three and two sub-ranges put the midnights inside a sub-range and inside the extra day. */
        foreach (var parts in new[] { 3, 2 })
        {
            var fewer = await ReadAsync(viewer, PlainId, From, To, parts, cold: true, ct);
            Assert.Equal(
                DailySummaryRangeCache.MissSubRanges(From, To, LastDay, parts).Select(part => (part.Start, part.ReadEnd)).ToList(),
                fewer.Reads);
            Assert.Equal(parts, fewer.Reads.Count);
            AssertSameRows(one.Rows, fewer.Rows);
        }

        /* A statement that stops at the midnight sees the part of each wait before it: the report wait becomes the old day's and
           silences that hour's DMV waits, and the long wait and the sampled wait each count on the old day. */
        var stoppedAtB1 = await ReadAsync(viewer, PlainId, From, B1, parts: 1, cold: true, ct);
        AssertBlocking(stoppedAtB1.Rows, B1.AddDays(-1), events: 1, peakMs: 20_000);
        var stoppedAtB2 = await ReadAsync(viewer, PlainId, From, B2, parts: 1, cold: true, ct);
        AssertBlocking(stoppedAtB2.Rows, B2.AddDays(-1), events: 1, peakMs: 1_800_000);
        var stoppedAtB3 = await ReadAsync(viewer, PlainId, From, B3, parts: 1, cold: true, ct);
        AssertBlocking(stoppedAtB3.Rows, B3.AddDays(-1), events: 1, peakMs: 540_000);
    });

    [Fact]
    public async Task AScopedMastersColdRangeReadAsSubRanges_ReturnsTheOneStatementsRows() => await WithStoreAsync(timescale: false, async (scratch, connection, ct) =>
    {
        await SeedMasterAsync(connection, ct);
        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        Assert.NotEmpty(await viewer.GetSeparatelyMonitoredAsync(MasterId, ct));

        var one = await ReadAsync(viewer, MasterId, From, To, parts: 1, cold: true, ct);
        Assert.Equal(new List<(DateTime, DateTime)> { (From, To) }, one.Reads);
        Assert.Equal(21, one.Rows.Count);

        /* The sibling database's reports are not the master's: its own report wait is the new day's, and the old day's 23:00 hour
           counts the master's two DMV waits. */
        AssertBlocking(one.Rows, B1.AddDays(-1), events: 2, peakMs: 70_000);
        AssertBlocking(one.Rows, B1, events: 1, peakMs: 40_000);

        /* On each side of B2 and B3: one deadlock in another database, one graph partly outside the sibling, one wholly inside it. */
        foreach (var midnight in new[] { B2, B3 })
        {
            Assert.Equal(2, Day(one.Rows, midnight.AddDays(-1)).DeadlockCount);
            Assert.Equal(2, Day(one.Rows, midnight).DeadlockCount);
        }

        var split = await ReadAsync(viewer, MasterId, From, To, parts: 4, cold: true, ct);
        Assert.Equal(
            new List<(DateTime, DateTime)> { (B1, To), (B2, B1.AddDays(1)), (B3, B2.AddDays(1)), (From, B3.AddDays(1)) },
            split.Reads);
        AssertSameRows(one.Rows, split.Rows);

        var hit = await ReadAsync(viewer, MasterId, From, To, parts: 4, cold: false, ct);
        Assert.Equal(new List<(DateTime, DateTime)> { (LastDay, To) }, hit.Reads);
        AssertSameRows(one.Rows, hit.Rows);

        var stoppedAtB1 = await ReadAsync(viewer, MasterId, From, B1, parts: 1, cold: true, ct);
        AssertBlocking(stoppedAtB1.Rows, B1.AddDays(-1), events: 1, peakMs: 20_000);
    });

    [Fact]
    public async Task AColdRangeReadAsSubRanges_ReturnsTheOneStatementsRows_OnTheHourlyRollupTier() => await WithStoreAsync(timescale: true, async (scratch, connection, ct) =>
    {
        await SeedPlainAsync(connection, ct);

        /* The query rollups: the hourly holds the window, the daily stops three days short, and the interval pair holds the last
           eight days, so the statement stitches inside the window. The wait rollups: the daily holds the days before the last week
           and the hourly four days more, so a day's waits come from the daily rollup, the hourly rollup or raw by its age. */
        await RefreshAsync(connection, TimescaleSupport.QueryStatsHourlyView, From.AddDays(-1), LastDay.AddHours(6), ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsDailyView, From.AddDays(-1), LastDay.AddDays(-3), ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, LastDay.AddDays(-8), LastDay.AddHours(6), ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalDailyView, LastDay.AddDays(-8), LastDay.AddDays(-3), ct);
        await RefreshAsync(connection, TimescaleSupport.WaitStatsHourlyView, From.AddDays(-1), LastDay.AddDays(-3), ct);
        await RefreshAsync(connection, TimescaleSupport.WaitStatsDailyView, From.AddDays(-1), LastDay.AddDays(-7), ct);

        await using (var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
            var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
            var tier = RetentionTierRouter.Resolve(
                LiveClock.Now(), From, rollups.QueryGrainHourly, rollups.QueryGrainDaily,
                coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView));
            Assert.Equal(RetentionTier.Hourly, tier);
            var routed = DailySummarySql.RangeSqlFor(tier, coverage, From);
            Assert.NotEqual(DailySummarySql.RangeSql, routed);
            Assert.Contains("wait_covered", routed, StringComparison.Ordinal);
            Assert.True(
                coverage.StitchFloor(TimescaleSupport.QueryStatsHourlyView, RollupCoverage.StitchTier.Hourly, From).HasValue,
                "the interval pair's floor sits inside the window");
        }

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var one = await ReadAsync(viewer, PlainId, From, To, parts: 1, cold: true, ct);
        Assert.Equal(new List<(DateTime, DateTime)> { (From, To) }, one.Reads);
        Assert.Equal(21, one.Rows.Count);
        Assert.True(Day(one.Rows, B1.AddDays(-1)).UniqueQueries > 0, "the day before the newest split has a query count");
        Assert.True(Day(one.Rows, B3).TotalWaitTimeSec > 0, "a day the daily wait rollup holds has its wait total");

        var split = await ReadAsync(viewer, PlainId, From, To, parts: 4, cold: true, ct);
        Assert.Equal(
            new List<(DateTime, DateTime)> { (B1, To), (B2, B1.AddDays(1)), (B3, B2.AddDays(1)), (From, B3.AddDays(1)) },
            split.Reads);
        AssertSameRows(one.Rows, split.Rows);

        var hit = await ReadAsync(viewer, PlainId, From, To, parts: 4, cold: false, ct);
        Assert.Equal(new List<(DateTime, DateTime)> { (LastDay, To) }, hit.Reads);
        AssertSameRows(one.Rows, hit.Rows);
    });

    private static async Task WithStoreAsync(bool timescale, Func<ScratchPostgres, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live #8549 split-read tests.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            if (timescale)
            {
                Assert.SkipWhen(!await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "The rollup-tier split-read test needs TimescaleDB.");
                await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
                await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
                await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            }

            await body(scratch, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    /// <summary>One range read through the Viewer as <paramref name="parts"/> sub-ranges, with each [start, end) the statement ran
    /// over. <paramref name="cold"/> swaps in an empty cache whose clock sits in the window's last day, so the read is a miss and the
    /// closed days end at <see cref="LastDay"/>.</summary>
    private static async Task<(List<DailySummaryRow> Rows, List<(DateTime Start, DateTime End)> Reads)> ReadAsync(
        ViewerDataService viewer, int id, DateTime from, DateTime to, int parts, bool cold, CancellationToken ct)
    {
        if (cold)
        {
            viewer.UseRangeCacheClockForTests(() => CacheNow);
        }

        var reads = new List<(DateTime Start, DateTime End)>();
        viewer.DailySummaryMissPartsForTests = parts;
        viewer.DailySummaryRangeReadHookForTests = (start, end) =>
        {
            lock (reads)
            {
                reads.Add((start, end));
            }
        };
        try
        {
            return (await viewer.GetDailySummaryRangeAsync(id, from, to, ct), reads);
        }
        finally
        {
            viewer.DailySummaryRangeReadHookForTests = null;
        }
    }

    private static DailySummaryRow Day(List<DailySummaryRow> rows, DateTime day) => rows.Single(row => row.SummaryDate.Date == day);

    private static void AssertBlocking(List<DailySummaryRow> rows, DateTime day, long events, long peakMs)
    {
        var row = Day(rows, day);
        Assert.Equal(events, row.BlockingEvents);
        Assert.Equal(peakMs, row.MaxBlockDurationMs);
    }

    /* Every field the statement returns and every judgment made from it. The wall-clock stamp and the horizon are the read's, not the day's. */
    private static string Line(DailySummaryRow row) => string.Create(CultureInfo.InvariantCulture,
        $"{row.SummaryDate:yyyy-MM-dd}|wait={row.TotalWaitTimeSec}|top={row.TopWaitType}|queries={row.UniqueQueries}|deadlocks={row.DeadlockCount}|blocking={row.BlockingEvents}|peak={row.MaxBlockDurationMs}|snapshot={row.PeakSnapshotWaitMs}|cpu={row.HighCpuEvents}|memory={row.MemoryPressureEvents}|critical={row.MemoryCriticalEvents}|errors={row.CollectionErrors}|runs={row.CollectionRuns}|alerts={row.AlertCount}|sources={row.SignalSourcesPresent}|data={row.HasData}|state={row.DataState}|band={row.HealthBand}");

    private static void AssertSameRows(List<DailySummaryRow> one, List<DailySummaryRow> other)
    {
        Assert.Equal(one.Select(Line).ToList(), other.Select(Line).ToList());
        for (var i = 0; i < one.Count; i++)
        {
            Assert.Equal(one[i].BlockingEvents, other[i].BlockingEvents);
            Assert.Equal(one[i].MaxBlockDurationMs, other[i].MaxBlockDurationMs);
            Assert.Equal(one[i].SignalSourcesPresent, other[i].SignalSourcesPresent);
        }
    }

    /* The bulk query rows give a day 3 + (day of month % 5) hashes. */
    private static long QueriesOn(DateTime day) => 3 + (day.Day % 5);

    private static string Graph(params string[] dbs) =>
        "<deadlock><process-list>" + string.Concat(dbs.Select((d, i) => $"<process id=\"p{i}\" currentdbname=\"{d}\" />")) + "</process-list></deadlock>";

    private static async Task SeedPlainAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await RegisterAsync(connection, PlainId, "host-b.example", null, 3, ct);
        await SeedDaysAsync(connection, PlainId, ct);

        /* A deadlock, a wait and a query one second before each split midnight and at it. */
        var edge = 0;
        foreach (var midnight in new[] { B1, B2, B3 })
        {
            foreach (var at in new[] { midnight.AddSeconds(-1), midnight })
            {
                await DeadAsync(connection, PlainId, at, "db", Graph("db"), ct);
                await WaitAsync(connection, PlainId, at, "LCK_M_X", 45_000L, ct);
                await QueryAsync(connection, PlainId, at, "EDGE_" + edge.ToString(CultureInfo.InvariantCulture), ct);
                edge++;
            }
        }

        /* A query row every 20 minutes from a day before the window, so a rollup's first bucket is older than the window. */
        await ExecuteAsync(connection, $"""
            INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                                             delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
            SELECT 856100000 + row_number() OVER (ORDER BY g.t), g.t, {PlainId}, 's', 'db',
                   'Q' || ((extract(epoch FROM g.t)::bigint / 1200) % (3 + extract(day FROM g.t)::int % 5)), '0x01', 10, 10, 1, 120
            FROM generate_series('{Ts(From.AddDays(-1))}'::timestamp, '{Ts(To.AddMinutes(-20))}'::timestamp, interval '20 minutes') AS g(t)
            """, ct);

        /* B1: one blocked session reported at 23:59:40, 23:59:50, 00:00:00 and 00:00:10. */
        for (var i = 0; i < 4; i++)
        {
            await BprAsync(connection, PlainId, B1.AddSeconds(-20 + (10 * i)), 10_000 * (i + 1), blocked: 70, blocker: 60, "db", ct);
        }

        /* B1: two DMV waits wholly inside the old day's 23:00 hour, and one sampled every 5 minutes from 23:50 to 01:05. */
        await DmvAsync(connection, PlainId, B1.AddMinutes(-40), 60_000, blocked: 71, blocker: 60, ct);
        await DmvAsync(connection, PlainId, B1.AddMinutes(-39), 120_000, blocked: 71, blocker: 60, ct);
        await DmvAsync(connection, PlainId, B1.AddMinutes(-30), 45_000, blocked: 72, blocker: 60, ct);
        for (var minute = -10; minute <= 65; minute += 5)
        {
            await DmvAsync(connection, PlainId, B1.AddMinutes(minute), (minute + 15) * 60_000, blocked: 73, blocker: 60, ct);
        }

        /* B2: one blocked session reported every 30 minutes from 23:30 until 02:00 two days on, 26 hours past the midnight. */
        for (var k = 1; k <= 54; k++)
        {
            await BprAsync(connection, PlainId, B2.AddHours(-1).AddMinutes(30 * k), 1_800_000 * k, blocked: 74, blocker: 61, "db", ct);
        }

        /* B3: one DMV wait sampled every minute from 23:51 to 00:09, with no report. */
        for (var minute = -9; minute <= 9; minute++)
        {
            await DmvAsync(connection, PlainId, B3.AddMinutes(minute), (minute + 10) * 60_000, blocked: 75, blocker: 62, ct);
        }
    }

    private static async Task SeedMasterAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await RegisterAsync(connection, MasterId, "host-a.example", "master", 5, ct);
        await RegisterAsync(connection, GpId, "host-a.example", "GP", 5, ct);
        await SeedDaysAsync(connection, MasterId, ct);

        /* B1: the master's own blocked session and a sibling's, each reported across the midnight, and a sibling's report and the
           master's two DMV waits inside the old day's 23:00 hour. */
        for (var i = 0; i < 4; i++)
        {
            await BprAsync(connection, MasterId, B1.AddSeconds(-20 + (10 * i)), 10_000 * (i + 1), blocked: 80, blocker: 60, "master", ct);
            await BprAsync(connection, MasterId, B1.AddSeconds(-25 + (10 * i)), 50_000 * (i + 1), blocked: 81, blocker: 61, "GP", ct);
        }

        await BprAsync(connection, MasterId, B1.AddMinutes(-20), 300_000, blocked: 82, blocker: 61, "GP", ct);
        await DmvAsync(connection, MasterId, B1.AddMinutes(-30), 45_000, blocked: 83, blocker: 60, ct);
        await DmvAsync(connection, MasterId, B1.AddMinutes(-25), 70_000, blocked: 84, blocker: 60, ct);

        /* B2 and B3: on each side of the midnight, a deadlock in another database, a graph partly outside the sibling and a graph
           wholly inside it. */
        foreach (var midnight in new[] { B2, B3 })
        {
            await DeadAsync(connection, MasterId, midnight.AddSeconds(-3), "Other", Graph("Other"), ct);
            await DeadAsync(connection, MasterId, midnight.AddSeconds(-2), "master", Graph("GP", "HS"), ct);
            await DeadAsync(connection, MasterId, midnight.AddSeconds(-1), null, Graph("gp"), ct);
            await DeadAsync(connection, MasterId, midnight, "Other", Graph("Other"), ct);
            await DeadAsync(connection, MasterId, midnight.AddSeconds(1), "master", Graph("GP", "HS"), ct);
            await DeadAsync(connection, MasterId, midnight.AddSeconds(2), "GP", Graph("GP", "GP"), ct);
        }
    }

    private static async Task RegisterAsync(NpgsqlConnection connection, int id, string host, string? database, int edition, CancellationToken ct)
    {
        var name = host + "/" + (database ?? "-");
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, sql_major_version, created_date, modified_date) VALUES ($1, $2, $2, TRUE, $3, 16, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC')",
            id, name, edition);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, engine_edition) VALUES ($1, now() AT TIME ZONE 'UTC', $2, 's', $3)",
            CollectionIdGenerator.Next(), id, edition);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO config.config_monitored_servers (server_id, name, host, database, is_enabled) VALUES ($1, $2, $3, $4, TRUE)",
            id, name, host, (object?)database ?? DBNull.Value);
    }

    /* A collection run and a wait on every day of the window, so every day is a row. */
    private static async Task SeedDaysAsync(NpgsqlConnection connection, int id, CancellationToken ct)
    {
        for (var day = From; day <= LastDay; day = day.AddDays(1))
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, 's', 'wait_stats', $3, 'SUCCESS')",
                CollectionIdGenerator.Next(), id, day.AddHours(1));
            await WaitAsync(connection, id, day.AddHours(2), "LCK_M_S", 30_000L, ct);
        }
    }

    private static Task WaitAsync(NpgsqlConnection connection, int id, DateTime at, string waitType, long waitMs, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, sample_interval_seconds) VALUES ($1, $2, $3, 's', $4, $5, $6, $7)",
            CollectionIdGenerator.Next(), at, id, waitType, 5L, waitMs, 60);

    private static Task BprAsync(NpgsqlConnection connection, int id, DateTime at, int waitMs, int blocked, int blocker, string db, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1, $2, $3, 's', $2, $4, $5, $6, 'suspended', $7)",
            CollectionIdGenerator.Next(), at, id, waitMs, blocker, blocked, db);

    private static Task DmvAsync(NpgsqlConnection connection, int id, DateTime at, int waitMs, int blocked, int blocker, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, blocking_status) VALUES ($1, $2, $3, 's', $2, 'master', $4, $5, $6, 'suspended')",
            CollectionIdGenerator.Next(), at, id, blocked, blocker, waitMs);

    private static Task DeadAsync(NpgsqlConnection connection, int id, DateTime at, string? stamp, string xml, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1, $2, $3, 's', $2, $4, $5)",
            CollectionIdGenerator.Next(), at, id, xml, (object?)stamp ?? DBNull.Value);

    private static Task QueryAsync(NpgsqlConnection connection, int id, DateTime at, string hash, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle, delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds) VALUES ($1, $2, $3, 's', 'db', $4, '0x01', 10, 10, 1, 120)",
            CollectionIdGenerator.Next(), at, id, hash);

    private static Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
        => ExecuteAsync(connection, $"CALL refresh_continuous_aggregate('collect.{view}'::regclass, '{Ts(from)}'::timestamp, '{Ts(to)}'::timestamp)", ct);

    private static string Ts(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }
}
