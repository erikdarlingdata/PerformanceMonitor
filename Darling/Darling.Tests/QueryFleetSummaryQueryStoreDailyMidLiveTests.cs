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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch databases through ScratchPostgres and never touches another test's rows. */

/// <summary>
/// #8570: the Query Store source of <c>get_query_fleet_summary</c> reads whole closed days from <c>query_store_stats_corrected_daily</c> and only the
/// edge hours from <c>query_store_stats_corrected_hourly</c>. The one-relation statement (<see cref="DarlingMcpQueryFleetSummaryTools.BuildQueryStoreSql"/>
/// over the hourly rollup) is the oracle: the daily-mid statement must answer the same rows in the same order on the same store, first_hour and
/// last_hour to the microsecond, with EXCEPT both ways, at several row caps (ties at the cut included) and with a NULL database_name pair.
/// </summary>
public sealed class QueryFleetSummaryQueryStoreDailyMidLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8570 Query Store fleet summary tests.";
    private const string Hash = "0xQS8570";
    private const string Other = "0xQSOTHER8570";
    private static readonly (int Id, string Name)[] Servers = [(-857011, "q8570-one"), (-857012, "q8570-two"), (-857013, "q8570-three")];
    private static long _intervalId = 850_000;

    private sealed record Row(string Server, string? Display, string? Database, long Executions, long CpuUs, long ElapsedUs, DateTime First, DateTime Last, long Pairs);

    private sealed record Store(ScratchPostgres Scratch, NpgsqlConnection Connection);

    /// <summary>The window ends at noon eight days back, so every whole day in it is older than the daily policy's four-day re-read span and the mid
    /// route may read it from the daily. <c>End</c> (12:00) and the start (13:00 seven days earlier) are both off midnight, so both edge days hold
    /// hours, and the days fall the same way at every hour the test runs: six whole days from <c>DStart</c> to <c>DEnd</c>.</summary>
    private sealed record Timeline(DateTime End, int HoursBack)
    {
        public DateTime Start => End.AddHours(-HoursBack);
        public DateTime DStart => Start.Date.AddDays(1);
        public DateTime DEnd => End.Date;
    }

    private static Timeline NewTimeline()
    {
        var end = DateTime.SpecifyKind(LiveClock.Now().Date.AddDays(-8).AddHours(12), DateTimeKind.Unspecified);
        return new Timeline(end, 167);
    }

    private static async Task<Store> CreateStoreAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
            Assert.SkipWhen(!timescaleEnabled, "The live #8570 tests need TimescaleDB.");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
            await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            foreach (var (id, name) in Servers)
            {
                await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);
            }

            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            return new Store(scratch, connection);
        }
        catch
        {
            await connection.DisposeAsync();
            await scratch.DisposeAsync();
            throw;
        }
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task Refresh(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        await refresh.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One Query Store snapshot of one interval. Whole-number averages, so a weighted sum is exact in double precision and the two
    /// statements, which add the same rows in different orders, must agree to the unit.</summary>
    private static Task Snapshot(NpgsqlConnection c, CancellationToken ct, int serverIndex, string? db, long intervalId, DateTime intervalStart, DateTime collected,
        long executions, long avgCpuUs, long avgDurationUs, string hash = Hash)
        => DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO collect.query_store_stats
                  (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
                   query_id, plan_id, execution_type_desc, replica_role,
                   runtime_stats_interval_id, interval_start_time_utc, first_execution_time,
                   execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, interval_end_time_utc)
              VALUES ($1, $2, $3, $4, $5, 'dbo.Proc', $6, $7, $7, 'Regular', 'PRIMARY', $7, $8, $8, $9, $10, $11, 900, 400, $8 + interval '1 hour')",
            CollectionIdGenerator.Next(), collected, Servers[serverIndex].Id, Servers[serverIndex].Name, (object?)db ?? DBNull.Value, hash, intervalId, intervalStart,
            executions, avgDurationUs, avgCpuUs);

    private static long NextInterval() => Interlocked.Increment(ref _intervalId);

    /// <summary>Every kind of pair a mid route can meet: one that runs the whole window (two snapshots of one interval, and an interval that
    /// straddles an hour boundary), one only on the first edge day, one only on the last, one only in the mid (two different days), one from the
    /// first edge day into the mid, one from the mid into the last edge day, a NULL database_name pair only in the mid, two pairs that tie on CPU
    /// in the mid, one only in the raw tail, and rows of another hash.</summary>
    private static async Task SeedAsync(NpgsqlConnection c, CancellationToken ct, Timeline t)
    {
        var from = t.Start.Date;
        for (var at = from; at < t.End; at = at.AddHours(1))
        {
            var h = (long)(at - from).TotalHours;
            var interval = NextInterval();
            await Snapshot(c, ct, 0, "Alpha", interval, at, at.AddMinutes(10), 1 + h % 5, 100 + h % 7, 200 + h % 11);
            await Snapshot(c, ct, 0, "Alpha", interval, at, at.AddMinutes(40), 3 + h % 5, 100 + h % 7, 200 + h % 11);
            if (h % 24 == 5)
            {
                await Snapshot(c, ct, 0, "Alpha", interval, at, at.AddMinutes(65), 4 + h % 5, 100 + h % 7, 200 + h % 11);
            }

            await Snapshot(c, ct, 0, "Alpha", NextInterval(), at, at.AddMinutes(20), 9, 50, 60, Other);

            if (at < t.DStart)
            {
                await Snapshot(c, ct, 1, "Beta", NextInterval(), at, at.AddMinutes(15), 4, 70 + h % 3, 80 + h % 3);
            }

            if (at >= t.DEnd)
            {
                await Snapshot(c, ct, 2, "Gamma", NextInterval(), at, at.AddMinutes(15), 5, 90 + h % 3, 95 + h % 3);
            }

            if (at < t.DStart.AddDays(2))
            {
                await Snapshot(c, ct, 1, "Alpha", NextInterval(), at, at.AddMinutes(25), 6, 30 + h % 4, 40 + h % 4);
            }

            if (at >= t.DEnd.AddDays(-2))
            {
                await Snapshot(c, ct, 2, "Alpha", NextInterval(), at, at.AddMinutes(25), 7, 60 + h % 4, 65 + h % 4);
            }
        }

        await Snapshot(c, ct, 0, "Delta", NextInterval(), t.DStart.AddDays(1).AddHours(5), t.DStart.AddDays(1).AddHours(5).AddMinutes(12), 11, 123, 234);
        await Snapshot(c, ct, 0, "Delta", NextInterval(), t.DStart.AddDays(3).AddHours(17), t.DStart.AddDays(3).AddHours(17).AddMinutes(48), 13, 543, 654);
        await Snapshot(c, ct, 2, null, NextInterval(), t.DStart.AddDays(2).AddHours(3), t.DStart.AddDays(2).AddHours(3).AddMinutes(9), 3, 777, 888);
        await Snapshot(c, ct, 2, null, NextInterval(), t.DStart.AddDays(4).AddHours(22), t.DStart.AddDays(4).AddHours(22).AddMinutes(9), 2, 999, 1_000);

        /* Two pairs with the same CPU total (and the same row caps cutting between them): the order is CPU descending, then server, then database. */
        await Snapshot(c, ct, 0, "TieA", NextInterval(), t.DStart.AddDays(2).AddHours(8), t.DStart.AddDays(2).AddHours(8).AddMinutes(5), 10, 1_000, 1_100);
        await Snapshot(c, ct, 0, "TieB", NextInterval(), t.DStart.AddDays(3).AddHours(8), t.DStart.AddDays(3).AddHours(8).AddMinutes(5), 10, 1_000, 1_100);
    }

    private static async Task RefreshAllAsync(NpgsqlConnection c, CancellationToken ct, Timeline t, DateTime? dailyThrough)
    {
        await Refresh(c, TimescaleSupport.QueryStoreStatsIntervalHourlyView, t.Start.Date.AddDays(-1), t.End, ct);
        await Refresh(c, TimescaleSupport.QueryStoreStatsCorrectedHourlyView, t.Start.Date.AddDays(-1), t.End, ct);
        if (dailyThrough is DateTime through)
        {
            await Refresh(c, TimescaleSupport.QueryStoreStatsCorrectedDailyView, t.Start.Date, through, ct);
        }
    }

    /// <summary>A raw snapshot at the rollup's boundary, from a pair the rollup never held: the tail both statements read the same way.</summary>
    private static Task InsertTailAsync(NpgsqlConnection c, CancellationToken ct, Timeline t)
        => Snapshot(c, ct, 0, "Eps", NextInterval(), t.End, t.End.AddMinutes(5), 1, 100, 200);

    private static async Task<(DarlingMcpQueryFleetSummaryTools.DailyMidPlan? Plan, DateTime RawFrom)> PlanAsync(NpgsqlDataSource pg, Timeline t, DateTime now, CancellationToken ct)
    {
        var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
        var route = await QueryStoreTrendRouting.ResolveAsync(coverage, pg, ct);
        Assert.True(route.UseRollup);
        var plan = DarlingMcpQueryFleetSummaryTools.PlanQueryStoreDailyMid(coverage, t.Start, t.End, route.RawStartUtc, now);
        if (plan is not null && !await DarlingMcpQueryFleetSummaryTools.DailyHasNoHoleAsync(pg, plan, ct))
        {
            plan = null;
        }

        return (plan, route.RawStartUtc);
    }

    private static async Task<List<Row>> RunAsync(NpgsqlDataSource pg, string sql, Timeline t, DateTime rawFrom, int limit, DarlingMcpQueryFleetSummaryTools.DailyMidPlan? mid, CancellationToken ct)
    {
        await using var command = pg.CreateCommand(sql);
        AddParameters(command, t, rawFrom, limit, mid);
        var rows = new List<Row>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new Row(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetInt64(3), reader.GetInt64(4), reader.GetInt64(5), reader.GetDateTime(6), reader.GetDateTime(7), reader.GetInt64(8)));
        }

        return rows;
    }

    private static void AddParameters(NpgsqlCommand command, Timeline t, DateTime rawFrom, int limit, DarlingMcpQueryFleetSummaryTools.DailyMidPlan? mid)
    {
        command.Parameters.AddWithValue(NpgsqlDbType.Text, Hash);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(t.Start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(t.End, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(rawFrom, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, limit);
        command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Integer, DBNull.Value);
        if (mid is not null)
        {
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(mid.DStart, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(mid.DEnd, DateTimeKind.Unspecified));
        }
    }

    /// <summary>The oracle and the mid statement on the same store: identical rows in the same order (the records compare first_hour and last_hour
    /// by their ticks, which hold the microseconds) and EXCEPT both ways, at every row cap given.</summary>
    private static async Task AssertMidSameAsOracleAsync(NpgsqlDataSource pg, Timeline t, DarlingMcpQueryFleetSummaryTools.DailyMidPlan plan, DateTime rawFrom, params int[] limits)
    {
        var ct = TestContext.Current.CancellationToken;
        foreach (var limit in limits)
        {
            var oracle = await RunAsync(pg, DarlingMcpQueryFleetSummaryTools.BuildQueryStoreSql(true), t, rawFrom, limit, null, ct);
            var mid = await RunAsync(pg, DarlingMcpQueryFleetSummaryTools.BuildQueryStoreMidSql(), t, rawFrom, limit, plan, ct);
            Assert.NotEmpty(oracle);
            Assert.Equal(oracle, mid);
            Assert.Empty(oracle.Except(mid));
            Assert.Empty(mid.Except(oracle));
        }
    }

    [Fact]
    public async Task MidRoute_AnswersWhatTheHourlyRollupAnswers_AcrossEdgeHours_NullDatabaseAndTiesAtTheCut()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct, t);
            await RefreshAllAsync(connection, ct, t, t.DEnd);
            await InsertTailAsync(connection, ct, t);
            await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

            var (plan, rawFrom) = await PlanAsync(pg, t, LiveClock.Now(), ct);
            Assert.NotNull(plan);
            Assert.Equal(t.DStart, plan!.DStart);
            Assert.Equal(t.DEnd, plan.DEnd);
            Assert.True((plan.DEnd - plan.DStart).TotalDays >= 5, "the seed must leave a mid of whole days between the edges");
            Assert.Equal(TimescaleSupport.QueryStoreStatsCorrectedDailyView, plan.DailyView);

            /* Caps of 200 (everything), and 2 to 6 across the CPU ties and the NULL database pair. */
            await AssertMidSameAsOracleAsync(pg, t, plan, rawFrom, 200, 2, 3, 4, 5, 6);

            var all = await RunAsync(pg, DarlingMcpQueryFleetSummaryTools.BuildQueryStoreSql(true), t, rawFrom, 200, null, ct);
            Assert.Contains(all, r => r.Database is null);
            Assert.Contains(all, r => r.Database == "Eps");
            var ties = all.Where(r => r.Database is "TieA" or "TieB").ToArray();
            Assert.Equal(2, ties.Length);
            Assert.Equal(ties[0].CpuUs, ties[1].CpuUs);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Guard: the daily's own ceiling. The daily here stops four days short of the window's last midnight, so the plan stops there and the days after
    /// it stay on the hourly edge. A plan that read the daily up to the hourly ceiling would miss them.</summary>
    [Fact]
    public async Task DailyTrailingTheHourly_StopsTheMidAtTheDailyCeiling_AndStillAnswersTheSame()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct, t);
            await RefreshAllAsync(connection, ct, t, t.DEnd.AddDays(-4));
            await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

            var (plan, rawFrom) = await PlanAsync(pg, t, LiveClock.Now(), ct);
            Assert.NotNull(plan);
            Assert.Equal(t.DEnd.AddDays(-4), plan!.DEnd);
            await AssertMidSameAsOracleAsync(pg, t, plan, rawFrom, 200, 4);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Guards: no daily rollup rows at all (the plan is null, no ceiling), a daily that holds fewer than two whole days (null), and a daily with a
    /// hole day the hourly holds rows for (the hole probe refuses it). In each the tool answers from the hourly rollup alone.</summary>
    [Fact]
    public async Task DailyMissingShortOrWithAHole_FallsBackToTheHourlyRollup()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct, t);
            await RefreshAllAsync(connection, ct, t, null);
            /* The coverage is cached per data source, so each step reads the store through its own. */
            await using (var pg = NpgsqlDataSource.Create(scratch.ConnectionString))
            {
                /* The daily was never refreshed: no ceiling, no plan. */
                var (none, _) = await PlanAsync(pg, t, LiveClock.Now(), ct);
                Assert.Null(none);
            }

            /* One whole day in the daily: under MinMidDays. */
            await Refresh(connection, TimescaleSupport.QueryStoreStatsCorrectedDailyView, t.DStart, t.DStart.AddDays(1), ct);
            await using (var pg = NpgsqlDataSource.Create(scratch.ConnectionString))
            {
                var (oneDay, _) = await PlanAsync(pg, t, LiveClock.Now(), ct);
                Assert.Null(oneDay);
            }

            /* Days 1, 2 and 4 of the mid in the daily, day 3 missing: the plan stands (a ceiling past it) and the hole probe refuses it. */
            await Refresh(connection, TimescaleSupport.QueryStoreStatsCorrectedDailyView, t.DStart.AddDays(1), t.DStart.AddDays(2), ct);
            await Refresh(connection, TimescaleSupport.QueryStoreStatsCorrectedDailyView, t.DStart.AddDays(3), t.DStart.AddDays(4), ct);
            await using (var pg = NpgsqlDataSource.Create(scratch.ConnectionString))
            {
                var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
                var route = await QueryStoreTrendRouting.ResolveAsync(coverage, pg, ct);
                var plan = DarlingMcpQueryFleetSummaryTools.PlanQueryStoreDailyMid(coverage, t.Start, t.End, route.RawStartUtc, LiveClock.Now());
                Assert.NotNull(plan);
                Assert.False(await DarlingMcpQueryFleetSummaryTools.DailyHasNoHoleAsync(pg, plan!, ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Guard: the daily policy's re-read span. With the window ending a day ago the whole-day cut is the daily span before now, not the window
    /// end, so the days inside it stay on the hourly edge.</summary>
    [Fact]
    public async Task DaysInsideTheDailyRereadSpan_StayOnTheHourlyEdge()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct, t);
            await RefreshAllAsync(connection, ct, t, t.DEnd);
            await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);
            var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
            var route = await QueryStoreTrendRouting.ResolveAsync(coverage, pg, ct);

            /* The clock four days after the window's last midnight: every day of the daily is settled. Two days earlier: the last two are not. */
            var settledNow = t.DEnd.AddDays(4).AddHours(1);
            var all = DarlingMcpQueryFleetSummaryTools.PlanQueryStoreDailyMid(coverage, t.Start, t.End, route.RawStartUtc, settledNow);
            var held = DarlingMcpQueryFleetSummaryTools.PlanQueryStoreDailyMid(coverage, t.Start, t.End, route.RawStartUtc, settledNow.AddDays(-2));
            Assert.Equal(t.DEnd, all!.DEnd);
            Assert.Equal(t.DEnd.AddDays(-2), held!.DEnd);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>The tool end to end on the real clock (every day of the window is settled): the reply holds the same rows as the one-relation statement, and
    /// the whole days really come from the daily. The daily here is one change behind (a late snapshot was refreshed into the hourly rollups only,
    /// as a repair does), so the reply carries the daily's sums for the mid days and the hourly's for the edge days. The old route, which reads the
    /// hourly for every day, answers with the late snapshot in the mid and goes red here.</summary>
    [Fact]
    public async Task Tool_ReadsWholeDaysFromTheDaily_AndTheEdgeHoursFromTheHourly()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var connection = store.Connection;
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct, t);
            await RefreshAllAsync(connection, ct, t, t.DEnd);
            await InsertTailAsync(connection, ct, t);
            await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);
            var asOf = t.End.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture) + "Z";

            /* Before the late snapshot: the reply is the oracle's. */
            var (plan, rawFrom) = await PlanAsync(pg, t, LiveClock.Now(), ct);
            Assert.NotNull(plan);
            var oracle = await RunAsync(pg, DarlingMcpQueryFleetSummaryTools.BuildQueryStoreSql(true), t, rawFrom, 200, null, ct);
            Assert.Equal(Shape(oracle), await ReplyAsync(pg, t, asOf, ct));

            /* A late snapshot in a mid day of a pair the daily holds and of one it has never seen, refreshed into both hourly layers and not the daily. */
            var lateHour = t.DStart.AddDays(2).AddHours(9);
            await Snapshot(connection, ct, 0, "Alpha", NextInterval(), lateHour, lateHour.AddMinutes(40), 90, 5_000, 6_000);
            await Snapshot(connection, ct, 1, "Zeta", NextInterval(), lateHour, lateHour.AddMinutes(41), 40, 700, 800);
            await Refresh(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, lateHour, lateHour.AddHours(1), ct);
            await Refresh(connection, TimescaleSupport.QueryStoreStatsCorrectedHourlyView, lateHour, lateHour.AddHours(1), ct);

            var hourly = await RunAsync(pg, DarlingMcpQueryFleetSummaryTools.BuildQueryStoreSql(true), t, rawFrom, 200, null, ct);
            Assert.Contains(hourly, r => r.Database == "Zeta");
            var reply = await ReplyAsync(pg, t, asOf, ct);
            Assert.DoesNotContain(reply, r => r.Database == "Zeta");
            Assert.Equal(Shape(oracle), reply);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    private sealed record Shaped(string? Server, string? Database, long Executions, double CpuMs, double ElapsedMs, string? First, string? Last);

    private static Shaped[] Shape(IEnumerable<Row> rows)
        => rows.Select(r => new Shaped(r.Server, r.Database ?? "", r.Executions, Math.Round(r.CpuUs / 1000.0, 2), Math.Round(r.ElapsedUs / 1000.0, 2),
            DateTime.SpecifyKind(r.First, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture),
            DateTime.SpecifyKind(r.Last, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture))).ToArray();

    private static async Task<Shaped[]> ReplyAsync(NpgsqlDataSource pg, Timeline t, string asOf, CancellationToken ct)
    {
        var json = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(pg, Hash, t.HoursBack, as_of: asOf, limit: 200, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        Assert.False(doc.RootElement.TryGetProperty("error", out _), json);
        return doc.RootElement.GetProperty("servers").EnumerateArray()
            .Where(r => r.GetProperty("source").GetString() == DarlingMcpQueryFleetSummaryTools.QueryStoreSource)
            .Select(r => new Shaped(r.GetProperty("server_name").GetString(), r.GetProperty("database_name").GetString(), r.GetProperty("execution_count").GetInt64(),
                r.GetProperty("cpu_ms").GetDouble(), r.GetProperty("elapsed_ms").GetDouble(), r.GetProperty("first_hour").GetString(), r.GetProperty("last_hour").GetString()))
            .ToArray();
    }

    private static async Task<long> AnalyzeAsync(NpgsqlDataSource pg, string sql, Timeline t, DateTime rawFrom, DarlingMcpQueryFleetSummaryTools.DailyMidPlan? mid, CancellationToken ct)
    {
        await using var command = pg.CreateCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql);
        AddParameters(command, t, rawFrom, 200, mid);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var doc = JsonDocument.Parse(json);
        var plan = doc.RootElement[0].GetProperty("Plan").Clone();
        return ExplainBuffers.DataPages(plan);
    }

    /// <summary>A store at the shape the field read hurts on: several servers, the hash on one page per hourly row, so the days between the edges cost a
    /// page per row on the hourly rollup and a page per day on the daily. Red on the one-relation statement (the page counts are the same), green on the mid.</summary>
    [Fact]
    public async Task SevenDayWindow_ReadsAFractionOfThePages_OfTheHourlyOnlyStatement()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var c = store.Connection;
        var bodySucceeded = false;
        try
        {
            const int serverCount = 12;
            const int groups = 60;
            for (var i = 1; i <= serverCount; i++)
            {
                await DarlingMcpTestData.RegisterServerAsync(c, -858100 - i, "p8570q-" + i, ct);
            }

            /* #8725: no autovacuum or autoanalyze on the relations this test reads. The page floor below counts the old statement's reads of the hourly
               rollup's chunks, and those depend on the plan: with no statistics (as seeded) the planner walks the hash and bucket index with a heap
               fetch per row, 2,000-odd pages; once an autoanalyze has run on the chunks (CI's slower runners reach one between the refresh and the
               EXPLAIN) it picks a bitmap heap scan, which reads each page once, 335 pages, and the floor fails. The statistics sample is random, so the mid
               statement's edge read flips between a bitmap scan and a bucket-index walk from run to run too. A table option set on the hypertable is
               copied to the chunks created after it, so the option goes on before the seed and the rollups' refresh. */
            await using (var relations = new NpgsqlCommand(@"SELECT 'collect.query_store_stats'
                UNION ALL SELECT format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name) FROM timescaledb_information.continuous_aggregates", c))
            await using (var reader = await relations.ExecuteReaderAsync(ct))
            {
                var tables = new List<string>();
                while (await reader.ReadAsync(ct))
                {
                    tables.Add(reader.GetString(0));
                }

                await reader.CloseAsync();
                foreach (var table in tables)
                {
                    await Exec(c, $"ALTER TABLE {table} SET (autovacuum_enabled = false)", ct);
                }
            }

            var from = t.Start.Date;
            await Exec(c, $@"INSERT INTO collect.query_store_stats
                  (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash, query_id, plan_id, execution_type_desc, replica_role,
                   runtime_stats_interval_id, interval_start_time_utc, first_execution_time, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, interval_end_time_utc)
                SELECT 900000000::bigint + row_number() OVER (), h.ts + interval '10 minutes', -858100 - s, 'p8570q-' || s, 'db' || (g % 10), 'dbo.P',
                  CASE WHEN g = 1 THEN '{Hash}' ELSE '0x' || g END, g, g, 'Regular', 'PRIMARY', g, h.ts, h.ts, 1 + (extract(hour FROM h.ts))::int % 5, 2000 + g, 1000 + g, 900, 400, h.ts + interval '1 hour'
                FROM generate_series(TIMESTAMP '{from:yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{t.End.AddHours(-1):yyyy-MM-dd HH:mm:ss}', interval '1 hour') AS h(ts)
                CROSS JOIN generate_series(1, {serverCount}) s CROSS JOIN generate_series(1, {groups}) g", ct);
            await RefreshAllAsync(c, ct, t, t.DEnd);
            await Exec(c, "VACUUM (ANALYZE) collect.query_store_stats", ct);
            await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

            var (plan, rawFrom) = await PlanAsync(pg, t, LiveClock.Now(), ct);
            Assert.NotNull(plan);
            var old = await AnalyzeAsync(pg, DarlingMcpQueryFleetSummaryTools.BuildQueryStoreSql(true), t, rawFrom, null, ct);
            var mid = await AnalyzeAsync(pg, DarlingMcpQueryFleetSummaryTools.BuildQueryStoreMidSql(), t, rawFrom, plan, ct);
            Assert.True(old > 1_000, $"the seed must put the hash on about a page per hourly row: the one-relation statement read {old} pages");
            Assert.True(mid < old, $"one-relation statement {old} pages, mid route {mid} pages");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
