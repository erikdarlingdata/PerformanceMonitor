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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through QueryStoreClosedDayRollupLiveTests.SeedAsync (ScratchPostgres) and touches nothing on the shared one. */

/// <summary>
/// #9173: the plan-churn arm of <c>get_query_store_clutter</c> takes whole built hours from the hourly clutter rollups at the ends of a window
/// (and the whole days between them from the daily ones), and the rest from raw <c>query_store_stats</c>. Each test seeds raw rows, reads the
/// windows before anything is built (every statement is the raw one), builds with the closed-day tick and its hourly step, and reads again: the
/// reply must be the same rows, digit for digit. The seeding, the tick and the store come from <see cref="QueryStoreClosedDayRollupLiveTests"/>
/// and <see cref="QueryStoreClutterRollupLiveTests"/>.
/// </summary>
public sealed class QueryStoreClutterHourlyLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the #9173 hourly clutter rollup live test.";
    private const int DayRetention = 30;

    /// <summary>The retention raw <c>query_store_stats</c> is purged on, which the hourly rollups follow. Twelve days from the tick's clock reaches the first seeded day.</summary>
    private const int RawRetention = 12;
    private static int A => QueryStoreClosedDayRollupLiveTests.ServerId;
    private const int B = -9173001;

    /// <summary>The newest instant a seeded row can sit at: a midnight. TickNow is two days later, so every hour and day before it is closed.</summary>
    private static DateTime E => QueryStoreClosedDayRollupLiveTests.DataEnd;

    private static DateTime TickNow => QueryStoreClosedDayRollupLiveTests.TickNow;

    /// <summary>A window and whether the route takes whole hours for it (false: whole days alone, with raw for the minutes at the edges). Raw's window
    /// is <c>(start, end]</c>, so a start on a midnight leaves that day out of the days and takes the hours after it.</summary>
    private sealed record Window(string Label, DateTime Start, DateTime End, string? Database, bool Hours);

    /// <summary>The instants the hour-edge rows sit around: two hour boundaries and two UTC day boundaries.</summary>
    private static readonly DateTime HourEdgeA = E.AddDays(-3).AddHours(6);
    private static readonly DateTime HourEdgeB = E.AddDays(-1).AddHours(9);
    private static readonly DateTime DayEdgeA = E.AddDays(-4);
    private static readonly DateTime DayEdgeB = E.AddDays(-2);

    private static readonly Window[] Windows =
    [
        new("24 hours, both ends inside an hour", E.AddHours(-3).AddMinutes(-20).AddDays(-1), E.AddHours(-3).AddMinutes(-20), null, true),
        new("24 hours, both ends on the hour", E.AddHours(-5).AddDays(-1), E.AddHours(-5), null, true),
        new("24 hours, one database", E.AddHours(-3).AddMinutes(-20).AddDays(-1), E.AddHours(-3).AddMinutes(-20), "dbB", true),
        new("7 days, both ends inside an hour", E.AddHours(-3).AddMinutes(-20).AddDays(-7), E.AddHours(-3).AddMinutes(-20), null, true),
        new("7 days, both ends on the hour", E.AddDays(-1).AddHours(9).AddDays(-7), E.AddDays(-1).AddHours(9), null, true),
        new("on the hour at both ends", HourEdgeA, HourEdgeB, null, true),
        new("a minute before the hour at both ends", HourEdgeA.AddMinutes(-1), HourEdgeB.AddMinutes(-1), null, true),
        new("a minute after the hour at both ends", HourEdgeA.AddMinutes(1), HourEdgeB.AddMinutes(1), null, true),
        new("on a UTC day boundary at both ends", DayEdgeA, DayEdgeB, null, true),
        new("a minute before the day boundary and after it", DayEdgeA.AddMinutes(-1), DayEdgeB.AddMinutes(1), null, false),
        new("a minute after the day boundary and before it", DayEdgeA.AddMinutes(1), DayEdgeB.AddMinutes(-1), null, true),
        new("one whole day", E.AddDays(-1), E, null, true),
    ];

    /// <summary>
    /// Rows on and beside the edges the windows use: for each instant, a row on it, a microsecond either side and a minute either side, each a plan
    /// of its own (seen once), plus a second row on the instant in another database (a tie on <c>collection_time</c>). Raw's window is
    /// <c>(start, end]</c>, so the row on a start is out of the window and the row on an end is in it.
    /// </summary>
    private const string EdgeRowsSql = @"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash,
                                       query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time)
SELECT 880000000 + row_number() OVER (), x.at, @server_id, 'hourly-edge', x.db, 5000 + x.n, 50000 + x.n, '0xQ', '0xP', 'SELECT 1', 3, 1000, 800, x.at
FROM
(
    SELECT n, at, db
    FROM
    (
        SELECT row_number() OVER () AS n, e.at AS at, 'dbA' AS db
        FROM
        (
            SELECT t.instant + o.offset_by AS at
            FROM unnest(@instants) AS t(instant)
            CROSS JOIN unnest(ARRAY[interval '0', interval '1 microsecond', interval '-1 microsecond', interval '1 minute', interval '-1 minute']) AS o(offset_by)
        ) AS e
    ) AS r
    UNION ALL
    SELECT 1000 + row_number() OVER (), t.instant, 'dbB' FROM unnest(@instants) AS t(instant)
) AS x;";

    private static async Task SeedAsync(QueryStoreClosedDayRollupLiveTests.Store store, bool twoServers, CancellationToken ct)
    {
        await QueryStoreClutterRollupLiveTests.SeedServerAsync(store, A, E.AddDays(-10), 10, 48, 12, true, ct, oddRows: true);
        await ExecuteEdgeRowsAsync(store, ct);
        if (twoServers)
        {
            await QueryStoreClutterRollupLiveTests.SeedServerAsync(store, B, E.AddDays(-6), 6, 24, 8, true, ct, oddRows: true);
        }
    }

    private static async Task ExecuteEdgeRowsAsync(QueryStoreClosedDayRollupLiveTests.Store store, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(EdgeRowsSql, store.Connection);
        command.Parameters.AddWithValue("server_id", A);
        command.Parameters.Add(new NpgsqlParameter("instants", NpgsqlDbType.Array | NpgsqlDbType.Timestamp)
        {
            Value = new[] { HourEdgeA, HourEdgeB, DayEdgeA, DayEdgeB },
        });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string Lines(List<DarlingQueryStoreClutterReader.PlanChurnRow> rows) => string.Join("\n", rows.Select(r => r.ToString()));

    private static async Task<List<DarlingQueryStoreClutterReader.PlanChurnRow>> ReadAsync(
        QueryStoreClosedDayRollupLiveTests.Store store, int[] servers, Window w, CancellationToken ct) =>
        await DarlingQueryStoreClutterReader.GetPlanChurnAsync(store.Postgres, servers, w.Start, w.End, DatabaseFilter.One(w.Database), cancellationToken: ct);

    private static async Task<DarlingQueryStoreClutterReader.ClutterRoute?> RouteAsync(
        QueryStoreClosedDayRollupLiveTests.Store store, int[] servers, Window w, CancellationToken ct) =>
        await DarlingQueryStoreClutterReader.ResolveClutterRouteAsync(store.Postgres, servers, w.Start, w.End, ct);

    private static bool CoversHour(DarlingQueryStoreClutterReader.ClutterRoute? route, DateTime hour) =>
        route is { } r && r.Start <= hour && hour < r.End;

    /// <summary>The reply as raw alone answers it: every built hour is marked and every day's clutter stamp cleared for the read, so nothing is
    /// served from a rollup, and both are put back after.</summary>
    private static async Task<List<DarlingQueryStoreClutterReader.PlanChurnRow>> ReadRawAsync(
        QueryStoreClosedDayRollupLiveTests.Store store, int[] servers, Window w, CancellationToken ct)
    {
        var hours = new List<(int Server, DateTime Hour)>();
        await using (var mark = new NpgsqlCommand(
            "UPDATE collect.query_store_clutter_hour_built SET rebuild_needed = true WHERE NOT rebuild_needed RETURNING server_id, hour_utc", store.Connection))
        await using (var reader = await mark.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                hours.Add((reader.GetInt32(0), reader.GetDateTime(1)));
            }
        }

        var folded = new List<(int Server, DateTime Day)>();
        await using (var hide = new NpgsqlCommand(
            "UPDATE collect.query_store_clutter_day_built SET rebuild_needed = true WHERE NOT rebuild_needed RETURNING server_id, day_utc", store.Connection))
        await using (var reader = await hide.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                folded.Add((reader.GetInt32(0), reader.GetDateTime(1)));
            }
        }

        var days = new List<(int Server, DateTime Day)>();
        await using (var clear = new NpgsqlCommand(
            "UPDATE collect.query_store_closed_day_built SET clutter_built_at = NULL WHERE clutter_built_at = built_at RETURNING server_id, day_utc", store.Connection))
        await using (var reader = await clear.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                days.Add((reader.GetInt32(0), reader.GetDateTime(1)));
            }
        }

        try
        {
            Assert.Null(await RouteAsync(store, servers, w, ct));
            return await ReadAsync(store, servers, w, ct);
        }
        finally
        {
            await using var restoreHours = new NpgsqlCommand(
                "UPDATE collect.query_store_clutter_hour_built AS b SET rebuild_needed = false FROM unnest(@s::int[], @h::timestamp[]) AS u(s, h) WHERE b.server_id = u.s AND b.hour_utc = u.h", store.Connection);
            restoreHours.Parameters.Add(new NpgsqlParameter("s", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = hours.Select(x => x.Server).ToArray() });
            restoreHours.Parameters.Add(new NpgsqlParameter("h", NpgsqlDbType.Array | NpgsqlDbType.Timestamp) { Value = hours.Select(x => x.Hour).ToArray() });
            await restoreHours.ExecuteNonQueryAsync(ct);

            await using var restoreFolded = new NpgsqlCommand(
                "UPDATE collect.query_store_clutter_day_built AS b SET rebuild_needed = false FROM unnest(@s::int[], @d::date[]) AS u(s, d) WHERE b.server_id = u.s AND b.day_utc = u.d", store.Connection);
            restoreFolded.Parameters.Add(new NpgsqlParameter("s", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = folded.Select(x => x.Server).ToArray() });
            restoreFolded.Parameters.Add(new NpgsqlParameter("d", NpgsqlDbType.Array | NpgsqlDbType.Date) { Value = folded.Select(x => x.Day).ToArray() });
            await restoreFolded.ExecuteNonQueryAsync(ct);

            await using var restoreDays = new NpgsqlCommand(
                "UPDATE collect.query_store_closed_day_built AS b SET clutter_built_at = b.built_at FROM unnest(@s::int[], @d::date[]) AS u(s, d) WHERE b.server_id = u.s AND b.day_utc = u.d", store.Connection);
            restoreDays.Parameters.Add(new NpgsqlParameter("s", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = days.Select(x => x.Server).ToArray() });
            restoreDays.Parameters.Add(new NpgsqlParameter("d", NpgsqlDbType.Array | NpgsqlDbType.Date) { Value = days.Select(x => x.Day).ToArray() });
            await restoreDays.ExecuteNonQueryAsync(ct);
        }
    }

    private static Task<QueryStoreClosedDayRollup.TickResult> TickAsync(QueryStoreClosedDayRollupLiveTests.Store store, DateTime now, CancellationToken ct) =>
        QueryStoreClosedDayRollupLiveTests.TickAsync(store, DayRetention, ct, now, RawRetention);

    private static Task<long> CountAsync(QueryStoreClosedDayRollupLiveTests.Store store, string sql, CancellationToken ct, params (string Name, object Value)[] parameters) =>
        QueryStoreClosedDayRollupLiveTests.ScalarAsync<long>(store.Connection, sql, ct, parameters);

    private static bool CoversDay(DarlingQueryStoreClutterReader.ClutterRoute? route, DateTime day) =>
        route is { } r && r.DayStart <= day && day.AddDays(1) <= r.DayEnd;

    /// <summary>The newest instant before TickNow that has a whole day of closed hours and is before the grid's close of that day: the grid has built
    /// no day yet, so any clutter day the route serves was built from its hours (#9384).</summary>
    private static DateTime FoldNow => E.AddHours(2);

    private static readonly Window ThreeDaysToMidnight = new("three days to the midnight", E.AddDays(-3), E, null, true);

    private const string FoldDiffSql = @"
SELECT (SELECT count(*) FROM (
          (SELECT database_name, query_id, plan_id, first_seen, last_seen FROM collect.query_store_clutter_plan_daily WHERE server_id = @server_id AND day_utc = @day
           EXCEPT
           SELECT database_name, query_id, plan_id, MIN(first_seen), MAX(last_seen) FROM collect.query_store_clutter_plan_hourly
           WHERE server_id = @server_id AND hour_utc >= @day::timestamp AND hour_utc < @day::timestamp + interval '1 day' GROUP BY database_name, query_id, plan_id)
          UNION ALL
          (SELECT database_name, query_id, plan_id, MIN(first_seen), MAX(last_seen) FROM collect.query_store_clutter_plan_hourly
           WHERE server_id = @server_id AND hour_utc >= @day::timestamp AND hour_utc < @day::timestamp + interval '1 day' GROUP BY database_name, query_id, plan_id
           EXCEPT
           SELECT database_name, query_id, plan_id, first_seen, last_seen FROM collect.query_store_clutter_plan_daily WHERE server_id = @server_id AND day_utc = @day)) AS p)
     + (SELECT count(*) FROM (
          (SELECT database_name, collections FROM collect.query_store_clutter_collections_daily WHERE server_id = @server_id AND day_utc = @day
           EXCEPT
           SELECT database_name, SUM(collections) FROM collect.query_store_clutter_collections_hourly
           WHERE server_id = @server_id AND hour_utc >= @day::timestamp AND hour_utc < @day::timestamp + interval '1 day' GROUP BY database_name)
          UNION ALL
          (SELECT database_name, SUM(collections) FROM collect.query_store_clutter_collections_hourly
           WHERE server_id = @server_id AND hour_utc >= @day::timestamp AND hour_utc < @day::timestamp + interval '1 day' GROUP BY database_name
           EXCEPT
           SELECT database_name, collections FROM collect.query_store_clutter_collections_daily WHERE server_id = @server_id AND day_utc = @day)) AS c);";

    [Fact]
    public async Task ADayWhoseHoursAreAllBuiltAndUnmarked_IsBuiltAsAClutterDayAfterMidnight_FromItsHours_AndTheRouteServesItFromTheDay()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await SeedAsync(store, false, ct);
        var day = E.AddDays(-1);
        var fleet = new[] { A };

        Assert.Null(await RouteAsync(store, fleet, ThreeDaysToMidnight, ct));
        var before = Lines(await ReadAsync(store, fleet, ThreeDaysToMidnight, ct));

        var tick = await TickAsync(store, FoldNow, ct);
        Assert.True(tick.ClutterDaysFolded > 0, "the tick folded no clutter day");

        /* The grid has built nothing yet (its close is more than a day after midnight), so the day is served from the fold alone. */
        Assert.Equal(0L, await CountAsync(store,
            "SELECT count(*) FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND day_utc = @day AND clutter_built_at = built_at", ct, ("server_id", A), ("day", DateOnly.FromDateTime(day))));
        Assert.Equal(1L, await CountAsync(store,
            "SELECT count(*) FROM collect.query_store_clutter_day_built WHERE server_id = @server_id AND day_utc = @day AND NOT rebuild_needed", ct, ("server_id", A), ("day", DateOnly.FromDateTime(day))));
        Assert.True(await CountAsync(store,
            "SELECT count(*) FROM collect.query_store_clutter_plan_daily WHERE server_id = @server_id AND day_utc = @day", ct, ("server_id", A), ("day", DateOnly.FromDateTime(day))) > 0);
        Assert.Equal(0L, await CountAsync(store, FoldDiffSql, ct, ("server_id", A), ("day", DateOnly.FromDateTime(day))));

        Assert.True(CoversDay(await RouteAsync(store, fleet, ThreeDaysToMidnight, ct), day), "the route does not serve the folded day from the day rows");
        Assert.Equal(before, Lines(await ReadAsync(store, fleet, ThreeDaysToMidnight, ct)));
        Assert.Equal(before, Lines(await ReadRawAsync(store, fleet, ThreeDaysToMidnight, ct)));
    }

    [Fact]
    public async Task ALateRowMarksItsHourAndItsClutterDay_AndBothRebuildToIncludeIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await SeedAsync(store, false, ct);
        var fleet = new[] { A };
        var day = DateOnly.FromDateTime(E.AddDays(-1));
        await TickAsync(store, FoldNow, ct);
        Assert.True(CoversDay(await RouteAsync(store, fleet, ThreeDaysToMidnight, ct), E.AddDays(-1)), "the day is not served before the late row");

        var lateAt = E.AddHours(-12).AddMinutes(33);
        var lateHour = QueryStoreClutterHourly.FloorHour(lateAt);
        await using (var tx = await store.Connection.BeginTransactionAsync(ct))
        {
            await using (var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time)
VALUES (990384001, @at, @server_id, 'late', 'dbA', 7778, 77780, '0xQ', '0xP', 'SELECT 1', 1, 1, 1, @at)", store.Connection, tx))
            {
                insert.Parameters.AddWithValue("at", lateAt);
                insert.Parameters.AddWithValue("server_id", A);
                await insert.ExecuteNonQueryAsync(ct);
            }

            Assert.True(await QueryStoreClosedDayRollup.TryMarkLateCollectionDayAsync(store.Connection, tx, A, lateAt, FoldNow, 30, null, ct));
            await tx.CommitAsync(ct);
        }

        Assert.Equal(1L, await CountAsync(store,
            "SELECT count(*) FROM collect.query_store_clutter_hour_built WHERE server_id = @server_id AND rebuild_needed AND hour_utc = @hour", ct, ("server_id", A), ("hour", lateHour)));
        Assert.Equal(1L, await CountAsync(store,
            "SELECT count(*) FROM collect.query_store_clutter_day_built WHERE server_id = @server_id AND rebuild_needed AND day_utc = @day", ct, ("server_id", A), ("day", day)));

        /* The marked day is read from the hours and raw: the reply has the row, and it is the raw reply. */
        var raw = Lines(await ReadRawAsync(store, fleet, ThreeDaysToMidnight, ct));
        Assert.False(CoversDay(await RouteAsync(store, fleet, ThreeDaysToMidnight, ct), E.AddDays(-1)), "the read still takes the marked day from the day rows");
        Assert.Equal(raw, Lines(await ReadAsync(store, fleet, ThreeDaysToMidnight, ct)));

        var tick = await TickAsync(store, FoldNow.AddHours(1), ct);
        Assert.True(tick.HoursBuilt >= 1);
        Assert.True(tick.ClutterDaysFolded >= 1, "the marked day was not folded again");
        Assert.Equal(0L, await CountAsync(store,
            "SELECT count(*) FROM collect.query_store_clutter_day_built WHERE server_id = @server_id AND rebuild_needed", ct, ("server_id", A)));
        Assert.Equal(1L, await CountAsync(store,
            "SELECT count(*) FROM collect.query_store_clutter_plan_daily WHERE server_id = @server_id AND day_utc = @day AND plan_id = 77780", ct, ("server_id", A), ("day", day)));
        Assert.True(CoversDay(await RouteAsync(store, fleet, ThreeDaysToMidnight, ct), E.AddDays(-1)), "the rebuilt day is not served");
        Assert.Equal(raw, Lines(await ReadAsync(store, fleet, ThreeDaysToMidnight, ct)));
    }

    [Fact]
    public async Task ThePlanChurnAnswer_IsTheSameBeforeAndAfterADayCloses_AndTheHourlyTailShrinksToTheOpenDay()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await SeedAsync(store, false, ct);
        var fleet = new[] { A };
        var day = E.AddDays(-1);

        /* Hours first, with the day still open: a day that has not been folded is marked, so the route has the day as hours. */
        await TickAsync(store, FoldNow, ct);
        await ExecuteAsync(store, "UPDATE collect.query_store_clutter_day_built SET rebuild_needed = true WHERE day_utc = @day", ct, ("day", DateOnly.FromDateTime(day)));
        var open = await RouteAsync(store, fleet, ThreeDaysToMidnight, ct);
        Assert.False(CoversDay(open, day));
        Assert.True(CoversHour(open, day.AddHours(5)), "the open day is not served from its hours");
        var answerOpen = Lines(await ReadAsync(store, fleet, ThreeDaysToMidnight, ct));

        var tick = await TickAsync(store, FoldNow.AddHours(1), ct);
        Assert.True(tick.ClutterDaysFolded >= 1);
        var closed = await RouteAsync(store, fleet, ThreeDaysToMidnight, ct);
        Assert.True(CoversDay(closed, day), "the closed day is not served from the day rows");
        Assert.Equal(answerOpen, Lines(await ReadAsync(store, fleet, ThreeDaysToMidnight, ct)));
        Assert.True(closed!.Value.DayEnd > open!.Value.DayEnd || closed.Value.DayEnd >= E, "the day rows did not grow to take the closed day");
    }

    private static async Task ExecuteAsync(QueryStoreClosedDayRollupLiveTests.Store store, string sql, CancellationToken ct, params (string Name, object Value)[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, store.Connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task TheRoutedReply_EqualsTheRawReply_AtADayAndAWeek_AndAroundHourAndDayBoundaries_ForOneServerAndTwo()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await SeedAsync(store, true, ct);

        var fleets = new (string Label, int[] Servers)[] { ("one server", [A]), ("two servers", [A, B]) };

        var before = new Dictionary<string, string>();
        foreach (var f in fleets)
        {
            foreach (var w in Windows)
            {
                Assert.Null(await RouteAsync(store, f.Servers, w, ct));
                var rows = await ReadAsync(store, f.Servers, w, ct);
                Assert.True(rows.Count > 0, $"{f.Label} / {w.Label}: the raw read returned no rows");
                before[f.Label + w.Label] = Lines(rows);
            }
        }

        /* the seed has the shapes the test names: a NULL query, a tie on collection_time, a plan seen once */
        Assert.True(await CountAsync(store, "SELECT count(*) FROM collect.query_store_stats WHERE server_id = @server_id AND query_id IS NULL AND plan_id IS NULL", ct, ("server_id", A)) > 0);
        Assert.True(await CountAsync(store,
            "SELECT count(*) FROM (SELECT 1 FROM collect.query_store_stats WHERE server_id = @server_id GROUP BY collection_time, database_name, query_id, plan_id HAVING count(*) > 1) AS t", ct, ("server_id", A)) > 0);

        var tick = await TickAsync(store, TickNow, ct);
        Assert.True(tick.HoursBuilt > 0, "the tick built no hour");

        foreach (var f in fleets)
        {
            foreach (var w in Windows)
            {
                var route = await RouteAsync(store, f.Servers, w, ct);
                Assert.True(route is not null, $"{f.Label} / {w.Label}: nothing is served from the rollups, so the comparison would prove nothing");
                Assert.True(w.Hours == route!.Value.Hours || f.Servers.Length > 1, $"{f.Label} / {w.Label}: expected hours {w.Hours}, route says {route.Value.Hours}");
                Assert.Equal(before[f.Label + w.Label], Lines(await ReadAsync(store, f.Servers, w, ct)));
            }
        }

        /* A 24-hour window has no whole day, so its answer is hours and raw edges alone. */
        var day = await RouteAsync(store, [A], Windows[0], ct);
        Assert.Equal(day!.Value.Start, day.Value.DayStart);
        Assert.Equal(day.Value.Start, day.Value.DayEnd);
    }

    [Fact]
    public async Task ALateRowInABuiltHour_SendsTheReadToRawUntilTheNextTickRebuildsTheHour_WithTheRawAnswerThroughout()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await SeedAsync(store, false, ct);
        var fleet = new[] { A };
        var windows = new[] { Windows[0], Windows[3] };

        await TickAsync(store, TickNow, ct);
        var lateAt = E.AddHours(-12).AddMinutes(33);
        var lateHour = QueryStoreClutterHourly.FloorHour(lateAt);
        var routedBefore = new List<string>();
        foreach (var w in windows)
        {
            Assert.True(CoversHour(await RouteAsync(store, fleet, w, ct), lateHour), $"{w.Label}: the hour is not served before the late row");
            routedBefore.Add(Lines(await ReadAsync(store, fleet, w, ct)));
        }

        /* A backfilled batch in a built hour, written as the collector's batch is: its rows and the mark in one transaction. */
        await using (var tx = await store.Connection.BeginTransactionAsync(ct))
        {
            await using (var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time)
VALUES (990173001, @at, @server_id, 'late', 'dbA', 7777, 77770, '0xQ', '0xP', 'SELECT 1', 1, 1, 1, @at),
       (990173002, @at, @server_id, 'late', 'dbB', 101, 1010, '0xQ', '0xP', 'SELECT 1', 1, 1, 1, @at)", store.Connection, tx))
            {
                insert.Parameters.AddWithValue("at", lateAt);
                insert.Parameters.AddWithValue("server_id", A);
                await insert.ExecuteNonQueryAsync(ct);
            }

            Assert.True(await QueryStoreClosedDayRollup.TryMarkLateCollectionDayAsync(store.Connection, tx, A, lateAt, TickNow, 30, null, ct));
            await tx.CommitAsync(ct);
        }

        Assert.Equal(1L, await CountAsync(store,
            "SELECT count(*) FROM collect.query_store_clutter_hour_built WHERE server_id = @server_id AND rebuild_needed AND hour_utc = @hour", ct, ("server_id", A), ("hour", lateHour)));

        /* The marked hour is read from raw: the reply has the row, and it is the raw reply. */
        var raw = new List<string>();
        for (var i = 0; i < windows.Length; i++)
        {
            var w = windows[i];
            raw.Add(Lines(await ReadRawAsync(store, fleet, w, ct)));
            Assert.NotEqual(routedBefore[i], raw[i]);
            Assert.False(CoversHour(await RouteAsync(store, fleet, w, ct), lateHour), $"{w.Label}: the read still takes the marked hour from the hourly rollup");
            Assert.Equal(raw[i], Lines(await ReadAsync(store, fleet, w, ct)));
        }

        /* The next tick builds the hour again, and the hourly rollup holds the late rows. */
        var tick = await TickAsync(store, TickNow.AddHours(1), ct);
        Assert.True(tick.HoursBuilt >= 1);
        Assert.Equal(0L, await CountAsync(store, "SELECT count(*) FROM collect.query_store_clutter_hour_built WHERE server_id = @server_id AND rebuild_needed", ct, ("server_id", A)));
        Assert.Equal(1L, await CountAsync(store,
            "SELECT count(*) FROM collect.query_store_clutter_plan_hourly WHERE server_id = @server_id AND hour_utc = @hour AND plan_id = 77770", ct, ("server_id", A), ("hour", lateHour)));
        for (var i = 0; i < windows.Length; i++)
        {
            Assert.True(CoversHour(await RouteAsync(store, fleet, windows[i], ct), lateHour), $"{windows[i].Label}: the rebuilt hour is not served");
            Assert.Equal(raw[i], Lines(await ReadAsync(store, fleet, windows[i], ct)));
        }
    }

    [Fact]
    public async Task TheHourlyPrune_FollowsTheRetentionRawIsPurgedOn_AndTheReadStaysEqualToRawAcrossTheCut()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await SeedAsync(store, false, ct);
        var fleet = new[] { A };

        const string Oldest = @"
SELECT LEAST((SELECT min(hour_utc) FROM collect.query_store_clutter_plan_hourly),
             (SELECT min(hour_utc) FROM collect.query_store_clutter_collections_hourly),
             (SELECT min(hour_utc) FROM collect.query_store_clutter_hour_built))";
        const string Newest = @"
SELECT GREATEST((SELECT max(hour_utc) FROM collect.query_store_clutter_plan_hourly),
                (SELECT max(hour_utc) FROM collect.query_store_clutter_hour_built))";

        /* The first whole hour inside raw's retention is the oldest hour kept. */
        await TickAsync(store, TickNow, ct);
        var firstEdge = TickNow.AddDays(-RawRetention).AddHours(1);
        Assert.Equal(firstEdge, await QueryStoreClosedDayRollupLiveTests.ScalarAsync<DateTime>(store.Connection, Oldest, ct));
        var wide = new Window("reaches before the cut", E.AddDays(-8).AddHours(-3), E.AddHours(-3).AddMinutes(-20), null, true);
        var whole = Lines(await ReadAsync(store, fleet, wide, ct));
        var across = new Window("hours around the cut", E.AddDays(-7).AddHours(-5).AddMinutes(-20), E.AddDays(-7).AddHours(10).AddMinutes(20), null, true);
        var routedAcrossBefore = await RouteAsync(store, fleet, across, ct);
        var acrossWhole = Lines(await ReadAsync(store, fleet, across, ct));

        /* Three days later raw's window has moved on: every hour before the new edge goes, from all three tables, and the rest stay. */
        var later = TickNow.AddDays(3);
        var removed = (await TickAsync(store, later, ct)).HoursRemoved;
        Assert.True(removed > 0, "the prune removed no rollup row");
        var secondEdge = later.AddDays(-RawRetention).AddHours(1);
        Assert.Equal(secondEdge, await QueryStoreClosedDayRollupLiveTests.ScalarAsync<DateTime>(store.Connection, Oldest, ct));
        Assert.Equal(0L, await CountAsync(store,
            "SELECT (SELECT count(*) FROM collect.query_store_clutter_plan_hourly WHERE hour_utc < @edge) + (SELECT count(*) FROM collect.query_store_clutter_collections_hourly WHERE hour_utc < @edge)", ct,
            ("edge", secondEdge)));
        Assert.True(await QueryStoreClosedDayRollupLiveTests.ScalarAsync<DateTime>(store.Connection, Newest, ct) > secondEdge);

        /* A window reaching before the cut reads the removed hours from raw, which still holds them, and the answer does not change. */
        var raw = Lines(await ReadRawAsync(store, fleet, wide, ct));
        Assert.Equal(whole, raw);
        Assert.Equal(raw, Lines(await ReadAsync(store, fleet, wide, ct)));

        /* A window with no whole day around the cut: its hours before the edge were served before the prune and are not now. */
        Assert.True(CoversHour(routedAcrossBefore, secondEdge.AddHours(-1)));
        Assert.False(CoversHour(await RouteAsync(store, fleet, across, ct), secondEdge.AddHours(-1)));
        var acrossRaw = Lines(await ReadRawAsync(store, fleet, across, ct));
        Assert.Equal(acrossWhole, acrossRaw);
        Assert.Equal(acrossRaw, Lines(await ReadAsync(store, fleet, across, ct)));
    }

    [Fact]
    public async Task AStaleHourHeartbeat_KeepsTheReadOffTheHourlyRows_UntilTheNextTickMarksWhatAnOlderBuildWroteWithoutAMark()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await SeedAsync(store, false, ct);
        var fleet = new[] { A };
        var hourOnly = Windows[0];
        var dayAndHours = Windows[3];

        var tickAt = E.AddHours(-2);
        await TickAsync(store, tickAt, ct);
        Assert.True((await RouteAsync(store, fleet, hourOnly, ct))!.Value.Hours);
        Assert.True((await RouteAsync(store, fleet, dayAndHours, ct))!.Value.Hours);
        var served = Lines(await ReadAsync(store, fleet, dayAndHours, ct));

        /* An older build (V285 to V288) keeps the day heartbeat fresh and knows nothing of the hourly tables: its row lands in a built hour with no
           mark, and the hour heartbeat stays where the last current tick left it. */
        var aged = E.AddHours(-14);
        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection,
            "UPDATE collect.query_store_closed_day_tick SET hour_ticked_at = @aged WHERE server_id = @server_id", ct, ("server_id", A), ("aged", aged));
        var lateAt = E.AddHours(-12).AddMinutes(33);
        var lateHour = QueryStoreClutterHourly.FloorHour(lateAt);
        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection, @"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time)
VALUES (990173101, @at, @server_id, 'older', 'dbA', 7778, 77780, '0xQ', '0xP', 'SELECT 1', 1, 1, 1, @at)", ct, ("at", lateAt), ("server_id", A));
        Assert.Equal(0L, await CountAsync(store, "SELECT count(*) FROM collect.query_store_clutter_hour_built WHERE server_id = @server_id AND rebuild_needed", ct, ("server_id", A)));

        /* The hour heartbeat is stale, so no hour is served; the days still are, and the answer is raw's, which has the row. */
        Assert.Null(await RouteAsync(store, fleet, hourOnly, ct));
        var weekRoute = await RouteAsync(store, fleet, dayAndHours, ct);
        Assert.True(weekRoute is { Hours: false });
        var viaRaw = new List<string>();
        foreach (var w in new[] { hourOnly, dayAndHours })
        {
            viaRaw.Add(Lines(await ReadRawAsync(store, fleet, w, ct)));
            Assert.Equal(viaRaw[^1], Lines(await ReadAsync(store, fleet, w, ct)));
        }

        Assert.NotEqual(served, viaRaw[1]);

        /* A current service's tick: the heartbeat had aged, so it marks the hours of rows collected since and builds them again. */
        var next = await TickAsync(store, tickAt.AddHours(1), ct);
        Assert.True(next.HoursBuilt >= 1);
        Assert.Equal(0L, await CountAsync(store, "SELECT count(*) FROM collect.query_store_clutter_hour_built WHERE server_id = @server_id AND rebuild_needed", ct, ("server_id", A)));
        Assert.Equal(1L, await CountAsync(store,
            "SELECT count(*) FROM collect.query_store_clutter_plan_hourly WHERE server_id = @server_id AND hour_utc = @hour AND plan_id = 77780", ct, ("server_id", A), ("hour", lateHour)));
        Assert.True(CoversHour(await RouteAsync(store, fleet, hourOnly, ct), lateHour));
        Assert.Equal(viaRaw[0], Lines(await ReadAsync(store, fleet, hourOnly, ct)));
        Assert.Equal(viaRaw[1], Lines(await ReadAsync(store, fleet, dayAndHours, ct)));
    }

    private const string OldestRawRow = "SELECT min(collection_time) FROM collect.query_store_stats WHERE server_id = @server_id";

    [Fact]
    public async Task TheFirstFill_BuildsNoHourBeforeTheOldestRawRow_AndTheRouteHoldsThatSpanAsNotCollected()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);

        /* Raw starts three days before the tick's newest data, well after the retention edge (twelve days back): the shape of a new store. */
        await QueryStoreClutterRollupLiveTests.SeedServerAsync(store, A, E.AddDays(-3), 3, 48, 12, true, ct);
        var floor = await QueryStoreClosedDayRollupLiveTests.ScalarAsync<DateTime>(store.Connection, OldestRawRow, ct, ("server_id", A));
        var firstHour = QueryStoreClutterHourly.CeilHour(floor);
        Assert.True(firstHour > TickNow.AddDays(-RawRetention).AddDays(3), "the raw rows do not start well after the retention edge, so the test would prove nothing");

        var tick = await TickAsync(store, TickNow, ct);
        Assert.True(tick.HoursBuilt > 0, "the tick built no hour");

        Assert.Equal(0L, await CountAsync(store,
            "SELECT count(*) FROM collect.query_store_clutter_hour_built WHERE server_id = @server_id AND hour_utc < @first", ct, ("server_id", A), ("first", firstHour)));
        Assert.Equal(firstHour, await QueryStoreClosedDayRollupLiveTests.ScalarAsync<DateTime>(store.Connection,
            "SELECT min(hour_utc) FROM collect.query_store_clutter_hour_built WHERE server_id = @server_id", ct, ("server_id", A)));

        /* The span before the oldest raw row is not served from the hourly rollup, so the route does not answer "no plans" for it. */
        var served = await QueryStoreClutterHourly.ResolveServedHoursAsync(store.Connection, null, A, E.AddDays(-9), firstHour, 30, ct);
        Assert.NotNull(served);
        Assert.Empty(served!);
    }

    [Fact]
    public async Task EmptyHourAndDayStampsBeforeTheOldestRawRow_AreRemovedOnTheNextTick_AndStampsWithRowsStay()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await QueryStoreClutterRollupLiveTests.SeedServerAsync(store, A, E.AddDays(-3), 3, 48, 12, true, ct);
        await TickAsync(store, TickNow, ct);
        var floor = await QueryStoreClosedDayRollupLiveTests.ScalarAsync<DateTime>(store.Connection, OldestRawRow, ct, ("server_id", A));

        /* What an earlier build left: empty hours and days stamped built before raw held them, beside an hour and a day that hold clutter rows. */
        var emptyHours = Enumerable.Range(0, 6).Select(i => E.AddDays(-8).AddHours(i)).ToArray();
        var rowHour = E.AddDays(-7).AddHours(2);
        var emptyDays = new[] { E.AddDays(-9), E.AddDays(-8) };
        var rowDay = E.AddDays(-7);
        foreach (var hour in emptyHours.Append(rowHour))
        {
            await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection,
                "INSERT INTO collect.query_store_clutter_hour_built (server_id, hour_utc, built_at, rebuild_needed) VALUES (@server_id, @hour, @at, false)", ct,
                ("server_id", A), ("hour", hour), ("at", TickNow));
        }

        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection,
            "INSERT INTO collect.query_store_clutter_plan_hourly (server_id, hour_utc, database_name, query_id, plan_id, first_seen, last_seen) VALUES (@server_id, @hour, 'dbA', 1, 1, @hour, @hour)", ct,
            ("server_id", A), ("hour", rowHour));
        foreach (var day in emptyDays.Append(rowDay))
        {
            await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection, @"
INSERT INTO collect.query_store_closed_day_built (server_id, day_utc, built_at, source_rows, clutter_built_at) VALUES (@server_id, @day, @at, 0, @at)
ON CONFLICT (server_id, day_utc) DO UPDATE SET built_at = EXCLUDED.built_at, clutter_built_at = EXCLUDED.clutter_built_at, rebuild_needed = false", ct,
                ("server_id", A), ("day", day), ("at", TickNow));
        }

        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection,
            "INSERT INTO collect.query_store_clutter_plan_daily (server_id, day_utc, database_name, query_id, plan_id, first_seen, last_seen) VALUES (@server_id, @day, 'dbA', 1, 1, @day, @day)", ct,
            ("server_id", A), ("day", rowDay));

        const string StampsAtOrAfterFloor = "SELECT count(*) FROM collect.query_store_clutter_hour_built WHERE server_id = @server_id AND hour_utc >= @first";
        var firstHour = QueryStoreClutterHourly.CeilHour(floor);
        var keptBefore = await CountAsync(store, StampsAtOrAfterFloor, ct, ("server_id", A), ("first", firstHour));
        Assert.True(keptBefore > 0);

        await TickAsync(store, TickNow, ct);

        foreach (var hour in emptyHours)
        {
            Assert.Equal(0L, await CountAsync(store,
                "SELECT count(*) FROM collect.query_store_clutter_hour_built WHERE server_id = @server_id AND hour_utc = @hour", ct, ("server_id", A), ("hour", hour)));
        }

        Assert.Equal(1L, await CountAsync(store,
            "SELECT count(*) FROM collect.query_store_clutter_hour_built WHERE server_id = @server_id AND hour_utc = @hour", ct, ("server_id", A), ("hour", rowHour)));
        Assert.Equal(keptBefore, await CountAsync(store, StampsAtOrAfterFloor, ct, ("server_id", A), ("first", firstHour)));

        const string DayStamped = "SELECT count(*) FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND day_utc = @day AND clutter_built_at = built_at";
        foreach (var day in emptyDays)
        {
            Assert.Equal(0L, await CountAsync(store, DayStamped, ct, ("server_id", A), ("day", day)));
        }

        Assert.Equal(1L, await CountAsync(store, DayStamped, ct, ("server_id", A), ("day", rowDay)));
    }

    [Fact]
    public async Task TheClutterWindowBlock_ReportsPlanChurnCoverageFromTheRollups_NotFromRawAlone_AndNamesTheWaitArmWhenItIsTheOneCut()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(QueryStoreClosedDayRollupLiveTests.BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await QueryStoreClutterRollupLiveTests.SeedServerAsync(store, A, E.AddDays(-10), 10, 48, 12, true, ct);
        await TickAsync(store, TickNow, ct);

        /* Raw keeps four days and the rollups keep the older ones: the purge drops the raw rows, and the built days and hours stay served. */
        var rawFloor = E.AddDays(-4);
        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection,
            "DELETE FROM collect.query_store_stats WHERE server_id = @server_id AND collection_time < @floor", ct, ("server_id", A), ("floor", rawFloor));

        /* A 144-hour window that starts half an hour before a midnight: the rollup days start at that midnight, so every arm reaches the window start. */
        var now = E.AddMinutes(-30);
        var asOf = DateTime.SpecifyKind(now, DateTimeKind.Utc).ToString("o", System.Globalization.CultureInfo.InvariantCulture);
        var fleet = new[] { A };
        Assert.True(await DarlingQueryStoreClutterReader.GetPlanChurnCoverageStartAsync(store.Postgres, fleet, now.AddHours(-144), now, ct) is { } coverage && coverage < rawFloor,
            "plan churn coverage does not start before raw's oldest row, so the rollups are not read");

        var json = await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(store.Postgres, "rollup-9111", hours_back: 144, as_of: asOf);
        using (var doc = System.Text.Json.JsonDocument.Parse(json))
        {
            var window = doc.RootElement.GetProperty("window");
            Assert.False(window.GetProperty("window_truncated").GetBoolean(), json);
            Assert.Equal(System.Text.Json.JsonValueKind.Null, window.GetProperty("truncation_note").ValueKind);
            Assert.True(window.GetProperty("effective_hours_back").GetDouble() >= 143.0, json);
        }

        /* A wait row 2 hours back is the oldest wait_stats row: the wait arm holds 2 hours of the window and the note names it. */
        await QueryStoreClosedDayRollupLiveTests.ExecuteAsync(store.Connection, @"
INSERT INTO collect.wait_stats (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
VALUES (990382001, @at, @server_id, 'rollup-9111', 'QDS_ASYNC_QUEUE', 10, 100, 0, 1, 1, 0, 60)", ct, ("at", now.AddHours(-2)), ("server_id", A));
        json = await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(store.Postgres, "rollup-9111", hours_back: 144, as_of: asOf);
        using (var doc = System.Text.Json.JsonDocument.Parse(json))
        {
            var window = doc.RootElement.GetProperty("window");
            Assert.True(window.GetProperty("window_truncated").GetBoolean(), json);
            Assert.Equal(2.0, window.GetProperty("effective_hours_back").GetDouble());
            var note = window.GetProperty("truncation_note").GetString()!;
            Assert.Contains("wait arm", note, StringComparison.Ordinal);
        }
    }
}
