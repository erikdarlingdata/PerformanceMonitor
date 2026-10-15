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
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #5812 daily read from its executed plan, at CI scale (a few thousand interval rows): which rows each half reads when
/// some built days are stale and some are partial. Every assertion reads the plan's JSON nodes or the read's own rows, never
/// the plan text.
///
/// <para><b>Half-stale.</b> A late write to two closed days marks four days stale (each and the day after it) (<c>built_seq &lt;&gt; late_seq</c>) through the real
/// trigger. The read takes the totals of the current days only and reads the stale days' raw intervals; their totals, though
/// tampered with, are invisible to it, and its answer equals the interval-table read.</para>
///
/// <para><b>Fourteen days with three partial ones.</b> The closed days are built whole and the three open days partially. The
/// interval scan reads no more rows than start at or above the lowest watermark, it returns exactly the rows after each
/// day's watermark, and the daily scan returns exactly the built days' totals. Each is counted from the executed plan's
/// JSON nodes (rows returned plus rows a filter or recheck removed, times loops), exact with parallelism off, so the test
/// does not depend on how the planner prints its index condition (#5812, L7).</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it. */
public sealed class PlanRegressionDailyWatermarkLiveTests
{
    private const int ServerA = -5812401;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task HalfStale_TheReadTakesTotalsForCurrentDaysAndIntervalsForStaleDays_AndAStaleDaysTotalsAreInvisible()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5812 watermark read test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        const int queries = 200;
        var now = LiveClock.Now(); // #5608: whole UTC days from here
        var today = now.Date;
        var window = today.AddDays(-14);

        /* Closed days only (T-16 through T-3), so the live half has nothing of its own but the stale days. */
        await SeedAsync(connection, today.AddDays(-16), today.AddDays(-3), queries, ct);
        await CoverageAsync(connection, today.AddDays(-20), ct);

        var closed = Enumerable.Range(3, 12).Select(i => DateOnly.FromDateTime(today.AddDays(-i))).OrderBy(d => d).ToList();
        foreach (var day in closed)
        {
            Assert.Equal(queries * 2L, await PlanRegressionDaily.BuildDayAsync(connection, ServerA, day, now, ct));
        }

        /* A late write to two closed days through the real trigger: plan 1 of every third query runs more often. The
           trigger marks the row's first-execution day and the day after it (an interval may end there), so four days go
           stale: T-10, T-9, T-5 and T-4. */
        var late = new[] { DateOnly.FromDateTime(today.AddDays(-10)), DateOnly.FromDateTime(today.AddDays(-5)) };
        var stale = late.SelectMany(d => new[] { d, d.AddDays(1) }).OrderBy(d => d).ToArray();
        foreach (var day in late)
        {
            await ExecAsync(connection, @"
UPDATE collect.query_store_interval_latest
SET execution_count = execution_count + 50
WHERE server_id = @s
AND   last_execution_time::date = @d
AND   query_id % 3 = 0
AND   plan_id % 10 = 1", ct, ("s", ServerA), ("d", day));
        }

        Assert.Equal(stale.ToList(),
            await DaysAsync(connection, "SELECT day FROM collect.plan_regression_daily_built WHERE server_id = @s AND built_seq <> late_seq ORDER BY day", ct));
        var current = closed.Except(stale).ToList();
        Assert.Equal(8, current.Count);

        /* The stale days' totals change too (plan 1 made a thousand times dearer). A read that used them would lose those
           queries' regressions; the read must not see them. */
        await ExecAsync(connection,
            "UPDATE collect.plan_regression_daily SET cpu_us_sum = cpu_us_sum * 1000, dur_us_sum = dur_us_sum * 1000 WHERE server_id = @s AND day = ANY(@d) AND plan_id % 10 = 1",
            ct, ("s", ServerA), ("d", stale));
        await AnalyzeAsync(connection, ct);

        var built = await BuiltDaysAsync(connection, window, ct);
        Assert.Equal(current, built.Select(b => b.Day).ToList());

        var dailyRows = await DailyRowsAsync(connection, window, built, ct);
        var tableRows = await TableRowsAsync(connection, window, ct);
        Assert.NotEmpty(tableRows);
        Assert.Equal(tableRows, dailyRows);

        /* Not vacuous: the same read handed the stale days as if they were valid takes their tampered totals and differs. */
        var asIfValid = built.Concat(stale.Select(d => new PlanRegressionDaily.BuiltDay(d, null))).OrderBy(b => b.Day).ToList();
        Assert.NotEqual(tableRows, await DailyRowsAsync(connection, window, asIfValid, ct));

        /* The executed plan: the interval scan returns exactly the stale days' rows, the daily scan exactly the current
           days' totals. */
        var plan = await ExplainAsync(connection, window, built, ct);
        var live = ScansOf(plan, "query_store_interval_latest").ToList();
        var daily = ScansOf(plan, "plan_regression_daily").ToList();
        Assert.NotEmpty(live);
        Assert.NotEmpty(daily);

        var staleIntervalRows = await ScalarAsync(connection,
            "SELECT count(*) FROM collect.query_store_interval_latest WHERE server_id = @s AND last_execution_time::date = ANY(@d)", ct,
            ("s", ServerA), ("d", stale));
        var currentTotalsRows = await ScalarAsync(connection,
            "SELECT count(*) FROM collect.plan_regression_daily WHERE server_id = @s AND day = ANY(@d)", ct,
            ("s", ServerA), ("d", current.ToArray()));
        Assert.Equal(stale.Length * queries * 2L, staleIntervalRows);
        Assert.Equal(current.Count * queries * 2L, currentTotalsRows);
        Assert.Equal(staleIntervalRows, live.Sum(s => s.RowsReturned));
        Assert.Equal(currentTotalsRows, daily.Sum(s => s.RowsReturned));
    }

    [Fact]
    public async Task FourteenDays_ThreePartial_TheIntervalScanStartsAtTheLowestWatermark_AndEachHalfReturnsExactlyItsRows()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5812 watermark read test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        const int queries = 150;
        var today = LiveClock.Now().Date; // #5608
        var window = today.AddDays(-14);

        /* One busy server, T-16 through today, hours 0-21 of each day. */
        await SeedAsync(connection, today.AddDays(-16), today, queries, ct);
        await CoverageAsync(connection, today.AddDays(-20), ct);

        /* The open hour: rows that started in the hour after today's watermark, of queries no build has seen. Planted with
           the trigger off, before the builds: the watermarks here are fixed hours, not the service's two hours back. */
        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late", ct);
        var openHour = today.AddHours(6);
        await ExecAsync(connection, @"
INSERT INTO collect.query_store_interval_latest
(
    server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
    last_execution_time, is_forced_plan, force_failure_count
)
SELECT
    @s, 'wmdb', 100000 + q, (100000 + q) * 10 + 1, NULL, 9001,
    @h + (q % 50) * interval '1 minute',
    @h + interval '55 minutes',
    'open' || q, 'oq' || q, 10, 5000, 9000,
    @h + (q % 50) * interval '1 minute' + interval '2 minutes', false, 0
FROM generate_series(1, 400) AS q", ct, ("s", ServerA), ("h", openHour));
        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest ENABLE TRIGGER trg_plan_regression_daily_late", ct);

        /* The closed days whole; the three open days partial, each through its own watermark. T-2's is the lowest. */
        var watermarks = new Dictionary<DateOnly, DateTime>
        {
            [DateOnly.FromDateTime(today.AddDays(-2))] = today.AddDays(-2).AddHours(20),
            [DateOnly.FromDateTime(today.AddDays(-1))] = today.AddDays(-1).AddHours(18),
            [DateOnly.FromDateTime(today)] = today.AddHours(6),
        };
        for (var i = 14; i >= 3; i--)
        {
            await BuildAsync(connection, DateOnly.FromDateTime(today.AddDays(-i)), today.AddDays(-i + 1), ct);
        }

        foreach (var (day, through) in watermarks)
        {
            await BuildAsync(connection, day, through, ct);
        }

        await AnalyzeAsync(connection, ct);

        var built = await BuiltDaysAsync(connection, window, ct);
        Assert.Equal(15, built.Count);
        var lowest = watermarks[DateOnly.FromDateTime(today.AddDays(-2))];
        Assert.Equal(lowest, PgFactCollector.PlanRegressionLiveFloorWithWatermarks(PgFactCollector.PlanRegressionWindowFloor(window), built));

        /* The rows after each open day's watermark, counted with the watermarks spelled out, not through $3/$6. */
        var liveRows = await ScalarAsync(connection, @"
SELECT count(*)
FROM collect.query_store_interval_latest
WHERE server_id = @s
AND   (   (last_execution_time::date = @d2 AND first_execution_time >= @w2)
       OR (last_execution_time::date = @d1 AND first_execution_time >= @w1)
       OR (last_execution_time::date = @d0 AND first_execution_time >= @w0))", ct,
            ("s", ServerA),
            ("d2", today.AddDays(-2)), ("w2", lowest),
            ("d1", today.AddDays(-1)), ("w1", today.AddDays(-1).AddHours(18)),
            ("d0", today), ("w0", today.AddHours(6)));
        var afterLowest = await ScalarAsync(connection,
            "SELECT count(*) FROM collect.query_store_interval_latest WHERE server_id = @s AND first_execution_time >= @w", ct,
            ("s", ServerA), ("w", lowest));
        var windowRows = await ScalarAsync(connection,
            "SELECT count(*) FROM collect.query_store_interval_latest WHERE server_id = @s AND last_execution_time >= @m", ct,
            ("s", ServerA), ("m", window));
        var builtTotals = await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily WHERE server_id = @s", ct, ("s", ServerA));
        Assert.True(liveRows >= 400 && liveRows < afterLowest && afterLowest < windowRows / 3,
            $"seed shape: {liveRows} live rows, {afterLowest} at or above the lowest watermark, {windowRows} in the window");

        /* Sequential scans off so a CI-sized table still shows the index path the field store takes. What is asserted is
           the rows the plan's nodes read, not the text of its index condition (L7). */
        var plan = await ExplainAsync(connection, window, built, ct, noSeqScan: true);
        var live = ScansOf(plan, "query_store_interval_latest").ToList();
        var daily = ScansOf(plan, "plan_regression_daily").ToList();
        Assert.NotEmpty(live);
        Assert.NotEmpty(daily);

        Assert.Equal(liveRows, live.Sum(s => s.RowsReturned));
        Assert.True(live.Sum(s => s.RowsRead) <= afterLowest,
            $"the interval scan read {live.Sum(s => s.RowsRead)} rows; {afterLowest} start at or above the lowest watermark, {windowRows} are in the window");
        Assert.Equal(builtTotals, daily.Sum(s => s.RowsReturned));
    }

    /* ---------------------------------------------------------------------------------------------------------- */

    /// <summary>
    /// #5812 (fix A): the tick's two stamps that are not builds, on the store's clock. Server A is idle: its T - 2 is partial
    /// at 22:00 with nothing after it, and its today is partial an hour below the current watermark with nothing after it.
    /// Server B has a row in today's span. Before the plan, the idle advance stamps A's T - 2 whole and A's today at the
    /// watermark, and tomorrow is stamped for both with no totals. The plan then holds B's today only (A's today has no slot).
    /// After the tick, A's floor is today's watermark, and A's read (T - 2 whole) equals the interval-table read.
    /// </summary>
    [Fact]
    public async Task ATick_AdvancesAnIdleDayWithoutABuild_StampsTomorrow_AndTheFloorIsTodaysWatermark()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5812 stamp test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var now = await SettledStoreNowAsync(connection, ct);
        var today = now.Date;
        var todayDay = DateOnly.FromDateTime(today);
        var watermark = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified).AddHours(-PlanRegressionDaily.PartialWatermarkLagHours);
        var window = today.AddDays(-14);

        await SeedAsync(connection, today.AddDays(-16), today.AddDays(-2), 40, ct);
        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late", ct);
        await EnrollAsync(connection, ServerA, today.AddDays(-20), ct);
        await EnrollAsync(connection, ServerB, today.AddDays(-20), ct);

        for (var d = window.AddDays(-1); d < today.AddDays(-2); d = d.AddDays(1))
        {
            await BuildAsync(connection, DateOnly.FromDateTime(d), d.AddDays(1), ct);
        }

        var tMinus2 = DateOnly.FromDateTime(today.AddDays(-2));
        await BuildAsync(connection, tMinus2, today.AddDays(-2).AddHours(22), ct);
        await BuildAsync(connection, DateOnly.FromDateTime(today.AddDays(-1)), today, ct);
        await BuildAsync(connection, todayDay, watermark.AddHours(-1), ct);

        /* Server B: every day whole, today partial an hour below the watermark, and one row of today inside that hour. */
        await ExecAsync(connection, @"
INSERT INTO collect.plan_regression_daily_built (server_id, day, late_seq, built_seq, partial_seq, built_through, built_at, source_rows)
SELECT @s, g::date, 0, CASE WHEN g::date = @d THEN NULL ELSE 0 END, CASE WHEN g::date = @d THEN 0 END,
       CASE WHEN g::date = @d THEN @t END, @t, 0
FROM generate_series(@f::timestamp, @d::timestamp, interval '1 day') AS g",
            ct, ("s", ServerB), ("d", todayDay), ("f", window.AddDays(-1)), ("t", watermark.AddHours(-1)));
        await ExecAsync(connection, @"
INSERT INTO collect.query_store_interval_latest
(
    server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
    last_execution_time, is_forced_plan, force_failure_count
)
VALUES (@s, 'wmdb', 1, 1, NULL, 1, @f, @f + interval '35 minutes', 'hashb', 'qhb', 10, 100, 100, @f + interval '20 minutes', false, 0)",
            ct, ("s", ServerB), ("f", watermark.AddMinutes(-30)));

        var (advanced, stamped) = await PlanRegressionDaily.StampWithoutBuildAsync(connection, now, ct);
        Assert.Equal(2, advanced);
        Assert.Equal(2, stamped);

        var plan = await PlanRegressionDaily.PlanBuildsAsync(connection, now, ct);
        Assert.Equal(new[] { new PlanRegressionDaily.Build(ServerB, todayDay) }, plan.ToArray());

        await using (var source = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            var tick = await PlanRegressionDaily.RunTickAsync(source, now, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ct);
            Assert.Equal(new PlanRegressionDaily.TickResult(1, 0, 0, 0, 0, 0, 0), tick);
        }

        const string Stamp = "SELECT COALESCE(built_seq::text, '-') || '|' || COALESCE(partial_seq::text, '-') || '|' || late_seq || '|' || COALESCE(to_char(built_through, 'YYYY-MM-DD HH24:MI'), '-') FROM collect.plan_regression_daily_built WHERE server_id = @s AND day = @d";
        var w = watermark.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture);
        Assert.Equal($"-|0|0|{w}", await TextAsync(connection, Stamp, ct, ("s", ServerA), ("d", todayDay)));
        Assert.Equal("0|-|0|-", await TextAsync(connection, Stamp, ct, ("s", ServerA), ("d", tMinus2)));
        Assert.Equal($"-|0|0|{w}", await TextAsync(connection, Stamp, ct, ("s", ServerA), ("d", todayDay.AddDays(1))));
        Assert.Equal($"-|0|0|{w}", await TextAsync(connection, Stamp, ct, ("s", ServerB), ("d", todayDay)));
        Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily WHERE day = @d", ct, ("d", todayDay.AddDays(1))));
        Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily WHERE server_id = @s AND day = @d", ct, ("s", ServerB), ("d", todayDay)));

        var built = await BuiltDaysAsync(connection, window, ct);
        Assert.Null(built.Single(b => b.Day == tMinus2).Through);
        Assert.Equal(watermark, PgFactCollector.PlanRegressionLiveFloorWithWatermarks(PgFactCollector.PlanRegressionWindowFloor(window), built));
        var daily = await DailyRowsAsync(connection, window, built, ct);
        Assert.NotEmpty(daily);
        Assert.Equal(await TableRowsAsync(connection, window, ct), daily);
    }

    /// <summary>
    /// #5812 M3: the seam between the builder's watermark, the trigger's bound and the read. A real build of today on the
    /// store's clock stamps <c>built_through</c> two hours before the current hour. Set an hour back, the idle advance stamps it
    /// to the watermark again. A row of today that starts 30 minutes below that watermark, written through the real trigger,
    /// moves the day's <c>late_seq</c>, so the stamped day is stale, and the daily read equals the interval-table read. The
    /// open day is the watermark's own: today from 04:00 UTC, yesterday before 02:00 UTC (the watermark is 22:00 or 23:00 of
    /// yesterday, a partial day too), and only from 02:00 to 04:00 UTC, when the rows two hours below the watermark would
    /// cross midnight, does it skip.
    /// </summary>
    [Fact]
    public async Task ABuildOnTheStoreClock_StampsTwoHoursBeforeTheHour_AndAWriteBelowAnAdvancedWatermarkMarksTheDay()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5812 seam test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var now = await SettledStoreNowAsync(connection, ct, skipBeforeFour: false);
        var watermark = Convert.ToDateTime(await TextAsync(connection,
            "SELECT to_char(date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '2 hours', 'YYYY-MM-DD HH24:MI:SS')", ct), CultureInfo.InvariantCulture);
        Assert.SkipWhen(watermark.AddHours(-2).Date != watermark.Date,
            "From 02:00 to 04:00 UTC the rows two hours below the watermark would cross midnight; the #5812 seam test needs them inside the watermark's day.");

        /* The open day: the watermark's own (today, or yesterday before 02:00 UTC). Every day before it is built whole. */
        var today = watermark.Date;
        var todayDay = DateOnly.FromDateTime(today);
        var window = today.AddDays(-14);

        await SeedAsync(connection, today.AddDays(-16), today.AddDays(-1), 40, ct);
        await EnrollAsync(connection, ServerA, today.AddDays(-20), ct);
        for (var d = window.AddDays(-1); d < today; d = d.AddDays(1))
        {
            await BuildAsync(connection, DateOnly.FromDateTime(d), d.AddDays(1), ct);
        }

        /* Today's rows, all more than an hour below the watermark, planted with the trigger off (they are not late writes). */
        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late", ct);
        await TodayRowAsync(connection, 1, watermark.AddMinutes(-90), ct);
        await TodayRowAsync(connection, 2, watermark.AddHours(-2), ct);
        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest ENABLE TRIGGER trg_plan_regression_daily_late", ct);

        Assert.Equal(2L, await PlanRegressionDaily.BuildDayAsync(connection, ServerA, todayDay, now, ct));
        const string Through = "SELECT to_char(built_through, 'YYYY-MM-DD HH24:MI:SS') FROM collect.plan_regression_daily_built WHERE server_id = @s AND day = @d";
        Assert.Equal(await TextAsync(connection,
                "SELECT to_char(date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '2 hours', 'YYYY-MM-DD HH24:MI:SS')", ct),
            await TextAsync(connection, Through, ct, ("s", ServerA), ("d", todayDay)));

        /* As if built an hour ago: the idle advance (nothing in the last hour) stamps it to the watermark again. */
        await ExecAsync(connection, "UPDATE collect.plan_regression_daily_built SET built_through = @t WHERE server_id = @s AND day = @d",
            ct, ("s", ServerA), ("d", todayDay), ("t", watermark.AddHours(-1)));
        var (advanced, _) = await PlanRegressionDaily.StampWithoutBuildAsync(connection, now, ct);
        Assert.Equal(1, advanced);
        Assert.Equal(watermark.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            await TextAsync(connection, Through, ct, ("s", ServerA), ("d", todayDay)));
        var seq = await ScalarAsync(connection, "SELECT partial_seq FROM collect.plan_regression_daily_built WHERE server_id = @s AND day = @d",
            ct, ("s", ServerA), ("d", todayDay));
        Assert.Equal(seq, await ScalarAsync(connection, "SELECT late_seq FROM collect.plan_regression_daily_built WHERE server_id = @s AND day = @d",
            ct, ("s", ServerA), ("d", todayDay)));

        /* The late write, through the real trigger: 30 minutes below the watermark, so below the trigger's bound too. */
        await TodayRowAsync(connection, 3, watermark.AddMinutes(-30), ct);
        Assert.True(await ScalarAsync(connection, "SELECT late_seq FROM collect.plan_regression_daily_built WHERE server_id = @s AND day = @d",
            ct, ("s", ServerA), ("d", todayDay)) > seq);

        var built = await BuiltDaysAsync(connection, window, ct);
        Assert.DoesNotContain(built, b => b.Day == todayDay);
        Assert.Equal(await TableRowsAsync(connection, window, ct), await DailyRowsAsync(connection, window, built, ct));
    }

    /// <summary>
    /// #5812 (review round 1, the lock-free stamps): <see cref="PlanRegressionDaily.AdvanceIdleSql"/> and
    /// <see cref="PlanRegressionDaily.StampTomorrowSql"/> take no per-server lock. A builder holds server A's lock mid-build of
    /// a partial day (it has read the day's <c>late_seq</c> and its watermark, and aggregates next) while a second builder's
    /// tick runs both stamps; the idle advance restamps that day. The build then finishes. The day reads consistent: its
    /// totals plus the live rows after its watermark equal the interval-table read, so no row is counted twice or lost, and
    /// tomorrow, stamped or not, has no totals. The open day is the watermark's own, as in the seam test.
    /// </summary>
    [Fact]
    public async Task TheTicksStampsDuringAHeldPartialBuild_LeaveTheDayConsistent_AndTomorrowWithoutTotals()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5812 stamp race test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var now = await SettledStoreNowAsync(connection, ct, skipBeforeFour: false);
        var watermark = Convert.ToDateTime(await TextAsync(connection,
            "SELECT to_char(date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '2 hours', 'YYYY-MM-DD HH24:MI:SS')", ct), CultureInfo.InvariantCulture);
        Assert.SkipWhen(watermark.AddHours(-2).Date != watermark.Date,
            "From 02:00 to 04:00 UTC the rows two hours below the watermark would cross midnight; the #5812 stamp race test needs them inside the watermark's day.");

        var open = watermark.Date;
        var openDay = DateOnly.FromDateTime(open);
        var window = open.AddDays(-14);
        await SeedAsync(connection, open.AddDays(-16), open.AddDays(-1), 40, ct);
        await EnrollAsync(connection, ServerA, open.AddDays(-20), ct);
        for (var d = window.AddDays(-1); d < open; d = d.AddDays(1))
        {
            await BuildAsync(connection, DateOnly.FromDateTime(d), d.AddDays(1), ct);
        }

        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late", ct);
        await TodayRowAsync(connection, 1, watermark.AddMinutes(-90), ct);
        await TodayRowAsync(connection, 2, watermark.AddHours(-2), ct);
        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest ENABLE TRIGGER trg_plan_regression_daily_late", ct);

        /* A partial build, set an hour back, so the idle advance has a day to restamp. */
        Assert.Equal(2L, await PlanRegressionDaily.BuildDayAsync(connection, ServerA, openDay, now, ct));
        await ExecAsync(connection, "UPDATE collect.plan_regression_daily_built SET built_through = @t WHERE server_id = @s AND day = @d",
            ct, ("s", ServerA), ("d", openDay), ("t", watermark.AddHours(-1)));

        /* The builder holds server A's lock, has read late_seq and the watermark, and aggregates next: the tick's stamps run. */
        await using var builder = new NpgsqlConnection(scratch.ConnectionString);
        await builder.OpenAsync(ct);
        var advanced = -1;
        var built = await PlanRegressionDaily.BuildDayAsync(builder, ServerA, openDay, now, ct, async inner =>
        {
            (advanced, _) = await PlanRegressionDaily.StampWithoutBuildAsync(connection, now, inner);
        });
        Assert.Equal(1, advanced);
        Assert.Equal(2L, built);

        const string Row = "SELECT to_char(built_through, 'YYYY-MM-DD HH24:MI:SS') || ' ' || (partial_seq = late_seq) || ' ' || (built_seq IS NULL) FROM collect.plan_regression_daily_built WHERE server_id = @s AND day = @d";
        Assert.Equal(watermark.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + " true true",
            await TextAsync(connection, Row, ct, ("s", ServerA), ("d", openDay)));
        var days = await BuiltDaysAsync(connection, window, ct);
        Assert.Equal(watermark, days.Single(b => b.Day == openDay).Through);
        Assert.Equal(await TableRowsAsync(connection, window, ct), await DailyRowsAsync(connection, window, days, ct));
        Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily WHERE server_id = @s AND day > @d",
            ct, ("s", ServerA), ("d", DateOnly.FromDateTime(now.Date))));
    }

    /// <summary>
    /// #5812 M1: a partial build is invisible to code from before V185. A partial day fails V184's validity predicate
    /// (<c>built_seq = late_seq</c>), so a V183 reader reads it live. A V183 builder's whole build after it (the old stamp,
    /// which sets <c>built_seq</c> and leaves <c>partial_seq</c> and <c>built_through</c> as they were) reads as whole, and the
    /// daily read counts no row twice: it equals the interval-table read.
    /// </summary>
    [Fact]
    public async Task APartialDay_IsUnbuiltToTheV183Predicate_AndAV183WholeBuildAfterItReadsWhole_WithNoDoubleCount()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5812 pre-V185 test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var today = LiveClock.Now().Date; // #5608
        var window = today.AddDays(-14);
        await SeedAsync(connection, today.AddDays(-16), today.AddDays(-3), 40, ct);
        await CoverageAsync(connection, today.AddDays(-20), ct);
        for (var d = window.AddDays(-1); d < today; d = d.AddDays(1))
        {
            await BuildAsync(connection, DateOnly.FromDateTime(d), d.AddDays(1), ct);
        }

        var day = DateOnly.FromDateTime(today.AddDays(-3));
        var dayStart = today.AddDays(-3);
        await BuildAsync(connection, day, dayStart.AddHours(12), ct);

        const string V183Valid = "SELECT count(*) FROM collect.plan_regression_daily_built WHERE server_id = @s AND day = @d AND built_seq = late_seq";
        Assert.Equal(0L, await ScalarAsync(connection, V183Valid, ct, ("s", ServerA), ("d", day)));
        var built = await BuiltDaysAsync(connection, window, ct);
        Assert.Equal(dayStart.AddHours(12), built.Single(b => b.Day == day).Through);
        Assert.Equal(await TableRowsAsync(connection, window, ct), await DailyRowsAsync(connection, window, built, ct));

        /* A V183 builder rebuilds the day whole: its aggregate (the watermark at the next midnight) and its stamp. */
        await ExecAsync(connection, "DELETE FROM collect.plan_regression_daily WHERE server_id = @s AND day = @d", ct, ("s", ServerA), ("d", day));
        await using (var build = new NpgsqlCommand(PlanRegressionDaily.BuildDaySql, connection))
        {
            build.Parameters.AddWithValue(ServerA);
            build.Parameters.AddWithValue(NpgsqlDbType.Date, day);
            build.Parameters.AddWithValue(NpgsqlDbType.Timestamp, dayStart.AddDays(1));
            build.Parameters.AddWithValue(NpgsqlDbType.Timestamp, dayStart.AddDays(-1));
            build.Parameters.AddWithValue(NpgsqlDbType.Timestamp, dayStart.AddDays(1));
            await build.ExecuteScalarAsync(ct);
        }

        await ExecAsync(connection,
            "UPDATE collect.plan_regression_daily_built SET built_seq = late_seq, built_at = @d, source_rows = 0 WHERE server_id = @s AND day = @d",
            ct, ("s", ServerA), ("d", day));
        Assert.Equal(1L, await ScalarAsync(connection, V183Valid, ct, ("s", ServerA), ("d", day)));
        Assert.Equal(1L, await ScalarAsync(connection,
            "SELECT count(*) FROM collect.plan_regression_daily_built WHERE server_id = @s AND day = @d AND partial_seq = late_seq AND built_through IS NOT NULL",
            ct, ("s", ServerA), ("d", day)));

        built = await BuiltDaysAsync(connection, window, ct);
        Assert.Null(built.Single(b => b.Day == day).Through);
        Assert.Equal(await TableRowsAsync(connection, window, ct), await DailyRowsAsync(connection, window, built, ct));
    }

    private const int ServerB = -5812402;

    /// <summary>The store's clock (naive UTC), settled: skips before 04:00 UTC unless <paramref name="skipBeforeFour"/> is
    /// false (today's rows two hours below the watermark would start before midnight) and waits out an hour boundary less
    /// than three minutes off, so the test and the statements read one hour.</summary>
    private static async Task<DateTime> SettledStoreNowAsync(NpgsqlConnection connection, CancellationToken ct, bool skipBeforeFour = true)
    {
        const string Sql = "SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')";
        var now = Convert.ToDateTime(await TextAsync(connection, Sql, ct), CultureInfo.InvariantCulture);
        var wait = PlanRegressionDailyTriggerLiveTests.WaitPastTheHour(now, TimeSpan.FromMinutes(3));
        if (wait > TimeSpan.Zero)
        {
            await Task.Delay(wait, ct);
            now = Convert.ToDateTime(await TextAsync(connection, Sql, ct), CultureInfo.InvariantCulture);
        }

        Assert.SkipWhen(skipBeforeFour && now.Hour < 4, "Before 04:00 UTC the rows two hours below today's watermark would start before midnight; the #5812 stamp tests need them inside today.");
        return now;
    }

    /// <summary>Enrolls a server (enabled) with a coverage claim.</summary>
    private static async Task EnrollAsync(NpgsqlConnection connection, int serverId, DateTime filledSince, CancellationToken ct)
    {
        await ExecAsync(connection, "INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (@s, 'wm' || @s, TRUE) ON CONFLICT (server_id) DO NOTHING",
            ct, ("s", serverId));
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_interval_latest_coverage (server_id, filled_since, applied_through) VALUES (@s, @f, @f) ON CONFLICT (server_id) DO UPDATE SET filled_since = EXCLUDED.filled_since",
            ct, ("s", serverId), ("f", filledSince));
    }

    /// <summary>One interval row of server A's today, of its own query, 20 minutes long.</summary>
    private static Task TodayRowAsync(NpgsqlConnection connection, int query, DateTime first, CancellationToken ct) =>
        ExecAsync(connection, @"
INSERT INTO collect.query_store_interval_latest
(
    server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
    last_execution_time, is_forced_plan, force_failure_count
)
VALUES (@s, 'wmdb', 900000 + @q, 9000000 + @q, NULL, 900 + @q, @f, @f + interval '35 minutes', 'hasht' || @q, 'qht' || @q, 100, 1000, 1000, @f + interval '20 minutes', false, 0)",
            ct, ("s", ServerA), ("q", query), ("f", first));

    private static async Task<string> TextAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params (string Name, object Value)[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        AddParameters(command, parameters);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) ?? "";
    }

    /// <summary>Two-plan intervals for every day from <paramref name="first"/> through <paramref name="last"/>: query q
    /// starts at hour q % 22, plan 2 runs a minute after plan 1 and costs four times as much, so every query is a regression
    /// candidate. The late-row trigger is off for the load (a seed is not a late arrival).</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime first, DateTime last, int queries, CancellationToken ct)
    {
        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late", ct);
        await ExecAsync(connection, @"
INSERT INTO collect.query_store_interval_latest
(
    server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
    last_execution_time, is_forced_plan, force_failure_count
)
SELECT
    @s, 'wmdb', q, q * 10 + p, NULL, (q % 22) + 1,
    d.d + (q % 22) * interval '1 hour',
    d.d + (q % 22) * interval '1 hour' + interval '35 minutes',
    'hash' || (q * 10 + p), 'qh' || q, 100, 10000 * (1 + 3 * (p - 1)), 30000 * p,
    d.d + (q % 22) * interval '1 hour' + interval '30 minutes' + p * interval '1 minute', false, 0
FROM generate_series(@f::timestamp, @l::timestamp, interval '1 day') AS d (d)
CROSS JOIN generate_series(1, @q) AS q
CROSS JOIN generate_series(1, 2) AS p", ct, ("s", ServerA), ("f", first), ("l", last), ("q", queries));
        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest ENABLE TRIGGER trg_plan_regression_daily_late", ct);
    }

    private static Task CoverageAsync(NpgsqlConnection connection, DateTime filledSince, CancellationToken ct) =>
        ExecAsync(connection,
            "INSERT INTO collect.query_store_interval_latest_coverage (server_id, filled_since, applied_through) VALUES (@s, @f, @f) ON CONFLICT (server_id) DO UPDATE SET filled_since = EXCLUDED.filled_since",
            ct, ("s", ServerA), ("f", filledSince));

    /// <summary>One build with a chosen watermark: what the builder does for a day (minus its lock and timeout), stamped at
    /// the sequence read before it, so the day is valid.</summary>
    private static async Task BuildAsync(NpgsqlConnection connection, DateOnly day, DateTime through, CancellationToken ct)
    {
        var d = day.ToDateTime(TimeOnly.MinValue);
        await ExecAsync(connection, "INSERT INTO collect.plan_regression_daily_built (server_id, day) VALUES (@s, @d) ON CONFLICT DO NOTHING",
            ct, ("s", ServerA), ("d", day));
        var seq = await ScalarAsync(connection, "SELECT late_seq FROM collect.plan_regression_daily_built WHERE server_id = @s AND day = @d",
            ct, ("s", ServerA), ("d", day));
        await ExecAsync(connection, "DELETE FROM collect.plan_regression_daily WHERE server_id = @s AND day = @d", ct, ("s", ServerA), ("d", day));
        await using (var build = new NpgsqlCommand(PlanRegressionDaily.BuildDaySql, connection))
        {
            build.Parameters.AddWithValue(ServerA);
            build.Parameters.AddWithValue(NpgsqlDbType.Date, day);
            build.Parameters.AddWithValue(NpgsqlDbType.Timestamp, d.AddDays(1));
            build.Parameters.AddWithValue(NpgsqlDbType.Timestamp, d.AddDays(-1));
            build.Parameters.AddWithValue(NpgsqlDbType.Timestamp, through);
            await build.ExecuteScalarAsync(ct);
        }

        await ExecAsync(connection,
            "UPDATE collect.plan_regression_daily_built SET " + PartialAwareStamp + ", built_at = @d, source_rows = 0 WHERE server_id = @s AND day = @d",
            ct, ("s", ServerA), ("d", day), ("q", seq), ("t", through));
    }

    private static async Task AnalyzeAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, "ANALYZE collect.query_store_interval_latest", ct);
        await ExecAsync(connection, "ANALYZE collect.plan_regression_daily", ct);
        await ExecAsync(connection, "ANALYZE collect.plan_regression_daily_built", ct);
    }

    /// <summary>The valid built days with their watermarks, from the statement the collector runs.</summary>
    private static async Task<List<PlanRegressionDaily.BuiltDay>> BuiltDaysAsync(NpgsqlConnection connection, DateTime window, CancellationToken ct)
    {
        var days = new List<PlanRegressionDaily.BuiltDay>();
        await using var command = new NpgsqlCommand(PlanRegressionDaily.BuiltDaysSql, connection);
        command.Parameters.AddWithValue(ServerA);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, PgFactCollector.PlanRegressionWindowFloor(window));
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            days.Add(new PlanRegressionDaily.BuiltDay(reader.GetFieldValue<DateOnly>(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1)));
        }

        return days;
    }

    /// <summary>The daily read's parameters, bound the way the collector binds them.</summary>
    private static void BindDaily(NpgsqlCommand command, DateTime window, IReadOnlyCollection<PlanRegressionDaily.BuiltDay> built)
    {
        var floor = PgFactCollector.PlanRegressionWindowFloor(window);
        command.Parameters.AddWithValue(ServerA);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = built.Select(b => b.Day).ToArray() });
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, PgFactCollector.PlanRegressionLiveFloorWithWatermarks(floor, built));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor.AddDays(-1));
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = built.Select(b => b.ThroughOrDayEnd).ToArray() });
    }

    private static async Task<List<string>> DailyRowsAsync(
        NpgsqlConnection connection, DateTime window, IReadOnlyCollection<PlanRegressionDaily.BuiltDay> built, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(PgFactCollector.PlanRegressionDailySql, connection);
        BindDaily(command, window, built);
        return await RowsAsync(command, ct);
    }

    private static async Task<List<string>> TableRowsAsync(NpgsqlConnection connection, DateTime window, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(PgFactCollector.PlanRegressionTableSql, connection);
        command.Parameters.AddWithValue(ServerA);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, window);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, window.AddDays(-1));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, window.AddDays(-1));
        return await RowsAsync(command, ct);
    }

    /// <summary>The executed plan of the daily read, parallelism off so per-node counts are whole counts.</summary>
    private static async Task<JsonElement> ExplainAsync(
        NpgsqlConnection connection, DateTime window, IReadOnlyCollection<PlanRegressionDaily.BuiltDay> built, CancellationToken ct, bool noSeqScan = false)
    {
        await ExecAsync(connection, "SET max_parallel_workers_per_gather = 0", ct);
        await ExecAsync(connection, noSeqScan ? "SET enable_seqscan = off" : "RESET enable_seqscan", ct);
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + PgFactCollector.PlanRegressionDailySql, connection);
        BindDaily(command, window, built);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        return JsonDocument.Parse(json).RootElement[0].GetProperty("Plan").Clone();
    }

    private readonly record struct Scan(string Relation, string NodeType, double RowsRead, double RowsReturned);

    /// <summary>Every scan of a relation whose name contains <paramref name="relationPart"/>: rows returned times loops, rows
    /// read adding what the filter and recheck threw away.</summary>
    private static IEnumerable<Scan> ScansOf(JsonElement node, string relationPart)
    {
        if (node.TryGetProperty("Relation Name", out var relation)
            && relation.GetString()!.Contains(relationPart, StringComparison.Ordinal)
            && node.GetProperty("Node Type").GetString()!.Contains("Scan", StringComparison.Ordinal))
        {
            var loops = node.GetProperty("Actual Loops").GetDouble();
            var returned = node.GetProperty("Actual Rows").GetDouble() * loops;
            var removed = (Number(node, "Rows Removed by Filter") + Number(node, "Rows Removed by Index Recheck")) * loops;
            yield return new Scan(relation.GetString()!, node.GetProperty("Node Type").GetString()!, returned + removed, returned);
        }

        if (node.TryGetProperty("Plans", out var plans))
        {
            foreach (var child in plans.EnumerateArray())
            {
                foreach (var scan in ScansOf(child, relationPart)) yield return scan;
            }
        }
    }

    private static double Number(JsonElement node, string name) => node.TryGetProperty(name, out var v) ? v.GetDouble() : 0;

    private static async Task<List<DateOnly>> DaysAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var days = new List<DateOnly>();
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("s", ServerA);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) days.Add(reader.GetFieldValue<DateOnly>(0));
        return days;
    }

    private static async Task<List<string>> RowsAsync(NpgsqlCommand command, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                values[i] = reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? "";
            }

            rows.Add(string.Join("|", values));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params (string Name, object Value)[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        AddParameters(command, parameters);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    /// <summary>The stamp <c>PlanRegressionDaily.MarkBuiltSql</c> writes (#5812): a watermark at the next midnight is a whole build
    /// (<c>built_seq</c>), an earlier one a partial build (<c>partial_seq</c> and <c>built_through</c>, <c>built_seq</c> NULL).</summary>
    private const string PartialAwareStamp = "built_seq = CASE WHEN @t::timestamp >= (@d::date + 1)::timestamp THEN @q::bigint END, "
        + "partial_seq = CASE WHEN @t::timestamp >= (@d::date + 1)::timestamp THEN NULL ELSE @q::bigint END, "
        + "built_through = CASE WHEN @t::timestamp >= (@d::date + 1)::timestamp THEN NULL ELSE @t::timestamp END";

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params (string Name, object Value)[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        AddParameters(command, parameters);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Named parameters; a DateTime binds as timestamp (naive UTC), a DateOnly as date.</summary>
    private static void AddParameters(NpgsqlCommand command, (string Name, object Value)[] parameters)
    {
        foreach (var (name, value) in parameters)
        {
            switch (value)
            {
                case DateTime t:
                    command.Parameters.AddWithValue(name, NpgsqlDbType.Timestamp, t);
                    break;
                case DateOnly d:
                    command.Parameters.AddWithValue(name, NpgsqlDbType.Date, d);
                    break;
                default:
                    command.Parameters.AddWithValue(name, value);
                    break;
            }
        }
    }
}
