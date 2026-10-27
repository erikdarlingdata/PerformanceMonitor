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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6800 part 2, step 3b: V221's day table <c>collect.query_store_hash_daily</c>, its ledger and the validity rule, against a real
/// PostgreSQL store (<c>DARLING_TEST_PG</c>). The seed is the shared 30-hour one anchored so that one whole UTC day (yesterday less one
/// day) is built, with a partial day on each side. Facts: a built day equals the sum of its 24 hourly rows and a direct total of the
/// wide table; a late write into any hour of a built day invalidates that server's day at once and only that server's, and an hour
/// rebuild alone does not make it valid again, the day rebuild does; a new pair invalidates (also one that leaves the sum alone); an
/// incomplete day is never built or valid; the backfill builds the newest days first within the cap; the cleanup removes whole days only;
/// a failing day build is recorded and backs off; the rung runs twice on a fresh store and on one upgraded from V220.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it, so it never touches the shared database and cannot race the
   live collection. */
public sealed class QueryStoreHashDailyLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the V221 hash-daily facts.";

    private const string Columns = "server_id, query_hash, wide_rows, mem_max, ec_sum, cpu_wsum, dur_wsum, reads_wsum, writes_wsum";

    private static string At(DateTime value) => ComposeStampLiveSupport.At(value);

    private static Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        ComposeStampLiveSupport.ExecAsync(connection, sql, ct);

    private static Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        ComposeStampLiveSupport.CountAsync(connection, sql, ct);

    /// <summary>Yesterday (UTC midnight) from the anchored clock. The seed's hour is Y + 3 h, so hours (Y - 2 d) 21:00 to Y 00:00 are built: the day Y - 1 d is whole.</summary>
    private static DateTime Yesterday() => LiveClock.AnchoredEnd().Date;

    private static DateTime FullDay() => Yesterday().AddDays(-1);

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection, NpgsqlDataSource Source, DateTime HourNow)> ArrangeAsync(CancellationToken ct)
    {
        var arranged = await ComposeStampLiveSupport.ArrangeAsync(ct, build: false, at: Yesterday().AddHours(3));
        await ExecAsync(arranged.Connection, @"
UPDATE collect.query_store_interval_wide
SET avg_logical_io_reads = CASE WHEN query_id % 4 = 0 THEN NULL ELSE ((query_id * 7 + plan_id) % 1000) + 1 END,
    avg_logical_io_writes = CASE WHEN query_id % 3 = 0 THEN NULL ELSE query_id * 5 + (plan_id % 7) END,
    max_query_max_used_memory = CASE WHEN query_id % 5 = 0 THEN NULL ELSE (runtime_stats_interval_id % 5000) + 1 END", ct);
        Assert.Equal(28, await ComposeStampLiveSupport.BuildAllAsync(arranged.Source, ct, ComposeStampLiveSupport.ClockWithin(arranged.HourNow)));
        return arranged;
    }

    private static string Fmt(IEnumerable<(DateTime Day, int ServerId)> valid) =>
        string.Join(",", valid.Select(v => v.Day.ToString("MMdd", CultureInfo.InvariantCulture) + "/" + v.ServerId));

    private static async Task<string> ValidAsync(NpgsqlConnection connection, int[] servers, DateTime from, DateTime to, CancellationToken ct) =>
        Fmt(await QueryStoreHashDaily.ReadValidDaysAsync(connection, servers, from, to, ct));

    private static string Expect(DateTime day, params int[] servers) =>
        string.Join(",", servers.Select(s => day.ToString("MMdd", CultureInfo.InvariantCulture) + "/" + s));

    /// <summary>Rows where the day table and <paramref name="direct"/> (a SELECT of <see cref="Columns"/>) differ, EXCEPT both ways.</summary>
    private static Task<long> MismatchesAsync(NpgsqlConnection connection, DateTime day, string direct, CancellationToken ct) => CountAsync(connection, $@"
WITH direct AS ({direct}),
stored AS (SELECT {Columns} FROM collect.query_store_hash_daily WHERE day = TIMESTAMP '{At(day)}')
SELECT (SELECT count(*) FROM (SELECT * FROM direct EXCEPT SELECT * FROM stored) AS a)
     + (SELECT count(*) FROM (SELECT * FROM stored EXCEPT SELECT * FROM direct) AS b)", ct);

    private static string FromHourly(DateTime day) => $@"
SELECT server_id, query_hash, sum(wide_rows)::bigint, max(mem_max), sum(ec_sum), sum(cpu_wsum), sum(dur_wsum), sum(reads_wsum), sum(writes_wsum)
FROM collect.query_store_hash_hourly WHERE hour >= TIMESTAMP '{At(day)}' AND hour < TIMESTAMP '{At(day.AddDays(1))}' GROUP BY server_id, query_hash";

    private static string FromWide(DateTime day) => $@"
SELECT w.server_id, w.query_hash, count(*), max(w.max_query_max_used_memory), sum(w.execution_count), sum(w.avg_cpu_time_us * w.execution_count),
       sum(w.avg_duration_us * w.execution_count), sum(w.avg_logical_io_reads * w.execution_count), sum(w.avg_logical_io_writes * w.execution_count)
FROM collect.query_store_interval_wide AS w WHERE w.collection_time >= TIMESTAMP '{At(day)}' AND w.collection_time < TIMESTAMP '{At(day.AddDays(1))}'
GROUP BY w.server_id, w.query_hash";

    private static DateTime Clock(DateTime hourNow) => ComposeStampLiveSupport.ClockWithin(hourNow);

    [Fact]
    public async Task ABuiltDaysRows_EqualTheSumOfItsHourlyRows_AndADirectTotalOfTheWideTable()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var day = FullDay();

            /* Teeth: the day is there, covers three servers and the NULL-hash group, holds a sum past a bigint, and the other days are not built. */
            Assert.Equal(3 * 6, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_hash_daily WHERE day = TIMESTAMP '{At(day)}'", ct));
            Assert.Equal(3, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_hash_daily WHERE day = TIMESTAMP '{At(day)}' AND query_hash IS NULL", ct));
            Assert.True(await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_daily WHERE dur_wsum > 9223372036854775807", ct) > 0);
            Assert.Equal(1, await CountAsync(connection, "SELECT count(DISTINCT day) FROM collect.query_store_hash_daily", ct));
            Assert.Equal(3, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_daily_built WHERE pair_count = 24", ct));
            Assert.Equal(3, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_daily_built", ct));

            Assert.Equal(0, await MismatchesAsync(connection, day, FromHourly(day), ct));
            Assert.Equal(0, await MismatchesAsync(connection, day, FromWide(day), ct));

            /* Valid for the whole day, for every server, in a window that also holds the two partial days. */
            Assert.Equal(Expect(day, 1, 2, 3), await ValidAsync(connection, new[] { 1, 2, 3 }, day.AddDays(-1), day.AddDays(3), ct));
            /* A window that cuts the day is not a whole day; a server that is not asked for is not returned. */
            Assert.Equal(string.Empty, await ValidAsync(connection, new[] { 1, 2, 3 }, day.AddHours(1), day.AddDays(1), ct));
            Assert.Equal(string.Empty, await ValidAsync(connection, new[] { 1, 2, 3 }, day, day.AddDays(1).AddHours(-1), ct));
            Assert.Equal(Expect(day, 2), await ValidAsync(connection, new[] { 2 }, day, day.AddDays(1), ct));

            /* A tick with nothing due builds nothing and changes nothing. */
            var tick = await ComposeStampLiveSupport.TickAtAsync(source, Clock(hourNow), ct);
            Assert.Equal(0, tick.DaysBuilt + tick.DaysFailed);
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task ALateWriteIntoAnyHourOfABuiltDay_InvalidatesThatServersDayAtOnce_AndOnlyTheDayRebuildMakesItValidAgain()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var day = FullDay();
            var servers = new[] { 1, 2, 3 };
            Assert.Equal(Expect(day, 1, 2, 3), await ValidAsync(connection, servers, day, day.AddDays(1), ct));

            /* The first hour, a middle hour and the last hour of the day, one server each. At once: no builder has run. */
            await ComposeStampLiveSupport.InsertRowAsync(connection, 1, day.AddMinutes(30), 910001, ct);
            Assert.Equal(Expect(day, 2, 3), await ValidAsync(connection, servers, day, day.AddDays(1), ct));
            await ComposeStampLiveSupport.InsertRowAsync(connection, 2, day.AddHours(11).AddMinutes(30), 910002, ct);
            Assert.Equal(Expect(day, 3), await ValidAsync(connection, servers, day, day.AddDays(1), ct));
            await ComposeStampLiveSupport.InsertRowAsync(connection, 3, day.AddHours(23).AddMinutes(30), 910003, ct);
            Assert.Equal(string.Empty, await ValidAsync(connection, servers, day, day.AddDays(1), ct));
            Assert.True(await MismatchesAsync(connection, day, FromWide(day), ct) > 0, "the late rows must make the stored day differ from the wide table");

            /* One server's late write on a fresh pass: rebuild only the hours (not the day) and the day stays invalid for that server. */
            foreach (var hour in new[] { day, day.AddHours(11), day.AddHours(23) })
            {
                Assert.NotNull(await QueryStoreComposeStamp.BuildHourAsync(connection, hour, Clock(hourNow), ct));
            }

            Assert.Equal(0, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE hash_built_seq IS DISTINCT FROM late_seq AND hour IN (SELECT hour FROM collect.query_store_compose_stamp_hours)", ct));
            Assert.Equal(string.Empty, await ValidAsync(connection, servers, day, day.AddDays(1), ct));

            /* The day rebuild: valid again, with the new values. */
            Assert.NotNull(await QueryStoreHashDaily.BuildDayAsync(connection, day, Clock(hourNow), ct));
            Assert.Equal(Expect(day, 1, 2, 3), await ValidAsync(connection, servers, day, day.AddDays(1), ct));
            Assert.Equal(0, await MismatchesAsync(connection, day, FromWide(day), ct));
            Assert.Equal(0, await MismatchesAsync(connection, day, FromHourly(day), ct));
            Assert.Equal(3, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_hash_daily WHERE day = TIMESTAMP '{At(day)}' AND query_hash = 'hashX'", ct));

            /* The same through the tick: a second late write is rebuilt by one tick, hour then day, and the other days are untouched. */
            await ComposeStampLiveSupport.InsertRowAsync(connection, 2, day.AddHours(5).AddMinutes(30), 910004, ct);
            Assert.Equal(Expect(day, 1, 3), await ValidAsync(connection, servers, day, day.AddDays(1), ct));
            var tick = await ComposeStampLiveSupport.TickAtAsync(source, Clock(hourNow), ct);
            Assert.Equal(1, tick.BuiltStale);
            Assert.Equal(1, tick.DaysBuilt);
            Assert.Equal(Expect(day, 1, 2, 3), await ValidAsync(connection, servers, day, day.AddDays(1), ct));
            Assert.Equal(0, await MismatchesAsync(connection, day, FromWide(day), ct));
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task ANewPair_InvalidatesTheDay_AlsoOneThatLeavesTheSumOfTheSeqsAlone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var day = FullDay();

            /* A server that has rows in only one hour of the day (before the build), then gets a late row in an hour it had no pair in. */
            await ExecAsync(connection, "INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (4, 'srv4', TRUE) ON CONFLICT (server_id) DO NOTHING", ct);
            await ComposeStampLiveSupport.InsertRowAsync(connection, 4, day.AddHours(5).AddMinutes(30), 920001, ct);
            Assert.Equal(Expect(day, 1, 2, 3), await ValidAsync(connection, new[] { 1, 2, 3, 4 }, day, day.AddDays(1), ct)); /* the new pair of server 4 is stale: not valid */
            var tick = await ComposeStampLiveSupport.TickAtAsync(source, Clock(hourNow), ct);
            Assert.Equal(1, tick.BuiltStale);
            Assert.Equal(1, tick.DaysBuilt);
            Assert.Equal(Expect(day, 1, 2, 3, 4), await ValidAsync(connection, new[] { 1, 2, 3, 4 }, day, day.AddDays(1), ct));
            Assert.Equal(1, await CountAsync(connection, $"SELECT pair_count FROM collect.query_store_hash_daily_built WHERE day = TIMESTAMP '{At(day)}' AND server_id = 4", ct));

            await ComposeStampLiveSupport.InsertRowAsync(connection, 4, day.AddHours(7).AddMinutes(30), 920002, ct);
            Assert.Equal(Expect(day, 1, 2, 3), await ValidAsync(connection, new[] { 1, 2, 3, 4 }, day, day.AddDays(1), ct));
            tick = await ComposeStampLiveSupport.TickAtAsync(source, Clock(hourNow), ct);
            Assert.Equal(1, tick.DaysBuilt);
            Assert.Equal(Expect(day, 1, 2, 3, 4), await ValidAsync(connection, new[] { 1, 2, 3, 4 }, day, day.AddDays(1), ct));
            Assert.Equal(2, await CountAsync(connection, $"SELECT pair_count FROM collect.query_store_hash_daily_built WHERE day = TIMESTAMP '{At(day)}' AND server_id = 4", ct));
            Assert.Equal(0, await MismatchesAsync(connection, day, FromWide(day), ct));

            /* The count alone: a pair added with hash_built_seq 0 (a rebuild creates one for a server with stamps and no pair) leaves the sum
               of the seqs where it was, and only the count tells. */
            await ExecAsync(connection, $"INSERT INTO collect.query_store_compose_stamp_built (server_id, hour, late_seq, built_seq, hash_built_seq) VALUES (4, TIMESTAMP '{At(day.AddHours(9))}', 0, 0, 0)", ct);
            Assert.Equal(Expect(day, 1, 2, 3), await ValidAsync(connection, new[] { 1, 2, 3, 4 }, day, day.AddDays(1), ct));
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task AnIncompleteDay_IsNeverBuiltAndNeverValid_AndAHourRemovedFromAFullDayInvalidatesIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var day = FullDay();
            var before = Yesterday().AddDays(-2);
            var after = Yesterday();

            /* The two partial days of the seed (3 hours and 1 hour built): no rows, no ledger rows, not valid, and a direct build refuses. */
            Assert.Equal(3, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp_hours WHERE hour >= TIMESTAMP '{At(before)}' AND hour < TIMESTAMP '{At(before.AddDays(1))}'", ct));
            Assert.Equal(1, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp_hours WHERE hour >= TIMESTAMP '{At(after)}' AND hour < TIMESTAMP '{At(after.AddDays(1))}'", ct));
            Assert.Equal(0, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_hash_daily WHERE day IN (TIMESTAMP '{At(before)}', TIMESTAMP '{At(after)}')", ct));
            Assert.Equal(Expect(day, 1, 2, 3), await ValidAsync(connection, new[] { 1, 2, 3 }, before, after.AddDays(1), ct));
            Assert.Null(await QueryStoreHashDaily.BuildDayAsync(connection, before, Clock(hourNow), ct));
            Assert.Null(await QueryStoreHashDaily.BuildDayAsync(connection, after, Clock(hourNow), ct));
            Assert.Equal(0, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_hash_daily WHERE day IN (TIMESTAMP '{At(before)}', TIMESTAMP '{At(after)}')", ct));
            Assert.Empty(await QueryStoreHashDaily.PlanAsync(connection, ct, Clock(hourNow)));

            /* A whole day loses an hour from the hours table (as a cleanup would): it is not valid at once, and not planned for a rebuild either. */
            await ExecAsync(connection, $"DELETE FROM collect.query_store_compose_stamp_hours WHERE hour = TIMESTAMP '{At(day.AddHours(10))}'", ct);
            Assert.Equal(string.Empty, await ValidAsync(connection, new[] { 1, 2, 3 }, day, day.AddDays(1), ct));
            Assert.Null(await QueryStoreHashDaily.BuildDayAsync(connection, day, Clock(hourNow), ct));
            Assert.Equal(18, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_hash_daily WHERE day = TIMESTAMP '{At(day)}'", ct));

            /* The hour is built again with nothing changed: no pair moved, so the day's fingerprint still holds and it is valid with no day rebuild. */
            var tick = await ComposeStampLiveSupport.TickAtAsync(source, Clock(hourNow), ct);
            Assert.Equal(1, tick.BuiltMissing);
            Assert.Equal(0, tick.DaysBuilt);
            Assert.Equal(Expect(day, 1, 2, 3), await ValidAsync(connection, new[] { 1, 2, 3 }, day, day.AddDays(1), ct));
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    /// <summary>Hours, pairs (late_seq and hash_built_seq both 2) and two hashes of hourly rows for <paramref name="days"/> whole days from <paramref name="first"/>, for servers 1 and 2.</summary>
    private static Task SeedSyntheticDaysAsync(NpgsqlConnection connection, DateTime first, int days, CancellationToken ct) => ExecAsync(connection, $@"
INSERT INTO collect.query_store_compose_stamp_hours (hour, built_at)
SELECT TIMESTAMP '{At(first)}' + g * interval '1 hour', TIMESTAMP '{At(first)}' FROM generate_series(0, {days * 24 - 1}) AS g;
INSERT INTO collect.query_store_compose_stamp_built (server_id, hour, late_seq, built_seq, hash_built_seq, built_at, source_rows, stamp_rows)
SELECT s, TIMESTAMP '{At(first)}' + g * interval '1 hour', 2, 2, 2, TIMESTAMP '{At(first)}', 1, 1 FROM generate_series(0, {days * 24 - 1}) AS g CROSS JOIN generate_series(1, 2) AS s;
INSERT INTO collect.query_store_hash_hourly (hour, wide_rows, mem_max, server_id, query_hash, ec_sum, cpu_wsum, dur_wsum, reads_wsum, writes_wsum)
SELECT TIMESTAMP '{At(first)}' + g * interval '1 hour', 3, g, s, h, g + 1, (g + 1) * 10, (g + 1) * 100, (g + 1) * 7, (g + 1) * 3
FROM generate_series(0, {days * 24 - 1}) AS g CROSS JOIN generate_series(1, 2) AS s CROSS JOIN (VALUES ('a'), ('b')) AS x (h)", ct);

    private static async Task<List<DateTime>> DaysAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var days = new List<DateTime>();
        await using var command = new NpgsqlCommand("SELECT DISTINCT day FROM collect.query_store_hash_daily ORDER BY day", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            days.Add(reader.GetDateTime(0));
        }

        return days;
    }

    [Fact]
    public async Task TheBackfill_BuildsTheNewestDaysFirst_WithinThePerTickCap_AndTheCleanupRemovesWholeDaysOnly()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ComposeStampLiveSupport.BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            var newest = FullDay();
            var first = newest.AddDays(-4);
            await SeedSyntheticDaysAsync(connection, first, 5, ct);
            var clock = Yesterday().AddHours(3).AddMinutes(30);

            /* Five whole days built, nothing built yet (no wide data: the hours part of the tick has nothing to plan). Two a tick, newest first. */
            Assert.Equal(2, QueryStoreHashDaily.MaxBuildsPerTick);
            var tick = await ComposeStampLiveSupport.TickAtAsync(source, clock, ct);
            Assert.Equal(2, tick.DaysBuilt);
            Assert.Equal(new[] { newest.AddDays(-1), newest }, await DaysAsync(connection, ct));
            tick = await ComposeStampLiveSupport.TickAtAsync(source, clock, ct);
            Assert.Equal(2, tick.DaysBuilt);
            Assert.Equal(new[] { newest.AddDays(-3), newest.AddDays(-2), newest.AddDays(-1), newest }, await DaysAsync(connection, ct));
            tick = await ComposeStampLiveSupport.TickAtAsync(source, clock, ct);
            Assert.Equal(1, tick.DaysBuilt);
            Assert.Equal(5, (await DaysAsync(connection, ct)).Count);
            tick = await ComposeStampLiveSupport.TickAtAsync(source, clock, ct);
            Assert.Equal(0, tick.DaysBuilt);

            /* Each built day is the sum of its 24 hourly rows: 24 x 3 wide rows, mem_max 23 above the day's first hour. */
            Assert.Equal(5 * 2 * 2, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_daily", ct));
            Assert.Equal(5 * 2 * 2, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_daily WHERE wide_rows = 72", ct));
            Assert.Equal(10, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_daily_built WHERE pair_count = 24 AND seq_sum = 48", ct));

            /* The cleanup: a floor in the middle of the third day removes the two days wholly below it and keeps the third (its hours below the
               floor are gone, so it is not valid) and the two above. */
            var floor = newest.AddDays(-2).AddHours(12);
            var now = floor.AddDays(ComposeStampLiveSupport.RetentionDays).Subtract(QueryStoreIntervalWide.PurgeEdgeMargin);
            Assert.Equal(floor, QueryStoreComposeStamp.FloorHour(now, ComposeStampLiveSupport.RetentionDays));
            await ExecAsync(connection, $"INSERT INTO collect.query_store_hash_daily_failures VALUES (TIMESTAMP '{At(first)}', 1, NULL, 'x', 60, TIMESTAMP '{At(first)}', TIMESTAMP '{At(first)}', TIMESTAMP '{At(first)}'), (TIMESTAMP '{At(newest)}', 1, NULL, 'x', 60, TIMESTAMP '{At(first)}', TIMESTAMP '{At(first)}', TIMESTAMP '{At(first)}')", ct);
            Assert.Equal(60, await QueryStoreComposeStamp.GcAsync(connection, now, ComposeStampLiveSupport.RetentionDays, ct));
            Assert.Equal(new[] { newest.AddDays(-2), newest.AddDays(-1), newest }, await DaysAsync(connection, ct));
            Assert.Equal(3, await CountAsync(connection, "SELECT count(DISTINCT day) FROM collect.query_store_hash_daily_built", ct));
            Assert.Equal(1, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_daily_failures", ct));
            Assert.Equal(Expect(newest.AddDays(-1), 1, 2) + "," + Expect(newest, 1, 2), await ValidAsync(connection, new[] { 1, 2 }, first, newest.AddDays(1), ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ADayBuildThatFails_IsRecordedAndBacksOff_AndABuildClearsTheRecord()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ComposeStampLiveSupport.BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            var day = FullDay();
            await SeedSyntheticDaysAsync(connection, day, 1, ct);
            var clock = Yesterday().AddHours(3).AddMinutes(30);
            await ExecAsync(connection, "ALTER TABLE collect.query_store_hash_daily ADD CONSTRAINT ck_fail CHECK (false)", ct);

            for (var attempt = 1; attempt <= QueryStoreComposeStamp.FailuresBeforeBackoff; attempt++)
            {
                var tick = await ComposeStampLiveSupport.TickAtAsync(source, clock, ct);
                Assert.Equal(0, tick.DaysBuilt);
                Assert.Equal(1, tick.DaysFailed);
                Assert.Equal(attempt, await CountAsync(connection, "SELECT failures FROM collect.query_store_hash_daily_failures", ct));
            }

            /* Backed off: the next tick does not try the day. */
            var quiet = await ComposeStampLiveSupport.TickAtAsync(source, clock, ct);
            Assert.Equal(0, quiet.DaysFailed + quiet.DaysBuilt);
            Assert.Equal(0, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_daily_failures WHERE retry_at <= TIMESTAMP '" + At(clock) + "'", ct));
            Assert.Equal(0, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_daily", ct));

            /* Due again and healthy: built, and the record is gone. */
            await ExecAsync(connection, "ALTER TABLE collect.query_store_hash_daily DROP CONSTRAINT ck_fail", ct);
            await ExecAsync(connection, "UPDATE collect.query_store_hash_daily_failures SET retry_at = retry_at - interval '2 days'", ct);
            var healed = await ComposeStampLiveSupport.TickAtAsync(source, clock, ct);
            Assert.Equal(1, healed.DaysBuilt);
            Assert.Equal(0, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_daily_failures", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task TheRung_RunsTwiceCleanly_OnAFreshStoreAndOnOneUpgradedFromV220_AndKeepsTheDayRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ComposeStampLiveSupport.BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            /* Fresh: the four objects are there (tables, the (day, server_id) index, the primary key of the ledger). */
            const string Objects = @"
SELECT count(*) FROM
(
    SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = 'ix_query_store_hash_daily_day_server' AND indexdef LIKE '%(day, server_id)'
    UNION ALL SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = 'pk_query_store_hash_daily_built'
    UNION ALL SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = 'pk_query_store_hash_daily_failures'
) AS x";
            Assert.Equal(3, await CountAsync(connection, Objects, ct));
            await ExecAsync(connection, @"
INSERT INTO collect.query_store_hash_daily (day, wide_rows, server_id, query_hash, ec_sum) VALUES (TIMESTAMP '2026-01-01', 1, 1, 'a', 5);
INSERT INTO collect.query_store_hash_daily_built (day, server_id, pair_count, seq_sum, built_at) VALUES (TIMESTAMP '2026-01-01', 1, 24, 48, TIMESTAMP '2026-01-02')", ct);
            await ExecAsync(connection, QueryStoreComposeStamp.HashHourlyCreateSql, ct);
            await ExecAsync(connection, QueryStoreComposeStamp.HashHourlyCreateSql, ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(3, await CountAsync(connection, Objects, ct));
            Assert.Equal(1, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_daily", ct));
            Assert.Equal(1, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_daily_built", ct));

            /* Upgraded from V220: the rung's objects are gone, then the migrator applies V221 and the tables are there and empty. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 220, ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 221", ct);
            Assert.Equal(0, await CountAsync(connection, "SELECT count(*) FROM pg_class WHERE relnamespace = 'collect'::regnamespace AND relname LIKE 'query_store_hash_daily%'", ct));
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(3, await CountAsync(connection, Objects, ct));
            Assert.Equal(0, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_daily", ct));
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(3, await CountAsync(connection, Objects, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
