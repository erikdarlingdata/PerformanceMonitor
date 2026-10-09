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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5582 part 3, lane 1: the V173 rollup against a real PostgreSQL store (<c>DARLING_TEST_PG</c>). A 30-hour seed (three servers,
/// several plans per query hash, Aborted and Exception rows, NULL module and hash, NULL durations, weighted products whose
/// SUM passes a bigint) is built hour by hour, and then:
/// every rollup column equals the same aggregate over the wide table, compared as text; the grain is unique; a replayed row
/// and an update that moves a row out of a built hour each make their (server, hour) pair stale and a rebuild makes it current;
/// a build that runs at the same time as a late write (two connections) leaves the pair stale, for a pair that existed at the
/// build and for one that did not; the cleanup removes all three tables below the floor; a second migrate changes nothing; the
/// triggers sit on the partitioned parent and on every leaf, including leaves made or attached later; the read-only roles can
/// SELECT the new tables.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it, so it never touches the shared database and cannot race the
   live collection. */
public sealed class QueryStoreComposeStampLiveTests
{
    private const int RetentionDays = 9;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the #5582 rollup's live facts.";

    private static string At(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<string> TextAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) ?? string.Empty;
    }

    private static DateTime ThisHour() => new(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour, 0, 0, DateTimeKind.Unspecified);

    private static async Task<NpgsqlConnection> MigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static async Task EnrollAsync(NpgsqlConnection connection, int serverId, CancellationToken ct) =>
        await ExecAsync(connection,
            $"INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES ({serverId}, 'srv{serverId}', TRUE) ON CONFLICT (server_id) DO NOTHING;"
            + $"INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through) VALUES ({serverId}, date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '30 hours', now() AT TIME ZONE 'UTC')", ct);

    /// <summary>
    /// 30 hours of rows ending at <paramref name="hourNow"/>: 3 servers x 12 stamps an hour x 6 query hashes x 3 plans each. Hash 5 has a
    /// NULL query_hash, hash 3 and 6 a NULL module; some durations are NULL; some rows are Aborted or Exception; the rows of hash 1 carry
    /// products near 6e18, so a group of three plans sums past a bigint.
    /// </summary>
    private static Task SeedAsync(NpgsqlConnection connection, DateTime hourNow, CancellationToken ct) => ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, replica_role,
 runtime_stats_interval_id, interval_start_time_utc)
SELECT t.ct, s.server_id, 'db' || (q % 2), q, q * 10 + p,
       CASE WHEN (h + m + q + p) % 11 = 0 THEN 'Aborted' WHEN (h + m + q + p) % 13 = 0 THEN 'Exception' ELSE 'Regular' END,
       t.ct - interval '3 minutes', t.ct,
       CASE WHEN q IN (3, 6) THEN NULL ELSE 'mod' || (q % 3) END,
       CASE WHEN q = 5 THEN NULL ELSE 'hash' || q END,
       CASE WHEN (h + m + p) % 17 = 0 AND q <> 1 THEN NULL WHEN q = 1 THEN 2000000 ELSE ((h + m + q + p) * 100 + 1)::bigint END,
       CASE WHEN (h + m + q) % 7 = 0 AND q <> 1 THEN NULL WHEN q = 1 THEN 3000000000000::bigint ELSE ((h * 1000 + m * 37 + q * 13 + p) * 1000)::bigint END,
       CASE WHEN (h + q) % 5 = 0 AND q <> 1 THEN NULL ELSE ((h + m + q) * 523 + p)::bigint END,
       ((h + 1) * 997 + m + q)::bigint, CASE WHEN (m + q) % 4 = 0 THEN NULL ELSE ((h + 2) * 311 + m * q)::bigint END,
       NULL,
       ((((h * 12 + m) * 3 + s.k) * 6 + q) * 3 + p), t.ct - interval '10 minutes'
FROM generate_series(0, 29) AS h
CROSS JOIN generate_series(0, 11) AS m
CROSS JOIN (VALUES (1, 0), (2, 1), (3, 2)) AS s (server_id, k)
CROSS JOIN generate_series(1, 6) AS q
CROSS JOIN generate_series(1, 3) AS p
CROSS JOIN LATERAL (SELECT TIMESTAMP '{At(hourNow)}' - interval '30 hours' + h * interval '1 hour' + m * interval '5 minutes' AS ct) AS t", ct);

    /// <summary>Builds every hour that is due, tick by tick, and returns how many hours were built.</summary>
    private static async Task<int> BuildAllAsync(NpgsqlDataSource source, CancellationToken ct)
    {
        var total = 0;
        for (var guard = 0; guard < 20; guard++)
        {
            var result = await QueryStoreComposeStamp.RunTickAsync(source, DateTime.UtcNow, RetentionDays, NullLogger.Instance, ct);
            Assert.Equal(0, result.Failed);
            if (result.Built == 0)
            {
                return total;
            }

            total += result.Built;
        }

        throw new InvalidOperationException("the builder never ran out of hours");
    }

    /// <summary>
    /// Rows where the rollup and the same aggregates over the wide table differ, for the built window. The wide side's select list is the
    /// map's FactExpression, so a map entry cannot drift from the builder; the comparison is as TEXT, so a numeric that lost a digit shows.
    /// </summary>
    internal static async Task<long> MismatchesAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        var map = QueryStoreComposeStamp.PartialColumnMap;
        var select = string.Join(", ", map.Select((c, i) => c.FactExpression + " AS c" + i.ToString(CultureInfo.InvariantCulture)));
        var diffs = string.Join(" OR ", map.Select((c, i) => $"w.c{i}::text IS DISTINCT FROM r.{c.Column}::text"));
        var sql = $@"
SELECT count(*)
FROM
(
    SELECT f.collection_time, f.server_id, f.database_name, f.module_name, f.query_hash, {select}
    FROM collect.query_store_interval_wide AS f
    WHERE f.collection_time >= TIMESTAMP '{At(from)}' AND f.collection_time < TIMESTAMP '{At(to)}'
    GROUP BY f.collection_time, f.server_id, f.database_name, f.module_name, f.query_hash
) AS w
FULL JOIN
(
    SELECT * FROM collect.query_store_compose_stamp
    WHERE collection_time >= TIMESTAMP '{At(from)}' AND collection_time < TIMESTAMP '{At(to)}'
) AS r
  ON r.collection_time = w.collection_time AND r.server_id = w.server_id
 AND r.database_name IS NOT DISTINCT FROM w.database_name AND r.module_name IS NOT DISTINCT FROM w.module_name
 AND r.query_hash IS NOT DISTINCT FROM w.query_hash
WHERE w.collection_time IS NULL OR r.collection_time IS NULL OR {diffs}";
        return await CountAsync(connection, sql, ct);
    }

    internal static Task<long> StalePairsAsync(NpgsqlConnection connection, CancellationToken ct) => CountAsync(connection, @"
SELECT count(*) FROM collect.query_store_compose_stamp_built AS b
JOIN collect.query_store_compose_stamp_hours AS h ON h.hour = b.hour
WHERE b.built_seq IS DISTINCT FROM b.late_seq", ct);

    internal static Task<string> StaleListAsync(NpgsqlConnection connection, CancellationToken ct) => TextAsync(connection, @"
SELECT COALESCE(string_agg(b.server_id || '@' || to_char(b.hour, 'YYYY-MM-DD HH24'), ',' ORDER BY b.server_id, b.hour), '')
FROM collect.query_store_compose_stamp_built AS b
JOIN collect.query_store_compose_stamp_hours AS h ON h.hour = b.hour
WHERE b.built_seq IS DISTINCT FROM b.late_seq", ct);

    internal static string Pair(int server, DateTime hour) => server.ToString(CultureInfo.InvariantCulture) + "@" + hour.ToString("yyyy-MM-dd HH", CultureInfo.InvariantCulture);

    private static Task InsertRowAsync(NpgsqlConnection connection, int serverId, DateTime collectionTime, long interval, CancellationToken ct) => ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, runtime_stats_interval_id)
VALUES (TIMESTAMP '{At(collectionTime)}', {serverId}, 'dbX', 99, 99, 'Regular', TIMESTAMP '{At(collectionTime.AddMinutes(-3))}', TIMESTAMP '{At(collectionTime)}',
        'modX', 'hashX', 7, 1000, 500, 2000, 900, {interval})", ct);

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection, NpgsqlDataSource Source, DateTime HourNow)> ArrangeAsync(CancellationToken ct, int servers = 3)
    {
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var connection = await MigratedAsync(scratch, ct);
        for (var s = 1; s <= servers; s++)
        {
            await EnrollAsync(connection, s, ct);
        }

        var hourNow = ThisHour();
        await SeedAsync(connection, hourNow, ct);
        await ComposeStampLiveSupport.SeedPrecisionRowsAsync(connection, hourNow, ComposeStampLiveSupport.PrecisionSpot.Built, ct);
        var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        Assert.Equal(28, await BuildAllAsync(source, ct));
        await ComposeStampLiveSupport.AssertPrecisionRowsHaveTeethAsync(connection, ct);
        return (scratch, connection, source, hourNow);
    }

    [Fact]
    public async Task AfterTheBuild_EveryRollupColumnEqualsTheSameAggregateOverTheWideTable_AsText_AndTheGrainIsUnique()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var from = hourNow.AddHours(-30);
            var to = hourNow.AddHours(-2);
            Assert.True(await CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp", ct) > 1000);
            Assert.Equal(0, await MismatchesAsync(connection, from, to, ct));

            /* Not generated from the map: the totals, and the shapes the seed was built to cover. */
            Assert.Equal(
                await CountAsync(connection, $"SELECT sum(execution_count) FROM collect.query_store_interval_wide WHERE collection_time >= TIMESTAMP '{At(from)}' AND collection_time < TIMESTAMP '{At(to)}'", ct),
                await CountAsync(connection, "SELECT sum(ec_sum) FROM collect.query_store_compose_stamp", ct));
            Assert.Equal(
                await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_interval_wide WHERE collection_time >= TIMESTAMP '{At(from)}' AND collection_time < TIMESTAMP '{At(to)}'", ct),
                await CountAsync(connection, "SELECT sum(wide_rows) FROM collect.query_store_compose_stamp", ct));
            Assert.True(await CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp WHERE dur_wsum > 9223372036854775807", ct) > 0, "a weighted sum past a bigint");
            Assert.True(await CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp WHERE module_name IS NULL", ct) > 0);
            Assert.True(await CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp WHERE query_hash IS NULL", ct) > 0);
            Assert.True(await CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp WHERE ec_count < wide_rows", ct) > 0, "a NULL execution_count is counted out");
            Assert.True(await CountAsync(connection, "SELECT count(*) FROM collect.query_store_interval_wide WHERE execution_type_desc IN ('Aborted', 'Exception')", ct) > 0);
            Assert.True(await CountAsync(connection, "SELECT count(*) FROM collect.query_store_interval_wide WHERE avg_duration_us IS NULL", ct) > 0);

            /* The grain is unique, and every server-hour of the rollup has a current pair. */
            Assert.Equal(0, await CountAsync(connection, @"SELECT count(*) FROM (SELECT 1 FROM collect.query_store_compose_stamp
                GROUP BY collection_time, server_id, database_name, module_name, query_hash HAVING count(*) > 1) AS d", ct));
            Assert.Equal(0, await StalePairsAsync(connection, ct));
            Assert.Equal(0, await CountAsync(connection, @"SELECT count(*) FROM (SELECT DISTINCT server_id, date_trunc('hour', collection_time) AS hour FROM collect.query_store_compose_stamp) AS k
                LEFT JOIN collect.query_store_compose_stamp_built AS b ON b.server_id = k.server_id AND b.hour = k.hour
                WHERE b.built_seq IS DISTINCT FROM b.late_seq OR b.server_id IS NULL", ct));
            Assert.Equal(28, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_hours", ct));

            /* A tick with nothing due builds nothing; the newest builds were the youngest due hour. */
            Assert.Equal(0, await BuildAllAsync(source, ct));
            Assert.Equal(hourNow.AddHours(-3), Convert.ToDateTime(await TextAsync(connection, "SELECT max(hour) FROM collect.query_store_compose_stamp_hours", ct), CultureInfo.InvariantCulture));
            bodySucceeded = true;
        }
        finally
        {
            await source.DisposeAsync();
            await connection.DisposeAsync();
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task AReplayedRow_AndAnUpdateThatMovesARowOutOfABuiltHour_MakeTheirPairStale_AndARebuildMakesItCurrent()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* The youngest hour the builder does is three hours old: the row below lands in an hour that is only just past the lag, which is
               why the trigger's offset is the one hour and not three (the red-plant). */
            var youngest = hourNow.AddHours(-3);
            var older = hourNow.AddHours(-10);
            await InsertRowAsync(connection, 1, youngest.AddMinutes(55), 9_000_001, ct);
            Assert.Equal(Pair(1, youngest), await StaleListAsync(connection, ct));

            /* Move a server-2 row out of an older built hour into the current hour (the open interval's refresh): the OLD hour is marked. */
            await ExecAsync(connection, $@"
UPDATE collect.query_store_interval_wide SET collection_time = TIMESTAMP '{At(hourNow.AddMinutes(5))}'
WHERE ctid = (SELECT ctid FROM collect.query_store_interval_wide WHERE server_id = 2 AND collection_time >= TIMESTAMP '{At(older)}'
                AND collection_time < TIMESTAMP '{At(older.AddHours(1))}' ORDER BY collection_time, query_id, plan_id LIMIT 1)", ct);
            Assert.Equal(Pair(1, youngest) + "," + Pair(2, older), string.Join(",", (await StaleListAsync(connection, ct)).Split(',').OrderBy(x => x, StringComparer.Ordinal)).Replace(Pair(2, older) + "," + Pair(1, youngest), Pair(1, youngest) + "," + Pair(2, older), StringComparison.Ordinal));
            Assert.True(await MismatchesAsync(connection, hourNow.AddHours(-30), hourNow.AddHours(-2), ct) > 0, "the stale pairs really are different from the wide table");

            var result = await QueryStoreComposeStamp.RunTickAsync(source, DateTime.UtcNow, RetentionDays, NullLogger.Instance, ct);
            Assert.Equal(0, result.Failed);
            Assert.Equal(2, result.BuiltStale);
            Assert.Equal(0, result.BuiltMissing);
            Assert.Equal(0, await StalePairsAsync(connection, ct));
            Assert.Equal(0, await MismatchesAsync(connection, hourNow.AddHours(-30), hourNow.AddHours(-2), ct));
            Assert.Equal(0, await BuildAllAsync(source, ct));
            bodySucceeded = true;
        }
        finally
        {
            await source.DisposeAsync();
            await connection.DisposeAsync();
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task ABuildThatRunsAtTheSameTimeAsALateWrite_LeavesThePairStale_ForAnExistingPairAndForANewOne()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct, servers: 4);
        var bodySucceeded = false;
        try
        {
            var hour = hourNow.AddHours(-3);
            await using var builder = await source.OpenConnectionAsync(ct);
            await using var writer = new NpgsqlConnection(scratch.ConnectionString);
            await writer.OpenAsync(ct);
            Task? lateWrite = null;

            /* Server 1's pair exists, and the build has already upserted it (step 4), so the writer's trigger has to bump a row the build holds: the writer waits for the commit. */
            await QueryStoreComposeStamp.BuildHourAsync(builder, hour, DateTime.UtcNow, ct, async () =>
            {
                lateWrite = InsertRowAsync(writer, 1, hour.AddMinutes(20), 9_000_002, ct);
                for (var i = 0; i < 100 && await CountAsync(connection, "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'", ct) == 0; i++)
                {
                    await Task.Delay(100, ct);
                }

                Assert.False(lateWrite.IsCompleted, "the late write should be waiting on the build's row lock");
            });
            await lateWrite!;
            Assert.Equal(Pair(1, hour), await StaleListAsync(connection, ct));

            /* Server 4 has no pair in this hour, so nothing to wait for: the writer commits its own new pair during the build. */
            await QueryStoreComposeStamp.BuildHourAsync(builder, hour, DateTime.UtcNow, ct, async () =>
            {
                await InsertRowAsync(writer, 4, hour.AddMinutes(25), 9_000_003, ct);
                await Task.CompletedTask;
            });
            Assert.Equal(Pair(4, hour), await StaleListAsync(connection, ct));
            Assert.True(await MismatchesAsync(connection, hour, hour.AddHours(1), ct) > 0, "the late rows are not in the rollup yet");

            var result = await QueryStoreComposeStamp.RunTickAsync(source, DateTime.UtcNow, RetentionDays, NullLogger.Instance, ct);
            Assert.Equal(0, result.Failed);
            Assert.Equal(1, result.BuiltStale);
            Assert.Equal(0, await StalePairsAsync(connection, ct));
            Assert.Equal(0, await MismatchesAsync(connection, hour, hour.AddHours(1), ct));
            bodySucceeded = true;
        }
        finally
        {
            await source.DisposeAsync();
            await connection.DisposeAsync();
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task TheCleanup_RemovesAllThreeTablesBelowTheFloor_AndKeepsTheRest()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            var floor = QueryStoreComposeStamp.FloorHour(now, RetentionDays);
            foreach (var hour in new[] { floor.AddHours(-2), floor.AddHours(-1), floor, floor.AddHours(1) })
            {
                await ExecAsync(connection, $@"
INSERT INTO collect.query_store_compose_stamp (collection_time, wide_rows, server_id) VALUES (TIMESTAMP '{At(hour.AddMinutes(7))}', 1, 1), (TIMESTAMP '{At(hour.AddMinutes(37))}', 1, 2);
INSERT INTO collect.query_store_compose_stamp_built (server_id, hour, late_seq, built_seq) VALUES (1, TIMESTAMP '{At(hour)}', 0, 0), (2, TIMESTAMP '{At(hour)}', 0, 0);
INSERT INTO collect.query_store_compose_stamp_hours (hour, built_at) VALUES (TIMESTAMP '{At(hour)}', now() AT TIME ZONE 'UTC')", ct);
            }

            /* A pair a late writer made for an hour the builder never did, below the floor. */
            await ExecAsync(connection, $"INSERT INTO collect.query_store_compose_stamp_built (server_id, hour, late_seq) VALUES (3, TIMESTAMP '{At(floor.AddHours(-5))}', 1)", ct);

            var removed = await QueryStoreComposeStamp.GcAsync(connection, now, RetentionDays, ct);
            Assert.Equal(2, removed);
            var floorLiteral = $"TIMESTAMP '{At(floor)}'";
            Assert.Equal(0, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp WHERE collection_time < {floorLiteral}", ct));
            Assert.Equal(0, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE hour < {floorLiteral}", ct));
            Assert.Equal(0, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp_hours WHERE hour < {floorLiteral}", ct));
            Assert.Equal(4, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp", ct));
            Assert.Equal(4, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_built", ct));
            Assert.Equal(2, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_hours", ct));
            Assert.Equal(0, await QueryStoreComposeStamp.GcAsync(connection, now, RetentionDays, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ASecondMigrate_AndASecondRunOfTheRungText_ChangeNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            const string Fingerprint = @"
SELECT md5(
  (SELECT string_agg(pg_get_triggerdef(t.oid), '|' ORDER BY c.relname, t.tgname) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
     WHERE NOT t.tgisinternal AND t.tgname LIKE 'trg_query_store_compose_stamp%')
  || (SELECT pg_get_functiondef('collect.query_store_compose_stamp_mark_late'::regproc))
  || (SELECT string_agg(table_name || '.' || column_name || ':' || data_type, '|' ORDER BY table_name, ordinal_position)
        FROM information_schema.columns WHERE table_schema = 'collect' AND table_name LIKE 'query_store_compose_stamp%')
  || (SELECT string_agg(indexdef, '|' ORDER BY indexname) FROM pg_indexes WHERE schemaname = 'collect' AND tablename LIKE 'query_store_compose_stamp%'))";
            var before = await TextAsync(connection, Fingerprint, ct);
            Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));
            await using (var transaction = await connection.BeginTransactionAsync(ct))
            {
                await using var command = new NpgsqlCommand(PgMigrations.Scripts[^1].Sql, connection, transaction) { CommandTimeout = 120 };
                await command.ExecuteNonQueryAsync(ct);
                await transaction.CommitAsync(ct);
            }

            Assert.Equal(before, await TextAsync(connection, Fingerprint, ct));
            Assert.Equal(3, await CountAsync(connection, "SELECT count(*) FROM pg_class WHERE relnamespace = 'collect'::regnamespace AND relname LIKE 'query_store_compose_stamp%' AND relkind = 'r'", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task TheTriggers_AreOnTheParentAndOnEveryLeaf_IncludingLeavesMadeOrAttachedLater_AndALateRowThroughALeafMarks()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            const string TriggersOf = "SELECT count(*) FROM pg_trigger WHERE tgrelid = '{0}'::regclass AND NOT tgisinternal AND tgname LIKE 'trg_query_store_compose_stamp_late_%'";
            Assert.Equal(2, await CountAsync(connection, string.Format(CultureInfo.InvariantCulture, TriggersOf, "collect.query_store_interval_wide"), ct));
            Assert.Equal(2, await CountAsync(connection, string.Format(CultureInfo.InvariantCulture, TriggersOf, "collect.query_store_interval_wide_legacy"), ct));

            /* The maintenance pass promotes the legacy table, creates the day partitions and DEFAULT; each is a leaf made after the rung. */
            var logger = NullLogger.Instance;
            var wide = QueryStoreIntervalPartitions.Wide;
            await QueryStoreIntervalPartitions.ArmAsync(connection, wide, DateTime.UtcNow, logger, ct);
            await QueryStoreIntervalPartitions.ValidateAsync(connection, wide, DateTime.UtcNow, logger, ct);
            await QueryStoreIntervalPartitions.PromoteAsync(connection, wide, DateTime.UtcNow, logger, ct);
            await QueryStoreIntervalPartitions.CreateAheadAsync(connection, wide, DateTime.UtcNow, logger, ct);

            var leaves = new List<string>();
            await using (var command = new NpgsqlCommand("SELECT relid::regclass::text FROM pg_partition_tree('collect.query_store_interval_wide') WHERE isleaf", connection))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    leaves.Add(reader.GetString(0));
                }
            }

            Assert.True(leaves.Count >= 3, $"expected the legacy table, DEFAULT or day partitions, found {leaves.Count}");
            foreach (var leaf in leaves)
            {
                Assert.Equal(2, await CountAsync(connection, string.Format(CultureInfo.InvariantCulture, TriggersOf, leaf), ct));
            }

            /* A leaf attached by hand later is cloned too. */
            await ExecAsync(connection, "CREATE TABLE collect.qsiw_attached (LIKE collect.query_store_interval_wide INCLUDING DEFAULTS)", ct);
            await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_wide ATTACH PARTITION collect.qsiw_attached FOR VALUES FROM ('2031-01-01') TO ('2031-01-02')", ct);
            Assert.Equal(2, await CountAsync(connection, string.Format(CultureInfo.InvariantCulture, TriggersOf, "collect.qsiw_attached"), ct));

            /* A late row written through the parent into a day leaf marks, and so does one aimed at a leaf directly. */
            var oldHour = ThisHour().AddHours(-6);
            await EnrollAsync(connection, 1, ct);
            await InsertRowAsync(connection, 1, oldHour.AddMinutes(10), 9_100_001, ct);
            Assert.Equal(1, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE server_id = 1 AND hour = TIMESTAMP '{At(oldHour)}' AND late_seq = 1", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task TheReadOnlyRoles_CanSelectTheThreeTables_WithNoGrantInTheRung()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        const string Role = "qs_stamp_reader_5582";
        var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            Assert.DoesNotContain("GRANT", QueryStoreComposeStamp.CreateSql, StringComparison.Ordinal);
            await ExecAsync(connection, "CREATE ROLE " + Role + " NOLOGIN", ct);
            await ExecAsync(connection, "GRANT USAGE ON SCHEMA collect TO " + Role, ct);
            await ExecAsync(connection, "ALTER DEFAULT PRIVILEGES FOR ROLE " + await TextAsync(connection, "SELECT current_user::text", ct) + " IN SCHEMA collect GRANT SELECT ON TABLES TO " + Role, ct);

            /* The tables are created AFTER the role's default privileges: only the schema's defaults can give it the SELECT. */
            await ExecAsync(connection, "DROP TRIGGER IF EXISTS trg_query_store_compose_stamp_late_ins ON collect.query_store_interval_wide; DROP TRIGGER IF EXISTS trg_query_store_compose_stamp_late_upd ON collect.query_store_interval_wide;"
                + " DROP TABLE collect.query_store_compose_stamp_hours, collect.query_store_compose_stamp_built, collect.query_store_compose_stamp", ct);
            await ExecAsync(connection, PgMigrations.Scripts[^1].Sql, ct);

            await ExecAsync(connection, "SET ROLE " + Role, ct);
            foreach (var table in new[] { QueryStoreComposeStamp.Table, QueryStoreComposeStamp.BuiltTable, QueryStoreComposeStamp.HoursTable })
            {
                Assert.Equal(0, await CountAsync(connection, "SELECT count(*) FROM " + table, ct));
            }

            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, "DELETE FROM " + QueryStoreComposeStamp.Table, ct));
            await ExecAsync(connection, "RESET ROLE", ct);
            bodySucceeded = true;
        }
        finally
        {
            try
            {
                await ExecAsync(connection, "RESET ROLE", CancellationToken.None);
                await ExecAsync(connection, "DROP OWNED BY " + Role, CancellationToken.None);
                await ExecAsync(connection, "DROP ROLE IF EXISTS " + Role, CancellationToken.None);
            }
            finally
            {
                await connection.DisposeAsync();
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
