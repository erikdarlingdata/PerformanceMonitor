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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5448: the late-row trigger (<c>trg_plan_regression_daily_late</c>) on <c>collect.query_store_interval_latest</c>,
/// against a real PostgreSQL store (<c>DARLING_TEST_PG</c>). The trigger is what keeps a built day total honest: a row
/// whose interval started before yesterday's midnight can only arrive late (a backfill, a lagging collector, a replay),
/// so each one bumps <c>late_seq</c> on the day it started and the next day, and the builder's day stays invalid until it
/// is rebuilt. The cases the design (plan v2, lane 2) pins:
///
/// <para>(1) steady rows never run the function (<c>pg_stat_user_functions.calls = 0</c>), so the steady write path pays
/// only the <c>WHEN</c> test; (2) a late row marks day(first) and the next day, one bump per (server, day) per
/// transaction (lane 6: a bump per row made a big late apply quadratic), and a rolled-back savepoint undoes both the bump
/// and the record of it; (3) an
/// <c>ON CONFLICT</c> winner marks and a <c>WHERE</c>-rejected loser does not; (4) a first older than 16 days marks
/// nothing; (5) COPY and (6) a plain INSERT both mark; (8) the table converted to a hypertable with
/// <c>migrate_data => true</c> keeps the trigger and later inserts still mark; (9) the trigger leaves
/// <c>n_tup_hot_upd / n_tup_upd</c> alone, because it writes another table and touches no indexed column. The fault
/// case (7), which needs the real writer, is in <see cref="QueryStoreIntervalLatestWriterTests"/>.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it (one test turns the table
   into a hypertable and disables the trigger, which must never happen to a shared store). It never touches the shared
   database's tables, so it cannot race the live collection. */
public sealed class PlanRegressionDailyTriggerLiveTests
{
    private const int ServerA = -5448001;
    private const int ServerB = -5448002;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string FunctionName = "plan_regression_daily_mark_late";

    private const string TriggerName = "trg_plan_regression_daily_late";

    /* ---- the cases --------------------------------------------------------------------------------------- */

    [Fact]
    public async Task SteadyRows_NeverRunTheFunction_AndLeaveTheBuiltTableEmpty_AndALateRowDoesRunIt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 trigger test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);

        /* Function statistics are off by default; this session turns them on. */
        await ExecAsync(connection, "SET track_functions = 'all'", ct);

        var now = DateTime.UtcNow;

        /* Steady: intervals that started within the last day, and re-collections of them (the ON CONFLICT update arm
           the writer takes every pass). The WHEN clause is false for every one, so the function body never runs. */
        for (var pass = 0; pass < 3; pass++)
        {
            await UpsertAsync(connection, ServerA, FirstTimes(now.AddHours(-1), 20, TimeSpan.FromMinutes(-1)), 100 + pass, 1000 + pass, ct);
            await UpsertAsync(connection, ServerA, FirstTimes(now.AddHours(-20), 20, TimeSpan.FromMinutes(-1)), 100 + pass, 1000 + pass, ct);
        }

        Assert.Equal(40, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.query_store_interval_latest", ct));
        Assert.Equal(0, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.plan_regression_daily_built", ct));
        Assert.Equal(0, await FunctionCallsAsync(connection, ct));

        /* The positive control: the same measurement sees a late row, so the zero above is not a blind probe. */
        await UpsertAsync(connection, ServerA, new[] { now.Date.AddDays(-6).AddHours(3) }, 1, 1, ct);
        Assert.Equal(1, await FunctionCallsAsync(connection, ct));
        Assert.Equal(2, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.plan_regression_daily_built", ct));
    }

    [Fact]
    public async Task ALateRow_MarksItsDayAndTheNext_OncePerServerDayPerTransaction()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 trigger test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);

        var today = DateTime.UtcNow.Date;
        var d5 = today.AddDays(-5);
        var d9 = today.AddDays(-9);

        /* Three rows whose first falls on d5 (one statement, so one transaction: one bump, not three), one at 23:30 of d9
           (its last can cross midnight, so d9 + 1 is marked). A bump per row was lane 1's shape, and it made a big late
           apply quadratic (#5448 lane 6). */
        await UpsertAsync(connection, ServerA, new[] { d5.AddHours(1), d5.AddHours(2), d5.AddHours(23).AddMinutes(30) }, 1, 1, ct);
        await UpsertAsync(connection, ServerA, new[] { d9.AddHours(23).AddMinutes(30) }, 1, 1, ct);

        var built = await BuiltAsync(connection, ServerA, ct);
        Assert.Equal(
            new Dictionary<DateTime, long>
            {
                [d5] = 1,
                [d5.AddDays(1)] = 1,
                [d9] = 1,
                [d9.AddDays(1)] = 1,
            },
            built);

        /* A row marks only its own server. */
        Assert.Empty(await BuiltAsync(connection, ServerB, ct));

        /* Marked days start with no build recorded: built_seq stays NULL until the builder claims the day. */
        Assert.Equal(4, await ScalarLongAsync(connection,
            "SELECT COUNT(*) FROM collect.plan_regression_daily_built WHERE built_seq IS NULL AND built_at IS NULL AND source_rows IS NULL", ct));

        /* A second server's late row on the same day is a separate pair of rows. */
        await UpsertAsync(connection, ServerB, new[] { d5.AddHours(4) }, 1, 1, ct);
        Assert.Equal(1L, (await BuiltAsync(connection, ServerB, ct))[d5]);
        Assert.Equal(1L, (await BuiltAsync(connection, ServerA, ct))[d5]);
    }

    /// <summary>
    /// #5448 lane 6: the bump is once per (server, day) per TRANSACTION. Late rows keep running the function (the function
    /// statistics count every one), but only the first for a pair writes the built table. Three statements in one
    /// transaction, with the third row's pair overlapping the first's on one day (its first day is the first row's next
    /// day), bump each of the three days once; the same rows' pairs in two transactions bump twice.
    /// </summary>
    [Fact]
    public async Task TwoLateRowsForOneDay_InOneTransaction_BumpOnce_AndInTwoTransactions_BumpTwice()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 trigger test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await ExecAsync(connection, "SET track_functions = 'all'", ct);

        var today = DateTime.UtcNow.Date;
        var d5 = today.AddDays(-5);
        var d8 = today.AddDays(-8);

        await ExecAsync(connection, "BEGIN", ct);
        await UpsertAsync(connection, ServerA, new[] { d5.AddHours(1), d5.AddHours(2) }, 1, 1, ct);
        await UpsertAsync(connection, ServerA, new[] { d5.AddHours(3) }, 1, 1, ct);
        await UpsertAsync(connection, ServerA, new[] { d5.AddDays(1).AddHours(4) }, 1, 1, ct);
        await ExecAsync(connection, "COMMIT", ct);

        Assert.Equal(4, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.query_store_interval_latest", ct));
        Assert.Equal(
            new Dictionary<DateTime, long> { [d5] = 1, [d5.AddDays(1)] = 1, [d5.AddDays(2)] = 1 },
            await BuiltAsync(connection, ServerA, ct));

        /* Two transactions, two late rows each, all four on one day: one bump per transaction. */
        await ExecAsync(connection, "BEGIN", ct);
        await UpsertAsync(connection, ServerB, new[] { d8.AddHours(1), d8.AddHours(2) }, 1, 1, ct);
        await ExecAsync(connection, "COMMIT", ct);
        await ExecAsync(connection, "BEGIN", ct);
        await UpsertAsync(connection, ServerB, new[] { d8.AddHours(3), d8.AddHours(4) }, 1, 1, ct);
        await ExecAsync(connection, "COMMIT", ct);

        Assert.Equal(
            new Dictionary<DateTime, long> { [d8] = 2, [d8.AddDays(1)] = 2 },
            await BuiltAsync(connection, ServerB, ct));

        /* The function itself still ran for each of the eight late rows: the saving is the write, not the call. */
        Assert.Equal(8, await FunctionCallsAsync(connection, ct));

        /* Nothing is left behind for the next transaction of the session. */
        Assert.Equal(string.Empty, await ScalarStringAsync(connection, "SELECT coalesce(current_setting('darling.plan_regression_marked', true), '')", ct));
    }

    /// <summary>
    /// #5448 lane 6: the transaction-local record of marked pairs is undone with a rolled-back savepoint, together with
    /// the bump it recorded, so a later late row for that day in the same transaction marks again instead of being
    /// skipped. A record that survived the rollback would drop the day's bump and leave a stale built total valid.
    /// </summary>
    [Fact]
    public async Task ALateRowInARolledBackSavepoint_IsUnmarked_SoTheNextLateRowMarksAgain()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 trigger test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);

        var today = DateTime.UtcNow.Date;
        var d5 = today.AddDays(-5);
        var d9 = today.AddDays(-9);

        await ExecAsync(connection, "BEGIN", ct);
        await UpsertAsync(connection, ServerA, new[] { d5.AddHours(1) }, 1, 1, ct);
        await ExecAsync(connection, "SAVEPOINT late_apply", ct);
        await UpsertAsync(connection, ServerA, new[] { d9.AddHours(1) }, 1, 1, ct);
        Assert.Equal(1L, (await BuiltAsync(connection, ServerA, ct))[d9]);
        await ExecAsync(connection, "ROLLBACK TO SAVEPOINT late_apply", ct);
        Assert.False((await BuiltAsync(connection, ServerA, ct)).ContainsKey(d9), "the rolled-back bump must be gone");

        /* d9 was unmarked with the savepoint, so this marks it again; d5 was marked outside the savepoint, so this does not
           bump it a second time. */
        await UpsertAsync(connection, ServerA, new[] { d9.AddHours(2) }, 1, 1, ct);
        await UpsertAsync(connection, ServerA, new[] { d5.AddHours(5) }, 1, 1, ct);
        await ExecAsync(connection, "COMMIT", ct);

        Assert.Equal(
            new Dictionary<DateTime, long> { [d5] = 1, [d5.AddDays(1)] = 1, [d9] = 1, [d9.AddDays(1)] = 1 },
            await BuiltAsync(connection, ServerA, ct));

        /* A whole rolled-back transaction leaves no bump and no record behind either. */
        await ExecAsync(connection, "BEGIN", ct);
        await UpsertAsync(connection, ServerA, new[] { d9.AddHours(6) }, 1, 1, ct);
        await ExecAsync(connection, "ROLLBACK", ct);
        await UpsertAsync(connection, ServerA, new[] { d9.AddHours(7) }, 1, 1, ct);
        Assert.Equal(2L, (await BuiltAsync(connection, ServerA, ct))[d9]);
    }

    /// <summary>
    /// The function's <c>SET search_path = pg_catalog, pg_temp</c> pin still holds with the transaction-local setting in it:
    /// <c>set_config</c> and <c>current_setting</c> are called unqualified, so they would resolve to a caller's decoy if the
    /// function inherited the session's path. The session here puts a schema with raising decoys of both FIRST. With the pin
    /// the apply works; with the pin removed (the control, in this scratch database only) the same apply fails on the decoy,
    /// so the first half proves something.
    /// </summary>
    [Fact]
    public async Task TheSearchPathPin_StillHolds_AgainstADecoySchemaFirstInTheSessionPath()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 trigger test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);

        Assert.Equal("{\"search_path=pg_catalog, pg_temp\"}", await ScalarStringAsync(connection,
            "SELECT p.proconfig::text FROM pg_proc p WHERE p.pronamespace = 'collect'::regnamespace AND p.proname = '" + FunctionName + "'", ct));

        await ExecAsync(connection, @"
CREATE SCHEMA decoy;
CREATE FUNCTION decoy.set_config(text, text, boolean) RETURNS text LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'decoy set_config ran'; END $$;
CREATE FUNCTION decoy.current_setting(text, boolean) RETURNS text LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'decoy current_setting ran'; END $$;
SET search_path = decoy, pg_catalog;", ct);

        var d5 = DateTime.UtcNow.Date.AddDays(-5);
        await UpsertAsync(connection, ServerA, new[] { d5.AddHours(1) }, 1, 1, ct);
        Assert.Equal(1L, (await BuiltAsync(connection, ServerA, ct))[d5]);

        /* The control: without the pin the function inherits the decoy path and fails. */
        await ExecAsync(connection, "ALTER FUNCTION collect." + FunctionName + "() RESET search_path", ct);
        var failure = await Assert.ThrowsAsync<PostgresException>(
            async () => await UpsertAsync(connection, ServerA, new[] { d5.AddHours(2) }, 1, 1, ct));
        Assert.Contains("decoy", failure.MessageText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnConflict_AWinnerMarks_AndAWhereRejectedLoserDoesNot()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 trigger test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);

        var d = DateTime.UtcNow.Date.AddDays(-4);
        var first = d.AddHours(6);

        await UpsertAsync(connection, ServerA, new[] { first }, executionCount: 10, collectionOffsetSeconds: 100, ct);
        Assert.Equal(1L, (await BuiltAsync(connection, ServerA, ct))[d]);

        /* A loser: older collection_time and a smaller execution_count, so the upsert's WHERE rejects it. The row is
           not updated, so no UPDATE trigger fires. */
        await UpsertAsync(connection, ServerA, new[] { first }, executionCount: 5, collectionOffsetSeconds: 50, ct);
        Assert.Equal(1L, (await BuiltAsync(connection, ServerA, ct))[d]);
        Assert.Equal(10, await ScalarLongAsync(connection, "SELECT execution_count FROM collect.query_store_interval_latest", ct));

        /* A winner: newer collection_time. It updates the row in place and bumps late_seq once more. */
        await UpsertAsync(connection, ServerA, new[] { first }, executionCount: 11, collectionOffsetSeconds: 200, ct);
        Assert.Equal(2L, (await BuiltAsync(connection, ServerA, ct))[d]);
        Assert.Equal(2L, (await BuiltAsync(connection, ServerA, ct))[d.AddDays(1)]);
        Assert.Equal(11, await ScalarLongAsync(connection, "SELECT execution_count FROM collect.query_store_interval_latest", ct));
    }

    [Fact]
    public async Task AFirstOlderThanSixteenDays_MarksNothing_AndTheEdgesAreExact()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 trigger test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);

        var now = DateTime.UtcNow;
        var today = now.Date;

        /* Past the 16 day clamp: nothing marked (L1b: the builder never builds days that old, so a mark would be a row
           the garbage collector deletes at once). */
        await UpsertAsync(connection, ServerA, new[] { now.AddDays(-17), now.AddDays(-30) }, 1, 1, ct);
        Assert.Equal(0, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.plan_regression_daily_built", ct));

        /* Inside the clamp: marked. */
        await UpsertAsync(connection, ServerA, new[] { now.AddDays(-15) }, 1, 1, ct);
        Assert.Equal(2, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.plan_regression_daily_built", ct));

        /* The recent edge: a first at or after yesterday's midnight is a live interval, never late. One second before
           it is late. */
        await ExecAsync(connection, "TRUNCATE collect.plan_regression_daily_built", ct);
        await UpsertAsync(connection, ServerA, new[] { today.AddDays(-1), today.AddHours(-1), now }, 1, 1, ct);
        Assert.Equal(0, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.plan_regression_daily_built", ct));

        await UpsertAsync(connection, ServerA, new[] { today.AddDays(-1).AddSeconds(-1) }, 1, 1, ct);
        var built = await BuiltAsync(connection, ServerA, ct);
        Assert.Equal(2, built.Count);
        Assert.Equal(1L, built[today.AddDays(-2)]);
        Assert.Equal(1L, built[today.AddDays(-1)]);
    }

    [Fact]
    public async Task CopyAndPlainInsert_BothMark()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 trigger test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);

        var today = DateTime.UtcNow.Date;
        var dCopy = today.AddDays(-7);
        var dInsert = today.AddDays(-3);

        /* (5) COPY straight into the table: row triggers fire for COPY FROM. */
        await using (var import = await connection.BeginBinaryImportAsync(
            "COPY collect.query_store_interval_latest (server_id, database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, collection_time, execution_count) FROM STDIN (FORMAT BINARY)", ct))
        {
            for (var i = 0; i < 25; i++)
            {
                await import.StartRowAsync(ct);
                await import.WriteAsync(ServerA, NpgsqlDbType.Integer, ct);
                await import.WriteAsync("db", NpgsqlDbType.Text, ct);
                await import.WriteAsync((long)i, NpgsqlDbType.Bigint, ct);
                await import.WriteAsync((long)i, NpgsqlDbType.Bigint, ct);
                await import.WriteAsync((long)i, NpgsqlDbType.Bigint, ct);
                await import.WriteAsync(DateTime.SpecifyKind(dCopy.AddMinutes(i), DateTimeKind.Unspecified), NpgsqlDbType.Timestamp, ct);
                await import.WriteAsync(DateTime.SpecifyKind(today, DateTimeKind.Unspecified), NpgsqlDbType.Timestamp, ct);
                await import.WriteAsync(1L, NpgsqlDbType.Bigint, ct);
            }

            await import.CompleteAsync(ct);
        }

        /* One COPY is one transaction, so its 25 late rows bump each of their two days once. */
        var built = await BuiltAsync(connection, ServerA, ct);
        Assert.Equal(1L, built[dCopy]);
        Assert.Equal(1L, built[dCopy.AddDays(1)]);

        /* (6) a plain multi-row INSERT with no ON CONFLICT. */
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_interval_latest (server_id, database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, collection_time, execution_count) "
            + "SELECT " + ServerA.ToString(CultureInfo.InvariantCulture) + ", 'db', g, g, g, '" + dInsert.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + " 05:00'::timestamp, now() AT TIME ZONE 'UTC', 1 FROM generate_series(1, 7) AS g", ct);

        built = await BuiltAsync(connection, ServerA, ct);
        Assert.Equal(1L, built[dInsert]);
        Assert.Equal(1L, built[dInsert.AddDays(1)]);
        Assert.Equal(1L, built[dCopy]);
    }

    [Fact]
    public async Task ConvertedToAHypertable_WithMigrateData_KeepsTheTrigger_AndLaterInsertsStillMark()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 trigger test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);

        /* A fresh scratch database does not inherit the extension; a store without TimescaleDB skips this case. */
        var timescale = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        Assert.SkipUnless(timescale, "TimescaleDB is not available on this store.");
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);

        var today = DateTime.UtcNow.Date;
        var dBefore = today.AddDays(-8);
        var dAfter = today.AddDays(-5);

        /* Rows exist before the conversion, so migrate_data moves them into chunks with the trigger already attached. */
        await UpsertAsync(connection, ServerA, new[] { dBefore.AddHours(2), dBefore.AddHours(3) }, 1, 1, ct);
        Assert.Equal(1L, (await BuiltAsync(connection, ServerA, ct))[dBefore]);

        await ExecAsync(connection,
            "SELECT create_hypertable('collect.query_store_interval_latest', by_range('first_execution_time', INTERVAL '1 day'), if_not_exists => true, migrate_data => true)", ct);

        Assert.Equal(2, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.query_store_interval_latest", ct));
        Assert.True(await ScalarLongAsync(connection,
            "SELECT COUNT(*) FROM pg_trigger WHERE tgname = '" + TriggerName + "' AND NOT tgisinternal", ct) >= 1,
            "the trigger must survive create_hypertable (on the table, and on its chunks)");

        /* migrate_data moves each row into its chunk with an INSERT, which fires the trigger: the conversion itself may bump
           late_seq (once per day for its whole transaction now). That is safe (an extra bump only invalidates a day, and
           the builder rebuilds it), so the pin is that the marks never go backwards. */
        var afterMigration = (await BuiltAsync(connection, ServerA, ct))[dBefore];
        Assert.True(afterMigration >= 1, "the pre-conversion marks must survive create_hypertable, got " + afterMigration);

        /* Later inserts (new chunks) and re-collections of migrated rows (old chunks) both mark, one bump per transaction. */
        await UpsertAsync(connection, ServerA, new[] { dAfter.AddHours(4) }, 1, 1, ct);
        await UpsertAsync(connection, ServerA, new[] { dBefore.AddHours(2) }, executionCount: 2, collectionOffsetSeconds: 500, ct);

        var built = await BuiltAsync(connection, ServerA, ct);
        Assert.Equal(1L, built[dAfter]);
        Assert.Equal(1L, built[dAfter.AddDays(1)]);
        Assert.Equal(afterMigration + 1, built[dBefore]);
        Assert.Equal(afterMigration + 1, built[dBefore.AddDays(1)]);
    }

    [Fact]
    public async Task TheTrigger_DoesNotChangeHotUpdates()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 trigger test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);

        var today = DateTime.UtcNow.Date;
        var setA = new List<DateTime>();
        var setB = new List<DateTime>();
        for (var i = 0; i < 1000; i++)
        {
            setA.Add(today.AddDays(-6).AddSeconds(i));
            setB.Add(today.AddDays(-7).AddSeconds(i));
        }

        /* Two sets of late rows, loaded the same way and then updated once each: A with the trigger off (the baseline),
           B with it on, so both start from the same page fill (fillfactor 50) and neither inherits the other's dead
           versions. The columns the upsert changes are in no index, so each update is HOT while the page has room, with
           or without the trigger. */
        await UpsertAsync(connection, ServerA, setA, 1, 1, ct);
        await UpsertAsync(connection, ServerA, setB, 1, 1, ct);
        await ExecAsync(connection, "TRUNCATE collect.plan_regression_daily_built", ct);

        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER " + TriggerName, ct);
        var off = await UpdateCountersAsync(connection, async () =>
            await UpsertAsync(connection, ServerA, setA, executionCount: 2, collectionOffsetSeconds: 10, ct), ct);
        Assert.Equal(0, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.plan_regression_daily_built", ct));

        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest ENABLE TRIGGER " + TriggerName, ct);
        var on = await UpdateCountersAsync(connection, async () =>
            await UpsertAsync(connection, ServerA, setB, executionCount: 2, collectionOffsetSeconds: 10, ct), ct);

        /* One upsert is one transaction, and all 1000 rows start on one day: one bump of that day and of the next. */
        Assert.Equal(2, await ScalarLongAsync(connection, "SELECT COALESCE(SUM(late_seq), 0) FROM collect.plan_regression_daily_built", ct));
        Assert.Equal(1000, off.Updates);
        Assert.Equal(1000, on.Updates);
        Assert.True(off.Hot > 0, "the baseline pass produced no HOT updates, so the comparison would prove nothing");
        Assert.Equal(off.Hot, on.Hot);
    }

    /* ---- helpers ----------------------------------------------------------------------------------------- */

    private static IReadOnlyList<DateTime> FirstTimes(DateTime start, int count, TimeSpan step)
    {
        var list = new List<DateTime>(count);
        for (var i = 0; i < count; i++)
        {
            list.Add(start + (step * i));
        }

        return list;
    }

    /// <summary>
    /// One apply-shaped upsert: the writer's own conflict target and its <c>WHERE</c> guard, one row per element of
    /// <paramref name="firsts"/> (the row's identity is its position, so the same list again hits the same rows).
    /// <c>collection_time</c> is a fixed base plus <paramref name="collectionOffsetSeconds"/>, so a larger offset wins.
    /// </summary>
    private static async Task UpsertAsync(
        NpgsqlConnection connection, int serverId, IReadOnlyList<DateTime> firsts, long executionCount, long collectionOffsetSeconds, CancellationToken ct)
    {
        var sql = @"
INSERT INTO collect.query_store_interval_latest AS t
    (server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
     collection_time, execution_count, avg_cpu_time_us, avg_duration_us, last_execution_time)
SELECT $1, 'db', n, n, NULL, n, f, '2026-01-01'::timestamp + make_interval(secs => $4), $3, 10, 20, f + interval '5 minutes'
FROM unnest($2::timestamp[]) WITH ORDINALITY AS u (f, n)
ON CONFLICT (" + QueryStoreIntervalLatest.IdentityColumns + @")
DO UPDATE SET
    collection_time = EXCLUDED.collection_time,
    execution_count = EXCLUDED.execution_count,
    last_execution_time = EXCLUDED.last_execution_time
WHERE (EXCLUDED.collection_time, EXCLUDED.execution_count) > (t.collection_time, t.execution_count)";

        var array = new DateTime[firsts.Count];
        for (var i = 0; i < array.Length; i++)
        {
            array[i] = DateTime.SpecifyKind(firsts[i], DateTimeKind.Unspecified);
        }

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = array });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = executionCount });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Double, Value = (double)collectionOffsetSeconds });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<Dictionary<DateTime, long>> BuiltAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        var result = new Dictionary<DateTime, long>();
        await using var command = new NpgsqlCommand(
            "SELECT day, late_seq FROM collect.plan_regression_daily_built WHERE server_id = $1", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result[reader.GetDateTime(0)] = reader.GetInt64(1);
        }

        return result;
    }

    private static async Task<long> FunctionCallsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        /* Function statistics are flushed by the session that counted them, on demand. */
        await ExecAsync(connection, "SELECT pg_stat_force_next_flush()", ct);
        return await ScalarLongAsync(connection,
            "SELECT COALESCE(SUM(calls), 0) FROM pg_stat_user_functions WHERE funcname = '" + FunctionName + "'", ct);
    }

    private static async Task<(long Updates, long Hot)> UpdateCountersAsync(NpgsqlConnection connection, Func<Task> body, CancellationToken ct)
    {
        async Task<(long, long)> ReadAsync()
        {
            await ExecAsync(connection, "SELECT pg_stat_force_next_flush()", ct);
            await using var command = new NpgsqlCommand(
                "SELECT n_tup_upd, n_tup_hot_upd FROM pg_stat_user_tables WHERE schemaname = 'collect' AND relname = 'query_store_interval_latest'", connection);
            await using var reader = await command.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            return (reader.GetInt64(0), reader.GetInt64(1));
        }

        var before = await ReadAsync();
        await body();
        var after = await ReadAsync();
        return (after.Item1 - before.Item1, after.Item2 - before.Item2);
    }

    private static async Task<NpgsqlConnection> OpenMigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarLongAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<string> ScalarStringAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) ?? string.Empty;
    }
}
