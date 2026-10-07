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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the builder of the per-day per-plan totals PLAN_REGRESSION reads for closed days (#5448),
/// <see cref="PlanRegressionDaily"/>'s hourly-tick tenant: what a built day holds (a hand aggregate), that an empty day is
/// still built (<c>source_rows</c> 0), that a build racing a late apply ends invalid and is rebuilt, that a second builder
/// skips, which servers and days are planned, the garbage collection, and where the tenant sits in the tick. Most seeds sit
/// at a fixed past date with the late-row trigger disabled and <c>nowUtc</c> passed explicitly, so no fact reads the clock;
/// the race fact needs the trigger, which reads <c>now()</c>, so it seeds relative to the real clock. Live facts run when
/// <c>DARLING_TEST_PG</c> is set, each on its own scratch store.
/// </summary>
[Collection("live-postgres")]
public sealed class PlanRegressionDailyBuilderLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the PLAN_REGRESSION day-totals builder's live pins (each mints its own scratch database).";

    private static readonly DateTime Now = new(2026, 3, 20, 12, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The first closed day for <see cref="Now"/>: T - 14.</summary>
    private static readonly DateTime FirstDay = new(2026, 3, 6, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>A day with rows in the hand-aggregate fact.</summary>
    private static readonly DateTime Day = new(2026, 3, 10, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>Closed days for <see cref="Now"/>: T - 14 through T - 3.</summary>
    private const int ClosedDays = 12;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static long s_interval;

    private static async Task<NpgsqlConnection> MigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string DateText(DateTime value) => value.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Enrolls the server (enabled unless told otherwise) with an interval-table coverage claim.</summary>
    private static async Task CoverAsync(
        NpgsqlConnection connection, int serverId, CancellationToken ct, string filledSince = "2026-01-01 00:00:00", bool enabled = true)
    {
        await ExecAsync(connection,
            $"INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES ({serverId}, 'srv{serverId}', {(enabled ? "TRUE" : "FALSE")}) ON CONFLICT (server_id) DO NOTHING", ct);
        await ExecAsync(connection,
            $"INSERT INTO collect.query_store_interval_latest_coverage (server_id, filled_since, applied_through) VALUES ({serverId}, TIMESTAMP '{filledSince}', TIMESTAMP '2026-03-01 00:00:00')", ct);
    }

    /// <summary>One interval row: first at <paramref name="first"/>, collected an hour later, last half an hour after first.</summary>
    private static async Task SeedAsync(
        NpgsqlConnection connection, int serverId, string? db, long plan, DateTime first, long execs, long cpu, long dur, bool forced, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO collect.query_store_interval_latest (server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time, collection_time, query_plan_hash, execution_count, avg_cpu_time_us, avg_duration_us, last_execution_time, is_forced_plan, force_failure_count) "
            + "VALUES (@server, @db, 10, @plan, NULL, @interval, @first, @first + interval '1 hour', 'h' || @plan, @execs, @cpu, @dur, @first + interval '30 minutes', @forced, 0)", connection);
        command.Parameters.Add(new NpgsqlParameter("server", NpgsqlDbType.Integer) { Value = serverId });
        command.Parameters.Add(new NpgsqlParameter("db", NpgsqlDbType.Text) { Value = (object?)db ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("plan", NpgsqlDbType.Bigint) { Value = plan });
        command.Parameters.Add(new NpgsqlParameter("interval", NpgsqlDbType.Bigint) { Value = Interlocked.Increment(ref s_interval) });
        command.Parameters.Add(new NpgsqlParameter("first", NpgsqlDbType.Timestamp) { Value = first });
        command.Parameters.Add(new NpgsqlParameter("execs", NpgsqlDbType.Bigint) { Value = execs });
        command.Parameters.Add(new NpgsqlParameter("cpu", NpgsqlDbType.Bigint) { Value = cpu });
        command.Parameters.Add(new NpgsqlParameter("dur", NpgsqlDbType.Bigint) { Value = dur });
        command.Parameters.Add(new NpgsqlParameter("forced", NpgsqlDbType.Boolean) { Value = forced });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static NpgsqlDataSource DataSource(ScratchPostgres scratch) => NpgsqlDataSource.Create(scratch.ConnectionString);

    private static async Task RunLiveAsync(Func<ScratchPostgres, NpgsqlConnection, CancellationToken, Task> body, bool disableTrigger = true)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        // #1776 own-store: a scratch database of its own, so no other class's migrations race it.
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            if (disableTrigger)
            {
                /* The trigger's WHEN clause is not under test here, and an old-dated insert would mark days: disable it. */
                await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late", ct);
            }

            await body(scratch, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ABuiltDay_EqualsAHandAggregate_AndAnEmptyDayIsBuiltWithZeroSourceRows()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            /* Plan 1: two intervals on the day (10 x 100 + 30 x 200 = 7000 cpu, 10 x 1000 + 30 x 2000 = 70000 dur), one forced.
               Plan 2, NULL database. Plan 1 again on the next day, which must not land in this one. */
            await SeedAsync(connection, 1, "db1", 1, Day.AddHours(2), 10, 100, 1000, false, ct);
            await SeedAsync(connection, 1, "db1", 1, Day.AddHours(5), 30, 200, 2000, true, ct);
            await SeedAsync(connection, 1, null, 2, Day.AddHours(3), 5, 50, 500, false, ct);
            await SeedAsync(connection, 1, "db1", 1, Day.AddDays(1).AddHours(2), 99, 1, 1, false, ct);

            await using var source = DataSource(scratch);
            var result = await PlanRegressionDaily.RunTickAsync(source, Now, NullLogger.Instance, ct);

            Assert.Equal(ClosedDays, result.Built);
            Assert.Equal(0, result.Failed);
            Assert.Equal(0, result.Skipped);
            Assert.Equal(0, result.Deferred);
            Assert.Equal("db1|1|40|7000|70000|true;<null>|2|5|250|2500|false", await ScalarAsync(connection,
                $"SELECT string_agg(COALESCE(database_name, '<null>') || '|' || plan_id || '|' || execs || '|' || cpu_us_sum || '|' || dur_us_sum || '|' || is_forced_plan::text, ';' ORDER BY plan_id) FROM collect.plan_regression_daily WHERE server_id = 1 AND day = DATE '{DateText(Day)}'", ct));
            Assert.Equal(2L, await ScalarAsync(connection, $"SELECT source_rows FROM collect.plan_regression_daily_built WHERE server_id = 1 AND day = DATE '{DateText(Day)}'", ct));

            /* The next day holds its one row; a day with nothing is built too, with source_rows 0, so it is not planned again. */
            Assert.Equal(1L, await ScalarAsync(connection, $"SELECT source_rows FROM collect.plan_regression_daily_built WHERE server_id = 1 AND day = DATE '{DateText(Day.AddDays(1))}'", ct));
            var empty = Day.AddDays(2);
            Assert.Equal(0L, await ScalarAsync(connection, $"SELECT source_rows FROM collect.plan_regression_daily_built WHERE server_id = 1 AND day = DATE '{DateText(empty)}'", ct));
            Assert.Equal(0L, await ScalarAsync(connection, $"SELECT count(*) FROM collect.plan_regression_daily WHERE day = DATE '{DateText(empty)}'", ct));

            /* Every built day is valid (built_seq = late_seq) and the window is T - 14 through T - 3 exactly. */
            Assert.Equal((long)ClosedDays, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily_built WHERE built_seq = late_seq", ct));
            Assert.Equal(DateText(FirstDay), (await ScalarAsync(connection, "SELECT min(day)::text FROM collect.plan_regression_daily_built", ct))!.ToString());
            Assert.Equal(DateText(Now.AddDays(-3)), (await ScalarAsync(connection, "SELECT max(day)::text FROM collect.plan_regression_daily_built", ct))!.ToString());

            /* Nothing is due now. */
            var again = await PlanRegressionDaily.RunTickAsync(source, Now, NullLogger.Instance, ct);
            Assert.Equal(0, again.Built);
        });
    }

    [Fact]
    public async Task ABuildRacingALateApply_EndsInvalid_ThenRebuilds()
    {
        /* The trigger reads now() in its WHEN clause, so this fact is relative to the real clock. */
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            var day = now.Date.AddDays(-5);
            var dayOnly = DateOnly.FromDateTime(day);
            await SeedAsync(connection, 1, "db1", 1, day.AddHours(2), 10, 100, 1000, false, ct);
            Assert.Equal(1L, await ScalarAsync(connection, $"SELECT late_seq FROM collect.plan_regression_daily_built WHERE server_id = 1 AND day = DATE '{DateText(day)}'", ct));

            await using var builder = new NpgsqlConnection(scratch.ConnectionString);
            await builder.OpenAsync(ct);

            /* A late apply lands between the build's read of late_seq (s0 = 1) and its aggregate. */
            var rows = await PlanRegressionDaily.BuildDayAsync(builder, 1, dayOnly, now, ct,
                afterSeqRead: async token => await SeedAsync(connection, 1, "db1", 2, day.AddHours(4), 5, 50, 500, false, token));

            Assert.Equal(2L, rows);
            Assert.Equal(1L, await ScalarAsync(connection, $"SELECT built_seq FROM collect.plan_regression_daily_built WHERE server_id = 1 AND day = DATE '{DateText(day)}'", ct));
            Assert.Equal(2L, await ScalarAsync(connection, $"SELECT late_seq FROM collect.plan_regression_daily_built WHERE server_id = 1 AND day = DATE '{DateText(day)}'", ct));
            var due = await PlanRegressionDaily.PlanBuildsAsync(connection, now, ct);
            Assert.Contains(new PlanRegressionDaily.Build(1, dayOnly), due);

            /* The rebuild stamps the new sequence, and the day is no longer due. */
            Assert.Equal(2L, await PlanRegressionDaily.BuildDayAsync(builder, 1, dayOnly, now, ct));
            Assert.Equal(2L, await ScalarAsync(connection, $"SELECT built_seq FROM collect.plan_regression_daily_built WHERE server_id = 1 AND day = DATE '{DateText(day)}'", ct));
            Assert.Equal(2L, await ScalarAsync(connection, $"SELECT late_seq FROM collect.plan_regression_daily_built WHERE server_id = 1 AND day = DATE '{DateText(day)}'", ct));
            Assert.DoesNotContain(new PlanRegressionDaily.Build(1, dayOnly), await PlanRegressionDaily.PlanBuildsAsync(connection, now, ct));
        }, disableTrigger: false);
    }

    [Fact]
    public async Task ASecondBuilder_SkipsWhileTheServersLockIsHeld_AndBuildsOnceItIsFree()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            await SeedAsync(connection, 1, "db1", 1, Day.AddHours(2), 10, 100, 1000, false, ct);

            await using var holder = new NpgsqlConnection(scratch.ConnectionString);
            await holder.OpenAsync(ct);
            await using var held = await holder.BeginTransactionAsync(ct);
            await ExecAsync(holder, "SELECT pg_advisory_xact_lock(hashtextextended('plan_regression_daily:1', 0))", ct);

            await using var source = DataSource(scratch);
            var skipped = await PlanRegressionDaily.RunTickAsync(source, Now, NullLogger.Instance, ct);
            Assert.Equal(0, skipped.Built);
            Assert.Equal(ClosedDays, skipped.Skipped);
            Assert.Equal(0, skipped.Failed);
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily", ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily_built WHERE built_seq IS NOT NULL", ct));

            await held.CommitAsync(ct);
            var built = await PlanRegressionDaily.RunTickAsync(source, Now, NullLogger.Instance, ct);
            Assert.Equal(ClosedDays, built.Built);
            Assert.Equal(0, built.Skipped);
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily", ct));
        });
    }

    [Fact]
    public async Task ThePlan_SkipsAPendingServer_ADisabledServer_AndDaysBeforeFilledSincePlusOne()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            /* Server 1: filled since the middle of 03-08, so its first plannable day is 03-09. Server 2: a pending batch.
               Server 3: disabled. Server 4: filled since long ago. */
            await CoverAsync(connection, 1, ct, filledSince: "2026-03-08 12:00:00");
            await CoverAsync(connection, 2, ct);
            await ExecAsync(connection,
                "INSERT INTO collect.query_store_interval_latest_pending (server_id, collection_time, database_name, recorded_at, failure) VALUES (2, TIMESTAMP '2026-03-18 00:00:00', 'db1', TIMESTAMP '2026-03-18 00:05:00', 'x')", ct);
            await CoverAsync(connection, 3, ct, enabled: false);
            await CoverAsync(connection, 4, ct);

            var plan = await PlanRegressionDaily.PlanBuildsAsync(connection, Now, ct);

            Assert.Equal(9, plan.Count(b => b.ServerId == 1));
            Assert.Equal(new DateOnly(2026, 3, 9), plan.Where(b => b.ServerId == 1).Min(b => b.Day));
            Assert.Equal(new DateOnly(2026, 3, 17), plan.Where(b => b.ServerId == 1).Max(b => b.Day));
            Assert.DoesNotContain(plan, b => b.ServerId is 2 or 3);
            Assert.Equal(ClosedDays, plan.Count(b => b.ServerId == 4));
            Assert.Equal(new DateOnly(2026, 3, 6), plan.Where(b => b.ServerId == 4).Min(b => b.Day));
            Assert.Equal(new DateOnly(2026, 3, 17), plan.Where(b => b.ServerId == 4).Max(b => b.Day));

            /* Oldest day first, then server. */
            Assert.Equal(plan.OrderBy(b => b.Day).ThenBy(b => b.ServerId).ToList(), plan.ToList());
        });
    }

    [Fact]
    public async Task ABacklogLargerThanTheCap_BuildsOnlyTheCap_AndTheDeadlineDefersTheRest()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            for (var server = 1; server <= 11; server++)
            {
                await CoverAsync(connection, server, ct);
            }

            await using var source = DataSource(scratch);

            /* 11 servers x 12 days = 132 due, cap 120. */
            var late = await PlanRegressionDaily.RunTickAsync(source, Now, NullLogger.Instance, () => PlanRegressionDaily.MaxTickDuration, ct);
            Assert.Equal(0, late.Built);
            Assert.Equal(PlanRegressionDaily.MaxBuildsPerTick, late.Deferred);

            var result = await PlanRegressionDaily.RunTickAsync(source, Now, NullLogger.Instance, ct);
            Assert.Equal(PlanRegressionDaily.MaxBuildsPerTick, result.Built);
            Assert.Equal(0, result.Deferred);

            var next = await PlanRegressionDaily.RunTickAsync(source, Now, NullLogger.Instance, ct);
            Assert.Equal(11 * ClosedDays - PlanRegressionDaily.MaxBuildsPerTick, next.Built);
        });
    }

    [Fact]
    public async Task AFailedBuild_IsLoggedAsAWarning_LeavesTheDayUnbuilt_AndDoesNotStopTheOthers()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            await SeedAsync(connection, 1, "db1", 1, Day.AddHours(2), 10, 100, 1000, false, ct);
            /* Any insert into the totals table now fails, so only the day with rows fails; the empty days insert nothing. */
            await ExecAsync(connection, "ALTER TABLE collect.plan_regression_daily ADD CONSTRAINT zz_refuse CHECK (false) NOT VALID", ct);

            var logger = new CapturingTestLogger();
            await using var source = DataSource(scratch);
            var result = await PlanRegressionDaily.RunTickAsync(source, Now, logger, ct);

            Assert.Equal(1, result.Failed);
            Assert.Equal(ClosedDays - 1, result.Built);
            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
            Assert.Contains("failed and is retried next tick", logger.Joined, StringComparison.Ordinal);
            Assert.IsType<DBNull>(await ScalarAsync(connection, $"SELECT built_seq FROM collect.plan_regression_daily_built WHERE server_id = 1 AND day = DATE '{DateText(Day)}'", ct));

            /* Still due. */
            Assert.Contains(new PlanRegressionDaily.Build(1, DateOnly.FromDateTime(Day)), await PlanRegressionDaily.PlanBuildsAsync(connection, Now, ct));
        });
    }

    [Fact]
    public async Task TheGc_RemovesOldDaysAndDisabledServersFromBothTables_InOneTransaction()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            await CoverAsync(connection, 9, ct, enabled: false);
            const string Totals = "INSERT INTO collect.plan_regression_daily (server_id, day, database_name, query_id, plan_id, replica_role, query_plan_hash, execs) VALUES ({0}, DATE '{1}', 'db1', 1, 1, NULL, 'h', 1)";
            const string Built = "INSERT INTO collect.plan_regression_daily_built (server_id, day, built_seq) VALUES ({0}, DATE '{1}', 0)";

            /* Now is 03-20, so the horizon is 03-04 (T - 16): 03-03 goes, 03-04 stays. Server 9 is not enabled: all its days go. */
            foreach (var (server, day) in new[] { (1, "2026-03-03"), (1, "2026-03-04"), (1, "2026-03-10"), (9, "2026-03-10") })
            {
                await ExecAsync(connection, string.Format(System.Globalization.CultureInfo.InvariantCulture, Totals, server, day), ct);
                await ExecAsync(connection, string.Format(System.Globalization.CultureInfo.InvariantCulture, Built, server, day), ct);
            }

            Assert.Equal(2L, await PlanRegressionDaily.GcAsync(connection, Now, ct));

            Assert.Equal("1|2026-03-04;1|2026-03-10", await ScalarAsync(connection, "SELECT string_agg(server_id || '|' || day, ';' ORDER BY server_id, day) FROM collect.plan_regression_daily", ct));
            Assert.Equal("1|2026-03-04;1|2026-03-10", await ScalarAsync(connection, "SELECT string_agg(server_id || '|' || day, ';' ORDER BY server_id, day) FROM collect.plan_regression_daily_built", ct));
        });
    }

    [Fact]
    public async Task TheGc_ProbesTheTotalsByIndex_NeverScansThem_AndRemovesExpiredDaysWhileLiveDaysStay()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            await CoverAsync(connection, 9, ct, enabled: false);

            /* Enough totals rows that a sequential scan would be the cheap plan if the delete had a predicate of its own:
               3 live days of one enabled server, 30,000 rows each, and one expired day of a disabled server. */
            await ExecAsync(connection, @"
INSERT INTO collect.plan_regression_daily_built (server_id, day, built_seq)
SELECT 1, d::date, 0 FROM generate_series(DATE '2026-03-10', DATE '2026-03-12', interval '1 day') AS d;
INSERT INTO collect.plan_regression_daily (server_id, day, database_name, query_id, plan_id, replica_role, query_plan_hash, execs)
SELECT 1, d::date, 'db1', q, 1, NULL, 'h', 1
FROM generate_series(DATE '2026-03-10', DATE '2026-03-12', interval '1 day') AS d, generate_series(1, 30000) AS q;
INSERT INTO collect.plan_regression_daily_built (server_id, day, built_seq) VALUES (9, DATE '2026-03-10', 0);
INSERT INTO collect.plan_regression_daily (server_id, day, database_name, query_id, plan_id, replica_role, query_plan_hash, execs)
SELECT 9, DATE '2026-03-10', 'db1', q, 1, NULL, 'h', 1 FROM generate_series(1, 10) AS q;
ANALYZE collect.plan_regression_daily;
ANALYZE collect.plan_regression_daily_built;", ct);

            /* The plan of the statement that removes one due key: executed inside a transaction that is rolled back, so the
               next case starts from the same rows. Every access to the totals table is an index probe, on the unique index,
               and it reads only that day's ten rows of the 90,010. */
            var due = await TotalsScansOfGcAsync(connection, 9, new DateOnly(2026, 3, 10), ct);
            Assert.NotEmpty(due);

            /* Both leading columns are in the index condition: a day test the index cannot use (a cast, say) would leave the probe
               on the server alone and read every day of it, which a small due day of a small server would hide. */
            Assert.All(due, node => Assert.True(node.IndexCond is not null && node.IndexCond.Contains("server_id", StringComparison.Ordinal)
                && node.IndexCond.Contains("day", StringComparison.Ordinal), $"index condition: {node.IndexCond ?? "none"}"));
            Assert.All(due, node => Assert.True(node.Index == "ux_plan_regression_daily", $"{node.NodeType} on {node.Index ?? "no index"}"));
            Assert.True(due.Sum(node => node.RowsRead) <= 10, $"the cleanup read {due.Sum(node => node.RowsRead)} totals rows for ten due ones");

            /* Nothing is read when nothing is due: the built-row delete returns no key, so no totals statement runs and the
               totals table is not touched. After the disabled server's day goes, a second pass removes nothing. */
            /* The explain above runs the statement text on its own. What GcAsync really runs is counted through
               pg_stat_statements (when this rig loads it; CI does): one due key is one keyed totals delete that touches a few
               blocks, where a delete with a predicate of its own would read the whole 90,010-row table, and a pass with
               nothing due runs no totals delete at all. */
            var counted = await StartStatementCountAsync(connection, ct);
            Assert.Equal(1L, await PlanRegressionDaily.GcAsync(connection, Now, ct));
            if (counted)
            {
                var (calls, blocks) = await TotalsDeleteStatementsAsync(connection, ct);
                Assert.True(calls == 1, $"the cleanup ran {calls} totals deletes for one due key");
                Assert.True(blocks <= 60, $"the cleanup's totals delete touched {blocks} blocks for ten due rows");
            }

            Assert.Equal(90000L, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily", ct));
            if (counted)
            {
                await ExecAsync(connection, "SELECT pg_stat_statements_reset(0, (SELECT oid FROM pg_database WHERE datname = current_database()), 0)", ct);
            }

            Assert.Equal(0L, await PlanRegressionDaily.GcAsync(connection, Now, ct));
            if (counted)
            {
                var (calls, _) = await TotalsDeleteStatementsAsync(connection, ct);
                Assert.True(calls == 0, $"the cleanup ran {calls} totals deletes when nothing had expired");
            }

            Assert.Equal(90000L, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily", ct));

            /* The live days stay, and an expired day of an enabled server goes with its totals. */
            await ExecAsync(connection, "INSERT INTO collect.plan_regression_daily_built (server_id, day, built_seq) VALUES (1, DATE '2026-03-03', 0)", ct);
            await ExecAsync(connection, "INSERT INTO collect.plan_regression_daily (server_id, day, database_name, query_id, plan_id, replica_role, query_plan_hash, execs) VALUES (1, DATE '2026-03-03', 'db1', 1, 1, NULL, 'h', 1)", ct);
            Assert.Equal(1L, await PlanRegressionDaily.GcAsync(connection, Now, ct));
            Assert.Equal(90000L, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily", ct));
            Assert.Equal(3L, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily_built", ct));
        });
    }

    /// <summary>Turns on pg_stat_statements for this database and clears its counters. False when the rig does not preload it.</summary>
    private static async Task<bool> StartStatementCountAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var preloadCommand = new NpgsqlCommand("SELECT current_setting('shared_preload_libraries')", connection);
        var preload = (string)(await preloadCommand.ExecuteScalarAsync(ct))!;
        if (!preload.Split(',').Select(name => name.Trim()).Contains("pg_stat_statements"))
        {
            return false;
        }

        await ExecAsync(connection, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements", ct);
        await ExecAsync(connection, "SELECT pg_stat_statements_reset(0, (SELECT oid FROM pg_database WHERE datname = current_database()), 0)", ct);
        return true;
    }

    /// <summary>The calls and the blocks touched (hit plus read) of the statements that delete from the totals table, in this database.</summary>
    private static async Task<(long Calls, long Blocks)> TotalsDeleteStatementsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
SELECT COALESCE(SUM(calls), 0)::bigint, COALESCE(SUM(shared_blks_hit + shared_blks_read), 0)::bigint
FROM pg_stat_statements
WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
AND   query ~ '^DELETE FROM collect\.plan_regression_daily(\s|$)';", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    private readonly record struct TotalsScan(string NodeType, string? Index, string? IndexCond, double RowsRead);

    /// <summary>Runs <see cref="PlanRegressionDaily.GcTotalsSql"/> for one key under EXPLAIN ANALYZE in a transaction that is
    /// rolled back, and returns every access to the totals table: its node type, index and rows read (rows returned,
    /// times loops).</summary>
    private static async Task<List<TotalsScan>> TotalsScansOfGcAsync(NpgsqlConnection connection, int serverId, DateOnly day, CancellationToken ct)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);
        string json;
        await using (var command = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + PlanRegressionDaily.GcTotalsSql, connection, transaction))
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            json = (string)(await command.ExecuteScalarAsync(ct))!;
        }

        await transaction.RollbackAsync(ct);

        var found = new List<TotalsScan>();
        void Walk(System.Text.Json.JsonElement node)
        {
            if (node.TryGetProperty("Relation Name", out var relation) && relation.GetString() == "plan_regression_daily"
                && node.GetProperty("Node Type").GetString()!.Contains("Scan", StringComparison.Ordinal))
            {
                var loops = node.GetProperty("Actual Loops").GetDouble();
                var rows = node.GetProperty("Actual Rows").GetDouble() * loops;
                found.Add(new TotalsScan(node.GetProperty("Node Type").GetString()!, node.TryGetProperty("Index Name", out var index) ? index.GetString() : null, node.TryGetProperty("Index Cond", out var cond) ? cond.GetString() : null, rows));
            }

            if (node.TryGetProperty("Plans", out var plans))
            {
                foreach (var child in plans.EnumerateArray()) Walk(child);
            }

            if (node.TryGetProperty("Plan", out var plan)) Walk(plan);
        }

        using var document = System.Text.Json.JsonDocument.Parse(json);
        Walk(document.RootElement[0]);
        return found;
    }

    [Fact]
    public async Task ABuildWhoseBuiltRowIsGoneByTheStamp_ThrowsAndRollsBack_LeavingNoOrphanTotals()
    {
        await RunLiveAsync(async (scratch, connection, ct) =>
        {
            await CoverAsync(connection, 1, ct);
            await SeedAsync(connection, 1, "db1", 1, Day.AddHours(2), 10, 100, 1000, false, ct);

            await using var builder = new NpgsqlConnection(scratch.ConnectionString);
            await builder.OpenAsync(ct);

            /* A second service's cleanup removes the built row (the server was just disabled) after the build read its
               sequence and before it stamps. The stamp then updates nothing. */
            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => PlanRegressionDaily.BuildDayAsync(
                builder, 1, DateOnly.FromDateTime(Day), Now, ct,
                afterSeqRead: async token => await ExecAsync(connection, "DELETE FROM collect.plan_regression_daily_built WHERE server_id = 1", token)));
            Assert.Contains("is gone", thrown.Message, StringComparison.Ordinal);

            /* Rolled back: no totals for a day no built row owns, and no built row. */
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily", ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily_built", ct));
        });
    }

    [Fact]
    public void TheTenant_IsTheSeventhAndLastAwaitInTheTick_WithItsOwnCatchAll()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(worker);
        var tick = Body(code, "private async Task RunStoreMaintenanceTickAsync(CancellationToken stoppingToken)");
        Assert.False(string.IsNullOrEmpty(tick), "could not locate RunStoreMaintenanceTickAsync");

        const string Refresh = "await RefreshModuleMapRecentAsync(stoppingToken);";
        const string Build = "await BuildPlanRegressionDailyAsync(stoppingToken);";
        var refreshAt = tick.IndexOf(Refresh, StringComparison.Ordinal);
        var buildAt = tick.IndexOf(Build, StringComparison.Ordinal);
        Assert.True(refreshAt >= 0 && buildAt > refreshAt, "the day-totals builder is awaited after the module-map refresh");
        Assert.Equal(1, tick.Split(Build).Length - 1);
        Assert.Equal(1, code.Split(Build).Length - 1);
        Assert.True(string.IsNullOrWhiteSpace(tick[(refreshAt + Refresh.Length)..buildAt]), "nothing sits between the refresh and the builder");
        Assert.True(string.IsNullOrWhiteSpace(tick[(buildAt + Build.Length)..]), "nothing follows the builder in the tick");

        /* Outside the TimescaleDB gate: the refresh it follows is outside it, so the builder runs on every store shape. */
        var gateAt = tick.IndexOf("else", StringComparison.Ordinal);
        Assert.True(gateAt >= 0 && gateAt < refreshAt, "the builder sits after the gated if/else, outside the TimescaleDB gate");

        var tenant = Body(code, "private async Task BuildPlanRegressionDailyAsync(CancellationToken stoppingToken)");
        Assert.False(string.IsNullOrEmpty(tenant), "could not locate BuildPlanRegressionDailyAsync");
        Assert.Contains("catch (Exception ex) when (ex is not OperationCanceledException)", tenant, StringComparison.Ordinal);
        Assert.Contains("PlanRegressionDaily.RunTickAsync(", tenant, StringComparison.Ordinal);
        Assert.DoesNotContain("throw", tenant, StringComparison.Ordinal);
    }

    [Fact]
    public void TheConstantsAndStatements_AreTheDocumentedOnes()
    {
        Assert.Equal(120, PlanRegressionDaily.MaxBuildsPerTick);
        Assert.Equal(60, PlanRegressionDaily.BuildStatementTimeoutSeconds);
        Assert.Equal(TimeSpan.FromMinutes(10), PlanRegressionDaily.MaxTickDuration);
        Assert.Equal(14, PlanRegressionDaily.WindowDays);
        Assert.Equal(3, PlanRegressionDaily.ClosedDayLagDays);
        Assert.Equal(16, PlanRegressionDaily.MarkLateWindowDays);

        Assert.Contains("pg_try_advisory_xact_lock(hashtextextended('plan_regression_daily:' || $1::integer::text, 0))", PlanRegressionDaily.LockSql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (server_id, day) DO NOTHING", PlanRegressionDaily.EnsureBuiltSql, StringComparison.Ordinal);

        /* The plan: enabled servers only, none with a pending batch, days from the day after filled_since, due when unstamped or moved. */
        Assert.Contains("sv.is_enabled", PlanRegressionDaily.PlanSql, StringComparison.Ordinal);
        Assert.Contains("collect.query_store_interval_latest_pending", PlanRegressionDaily.PlanSql, StringComparison.Ordinal);
        Assert.Contains("d.day >= s.filled_since::date + 1", PlanRegressionDaily.PlanSql, StringComparison.Ordinal);
        Assert.Contains("b.built_seq IS NULL OR b.built_seq <> b.late_seq", PlanRegressionDaily.PlanSql, StringComparison.Ordinal);
        Assert.Contains("- 14)::timestamp", PlanRegressionDaily.PlanSql, StringComparison.Ordinal);
        Assert.Contains("- 3)::timestamp", PlanRegressionDaily.PlanSql, StringComparison.Ordinal);

        /* Both tables in one transaction: the built rows first, then the totals by key. */
        Assert.Contains("DELETE FROM collect.plan_regression_daily_built", PlanRegressionDaily.GcBuiltSql, StringComparison.Ordinal);
        Assert.Contains("RETURNING server_id, day", PlanRegressionDaily.GcBuiltSql, StringComparison.Ordinal);
        Assert.Contains("DELETE FROM collect.plan_regression_daily\nWHERE server_id = $1::integer\nAND   day = $2::date;", PlanRegressionDaily.GcTotalsSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
    }

    private static string Body(string source, string signature)
    {
        var start = source.IndexOf(signature, StringComparison.Ordinal);
        if (start < 0)
        {
            return string.Empty;
        }

        var open = source.IndexOf('{', start);
        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{')
            {
                depth++;
            }
            else if (source[i] == '}' && --depth == 0)
            {
                return source[(open + 1)..i];
            }
        }

        return string.Empty;
    }
}
