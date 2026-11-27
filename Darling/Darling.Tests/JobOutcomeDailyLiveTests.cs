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
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every live test mints its own scratch database through ScratchPostgres and touches nothing shared. */

/// <summary>
/// #9110: the per-day rollup of <c>job_history</c>'s successful job outcomes (V282) that the Job History read's per-job average and last
/// success read whole days from. A built day must equal a GROUP BY over the raw rows, the first ticks must fill the days the store already
/// holds (newest first, only days that have ended on the server's own clock), a rebuild a day later must pick up a late row, and retention
/// must delete the days job_history's own retention has passed.
/// </summary>
public sealed class JobOutcomeDailyLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #9110 job outcome daily tests.";
    private const int ServerA = 9201;

    // ── The rung ──────────────────────────────────────────────────────────────

    [Fact]
    public void TheRungIsOnADenseLadder_AndTheProbeMapsItWhenItsTablesAreThere()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == JobOutcomeDaily.Version);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        var own = Array.FindIndex(parameters, p => p.Name == "hasJobOutcomeDaily");
        Assert.True(own >= 0, "the probe has no sentinel for the job outcome daily rung");

        /* The sentinels are in ladder order. With every newer rung's sentinel off, this rung is the newest one the probe saw. */
        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        for (var i = own + 1; i < all.Length; i++)
        {
            all[i] = false;
        }
        Assert.Equal(rung.Version, (int)method.Invoke(null, all)!);
        all[own] = false;
        Assert.Equal(rung.Version - 1, (int)method.Invoke(null, all)!);
    }

    [Fact]
    public async Task TheRungMakesBothTablesEmpty_RunsTwice_AndTheStoreProbeSeesThem()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);

        await ExecuteAsync(connection, "DROP TABLE IF EXISTS collect.job_outcome_daily; DROP TABLE IF EXISTS collect.job_outcome_daily_days; DROP TABLE IF EXISTS collect.job_outcome_daily_watermark", ct);
        var rung = PgMigrations.Scripts.Single(s => s.Version == JobOutcomeDaily.Version);
        await ExecuteAsync(connection, rung.Sql, ct);
        await ExecuteAsync(connection, rung.Sql, ct);
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_outcome_daily", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_outcome_daily_days", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_outcome_daily_watermark", ct));

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        Assert.Equal(StorageVersion.SchemaVersion, await viewer.GetStoreSchemaVersionAsync(ct));

        /* With every newer rung's sentinel gone, the store reads as this rung, and without one of this rung's tables as the rung below. */
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, JobOutcomeDaily.Version, ct);
        Assert.Equal(JobOutcomeDaily.Version, await viewer.GetStoreSchemaVersionAsync(ct));
        await ExecuteAsync(connection, "DROP TABLE collect.job_outcome_daily_watermark", ct);
        Assert.Equal(JobOutcomeDaily.Version - 1, await viewer.GetStoreSchemaVersionAsync(ct));
    }

    // ── The ticks ─────────────────────────────────────────────────────────────

    /// <summary>
    /// The first ticks work forward from the oldest collection_time a day at a time, and the invariant holds after each step: the rollup equals a
    /// GROUP BY over the qualifying rows collected at or before the watermark, the days table holds each day's newest collection_time, and no row
    /// above the watermark is counted. Rows of another status, another step, no job id and no run time are not in it, and the average the rollup
    /// makes equals AVG over the same rows, NULL durations left out.
    /// </summary>
    [Fact]
    public async Task TheStepsBackfillOldestFirst_AndTheInvariantHoldsAfterEachOne()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedServersAsync(connection, ct);

        var rows = new StringBuilder();
        long id = 1;
        void Add(string? job, int step, int status, string run, int? duration, string collected) =>
            rows.Append(JobRow(id++, ServerA, job, step, status, run, duration, collected));

        Add("A", 0, 1, "2026-11-10 08:00:00", 10, "2026-11-10 08:30:00");
        Add("A", 0, 1, "2026-11-10 12:00:00", null, "2026-11-10 12:30:00");
        Add("A", 0, 1, "2026-11-10 23:59:59", 31, "2026-11-11 00:30:00");
        Add("A", 0, 0, "2026-11-10 13:00:00", 999, "2026-11-10 13:30:00");
        Add("A", 1, 1, "2026-11-10 14:00:00", 888, "2026-11-10 14:30:00");
        Add(null, 0, 1, "2026-11-10 07:00:00", 5, "2026-11-10 07:30:00");
        Add("B", 0, 1, "2026-11-11 05:00:00", 7, "2026-11-11 05:30:00");
        Add("C", 0, 1, "2026-11-11 06:00:00", null, "2026-11-11 06:30:00");
        /* A late row: an old run day that reached the store days later. */
        Add("A", 0, 1, "2026-11-10 09:00:00", 20, "2026-11-14 10:00:00");
        Add("A", 0, 1, "2026-11-14 09:00:00", 40, "2026-11-14 09:30:00");
        /* A row with no run time. */
        Add("A", 0, 1, "2026-11-12 09:00:00", 99, "2026-11-12 09:30:00");
        await SeedAsync(connection, rows.ToString(), ct);
        await ExecuteAsync(connection, "UPDATE collect.job_history SET run_datetime = NULL WHERE job_history_id = 900000011", ct);

        /* The first step starts at the oldest row and covers one day of collection_time. */
        var now = new DateTime(2026, 11, 20, 12, 0, 0, DateTimeKind.Utc);
        var plan = await JobOutcomeDaily.PlanStepsAsync(connection, now, 10, ct);
        var first = plan.Single(p => p.ServerId == ServerA);
        Assert.Equal(new DateTime(2026, 11, 10, 7, 30, 0, DateTimeKind.Utc).AddTicks(-10), first.WOld);
        Assert.Equal(first.WOld.AddDays(1), first.WNew);
        await JobOutcomeDaily.StepAsync(connection, first.ServerId, first.WOld, first.WNew, ct);
        await AssertInvariantAsync(connection, ct);
        Assert.Equal(["A|2026-11-10|2|41"], await ReadAsync(connection, "SELECT job_id, run_day::text, success_count, success_duration_sum FROM collect.job_outcome_daily WHERE job_id = 'A'", ct));
        Assert.Equal(["9201|2026-11-10|2026-11-11 00:30:00"], await ReadAsync(connection, "SELECT server_id, run_day::text, (max_collection_time AT TIME ZONE 'UTC')::text FROM collect.job_outcome_daily_days WHERE run_day = DATE '2026-11-10'", ct));

        /* Further steps go forward only, up to the margin behind now. */
        var before = await ScalarAsync<DateTime>(connection, "SELECT collected_through FROM collect.job_outcome_daily_watermark WHERE server_id = 9201", ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tick = await JobOutcomeDaily.RunTickAsync(dataSource, now, NullLogger.Instance, ct);
        Assert.Equal(0, tick.Failed);
        var after = await ScalarAsync<DateTime>(connection, "SELECT collected_through FROM collect.job_outcome_daily_watermark WHERE server_id = 9201", ct);
        Assert.True(after > before);
        Assert.Equal(now.AddHours(-1), DateTime.SpecifyKind(after, DateTimeKind.Utc));
        await AssertInvariantAsync(connection, ct);

        /* Every qualifying row is counted once, the late one on its own old day. */
        Assert.Equal(["A|2026-11-10|3|61|2026-11-10 23:59:59", "A|2026-11-14|1|40|2026-11-14 09:00:00", "B|2026-11-11|1|7|2026-11-11 05:00:00", "C|2026-11-11|0|0|2026-11-11 06:00:00"],
            await ReadAsync(connection, "SELECT job_id, run_day::text, success_count, success_duration_sum, last_success_run_local::text FROM collect.job_outcome_daily ORDER BY job_id, run_day", ct));
        Assert.Equal("2026-11-14 10:00:00", await ScalarAsync<string>(connection, "SELECT (max_collection_time AT TIME ZONE 'UTC')::text FROM collect.job_outcome_daily_days WHERE run_day = DATE '2026-11-10'", ct));

        /* sum / count is AVG(run_duration_seconds) for a job with some NULL durations. */
        Assert.Equal(
            await ReadAsync(connection, "SELECT job_id, AVG(run_duration_seconds)::text FROM collect.job_history WHERE step_id = 0 AND run_status = 1 AND job_id = 'A' AND run_datetime::date = DATE '2026-11-10' GROUP BY job_id", ct),
            await ReadAsync(connection, "SELECT job_id, (sum(success_duration_sum)::numeric / NULLIF(sum(success_count), 0))::text FROM collect.job_outcome_daily WHERE job_id = 'A' AND run_day = DATE '2026-11-10' GROUP BY job_id", ct));

        /* A server with no job_history gets a watermark with nothing to add, and a disabled server gets nothing. */
        Assert.Equal(1L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_outcome_daily_watermark WHERE server_id = 9204", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_outcome_daily_watermark WHERE server_id = 9203", ct));
    }

    /// <summary>
    /// A row collected inside the margin (the last hour) is counted only by a later tick, a late row (an old run day collected now) is added to its
    /// own day and moves that day's newest collection time, and nothing is counted twice across ticks.
    /// </summary>
    [Fact]
    public async Task ARowInsideTheMargin_IsCountedOnALaterTick_AndALateRowLandsOnItsOwnDay()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedServersAsync(connection, ct);
        await SeedAsync(connection, JobRow(1, ServerA, "A", 0, 1, "2026-11-19 09:00:00", 10, "2026-11-19 09:30:00"), ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var now1 = new DateTime(2026, 11, 20, 12, 0, 0, DateTimeKind.Utc);
        await JobOutcomeDaily.RunTickAsync(dataSource, now1, NullLogger.Instance, ct);
        Assert.Equal(["A|2026-11-19|1|10"], await ReadAsync(connection, "SELECT job_id, run_day::text, success_count, success_duration_sum FROM collect.job_outcome_daily", ct));

        /* Collected at 11:30, inside the hour margin of the first tick, and a late row of the 2nd collected at 11:45. */
        await SeedAsync(connection, JobRow(2, ServerA, "A", 0, 1, "2026-11-20 11:00:00", 30, "2026-11-20 11:30:00") + JobRow(3, ServerA, "A", 0, 1, "2026-11-02 11:00:00", 50, "2026-11-20 11:45:00"), ct);
        await JobOutcomeDaily.RunTickAsync(dataSource, now1, NullLogger.Instance, ct);
        Assert.Equal(["A|2026-11-19|1|10"], await ReadAsync(connection, "SELECT job_id, run_day::text, success_count, success_duration_sum FROM collect.job_outcome_daily", ct));
        await AssertInvariantAsync(connection, ct);

        var now2 = now1.AddHours(3);
        await JobOutcomeDaily.RunTickAsync(dataSource, now2, NullLogger.Instance, ct);
        await JobOutcomeDaily.RunTickAsync(dataSource, now2, NullLogger.Instance, ct);
        Assert.Equal(["A|2026-11-02|1|50", "A|2026-11-19|1|10", "A|2026-11-20|1|30"], await ReadAsync(connection, "SELECT job_id, run_day::text, success_count, success_duration_sum FROM collect.job_outcome_daily ORDER BY run_day", ct));
        Assert.Equal("2026-11-20 11:45:00", await ScalarAsync<string>(connection, "SELECT (max_collection_time AT TIME ZONE 'UTC')::text FROM collect.job_outcome_daily_days WHERE run_day = DATE '2026-11-02'", ct));
        await AssertInvariantAsync(connection, ct);
    }

    /// <summary>
    /// Retention deletes the days more than two before job_history's own retention edge (the fleet override) from the rollup and the days table,
    /// keeps the rest and every watermark row, and moves every watermark row's gc_before up to the first day kept. A server that is already
    /// ahead stays there, and a later pass at an earlier edge does not move it back.
    /// </summary>
    [Fact]
    public async Task Retention_DeletesTheDaysTwoPastJobHistorysEdge_KeepsTheWatermark_AndMovesGcBeforeOnlyForward()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedServersAsync(connection, ct);
        await ExecuteAsync(connection, "INSERT INTO config.config_collector_schedules (server_id, collector_name, retention_days) VALUES (NULL, 'job_history', 10)", ct);

        /* Now is 2026-11-20, so the edge is 11-10 and the first day kept is 11-08. */
        await ExecuteAsync(connection, @"
INSERT INTO collect.job_outcome_daily (server_id, job_id, run_day, success_count, success_duration_sum, last_success_run_local) VALUES
    (9201, 'old', DATE '2026-11-07', 1, 1, '2026-11-07 01:00:00'),
    (9201, 'margin', DATE '2026-11-08', 1, 1, '2026-11-08 01:00:00'),
    (9201, 'edge', DATE '2026-11-10', 1, 1, '2026-11-10 01:00:00'),
    (9201, 'new', DATE '2026-11-18', 1, 1, '2026-11-18 01:00:00');
INSERT INTO collect.job_outcome_daily_days (server_id, run_day, max_collection_time) VALUES
    (9201, DATE '2026-11-07', now()), (9201, DATE '2026-11-08', now()), (9201, DATE '2026-11-10', now()), (9201, DATE '2026-11-18', now());
INSERT INTO collect.job_outcome_daily_watermark (server_id, collected_through, gc_before) VALUES (9201, now(), NULL), (9202, now(), DATE '2026-11-15')", ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var result = await JobOutcomeDaily.RunTickAsync(dataSource, new DateTime(2026, 11, 20, 12, 0, 0, DateTimeKind.Utc), NullLogger.Instance, ct);
        Assert.Equal(1, result.DaysRemoved);
        Assert.Equal(["edge", "margin", "new"], await ReadAsync(connection, "SELECT job_id FROM collect.job_outcome_daily ORDER BY job_id", ct));
        Assert.Equal(["2026-11-08", "2026-11-10", "2026-11-18"], await ReadAsync(connection, "SELECT run_day::text FROM collect.job_outcome_daily_days ORDER BY run_day", ct));
        Assert.Equal(2L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_outcome_daily_watermark WHERE server_id IN (9201, 9202)", ct));
        Assert.Equal(["9201 2026-11-08", "9202 2026-11-15"], await ReadAsync(connection, "SELECT server_id || ' ' || gc_before::text FROM collect.job_outcome_daily_watermark WHERE server_id IN (9201, 9202) ORDER BY server_id", ct));

        /* An earlier edge (retention raised to 30 days) deletes nothing and leaves gc_before where it was. */
        await ExecuteAsync(connection, "UPDATE config.config_collector_schedules SET retention_days = 30 WHERE collector_name = 'job_history'", ct);
        result = await JobOutcomeDaily.RunTickAsync(dataSource, new DateTime(2026, 11, 21, 12, 0, 0, DateTimeKind.Utc), NullLogger.Instance, ct);
        Assert.Equal(0, result.DaysRemoved);
        Assert.Equal(["9201 2026-11-08", "9202 2026-11-15"], await ReadAsync(connection, "SELECT server_id || ' ' || gc_before::text FROM collect.job_outcome_daily_watermark WHERE server_id IN (9201, 9202) ORDER BY server_id", ct));
    }

    /// <summary>The deletes and the gc_before move are one transaction: when the move fails, no day is gone and no gc_before moved.</summary>
    [Fact]
    public async Task Retention_WhenTheGcBeforeMoveFails_RollsTheDeletesBack()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedServersAsync(connection, ct);
        await ExecuteAsync(connection, @"
INSERT INTO collect.job_outcome_daily (server_id, job_id, run_day, success_count, success_duration_sum, last_success_run_local) VALUES
    (9201, 'old', DATE '2026-11-01', 1, 1, '2026-11-01 01:00:00');
INSERT INTO collect.job_outcome_daily_days (server_id, run_day, max_collection_time) VALUES (9201, DATE '2026-11-01', now());
INSERT INTO collect.job_outcome_daily_watermark (server_id, collected_through) VALUES (9201, now());
CREATE FUNCTION collect.refuse_gc_before() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'refused'; END $$;
CREATE TRIGGER refuse_gc_before BEFORE UPDATE ON collect.job_outcome_daily_watermark FOR EACH ROW EXECUTE FUNCTION collect.refuse_gc_before();", ct);

        await Assert.ThrowsAnyAsync<PostgresException>(() => JobOutcomeDaily.GcAsync(connection, new DateTime(2026, 11, 20, 12, 0, 0, DateTimeKind.Utc), 10, ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_outcome_daily WHERE run_day = DATE '2026-11-01'", ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_outcome_daily_days WHERE run_day = DATE '2026-11-01'", ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_outcome_daily_watermark WHERE gc_before IS NULL", ct));

        await ExecuteAsync(connection, "DROP TRIGGER refuse_gc_before ON collect.job_outcome_daily_watermark", ct);
        Assert.Equal(1, await JobOutcomeDaily.GcAsync(connection, new DateTime(2026, 11, 20, 12, 0, 0, DateTimeKind.Utc), 10, ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_outcome_daily_days", ct));
        Assert.Equal(["2026-11-08"], await ReadAsync(connection, "SELECT gc_before::text FROM collect.job_outcome_daily_watermark", ct));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static string JobRow(long id, int server, string? job, int step, int status, string run, int? duration, string collected) =>
        string.Create(CultureInfo.InvariantCulture,
            $"({900000000 + id}, '{collected}', {server}, 'jod-{server}', {id}, {(job is null ? "NULL" : $"'{job}'")}, {step}, {status}, '{run}', {(duration is null ? "NULL" : duration.Value.ToString(CultureInfo.InvariantCulture))}),");

    private static async Task SeedAsync(NpgsqlConnection connection, string valueRows, CancellationToken ct) =>
        await ExecuteAsync(connection, @"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, step_id, run_status, run_datetime, run_duration_seconds)
VALUES " + valueRows.TrimEnd(','), ct);

    private static async Task SeedServersAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await ExecuteAsync(connection, @"
INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES
    (9201, 'jod-9201', 'A', TRUE, 16, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC'),
    (9202, 'jod-9202', 'Offset', TRUE, 16, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC'),
    (9203, 'jod-9203', 'Disabled', FALSE, 16, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC'),
    (9204, 'jod-9204', 'NoJobs', TRUE, 16, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC');
INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes) VALUES
    (1, now() AT TIME ZONE 'UTC', 9202, 'jod-9202', 330)", ct);

    /// <summary>The invariant: per server with a watermark, the rollup equals a GROUP BY over the qualifying rows collected at or before it.</summary>
    private static async Task AssertInvariantAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var expected = await ReadAsync(connection, @"
SELECT jh.server_id, jh.job_id, jh.run_datetime::date::text, count(jh.run_duration_seconds), COALESCE(sum(jh.run_duration_seconds), 0), max(jh.run_datetime)::text
FROM collect.job_history AS jh
JOIN collect.job_outcome_daily_watermark AS w ON w.server_id = jh.server_id AND jh.collection_time <= (w.collected_through AT TIME ZONE 'UTC')
WHERE jh.step_id = 0 AND jh.run_status = 1 AND jh.job_id IS NOT NULL AND jh.run_datetime IS NOT NULL
GROUP BY jh.server_id, jh.job_id, jh.run_datetime::date ORDER BY 1, 2, 3", ct);
        var actual = await ReadAsync(connection, "SELECT server_id, job_id, run_day::text, success_count, success_duration_sum, last_success_run_local::text FROM collect.job_outcome_daily ORDER BY 1, 2, 3", ct);
        Assert.Equal(expected, actual);
        var days = await ReadAsync(connection, @"
SELECT jh.server_id, jh.run_datetime::date::text, (max(jh.collection_time))::text
FROM collect.job_history AS jh
JOIN collect.job_outcome_daily_watermark AS w ON w.server_id = jh.server_id AND jh.collection_time <= (w.collected_through AT TIME ZONE 'UTC')
WHERE jh.step_id = 0 AND jh.run_status = 1 AND jh.job_id IS NOT NULL AND jh.run_datetime IS NOT NULL
GROUP BY jh.server_id, jh.run_datetime::date ORDER BY 1, 2", ct);
        Assert.Equal(days, await ReadAsync(connection, "SELECT server_id, run_day::text, (max_collection_time AT TIME ZONE 'UTC')::text FROM collect.job_outcome_daily_days ORDER BY 1, 2", ct));
    }

    private static async Task<List<string>> ReadAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var cells = new string[reader.FieldCount];
            for (var i = 0; i < cells.Length; i++)
            {
                cells[i] = reader.IsDBNull(i) ? "NULL" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture)!;
            }

            rows.Add(string.Join('|', cells));
        }

        return rows;
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }
}
