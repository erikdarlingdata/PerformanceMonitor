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
    private const int ServerOffset = 9202;
    private const int ServerDisabled = 9203;

    private static readonly DateTime Now1 = new(2026, 11, 20, 12, 0, 0);

    // ── The rung ──────────────────────────────────────────────────────────────

    [Fact]
    public void TheRungIsTheNewestOnADenseLadder_AndTheProbeMapsItWhenItsTablesAreThere()
    {
        var rung = PgMigrations.Scripts[^1];
        Assert.Equal(JobOutcomeDaily.Version, rung.Version);
        Assert.Equal(StorageVersion.SchemaVersion, rung.Version);

        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Equal(rung.Version - 1, PgMigrations.Scripts[^2].Version);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        Assert.Equal("hasJobOutcomeDaily", parameters[^1].Name);
        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(rung.Version, (int)method.Invoke(null, all)!);
        all[^1] = false;
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

        await ExecuteAsync(connection, "DROP TABLE IF EXISTS collect.job_outcome_daily; DROP TABLE IF EXISTS collect.job_outcome_daily_built", ct);
        await ExecuteAsync(connection, PgMigrations.Scripts[^1].Sql, ct);
        await ExecuteAsync(connection, PgMigrations.Scripts[^1].Sql, ct);
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_outcome_daily", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_outcome_daily_built", ct));

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        Assert.Equal(StorageVersion.SchemaVersion, await viewer.GetStoreSchemaVersionAsync(ct));

        await ExecuteAsync(connection, "DROP TABLE collect.job_outcome_daily_built", ct);
        Assert.Equal(StorageVersion.SchemaVersion - 1, await viewer.GetStoreSchemaVersionAsync(ct));
    }

    // ── One built day ─────────────────────────────────────────────────────────

    /// <summary>
    /// A built day equals a GROUP BY over the raw successful job-outcome rows of that day: the count and sum of the non-NULL durations, the
    /// newest run (a NULL-duration success still counts for it), and the average the two make. Rows of another status, another step, no job id,
    /// the day before's last second and the next day's first second are not in it. The built row says how far its source reached.
    /// </summary>
    [Fact]
    public async Task ABuiltDay_EqualsAGroupByOverTheRawRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var day = new DateOnly(2026, 11, 15);
        var rows = new StringBuilder();
        long id = 1;
        void Add(string? job, int step, int status, string run, int? duration, string collected) =>
            rows.Append(JobRow(id++, ServerA, job, step, status, run, duration, collected));

        Add("A", 0, 1, "2026-11-15 00:00:00", 10, "2026-11-15 00:30:00");
        Add("A", 0, 1, "2026-11-15 08:00:00", 20, "2026-11-15 08:30:00");
        Add("A", 0, 1, "2026-11-15 23:59:59", 31, "2026-11-16 00:30:00");
        Add("A", 0, 1, "2026-11-15 12:00:00", null, "2026-11-15 12:30:00");
        Add("A", 0, 0, "2026-11-15 13:00:00", 999, "2026-11-15 13:30:00");
        Add("A", 1, 1, "2026-11-15 14:00:00", 888, "2026-11-15 14:30:00");
        Add("A", 0, 1, "2026-11-14 23:59:59", 777, "2026-11-15 00:30:00");
        Add("A", 0, 1, "2026-11-16 00:00:00", 666, "2026-11-16 00:30:00");
        Add("B", 0, 1, "2026-11-15 05:00:00", 7, "2026-11-15 05:30:00");
        Add("C", 0, 1, "2026-11-15 06:00:00", null, "2026-11-15 06:30:00");
        Add(null, 0, 1, "2026-11-15 07:00:00", 5, "2026-11-15 07:30:00");
        await SeedAsync(connection, rows.ToString(), ct);

        await JobOutcomeDaily.BuildDayAsync(connection, ServerA, day, Now1, ct);

        var expected = await ReadAsync(connection, @"
SELECT job_id, count(run_duration_seconds), COALESCE(sum(run_duration_seconds), 0), max(run_datetime)::text
FROM collect.job_history
WHERE server_id = 9201 AND step_id = 0 AND run_status = 1 AND job_id IS NOT NULL AND run_datetime::date = DATE '2026-11-15'
GROUP BY job_id ORDER BY job_id", ct);
        var actual = await ReadAsync(connection, @"
SELECT job_id, success_count, success_duration_sum, last_success_run_local::text
FROM collect.job_outcome_daily WHERE server_id = 9201 AND run_day = DATE '2026-11-15' ORDER BY job_id", ct);
        Assert.Equal(3, expected.Count);
        Assert.Equal(expected, actual);
        Assert.Contains("A|3|61|2026-11-15 23:59:59", actual);
        Assert.Contains("C|0|0|2026-11-15 06:00:00", actual);

        /* sum / count is AVG(run_duration_seconds), NULL durations left out, for the job with some and the one with none. */
        var avgRollup = await ReadAsync(connection, "SELECT job_id, (success_duration_sum::numeric / NULLIF(success_count, 0))::text FROM collect.job_outcome_daily ORDER BY job_id", ct);
        var avgRaw = await ReadAsync(connection, @"
SELECT job_id, AVG(run_duration_seconds)::text FROM collect.job_history
WHERE step_id = 0 AND run_status = 1 AND job_id IS NOT NULL AND run_datetime::date = DATE '2026-11-15' GROUP BY job_id ORDER BY job_id", ct);
        Assert.Equal(avgRaw, avgRollup);

        /* The built row: source_through is the newest collection_time the build read, at UTC. */
        var built = await ReadAsync(connection, "SELECT server_id, run_day::text, (source_through AT TIME ZONE 'UTC')::text, (built_at AT TIME ZONE 'UTC')::text FROM collect.job_outcome_daily_built", ct);
        Assert.Equal(["9201|2026-11-15|2026-11-16 00:30:00|2026-11-20 12:00:00"], built);

        /* A day with no success row is built and empty, and has no source. */
        await JobOutcomeDaily.BuildDayAsync(connection, ServerA, new DateOnly(2026, 11, 3), Now1, ct);
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_outcome_daily WHERE run_day = DATE '2026-11-03'", ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_outcome_daily_built WHERE run_day = DATE '2026-11-03' AND source_through IS NULL", ct));

        /* A rebuild replaces the day's rows: it does not add to them. */
        await JobOutcomeDaily.BuildDayAsync(connection, ServerA, day, Now1.AddHours(1), ct);
        Assert.Equal(expected, await ReadAsync(connection, @"
SELECT job_id, success_count, success_duration_sum, last_success_run_local::text
FROM collect.job_outcome_daily WHERE server_id = 9201 AND run_day = DATE '2026-11-15' ORDER BY job_id", ct));
    }

    // ── The ticks ─────────────────────────────────────────────────────────────

    /// <summary>
    /// The first ticks after the upgrade fill the days the store holds, newest first, and only days that have ended on the server's own clock
    /// and settled for <see cref="JobOutcomeDaily.BuildAfter"/>. A server on a clock ahead of UTC has its day built when its own midnight has
    /// passed. A server that is not enabled, and a server with no job_history, are never planned. A tick with nothing left to do does nothing.
    /// </summary>
    [Fact]
    public async Task TheFirstTicksBackfillTheExistingDays_NewestFirst_OnEachServersOwnClock()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedServersAsync(connection, ct);
        await SetRetentionAsync(connection, 10, ct);

        var rows = new StringBuilder();
        long id = 1;
        foreach (var server in new[] { ServerA, ServerOffset, ServerDisabled })
        {
            for (var d = 12; d <= 20; d++)
            {
                var run = $"2026-11-{d:00} 09:00:00";
                rows.Append(JobRow(id++, server, "A", 0, 1, run, 10 + d, $"2026-11-{d:00} 09:30:00"));
            }
        }

        await SeedAsync(connection, rows.ToString(), ct);

        /* 21:00 UTC on the 20th is 02:30 on the 21st for the server at +05:30, so the 20th is closed and settled there and not on the other. */
        var now = new DateTime(2026, 11, 20, 21, 0, 0);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var plan = await PlanAsync(connection, now, ct);
        Assert.Equal((ServerOffset, new DateOnly(2026, 11, 20)), plan[0]);
        Assert.Equal((ServerA, new DateOnly(2026, 11, 19)), plan[1]);
        Assert.DoesNotContain(plan, p => p.ServerId == ServerDisabled);
        Assert.DoesNotContain(plan, p => p.ServerId == ServerA && p.Day == new DateOnly(2026, 11, 20));
        Assert.Equal(plan.OrderByDescending(p => p.Day).ThenBy(p => p.ServerId).ToList(), plan);

        var first = await JobOutcomeDaily.RunTickAsync(dataSource, now, NullLogger.Instance, ct);
        Assert.Equal(0, first.Failed);
        Assert.Equal(plan.Count, first.Built);

        var built = await ReadAsync(connection, "SELECT server_id, run_day::text FROM collect.job_outcome_daily_built ORDER BY server_id, run_day", ct);
        var expectedA = Enumerable.Range(10, 10).Select(d => $"{ServerA}|2026-11-{d:00}");
        var expectedOffset = Enumerable.Range(10, 11).Select(d => $"{ServerOffset}|2026-11-{d:00}");
        Assert.Equal(expectedA.Concat(expectedOffset).ToList(), built);

        /* Each built day with a run holds it, and the day the offset server's clock had just closed holds that day's run. */
        Assert.Equal("9202|A|1|30", (await ReadAsync(connection, "SELECT server_id, job_id, success_count, success_duration_sum FROM collect.job_outcome_daily WHERE server_id = 9202 AND run_day = DATE '2026-11-20'", ct)).Single());
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_outcome_daily WHERE server_id = 9201 AND run_day = DATE '2026-11-20'", ct));

        /* Everything due is built, and nothing is due again until a day closes. */
        var second = await JobOutcomeDaily.RunTickAsync(dataSource, now, NullLogger.Instance, ct);
        Assert.Equal(0, second.Built);
        Assert.Equal(0, second.Failed);
        Assert.Equal(0, second.Deferred);
    }

    /// <summary>
    /// A rebuild a day later (the end of the day plus <see cref="JobOutcomeDaily.FinalBuildAfter"/>) picks up a row that reached the store after
    /// the first build, once, and a day built after that instant is not built again.
    /// </summary>
    [Fact]
    public async Task ARebuildADayLater_PicksUpALateRow_Once()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedServersAsync(connection, ct);
        await SetRetentionAsync(connection, 3, ct);

        await SeedAsync(connection, JobRow(1, ServerA, "A", 0, 1, "2026-11-19 09:00:00", 10, "2026-11-19 09:30:00"), ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await JobOutcomeDaily.RunTickAsync(dataSource, Now1, NullLogger.Instance, ct);
        Assert.Equal("A|1|10", (await ReadAsync(connection, "SELECT job_id, success_count, success_duration_sum FROM collect.job_outcome_daily WHERE run_day = DATE '2026-11-19'", ct)).Single());
        var firstThrough = await ScalarAsync<DateTime>(connection, "SELECT source_through AT TIME ZONE 'UTC' FROM collect.job_outcome_daily_built WHERE run_day = DATE '2026-11-19'", ct);
        Assert.Equal(new DateTime(2026, 11, 19, 9, 30, 0), firstThrough);

        /* A row of the 19th that reached the store after the first build. Not due yet: the 19th ends at midnight and the second build is 26 hours later. */
        await SeedAsync(connection, JobRow(2, ServerA, "A", 0, 1, "2026-11-19 22:00:00", 30, "2026-11-20 13:00:00"), ct);
        var early = await JobOutcomeDaily.RunTickAsync(dataSource, Now1.AddHours(10), NullLogger.Instance, ct);
        Assert.DoesNotContain(await PlanAsync(connection, Now1.AddHours(10), ct), p => p.Day == new DateOnly(2026, 11, 19));
        Assert.Equal("A|1|10", (await ReadAsync(connection, "SELECT job_id, success_count, success_duration_sum FROM collect.job_outcome_daily WHERE run_day = DATE '2026-11-19'", ct)).Single());
        Assert.Equal(0, early.Failed);

        /* 03:00 on the 21st is past the 19th's end plus 26 hours. */
        var late = new DateTime(2026, 11, 21, 3, 0, 0);
        Assert.Contains(await PlanAsync(connection, late, ct), p => p.Day == new DateOnly(2026, 11, 19));
        await JobOutcomeDaily.RunTickAsync(dataSource, late, NullLogger.Instance, ct);
        Assert.Equal("A|2|40", (await ReadAsync(connection, "SELECT job_id, success_count, success_duration_sum FROM collect.job_outcome_daily WHERE run_day = DATE '2026-11-19'", ct)).Single());
        Assert.Equal(new DateTime(2026, 11, 20, 13, 0, 0), await ScalarAsync<DateTime>(connection, "SELECT source_through AT TIME ZONE 'UTC' FROM collect.job_outcome_daily_built WHERE run_day = DATE '2026-11-19'", ct));
        Assert.DoesNotContain(await PlanAsync(connection, late, ct), p => p.Day == new DateOnly(2026, 11, 19));
    }

    /// <summary>
    /// Retention deletes the days before job_history's own retention edge from both tables, with the rows of a server that is not enabled, and
    /// keeps the rest. The edge follows the fleet retention override for job_history.
    /// </summary>
    [Fact]
    public async Task Retention_DeletesTheDaysPastJobHistorysHorizon_AndTheRowsOfADisabledServer()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedServersAsync(connection, ct);
        await SetRetentionAsync(connection, 10, ct);

        await ExecuteAsync(connection, @"
INSERT INTO collect.job_outcome_daily (server_id, job_id, run_day, success_count, success_duration_sum, last_success_run_local) VALUES
    (9201, 'old', DATE '2026-11-09', 1, 1, '2026-11-09 01:00:00'),
    (9201, 'edge', DATE '2026-11-10', 1, 1, '2026-11-10 01:00:00'),
    (9201, 'new', DATE '2026-11-18', 1, 1, '2026-11-18 01:00:00'),
    (9203, 'off', DATE '2026-11-18', 1, 1, '2026-11-18 01:00:00');
INSERT INTO collect.job_outcome_daily_built (server_id, run_day, built_at, source_through) VALUES
    (9201, DATE '2026-11-09', now(), now()),
    (9201, DATE '2026-11-10', now(), now()),
    (9201, DATE '2026-11-18', now(), now()),
    (9203, DATE '2026-11-18', now(), now())", ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var result = await JobOutcomeDaily.RunTickAsync(dataSource, Now1, NullLogger.Instance, ct);
        Assert.Equal(2, result.DaysRemoved);

        Assert.Equal(["edge", "new"], (await ReadAsync(connection, "SELECT job_id FROM collect.job_outcome_daily ORDER BY job_id", ct)));
        Assert.Equal(["9201|2026-11-10", "9201|2026-11-18"], (await ReadAsync(connection, "SELECT server_id, run_day::text FROM collect.job_outcome_daily_built ORDER BY run_day", ct)));
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

    private static async Task SetRetentionAsync(NpgsqlConnection connection, int days, CancellationToken ct) =>
        await ExecuteAsync(connection, $"INSERT INTO config.config_collector_schedules (server_id, collector_name, retention_days) VALUES (NULL, 'job_history', {days})", ct);

    private static async Task<List<(int ServerId, DateOnly Day)>> PlanAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        var retention = await ScalarAsync<int>(connection, JobOutcomeDaily.RetentionDaysSql, ct);
        var plan = await JobOutcomeDaily.PlanBuildsAsync(connection, now, retention, ct);
        return plan.Select(b => (b.ServerId, b.Day)).ToList();
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
