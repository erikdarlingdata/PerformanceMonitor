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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every live test mints its own scratch database through ScratchPostgres and touches nothing shared. */

/// <summary>
/// #8549: the Job History Category list reads <c>collect.job_history_category_hourly</c> (V265) where the pass's coverage holds the window and the
/// old statement everywhere else. The answer must be the old statement's, for several servers on different clocks (a negative offset, a
/// positive one with a half hour, a NULL offset, none collected), empty and NULL category names, categories seen only in an edge hour (inside and
/// outside each bound), the bounded and unbounded forms, a one-server read, a table the pass has only partly covered and a table that is missing.
/// </summary>
public sealed class JobHistoryCategoryHourlyLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #8549 job history category tests.";
    private static readonly int[] Servers = [9101, 9102, 9103, 9104];

    // ── The rung ────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void TheRungIsNewestOnADenseLadder_AndTheProbeMapsItToTheNewestVersion()
    {
        var rung = PgMigrations.Scripts[^1];
        Assert.Equal("job-history-category-hourly", rung.Name);
        Assert.Equal(StorageVersion.SchemaVersion, rung.Version);
        Assert.Contains("collect.job_history_category_hourly", rung.Sql, StringComparison.Ordinal);
        Assert.Contains("collect.job_history_category_coverage", rung.Sql, StringComparison.Ordinal);
        Assert.Contains("to_regclass('collect.job_history_category_hourly') IS NOT NULL", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        Assert.Equal("hasJobHistoryCategoryHourly", parameters[^1].Name);
        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(rung.Version, (int)method.Invoke(null, all)!);
        var without = (object[])all.Clone();
        without[^1] = false;
        Assert.Equal(rung.Version - 1, (int)method.Invoke(null, without)!);
    }

    [Fact]
    public async Task TheRungMakesBothTablesEmpty_AndRunsTwice()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);

        await ExecuteAsync(connection, "DROP TABLE IF EXISTS collect.job_history_category_hourly; DROP TABLE IF EXISTS collect.job_history_category_coverage", ct);
        await ExecuteAsync(connection, PgMigrations.Scripts[^1].Sql, ct);
        await ExecuteAsync(connection, PgMigrations.Scripts[^1].Sql, ct);
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_history_category_hourly", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_history_category_coverage", ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection, "SELECT count(*) FROM pg_constraint WHERE conname = 'pk_job_history_category_hourly'", ct));
    }

    // ── The pass's arithmetic ───────────────────────────────────────────────────────────────

    [Fact]
    public void ThePlan_JoinsTheOldCoverage_WalksBackADay_AndHoldsTheRetentionEdge()
    {
        var hour = new DateTime(2026, 11, 12, 10, 0, 0);

        /* First pass: the last two closed hours, then a day behind them. */
        var first = JobHistoryCategoryHourly.Plan(null, null, 365, hour);
        Assert.Equal(hour.AddHours(-2), first.ForwardFrom);
        Assert.Equal(hour.AddHours(-2).AddDays(-1), first.BackfillFrom);
        Assert.Equal(hour.AddHours(-2), first.BackfillTo);
        Assert.Equal(hour.AddHours(-2).AddDays(-1), first.CoveredFrom);

        /* Later pass: the forward range starts two hours before the old end, the backfill continues from the old start. */
        var later = JobHistoryCategoryHourly.Plan(hour.AddDays(-3), hour.AddHours(-1), 365, hour);
        Assert.Equal(hour.AddHours(-3), later.ForwardFrom);
        Assert.Equal(hour.AddDays(-4), later.BackfillFrom);
        Assert.Equal(hour.AddDays(-3), later.BackfillTo);

        /* The coverage reaches the retention edge: no backfill, and the start rides the edge as it moves. */
        var done = JobHistoryCategoryHourly.Plan(hour.AddDays(-365).AddHours(-1), hour.AddHours(-1), 365, hour);
        Assert.Null(done.BackfillFrom);
        Assert.Equal(hour.AddDays(-365), done.CoveredFrom);

        /* A backfill never walks past the edge. */
        var near = JobHistoryCategoryHourly.Plan(hour.AddDays(-364).AddHours(-6), hour.AddHours(-1), 365, hour);
        Assert.Equal(hour.AddDays(-365), near.BackfillFrom);
    }

    [Fact]
    public void TheCoverageRule_NeedsTheFloorInsideTheCoverage_AndAWindowThatIsNotBackwards()
    {
        var since = new DateTime(2026, 11, 10, 6, 20, 0);
        var floor = since.AddDays(-1);
        var coverage = new JobHistoryCategoryHourly.Coverage(floor, since.AddDays(3));

        Assert.True(JobHistoryCategoryHourly.Covers(coverage, floor, since, null));
        Assert.True(JobHistoryCategoryHourly.Covers(coverage, floor, since, since.AddHours(1)));
        Assert.False(JobHistoryCategoryHourly.Covers(coverage, floor, since, since.AddHours(-1)), "a backwards window is the old statement's");
        Assert.False(JobHistoryCategoryHourly.Covers(coverage with { From = floor.AddMinutes(1) }, floor, since, null), "coverage starting after the floor is partial");
    }

    // ── The answers ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Old statement against the read, the table partly covered, then whole, then missing: the same categories in the same order for every window.
    /// </summary>
    [Fact]
    public async Task TheCategoryRead_EqualsTheOldStatement_PartlyCovered_Covered_AndWithTheTableMissing()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var now = await SeedAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* No pass has run: no coverage row, so every window reads the old statement. */
        await AssertAllWindowsEqualAsync(dataSource, now, ct);

        /* One pass covers the last two hours and a day behind them: the 3-day windows are still the old statement's. */
        await JobHistoryCategoryHourly.RollUpAsync(connection, now, null, ct);
        var partial = await CoverageAsync(connection, ct);
        Assert.True(partial.To == Floor(now));
        Assert.True(partial.From > Floor(now).AddDays(-2));
        await AssertAllWindowsEqualAsync(dataSource, now, ct);

        /* The backfill, a day a pass, until the coverage holds the 12 days seeded and the windows read the summary. */
        for (var pass = 0; pass < 14; pass++)
        {
            await JobHistoryCategoryHourly.RollUpAsync(connection, now, null, ct);
        }

        Assert.True((await CoverageAsync(connection, ct)).From <= Floor(now).AddDays(-12));
        await AssertAllWindowsEqualAsync(dataSource, now, ct);

        /* A pass again changes no row. */
        var before = await RowsAsync(connection, ct);
        await JobHistoryCategoryHourly.RollUpAsync(connection, now, null, ct);
        Assert.Equal(before, await RowsAsync(connection, ct));

        /* The runs the pass has not folded (the open hour) come from the raw part. */
        Assert.Contains("Cat_Tail", await DarlingJobHistoryReader.GetCategoriesAsync(dataSource, Floor(now).AddDays(-2), null, 60, cancellationToken: ct));

        /* A store that does not have the tables reads the old statement. */
        await ExecuteAsync(connection, "DROP TABLE collect.job_history_category_hourly; DROP TABLE collect.job_history_category_coverage", ct);
        await AssertAllWindowsEqualAsync(dataSource, now, ct);
    }

    /// <summary>
    /// What a covered window reads is the summary: with the raw runs of the interior hours gone (as retention drops a chunk, or any delete does)
    /// the category that only they held is still offered, and a category that only an edge hour holds is decided by that hour's smallest and
    /// largest run. The old statement offers neither interior category here, so this fails on it.
    /// </summary>
    [Fact]
    public async Task ACoveredWindow_ReadsTheSummary_NotJobHistory_ForItsInteriorHours()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var now = await SeedAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        for (var pass = 0; pass < 15; pass++)
        {
            await JobHistoryCategoryHourly.RollUpAsync(connection, now, null, ct);
        }

        var since = Floor(now).AddDays(-6).AddMinutes(20);
        var viaSummary = await DarlingJobHistoryReader.GetCategoriesAsync(dataSource, since, null, 60, cancellationToken: ct);
        Assert.Contains("Cat_Interior", viaSummary);
        Assert.Equal(await OldAsync(dataSource, since, null, null, now, ct), viaSummary);

        /* Delete every raw run of that category: only the summary holds them now. */
        await ExecuteAsync(connection, "DELETE FROM collect.job_history WHERE category_name = 'Cat_Interior'", ct);
        Assert.DoesNotContain("Cat_Interior", await OldAsync(dataSource, since, null, null, now, ct));
        Assert.Contains("Cat_Interior", await DarlingJobHistoryReader.GetCategoriesAsync(dataSource, since, null, 60, cancellationToken: ct));

        /* The window's own raw part still counts: a run collected after the coverage, in a category the summary lacks. */
        await ExecuteAsync(connection, @"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, category_name, run_datetime, run_status)
VALUES (990000001, (now() AT TIME ZONE 'utc'), 9104, 'jh-cat', 1, 'j', 'late', 'Cat_Fresh', (now() AT TIME ZONE 'utc') - interval '1 minute', 1)", ct);
        Assert.Contains("Cat_Fresh", await DarlingJobHistoryReader.GetCategoriesAsync(dataSource, since, null, 60, cancellationToken: ct));
    }

    /// <summary>
    /// A run older than the retention edge is neither folded nor kept, and the coverage never starts before the edge, so a window reaching past it
    /// reads the old statement and the answers stay equal.
    /// </summary>
    [Fact]
    public async Task ARunOlderThanRetention_IsNotKept_AndAWindowPastTheEdgeReadsTheOldStatement()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var now = await SeedAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var retentionDays = await ScalarAsync<int>(connection, "SELECT " + DarlingRetentionHorizons.FleetRetentionDaysSql("job_history"), ct);
        await ExecuteAsync(connection, $@"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, category_name, run_datetime, run_status)
VALUES (990000002, '{Floor(now).AddDays(-1):yyyy-MM-dd HH:mm:ss}', 9104, 'jh-cat', 1, 'j', 'ancient', 'Cat_Ancient', '{Floor(now).AddDays(-retentionDays - 30):yyyy-MM-dd HH:mm:ss}', 1)", ct);
        for (var pass = 0; pass < 3; pass++)
        {
            await JobHistoryCategoryHourly.RollUpAsync(connection, now, null, ct);
        }

        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_history_category_hourly WHERE category_name = 'Cat_Ancient'", ct));
        var since = Floor(now).AddDays(-retentionDays - 40);
        var read = await DarlingJobHistoryReader.GetCategoriesAsync(dataSource, since, null, 60, cancellationToken: ct);
        Assert.Contains("Cat_Ancient", read);
        Assert.Equal(await OldAsync(dataSource, since, null, null, now, ct), read);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────────────────

    private static DateTime Floor(DateTime t) => new(t.Year, t.Month, t.Day, t.Hour, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The windows the old statement and the read are compared on: bounded and unbounded, all servers and one, edges seeded and not.</summary>
    private static async Task AssertAllWindowsEqualAsync(NpgsqlDataSource dataSource, DateTime now, CancellationToken ct)
    {
        var s = EdgeSince(now);
        var u = EdgeUntil(now);
        var windows = new List<(DateTime Since, DateTime? Until, int? Server)>
        {
            (s, null, null),
            (s, u, null),
            (s, u, 9102),
            (s, null, 9103),
            (s, u, 9103),
            (s, null, 9104),
            (s, u, 9101),
            (Floor(now).AddDays(-5).AddMinutes(7), null, null),
            (Floor(now).AddDays(-5).AddMinutes(7), Floor(now).AddDays(-2).AddMinutes(41), null),
            (Floor(now).AddDays(-1).AddMinutes(33), null, null),
            (Floor(now).AddDays(-1).AddMinutes(33), null, 9101),
            (Floor(now).AddDays(-100), null, null),
            (s, s.AddHours(-3), null),
            (s, s, null),
            (Floor(now).AddHours(-1), null, null),
        };

        foreach (var (since, until, server) in windows)
        {
            var expected = await OldAsync(dataSource, since, until, server, now, ct);
            var actual = await DarlingJobHistoryReader.GetCategoriesAsync(dataSource, since, until, 60, server, ct);
            Assert.True(expected.SequenceEqual(actual),
                $"window {since:u} to {until:u} server {server}: old [{string.Join(",", expected)}] new [{string.Join(",", actual)}]");
        }

        /* The seeded edges decide as the old statement decides: the run inside each bound is offered, the one outside it is not. */
        var all = await DarlingJobHistoryReader.GetCategoriesAsync(dataSource, s, u, 60, cancellationToken: ct);
        foreach (var id in Servers)
        {
            Assert.Contains($"E_LoIn_{id}", all);
            Assert.DoesNotContain($"E_LoOut_{id}", all);
            Assert.Contains($"E_HiIn_{id}", all);
            Assert.DoesNotContain($"E_HiOut_{id}", all);
        }

        Assert.DoesNotContain(string.Empty, all);
        Assert.Contains("Cat_A", all);
        Assert.Contains("Cat_B", all);
        Assert.Contains("Cat_C", all);
    }

    /// <summary>The old statement, bound the way <see cref="DarlingJobHistoryReader.GetCategoriesAsync"/> binds it.</summary>
    private static async Task<List<string>> OldAsync(NpgsqlDataSource dataSource, DateTime since, DateTime? until, int? server, DateTime now, CancellationToken ct)
    {
        _ = now;
        var result = new List<string>();
        await using var command = dataSource.CreateCommand(DarlingJobHistoryReader.BuildCategoriesSql(until.HasValue, server.HasValue));
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = since });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(since) });
        if (until is { } u)
        {
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = u });
        }

        if (server is { } id)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = id });
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(reader.GetString(0));
        }

        return result;
    }

    private static DateTime EdgeSince(DateTime now) => Floor(now).AddDays(-3).AddMinutes(20);

    private static DateTime EdgeUntil(DateTime now) => Floor(now).AddDays(-1).AddMinutes(20);

    /// <summary>
    /// Four servers, 12 days of a half-hourly collection each, on different clocks: 9101 at UTC-8, 9102 at UTC+5:30, 9103 with a NULL offset
    /// collected, 9104 with none. A run's wall-clock time is its collection's UTC time plus the offset, a little before it. 9101 also holds runs with an empty
    /// and a NULL category. Each server holds runs in a category of its own only inside and only outside the window's two bounds (the edge
    /// hour's run before the bound, or after it), and one category held only by runs of the interior hours (<c>Cat_Interior</c>). A run in the
    /// open hour holds <c>Cat_Tail</c>. Returns the clock the pass is run on.
    /// </summary>
    private static async Task<DateTime> SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var now = await ScalarAsync<DateTime>(connection, "SELECT now() AT TIME ZONE 'utc'", ct);
        var nowHour = Floor(now);
        await ExecuteAsync(connection, @"
INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes) VALUES
    (1, now() AT TIME ZONE 'utc', 9101, 'jh-cat-a', -480),
    (2, now() AT TIME ZONE 'utc', 9102, 'jh-cat-b', 330),
    (3, now() AT TIME ZONE 'utc', 9103, 'jh-cat-c', NULL)", ct);

        await ExecuteAsync(connection, $@"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, category_name, run_datetime, run_status)
SELECT 900000000 + row_number() OVER (), t, s.server_id, 'jh-cat', 1, 'j', 'job',
       CASE WHEN s.server_id = 9102 AND extract(hour FROM t)::int % 2 = 1 THEN 'Cat_B'
            WHEN s.server_id = 9103 THEN 'Cat_C'
            ELSE 'Cat_A' END,
       t - interval '5 minutes' + make_interval(mins => s.off),
       1
FROM generate_series('{nowHour.AddDays(-12):yyyy-MM-dd HH:mm:ss}'::timestamp, '{nowHour:yyyy-MM-dd HH:mm:ss}'::timestamp - interval '1 minute', interval '30 minutes') AS t
CROSS JOIN (VALUES (9101, -480), (9102, 330), (9103, 0), (9104, 0)) AS s(server_id, off)", ct);

        /* An empty and a NULL category, and a category only the interior hours hold. */
        await ExecuteAsync(connection, $@"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, category_name, run_datetime, run_status)
SELECT 910000000 + row_number() OVER (), t, 9101, 'jh-cat', 1, 'j', 'odd',
       CASE row_number() OVER () % 3 WHEN 0 THEN '' WHEN 1 THEN NULL ELSE 'Cat_Interior' END,
       t - interval '5 minutes' - interval '8 hours', 1
FROM generate_series('{nowHour.AddDays(-9):yyyy-MM-dd HH:mm:ss}'::timestamp, '{nowHour.AddDays(-1):yyyy-MM-dd HH:mm:ss}'::timestamp, interval '1 hour') AS t", ct);

        /* The edge hours: per server, a run five minutes before and five minutes after each bound, on the server's own clock. */
        var lo = EdgeSince(now);
        var hi = EdgeUntil(now);
        foreach (var (id, offset) in new[] { (9101, -480), (9102, 330), (9103, 0), (9104, 0) })
        {
            foreach (var (category, bound, shift) in new[]
            {
                ($"E_LoOut_{id}", lo.AddHours(-1), -5),
                ($"E_LoIn_{id}", lo.AddHours(-1), 5),
                ($"E_HiIn_{id}", hi.AddHours(1), -5),
                ($"E_HiOut_{id}", hi.AddHours(1), 5),
            })
            {
                var local = bound.AddMinutes(offset + shift);
                var collected = local.AddMinutes(-offset + 3);
                await ExecuteAsync(connection, $@"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, category_name, run_datetime, run_status)
VALUES ({920000000 + id * 10 + Math.Abs(category.GetHashCode() % 7)}, '{collected:yyyy-MM-dd HH:mm:ss}', {id}, 'jh-cat', 1, 'j', 'edge', '{category}', '{local:yyyy-MM-dd HH:mm:ss}', 1)", ct);
            }
        }

        await ExecuteAsync(connection, $@"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, category_name, run_datetime, run_status)
VALUES (930000001, '{nowHour.AddMinutes(1):yyyy-MM-dd HH:mm:ss}', 9104, 'jh-cat', 1, 'j', 'tail', 'Cat_Tail', '{nowHour.AddMinutes(-2):yyyy-MM-dd HH:mm:ss}', 1)", ct);
        return now;
    }

    private static async Task<JobHistoryCategoryHourly.Coverage> CoverageAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(JobHistoryCategoryHourly.CoverageReadSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "a pass wrote the coverage row");
        return new JobHistoryCategoryHourly.Coverage(reader.GetDateTime(0), reader.GetDateTime(1));
    }

    private static async Task<List<string>> RowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = new NpgsqlCommand("SELECT server_id || '|' || run_hour || '|' || category_name || '|' || first_run || '|' || last_run FROM collect.job_history_category_hourly ORDER BY 1", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(reader.GetString(0));
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
