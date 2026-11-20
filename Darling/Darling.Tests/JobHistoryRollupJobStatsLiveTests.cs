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

/* #1776 own-store: every live test mints its own scratch database through ScratchPostgres and touches nothing shared. */

/// <summary>
/// #9110: the Job History read's per-job average successful duration and last success (<c>job_stats</c>) from the daily summary
/// (<c>job_outcome_daily</c>, its days table and the server's watermark) must equal the read of <c>job_history</c> alone, row for row.
/// The two reads are compared whole: <see cref="DarlingJobHistoryRow"/> carries the last success and <c>IsLongRunning</c>, and every job
/// has a ladder of failed step-0 rows of 61 to 700 seconds (three apart), each flagged exactly when it is more than twice the job's average,
/// so a difference in the average of a few seconds changes some row's flag.
/// </summary>
public sealed class JobHistoryRollupJobStatsLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #9110 job_stats summary tests.";
    private const int ServerA = 9301;
    private const int ServerOffset = 9302;

    /// <summary>The windows read: the past day, 7 days and 30 days, each with no end, and two that ended in the past.</summary>
    private static readonly (TimeSpan Span, TimeSpan? EndAgo)[] Windows =
    [
        (TimeSpan.FromDays(1), null),
        (TimeSpan.FromDays(7), null),
        (TimeSpan.FromDays(30), null),
        (TimeSpan.FromDays(7), TimeSpan.FromDays(2) + TimeSpan.FromHours(5)),
        (TimeSpan.FromDays(30), TimeSpan.FromDays(9) + TimeSpan.FromMinutes(40)),
    ];

    [Fact]
    public async Task WithTheSummaryBuiltToTheHour_EveryWindowReadsWhatTheRawRowsRead()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var anchor = await SeedAsync(dataSource, ct);
        await InsertLateRowsAsync(dataSource, aboveTheWatermark: false, ct);
        await InsertLateRowsAsync(dataSource, aboveTheWatermark: true, ct);

        await RunTickAsync(dataSource, null, ct);
        Assert.True(await DarlingJobHistoryReader.RollupTablesExistAsync(dataSource, 60, ct));
        Assert.True(await ScalarAsync<long>(dataSource, "SELECT count(*) FROM collect.job_outcome_daily", ct) > 100);

        await AssertEveryReadMatchesAsync(dataSource, anchor, ct);
    }

    /// <summary>
    /// The rows collected in the last half hour reach the store after the summary was built, so they are read raw above the watermark; a later
    /// tick then counts them in the summary, and the read does not change.
    /// </summary>
    [Fact]
    public async Task ARowCollectedAfterTheSummaryWasBuilt_IsReadAboveTheWatermark_AndOnceMoreAfterTheNextTickCountsIt()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var anchor = await SeedAsync(dataSource, ct);
        await InsertLateRowsAsync(dataSource, aboveTheWatermark: false, ct);

        await RunTickAsync(dataSource, null, ct);
        await InsertLateRowsAsync(dataSource, aboveTheWatermark: true, ct);
        await AssertEveryReadMatchesAsync(dataSource, anchor, ct);

        var counted = await ScalarAsync<long>(dataSource, "SELECT sum(success_count)::bigint FROM collect.job_outcome_daily", ct);
        await RunTickAsync(dataSource, anchor + TimeSpan.FromHours(2), ct);
        Assert.True(await ScalarAsync<long>(dataSource, "SELECT sum(success_count)::bigint FROM collect.job_outcome_daily", ct) > counted);
        await AssertEveryReadMatchesAsync(dataSource, anchor, ct);
    }

    /// <summary>A server with no watermark row has nothing counted, so every window reads the raw rows for it, and the other server still reads its summary.</summary>
    [Fact]
    public async Task AServerWithNoWatermark_ReadsRaw_WhileTheOtherReadsItsSummary()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var anchor = await SeedAsync(dataSource, ct);
        await InsertLateRowsAsync(dataSource, aboveTheWatermark: false, ct);
        await InsertLateRowsAsync(dataSource, aboveTheWatermark: true, ct);

        await AssertEveryReadMatchesAsync(dataSource, anchor, ct);

        await RunTickAsync(dataSource, null, ct);
        await ExecuteAsync(dataSource, $"DELETE FROM collect.job_outcome_daily_watermark WHERE server_id = {ServerOffset}", ct);
        await ExecuteAsync(dataSource, $"DELETE FROM collect.job_outcome_daily WHERE server_id = {ServerOffset}", ct);
        await ExecuteAsync(dataSource, $"DELETE FROM collect.job_outcome_daily_days WHERE server_id = {ServerOffset}", ct);
        await AssertEveryReadMatchesAsync(dataSource, anchor, ct);
    }

    /// <summary>The summary built only to a point inside the windows (a store still backfilling): the days past it are raw rows above the watermark.</summary>
    [Fact]
    public async Task AStoreStillBackfilling_ReadsTheDaysPastTheWatermarkRaw()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var anchor = await SeedAsync(dataSource, ct);
        await InsertLateRowsAsync(dataSource, aboveTheWatermark: false, ct);
        await InsertLateRowsAsync(dataSource, aboveTheWatermark: true, ct);

        await RunTickAsync(dataSource, anchor - TimeSpan.FromDays(3) - TimeSpan.FromHours(4), ct);
        var through = await ScalarAsync<DateTime>(dataSource, $"SELECT collected_through AT TIME ZONE 'UTC' FROM collect.job_outcome_daily_watermark WHERE server_id = {ServerA}", ct);
        Assert.True(through < anchor - TimeSpan.FromDays(3) && through > anchor - TimeSpan.FromDays(4), through.ToString("o", CultureInfo.InvariantCulture));
        await AssertEveryReadMatchesAsync(dataSource, anchor, ct);

        await RunTickAsync(dataSource, anchor - TimeSpan.FromDays(1) - TimeSpan.FromHours(13), ct);
        await AssertEveryReadMatchesAsync(dataSource, anchor, ct);
    }

    /// <summary>
    /// The equality tests would pass if the read never touched the summary. Bumping one full day's stored sum moves that job's average, so the
    /// read that uses the summary must differ from the raw one; the same bump on the window's first day must change nothing, because an edge day
    /// is read raw.
    /// </summary>
    [Fact]
    public async Task TheFullDaysOfAWindowAreReadFromTheSummary_AndItsEdgeDaysAreNot()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var anchor = await SeedAsync(dataSource, ct);
        await RunTickAsync(dataSource, null, ct);

        const string bump = "UPDATE collect.job_outcome_daily SET success_duration_sum = success_duration_sum + {0} WHERE server_id = 9301 AND job_id = 'job1' AND run_day = ((now() AT TIME ZONE 'UTC') - interval '{1} days')::date";
        var since = anchor - TimeSpan.FromDays(7);
        var raw = await DarlingJobHistoryReader.GetWithoutSummaryAsync(dataSource, since, ServerA, 100000, 60, null, ct);
        var ladder = raw.Where(r => r.JobId == "job1" && r.StepId == 0 && r.RunStatus == 0).ToList();
        Assert.Contains(ladder, r => r.IsLongRunning);
        Assert.Contains(ladder, r => !r.IsLongRunning);

        /* Five days back is a full day of the 7-day window: not its first day and not the day the floor cuts. */
        await ExecuteAsync(dataSource, string.Format(CultureInfo.InvariantCulture, bump, 100000, 5), ct);
        var rolled = await DarlingJobHistoryReader.GetAsync(dataSource, since, ServerA, 100000, 60, null, ct);
        Assert.Equal(raw.Count, rolled.Count);
        Assert.NotEqual(ladder.Select(r => r.IsLongRunning), rolled.Where(r => r.JobId == "job1" && r.StepId == 0 && r.RunStatus == 0).Select(r => r.IsLongRunning));

        await ExecuteAsync(dataSource, string.Format(CultureInfo.InvariantCulture, bump, -100000, 5), ct);
        await ExecuteAsync(dataSource, string.Format(CultureInfo.InvariantCulture, bump, 100000, 7), ct);
        AssertSame(raw, await DarlingJobHistoryReader.GetAsync(dataSource, since, ServerA, 100000, 60, null, ct), "first day bumped");
    }

    // ── The comparison ────────────────────────────────────────────────────────

    private static async Task AssertEveryReadMatchesAsync(NpgsqlDataSource dataSource, DateTime anchor, CancellationToken ct)
    {
        foreach (var (span, endAgo) in Windows)
        {
            foreach (var serverId in new int?[] { null, ServerA, ServerOffset })
            {
                foreach (var limit in new[] { 5, 100000 })
                {
                    var since = anchor - span;
                    var filter = endAgo is { } ago ? new JobHistoryFilter(UntilUtc: anchor - ago) : null;
                    var raw = await DarlingJobHistoryReader.GetWithoutSummaryAsync(dataSource, since, serverId, limit, 120, filter, ct);
                    var rolled = await DarlingJobHistoryReader.GetAsync(dataSource, since, serverId, limit, 120, filter, ct);
                    var label = string.Create(CultureInfo.InvariantCulture, $"span {span.TotalDays}d end {endAgo} server {serverId} limit {limit}");
                    Assert.NotEmpty(raw);
                    AssertSame(raw, rolled, label);
                }
            }
        }
    }

    private static void AssertSame(List<DarlingJobHistoryRow> raw, List<DarlingJobHistoryRow> rolled, string label)
    {
        Assert.True(raw.Count == rolled.Count, $"{label}: {raw.Count} raw rows against {rolled.Count}");
        for (var i = 0; i < raw.Count; i++)
        {
            if (raw[i] != rolled[i])
            {
                Assert.Fail($"{label}: row {i} job {raw[i].JobId} step {raw[i].StepId} duration {raw[i].RunDurationSeconds}: raw last {raw[i].LastSuccessfulRunUtc:o} long {raw[i].IsLongRunning}, summary last {rolled[i].LastSuccessfulRunUtc:o} long {rolled[i].IsLongRunning}");
            }
        }
    }

    // ── Seed ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Two servers (one on UTC+5:30), five jobs of successful runs every 3 hours 7 minutes for 32 days, and a ladder of failed step-0 rows for
    /// six jobs. Returns the database clock the data was laid out against (naive UTC).
    /// </summary>
    private static async Task<DateTime> SeedAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await ExecuteAsync(dataSource, $@"
INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES
    ({ServerA}, 'rl-{ServerA}', 'A', TRUE, 16, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC'),
    ({ServerOffset}, 'rl-{ServerOffset}', 'Offset', TRUE, 16, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC');
INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes) VALUES
    (1, now() AT TIME ZONE 'UTC', {ServerA}, 'rl-{ServerA}', 0),
    (2, now() AT TIME ZONE 'UTC', {ServerOffset}, 'rl-{ServerOffset}', 330)", ct);

        var anchor = await ScalarAsync<DateTime>(dataSource, "SELECT date_trunc('second', now() AT TIME ZONE 'UTC')", ct);

        /* Jobs 1 to 3 have varied durations, job 4 has some NULL durations, job 5 only NULL durations, job 6 (below) no success but its late one. */
        await ExecuteAsync(dataSource, @"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled, step_id, step_name, run_status, run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
SELECT 930000000 + row_number() OVER (), r.t + interval '5 minutes', sv.server_id, 'rl-' || sv.server_id, 930000000 + row_number() OVER (),
       'job' || j, 'job' || j, true, 0, '(Job outcome)', 1, 'ok', r.t + make_interval(mins => sv.off),
       CASE WHEN j = 5 OR (j = 4 AND (r.n % 5) = 0) THEN NULL ELSE 60 + ((r.n * 37 + j * 11) % 23) * 10 END, 0, NULL
FROM (VALUES (9301, 0), (9302, 330)) AS sv(server_id, off),
     generate_series(1, 5) AS j,
     (SELECT t, row_number() OVER (ORDER BY t) AS n
      FROM generate_series(date_trunc('second', now() AT TIME ZONE 'UTC') - interval '32 days', date_trunc('second', now() AT TIME ZONE 'UTC') - interval '2 hours', interval '3 hours 7 minutes') AS g(t)) AS r", ct);

        /* The ladder: per job and server, failed step-0 rows of 61 to 700 seconds, three hours ago. */
        await ExecuteAsync(dataSource, @"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled, step_id, step_name, run_status, run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
SELECT 931000000 + row_number() OVER (), (now() AT TIME ZONE 'UTC') - interval '170 minutes', sv.server_id, 'rl-' || sv.server_id, 931000000 + row_number() OVER (),
       'job' || j, 'job' || j, true, 0, '(Job outcome)', 0, 'failed',
       (now() AT TIME ZONE 'UTC') - interval '3 hours' - make_interval(mins => j) + make_interval(secs => d) + make_interval(mins => sv.off), d, 0, NULL
FROM (VALUES (9301, 0), (9302, 330)) AS sv(server_id, off), generate_series(1, 6) AS j, generate_series(61, 700, 3) AS d", ct);

        return anchor;
    }

    /// <summary>
    /// Rows that reach the store long after they ran, on both servers, for every window: on the window's first day, on a full day, on the day
    /// the floor cuts (collected before the floor), and in the last hour. Those collected in the last 50 minutes are above the watermark a tick
    /// builds just now (<paramref name="aboveTheWatermark"/>); the rest are at or below it. All are successes of 3000 seconds or more, so a
    /// miss or a double count moves the average.
    /// </summary>
    private static async Task InsertLateRowsAsync(NpgsqlDataSource dataSource, bool aboveTheWatermark, CancellationToken ct) =>
        await ExecuteAsync(dataSource, $@"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled, step_id, step_name, run_status, run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
SELECT 932000000 + l.duration * 10 + sv.server_id - 9300, (now() AT TIME ZONE 'UTC') - make_interval(mins => l.collected_mins_ago), sv.server_id, 'rl-' || sv.server_id, 932000000 + l.duration * 10 + sv.server_id - 9300,
       l.job, l.job, true, 0, '(Job outcome)', 1, 'ok',
       (now() AT TIME ZONE 'UTC') - make_interval(mins => l.ran_mins_ago) + make_interval(mins => sv.off), l.duration, 0, NULL
FROM (VALUES (9301, 0), (9302, 330)) AS sv(server_id, off),
     (VALUES
        /* the window's first day (1, 7 and 30 days), collected hours later and 20 minutes ago */
        ('job1', 1440 - 60,   100,  3000),
        ('job2', 1440 - 120,  20,   3001),
        ('job1', 10080 - 60,  120,  3002),
        ('job2', 10080 - 120, 21,   3003),
        ('job1', 43200 - 60,  130,  3004),
        ('job2', 43200 - 120, 22,   3005),
        /* the day the floor cuts: ran inside the window, collected before the floor (a day before the window starts) */
        ('job4', 1440 - 60,   2880 + 180,  3006),
        ('job4', 10080 - 60,  11520 + 180, 3007),
        ('job4', 43200 - 60,  44640 + 180, 3008),
        /* full days: collected days after they ran, and in the last half hour */
        ('job3', 8 * 1440,    5 * 1440,   3009),
        ('job1', 6 * 1440,    25,         3010),
        ('job3', 20 * 1440,   12 * 1440,  3011),
        ('job2', 21 * 1440,   26,         3012),
        ('job3', 29 * 1440,   28 * 1440,  3013),
        ('job5', 3 * 1440,    2 * 1440,   3014),
        ('job6', 4 * 1440,    5,          3015),
        ('job3', 480,         30,         3016),
        ('job1', 60,          5,          3017)
     ) AS l(job, ran_mins_ago, collected_mins_ago, duration)
WHERE (l.collected_mins_ago < 50) = {(aboveTheWatermark ? "true" : "false")}", ct);

    private static async Task RunTickAsync(NpgsqlDataSource dataSource, DateTime? nowUtc, CancellationToken ct)
    {
        /* A backfill of 32 days for two servers is more steps than one tick runs, so tick until nothing is deferred. */
        for (var tick = 0; tick < 5; tick++)
        {
            var result = await JobOutcomeDaily.RunTickAsync(dataSource, nowUtc, NullLogger.Instance, ct);
            Assert.Equal(0, result.Failed);
            if (!result.Deferred)
            {
                return;
            }
        }

        Assert.Fail("The summary tick was still deferred after five ticks.");
    }

    private static async Task ExecuteAsync(NpgsqlDataSource dataSource, string sql, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(sql);
        command.CommandTimeout = 120;
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlDataSource dataSource, string sql, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(sql);
        command.CommandTimeout = 120;
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }
}
