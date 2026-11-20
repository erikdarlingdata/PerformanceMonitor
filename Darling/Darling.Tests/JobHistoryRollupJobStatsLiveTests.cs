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
        await InsertLateRowsAsync(dataSource, anchor, aboveTheWatermark: false, ct);
        await InsertLateRowsAsync(dataSource, anchor, aboveTheWatermark: true, ct);

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
        await InsertLateRowsAsync(dataSource, anchor, aboveTheWatermark: false, ct);

        await RunTickAsync(dataSource, null, ct);
        await InsertLateRowsAsync(dataSource, anchor, aboveTheWatermark: true, ct);
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
        await InsertLateRowsAsync(dataSource, anchor, aboveTheWatermark: false, ct);
        await InsertLateRowsAsync(dataSource, anchor, aboveTheWatermark: true, ct);

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
        await InsertLateRowsAsync(dataSource, anchor, aboveTheWatermark: false, ct);
        await InsertLateRowsAsync(dataSource, anchor, aboveTheWatermark: true, ct);

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
        var rolled = await DarlingJobHistoryReader.GetWithSummaryAsync(dataSource, since, ServerA, 100000, 60, null, ct);
        Assert.Equal(raw.Count, rolled.Count);
        Assert.NotEqual(ladder.Select(r => r.IsLongRunning), rolled.Where(r => r.JobId == "job1" && r.StepId == 0 && r.RunStatus == 0).Select(r => r.IsLongRunning));

        /* The read itself uses the summary for a long window and not for a short one (DarlingJobHistoryReader.SummaryMinWindow). */
        var since30 = anchor - TimeSpan.FromDays(30);
        var long30 = await DarlingJobHistoryReader.GetAsync(dataSource, since30, ServerA, 100000, 60, null, ct);
        var ladder30 = long30.Where(r => r.JobId == "job1" && r.StepId == 0 && r.RunStatus == 0).Select(r => r.IsLongRunning).ToList();
        var raw30 = await DarlingJobHistoryReader.GetWithoutSummaryAsync(dataSource, since30, ServerA, 100000, 60, null, ct);
        Assert.NotEqual(raw30.Where(r => r.JobId == "job1" && r.StepId == 0 && r.RunStatus == 0).Select(r => r.IsLongRunning), ladder30);
        AssertSame(raw, await DarlingJobHistoryReader.GetAsync(dataSource, since, ServerA, 100000, 60, null, ct), "short window reads raw");
        Assert.False(DarlingJobHistoryReader.UsesSummary(DateTime.UtcNow - TimeSpan.FromDays(7)));
        Assert.True(DarlingJobHistoryReader.UsesSummary(DateTime.UtcNow - TimeSpan.FromDays(30)));

        await ExecuteAsync(dataSource, string.Format(CultureInfo.InvariantCulture, bump, -100000, 5), ct);
        await ExecuteAsync(dataSource, string.Format(CultureInfo.InvariantCulture, bump, 100000, 7), ct);
        AssertSame(raw, await DarlingJobHistoryReader.GetWithSummaryAsync(dataSource, since, ServerA, 100000, 60, null, ct), "first day bumped");
    }

    // ── Retention (#9110) ─────────────────────────────────────────────────────

    /// <summary>
    /// A retention pass that runs a day ahead of the window's first day: the old pass deleted the days table's row of the window's partial first
    /// day, so the read lost that day's rows. The server is behind UTC, where the first day is the one before the UTC edge day.
    /// </summary>
    [Fact]
    public async Task ARetentionPassPastTheWindowsFirstDay_StillReadsWhatTheRawRowsRead()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var anchor = await SeedAsync(dataSource, ct);
        await MoveOffsetServerAsync(dataSource, -300, ct);
        await SetRetentionAsync(dataSource, 30, ct);
        await RunTickAsync(dataSource, null, ct);

        /* The pass of the next day: its edge is past the first day of a 30-day window read now. */
        await using (var connection = await dataSource.OpenConnectionAsync(ct))
        {
            Assert.True(await JobOutcomeDaily.GcAsync(connection, anchor + TimeSpan.FromDays(1), 30, ct) > 0);
        }

        await AssertWindowMatchesAsync(dataSource, anchor, TimeSpan.FromDays(30), ct);
    }

    /// <summary>
    /// Retention at 20 days with a 30-day window. The raw rows older than the horizon are gone (what the chunk drop does), and the rollup's
    /// days that are whole hold rows the drop removed, so a read that took them from the rollup would count rows the raw read cannot see.
    /// </summary>
    [Fact]
    public async Task WhenRetentionIsShorterThanTheWindow_TheReadSeesWhatTheRawRowsStillHold()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var anchor = await SeedAsync(dataSource, ct);
        await InsertLateRowsAsync(dataSource, anchor, aboveTheWatermark: false, ct);
        await InsertLateRowsAsync(dataSource, anchor, aboveTheWatermark: true, ct);
        await SetRetentionAsync(dataSource, 20, ct);

        await RunTickAsync(dataSource, null, ct);
        await ExecuteAsync(dataSource, "DELETE FROM collect.job_history WHERE collection_time < (now() AT TIME ZONE 'UTC') - interval '20 days'", ct);

        await AssertEveryReadMatchesAsync(dataSource, anchor, ct);
    }

    /// <summary>
    /// Retention at 14 days deletes the rollup's days older than that, and a row that was collected late for a day 20 days back is counted and
    /// deleted with them. Retention is then raised to 30 days: raw keeps that row and the days the rollup has lost, so a 30-day window cannot read
    /// the rollup for them.
    /// </summary>
    [Fact]
    public async Task WhenRetentionIsRaisedAfterDaysWereDeleted_TheReadStillSeesWhatTheRawRowsHold()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var anchor = await SeedAsync(dataSource, ct);
        await ExecuteAsync(dataSource, @"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled, step_id, step_name, run_status, run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
VALUES (933000001, (now() AT TIME ZONE 'UTC') - interval '2 hours', 9301, 'rl-9301', 933000001, 'job1', 'job1', true, 0, '(Job outcome)', 1, 'ok', (now() AT TIME ZONE 'UTC') - interval '20 days', 5000, 0, NULL)", ct);
        await SetRetentionAsync(dataSource, 14, ct);

        await RunTickAsync(dataSource, null, ct);
        await RunTickAsync(dataSource, null, ct);
        Assert.Equal(0L, await ScalarAsync<long>(dataSource, "SELECT count(*) FROM collect.job_outcome_daily WHERE run_day < ((now() AT TIME ZONE 'UTC') - interval '17 days')::date", ct));
        Assert.True(await ScalarAsync<long>(dataSource, "SELECT count(*) FROM collect.job_outcome_daily_watermark WHERE gc_before IS NOT NULL", ct) > 0);

        await SetRetentionAsync(dataSource, 30, ct);
        await AssertEveryReadMatchesAsync(dataSource, anchor, ct);
    }

    /// <summary>
    /// The default (Past 30 days, 30 days of retention) reads the summary for a server at any UTC offset: a bump of one full day's stored sum
    /// changes the average, which a raw read would not show. The window start is the app's clock some seconds before the store's now, which the
    /// retention check allows for.
    /// </summary>
    [Theory]
    [InlineData(-720)]
    [InlineData(-300)]
    [InlineData(330)]
    [InlineData(840)]
    public async Task Past30DaysAtThirtyDaysOfRetention_ReadsTheSummary_AtAnyOffset(int offsetMinutes)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var anchor = await SeedAsync(dataSource, ct);
        await MoveOffsetServerAsync(dataSource, offsetMinutes, ct);
        await SetRetentionAsync(dataSource, 30, ct);
        await RunTickAsync(dataSource, null, ct);

        var since = anchor - TimeSpan.FromDays(30);
        var raw = await DarlingJobHistoryReader.GetWithoutSummaryAsync(dataSource, since, ServerOffset, 100000, 60, null, ct);
        AssertSame(raw, await DarlingJobHistoryReader.GetWithSummaryAsync(dataSource, since, ServerOffset, 100000, 60, null, ct), "before the bump");
        await AssertStatsSameAsync(dataSource, since, ServerOffset, 100000, null, "before the bump", ct);
        var ladder = raw.Where(r => r.JobId == "job1" && r.StepId == 0 && r.RunStatus == 0).Select(r => r.IsLongRunning).ToList();

        await ExecuteAsync(dataSource, string.Create(CultureInfo.InvariantCulture, $"UPDATE collect.job_outcome_daily SET success_duration_sum = success_duration_sum + 100000 WHERE server_id = {ServerOffset} AND job_id = 'job1' AND run_day = ((now() AT TIME ZONE 'UTC') + make_interval(mins => {offsetMinutes}) - interval '5 days')::date"), ct);
        var rolled = await DarlingJobHistoryReader.GetWithSummaryAsync(dataSource, since, ServerOffset, 100000, 60, null, ct);
        Assert.NotEqual(ladder, rolled.Where(r => r.JobId == "job1" && r.StepId == 0 && r.RunStatus == 0).Select(r => r.IsLongRunning));
    }

    /// <summary>
    /// A server whose stored UTC offset is wrong: none stored (the read uses 0) with rows on a +10 h clock, or 0 stored with rows on a +14 h
    /// clock. Its rows were collected up to 14 hours before the server-local time the stored offset puts them at, so two things went wrong when
    /// the read trusted the stored offset: a full day at the start of the window held rows that raw retention had already dropped (the summary
    /// counted them, the raw read could not), and an edge day lost the rows collected before its offset-adjusted start. Raw rows older than the
    /// retention horizon are dropped as the purge would, and two windows are read: one that starts at 22:00 UTC (its first full day is near the
    /// horizon) and one that starts at 02:00 UTC (its edge day starts after rows that are in the window).
    /// </summary>
    [Theory]
    [InlineData(false, 600)]
    [InlineData(true, 840)]
    public async Task AServerWhoseStoredOffsetIsWrong_ReadsWhatTheRawRowsRead_OnceRetentionHasDroppedRaw(bool storedOffsetIsZero, int realOffsetMinutes)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var anchor = await SeedAsync(dataSource, ct);
        await MoveOffsetServerAsync(dataSource, realOffsetMinutes, ct);
        await ExecuteAsync(dataSource, storedOffsetIsZero
            ? string.Create(CultureInfo.InvariantCulture, $"UPDATE server_properties SET utc_offset_minutes = 0 WHERE server_id = {ServerOffset}")
            : string.Create(CultureInfo.InvariantCulture, $"DELETE FROM server_properties WHERE server_id = {ServerOffset}"), ct);
        await SetRetentionAsync(dataSource, 30, ct);
        await RunTickAsync(dataSource, null, ct);
        Assert.Equal(1L, await ScalarAsync<long>(dataSource, string.Create(CultureInfo.InvariantCulture, $"SELECT count(*) FROM collect.job_outcome_daily_watermark WHERE server_id = {ServerOffset}"), ct));
        await ExecuteAsync(dataSource, "DELETE FROM collect.job_history WHERE collection_time < (now() AT TIME ZONE 'UTC') - interval '30 days'", ct);

        var horizon = anchor - TimeSpan.FromDays(30);
        var nearTheHorizon = horizon.Date + TimeSpan.FromHours(22);
        if (nearTheHorizon < horizon)
        {
            nearTheHorizon += TimeSpan.FromDays(1);
        }

        var earlyMorning = (anchor - TimeSpan.FromDays(20)).Date + TimeSpan.FromHours(2);
        foreach (var since in new[] { nearTheHorizon, earlyMorning })
        {
            foreach (var serverId in new int?[] { null, ServerOffset })
            {
                var raw = await DarlingJobHistoryReader.GetWithoutSummaryAsync(dataSource, since, serverId, 100000, 120, null, ct);
                Assert.NotEmpty(raw);
                var label = string.Create(CultureInfo.InvariantCulture, $"since {since:o} server {serverId}");
                AssertSame(raw, await DarlingJobHistoryReader.GetWithSummaryAsync(dataSource, since, serverId, 100000, 120, null, ct), label);
                await AssertStatsSameAsync(dataSource, since, serverId, 100000, null, label, ct);
            }
        }

        /* The window near the horizon still reads the summary for its whole days: a bump of the summary's sums shows in the average. */
        await ExecuteAsync(dataSource, string.Create(CultureInfo.InvariantCulture, $"UPDATE collect.job_outcome_daily SET success_duration_sum = success_duration_sum + 100000 WHERE server_id = {ServerOffset} AND job_id = 'job1'"), ct);
        var unbumped = await DarlingJobHistoryReader.GetJobStatsAsync(dataSource, nearTheHorizon, ServerOffset, 100000, 120, rollup: false, cancellationToken: ct);
        var bumped = await DarlingJobHistoryReader.GetJobStatsAsync(dataSource, nearTheHorizon, ServerOffset, 100000, 120, rollup: true, cancellationToken: ct);
        Assert.NotEqual(unbumped, bumped);
    }

    /// <summary>
    /// A server whose clock really runs at +14 h but whose stored offset was changed since: the window's first full day (the first local day
    /// after the window's start, by the stored offset) starts before the retention horizon in real time, so raw retention has dropped some of its
    /// rows while the summary still counts them. The stored offset is picked from the window's start so that this holds whatever the time of day
    /// of the run, and one row is inserted on that day, collected 30 minutes before the horizon, so the day has a row to lose.
    /// </summary>
    [Fact]
    public async Task AServerWhoseStoredOffsetChanged_NeverReadsAFullDayRawRetentionHasDroppedRowsOf()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var anchor = await SeedAsync(dataSource, ct);
        await MoveOffsetServerAsync(dataSource, 840, ct);

        /* The window starts on a whole minute at the horizon. The fixture row ran at +14 h, 35 minutes before the start, on local day M; the stored
           offset puts the window's start at M's midnight, so M is the first full day. */
        var since = new DateTime(((anchor - TimeSpan.FromDays(30)).Ticks + TimeSpan.TicksPerMinute - 1) / TimeSpan.TicksPerMinute * TimeSpan.TicksPerMinute, DateTimeKind.Unspecified);
        var collected = since - TimeSpan.FromMinutes(30);
        var ranLocal = collected - TimeSpan.FromMinutes(5) + TimeSpan.FromMinutes(840);
        var storedOffset = (int)(ranLocal.Date - since).TotalMinutes;
        await ExecuteAsync(dataSource, string.Create(CultureInfo.InvariantCulture, $@"
UPDATE server_properties SET utc_offset_minutes = {storedOffset} WHERE server_id = {ServerOffset};
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled, step_id, step_name, run_status, run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
VALUES (934000001, '{collected:yyyy-MM-dd HH:mm:ss}', {ServerOffset}, 'rl-{ServerOffset}', 934000001, 'job1', 'job1', true, 0, '(Job outcome)', 1, 'ok', '{ranLocal:yyyy-MM-dd HH:mm:ss}', 20000, 0, NULL)"), ct);
        await SetRetentionAsync(dataSource, 30, ct);
        await RunTickAsync(dataSource, null, ct);
        Assert.Equal(1L, await ScalarAsync<long>(dataSource, string.Create(CultureInfo.InvariantCulture, $"SELECT count(*) FROM collect.job_outcome_daily WHERE server_id = {ServerOffset} AND job_id = 'job1' AND run_day = '{ranLocal.Date:yyyy-MM-dd}' AND success_duration_sum >= 20000"), ct));
        await ExecuteAsync(dataSource, "DELETE FROM collect.job_history WHERE collection_time < (now() AT TIME ZONE 'UTC') - interval '30 days'", ct);
        Assert.Equal(0L, await ScalarAsync<long>(dataSource, "SELECT count(*) FROM collect.job_history WHERE job_history_id = 934000001", ct));

        foreach (var serverId in new int?[] { null, ServerOffset })
        {
            var raw = await DarlingJobHistoryReader.GetWithoutSummaryAsync(dataSource, since, serverId, 100000, 120, null, ct);
            Assert.NotEmpty(raw);
            var label = string.Create(CultureInfo.InvariantCulture, $"since {since:o} stored offset {storedOffset} server {serverId}");
            AssertSame(raw, await DarlingJobHistoryReader.GetWithSummaryAsync(dataSource, since, serverId, 100000, 120, null, ct), label);
            await AssertStatsSameAsync(dataSource, since, serverId, 100000, null, label, ct);
        }
    }

    private static async Task AssertWindowMatchesAsync(NpgsqlDataSource dataSource, DateTime anchor, TimeSpan span, CancellationToken ct)
    {
        foreach (var serverId in new int?[] { null, ServerA, ServerOffset })
        {
            var since = anchor - span;
            var raw = await DarlingJobHistoryReader.GetWithoutSummaryAsync(dataSource, since, serverId, 100000, 120, null, ct);
            var rolled = await DarlingJobHistoryReader.GetWithSummaryAsync(dataSource, since, serverId, 100000, 120, null, ct);
            Assert.NotEmpty(raw);
            AssertSame(raw, rolled, string.Create(CultureInfo.InvariantCulture, $"span {span.TotalDays}d server {serverId}"));
            await AssertStatsSameAsync(dataSource, since, serverId, 100000, null, string.Create(CultureInfo.InvariantCulture, $"span {span.TotalDays}d server {serverId}"), ct);
        }
    }

    /// <summary>
    /// Puts the second server at <paramref name="offsetMinutes"/> from UTC. <see cref="SeedAsync"/> lays it out at +330, so its stored local run
    /// times move by the difference and its rows stay at the same UTC instants.
    /// </summary>
    private static async Task MoveOffsetServerAsync(NpgsqlDataSource dataSource, int offsetMinutes, CancellationToken ct)
    {
        await ExecuteAsync(dataSource, string.Create(CultureInfo.InvariantCulture, $@"
UPDATE collect.job_history SET run_datetime = run_datetime + make_interval(mins => {offsetMinutes - 330}) WHERE server_id = {ServerOffset};
UPDATE server_properties SET utc_offset_minutes = {offsetMinutes} WHERE server_id = {ServerOffset}"), ct);
    }

    /// <summary>Sets the fleet retention of job_history, the horizon the purge applies.</summary>
    private static async Task SetRetentionAsync(NpgsqlDataSource dataSource, int days, CancellationToken ct) =>
        await ExecuteAsync(dataSource, string.Create(CultureInfo.InvariantCulture, $@"
DELETE FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = 'job_history';
INSERT INTO config.config_collector_schedules (server_id, collector_name, retention_days) VALUES (NULL, 'job_history', {days})"), ct);

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
                    var rolled = await DarlingJobHistoryReader.GetWithSummaryAsync(dataSource, since, serverId, limit, 120, filter, ct);
                    var label = string.Create(CultureInfo.InvariantCulture, $"span {span.TotalDays}d end {endAgo} server {serverId} limit {limit}");
                    Assert.NotEmpty(raw);
                    AssertSame(raw, rolled, label);
                    await AssertStatsSameAsync(dataSource, since, serverId, limit, filter, label, ct);
                }
            }
        }
    }

    /// <summary>
    /// The <c>job_stats</c> of the two statements, per (server, job): the average successful duration as Postgres prints it, digit for digit, and
    /// the last success. <see cref="DarlingJobHistoryRow"/> carries no average, only the flag it decides.
    /// </summary>
    private static async Task AssertStatsSameAsync(NpgsqlDataSource dataSource, DateTime since, int? serverId, int limit, JobHistoryFilter? filter, string label, CancellationToken ct)
    {
        var raw = await DarlingJobHistoryReader.GetJobStatsAsync(dataSource, since, serverId, limit, 120, rollup: false, filter, ct);
        var rolled = await DarlingJobHistoryReader.GetJobStatsAsync(dataSource, since, serverId, limit, 120, rollup: true, filter, ct);
        Assert.NotEmpty(raw);
        Assert.True(raw.Count == rolled.Count, $"{label}: {raw.Count} raw job averages against {rolled.Count}");
        for (var i = 0; i < raw.Count; i++)
        {
            if (raw[i] != rolled[i])
            {
                Assert.Fail($"{label}: server {raw[i].ServerId} job {raw[i].JobId}: raw average {raw[i].AvgSuccessDuration} last {raw[i].LastSuccessRunLocal:o}, summary average {rolled[i].AvgSuccessDuration} last {rolled[i].LastSuccessRunLocal:o}");
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
    private static async Task InsertLateRowsAsync(NpgsqlDataSource dataSource, DateTime anchor, bool aboveTheWatermark, CancellationToken ct) =>
        await ExecuteAsync(dataSource, $@"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled, step_id, step_name, run_status, run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
SELECT 932000000 + l.duration * 10 + sv.server_id - 9300, (now() AT TIME ZONE 'UTC') - make_interval(mins => l.collected_mins_ago), sv.server_id, 'rl-' || sv.server_id, 932000000 + l.duration * 10 + sv.server_id - 9300,
       l.job, l.job, true, 0, '(Job outcome)', 1, 'ok',
       CASE WHEN l.cut_span_mins IS NULL
            THEN (now() AT TIME ZONE 'UTC') - make_interval(mins => l.ran_mins_ago) + make_interval(mins => sv.off)
            ELSE w.since_local + LEAST(interval '1 hour', (date_trunc('day', w.since_local) + interval '1 day' - w.since_local) / 2)
       END, l.duration, 0, NULL
FROM (VALUES (9301, 0), (9302, 330)) AS sv(server_id, off),
     (VALUES
        /* the window's first day (1, 7 and 30 days), collected hours later and 20 minutes ago */
        ('job1', 1440 - 60,   100,  3000, NULL),
        ('job2', 1440 - 120,  20,   3001, NULL),
        ('job1', 10080 - 60,  120,  3002, NULL),
        ('job2', 10080 - 120, 21,   3003, NULL),
        ('job1', 43200 - 60,  130,  3004, NULL),
        ('job2', 43200 - 120, 22,   3005, NULL),
        /* the day the floor cuts: ran inside the window, collected before the floor (a day before the window starts). It sits on the window's first day
           on each server's own clock, whatever the hour: a row on a later day is a full day to the summary, which counts every row of a full day, while the
           raw read drops it at the floor (a real row is never collected more than a day before it ran, so the floor never cuts a full day). */
        ('job4', 1440 - 60,   2880 + 180,  3006, 1440),
        ('job4', 10080 - 60,  11520 + 180, 3007, 10080),
        ('job4', 43200 - 60,  44640 + 180, 3008, 43200),
        /* full days: collected days after they ran, and in the last half hour */
        ('job3', 8 * 1440,    5 * 1440,   3009, NULL),
        ('job1', 6 * 1440,    25,         3010, NULL),
        ('job3', 20 * 1440,   12 * 1440,  3011, NULL),
        ('job2', 21 * 1440,   26,         3012, NULL),
        ('job3', 29 * 1440,   28 * 1440,  3013, NULL),
        ('job5', 3 * 1440,    2 * 1440,   3014, NULL),
        ('job6', 4 * 1440,    5,          3015, NULL),
        ('job3', 480,         30,         3016, NULL),
        ('job1', 60,          5,          3017, NULL)
     ) AS l(job, ran_mins_ago, collected_mins_ago, duration, cut_span_mins)
CROSS JOIN LATERAL (SELECT '{anchor:yyyy-MM-dd HH:mm:ss}'::timestamp - make_interval(mins => l.cut_span_mins) + make_interval(mins => sv.off) AS since_local) AS w
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
