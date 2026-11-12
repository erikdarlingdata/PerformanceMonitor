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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8302: a TimescaleDB job that crashes once must run again on its own, and a daily rollup must not lose a day.
/// Three behaviours, each on a scratch store: every job this product schedules ends setup with a short
/// <c>retry_period</c> and a converged store issues no further job alteration; a daily refresh window of four
/// days fills every bucket a one-day outage skipped; and the policy-job health read reports a job whose crash
/// count stands, and a daily rollup whose newest materialized bucket is stale, and reports neither on a healthy
/// store.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every live test here goes through
   ScratchPostgres.CreateAsync, which reaches DARLING_TEST_PG only to CREATE and DROP its own database and then
   works entirely inside it. It never touches the shared database's tables. */
public sealed class TimescaleJobRetryAndDailyWindowTests
{
    private const int ServerId = -830201;
    private const string ServerName = "retry-window";

    /// <summary>Fixed anchor day (never wall clock), so the tests pass at any hour.</summary>
    private static readonly DateTime D0 = new(2026, 3, 2, 0, 0, 0, DateTimeKind.Unspecified);

    private static readonly string Hourly = TimescaleSupport.QueryStoreStatsIntervalHourlyView;
    private static readonly string Daily = TimescaleSupport.QueryStoreStatsIntervalDailyView;

    /// <summary>The retry period every scheduled job this product creates ends with.</summary>
    private static readonly TimeSpan ExpectedRetry = TimeSpan.FromMinutes(15);

    /// <summary>The scheduler's floor on a crashed job's wait: it never re-runs one sooner than this after a reload.</summary>
    private static readonly TimeSpan CrashWaitFloor = TimeSpan.FromMinutes(5);

    /// <summary>A healthy daily's newest bucket end is at most this old: end offset (1 day) + one bucket + one schedule
    /// interval, reached when a run lands just before 00:00 UTC and held until the next run.</summary>
    private static readonly TimeSpan HealthyDailyEndAgeMaximum = TimeSpan.FromDays(3);

    /// <summary>The daily's staleness threshold: its 4-day start offset less twelve hours.</summary>
    private static readonly TimeSpan StaleDailyThreshold = TimeSpan.FromDays(3.5);

    /* ---------------- pure pins ---------------- */

    [Fact]
    public void TheDailyWindow_IsFourDays_AndTheDailyParentedChildIsOneBucketWider()
    {
        Assert.Equal(TimeSpan.FromDays(4), TimescaleSupport.DailyRefreshStartSpan);
        Assert.Equal("4 days", TimescaleSupport.DailyRefreshStartOffset);
        Assert.Equal(TimeSpan.FromDays(5), TimescaleSupport.DailyChildRefreshStartSpan);
        Assert.Equal("5 days", TimescaleSupport.DailyChildRefreshStartOffset);

        /* The retention guarantee: the hourly tier is dropped long after the daily refresh can still reach it, and
           the interval-grain daily's own retention stays clear of the widest daily window. */
        Assert.True(TimescaleSupport.DailyChildRefreshStartSpan < TimescaleSupport.HourlyRetentionSpan);
        Assert.True(TimescaleSupport.DailyChildRefreshStartSpan < TimescaleSupport.IntervalDailyRetentionSpan);
    }

    [Fact]
    public void TheRetryPeriod_StaysAboveTheSchedulersCrashFloor_AndWellUnderTheHourlyReload()
    {
        var span = typeof(TimescaleSupport).GetField("PolicyJobRetrySpan", BindingFlags.Public | BindingFlags.Static)?.GetValue(null) as TimeSpan?;
        Assert.True(span.HasValue, "TimescaleSupport.PolicyJobRetrySpan: the retry period every scheduled job this product creates ends with");

        /* After a crash the scheduler re-runs a job max(5 minutes, retry_period x consecutive_crashes) after it last
           reloaded its job list, and the hourly purge record reloads that list every hour. A retry period at or
           over the hour is never reached; one under the floor buys nothing. */
        Assert.True(span!.Value >= CrashWaitFloor, $"retry period {span} is under the scheduler's {CrashWaitFloor} crash floor");
        Assert.True(span.Value <= TimeSpan.FromMinutes(20), $"retry period {span} is not well under the hourly reload");
        Assert.True(span.Value * 3 < TimescaleSupport.HourlyRefreshScheduleSpan, $"three consecutive crashes at {span} would outlast the hourly reload");
        Assert.Equal(ExpectedRetry, span.Value);
    }

    /* ---------------- live: the daily window ---------------- */

    /// <summary>
    /// The daily runs as the policy runs them (its window, at 10:00 each day), with one day's run missing: the store
    /// was down. Every day the outage skipped is still inside a later run's window, so nothing is lost.
    /// </summary>
    [Fact]
    public async Task ADailyRollup_FillsEveryBucketTheOutageSkipped_OnTheNextRuns()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8302 window test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        Assert.SkipWhen(!await PrepareAsync(connection, scratch.ConnectionString, ct), "The live #8302 tests need TimescaleDB.");

        for (var day = D0; day < D0.AddDays(4); day = day.AddDays(1))
        {
            await InsertDayAsync(connection, day, ct);
        }

        await RefreshAsync(connection, Hourly, D0, D0.AddDays(4), ct);

        var span = TimescaleSupport.DailyRefreshStartSpanFor(Daily);
        var end = TimeSpan.FromDays(1);

        // The run on D0+2 fills D0. The run on D0+3 never happens: the store is down for that day.
        await RefreshAsync(connection, Daily, D0.AddDays(2).AddHours(10) - span, D0.AddDays(2).AddHours(10) - end, ct);
        await RefreshAsync(connection, Daily, D0.AddDays(4).AddHours(10) - span, D0.AddDays(4).AddHours(10) - end, ct);
        await RefreshAsync(connection, Daily, D0.AddDays(5).AddHours(10) - span, D0.AddDays(5).AddHours(10) - end, ct);

        var materialization = await ResolveAsync(connection, Daily, ct);
        for (var day = D0; day < D0.AddDays(4); day = day.AddDays(1))
        {
            Assert.True(48L == await SamplesAsync(connection, materialization, day, ct), $"the daily bucket for {day:yyyy-MM-dd} was never materialized");
        }
    }

    /// <summary>A store made under the three-day window converges onto the four-day one at startup, through the window converge.</summary>
    [Fact]
    public async Task AStoreOnTheOldDailyWindow_ConvergesOntoFourDays()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8302 converge test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        Assert.SkipWhen(!await PrepareAsync(connection, scratch.ConnectionString, ct), "The live #8302 tests need TimescaleDB.");

        var jobId = await RefreshJobIdAsync(connection, Daily, ct);
        await using (var narrow = new NpgsqlCommand(TimescaleSupport.SetDailyRefreshWindowSql, connection))
        {
            narrow.Parameters.AddWithValue(jobId);
            narrow.Parameters.AddWithValue("3 days");
            await narrow.ExecuteNonQueryAsync(ct);
        }

        Assert.Equal(3L * 86_400L, await StartOffsetSecondsAsync(connection, jobId, ct));

        var converged = await TimescaleSupport.ConvergeDailyChildRefreshWindowAsync(connection, null, ct);
        Assert.True(converged >= 1, "the window converge left a three-day daily policy where it was");
        Assert.Equal((long)TimescaleSupport.DailyRefreshStartSpan.TotalSeconds, await StartOffsetSecondsAsync(connection, jobId, ct));
        Assert.Equal(0, await TimescaleSupport.ConvergeDailyChildRefreshWindowAsync(connection, null, ct));
    }

    /* ---------------- live: the retry period ---------------- */

    /// <summary>
    /// After setup every scheduled job this product creates carries the short retry period; a job moved off it
    /// converges back on the next pass; and a converged store issues nothing, which is what keeps the scheduler's
    /// job list from being reloaded more often than the crash floor allows.
    /// </summary>
    [Fact]
    public async Task EveryScheduledJob_EndsSetupWithTheShortRetryPeriod_AndAConvergedStoreAltersNothing()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8302 retry test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        Assert.SkipWhen(!await PrepareAsync(connection, scratch.ConnectionString, ct), "The live #8302 tests need TimescaleDB.");

        await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct);
        await TimescaleSupport.EnsureAggregateCompressionAsync(connection, null, ct);

        var converge = typeof(TimescaleSupport).GetMethod("ConvergePolicyJobRetryPeriodAsync", BindingFlags.Public | BindingFlags.Static);
        Assert.True(converge is not null, "TimescaleSupport.ConvergePolicyJobRetryPeriodAsync(NpgsqlConnection, ILogger?, CancellationToken): the setup step that puts every scheduled job on the short retry period");

        var first = await (Task<int>)converge!.Invoke(null, new object?[] { connection, null, ct })!;
        var jobs = await ScheduledJobRetryAsync(connection, ct);
        Assert.NotEmpty(jobs);
        Assert.True(first >= jobs.Count, $"the first pass set {first} retry period(s) over {jobs.Count} scheduled job(s)");
        foreach (var (jobId, proc, relation, retrySeconds) in jobs)
        {
            Assert.True((long)ExpectedRetry.TotalSeconds == retrySeconds, $"job {jobId} ({proc} on {relation}) has retry_period {retrySeconds}s after setup");
        }

        // A job moved off the short period (an older build, or a hand edit) converges back on the next pass.
        var moved = jobs[0].JobId;
        await using (var widen = new NpgsqlCommand("SELECT alter_job($1::integer, retry_period => INTERVAL '1 day')", connection))
        {
            widen.Parameters.AddWithValue(moved);
            await widen.ExecuteNonQueryAsync(ct);
        }

        Assert.Equal(1, await (Task<int>)converge.Invoke(null, new object?[] { connection, null, ct })!);
        Assert.Equal((long)ExpectedRetry.TotalSeconds, (await ScheduledJobRetryAsync(connection, ct)).Single(j => j.JobId == moved).RetrySeconds);

        // A converged store issues no job alteration at all.
        Assert.Equal(0, await (Task<int>)converge.Invoke(null, new object?[] { connection, null, ct })!);
    }

    /* ---------------- live: the health read ---------------- */

    /// <summary>
    /// A job whose crash count stands is reported however its next start reads, because the scheduler recomputes
    /// that start from the count on every reload; once a run clears the count the job is healthy again.
    /// </summary>
    [Fact]
    public async Task AJobWhoseCrashCountStands_IsReportedUntilARunClearsIt()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8302 crash test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        Assert.SkipWhen(!await PrepareAsync(connection, scratch.ConnectionString, ct), "The live #8302 tests need TimescaleDB.");

        var jobId = await RefreshJobIdAsync(connection, Daily, ct);
        var now = DateTime.UtcNow;

        var healthy = await TimescaleSupport.ReadStuckPolicyJobsAsync(connection, now, null, ct);
        Assert.DoesNotContain(healthy.Stuck, j => j.JobId == jobId);

        /* A crashed run as the scheduler records it, with a FINITE next start: the shape a job is left in after
           CALL run_job, which runs it but does not clear the count. */
        try
        {
            await using var crash = new NpgsqlCommand(@"
INSERT INTO _timescaledb_internal.bgw_job_stat
    (job_id, last_start, last_finish, next_start, last_successful_finish, last_run_success, total_runs, total_duration,
     total_duration_failures, total_successes, total_failures, total_crashes, consecutive_failures, consecutive_crashes, flags)
VALUES ($1, now() - INTERVAL '2 hours', '-infinity', now() + INTERVAL '1 day', now() - INTERVAL '1 day', true, 3, INTERVAL '1 minute',
     INTERVAL '0', 2, 0, 1, 0, 1, 0)", connection);
            crash.Parameters.AddWithValue(jobId);
            await crash.ExecuteNonQueryAsync(ct);
        }
        catch (PostgresException ex)
        {
            Assert.Skip($"This cluster does not let the test mark a job crashed ({ex.SqlState}: {ex.MessageText}).");
        }

        var crashed = await TimescaleSupport.ReadStuckPolicyJobsAsync(connection, now, null, ct);
        var reported = Assert.Single(crashed.Stuck, j => j.JobId == jobId);
        Assert.Equal(TimescaleSupport.StorePolicyJobFamily.Refresh, reported.Family);
        Assert.Contains("crash", reported.Reason, StringComparison.OrdinalIgnoreCase);

        // The scheduler ran it: the count is cleared, and the job is healthy again.
        await using (var ran = new NpgsqlCommand(
            "UPDATE _timescaledb_internal.bgw_job_stat SET consecutive_crashes = 0, last_finish = now(), last_successful_finish = now() WHERE job_id = $1", connection))
        {
            ran.Parameters.AddWithValue(jobId);
            await ran.ExecuteNonQueryAsync(ct);
        }

        var recovered = await TimescaleSupport.ReadStuckPolicyJobsAsync(connection, now, null, ct);
        Assert.DoesNotContain(recovered.Stuck, j => j.JobId == jobId);
    }

    /// <summary>
    /// A daily rollup whose newest materialized bucket is older than its refresh window is reported under its own
    /// refresh job; the same rollup read a day earlier, inside the window, is not.
    /// </summary>
    [Fact]
    public async Task AStaleDailyRollup_IsReported_AndACurrentOneIsNot()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8302 stale-rollup test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        Assert.SkipWhen(!await PrepareAsync(connection, scratch.ConnectionString, ct), "The live #8302 tests need TimescaleDB.");

        var child = TimescaleSupport.QueryStoreStatsDayGrainDailyView;
        await InsertDayAsync(connection, D0, ct);
        await InsertDayAsync(connection, D0.AddDays(1), ct);
        await RefreshAsync(connection, Hourly, D0, D0.AddDays(2), ct);
        await RefreshAsync(connection, Daily, D0, D0.AddDays(2), ct);
        await RefreshAsync(connection, child, D0, D0.AddDays(2), ct);

        var dailyJob = await RefreshJobIdAsync(connection, Daily, ct);
        var childJob = await RefreshJobIdAsync(connection, child, ct);

        /* A frozen source: the hourly the daily is built on has nothing newer either (collection off, every server
           offline), and the child is level with the daily. However long ago that was, nothing is reported. */
        var frozen = await TimescaleSupport.ReadStuckPolicyJobsAsync(connection, D0.AddDays(30), null, ct);
        Assert.Empty(frozen.Stuck);

        // The source moves on without the daily: four more days of raw, rolled up hourly, while the daily's job never ran.
        for (var day = D0.AddDays(2); day < D0.AddDays(6); day = day.AddDays(1))
        {
            await InsertDayAsync(connection, day, ct);
        }

        await RefreshAsync(connection, Hourly, D0, D0.AddDays(6), ct);

        /* The daily is four days old against the clock with its source ahead: reported. The child is as old, but its
           source (the daily) has nothing newer than it has, so it is not. */
        var stale = await TimescaleSupport.ReadStuckPolicyJobsAsync(connection, D0.AddDays(6), null, ct);
        var reported = Assert.Single(stale.Stuck);
        Assert.Equal(dailyJob, reported.JobId);
        Assert.Equal(TimescaleSupport.StorePolicyJobFamily.Refresh, reported.Family);
        Assert.Equal(Daily, reported.HypertableName);
        Assert.Contains("source", reported.Reason, StringComparison.Ordinal);
        Assert.Contains("refresh_continuous_aggregate", reported.Reason, StringComparison.Ordinal);

        /* The daily catches up and the child does not. The child's source now sits four days ahead of it; what decides
           is the child's own age against the clock. Inside its 4.5-day threshold: nothing. An hour past it: reported,
           while the daily, a few hours behind the clock, is not. */
        await RefreshAsync(connection, Daily, D0, D0.AddDays(6), ct);
        var childInside = await TimescaleSupport.ReadStuckPolicyJobsAsync(connection, D0.AddDays(2) + TimeSpan.FromDays(4.5), null, ct);
        Assert.Empty(childInside.Stuck);

        var childPast = await TimescaleSupport.ReadStuckPolicyJobsAsync(connection, D0.AddDays(2) + TimeSpan.FromDays(4.5) + TimeSpan.FromHours(1), null, ct);
        var reportedChild = Assert.Single(childPast.Stuck);
        Assert.Equal(childJob, reportedChild.JobId);
        Assert.Equal(child, reportedChild.HypertableName);
        Assert.Contains("source", reportedChild.Reason, StringComparison.Ordinal);

        // The child catches up: nothing to report again.
        await RefreshAsync(connection, child, D0, D0.AddDays(6), ct);
        var caughtUp = await TimescaleSupport.ReadStuckPolicyJobsAsync(connection, D0.AddDays(2) + TimeSpan.FromDays(4.5) + TimeSpan.FromHours(1), null, ct);
        Assert.Empty(caughtUp.Stuck);
    }

    /// <summary>The threshold sits half a day under each daily's own window: 3.5 days, 4.5 for the daily-parented child.</summary>
    [Fact]
    public void TheStaleThreshold_IsEachDailysStartOffsetLessTwelveHours()
    {
        Assert.Equal(TimeSpan.FromHours(12), TimescaleSupport.DailyRollupStaleMarginSpan);
        Assert.Equal(StaleDailyThreshold, TimescaleSupport.DailyRollupStaleAfterSpanFor(Daily));
        Assert.Equal(TimeSpan.FromDays(4.5), TimescaleSupport.DailyRollupStaleAfterSpanFor(TimescaleSupport.QueryStoreStatsDayGrainDailyView));

        foreach (var (_, view) in TimescaleSupport.DailyAggregates)
        {
            Assert.Equal(TimescaleSupport.DailyRefreshStartSpanFor(view) - TimeSpan.FromHours(12), TimescaleSupport.DailyRollupStaleAfterSpanFor(view));
        }
    }

    /// <summary>The staleness predicate: the rollup's watermark, its source's watermark, and the clock. The age against
    /// the clock carries the loss arithmetic; the source only gates out a rollup that is level with a source that stopped.</summary>
    private static bool Stale(string view, DateTime rollupWatermark, DateTime sourceWatermark, DateTime nowUtc)
    {
        var predicate = typeof(TimescaleSupport).GetMethod(
            "IsDailyRollupStale", BindingFlags.Public | BindingFlags.Static, new[] { typeof(string), typeof(DateTime), typeof(DateTime), typeof(DateTime) });
        Assert.True(predicate is not null, "TimescaleSupport.IsDailyRollupStale(string view, DateTime rollupWatermark, DateTime sourceWatermark, DateTime nowUtc): stale when the rollup's newest bucket is older than its threshold against the clock AND its source's newest bucket is past its own");
        return (bool)predicate!.Invoke(null, new object[] { view, rollupWatermark, sourceWatermark, nowUtc })!;
    }

    /// <summary>
    /// The boundary on each side, for an hourly-parented daily and for the daily-parented child. The age is measured
    /// against the clock, because a daily-parented daily's source sits one to three days behind the clock itself, so a
    /// lag against the parent would only fire after the child's window had already lost a day. The source gates out a
    /// frozen source: a rollup at or past its source's newest bucket is silent at any age.
    /// </summary>
    [Fact]
    public void TheStaleBoundary_IsAgainstTheClock_GatedByTheSourceBeingAhead()
    {
        var now = new DateTime(2026, 3, 10, 9, 0, 0, DateTimeKind.Unspecified);
        var child = TimescaleSupport.QueryStoreStatsDayGrainDailyView;
        var hourlySource = now - TimeSpan.FromHours(1);
        var healthyParent = now - TimeSpan.FromDays(2);

        // An hourly-parented daily: silent at the healthy maximum and at the threshold, firing one hour past it.
        Assert.False(Stale(Daily, now - HealthyDailyEndAgeMaximum, hourlySource, now));
        Assert.False(Stale(Daily, now - StaleDailyThreshold, hourlySource, now));
        Assert.True(Stale(Daily, now - StaleDailyThreshold - TimeSpan.FromHours(1), hourlySource, now));
        Assert.True(Stale(Daily, now - TimescaleSupport.DailyRefreshStartSpan, hourlySource, now));

        // The daily-parented child with a healthy parent: its own 4.5-day threshold against the clock, not against the parent.
        Assert.True(Stale(child, now - TimeSpan.FromDays(4.5) - TimeSpan.FromHours(1), healthyParent, now));
        Assert.False(Stale(child, now - TimeSpan.FromDays(4.5), healthyParent, now));
        Assert.False(Stale(child, now - HealthyDailyEndAgeMaximum - TimeSpan.FromDays(1), healthyParent, now));

        // A frozen source: a rollup level with it, or past it, is silent at any age.
        var frozen = now - TimeSpan.FromDays(30);
        Assert.False(Stale(Daily, frozen, frozen, now));
        Assert.False(Stale(Daily, frozen + TimeSpan.FromDays(1), frozen, now));
        Assert.False(Stale(child, frozen, frozen, now));

        // Twelve hours of margin on each side of the threshold: above the healthy maximum, and below the window.
        Assert.Equal(TimeSpan.FromHours(12), StaleDailyThreshold - HealthyDailyEndAgeMaximum);
        Assert.Equal(TimeSpan.FromHours(12), TimescaleSupport.DailyRefreshStartSpan - StaleDailyThreshold);
    }

    /// <summary>
    /// A daily behind its source whose refresh job has also crashed is reported on BOTH arms, under the same job: the crash
    /// clears when a run finishes, the staleness when the rollup catches up, and dropping either row would misreport the other.
    /// </summary>
    [Fact]
    public async Task AStaleDailyWhoseJobCrashed_IsReportedOnBothArms()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8302 two-arm test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        Assert.SkipWhen(!await PrepareAsync(connection, scratch.ConnectionString, ct), "The live #8302 tests need TimescaleDB.");

        await InsertDayAsync(connection, D0, ct);
        await InsertDayAsync(connection, D0.AddDays(1), ct);
        await RefreshAsync(connection, Hourly, D0, D0.AddDays(2), ct);
        await RefreshAsync(connection, Daily, D0, D0.AddDays(2), ct);
        for (var day = D0.AddDays(2); day < D0.AddDays(6); day = day.AddDays(1))
        {
            await InsertDayAsync(connection, day, ct);
        }

        await RefreshAsync(connection, Hourly, D0, D0.AddDays(6), ct);
        var jobId = await RefreshJobIdAsync(connection, Daily, ct);

        try
        {
            await using var crash = new NpgsqlCommand(@"
INSERT INTO _timescaledb_internal.bgw_job_stat
    (job_id, last_start, last_finish, next_start, last_successful_finish, last_run_success, total_runs, total_duration,
     total_duration_failures, total_successes, total_failures, total_crashes, consecutive_failures, consecutive_crashes, flags)
VALUES ($1, now() - INTERVAL '2 hours', '-infinity', now() + INTERVAL '1 day', now() - INTERVAL '1 day', true, 3, INTERVAL '1 minute',
     INTERVAL '0', 2, 0, 1, 0, 1, 0)", connection);
            crash.Parameters.AddWithValue(jobId);
            await crash.ExecuteNonQueryAsync(ct);
        }
        catch (PostgresException ex)
        {
            Assert.Skip($"This cluster does not let the test mark a job crashed ({ex.SqlState}: {ex.MessageText}).");
        }

        var both = await TimescaleSupport.ReadStuckPolicyJobsAsync(connection, D0.AddDays(6), null, ct);
        var rows = both.Stuck.Where(j => j.JobId == jobId).ToList();
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, j => j.Arm == StuckPolicyJobArm.NextStartNegativeInfinity);
        Assert.Contains(rows, j => j.Arm == StuckPolicyJobArm.DailyRollupStale);
    }

    /* ---------------- helpers ---------------- */

    private static async Task<bool> PrepareAsync(NpgsqlConnection connection, string connectionString, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);

        /* #1922: CREATE EXTENSION can end the backend it runs on when timescaledb is on disk but not preloaded,
           so it runs on the probe's own connection, never on the one these tests keep using. */
        if (!await LiveTimescaleProbe.TryEnableAsync(connectionString, ct))
        {
            return false;
        }

        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        return true;
    }

    /// <summary>One whole day of raw: 24 hours, two queries an hour, so each day holds 48 hourly samples.</summary>
    private static async Task InsertDayAsync(NpgsqlConnection connection, DateTime day, CancellationToken ct)
    {
        await using var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT (extract(epoch FROM $1::timestamp)::bigint * 100) + h * 10 + q, $1::timestamp + (h * interval '1 hour') + interval '30 minutes', $2::int, $3::text, 'RetryDb', 100 + q, 1000 + q,
    'Regular', $1::timestamp, 'mod', md5('retry' || q), 10, 500, 300, 900, 700, 'PRIMARY', (extract(epoch FROM $1::timestamp)::bigint / 3600) + h, $1::timestamp + (h * interval '1 hour')
FROM generate_series(0, 23) AS h CROSS JOIN generate_series(0, 1) AS q", connection);
        seed.Parameters.AddWithValue(day);
        seed.Parameters.AddWithValue(ServerId);
        seed.Parameters.AddWithValue(ServerName);
        await seed.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ResolveAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, view, ct);
        Assert.NotNull(materialization);
        return $"\"{materialization.Value.Schema}\".\"{materialization.Value.Name}\"";
    }

    private static async Task<long> SamplesAsync(NpgsqlConnection connection, string relation, DateTime day, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            $"SELECT coalesce(sum(sample_count), 0)::bigint FROM {relation} WHERE server_id = $1 AND bucket >= $2 AND bucket < $3", connection);
        read.Parameters.AddWithValue(ServerId);
        read.Parameters.AddWithValue(day);
        read.Parameters.AddWithValue(day.AddDays(1));
        return Convert.ToInt64(await read.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<int> RefreshJobIdAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(@"
SELECT j.job_id
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_refresh_continuous_aggregate'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = $1", connection);
        read.Parameters.AddWithValue(view);
        var id = await read.ExecuteScalarAsync(ct);
        Assert.NotNull(id);
        return Convert.ToInt32(id, System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<long> StartOffsetSecondsAsync(NpgsqlConnection connection, int jobId, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            "SELECT EXTRACT(EPOCH FROM (j.config->>'start_offset')::interval)::bigint FROM timescaledb_information.jobs AS j WHERE j.job_id = $1", connection);
        read.Parameters.AddWithValue(jobId);
        return Convert.ToInt64(await read.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>Every scheduled policy job this product creates: refresh policies on its aggregates, and the compression policies.</summary>
    private static async Task<List<(int JobId, string Proc, string Relation, long RetrySeconds)>> ScheduledJobRetryAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var jobs = new List<(int, string, string, long)>();
        await using var read = new NpgsqlCommand(@"
SELECT j.job_id, j.proc_name, coalesce(j.hypertable_schema || '.' || j.hypertable_name, ''), EXTRACT(EPOCH FROM j.retry_period)::bigint
FROM timescaledb_information.jobs AS j
WHERE j.proc_name LIKE '%compression%'
   OR j.proc_name LIKE '%columnstore%'
   OR (j.hypertable_schema = 'collect' AND j.proc_name = 'policy_refresh_continuous_aggregate')
ORDER BY j.job_id", connection);
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            jobs.Add((reader.GetInt32(0), reader.GetString(1), reader.GetString(2), reader.GetInt64(3)));
        }

        return jobs;
    }
}
