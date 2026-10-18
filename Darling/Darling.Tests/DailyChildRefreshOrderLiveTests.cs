/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6070, end to end on a scratch store: the day-grain Query Store daily (<c>query_store_stats_daygrain_daily</c>)
/// is a daily built on ANOTHER daily (<c>query_store_stats_interval_daily</c>), and both daily refresh jobs
/// are on finish-to-start scheduling, so the child's job can run before its parent's every day. At any run time
/// a daily policy's window holds exactly one whole bucket, so the child refreshed a day its parent had not
/// written yet and never came back to it. Three pins: the window shape that fixes it (driven with explicit
/// refresh windows in the bug's order, never the job scheduler), the startup converge that moves an existing
/// job onto it, and the repair that fills the days already missed.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here goes through
   ScratchPostgres.CreateAsync, which reaches DARLING_TEST_PG only to CREATE and DROP its own database and then
   works entirely inside it. It never touches the shared database's tables, so it cannot race the live
   collection, and serializing it would be pure slowdown. Leave it out; this comment is here so the next sweep
   does not "fix" it. */
public sealed class DailyChildRefreshOrderLiveTests
{
    private const int ServerId = -607001;
    private const string ServerName = "daygrain-order";

    /// <summary>Fixed anchor day (never wall clock), so the tests pass at any hour.</summary>
    private static readonly DateTime D0 = new(2026, 3, 2, 0, 0, 0, DateTimeKind.Unspecified);

    private static readonly string Hourly = TimescaleSupport.QueryStoreStatsIntervalHourlyView;
    private static readonly string Parent = TimescaleSupport.QueryStoreStatsIntervalDailyView;
    private static readonly string Child = TimescaleSupport.QueryStoreStatsDayGrainDailyView;

    /// <summary>
    /// The bug's order, run the way the policies run it. Raw rows for ONE day, the hourly refreshed (it is
    /// refreshed every hour, so it holds the day long before either daily runs). Then, on each following day,
    /// the child's policy window and then the parent's, both at that day's 10:00. The child's window is the one
    /// <see cref="TimescaleSupport.DailyRefreshStartSpanFor"/> names, so on the old 3-day window the day is never
    /// refreshed on the child, and on the new 4-day window it is refreshed on the day after the parent wrote it.
    /// The control run with an explicit 3-day span shows the mechanism, so the pin cannot pass for the wrong reason.
    /// </summary>
    [Fact]
    public async Task TheChildsWindow_StillHoldsTheDayItsParentWroteAfterIt_WhereThreeDaysNeverComesBack()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #6070 order test.");
        var ct = TestContext.Current.CancellationToken;

        // The shipped span, then the old one as the control.
        foreach (var (span, childHoldsTheDay) in new[] { (TimescaleSupport.DailyRefreshStartSpanFor(Child), true), (TimeSpan.FromDays(3), false) })
        {
            await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            var bodySucceeded = false;
            try
            {
                Assert.SkipWhen(!await PrepareAsync(connection, scratch.ConnectionString, ct), "The live #6070 tests need TimescaleDB.");

                await InsertDayAsync(connection, D0, ct);
                await RefreshAsync(connection, Hourly, D0, D0.AddDays(1), ct);

                // Day D0+2 is the first run that can see D0 (the policy window ends one day before the run).
                for (var run = D0.AddDays(2); run <= D0.AddDays(5); run = run.AddDays(1))
                {
                    var at = run.AddHours(10);

                    // The child first (the bug's order), then the parent.
                    await RefreshAsync(connection, Child, at - span, at - TimeSpan.FromDays(1), ct);
                    await RefreshAsync(connection, Parent, at - TimescaleSupport.DailyRefreshStartSpan, at - TimeSpan.FromDays(1), ct);
                }

                var parentMaterialization = await ResolveAsync(connection, Parent, ct);
                var childMaterialization = await ResolveAsync(connection, Child, ct);
                var parentSamples = await SamplesAsync(connection, parentMaterialization, D0, ct);
                Assert.Equal(48L, parentSamples);   // the parent always gets there: its own window holds the day on its own run

                var childSamples = await SamplesAsync(connection, childMaterialization, D0, ct);
                if (childHoldsTheDay)
                {
                    Assert.Equal(parentSamples, childSamples);
                }
                else
                {
                    Assert.Equal(0L, childSamples);   // the bug: 12 days, 0 rows
                }

                bodySucceeded = true;
            }
            finally
            {
                await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
            }
        }
    }

    /// <summary>The emitted window follows the PARENT relation: only a daily built on a daily is a bucket wider.</summary>
    [Fact]
    public void TheWiderWindow_IsDerivedFromTheParentRelation_NotFromAName()
    {
        Assert.Equal(new[] { Child }, TimescaleSupport.DailyAggregates.Select(a => a.View).Where(TimescaleSupport.IsDailyParentedDaily).ToArray());

        foreach (var (_, view) in TimescaleSupport.DailyAggregates)
        {
            var wide = view == Child;
            Assert.Equal(wide ? "4 days" : "3 days", TimescaleSupport.DailyRefreshStartOffsetFor(view));
            Assert.Equal(TimeSpan.FromDays(wide ? 4 : 3), TimescaleSupport.DailyRefreshStartSpanFor(view));
            Assert.Contains($"start_offset => INTERVAL '{(wide ? 4 : 3)} days'", TimescaleSupport.AddDailyRefreshPolicySql(view), StringComparison.Ordinal);
        }

        Assert.True(TimescaleSupport.DailyChildRefreshStartSpan < TimescaleSupport.HourlyRetentionSpan);
        Assert.True(TimescaleSupport.AggregateCompressAfterSpanFor(Child) >= TimescaleSupport.DailyChildRefreshStartSpan + TimescaleSupport.DailyBucket,
            "compress_after must clear the wider refresh window aligned down to a bucket");
    }

    /// <summary>
    /// A store made on the old code has the daygrain job on 3 days. The converge moves ONLY that job, ONLY its
    /// <c>start_offset</c>, leaves every other key and the schedule and the paused state alone, and a second pass
    /// issues no <c>alter_job</c>. The aggregate ensure that follows then finds a matching policy (no 22023).
    /// </summary>
    [Fact]
    public async Task TheConverge_WidensAnOldJobOnce_AndTouchesNothingElse()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #6070 converge test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            Assert.SkipWhen(!await PrepareAsync(connection, scratch.ConnectionString, ct), "The live #6070 tests need TimescaleDB.");

            // Make the old shape: the child's job on 3 days, and paused, so "never arms" is observable.
            var before = await JobAsync(connection, Child, ct);
            Assert.Equal(4 * 86400L, before.StartOffsetSeconds);
            await using (var old = new NpgsqlCommand(
                "SELECT alter_job(j.job_id, scheduled => false, config => jsonb_set(j.config, '{start_offset}', to_jsonb('3 days'::text))) " +
                "FROM timescaledb_information.jobs AS j WHERE j.job_id = $1::integer", connection))
            {
                old.Parameters.AddWithValue(before.JobId);
                await old.ExecuteNonQueryAsync(ct);
            }

            var oldJob = await JobAsync(connection, Child, ct);
            Assert.Equal(3 * 86400L, oldJob.StartOffsetSeconds);
            var hourlyParentedBefore = await JobAsync(connection, TimescaleSupport.QueryStoreStatsDailyView, ct);

            var log = new CapturingTestLogger();
            Assert.Equal(1, await TimescaleSupport.ConvergeDailyChildRefreshWindowAsync(connection, log, ct));
            Assert.Contains("#6070", log.Joined, StringComparison.Ordinal);

            var after = await JobAsync(connection, Child, ct);
            Assert.Equal(4 * 86400L, after.StartOffsetSeconds);
            Assert.Equal(oldJob.EndOffsetSeconds, after.EndOffsetSeconds);
            Assert.Equal(oldJob.BucketsPerBatch, after.BucketsPerBatch);
            Assert.Equal(oldJob.FixedSchedule, after.FixedSchedule);
            Assert.Equal(oldJob.InitialStart, after.InitialStart);
            Assert.False(after.Scheduled, "the converge must never arm a paused job");

            // An hourly-parented daily is not touched.
            Assert.Equal(hourlyParentedBefore, await JobAsync(connection, TimescaleSupport.QueryStoreStatsDailyView, ct));

            // Second pass: nothing to do, nothing logged.
            var second = new CapturingTestLogger();
            Assert.Equal(0, await TimescaleSupport.ConvergeDailyChildRefreshWindowAsync(connection, second, ct));
            Assert.DoesNotContain("#6070", second.Joined, StringComparison.Ordinal);

            // And the ensure that follows keeps the 4-day window (no 22023 on a matching policy).
            var ensureLog = new CapturingTestLogger();
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, ensureLog, ct);
            Assert.DoesNotContain("setup failed", ensureLog.Joined, StringComparison.Ordinal);
            Assert.Equal(4 * 86400L, (await JobAsync(connection, Child, ct)).StartOffsetSeconds);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The repair: days the parent daily holds and the child lacks are refreshed on the child, newest first, only
    /// when complete, and a second pass finds nothing. D0 and D0+2 are the holes; D0+1 is already held (left
    /// alone); D0+3 is the day still filling at the first clock, so it waits for the second.
    /// </summary>
    [Fact]
    public async Task TheRepair_FillsTheDaysTheParentHoldsAndTheChildLacks_CompleteDaysOnly()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #6070 repair test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            Assert.SkipWhen(!await PrepareAsync(connection, scratch.ConnectionString, ct), "The live #6070 tests need TimescaleDB.");

            // A child that has materialized nothing at all (the shape the hole scan skips) cannot be repaired
            // from a parent that holds nothing either.
            var nothing = await TimescaleSupport.RepairDailyChildHolesAsync(connection, null, D0.AddDays(10), ct);
            Assert.Equal(0, nothing.DaysFound);

            foreach (var day in Enumerable.Range(0, 4).Select(n => D0.AddDays(n)))
            {
                await InsertDayAsync(connection, day, ct);
            }

            await RefreshAsync(connection, Hourly, D0, D0.AddDays(4), ct);
            await RefreshAsync(connection, Parent, D0, D0.AddDays(4), ct);
            await RefreshAsync(connection, Child, D0.AddDays(1), D0.AddDays(2), ct);

            var parentMaterialization = await ResolveAsync(connection, Parent, ct);
            var childMaterialization = await ResolveAsync(connection, Child, ct);
            foreach (var day in Enumerable.Range(0, 4).Select(n => D0.AddDays(n)))
            {
                Assert.Equal(48L, await SamplesAsync(connection, parentMaterialization, day, ct));
            }

            Assert.Equal(48L, await SamplesAsync(connection, childMaterialization, D0.AddDays(1), ct));
            Assert.Equal(0L, await SamplesAsync(connection, childMaterialization, D0, ct));

            var log = new CapturingTestLogger();
            var first = await TimescaleSupport.RepairDailyChildHolesAsync(connection, log, D0.AddDays(3).AddHours(12), ct);
            Assert.Equal(1, first.DailiesScanned);
            Assert.Equal(2, first.DaysFound);
            Assert.Equal(2, first.DaysRefreshed);
            Assert.Equal(0, first.Failures);
            Assert.False(first.DaysRemaining);
            Assert.Contains("#6070", log.Joined, StringComparison.Ordinal);

            Assert.Equal(48L, await SamplesAsync(connection, childMaterialization, D0, ct));
            Assert.Equal(48L, await SamplesAsync(connection, childMaterialization, D0.AddDays(2), ct));
            Assert.Equal(0L, await SamplesAsync(connection, childMaterialization, D0.AddDays(3), ct));

            var second = await TimescaleSupport.RepairDailyChildHolesAsync(connection, null, D0.AddDays(3).AddHours(13), ct);
            Assert.Equal(0, second.DaysFound);

            // The day that was still filling is complete a day later, and is then filled.
            var third = await TimescaleSupport.RepairDailyChildHolesAsync(connection, null, D0.AddDays(4).AddHours(1), ct);
            Assert.Equal(1, third.DaysFound);
            Assert.Equal(1, third.DaysRefreshed);
            Assert.Equal(48L, await SamplesAsync(connection, childMaterialization, D0.AddDays(3), ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Migrates, enables TimescaleDB, builds every aggregate with its real policy, then stops the scheduler
    /// so only the explicit refreshes in the tests run. False when TimescaleDB is not available.</summary>
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
SELECT (extract(epoch FROM $1::timestamp)::bigint * 100) + h * 10 + q, $1::timestamp + (h * interval '1 hour') + interval '30 minutes', $2::int, $3::text, 'OrderDb', 100 + q, 1000 + q,
    'Regular', $1::timestamp, 'mod', md5('order' || q), 10, 500, 300, 900, 700, 'PRIMARY', (extract(epoch FROM $1::timestamp)::bigint / 3600) + h, $1::timestamp + (h * interval '1 hour')
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
        return Convert.ToInt64(await read.ExecuteScalarAsync(ct));
    }

    private sealed record JobState(
        int JobId, long? StartOffsetSeconds, long? EndOffsetSeconds, int? BucketsPerBatch, bool FixedSchedule, DateTime? InitialStart, bool Scheduled);

    private static async Task<JobState> JobAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(@"
SELECT j.job_id,
       EXTRACT(EPOCH FROM (j.config->>'start_offset')::interval)::bigint,
       EXTRACT(EPOCH FROM (j.config->>'end_offset')::interval)::bigint,
       (j.config->>'buckets_per_batch')::int,
       j.fixed_schedule, j.initial_start AT TIME ZONE 'UTC', j.scheduled
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_refresh_continuous_aggregate' AND j.hypertable_schema = 'collect' AND j.hypertable_name = $1", connection);
        read.Parameters.AddWithValue(view);
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"{view} has no refresh policy job");
        return new JobState(
            Convert.ToInt32(reader.GetValue(0)),
            reader.IsDBNull(1) ? null : reader.GetInt64(1),
            reader.IsDBNull(2) ? null : reader.GetInt64(2),
            reader.IsDBNull(3) ? null : reader.GetInt32(3),
            reader.GetBoolean(4),
            reader.IsDBNull(5) ? null : reader.GetDateTime(5),
            reader.GetBoolean(6));
    }
}
