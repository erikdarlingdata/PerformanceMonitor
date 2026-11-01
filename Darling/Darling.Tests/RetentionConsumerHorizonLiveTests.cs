/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7114: the retention coverage gate judges a consumer only within its own armed retention. The field case:
/// <c>query_store_stats_interval_hourly</c> (7 days) held 27 days, <c>query_store_stats_interval_daily</c>
/// (10 days) trimmed itself to now minus 10 days on its own daily run, and the gate called it short on every
/// pass, so the hold grew a day each day and never released.
///
/// <para>Each test builds the source and its consumers as plain hypertables under the real relation names in
/// its own database, so the gate's job-catalog lookup and its coverage probe run exactly as they do on a store,
/// with no continuous-aggregate refresh to wait on. "now" is passed to the measure, so the seeded buckets sit at
/// known distances from it. Every consumer retention job is parked at <c>next_start = infinity</c> with its
/// <c>scheduled</c> flag left as the test sets it, and the background workers are stopped, so no retention run
/// moves the rows under a test.</para>
///
/// <para><b>#1776 own-store</b>: deliberately NOT <c>[Collection("live-postgres")]</c>. Goes through
/// <see cref="ScratchPostgres.CreateAsync"/> and never touches the shared database's tables.</para>
/// </summary>
public sealed class RetentionConsumerHorizonLiveTests
{
    private const string Source = TimescaleSupport.QueryStoreStatsIntervalHourlyView;
    private const string IntervalDaily = TimescaleSupport.QueryStoreStatsIntervalDailyView;
    private const string DayGrainDaily = TimescaleSupport.QueryStoreStatsDayGrainDailyView;
    private const int SetupTimeoutSeconds = 60;

    /// <summary>The service clock the measure is given: a whole hour, so day and hour arithmetic below stays exact.</summary>
    private static readonly DateTime Now = TimescaleSupport.AlignDown(DateTime.UtcNow, TimeSpan.FromHours(1));

    /// <summary>The field case. The source holds 27 days, the consumer's 10-day retention is armed, and the
    /// consumer holds from its own horizon (the day bucket that contains now minus 10 days). Covered, and the
    /// ARMED clause names the retention the check was made within. RED before #7114: Short, with the consumer
    /// named short.</summary>
    [Fact]
    public async Task ArmedConsumer_HoldingFromItsOwnHorizon_CoversASourceHeldPastIt()
    {
        await RunAsync(async connection =>
        {
            await CreateSourceAsync(connection, Now.AddDays(-27));
            await CreateConsumerAsync(connection, IntervalDaily, Now.AddDays(-10), retention: "10 days", scheduled: true);

            /* The consumer's own consumer, as in the field: the day-grain daily keeps every day and reaches
               interval_daily's oldest bucket, so interval_daily's own gate is Covered on this measure. */
            await CreateConsumerAsync(connection, DayGrainDaily, Now.AddDays(-10), retention: null, scheduled: false);

            var measure = await MeasureAsync(connection, IntervalDaily);

            Assert.True(measure.Verdict == TimescaleSupport.RetentionCoverage.Covered,
                $"a consumer holding from its own armed 10-day horizon keeps everything it would keep, so the source must read Covered; got {measure.Verdict}, short: {string.Join(", ", measure.ShortConsumers)}");
            Assert.Equal(", checked within query_store_stats_interval_daily's 10-day retention", TimescaleSupport.DescribeHorizonCoverage(measure.HorizonCovered));
        });
    }

    /// <summary>The consumer covers from its own horizon, but its own gate is Short on this same measure: its
    /// day-grain consumer (no retention, keeps every day) starts five days later than it, the rebuilt-consumer
    /// shape of #1869/#1877. The sweep goes source-first, so it judges the hourly source before it re-holds
    /// interval_daily. The horizon rule must not apply here: interval_daily is about to be held, and the days
    /// the hourly source would drop below its horizon are the days a backfill needs to rebuild the day-grain
    /// daily. Short, naming interval_daily. RED on the first #7114 rule (092e9e3f8): Covered.</summary>
    [Fact]
    public async Task HorizonCoveredConsumer_WhoseOwnGateIsShort_KeepsTheStrictRule()
    {
        await RunAsync(async connection =>
        {
            await CreateSourceAsync(connection, Now.AddDays(-27));
            await CreateConsumerAsync(connection, IntervalDaily, Now.AddDays(-10), retention: "10 days", scheduled: true);
            await CreateConsumerAsync(connection, DayGrainDaily, Now.AddDays(-5), retention: null, scheduled: false);

            var measure = await MeasureAsync(connection, IntervalDaily);

            Assert.True(measure.Verdict == TimescaleSupport.RetentionCoverage.Short,
                $"interval_daily's own gate is Short (its day-grain consumer starts later than it), so its retention is about to be held and the hourly source must keep the strict rule; got {measure.Verdict}");
            Assert.Equal(new[] { IntervalDaily }, measure.ShortConsumers);
            Assert.Empty(measure.HorizonCovered);
        });
    }

    /// <summary>The job lookup on a real store: migrated by <see cref="PgMigrations"/>, with its real
    /// continuous aggregates and the real retention policies the sweep creates and arms (an empty store arms
    /// every policy). TimescaleDB reports a policy on an aggregate under the aggregate's view name (#3610), and
    /// <see cref="TimescaleSupport.ConsumerRetentionHorizonSql"/> matches the consumer names the coverage lists
    /// carry, which are view names. If the catalog named the materialization hypertable instead, the lookup
    /// would return nothing and the horizon rule would never apply to an aggregate.</summary>
    [Fact]
    public async Task ConsumerRetentionHorizonSql_OnAMigratedStore_FindsIntervalDailyUnderItsViewName()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #7114 consumer-horizon tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.SkipWhen(!await TimescaleSupport.TryEnableAsync(connection, null, ct), "The live #7114 consumer-horizon tests need TimescaleDB.");
            await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, TimescaleSupport.RetentionSweepPass.Startup, null, ct);

            await using var read = new NpgsqlCommand(TimescaleSupport.ConsumerRetentionHorizonSql, connection) { CommandTimeout = SetupTimeoutSeconds };
            read.Parameters.AddWithValue(new[] { IntervalDaily });
            await using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct),
                $"no scheduled retention job was found under the view name {IntervalDaily}, so the horizon rule could never apply to it");
            Assert.Equal(IntervalDaily, reader.GetString(0));
            Assert.Equal(TimescaleSupport.IntervalDailyRetentionInterval, reader.GetString(1));
            Assert.Equal(TimeSpan.FromDays(10).TotalSeconds, reader.GetDouble(2));
            Assert.False(await reader.ReadAsync(ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>A real gap inside the consumer's horizon: it holds only from now minus 8 days, so two days it
    /// would keep are missing. Short.</summary>
    [Fact]
    public async Task ArmedConsumer_MissingDaysInsideItsHorizon_IsShort()
    {
        await RunAsync(async connection =>
        {
            await CreateSourceAsync(connection, Now.AddDays(-27));
            await CreateConsumerAsync(connection, IntervalDaily, Now.AddDays(-8), retention: "10 days", scheduled: true);

            var measure = await MeasureAsync(connection, IntervalDaily);

            Assert.Equal(TimescaleSupport.RetentionCoverage.Short, measure.Verdict);
            Assert.Equal(new[] { IntervalDaily }, measure.ShortConsumers);
            Assert.Empty(measure.HorizonCovered);
        });
    }

    /// <summary>The consumer's own retention exists but is not scheduled (held by the same gate, or paused by
    /// hand): it drops nothing, so it keeps the strict rule and its later floor is Short.</summary>
    [Fact]
    public async Task UnscheduledConsumerRetention_KeepsTheStrictRule()
    {
        await RunAsync(async connection =>
        {
            await CreateSourceAsync(connection, Now.AddDays(-27));
            await CreateConsumerAsync(connection, IntervalDaily, Now.AddDays(-10), retention: "10 days", scheduled: false);

            var measure = await MeasureAsync(connection, IntervalDaily);

            Assert.Equal(TimescaleSupport.RetentionCoverage.Short, measure.Verdict);
            Assert.Equal(new[] { IntervalDaily }, measure.ShortConsumers);
        });
    }

    /// <summary>A consumer with no retention policy keeps everything, so it must reach the source's oldest row.
    /// Short, and Covered once it does.</summary>
    [Fact]
    public async Task ConsumerWithNoRetention_KeepsTheStrictRule()
    {
        await RunAsync(async connection =>
        {
            await CreateSourceAsync(connection, Now.AddDays(-27));
            await CreateConsumerAsync(connection, IntervalDaily, Now.AddDays(-10), retention: null, scheduled: false);

            var shortMeasure = await MeasureAsync(connection, IntervalDaily);
            Assert.Equal(TimescaleSupport.RetentionCoverage.Short, shortMeasure.Verdict);

            await InsertDaysAsync(connection, IntervalDaily, Now.AddDays(-27), Now.AddDays(-10));
            var coveredMeasure = await MeasureAsync(connection, IntervalDaily);
            Assert.Equal(TimescaleSupport.RetentionCoverage.Covered, coveredMeasure.Verdict);
            Assert.Empty(coveredMeasure.HorizonCovered);
        });
    }

    /// <summary>Two consumers: one covered within its own armed horizon, one with no retention that is short.
    /// The verdict is still an AND, so the source is Short and only the second consumer is named.</summary>
    [Fact]
    public async Task HorizonCoveredConsumer_DoesNotCoverForAShortConsumerWithNoRetention()
    {
        await RunAsync(async connection =>
        {
            await CreateSourceAsync(connection, Now.AddDays(-27));
            await CreateConsumerAsync(connection, IntervalDaily, Now.AddDays(-10), retention: "10 days", scheduled: true);
            await CreateConsumerAsync(connection, DayGrainDaily, Now.AddDays(-10), retention: null, scheduled: false);

            var measure = await MeasureAsync(connection, IntervalDaily, DayGrainDaily);

            Assert.Equal(TimescaleSupport.RetentionCoverage.Short, measure.Verdict);
            Assert.Equal(new[] { DayGrainDaily }, measure.ShortConsumers);
            Assert.Equal(new[] { IntervalDaily }, measure.HorizonCovered.Select(c => c.Consumer));
        });
    }

    private static Task<TimescaleSupport.RetentionCoverageMeasure> MeasureAsync(NpgsqlConnection connection, params string[] consumers)
        => TimescaleSupport.MeasureRetentionCoverageAsync(connection, Source, "bucket", consumers, Now, default);

    /// <summary>The source: one hourly row a day from <paramref name="oldest"/> up to now.</summary>
    private static async Task CreateSourceAsync(NpgsqlConnection connection, DateTime oldest)
    {
        await ExecuteAsync(connection, $"CREATE TABLE collect.{Source} (bucket timestamp NOT NULL)");
        await ExecuteAsync(connection, $"SELECT create_hypertable('collect.{Source}', 'bucket', chunk_time_interval => INTERVAL '1 day')");
        await using var insert = new NpgsqlCommand(
            $"INSERT INTO collect.{Source} (bucket) SELECT g FROM generate_series($1::timestamp, $2::timestamp, INTERVAL '1 day') AS g",
            connection) { CommandTimeout = SetupTimeoutSeconds };
        insert.Parameters.AddWithValue(DateTime.SpecifyKind(oldest, DateTimeKind.Unspecified));
        insert.Parameters.AddWithValue(DateTime.SpecifyKind(Now, DateTimeKind.Unspecified));
        await insert.ExecuteNonQueryAsync();
    }

    /// <summary>A day-bucketed consumer holding one bucket a day from the day containing <paramref name="from"/>
    /// up to today, with a retention policy of <paramref name="retention"/> (none when null) whose job is parked
    /// at <c>next_start = infinity</c> and carries <paramref name="scheduled"/>.</summary>
    private static async Task CreateConsumerAsync(NpgsqlConnection connection, string consumer, DateTime from, string? retention, bool scheduled)
    {
        await ExecuteAsync(connection, $"CREATE TABLE collect.{consumer} (bucket timestamp NOT NULL)");
        await ExecuteAsync(connection, $"SELECT create_hypertable('collect.{consumer}', 'bucket', chunk_time_interval => INTERVAL '1 day')");
        await InsertDaysAsync(connection, consumer, from, Now);

        if (retention is not null)
        {
            await ExecuteAsync(connection, $"SELECT add_retention_policy('collect.{consumer}', INTERVAL '{retention}')");
            await ExecuteAsync(connection, $@"SELECT alter_job(j.job_id, scheduled => {(scheduled ? "true" : "false")}, next_start => 'infinity'::timestamptz)
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention' AND j.hypertable_schema = 'collect' AND j.hypertable_name = '{consumer}'");
        }
    }

    /// <summary>One day bucket per day, from the day containing <paramref name="from"/> to the day containing <paramref name="to"/>.</summary>
    private static async Task InsertDaysAsync(NpgsqlConnection connection, string consumer, DateTime from, DateTime to)
    {
        await using var insert = new NpgsqlCommand(
            $"INSERT INTO collect.{consumer} (bucket) SELECT g FROM generate_series(date_trunc('day', $1::timestamp), date_trunc('day', $2::timestamp), INTERVAL '1 day') AS g",
            connection) { CommandTimeout = SetupTimeoutSeconds };
        insert.Parameters.AddWithValue(DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        insert.Parameters.AddWithValue(DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        await insert.ExecuteNonQueryAsync();
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = SetupTimeoutSeconds };
        await command.ExecuteNonQueryAsync();
    }

    private static async Task RunAsync(Func<NpgsqlConnection, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #7114 consumer-horizon tests.");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync();
            Assert.SkipWhen(!await TimescaleSupport.TryEnableAsync(connection, null, default), "The live #7114 consumer-horizon tests need TimescaleDB.");
            await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()");
            await ExecuteAsync(connection, "CREATE SCHEMA IF NOT EXISTS collect");

            await body(connection);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
