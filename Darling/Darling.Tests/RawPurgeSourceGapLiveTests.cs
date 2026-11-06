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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7961: the field sequence after a store outage. Raw <c>query_store_stats</c> (4-day retention) is held with
/// rows from before an outage gap, and its short consumer <c>query_store_stats_interval_hourly</c> (7-day
/// retention) has been backfilled over all of it. The startup pass arms the consumer, the consumer's own
/// retention run trims it to the first bucket after the gap, and the next pass must still find raw covered
/// (no RE-HELD) and the raw purge must run. The twin keeps the promise the gate exists for: a consumer that never
/// stored part of raw's span inside its own horizon keeps raw held.
///
/// <para>A real migrated store with its real continuous aggregates, retention policies and purge trigger. The
/// background workers are stopped, so the only retention runs are the ones each test makes itself.</para>
///
/// <para><b>#1776 own-store</b>: deliberately NOT <c>[Collection("live-postgres")]</c>. Goes through
/// <see cref="ScratchPostgres.CreateAsync"/> and never touches the shared database's tables.</para>
/// </summary>
public sealed class RawPurgeSourceGapLiveTests
{
    private const string Raw = "query_store_stats";
    private const string ShortConsumer = TimescaleSupport.QueryStoreStatsIntervalHourlyView;
    private const int SetupTimeoutSeconds = 120;

    /// <summary>Hours back of the newest and oldest raw row before the outage gap, and of the oldest after it.
    /// The gap (15 days back to 3.5 days back) spans the consumer's 7-day horizon, as in the field.</summary>
    private const int PreGapNewestHours = 15 * 24;
    private const int PreGapOldestHours = 28 * 24;
    private const int PostGapOldestHours = 84;

    /// <summary>The hole test's extra raw rows, hours back: the consumer's oldest bucket, and the bucket deleted
    /// after it. Both are inside the consumer's 7-day horizon (168 hours) and older than raw's 4 days (96 hours),
    /// and the hole is more than a day inside the horizon floor (about 167 hours), so a scan that starts a day
    /// late misses it.</summary>
    private const int HoleSeedOldestHours = 160;
    private const int HoleHours = 150;

    /// <summary>The field case. RED before #7961: the periodic pass re-holds raw, because the consumer's oldest
    /// bucket (the first after the gap) is later than now - 7 days + 1 hour.</summary>
    [Fact]
    public async Task ConsumerTrimmedAcrossAnOutageGap_RawStaysArmedAndPurges()
    {
        await RunAsync(async connection =>
        {
            var now = await SeedAndBackfillAsync(connection, shortConsumerFromHours: null);

            await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, new CapturingTestLogger(), TimescaleSupport.RetentionSweepPass.Startup, default);
            Assert.True(await RawArmedAsync(connection), "raw must arm at startup: the backfill gave both consumers every raw row");
            Assert.True(await ConsumerArmedAsync(connection), "the consumer's own gate is Covered, so the startup pass must arm it");
            await RunConsumerRetentionAsync(connection);

            /* The field shape, or this test proves nothing: the consumer's own run trimmed it to the first
               bucket after the gap, while raw still holds rows from before the gap. */
            var consumerOldest = await OldestAsync(connection, ShortConsumer, "bucket");
            var rawOldest = await OldestAsync(connection, Raw, "collection_time");
            Assert.True(consumerOldest >= now.AddHours(-PostGapOldestHours),
                $"the consumer's own 7-day run must trim it to the first bucket after the gap; oldest bucket {consumerOldest:O}, now {now:O}");
            Assert.True(rawOldest <= now.AddHours(-PreGapNewestHours),
                $"raw must still hold rows from before the gap; oldest {rawOldest:O}");

            var periodicLog = new CapturingTestLogger();
            await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, periodicLog, TimescaleSupport.RetentionSweepPass.Periodic, default);
            Assert.DoesNotContain($"Retention policy for {Raw} RE-HELD", periodicLog.Joined, StringComparison.Ordinal);
            Assert.True(await RawArmedAsync(connection), $"raw must stay armed; log: {periodicLog.Joined}");

            await StampCurrentEpochAsync(connection);
            var triggerLog = new CapturingTestLogger();
            await DarlingWorker.TriggerRawPurgeCoreAsync(connection, triggerLog, default);

            var record = await TimescaleSupport.ReadRawLastPurgeOutcomeAsync(connection, Raw, null, default);
            Assert.NotNull(record);
            Assert.True(record!.Outcome == "ran", $"the raw purge must run; outcome {record.Outcome}; log: {triggerLog.Joined}");
            Assert.True(await OldestAsync(connection, Raw, "collection_time") >= now.AddHours(-PostGapOldestHours),
                "the purge must drop the rows from before the gap");
        });
    }

    /// <summary>The negative twin: the same gap, but the consumer never stored the first 36 hours after it (it
    /// starts 2 days back). Those raw rows are inside its 7-day horizon, so raw is Short and stays held, and the
    /// purge does not run.</summary>
    [Fact]
    public async Task ConsumerMissingRowsInsideItsHorizon_RawStaysHeld()
    {
        await RunAsync(async connection =>
        {
            var now = await SeedAndBackfillAsync(connection, shortConsumerFromHours: 48);

            await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, new CapturingTestLogger(), TimescaleSupport.RetentionSweepPass.Startup, default);
            await RunConsumerRetentionAsync(connection);
            Assert.True(await ConsumerArmedAsync(connection), "the consumer's own gate is Covered, so the startup pass must arm it");

            var periodicLog = new CapturingTestLogger();
            await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, periodicLog, TimescaleSupport.RetentionSweepPass.Periodic, default);
            Assert.False(await RawArmedAsync(connection), $"raw rows inside the consumer's horizon that it never stored must keep raw held; log: {periodicLog.Joined}");

            await StampCurrentEpochAsync(connection);
            await DarlingWorker.TriggerRawPurgeCoreAsync(connection, new CapturingTestLogger(), default);

            var record = await TimescaleSupport.ReadRawLastPurgeOutcomeAsync(connection, Raw, null, default);
            Assert.NotNull(record);
            Assert.Equal("not_covered", record!.Outcome);
            Assert.True(await OldestAsync(connection, Raw, "collection_time") <= now.AddHours(-PreGapNewestHours),
                "raw must keep its rows from before the gap");
        });
    }

    /// <summary>The narrowed hole scan's negative (#7961 review round 1). The field case, plus raw rows 160 and 150
    /// hours back, inside the consumer's 7-day horizon and older than raw's 4 days. The consumer's oldest bucket
    /// is then the 160-hour one, so the gate's leading-edge check still says Covered and the consumer sits in
    /// <c>HorizonCovered</c>. One bucket after its oldest (150 hours back) is then deleted: a hole the gate cannot
    /// see, which only the scan finds. The scan must start at the consumer's horizon floor (about 167 hours back),
    /// so it reads the hole and the raw purge records <c>hole</c>. A scan that starts at the drop range's end or a
    /// day late (143 hours back) misses it and the purge runs.</summary>
    [Fact]
    public async Task ConsumerHoleInsideItsHorizonBelowRaw_RawStaysHeldAtTheHoleScan()
    {
        await RunAsync(async connection =>
        {
            var now = await SeedAndBackfillAsync(connection, shortConsumerFromHours: null, extraRawHoursBack: new[] { HoleSeedOldestHours, HoleHours });

            await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, new CapturingTestLogger(), TimescaleSupport.RetentionSweepPass.Startup, default);
            Assert.True(await RawArmedAsync(connection), "raw must arm at startup: the backfill gave both consumers every raw row");
            Assert.True(await ConsumerArmedAsync(connection), "the consumer's own gate is Covered, so the startup pass must arm it");
            await RunConsumerRetentionAsync(connection);

            /* The hole sits one bucket after the consumer's oldest, so the leading-edge gate cannot see it. */
            var holeBucket = now.AddHours(-HoleHours);
            Assert.Equal(now.AddHours(-HoleSeedOldestHours), await OldestAsync(connection, ShortConsumer, "bucket"));
            var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, ShortConsumer, default);
            Assert.NotNull(materialization);
            await using (var delete = new NpgsqlCommand(
                $"DELETE FROM \"{materialization!.Value.Schema}\".\"{materialization.Value.Name}\" WHERE bucket = $1", connection)
            { CommandTimeout = SetupTimeoutSeconds })
            {
                delete.Parameters.AddWithValue(DateTime.SpecifyKind(holeBucket, DateTimeKind.Unspecified));
                Assert.Equal(1, await delete.ExecuteNonQueryAsync());
            }

            /* The test reaches the scan, not the gate: the measure says Covered and names the consumer as
               judged within its own horizon, the only state in which the scan's floor moves. */
            var measure = await TimescaleSupport.MeasureRawTierCoverageAsync(connection, Raw, DateTime.UtcNow, default);
            Assert.Equal(TimescaleSupport.RetentionCoverage.Covered, measure.Verdict);
            Assert.Contains(measure.HorizonCovered, c => c.Consumer == ShortConsumer);

            await StampCurrentEpochAsync(connection);
            var triggerLog = new CapturingTestLogger();
            await DarlingWorker.TriggerRawPurgeCoreAsync(connection, triggerLog, default);

            var record = await TimescaleSupport.ReadRawLastPurgeOutcomeAsync(connection, Raw, null, default);
            Assert.NotNull(record);
            Assert.True(record!.Outcome == "hole", $"the hole scan must find the missing bucket; outcome {record.Outcome}; log: {triggerLog.Joined}");
            Assert.True(await OldestAsync(connection, Raw, "collection_time") <= now.AddHours(-PreGapNewestHours),
                "raw must keep its rows from before the gap");
        });
    }

    /// <summary>Seeds one raw row an hour on both sides of the gap, then refreshes every rollup built on raw
    /// (dependency order, then the baselines) from before raw's oldest row, the way <c>--backfill-rollups</c>
    /// leaves them. With <paramref name="shortConsumerFromHours"/> set, the short consumer is refreshed only from
    /// that many hours back. <paramref name="extraRawHoursBack"/> adds raw rows that many hours back, inside the gap. Returns the hour-aligned "now" the rows were placed against.</summary>
    private static async Task<DateTime> SeedAndBackfillAsync(NpgsqlConnection connection, int? shortConsumerFromHours, int[]? extraRawHoursBack = null)
    {
        await ExecuteAsync(connection, $"SELECT add_retention_policy('collect.{Raw}', drop_after => interval '{TimescaleSupport.RawRetentionInterval}')");

        var now = TimescaleSupport.AlignDown(DateTime.UtcNow, TimeSpan.FromHours(1));
        await using (var seed = new NpgsqlCommand($@"
INSERT INTO collect.{Raw}
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us)
SELECT
    n, $1::timestamp - (n * INTERVAL '1 hour'), 1, 'SQL01', 'GapDb', 'dbo.GapProc', '0xABCD',
    7, 9, 'Regular', 'PRIMARY',
    100000 - n, $1::timestamp - (n * INTERVAL '1 hour'), $1::timestamp - (n * INTERVAL '1 hour'),
    10, 1000, 500, 900, 400
FROM generate_series(1, $2) AS n
WHERE n <= $3 OR n >= $4 OR n = ANY($5)", connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            seed.Parameters.AddWithValue(DateTime.SpecifyKind(now, DateTimeKind.Unspecified));
            seed.Parameters.AddWithValue(PreGapOldestHours);
            seed.Parameters.AddWithValue(PostGapOldestHours);
            seed.Parameters.AddWithValue(PreGapNewestHours);
            seed.Parameters.AddWithValue(extraRawHoursBack ?? Array.Empty<int>());
            await seed.ExecuteNonQueryAsync();
        }

        var views = RollupBackfill.Targets
            .Where(t => string.Equals(t.RawTable, Raw, StringComparison.Ordinal))
            .Select(t => t.View)
            .Concat(TimescaleSupport.MaterializationHoleTargets.Where(t => string.Equals(t.Source, Raw, StringComparison.Ordinal)).Select(t => t.View))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        Assert.Contains(ShortConsumer, views);

        var backfillFrom = TimescaleSupport.AlignDown(now.AddHours(-PreGapOldestHours), TimeSpan.FromDays(1)).AddDays(-1);
        foreach (var view in views)
        {
            var from = string.Equals(view, ShortConsumer, StringComparison.Ordinal) && shortConsumerFromHours is int hours
                ? now.AddHours(-hours)
                : backfillFrom;
            await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, NULL)", connection) { CommandTimeout = SetupTimeoutSeconds };
            refresh.Parameters.AddWithValue(DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
            await refresh.ExecuteNonQueryAsync();
        }

        Assert.True(await OldestAsync(connection, ShortConsumer, "bucket") is DateTime, "the backfill must have materialized the short consumer");
        return now;
    }

    /// <summary>The consumer's own retention run, as TimescaleDB runs it once the startup pass arms it.</summary>
    private static async Task RunConsumerRetentionAsync(NpgsqlConnection connection)
    {
        int jobId;
        await using (var read = new NpgsqlCommand($@"SELECT j.job_id FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention' AND j.hypertable_schema = 'collect' AND j.hypertable_name = '{ShortConsumer}'", connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            var value = await read.ExecuteScalarAsync();
            Assert.True(value is int, $"the startup pass must have attached a retention policy to {ShortConsumer}");
            jobId = (int)value!;
        }

        await ExecuteAsync(connection, $"CALL run_job({jobId})");
    }

    private static async Task<bool> RawArmedAsync(NpgsqlConnection connection)
    {
        await using var read = new NpgsqlCommand(TimescaleSupport.RawArmedStateSql(Raw), connection) { CommandTimeout = SetupTimeoutSeconds };
        return await read.ExecuteScalarAsync() is true;
    }

    private static async Task<bool> ConsumerArmedAsync(NpgsqlConnection connection)
    {
        await using var read = new NpgsqlCommand(TimescaleSupport.RetentionPolicyScheduledSql(ShortConsumer), connection) { CommandTimeout = SetupTimeoutSeconds };
        return await read.ExecuteScalarAsync() is true;
    }

    private static async Task<DateTime?> OldestAsync(NpgsqlConnection connection, string relation, string column)
    {
        await using var read = new NpgsqlCommand($"SELECT min({column}) FROM collect.{relation}", connection) { CommandTimeout = SetupTimeoutSeconds };
        return await read.ExecuteScalarAsync() is DateTime oldest ? oldest : null;
    }

    private static async Task StampCurrentEpochAsync(NpgsqlConnection connection)
    {
        long epoch;
        await using (var read = new NpgsqlCommand(TimescaleSupport.PostmasterStartEpochMicrosecondsSql, connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            epoch = (long)(await read.ExecuteScalarAsync())!;
        }

        await using var stamp = new NpgsqlCommand(TimescaleSupport.RawRepairEpochStampSql(Raw), connection) { CommandTimeout = SetupTimeoutSeconds };
        stamp.Parameters.AddWithValue(epoch);
        await stamp.ExecuteNonQueryAsync();
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
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #7961 source-gap tests.");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            /* Max Auto Prepare=0, as in RawPurgeTriggerLiveTests: the purge runs a multi-statement string on
               this connection, which Npgsql refuses to auto-prepare. */
            var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { MaxAutoPrepare = 0 }.ConnectionString;
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            await PgMigrations.MigrateAsync(connection, default);

            var enabled = await TimescaleSupport.TryEnableAsync(connection, null, default);
            Assert.SkipWhen(!enabled, "The live #7961 source-gap tests need TimescaleDB.");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, default);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, default);
            await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()");

            await body(connection);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
