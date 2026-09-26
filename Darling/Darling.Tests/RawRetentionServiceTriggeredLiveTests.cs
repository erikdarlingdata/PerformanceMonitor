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
/// #4299 (d'): the raw retention jobs (<see cref="TimescaleSupport.RawRelations"/>) never run on
/// TimescaleDB's own scheduler any more — <see cref="TimescaleSupport.EnsureRetentionPoliciesAsync"/>
/// converges each one onto <c>scheduled = false</c> unconditionally and carries the coverage gate's
/// verdict on <c>config-&gt;&gt;'darling_armed'</c> instead. These pins hold the sweep's four load-bearing
/// behaviors against a real TimescaleDB store: a product-created raw job converges onto the new shape, a
/// DBA's <c>alter_job(scheduled =&gt; true)</c> is reverted on the very next pass regardless of the
/// coverage verdict that pass reaches, a non-raw relation's arm/hold semantics are untouched, and a
/// Covered verdict still leaves <c>scheduled = false</c> (never <c>true</c>, which is how the OLD code
/// armed a raw relation).
///
/// <para><b>#1776 own-store</b>: deliberately NOT <c>[Collection("live-postgres")]</c>. Every test here
/// goes through <see cref="ScratchPostgres.CreateAsync"/>, which reaches <c>DARLING_TEST_PG</c> only to
/// CREATE and DROP its own database, and works entirely inside it afterward. It never touches the shared
/// database's tables, so it cannot race the live collection, and serializing it would be pure slowdown.</para>
/// </summary>
public sealed class RawRetentionServiceTriggeredLiveTests
{
    private const string Raw = "query_stats";
    private const int PolicyReadTimeoutSeconds = 30;

    private static async Task<bool?> ScheduledAsync(NpgsqlConnection connection, string relation)
    {
        using var read = new NpgsqlCommand(TimescaleSupport.RetentionPolicyScheduledSql(relation), connection) { CommandTimeout = PolicyReadTimeoutSeconds };
        var flag = await read.ExecuteScalarAsync();
        return flag is bool b ? b : (bool?)null;
    }

    private static async Task<string?> ArmedKeyAsync(NpgsqlConnection connection, string relation)
    {
        using var read = new NpgsqlCommand($@"SELECT j.config->>'darling_armed' FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention' AND j.hypertable_schema = 'collect' AND j.hypertable_name = '{relation}'", connection) { CommandTimeout = PolicyReadTimeoutSeconds };
        var value = await read.ExecuteScalarAsync();
        return value as string;
    }

    private static async Task<string?> DropAfterAsync(NpgsqlConnection connection, string relation)
    {
        using var read = new NpgsqlCommand($@"SELECT j.config->>'drop_after' FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention' AND j.hypertable_schema = 'collect' AND j.hypertable_name = '{relation}'", connection) { CommandTimeout = PolicyReadTimeoutSeconds };
        var value = await read.ExecuteScalarAsync();
        return value as string;
    }

    /// <summary>
    /// Boots a fresh scratch store, migrates it, enables TimescaleDB, converts the collectors and
    /// collection_log to hypertables, ensures the aggregates, and runs the START-path retention sweep once —
    /// the product's real create-then-judge path, which is what makes <see cref="Raw"/>'s job here a
    /// PRODUCT-CREATED one rather than a hand-built row.
    /// </summary>
    private static async Task<NpgsqlConnection> BootAsync(ScratchPostgres scratch)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync();
        await PgMigrations.MigrateAsync(connection, default);

        var enabled = await TimescaleSupport.TryEnableAsync(connection, null, default);
        Assert.SkipWhen(!enabled, "The live #4299 raw-retention test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, default);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, default));
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, default);

        /* Stop the scratch database's own scheduler: PASS 0 below creates every policy PAUSED (the create's
           own transaction does that), so nothing here needs the background workers running, and a policy
           that fired on its own clock mid-test could flip the very flags these pins are reading. */
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync();
        }

        return connection;
    }

    [Fact]
    public async Task ProductCreatedRawJob_Converges_FromTheOldScheduledShape_ToTheConfigKeyVerdict()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4299 raw-retention test.");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);
        await using var connection = await BootAsync(scratch);

        var bodySucceeded = false;
        try
        {
            /* PASS 0 (inside BootAsync's callers via EnsureRetentionPoliciesAsync would be circular here —
               call it explicitly): an empty store's coverage is Short, so query_stats is created and HELD.
               The sweep already converges it onto the new shape once; force the OLD shape back on top so this
               pin proves the NEXT pass re-converges it, not that creation happened to land there. */
            var start = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, default);
            Assert.True(start.InPlace == TimescaleSupport.RetentionPolicies.Count, "the start pass must apply every policy");

            using (var force = new NpgsqlCommand($@"SELECT alter_job(j.job_id, scheduled => true, config => (j.config - 'darling_armed'))
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention' AND j.hypertable_schema = 'collect' AND j.hypertable_name = '{Raw}'", connection) { CommandTimeout = PolicyReadTimeoutSeconds })
            {
                await force.ExecuteNonQueryAsync();
            }

            Assert.True(await ScheduledAsync(connection, Raw), "the forced OLD shape must have taken, or this pin proves nothing");
            Assert.Null(await ArmedKeyAsync(connection, Raw));

            var pass = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, TimescaleSupport.RetentionSweepPass.Periodic, default);

            Assert.False(await ScheduledAsync(connection, Raw), "the converge pass must force scheduled = false on a raw relation");
            var armedKey = await ArmedKeyAsync(connection, Raw);
            Assert.NotNull(armedKey);
            /* An empty raw source reads Covered immediately (MeasureRetentionCoverageAsync: no rows in the
               source at all means nothing to lose, so it arms on sight) — deterministic on a freshly-migrated
               scratch store with no query_stats rows. */
            Assert.Equal("true", armedKey);
            Assert.NotNull(await DropAfterAsync(connection, Raw));
            _ = pass;

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                foreach (var relation in TimescaleSupport.RetentionPolicies.Select(p => p.Relation))
                {
                    await batch.RemoveRetentionPolicyAsync(relation, cleanupCt);
                }
            });
        }
    }

    [Fact]
    public async Task DbaHandArm_OfAConvergedRawJob_IsRevertedByTheVeryNextPass()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4299 raw-retention test.");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);
        await using var connection = await BootAsync(scratch);

        var bodySucceeded = false;
        try
        {
            var start = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, default);
            Assert.True(start.InPlace == TimescaleSupport.RetentionPolicies.Count, "the start pass must apply every policy");

            /* Starting from an ALREADY-CONVERGED job (the start pass's own output), not a hand-built shape:
               this proves the converge runs every pass, not merely that creation converges once. */
            Assert.False(await ScheduledAsync(connection, Raw));
            Assert.NotNull(await ArmedKeyAsync(connection, Raw));

            using (var handArm = new NpgsqlCommand($@"SELECT alter_job(j.job_id, scheduled => true)
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention' AND j.hypertable_schema = 'collect' AND j.hypertable_name = '{Raw}'", connection) { CommandTimeout = PolicyReadTimeoutSeconds })
            {
                await handArm.ExecuteNonQueryAsync();
            }

            Assert.True(await ScheduledAsync(connection, Raw), "the hand-arm must have taken, or the revert below proves nothing");

            var pass = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, TimescaleSupport.RetentionSweepPass.Periodic, default);
            Assert.False(await ScheduledAsync(connection, Raw), "the next pass must revert a DBA's alter_job(scheduled => true) regardless of the coverage verdict it reaches");
            _ = pass;

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                foreach (var relation in TimescaleSupport.RetentionPolicies.Select(p => p.Relation))
                {
                    await batch.RemoveRetentionPolicyAsync(relation, cleanupCt);
                }
            });
        }
    }

    [Fact]
    public async Task NonRawRelation_KeepsTodaysArmHoldSemantics_AndGainsNoArmedKey()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4299 raw-retention test.");

        var nonRaw = TimescaleSupport.RetentionPolicies
            .Select(p => p.Relation)
            .FirstOrDefault(relation => !TimescaleSupport.RawRelations.Contains(relation));
        Assert.NotNull(nonRaw);

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);
        await using var connection = await BootAsync(scratch);

        var bodySucceeded = false;
        try
        {
            var start = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, default);
            Assert.True(start.InPlace == TimescaleSupport.RetentionPolicies.Count, "the start pass must apply every policy");

            var scheduledBefore = await ScheduledAsync(connection, nonRaw!);
            Assert.NotNull(scheduledBefore);
            Assert.Null(await ArmedKeyAsync(connection, nonRaw!));

            var pass = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, TimescaleSupport.RetentionSweepPass.Periodic, default);

            /* The non-raw relation's scheduled flag still follows the ordinary arm/hold verdict (unchanged
               shape) and it never picks up the raw-only config key. */
            Assert.NotNull(await ScheduledAsync(connection, nonRaw!));
            Assert.Null(await ArmedKeyAsync(connection, nonRaw!));
            _ = pass;

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                foreach (var relation in TimescaleSupport.RetentionPolicies.Select(p => p.Relation))
                {
                    await batch.RemoveRetentionPolicyAsync(relation, cleanupCt);
                }
            });
        }
    }

    [Fact]
    public async Task CoveredRawJob_ReadsArmedTrue_ButStaysUnscheduled()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4299 raw-retention test.");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);
        await using var connection = await BootAsync(scratch);

        /* Seed BEFORE the start pass, so the FIRST verdict is genuinely Short (a non-empty source with an
           unmaterialized consumer), not the trivially-Covered empty-store case
           (MeasureRetentionCoverageAsync arms on sight when the source has no rows at all). Thirty days back
           is outside the hourly aggregate's own refresh window, so nothing here self-heals until this test
           moves the coverage itself. */
        var seeded = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-30), DateTimeKind.Unspecified);
        using (var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count)
VALUES (1, $1, 9138, 'probe-covered-raw', 'TestDb', decode(md5('1b2cov'), 'hex'), decode(md5('h'), 'hex'), 1, 1, 1)", connection) { CommandTimeout = PolicyReadTimeoutSeconds })
        {
            seed.Parameters.AddWithValue(seeded);
            await seed.ExecuteNonQueryAsync();
        }

        var bodySucceeded = false;
        try
        {
            var start = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, default);
            Assert.True(start.InPlace == TimescaleSupport.RetentionPolicies.Count, "the start pass must apply every policy");
            Assert.False(await ScheduledAsync(connection, Raw), "a raw relation is never scheduled by TimescaleDB's own runner");
            Assert.Equal("false", await ArmedKeyAsync(connection, Raw));

            /* Move the coverage the way a backfill does: materialize every consumer RawTierCoverage names
               for query_stats so the verdict flips from Short to Covered. */
            async Task RefreshAsync(string view, DateTime from, DateTime to)
            {
                using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection) { CommandTimeout = PolicyReadTimeoutSeconds };
                refresh.Parameters.AddWithValue(DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
                refresh.Parameters.AddWithValue(DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
                await refresh.ExecuteNonQueryAsync();
            }

            await RefreshAsync(TimescaleSupport.QueryStatsIntervalHourlyView, seeded.AddDays(-1), seeded.AddDays(1));
            await RefreshAsync(TimescaleSupport.QueryStatsDbIntervalHourlyView, seeded.AddDays(-1), seeded.AddDays(1));
            await RefreshAsync(TimescaleSupport.QueryStatsIntervalDailyView, seeded.AddDays(-2), seeded.AddDays(2));
            await RefreshAsync(TimescaleSupport.QueryStatsDbIntervalDailyView, seeded.AddDays(-2), seeded.AddDays(2));

            var pass = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, TimescaleSupport.RetentionSweepPass.Periodic, default);
            Assert.True(pass.Armed >= 1, "the covered verdict must arm at least query_stats this pass");

            Assert.Equal("true", await ArmedKeyAsync(connection, Raw));
            Assert.False(await ScheduledAsync(connection, Raw), "a Covered raw relation must still read scheduled = false — the OLD code armed it with scheduled = true");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                foreach (var relation in TimescaleSupport.RetentionPolicies.Select(p => p.Relation))
                {
                    await batch.RemoveRetentionPolicyAsync(relation, cleanupCt);
                }

                using var unseed = new NpgsqlCommand(
                    "DELETE FROM collect.query_stats WHERE server_name = 'probe-covered-raw'", cleanup) { CommandTimeout = PolicyReadTimeoutSeconds };
                await unseed.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
