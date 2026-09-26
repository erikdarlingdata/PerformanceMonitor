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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4391: pins for the two review fixes to <see cref="DarlingWorker"/>'s raw-purge gates — the repair epoch
/// stamps only after a materialization-hole repair pass with zero isolated failures
/// (<see cref="DarlingWorker.RepairEpochStampAllowed"/>), and <see cref="DarlingWorker.TriggerRawPurgeCoreAsync"/>
/// records <c>gate_error</c> (and logs at Warning) when the trigger's OWN gate check fails partway through,
/// rather than the run-of-the-mill "not covered this pass" outcomes.
///
/// <para><b>#1776 own-store</b>: the live pin goes through <see cref="ScratchPostgres.CreateAsync"/> and
/// never touches the shared database's tables.</para>
/// </summary>
public sealed class RawPurgeTriggerGateErrorTests
{
    private const string Raw = "query_stats";
    private const int SetupTimeoutSeconds = 60;

    /// <summary>Pure pin for <see cref="DarlingWorker.RepairEpochStampAllowed"/>: allowed with zero failures
    /// (even with holes deferred past the cap — those are re-measured fresh by the trigger's own hole scan,
    /// not this gate), refused with one. Compile-RED at <c>34064e99e</c> — <c>RepairEpochStampAllowed</c> did
    /// not exist to call.</summary>
    [Fact]
    public void RepairEpochStampAllowed_TrueOnlyWithZeroFailures()
    {
        var clean = new TimescaleSupport.MaterializationHoleRepairSummary(
            AggregatesScanned: 5, AggregatesSkipped: 0, HolesFound: 2, BucketsFound: 4, HolesRepaired: 2,
            BucketsRepaired: 4, HolesDeferred: 1, BucketsDeferred: 3, HolesRemaining: 0, Failures: 0,
            HolesForced: 0, Elapsed: TimeSpan.FromSeconds(1));
        Assert.True(
            DarlingWorker.RepairEpochStampAllowed(clean),
            "zero failures must allow the stamp even when a hole was deferred past the cap");

        var failed = clean with { Failures = 1 };
        Assert.False(
            DarlingWorker.RepairEpochStampAllowed(failed),
            "even one isolated per-aggregate failure must refuse the stamp");
    }

    private static async Task<(NpgsqlConnection Connection, ScratchPostgres Scratch)> OpenAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4391 gate_error pin.");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);

        /* Max Auto Prepare=0: RunRetentionPurgeJobSql is a multi-statement string run through NpgsqlCommand,
           and this rig's connection reruns other multi-statement SQL enough times over its life that
           Npgsql's default auto-prepare threshold can trigger on it — same reasoning as RawPurgeTriggerLiveTests. */
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { MaxAutoPrepare = 0 }.ConnectionString;
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await PgMigrations.MigrateAsync(connection, default);

        var enabled = await TimescaleSupport.TryEnableAsync(connection, null, default);
        Assert.SkipWhen(!enabled, "The live #4391 gate_error pin needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, default);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, default);

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            await stop.ExecuteNonQueryAsync();
        }

        return (connection, scratch);
    }

    private static async Task<long> ReadPostmasterEpochAsync(NpgsqlConnection connection)
    {
        await using var read = new NpgsqlCommand(TimescaleSupport.PostmasterStartEpochMicrosecondsSql, connection) { CommandTimeout = SetupTimeoutSeconds };
        return (long)(await read.ExecuteScalarAsync())!;
    }

    private static async Task ArmRawJobAsync(NpgsqlConnection connection, string relation)
    {
        await using var arm = new NpgsqlCommand(
            $"SELECT add_retention_policy('collect.{relation}', drop_after => interval '4 days') AS job_id", connection) { CommandTimeout = SetupTimeoutSeconds };
        await arm.ExecuteScalarAsync();
    }

    /// <summary>Ten days back through one day back, one row per hour, matching RawPurgeTriggerLiveTests'
    /// seed shape so the drop range and the successor refreshes behave the same way.</summary>
    private static async Task SeedRawAsync(NpgsqlConnection connection)
    {
        await using var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT
    -1,
    now() - (n || ' hours')::interval,
    1,
    'lane-4391-r2b',
    'ProbeDb',
    decode(md5('r2b-' || n), 'hex'),
    decode(md5('r2b-h-' || n), 'hex'),
    1, 1, 1, 30
FROM generate_series(24, 240) AS n", connection) { CommandTimeout = SetupTimeoutSeconds };
        await seed.ExecuteNonQueryAsync();
    }

    private static async Task RefreshSuccessorsAsync(NpgsqlConnection connection, DateTime from, DateTime to)
    {
        async Task RefreshAsync(string view)
        {
            await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection) { CommandTimeout = SetupTimeoutSeconds };
            refresh.Parameters.AddWithValue(DateTime.SpecifyKind(from.AddDays(-1), DateTimeKind.Unspecified));
            refresh.Parameters.AddWithValue(DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
            await refresh.ExecuteNonQueryAsync();
        }

        await RefreshAsync(TimescaleSupport.QueryStatsIntervalHourlyView);
        await RefreshAsync(TimescaleSupport.QueryStatsDbIntervalHourlyView);
        await RefreshAsync(TimescaleSupport.QueryStatsBaselineView);
    }

    private static async Task<long> ChunkCountAsync(NpgsqlConnection connection)
    {
        await using var read = new NpgsqlCommand(
            "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'query_stats'",
            connection) { CommandTimeout = SetupTimeoutSeconds };
        return (long)(await read.ExecuteScalarAsync())!;
    }

    private static async Task StampCurrentEpochAsync(NpgsqlConnection connection, string relation)
    {
        var epoch = await ReadPostmasterEpochAsync(connection);
        await using var stamp = new NpgsqlCommand(TimescaleSupport.RawRepairEpochStampSql(relation), connection) { CommandTimeout = SetupTimeoutSeconds };
        stamp.Parameters.AddWithValue(epoch);
        await stamp.ExecuteNonQueryAsync();
    }

    /// <summary>Covered, current epoch, no hole — everything the earlier gates need to pass — but the hole
    /// scan itself fails with a lock timeout (57014) on the hourly successor's materialization hypertable, held
    /// ACCESS EXCLUSIVE from a second connection while the trigger's own connection runs under a short
    /// <c>statement_timeout</c>. Asserts: no chunk dropped, and the recorded outcome is <c>gate_error</c> with
    /// SqlState <c>57014</c>. RED at <c>34064e99e</c> — the catch there records nothing at all, so the
    /// relation's last-purge record stays whatever it was before this call (here, never written, so still
    /// null).</summary>
    [Fact]
    public async Task GateCheckThrows_NoDropAndGateErrorRecorded()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        NpgsqlConnection? locker = null;
        try
        {
            await ArmRawJobAsync(connection, Raw);
            await SeedRawAsync(connection);

            var dropFrom = TimescaleSupport.AlignDown(DateTime.UtcNow.AddDays(-11), TimeSpan.FromHours(1));
            var dropTo = TimescaleSupport.AlignDown(DateTime.UtcNow, TimeSpan.FromHours(1));
            await RefreshSuccessorsAsync(connection, dropFrom, dropTo);

            var periodic = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, TimescaleSupport.RetentionSweepPass.Periodic, default);
            Assert.True(periodic.Armed >= 1, "the seeded and fully-refreshed raw relation must read Covered this pass");

            await StampCurrentEpochAsync(connection, Raw);

            /* query_stats_baseline, NOT the hourly successor: the coverage probe (IsRawTierDropSafeAsync)
               only reads query_stats's TWO coverage relations (RawTierCoverage) and swallows any read
               failure into "not_covered" rather than throwing, so locking a coverage relation would never
               reach the trigger's own catch. query_stats_baseline is a MaterializationHoleTarget over
               query_stats (SourceTableFor) but is NOT one of query_stats's coverage relations, so the
               coverage probe passes and the hole scan is the first thing that reaches this locked table. */
            var target = TimescaleSupport.MaterializationHoleTargets.First(
                t => string.Equals(t.View, TimescaleSupport.QueryStatsBaselineView, StringComparison.Ordinal));
            var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, target.View, default);
            Assert.NotNull(materialization);

            /* Second session: hold ACCESS EXCLUSIVE on query_stats_baseline's materialization hypertable in
               an open transaction, so the trigger's own hole scan — which reads that relation — cannot
               acquire the lock and instead times out. */
            locker = new NpgsqlConnection(scratch.ConnectionString);
            await locker.OpenAsync();
            await using (var beginLock = new NpgsqlCommand("BEGIN", locker) { CommandTimeout = SetupTimeoutSeconds })
            {
                await beginLock.ExecuteNonQueryAsync();
            }
            await using (var takeLock = new NpgsqlCommand(
                $"LOCK TABLE {materialization!.Value.Schema}.{materialization.Value.Name} IN ACCESS EXCLUSIVE MODE", locker) { CommandTimeout = SetupTimeoutSeconds })
            {
                await takeLock.ExecuteNonQueryAsync();
            }

            var before = await ChunkCountAsync(connection);
            Assert.True(before > 0, "the seed must have produced at least one raw chunk, or this pin proves nothing");

            await using (var setTimeout = new NpgsqlCommand("SET statement_timeout = '2s'", connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                await setTimeout.ExecuteNonQueryAsync();
            }

            var logger = new CapturingTestLogger();
            await DarlingWorker.TriggerRawPurgeCoreAsync(connection, logger, default);

            await using (var clearTimeout = new NpgsqlCommand("SET statement_timeout = 0", connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                await clearTimeout.ExecuteNonQueryAsync();
            }

            var after = await ChunkCountAsync(connection);
            Assert.Equal(before, after);

            Assert.True(logger.CountAtLevel(LogLevel.Warning) >= 1,
                $"the gate check's own failure must log at Warning; log: {logger.Joined}");

            var rec = await TimescaleSupport.ReadRawLastPurgeOutcomeAsync(connection, Raw, null, default);
            Assert.NotNull(rec);
            Assert.Equal("gate_error", rec!.Outcome);
            Assert.Equal("57014", rec.SqlState);

            bodySucceeded = true;
        }
        finally
        {
            if (locker is not null)
            {
                await using (var rollbackLock = new NpgsqlCommand("ROLLBACK", locker) { CommandTimeout = SetupTimeoutSeconds })
                {
                    await rollbackLock.ExecuteNonQueryAsync();
                }
                await locker.DisposeAsync();
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.RemoveRetentionPolicyAsync(Raw, cleanupCt);
            });
            await connection.DisposeAsync();
        }
    }
}
