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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4299 L2: end-to-end live pins for <see cref="DarlingWorker.TriggerRawPurgeCoreAsync"/> — the seam that
/// composes the coverage verdict, the repair epoch and <see cref="TimescaleSupport.HoleFreeThroughAsync"/>
/// into the decision to actually drop chunks off a raw hypertable. Every pin seeds
/// <c>collect.query_stats</c> across the drop range, refreshes (or deliberately withholds refreshing) its
/// successors, and reads <c>timescaledb_information.chunks</c> before and after the trigger to prove the
/// chunk count moves (or does not) exactly as the gate requires.
///
/// <para><b>#1776 own-store</b>: deliberately NOT <c>[Collection("live-postgres")]</c>. Goes through
/// <see cref="ScratchPostgres.CreateAsync"/> and never touches the shared database's tables.</para>
/// </summary>
public sealed class RawPurgeTriggerLiveTests
{
    private const string Raw = "query_stats";
    private const int SetupTimeoutSeconds = 60;

    private static async Task<(NpgsqlConnection Connection, ScratchPostgres Scratch)> OpenAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4299 L2 trigger pins.");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);

        /* Max Auto Prepare=0: RunRetentionPurgeJobSql is a multi-statement string (BEGIN; ...; COMMIT;) run
           through NpgsqlCommand, and this rig's connection reruns other multi-statement SQL enough times over
           its life that Npgsql's default auto-prepare threshold can trigger on it, which Npgsql rejects with
           42601 ("cannot insert multiple commands into a prepared statement") — a client-side caching
           artifact of this test's connection reuse, not a product behavior. */
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { MaxAutoPrepare = 0 }.ConnectionString;
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await PgMigrations.MigrateAsync(connection, default);

        var enabled = await TimescaleSupport.TryEnableAsync(connection, null, default);
        Assert.SkipWhen(!enabled, "The live #4299 L2 trigger pins need TimescaleDB.");
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

    /// <summary>Ten days back through one day back, one row per hour, so raw holds chunks both inside and
    /// outside the 4-day drop range and the interval-honest filter (<c>sample_interval_seconds IS DISTINCT
    /// FROM 0</c>) admits every one of them.</summary>
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
    'lane-4299-2d',
    'ProbeDb',
    decode(md5('l2d-' || n), 'hex'),
    decode(md5('l2d-h-' || n), 'hex'),
    1, 1, 1, 30
FROM generate_series(24, 240) AS n", connection) { CommandTimeout = SetupTimeoutSeconds };
        await seed.ExecuteNonQueryAsync();
    }

    /// <summary>Refreshes both successor hourlies over <c>[from, to)</c>, widened one day EARLIER than
    /// <paramref name="from"/> — the trigger's own <c>dropFrom</c> is the OLDEST RAW CHUNK's
    /// <c>range_start</c>, which is day-aligned (<see cref="TimescaleSupport.ChunkIntervalDays"/>) and can
    /// fall hours before an hour-aligned <paramref name="from"/>, so a refresh window that starts exactly at
    /// <paramref name="from"/> leaves that gap unmaterialized — a real hole by the gate's own definition, but
    /// one this pin does not intend to create.</summary>
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

        /* query_stats_baseline is also a MaterializationHoleTarget over collect.query_stats (SourceTableFor),
           so the hole gate checks it too. */
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

    /// <summary>Covered, current epoch, no hole: the trigger drops query_stats's chunks older than the
    /// 4-day drop_after. RED on <c>d70358a5b</c> — <c>TriggerRawPurgeCoreAsync</c> did not exist to call.</summary>
    [Fact]
    public async Task Covered_CurrentEpoch_NoHole_ChunkCountDrops()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        try
        {
            await ArmRawJobAsync(connection, Raw);
            await SeedRawAsync(connection);

            /* Bucket-aligned (hour boundary), matching how the product's own dropFrom (the oldest chunk's
               range_start) and refresh_continuous_aggregate's own materialized buckets align — an unaligned
               window here would make the hole scan's generated series miss real materialized buckets and
               read a false hole that has nothing to do with the gate under test. */
            var dropFrom = TimescaleSupport.AlignDown(DateTime.UtcNow.AddDays(-11), TimeSpan.FromHours(1));
            var dropTo = TimescaleSupport.AlignDown(DateTime.UtcNow, TimeSpan.FromHours(1));
            await RefreshSuccessorsAsync(connection, dropFrom, dropTo);

            var periodic = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, TimescaleSupport.RetentionSweepPass.Periodic, default);
            Assert.True(periodic.Armed >= 1, "the seeded and fully-refreshed raw relation must read Covered this pass");

            await StampCurrentEpochAsync(connection, Raw);

            var before = await ChunkCountAsync(connection);
            Assert.True(before > 0, "the seed must have produced at least one raw chunk, or this pin proves nothing");

            var logger = new CapturingTestLogger();
            await DarlingWorker.TriggerRawPurgeCoreAsync(connection, logger, default);

            var after = await ChunkCountAsync(connection);
            Assert.True(after < before, $"a Covered, current-epoch, hole-free purge must drop chunks (before={before}, after={after}); log: {logger.Joined}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.RemoveRetentionPolicyAsync(Raw, cleanupCt);
            });
            await connection.DisposeAsync();
        }
    }

    /// <summary>Short (successors never refreshed, so the sweep holds rather than arms): the trigger's
    /// armed-read gate fails before it ever reaches the epoch or hole checks, and the chunk count is
    /// unchanged. RED on <c>d70358a5b</c> — no trigger to call.</summary>
    [Fact]
    public async Task Short_ChunkCountUnchanged()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        try
        {
            await ArmRawJobAsync(connection, Raw);
            await SeedRawAsync(connection);

            /* Deliberately skip the successor refresh, so the coverage sweep measures Short. */
            var periodic = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, TimescaleSupport.RetentionSweepPass.Periodic, default);
            _ = periodic;

            await using (var armedRead = new NpgsqlCommand(TimescaleSupport.RawArmedStateSql(Raw), connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                var value = await armedRead.ExecuteScalarAsync();
                Assert.False(value is bool b && b, "an unrefreshed raw relation must read Short/Unknown, not Covered, or this pin proves nothing");
            }

            await StampCurrentEpochAsync(connection, Raw);

            var before = await ChunkCountAsync(connection);
            Assert.True(before > 0, "the seed must have produced at least one raw chunk, or this pin proves nothing");

            await DarlingWorker.TriggerRawPurgeCoreAsync(connection, NullLogger.Instance, default);

            var after = await ChunkCountAsync(connection);
            Assert.Equal(before, after);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.RemoveRetentionPolicyAsync(Raw, cleanupCt);
            });
            await connection.DisposeAsync();
        }
    }

    /// <summary>Covered and hole-free, but the repair epoch is stale (stamped one microsecond behind the
    /// current postmaster start): the trigger's epoch gate fails, and the chunk count is unchanged. RED on
    /// <c>d70358a5b</c> — no trigger to call.</summary>
    [Fact]
    public async Task StaleEpoch_ChunkCountUnchanged()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        try
        {
            await ArmRawJobAsync(connection, Raw);
            await SeedRawAsync(connection);

            var dropFrom = TimescaleSupport.AlignDown(DateTime.UtcNow.AddDays(-11), TimeSpan.FromHours(1));
            var dropTo = TimescaleSupport.AlignDown(DateTime.UtcNow, TimeSpan.FromHours(1));
            await RefreshSuccessorsAsync(connection, dropFrom, dropTo);

            var periodic = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, TimescaleSupport.RetentionSweepPass.Periodic, default);
            Assert.True(periodic.Armed >= 1, "the seeded and fully-refreshed raw relation must read Covered this pass");

            var currentEpoch = await ReadPostmasterEpochAsync(connection);
            await using (var stamp = new NpgsqlCommand(TimescaleSupport.RawRepairEpochStampSql(Raw), connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                stamp.Parameters.AddWithValue(currentEpoch - 1);
                await stamp.ExecuteNonQueryAsync();
            }

            await using (var match = new NpgsqlCommand(TimescaleSupport.RawRepairEpochMatchesSql(Raw), connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                var result = await match.ExecuteScalarAsync();
                Assert.False(result is bool b && b, "a one-microsecond-stale epoch must read as no-match, or this pin proves nothing");
            }

            var before = await ChunkCountAsync(connection);
            Assert.True(before > 0, "the seed must have produced at least one raw chunk, or this pin proves nothing");

            await DarlingWorker.TriggerRawPurgeCoreAsync(connection, NullLogger.Instance, default);

            var after = await ChunkCountAsync(connection);
            Assert.Equal(before, after);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.RemoveRetentionPolicyAsync(Raw, cleanupCt);
            });
            await connection.DisposeAsync();
        }
    }

    /// <summary>Covered and current epoch, but a hole in the drop range: one bucket's rows are deleted from
    /// the hourly successor's materialization hypertable, inside the window the purge is about to drop. The
    /// hole gate fails, and the chunk count is unchanged. RED on <c>d70358a5b</c> — no trigger to call.</summary>
    [Fact]
    public async Task HoleInDropRange_ChunkCountUnchanged()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
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

            /* Punch a hole: delete one hour's bucket, well inside the 4-day drop range, from the hourly
               successor's materialization hypertable directly — the same relation the product resolves via
               ResolveMaterializationAsync, so the deletion is invisible to the view but visible to the gate. */
            var target = TimescaleSupport.MaterializationHoleTargets.First(
                t => string.Equals(t.View, TimescaleSupport.QueryStatsIntervalHourlyView, StringComparison.Ordinal));
            var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, target.View, default);
            Assert.NotNull(materialization);

            var holeBucket = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-6), DateTimeKind.Unspecified);
            await using (var punch = new NpgsqlCommand(
                $"DELETE FROM {materialization!.Value.Schema}.{materialization.Value.Name} WHERE bucket = $1", connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                punch.Parameters.AddWithValue(holeBucket);
                var deleted = await punch.ExecuteNonQueryAsync();
                Assert.True(deleted > 0, "the punched bucket must have held a row, or this pin proves nothing");
            }

            var before = await ChunkCountAsync(connection);
            Assert.True(before > 0, "the seed must have produced at least one raw chunk, or this pin proves nothing");

            await DarlingWorker.TriggerRawPurgeCoreAsync(connection, NullLogger.Instance, default);

            var after = await ChunkCountAsync(connection);
            Assert.Equal(before, after);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.RemoveRetentionPolicyAsync(Raw, cleanupCt);
            });
            await connection.DisposeAsync();
        }
    }

    /// <summary>The Startup sweep pass itself never purges, regardless of coverage or epoch — the trigger is
    /// Periodic-only by construction, so only <see cref="DarlingWorker.TriggerRawPurgeCoreAsync"/> can drop
    /// chunks, and this pin proves calling the Startup sweep alone (without the trigger) leaves the chunk
    /// count unchanged even when every other gate would pass. RED on <c>d70358a5b</c> in the trivial sense
    /// that the sweep-alone behavior it pins was already true; the pin exists so this file's suite states the
    /// construction explicitly, not implicitly by omission.</summary>
    [Fact]
    public async Task StartupPassAlone_NeverPurges_ChunkCountUnchanged()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
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

            var before = await ChunkCountAsync(connection);
            Assert.True(before > 0, "the seed must have produced at least one raw chunk, or this pin proves nothing");

            /* Run the Startup pass — NOT the trigger — even though every gate the trigger checks (Covered,
               current epoch, no hole) would pass right now. */
            await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, TimescaleSupport.RetentionSweepPass.Startup, default);

            var after = await ChunkCountAsync(connection);
            Assert.Equal(before, after);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.RemoveRetentionPolicyAsync(Raw, cleanupCt);
            });
            await connection.DisposeAsync();
        }
    }

    /// <summary>#4299 L2 (two-service pin): two INDEPENDENT <see cref="NpgsqlDataSource"/>s against the SAME
    /// scratch store, standing in for two service processes. Service A stamps the current postmaster epoch;
    /// Service B's own relaunch-decision read (<see cref="DarlingWorker.ShouldLaunchMaterializationHoleRepair"/>,
    /// fed by a fresh <see cref="TimescaleSupport.RawRepairEpochMatchesSql"/> read on B's OWN connection) must
    /// see the stamp A wrote and answer "don't launch" — the store-side guard
    /// (<see cref="TimescaleSupport.RawRepairEpochStampSql"/>'s <c>IS DISTINCT FROM</c>) is what stops a SECOND
    /// SERVICE from repeating A's repair, independent of either process's own in-memory flag. B then re-stamping
    /// the identical value touches 0 rows (the same guard, proven from the write side). Finally A and B both call
    /// <see cref="DarlingWorker.TriggerRawPurgeCoreAsync"/> in turn on state (a): the chunk count drops once, on
    /// A's call, and B's later call — same relation, chunk already gone — drops nothing further and throws
    /// nothing, proving the purge itself is naturally idempotent once the chunks are already dropped. RED on
    /// <c>d70358a5b</c>: neither <c>TriggerRawPurgeCoreAsync</c> nor <c>ShouldLaunchMaterializationHoleRepair</c>
    /// existed to call.</summary>
    [Fact]
    public async Task TwoServices_RelaunchGuardHolds_PurgeRunsOnceOnly()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4299 L2 two-service pin.");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);
        var bodySucceeded = false;

        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { MaxAutoPrepare = 0 }.ConnectionString;

        await using var serviceA = NpgsqlDataSource.Create(connectionString);
        await using var serviceB = NpgsqlDataSource.Create(connectionString);

        try
        {
            await using var connectionA = await serviceA.OpenConnectionAsync();
            await PgMigrations.MigrateAsync(connectionA, default);

            var enabled = await TimescaleSupport.TryEnableAsync(connectionA, null, default);
            Assert.SkipWhen(!enabled, "The live #4299 L2 two-service pin needs TimescaleDB.");
            await TimescaleSupport.ConvertToHypertablesAsync(connectionA, null, default);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connectionA, null, default);

            await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connectionA) { CommandTimeout = SetupTimeoutSeconds })
            {
                await stop.ExecuteNonQueryAsync();
            }

            await using var connectionB = await serviceB.OpenConnectionAsync();

            await ArmRawJobAsync(connectionA, Raw);
            await SeedRawAsync(connectionA);

            var dropFrom = TimescaleSupport.AlignDown(DateTime.UtcNow.AddDays(-11), TimeSpan.FromHours(1));
            var dropTo = TimescaleSupport.AlignDown(DateTime.UtcNow, TimeSpan.FromHours(1));
            await RefreshSuccessorsAsync(connectionA, dropFrom, dropTo);

            var periodic = await TimescaleSupport.EnsureRetentionPoliciesAsync(connectionA, null, TimescaleSupport.RetentionSweepPass.Periodic, default);
            Assert.True(periodic.Armed >= 1, "the seeded and fully-refreshed raw relation must read Covered this pass");

            /* Service A stamps the epoch. */
            await StampCurrentEpochAsync(connectionA, Raw);

            /* Service B's relaunch decision, on B's OWN connection: the epoch B reads must match (A's stamp
               is visible across the two independent connections/data sources against the same store), so the
               pure decision method must answer "don't launch" regardless of B's own in-memory flag state. */
            bool epochCurrentForB;
            await using (var epochCheck = new NpgsqlCommand(TimescaleSupport.RawRepairEpochMatchesSql(Raw), connectionB) { CommandTimeout = SetupTimeoutSeconds })
            {
                var value = await epochCheck.ExecuteScalarAsync();
                epochCurrentForB = value is bool b && b;
            }
            Assert.True(epochCurrentForB, "Service B must see Service A's epoch stamp as current, or this pin proves nothing");

            Assert.False(
                DarlingWorker.ShouldLaunchMaterializationHoleRepair(repairRunningInThisProcess: false, epochCurrentInStore: epochCurrentForB),
                "Service B must NOT decide to launch a repair when the store's epoch stamp already matches the current postmaster start");

            /* Service B re-stamping the SAME value touches 0 rows (the store-side IS DISTINCT FROM guard). */
            /* RawRepairEpochStampSql is a SELECT (its IS DISTINCT FROM guard lives in the WHERE clause), so
               ExecuteNonQueryAsync's RecordsAffected is always -1 for it — the row count that matters here is
               how many rows the SELECT itself returned, read the same way StampCurrentEpochAsync's own SELECT
               is consumed. Zero rows returned means the guard's IS DISTINCT FROM excluded every job row, which
               is exactly the "already stamped this value" no-op the guard exists for. */
            var epoch = await ReadPostmasterEpochAsync(connectionB);
            var restampRowsReturned = 0;
            await using (var restamp = new NpgsqlCommand(TimescaleSupport.RawRepairEpochStampSql(Raw), connectionB) { CommandTimeout = SetupTimeoutSeconds })
            {
                restamp.Parameters.AddWithValue(epoch);
                await using var reader = await restamp.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    restampRowsReturned++;
                }
            }
            Assert.Equal(0, restampRowsReturned);

            /* Then the purge: A and B both call TriggerRawPurgeCoreAsync in turn on state (a). */
            var before = await ChunkCountAsync(connectionA);
            Assert.True(before > 0, "the seed must have produced at least one raw chunk, or this pin proves nothing");

            var loggerA = new CapturingTestLogger();
            await DarlingWorker.TriggerRawPurgeCoreAsync(connectionA, loggerA, default);

            var afterA = await ChunkCountAsync(connectionA);
            Assert.True(afterA < before, $"Service A's purge call must drop chunks (before={before}, after={afterA}); log: {loggerA.Joined}");

            var loggerB = new CapturingTestLogger();
            await DarlingWorker.TriggerRawPurgeCoreAsync(connectionB, loggerB, default);

            var afterB = await ChunkCountAsync(connectionA);
            Assert.Equal(afterA, afterB);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.RemoveRetentionPolicyAsync(Raw, cleanupCt);
            });
        }
    }
}
