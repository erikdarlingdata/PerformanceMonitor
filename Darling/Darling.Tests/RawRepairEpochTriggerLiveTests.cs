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
/// #4299: live pins for the repair epoch stamp and the trigger gate primitives DarlingWorker's Periodic
/// pass composes — <see cref="TimescaleSupport.RawRepairEpochStampSql"/>,
/// <see cref="TimescaleSupport.RawRepairEpochMatchesSql"/> and <see cref="TimescaleSupport.HoleFreeThroughAsync"/>
/// against a real TimescaleDB. DarlingWorker's own trigger method is private and not exercised here directly;
/// these pins prove the primitives it calls behave as required, on a live server.
///
/// <para><b>#1776 own-store</b>: deliberately NOT <c>[Collection("live-postgres")]</c>. Goes through
/// <see cref="ScratchPostgres.CreateAsync"/> and never touches the shared database's tables.</para>
/// </summary>
public sealed class RawRepairEpochTriggerLiveTests
{
    private const string Raw = "query_stats";
    private const int SetupTimeoutSeconds = 60;

    private static async Task<(NpgsqlConnection Connection, ScratchPostgres Scratch)> OpenAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4299 trigger pins.");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync();
        await PgMigrations.MigrateAsync(connection, default);

        var enabled = await TimescaleSupport.TryEnableAsync(connection, null, default);
        Assert.SkipWhen(!enabled, "The live #4299 trigger pins need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, default);

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

    /// <summary>
    /// The epoch round-trips to the microsecond: stamping the CURRENT postmaster start and then reading
    /// <see cref="TimescaleSupport.RawRepairEpochMatchesSql"/> immediately answers true. RED before
    /// <c>d70358a5b</c> — neither statement existed to assert this of.
    /// </summary>
    [Fact]
    public async Task RepairEpoch_StampedWithCurrentPostmasterStart_MatchesImmediately()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        try
        {
            await ArmRawJobAsync(connection, Raw);
            var epoch = await ReadPostmasterEpochAsync(connection);

            await using (var stamp = new NpgsqlCommand(TimescaleSupport.RawRepairEpochStampSql(Raw), connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                stamp.Parameters.AddWithValue(epoch);
                await stamp.ExecuteNonQueryAsync();
            }

            await using (var match = new NpgsqlCommand(TimescaleSupport.RawRepairEpochMatchesSql(Raw), connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                var result = await match.ExecuteScalarAsync();
                Assert.Equal(true, result);
            }

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

    /// <summary>
    /// A second stamp with the SAME value changes nothing — <c>IS DISTINCT FROM</c> filters the row out of
    /// the <c>SELECT alter_job(...)</c> on the repeat, so it returns zero rows rather than calling
    /// <c>alter_job</c> a second time. RED before this statement existed.
    /// </summary>
    [Fact]
    public async Task RepairEpoch_SecondStampWithSameValue_ReturnsZeroRows()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        try
        {
            await ArmRawJobAsync(connection, Raw);
            var epoch = await ReadPostmasterEpochAsync(connection);

            await using (var stamp = new NpgsqlCommand(TimescaleSupport.RawRepairEpochStampSql(Raw), connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                stamp.Parameters.AddWithValue(epoch);
                await stamp.ExecuteScalarAsync();
            }

            await using (var stampAgain = new NpgsqlCommand(TimescaleSupport.RawRepairEpochStampSql(Raw), connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                stampAgain.Parameters.AddWithValue(epoch);
                using var reader = await stampAgain.ExecuteReaderAsync();
                var rows = 0;
                while (await reader.ReadAsync())
                {
                    rows++;
                }
                Assert.Equal(0, rows);
            }

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

    /// <summary>
    /// A stale epoch (no stamp at all, standing in for a stamp from an earlier postmaster start) reads as
    /// no-match — fail-closed, never defaulting to true. RED before this statement existed.
    /// </summary>
    [Fact]
    public async Task RepairEpoch_NoStamp_ReadsAsNoMatch()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        try
        {
            await ArmRawJobAsync(connection, Raw);

            await using (var match = new NpgsqlCommand(TimescaleSupport.RawRepairEpochMatchesSql(Raw), connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                var result = await match.ExecuteScalarAsync();
                Assert.False(result is bool b && b, "an unstamped job must read as no-match, not true");
            }

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

    /// <summary>
    /// The trigger's hole check: a bucket the materialization holds nothing for, inside the drop range,
    /// reads as NOT hole-free — <see cref="TimescaleSupport.HoleFreeThroughAsync"/> must call it live, not
    /// assume clean. RED before <c>HoleFreeThroughAsync</c> existed (#4299, already on this branch) — this
    /// pin proves the trigger composition calls it for real over a genuinely empty materialization.
    /// </summary>
    [Fact]
    public async Task HoleFreeThroughAsync_EmptyMaterializationInDropRange_ReadsAsNotHoleFree()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        try
        {
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, default);

            var target = TimescaleSupport.MaterializationHoleTargets.First(t => string.Equals(t.Source, Raw, StringComparison.Ordinal));

            /* Seed one raw row old enough to fall inside the drop range, so the source has something the
               materialization ought to hold but (being freshly created and unrefreshed) does not. */
            await using (var seed = new NpgsqlCommand($@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (-1, now() - interval '10 days', 1, 'probe-server', 'ProbeDb', '0xL2HASH', '0xL2HANDLE', 0, 0, 0, 30)", connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                await seed.ExecuteNonQueryAsync();
            }

            var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, target.View, default);
            Assert.NotNull(materialization);

            var dropFrom = DateTime.UtcNow.AddDays(-11);
            var dropTo = DateTime.UtcNow.AddDays(-8);
            var holeFree = await TimescaleSupport.HoleFreeThroughAsync(
                connection, target, materialization!.Value, dropFrom, dropTo, Array.Empty<(DateTime, DateTime)>(), default);

            Assert.False(holeFree, "a raw row with no materialized bucket in the drop range must read as a hole");

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
}
