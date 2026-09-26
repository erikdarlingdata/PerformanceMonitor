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
/// #4299 L3a: live pins for the two census redirects — <see cref="TimescaleSupport.RetentionHoldReadSql"/>
/// now reads a raw relation's armed verdict off <c>config-&gt;&gt;'darling_armed'</c> instead of the
/// permanently-false <c>j.scheduled</c>, and <see cref="TimescaleSupport.JobCadenceReadSql"/> excludes the
/// three raw retention jobs from the #2136 cadence reading (their <c>schedule_interval</c> is a scheduler
/// artifact once the job never runs on TimescaleDB's own clock).
///
/// <para><b>#1776 own-store</b>: deliberately NOT <c>[Collection("live-postgres")]</c>. Goes through
/// <see cref="ScratchPostgres.CreateAsync"/> and never touches the shared database's tables.</para>
/// </summary>
public sealed class RetentionHoldAndCadenceRawRedirectLiveTests
{
    private const string Raw = "query_stats";
    private const int SetupTimeoutSeconds = 60;

    private static async Task<(NpgsqlConnection Connection, ScratchPostgres Scratch)> OpenAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4299 L3a redirect pins.");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);

        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { MaxAutoPrepare = 0 }.ConnectionString;
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await PgMigrations.MigrateAsync(connection, default);

        var enabled = await TimescaleSupport.TryEnableAsync(connection, null, default);
        Assert.SkipWhen(!enabled, "The live #4299 L3a redirect pins need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, default);

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            await stop.ExecuteNonQueryAsync();
        }

        return (connection, scratch);
    }

    private static async Task ArmRawJobAsync(NpgsqlConnection connection, string relation)
    {
        await using var arm = new NpgsqlCommand(
            $"SELECT add_retention_policy('collect.{relation}', drop_after => interval '4 days') AS job_id", connection) { CommandTimeout = SetupTimeoutSeconds };
        await arm.ExecuteScalarAsync();
    }

    private static async Task SetDarlingArmedAsync(NpgsqlConnection connection, string relation, bool armed)
    {
        await using var command = new NpgsqlCommand(TimescaleSupport.ConvergeRawArmedStateSql(relation), connection) { CommandTimeout = SetupTimeoutSeconds };
        command.Parameters.AddWithValue(armed);
        await command.ExecuteScalarAsync();
    }

    /// <summary>
    /// <see cref="TimescaleSupport.RetentionHoldReadSql"/>: a raw relation's job carries
    /// <c>darling_armed = true</c> but its permanent <c>scheduled</c> is <c>false</c> (the never-scheduled
    /// design). RED before this lane's redirect — the row read <c>Armed = false</c> off <c>j.scheduled</c>
    /// regardless of the real verdict, which is exactly the false-hold the census (row #7) flagged.
    /// </summary>
    [Fact]
    public async Task RetentionHoldRead_RawRelation_DarlingArmedTrue_ReadsArmedTrue_EvenThoughScheduledIsFalse()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        try
        {
            await ArmRawJobAsync(connection, Raw);
            await SetDarlingArmedAsync(connection, Raw, armed: true);

            var readings = await TimescaleSupport.ReadRetentionHoldReadingsAsync(connection, null, default);
            var row = Assert.Single(readings, r => r.HypertableName == Raw);

            Assert.True(row.Armed, "darling_armed=true must read Armed=true for a raw relation, even though j.scheduled is permanently false");

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
    /// The regression case the census (row #7) named as the meaningful one: <c>darling_armed = false</c> on a
    /// raw relation must still read <c>Armed = false</c> (unchanged from before this lane — the true/true
    /// fixture above is the one the redirect actually moves). Held here so a future change to the CASE/branch
    /// cannot silently invert the false case while keeping the true case green.
    /// </summary>
    [Fact]
    public async Task RetentionHoldRead_RawRelation_DarlingArmedFalse_ReadsArmedFalse()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        try
        {
            await ArmRawJobAsync(connection, Raw);
            await SetDarlingArmedAsync(connection, Raw, armed: false);

            var readings = await TimescaleSupport.ReadRetentionHoldReadingsAsync(connection, null, default);
            var row = Assert.Single(readings, r => r.HypertableName == Raw);

            Assert.False(row.Armed, "darling_armed=false must read Armed=false for a raw relation");

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
    /// <see cref="TimescaleSupport.JobCadenceReadSql"/>: a raw job's <c>job_id</c> is ABSENT from the #2136
    /// cadence reading, even once it has a <c>Success</c> <c>job_stats</c> row from the service's own
    /// <c>CALL run_job(id)</c> (<see cref="TimescaleSupport.RunRetentionPurgeJobAsync"/>) — the trigger's own
    /// admin call updates <c>job_stats</c> through the same catalog path TimescaleDB's scheduler uses. RED
    /// before this lane's WHERE exclusion — the raw job's row was present, and its <c>schedule_interval</c>
    /// (a scheduler artifact for a job that never runs on the scheduler's own clock) fed the #2136 alert's
    /// cadence math as if it meant something.
    /// </summary>
    [Fact]
    public async Task JobCadenceRead_RawRelation_AbsentFromReadings_EvenAfterASuccessfulTriggeredRun()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        try
        {
            await ArmRawJobAsync(connection, Raw);

            long jobId;
            await using (var read = new NpgsqlCommand(
                "SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention' AND hypertable_schema = 'collect' AND hypertable_name = $1",
                connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                read.Parameters.AddWithValue(Raw);
                jobId = Convert.ToInt64((await read.ExecuteScalarAsync())!, System.Globalization.CultureInfo.InvariantCulture);
            }

            var outcome = await TimescaleSupport.RunRetentionPurgeJobAsync(connection, jobId, null, default);
            Assert.True(outcome.Ran, $"the triggered run must succeed for job_stats to carry a Success row; SqlState={outcome.SqlState}");

            var readings = await TimescaleSupport.ReadJobCadenceReadingsAsync(connection, null, default);
            Assert.DoesNotContain(readings, r => r.JobId == jobId);

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
