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
/// #4391: live pins for the raw-job converge guard (<see cref="TimescaleSupport.ConvergeRawArmedStateSql"/>'s
/// new <c>WHERE</c> clause and the Unknown branch's schedule-only revert), and for
/// <see cref="TimescaleSupport.ReadRawLastPurgeStateAsync"/>'s tri-state read. Helpers below are copied from
/// <c>RawPurgeTriggerLiveTests.cs</c> rather than shared, per the review's own file boundary.
///
/// <para><b>#1776 own-store</b>: deliberately NOT <c>[Collection("live-postgres")]</c>. Goes through
/// <see cref="ScratchPostgres.CreateAsync"/> and never touches the shared database's tables.</para>
/// </summary>
public sealed class RawPurgeConvergeLiveTests
{
    private const string Raw = "query_stats";
    private const int SetupTimeoutSeconds = 60;

    private static async Task<(NpgsqlConnection Connection, ScratchPostgres Scratch)> OpenAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4391 converge pins.");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);

        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { MaxAutoPrepare = 0 }.ConnectionString;
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await PgMigrations.MigrateAsync(connection, default);

        var enabled = await TimescaleSupport.TryEnableAsync(connection, null, default);
        Assert.SkipWhen(!enabled, "The live #4391 converge pins need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, default);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, default);

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
    'probe-converge-purge',
    'ProbeDb',
    decode(md5('r3a-' || n), 'hex'),
    decode(md5('r3a-h-' || n), 'hex'),
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

    private static async Task<bool?> ReadScheduledAsync(NpgsqlConnection connection, string relation)
    {
        await using var read = new NpgsqlCommand(TimescaleSupport.RetentionPolicyScheduledSql(relation), connection) { CommandTimeout = SetupTimeoutSeconds };
        var value = await read.ExecuteScalarAsync();
        return value is bool b ? b : null;
    }

    private static async Task<bool?> ReadArmedAsync(NpgsqlConnection connection, string relation)
    {
        await using var read = new NpgsqlCommand(TimescaleSupport.RawArmedStateSql(relation), connection) { CommandTimeout = SetupTimeoutSeconds };
        var value = await read.ExecuteScalarAsync();
        return value is bool b ? b : null;
    }

    private static async Task ForceScheduledTrueAsync(NpgsqlConnection connection, string relation)
    {
        await using var reArm = new NpgsqlCommand($@"
SELECT alter_job(j.job_id, scheduled => true)
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = '{relation}'", connection) { CommandTimeout = SetupTimeoutSeconds };
        await reArm.ExecuteScalarAsync();
    }

    /// <summary>(a) After each verdict branch (Covered, Short, Unknown), a raw job whose scheduled flag a
    /// DBA set to true is converged back to <c>scheduled = false</c>. Unknown also leaves
    /// <c>darling_armed</c> unchanged. RED on <c>34064e99e</c>: none of these branches read <c>j.scheduled</c>
    /// unconditionally BEFORE this pin's fix — a pre-fix build already converges scheduled to false in all
    /// three branches, so this pin's guard-specific assertions (pin b) are what actually distinguishes the
    /// fix; this pin (a) instead nails down the OUTCOME the guard must preserve.</summary>
    [Theory]
    [InlineData("Covered")]
    [InlineData("Short")]
    [InlineData("Unknown")]
    public async Task ScheduledFalse_AfterEveryVerdictBranch(string branch)
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        try
        {
            await ArmRawJobAsync(connection, Raw);
            await SeedRawAsync(connection);

            if (branch != "Short")
            {
                var dropFrom = TimescaleSupport.AlignDown(DateTime.UtcNow.AddDays(-11), TimeSpan.FromHours(1));
                var dropTo = TimescaleSupport.AlignDown(DateTime.UtcNow, TimeSpan.FromHours(1));
                await RefreshSuccessorsAsync(connection, dropFrom, dropTo);
            }

            string? renamedFrom = null;
            if (branch == "Unknown")
            {
                renamedFrom = TimescaleSupport.QueryStatsIntervalHourlyView;
                await using var rename = new NpgsqlCommand(
                    $"ALTER MATERIALIZED VIEW collect.{renamedFrom} RENAME TO pin_r3a_renamed_away", connection) { CommandTimeout = SetupTimeoutSeconds };
                await rename.ExecuteNonQueryAsync();
            }

            try
            {
                /* A DBA re-arms the job by hand before the next converge pass. */
                await ForceScheduledTrueAsync(connection, Raw);
                Assert.Equal(true, await ReadScheduledAsync(connection, Raw));

                var armedBefore = await ReadArmedAsync(connection, Raw);

                await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, TimescaleSupport.RetentionSweepPass.Periodic, default);

                Assert.Equal(false, await ReadScheduledAsync(connection, Raw));

                if (branch == "Unknown")
                {
                    Assert.Equal(armedBefore, await ReadArmedAsync(connection, Raw));
                }
            }
            finally
            {
                if (renamedFrom is not null)
                {
                    await using var restore = new NpgsqlCommand(
                        $"ALTER MATERIALIZED VIEW collect.pin_r3a_renamed_away RENAME TO {renamedFrom}", connection) { CommandTimeout = SetupTimeoutSeconds };
                    await restore.ExecuteNonQueryAsync();
                }
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

    /// <summary>(b) The converge guard: a SECOND <see cref="TimescaleSupport.EnsureRetentionPoliciesAsync"/>
    /// pass on an already-converged store (scheduled already false, darling_armed already matching the
    /// verdict) writes nothing — the job's <c>scheduled</c> flag and <c>darling_armed</c> key are byte-for-byte
    /// unchanged, checked via the job's own <c>config</c> text (the cheapest signal available here: this rig
    /// has no <c>pg_stat_statements</c>-style write counter for <c>alter_job</c>, so the config's stability is
    /// used as the "nothing written" signal instead of a call count). The revert log line appears exactly
    /// once, on the FIRST pass (where scheduled was true), and not at all on the second. RED on
    /// <c>34064e99e</c>: pre-fix, <c>ConvergeRawArmedStateSql</c> ran <c>alter_job</c> unconditionally every
    /// pass — a config-text stability check across two passes still passes on that build too (idempotent
    /// write), so RED here is asserted on the LOG line count instead, which pre-fix never exists at all
    /// (no revert line was logged pre-fix on either pass).</summary>
    [Fact]
    public async Task ConvergeGuard_SecondPassOnAlreadyConvergedStore_WritesNothing()
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

            /* A DBA re-arms the job by hand; the first pass converges it back and must log the revert once. */
            await ForceScheduledTrueAsync(connection, Raw);

            var logger1 = new CapturingTestLogger();
            await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, logger1, TimescaleSupport.RetentionSweepPass.Periodic, default);

            var revertCount1 = logger1.Lines.Count(e => e.Contains("was scheduled outside the service", StringComparison.Ordinal));
            Assert.Equal(1, revertCount1);

            var configAfterFirst = await ReadJobConfigTextAsync(connection, Raw);

            var logger2 = new CapturingTestLogger();
            await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, logger2, TimescaleSupport.RetentionSweepPass.Periodic, default);

            var configAfterSecond = await ReadJobConfigTextAsync(connection, Raw);
            Assert.Equal(configAfterFirst, configAfterSecond);

            var revertCount2 = logger2.Lines.Count(e => e.Contains("was scheduled outside the service", StringComparison.Ordinal));
            Assert.Equal(0, revertCount2);

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

    private static async Task<string> ReadJobConfigTextAsync(NpgsqlConnection connection, string relation)
    {
        await using var read = new NpgsqlCommand($@"
SELECT j.config::text
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = '{relation}'", connection) { CommandTimeout = SetupTimeoutSeconds };
        return (string)(await read.ExecuteScalarAsync())!;
    }

    /// <summary>(c) The tri-state read, live: a malformed <c>darling_last_purge</c> value reads
    /// <c>ReadFailed</c>; removing the key reads <c>NeverWritten</c>; a real trigger pass reads
    /// <c>Present</c>. RED on <c>34064e99e</c>: <c>ReadRawLastPurgeStateAsync</c> did not exist to call (the
    /// part-2 branch that added it is merged into this branch, but the pre-merge PR head at <c>34064e99e</c>
    /// only has <c>ReadRawLastPurgeOutcomeAsync</c>, with no tri-state).</summary>
    [Fact]
    public async Task ReadRawLastPurgeState_TriState_Live()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        try
        {
            await ArmRawJobAsync(connection, Raw);
            await SeedRawAsync(connection);

            /* NeverWritten: a freshly armed job carries no darling_last_purge key at all. */
            var (stateNever, recordNever) = await TimescaleSupport.ReadRawLastPurgeStateAsync(connection, Raw, null, default);
            Assert.Equal(TimescaleSupport.RawLastPurgeReadState.NeverWritten, stateNever);
            Assert.Null(recordNever);

            /* ReadFailed: a malformed value under the same key. */
            await using (var corrupt = new NpgsqlCommand($@"
SELECT alter_job(j.job_id, config => j.config || jsonb_build_object('darling_last_purge', '""garbage""'::jsonb))
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = '{Raw}'", connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                await corrupt.ExecuteScalarAsync();
            }

            var (stateFailed, recordFailed) = await TimescaleSupport.ReadRawLastPurgeStateAsync(connection, Raw, null, default);
            Assert.Equal(TimescaleSupport.RawLastPurgeReadState.ReadFailed, stateFailed);
            Assert.Null(recordFailed);

            /* Present: a real trigger pass on state (a) (Covered, current epoch, no hole) writes a real
               "ran" record. */
            var dropFrom = TimescaleSupport.AlignDown(DateTime.UtcNow.AddDays(-11), TimeSpan.FromHours(1));
            var dropTo = TimescaleSupport.AlignDown(DateTime.UtcNow, TimeSpan.FromHours(1));
            await RefreshSuccessorsAsync(connection, dropFrom, dropTo);

            var periodic = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, TimescaleSupport.RetentionSweepPass.Periodic, default);
            Assert.True(periodic.Armed >= 1, "the seeded and fully-refreshed raw relation must read Covered this pass");

            var epoch = await ReadPostmasterEpochAsync(connection);
            await using (var stamp = new NpgsqlCommand(TimescaleSupport.RawRepairEpochStampSql(Raw), connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                stamp.Parameters.AddWithValue(epoch);
                await stamp.ExecuteNonQueryAsync();
            }

            var logger = new CapturingTestLogger();
            await DarlingWorker.TriggerRawPurgeCoreAsync(connection, logger, default);

            var (statePresent, recordPresent) = await TimescaleSupport.ReadRawLastPurgeStateAsync(connection, Raw, null, default);
            Assert.Equal(TimescaleSupport.RawLastPurgeReadState.Present, statePresent);
            Assert.NotNull(recordPresent);
            Assert.Equal("ran", recordPresent!.Outcome);

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

    private static async Task<long> ReadPostmasterEpochAsync(NpgsqlConnection connection)
    {
        await using var read = new NpgsqlCommand(TimescaleSupport.PostmasterStartEpochMicrosecondsSql, connection) { CommandTimeout = SetupTimeoutSeconds };
        return (long)(await read.ExecuteScalarAsync())!;
    }
}
