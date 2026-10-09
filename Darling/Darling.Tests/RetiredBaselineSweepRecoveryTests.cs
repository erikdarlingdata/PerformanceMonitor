/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5416: the retirement sweep on a connection that a failed drop has just broken. A refresh job that is
/// running when the sweep's <c>DROP MATERIALIZED VIEW ... CASCADE</c> lands makes the drop fail with
/// <c>XX000: tuple concurrently deleted</c> (severity ERROR, not FATAL). Npgsql 10 closes the connection on an
/// ERROR in SQLSTATE classes XX, 58 and 53 (measured: <c>State</c> goes to <c>Closed</c>, <c>FullState</c> to
/// <c>Broken</c>), and the sweep's one connection is shared by every relation it walks and by every convergence step
/// that follows it. Before this, one lost race left the next relation's attempts, and the baseline fallback views
/// and statement statistics steps behind it, failing on "Connection is not open" until the next hourly pass.
/// The sweep now reopens its own connection after a failed attempt.
/// </summary>
[Collection("live-postgres")]
public sealed class RetiredBaselineSweepRecoveryLiveTests
{
    /// <summary>
    /// Deterministic: an event trigger raises <c>XX000</c> from inside the cpu aggregate's DROP, which is the
    /// error (and the broken connection) the refresh-job race produces, on demand. The file_io view behind it in
    /// the sweep must still drop in the SAME pass, the failure must be logged once, and the connection must be
    /// Open when the sweep returns.
    /// </summary>
    [Fact]
    public async Task Sweep_ReopensItsConnection_AfterADropBreaksIt_AndDropsTheNextRelation_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live sweep recovery test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            /* The sweep runs on the connection shape the product's service uses: the product search path, so an
               unqualified TimescaleDB call in the sweep (alter_job) resolves on a fresh store too, where the extension
               lives in collect and a pooled backend started before the migration has no path to it. */
            using var connection = new NpgsqlConnection(WithProductSearchPath(connectionString!));
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await CreateRetiredFixtureAsync(connection, ct);

            await ExecuteAsync(connection, @"
CREATE OR REPLACE FUNCTION collect.w5416_break_drop() RETURNS event_trigger LANGUAGE plpgsql AS $fn$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_event_trigger_dropped_objects() WHERE object_name = 'cpu_utilization_baseline')
    THEN
        RAISE EXCEPTION 'planted #5416 drop failure' USING ERRCODE = 'XX000';
    END IF;
END
$fn$", ct);
            await ExecuteAsync(connection, "CREATE EVENT TRIGGER w5416_break_drop ON sql_drop EXECUTE FUNCTION collect.w5416_break_drop()", ct);

            /* Sweep 1: cpu's drop fails and breaks the connection; file_io must still go. */
            var firstLog = new CapturingTestLogger();
            var dropped = await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, firstLog, ct);
            Assert.True(connection.State == ConnectionState.Open,
                $"the sweep left its connection {connection.State} after a failed drop: {firstLog.Joined}");
            Assert.True(dropped == 1, $"the sweep dropped {dropped} relations, wanted 1 (file_io, behind the failed cpu drop): {firstLog.Joined}");
            Assert.Equal(1, firstLog.Lines.Count(l => l.StartsWith("Warning:", StringComparison.Ordinal)
                && l.Contains("cpu_utilization_baseline", StringComparison.Ordinal)));
            Assert.Equal(1, firstLog.CountAtLevel(LogLevel.Warning));
            Assert.True(await ScalarAsync<bool>(connection, "SELECT to_regclass('collect.file_io_baseline') IS NULL", ct), "file_io_baseline must be gone");
            Assert.True(await ScalarAsync<bool>(connection, "SELECT to_regclass('collect.cpu_utilization_baseline') IS NOT NULL", ct), "cpu_utilization_baseline survives the failed drop");

            /* The same connection object keeps working afterwards: the retry on the next pass. */
            await ExecuteAsync(connection, "DROP EVENT TRIGGER w5416_break_drop", ct);
            var secondLog = new CapturingTestLogger();
            Assert.Equal(1, await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, secondLog, ct));
            Assert.Equal(0, secondLog.CountAtLevel(LogLevel.Warning));
            Assert.True(await ScalarAsync<bool>(connection, "SELECT to_regclass('collect.cpu_utilization_baseline') IS NULL", ct), "cpu_utilization_baseline must go on the retry");
            Assert.Equal(0, await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, null, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await ExecuteAsync(cleanup, "DROP EVENT TRIGGER IF EXISTS w5416_break_drop", cleanupCt);
                await ExecuteAsync(cleanup, "DROP FUNCTION IF EXISTS collect.w5416_break_drop()", cleanupCt);
                await ExecuteAsync(cleanup, TimescaleSupport.DropRetiredBaselineRelationSql("cpu_utilization_baseline"), cleanupCt);
                await ExecuteAsync(cleanup, TimescaleSupport.DropRetiredBaselineRelationSql("file_io_baseline"), cleanupCt);
            });
        }
    }

    /// <summary>
    /// A refresh job in flight when the sweep's drop lands: the drop loses, the connection must be Open when the
    /// sweep returns, and the retry loop must finish the job once the refresh is done.
    ///
    /// <para>#5549: this used to race a real <c>CALL run_job</c> against the drop, 40 times, with the policy also
    /// due on the scheduler. On CI the client backend running that refresh died with 0xC0000005 while the drop was
    /// running, and the postmaster restarted every process on the shared cluster, so the tests that were running
    /// at the time failed too. A TimescaleDB job must not run while its aggregate is being dropped, so the race is
    /// now staged instead. The refresh job still runs for real, to completion, before the sweep starts. Then the
    /// runner holds what a refresh holds until it commits, <c>ROW EXCLUSIVE</c> on the materialization hypertable
    /// (its DELETE and INSERT), in an open transaction. A <c>lock_timeout</c> on the test's own sweeper session makes
    /// the first drop give up behind that lock instead of waiting it out (the limit is reset right after that attempt,
    /// so the retries run with the server's own). That is the drop losing to the job with the connection still Open,
    /// the shape of the deadlock arm (40P01), on every iteration. The broken-connection arm
    /// (XX000) is pinned by the deterministic test above. The policy stays a day out, so the scheduler never
    /// launches its own refresh beside the drop either.</para>
    /// </summary>
    [Fact]
    public async Task Sweep_LeavesItsConnectionOpen_WhenARefreshHoldsALockDuringTheDrop_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live sweep race test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            using var setup = new NpgsqlConnection(connectionString);
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);

            /* run_job is qualified with the extension's schema. A pooled connection opened before a fresh store's
               migration has no search path to it, and the old test swallowed that 42883 with every other error, so on
               a fresh database its refresh job never ran at all. */
            var runJob = await ScalarAsync<string>(setup,
                "SELECT format('%I.run_job', n.nspname) FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'timescaledb'", ct);

            for (var i = 0; i < 5; i++)
            {
                await CreateRetiredFixtureAsync(setup, ct);
                var jobId = await ScalarAsync<int>(setup,
                    "SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_refresh_continuous_aggregate' AND hypertable_schema = 'collect' AND hypertable_name = 'cpu_utilization_baseline'", ct);
                var materialization = await ScalarAsync<string>(setup,
                    "SELECT format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name) FROM timescaledb_information.continuous_aggregates WHERE view_schema = 'collect' AND view_name = 'cpu_utilization_baseline'", ct);

                /* Opened with the product search path, like the service's own connections; run_job below stays qualified. */
                using var sweeper = new NpgsqlConnection(WithProductSearchPath(connectionString!));
                await sweeper.OpenAsync(ct);
                using var runner = new NpgsqlConnection(WithProductSearchPath(connectionString!));
                await runner.OpenAsync(ct);

                /* The refresh runs for real and finishes before any drop starts (#5549). */
                await ExecuteAsync(runner, $"CALL {runJob}({jobId})", ct);

                /* The next refresh, in flight: the lock its materialization writes hold until it commits. */
                using var inFlight = await runner.BeginTransactionAsync(ct);
                await ExecuteAsync(runner, $"LOCK TABLE {materialization} IN ROW EXCLUSIVE MODE", ct);
                await ExecuteAsync(sweeper, "SET lock_timeout = '500ms'", ct);

                var log = new CapturingTestLogger();
                for (var attempt = 0; attempt < 5; attempt++)
                {
                    await TimescaleSupport.DropRetiredBaselineAggregatesAsync(sweeper, log, ct);
                    Assert.True(sweeper.State == ConnectionState.Open,
                        $"iteration {i}: the connection is {sweeper.State} after sweep attempt {attempt + 1}: {log.Joined}");
                    if (attempt == 0)
                    {
                        /* The drop lost to the job: lock_timeout (55P03), logged once, and the aggregate is still there. */
                        Assert.True(await ScalarAsync<bool>(sweeper, "SELECT to_regclass('collect.cpu_utilization_baseline') IS NOT NULL", ct),
                            $"iteration {i}: the first sweep dropped the aggregate through the in-flight job's lock: {log.Joined}");
                        Assert.Equal(1, log.Lines.Count(l => l.StartsWith("Warning:", StringComparison.Ordinal)
                            && l.Contains("cpu_utilization_baseline", StringComparison.Ordinal)
                            && l.Contains("55P03", StringComparison.Ordinal)));
                        /* The 500 ms limit was for the attempt that has to lose. Later attempts run with the server's own
                           lock_timeout, so autovacuum or another job holding a lock briefly does not fail the test. */
                        await ExecuteAsync(sweeper, "RESET lock_timeout", ct);
                        await inFlight.CommitAsync(ct);
                    }

                    if (await ScalarAsync<bool>(sweeper, "SELECT to_regclass('collect.cpu_utilization_baseline') IS NULL AND to_regclass('collect.file_io_baseline') IS NULL", ct))
                    {
                        break;
                    }

                    await Task.Delay(TimeSpan.FromMilliseconds(250), ct);
                }

                Assert.True(await ScalarAsync<bool>(sweeper, "SELECT to_regclass('collect.cpu_utilization_baseline') IS NULL AND to_regclass('collect.file_io_baseline') IS NULL", ct),
                    $"iteration {i}: the retired relations survived five sweep attempts: {log.Joined}");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await ExecuteAsync(cleanup, TimescaleSupport.DropRetiredBaselineRelationSql("cpu_utilization_baseline"), cleanupCt);
                await ExecuteAsync(cleanup, TimescaleSupport.DropRetiredBaselineRelationSql("file_io_baseline"), cleanupCt);
            });
        }
    }

    /// <summary>The connection string with <c>Search Path</c> set to the product's (<see cref="PgSchemaGenerator.SearchPath"/>), as DarlingWorker sets it.</summary>
    private static string WithProductSearchPath(string connectionString) =>
        new NpgsqlConnectionStringBuilder(connectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;

    /// <summary>The pre-#2007 shapes the sweep retires: cpu as a continuous aggregate with its policies, file_io as a plain view.</summary>
    private static async Task CreateRetiredFixtureAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecuteAsync(connection,
            "SELECT create_hypertable('collect.cpu_utilization_stats', by_range('collection_time', INTERVAL '1 day'), if_not_exists => true, migrate_data => true)", ct);
        await ExecuteAsync(connection, @"
CREATE MATERIALIZED VIEW IF NOT EXISTS collect.cpu_utilization_baseline
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT server_id, time_bucket('1 hour', collection_time) AS bucket, collection_time,
       sum(sqlserver_cpu_utilization) AS cpu_sum,
       sum(power(sqlserver_cpu_utilization, 2)) AS cpu_sumsq,
       count(sqlserver_cpu_utilization) AS cpu_count
FROM collect.cpu_utilization_stats
GROUP BY server_id, bucket, collection_time
WITH NO DATA", ct);
        /* The refresh policy of this fixture's aggregate is parked a day out in both tests, so the scheduler never launches
           that policy's refresh while the sweep is dropping the aggregate (#5549). It stops only this policy: the other
           jobs the migration scheduled in the test database still run on their own schedules. */
        const string initialStart = "now() + INTERVAL '1 day'";
        await ExecuteAsync(connection,
            $"SELECT add_continuous_aggregate_policy('collect.cpu_utilization_baseline', start_offset => INTERVAL '3 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '1 hour', initial_start => {initialStart}, if_not_exists => true)", ct);
        await ExecuteAsync(connection,
            "CREATE OR REPLACE VIEW collect.file_io_baseline AS SELECT server_id, date_trunc('hour', collection_time) AS bucket, collection_time, count(*) AS row_count FROM collect.file_io_stats GROUP BY 1, 2, 3", ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        var value = await command.ExecuteScalarAsync(ct);
        Assert.NotNull(value);
        return (T)Convert.ChangeType(value, typeof(T), System.Globalization.CultureInfo.InvariantCulture)!;
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }
}
