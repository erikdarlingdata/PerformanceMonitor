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
            using var connection = new NpgsqlConnection(connectionString);
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
    /// The real race, with the policy's refresh job left to run (no <c>initial_start</c> delay) and one running
    /// beside every drop: the sweep either wins, loses to a deadlock (40P01, connection stays Open) or loses to
    /// <c>XX000</c> (connection broken). Whichever it is, the connection must be Open when the sweep returns and
    /// the retry loop must finish the job. Whether the XX000 arm fires is down to timing (about one iteration in
    /// forty on a laptop), so this checks the invariant on every iteration and the deterministic test above is
    /// what pins the broken-connection arm.
    /// </summary>
    [Fact]
    public async Task Sweep_LeavesItsConnectionOpen_WhenARefreshJobRunsDuringTheDrop_AgainstDevPostgres()
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

            for (var i = 0; i < 40; i++)
            {
                await CreateRetiredFixtureAsync(setup, ct, delayRefreshPolicy: false);
                var jobId = await ScalarAsync<int>(setup,
                    "SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_refresh_continuous_aggregate' AND hypertable_schema = 'collect' AND hypertable_name = 'cpu_utilization_baseline'", ct);

                using var sweeper = new NpgsqlConnection(connectionString);
                await sweeper.OpenAsync(ct);
                using var runner = new NpgsqlConnection(connectionString);
                await runner.OpenAsync(ct);
                var run = Task.Run(async () =>
                {
                    try { await ExecuteAsync(runner, $"CALL run_job({jobId})", ct); }
                    catch (Exception ex) when (ex is not OperationCanceledException) { /* the job losing to the DROP is the point */ }
                }, ct);

                var log = new CapturingTestLogger();
                for (var attempt = 0; attempt < 5; attempt++)
                {
                    await TimescaleSupport.DropRetiredBaselineAggregatesAsync(sweeper, log, ct);
                    Assert.True(sweeper.State == ConnectionState.Open,
                        $"iteration {i}: the connection is {sweeper.State} after sweep attempt {attempt + 1}: {log.Joined}");
                    if (await ScalarAsync<bool>(sweeper, "SELECT to_regclass('collect.cpu_utilization_baseline') IS NULL AND to_regclass('collect.file_io_baseline') IS NULL", ct))
                    {
                        break;
                    }

                    await Task.Delay(TimeSpan.FromMilliseconds(250), ct);
                }

                await run;
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

    /// <summary>The pre-#2007 shapes the sweep retires: cpu as a continuous aggregate with its policies, file_io as a plain view.</summary>
    private static async Task CreateRetiredFixtureAsync(NpgsqlConnection connection, CancellationToken ct, bool delayRefreshPolicy = true)
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
        /* The race test wants the refresh job live (initial_start now), so it can run beside the drop. The
           deterministic test plants its own failure and keeps the job a day out. */
        var initialStart = delayRefreshPolicy ? "now() + INTERVAL '1 day'" : "now()";
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
