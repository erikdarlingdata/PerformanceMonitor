/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605 part 2: LIVE proof, against a real TimescaleDB refresh policy, that
/// <see cref="RollupCoverage.ProvenContiguousFromOf"/> is exact — and that it is exact ONLY because it refuses
/// to answer for a job whose batching config cannot guarantee a whole-window refresh per successful run.
///
/// <para><b>#1776 own-store</b> — mints its own scratch database through <see cref="ScratchPostgres"/> so it
/// never races the shared fixture or another test's rollup.</para>
/// </summary>
public sealed class RollupProvenContiguityLiveTests
{
    [Fact]
    public async Task ProvenContiguousFromOf_MatchesTheJobsOwnWindow_AndRefusesABatchedPartialRun()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live proven-contiguity test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        Assert.True(timescaleEnabled, "TimescaleDB must be available on CI for the live proven-contiguity test");

        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        var view = TimescaleSupport.QueryStatsIntervalHourlyView;
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        /* ── (a) 30 hours of raw, the shipped unbatched hourly policy, ONE run: the window it just proved
               whole must equal last_successful_finish - start_offset, and the ceiling must sit at the
               end_offset boundary. ── */
        await SeedQueryStatsAsync(connection, now.AddHours(-30), now, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var jobId = await ReadJobIdAsync(connection, view, ct);
        await RunJobAsync(connection, jobId, ct);

        var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        try
        {
            var availability = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
            var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, ct);

            var provenFrom = coverage.ProvenContiguousFromOf(view);
            Assert.NotNull(provenFrom);
            var expectedProvenFrom = await ReadLastSuccessfulFinishAsync(connection, jobId, ct) - TimeSpan.FromDays(1);
            Assert.Equal(expectedProvenFrom, provenFrom!.Value);

            var ceiling = coverage.CeilingOf(view);
            Assert.NotNull(ceiling);
            Assert.True(provenFrom!.Value < ceiling!.Value, "the proven span must sit strictly below the ceiling");

            /* ── (b) a HOLE: raw rows written after the refresh, older than the proven window's floor.
                   They must not be claimed — the proven floor does not move just because more raw arrived. ── */
            await SeedQueryStatsAsync(connection, now.AddHours(-28), now.AddHours(-27), ct);
            var afterHoleAvailability = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
            var afterHoleCoverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, afterHoleAvailability, ct);
            Assert.Equal(provenFrom, afterHoleCoverage.ProvenContiguousFromOf(view));

            /* ── (d) PAUSE the job: the signal must go null, not stale-true. ── */
            await PauseJobAsync(connection, jobId, ct);
            var pausedAvailability = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
            var pausedCoverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, pausedAvailability, ct);
            Assert.Null(pausedCoverage.ProvenContiguousFromOf(view));
            await ResumeJobAsync(connection, jobId, ct);
        }
        finally
        {
            await dataSource.DisposeAsync();
        }
    }

    /// <summary>
    /// THE BATCHING TRAP (#4605): a policy configured <c>buckets_per_batch => 1, max_batches_per_execution => 1</c>
    /// over a multi-day backlog reports Success on every run while materializing only ONE bucket per run —
    /// the shape a naive <c>last_successful_finish - start_offset</c> read would mistake for a whole-window
    /// proof. The signal must answer null for this job, not merely "a smaller span".
    /// </summary>
    [Fact]
    public async Task ProvenContiguousFromOf_AnswersNull_WhenTheJobCapsBatchesPerExecution()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live batching-trap test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        Assert.True(timescaleEnabled, "TimescaleDB must be available on CI for the live batching-trap test");

        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        var view = TimescaleSupport.QueryStatsIntervalHourlyView;
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        await SeedQueryStatsAsync(connection, now.AddHours(-30), now, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var jobId = await ReadJobIdAsync(connection, view, ct);

        /* Cap this ONE job to one bucket per run, same as the config the product ships nowhere today but
           TimescaleDB itself allows an operator (or a future policy) to set. */
        await using (var cap = new NpgsqlCommand(
            "SELECT alter_job(j.job_id, config => jsonb_set(jsonb_set(j.config, '{buckets_per_batch}', '1'), '{max_batches_per_execution}', '1')) FROM timescaledb_information.jobs AS j WHERE j.job_id = $1::integer",
            connection))
        {
            cap.Parameters.AddWithValue(jobId);
            await cap.ExecuteNonQueryAsync(ct);
        }

        await RunJobAsync(connection, jobId, ct);

        /* Confirm the trap actually fired: only PART of the backlog materialized, and the run still reports
           Success — otherwise this test would be proving nothing. */
        var materializedRows = await CountAsync(connection, $"SELECT count(*) FROM collect.{view}", ct);
        Assert.True(materializedRows > 0 && materializedRows < 30, "the capped run must have materialized only part of the 30-hour backlog for this test to prove anything");

        var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        try
        {
            var availability = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
            var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, ct);
            Assert.Null(coverage.ProvenContiguousFromOf(view));
        }
        finally
        {
            await dataSource.DisposeAsync();
        }
    }

    private static async Task SeedQueryStatsAsync(NpgsqlConnection connection, DateTime fromUtc, DateTime toUtc, System.Threading.CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(
            @"INSERT INTO collect.query_stats
                (server_id, server_name, database_name, query_hash, sql_handle, collection_time,
                 delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
              SELECT -460500, 'PROVEN-CONTIG-4605-SRV', 'msdb', 'hash', 'handle', gs,
                     1000, 2000, 1, 900
              FROM generate_series($1::timestamp, $2::timestamp, INTERVAL '15 minutes') AS gs",
            connection);
        insert.Parameters.AddWithValue(DateTime.SpecifyKind(fromUtc, DateTimeKind.Unspecified));
        insert.Parameters.AddWithValue(DateTime.SpecifyKind(toUtc, DateTimeKind.Unspecified));
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int> ReadJobIdAsync(NpgsqlConnection connection, string view, System.Threading.CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            @"SELECT j.job_id FROM timescaledb_information.jobs AS j
              JOIN timescaledb_information.continuous_aggregates AS ca
                ON ca.view_schema = j.hypertable_schema AND ca.view_name = j.hypertable_name
              WHERE j.proc_name = 'policy_refresh_continuous_aggregate' AND ca.view_name = $1",
            connection);
        read.Parameters.AddWithValue(view);
        var result = await read.ExecuteScalarAsync(ct);
        return Convert.ToInt32(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task RunJobAsync(NpgsqlConnection connection, int jobId, System.Threading.CancellationToken ct)
    {
        await using var run = new NpgsqlCommand($"CALL run_job({jobId})", connection);
        await run.ExecuteNonQueryAsync(ct);
    }

    private static async Task<DateTime> ReadLastSuccessfulFinishAsync(NpgsqlConnection connection, int jobId, System.Threading.CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            "SELECT last_finish FROM _timescaledb_internal.bgw_job_stat WHERE job_id = $1", connection);
        read.Parameters.AddWithValue(jobId);
        var value = await read.ExecuteScalarAsync(ct);
        return DateTime.SpecifyKind((DateTime)value!, DateTimeKind.Utc);
    }

    private static async Task PauseJobAsync(NpgsqlConnection connection, int jobId, System.Threading.CancellationToken ct)
    {
        await using var pause = new NpgsqlCommand("SELECT alter_job($1::integer, scheduled => false)", connection);
        pause.Parameters.AddWithValue(jobId);
        await pause.ExecuteNonQueryAsync(ct);
    }

    private static async Task ResumeJobAsync(NpgsqlConnection connection, int jobId, System.Threading.CancellationToken ct)
    {
        await using var resume = new NpgsqlCommand("SELECT alter_job($1::integer, scheduled => true)", connection);
        resume.Parameters.AddWithValue(jobId);
        await resume.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }
}
