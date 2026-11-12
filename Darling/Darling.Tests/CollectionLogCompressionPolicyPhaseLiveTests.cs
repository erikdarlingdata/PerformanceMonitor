/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own empty scratch database through ScratchPostgres and never touches another test's
   rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #8275: a store whose database carries TimescaleDB before the migrations run (a prepared database, or a cluster whose
/// template database has the extension) never holds an unphased compression policy on <c>collect.collection_log</c>.
/// V23 converts the table to a hypertable and enables compression; the policy comes only from the runtime
/// <see cref="TimescaleSupport.EnsureCollectionLogHypertableAsync"/>, which puts it on the hourly cadence and the phase
/// minute the hourly refreshes leave clear. A policy added with TimescaleDB's defaults runs within seconds of its
/// creation, on the creation minute, and the first housekeeping pass then reads that run as an overrun with no clearance
/// when the minute is one an hourly refresh starts on.
/// </summary>
public sealed class CollectionLogCompressionPolicyPhaseLiveTests
{
    private const int V22 = 22;

    private const int Server = -827501;

    private const long LogIdBase = -827_500_000_000L;

    /* The version stamp table as the migrator creates it, so rungs applied one at a time here are stamped the same way. */
    private const string VersionTableSql = @"
CREATE TABLE IF NOT EXISTS darling_schema_version (
    version integer NOT NULL PRIMARY KEY,
    name text NOT NULL,
    applied_at timestamp NOT NULL
);";

    private const string JobSql = @"
SELECT j.fixed_schedule,
       j.schedule_interval,
       j.initial_start AT TIME ZONE 'UTC',
       j.job_id
FROM timescaledb_information.jobs AS j
WHERE j.hypertable_schema = 'collect'
AND   j.hypertable_name = 'collection_log'
AND   (j.proc_name LIKE '%compression%' OR j.proc_name LIKE '%columnstore%')";

    /// <summary>
    /// Both orders the extension can precede V23 in: a store at V22 whose collection_log is a plain table carrying rows
    /// (the upgrade V23 was written for), and a fresh store whose database already has the extension when the first
    /// migration runs. In both, the job that exists after the startup convergence is the phased one, and no unphased
    /// job exists at any point before it.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AStoreWithTheExtensionBeforeV23_NeverCarriesAnUnphasedPolicy_AndEndsOnThePhasedHourlySchedule(bool fromV22)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live collection_log policy phase test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB is not available on this cluster.");

        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await ExecAsync(connection, "SET search_path = " + PgSchemaGenerator.SearchPath, ct);
        Assert.Equal(1L, await ScalarAsync<long>(connection, "SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb'", ct));

        if (fromV22)
        {
            await ApplyThroughAsync(connection, V22, ct);
            Assert.Equal(V22, await ScalarAsync<int>(connection, "SELECT max(version) FROM darling_schema_version", ct));
            Assert.False(await IsHypertableAsync(connection, ct), "collection_log is still a plain table at V22");

            /* Two days of runs, as a store that has been collecting holds when it upgrades through V23. Fixed dates, so the
               rows sit in chunks V23's migrate_data must create whatever the clock says. */
            var day = new DateTime(2026, 1, 10, 0, 0, 0, DateTimeKind.Unspecified);
            for (var i = 0; i < 48; i++)
            {
                await using var seed = new NpgsqlCommand(
                    "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) " +
                    "VALUES ($1, $2, 'PHASE-SRV', 'wait_stats', $3, 5, 'SUCCESS', 1)", connection);
                seed.Parameters.AddWithValue(LogIdBase - i);
                seed.Parameters.AddWithValue(Server);
                seed.Parameters.AddWithValue(day.AddMinutes(30 * i));
                await seed.ExecuteNonQueryAsync(ct);
            }
        }

        await PgMigrations.MigrateAsync(connection, ct);

        /* After the migrations and before anything at runtime has run: V23 ran its hypertable branch (the extension was there), so the
           table is a hypertable, and whatever compression job exists for it is already on the phased hourly schedule. A job on
           TimescaleDB's defaults here (fixed_schedule false, a 12-hour cadence) runs on its creation minute within seconds, which
           is the run the first housekeeping pass reads as an overrun with no clearance. */
        Assert.True(await IsHypertableAsync(connection, ct), "V23 converts collection_log when the extension is present");
        var afterMigrate = await ReadJobAsync(connection, ct);
        Assert.True(
            afterMigrate is null || afterMigrate.Value.FixedSchedule && afterMigrate.Value.ScheduleInterval == TimeSpan.FromHours(1),
            afterMigrate is null
                ? "unreachable"
                : $"the migrations left collection_log an unphased compression job (job {afterMigrate.Value.JobId}: fixed_schedule {afterMigrate.Value.FixedSchedule}, "
                  + $"schedule_interval {afterMigrate.Value.ScheduleInterval}), which runs on its creation minute before the runtime convergence can phase it");

        if (fromV22)
        {
            Assert.Equal(48L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.collection_log WHERE server_id = " + Server, ct));
        }

        /* The startup convergence as the service runs it: collection_log's own ensure, then the schedule converge over every policy. */
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await TimescaleSupport.ConvergeCompressionScheduleAsync(connection, null, ct);

        var job = await ReadJobAsync(connection, ct);
        Assert.NotNull(job);
        Assert.True(job.Value.FixedSchedule, "the compression job must be on a fixed schedule");
        Assert.Equal(TimeSpan.FromHours(1), job.Value.ScheduleInterval);
        Assert.True(TimescaleSupport.TryCompressionPhaseMinutesFor(TimescaleSupport.CollectionLogTable, out var phaseMinute));
        Assert.NotNull(job.Value.InitialStartUtc);
        Assert.Equal(phaseMinute, job.Value.InitialStartUtc!.Value.Minute);
        Assert.Equal(0, job.Value.InitialStartUtc.Value.Second);
    }

    /* Rungs 1 through <paramref name="version"/>, one transaction and one version stamp each, the way the migrator applies them. */
    private static async Task ApplyThroughAsync(NpgsqlConnection connection, int version, CancellationToken ct)
    {
        await ExecAsync(connection, VersionTableSql, ct);
        foreach (var rung in PgMigrations.Scripts.Where(m => m.Version <= version).OrderBy(m => m.Version))
        {
            await using var transaction = await connection.BeginTransactionAsync(ct);
            await using (var apply = new NpgsqlCommand(rung.Sql, connection, transaction) { CommandTimeout = 300 })
            {
                await apply.ExecuteNonQueryAsync(ct);
            }

            await using (var stamp = new NpgsqlCommand(
                "INSERT INTO darling_schema_version (version, name, applied_at) VALUES ($1, $2, TIMESTAMP '2026-01-01 00:00:00')", connection, transaction))
            {
                stamp.Parameters.AddWithValue(rung.Version);
                stamp.Parameters.AddWithValue(rung.Name);
                await stamp.ExecuteNonQueryAsync(ct);
            }

            await transaction.CommitAsync(ct);
        }
    }

    private static async Task<(bool FixedSchedule, TimeSpan ScheduleInterval, DateTime? InitialStartUtc, int JobId)?> ReadJobAsync(
        NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(JobSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        var job = (
            reader.GetBoolean(0),
            reader.GetFieldValue<TimeSpan>(1),
            reader.IsDBNull(2) ? (DateTime?)null : reader.GetDateTime(2),
            reader.GetInt32(3));
        Assert.False(await reader.ReadAsync(ct), "collection_log carries more than one compression job");
        return job;
    }

    private static async Task<bool> IsHypertableAsync(NpgsqlConnection connection, CancellationToken ct)
        => await ScalarAsync<long>(
            connection,
            "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_schema = 'collect' AND hypertable_name = 'collection_log'",
            ct) == 1L;

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }
}
