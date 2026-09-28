/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4510, live: <see cref="TimescaleSupport.ConvergeCompressionScheduleAsync(NpgsqlConnection, Microsoft.Extensions.Logging.ILogger, System.Threading.CancellationToken)"/>
/// retunes a real hypertable's <c>compress_after</c> onto its own <see cref="TimescaleSupport.CompressAfterFor"/>
/// stagger, using <see cref="TimescaleSupport.SetCompressAfterSql"/> against a real job's config, and settles
/// on the second pass. The pure map and literal are pinned in <c>CompressAfterStaggerTests</c>; this is the
/// converge itself against real <c>timescaledb_information.jobs</c> rows.
/// </summary>
public sealed class CompressAfterStaggerLiveTests
{
    /// <summary>
    /// Two heavy hypertables carrying the OLD unstaggered policy converge onto their own offsets, with the
    /// SAME job id before and after (never a remove/re-add), a second pass issues no further alter_job, and a
    /// non-heavy table on the same store keeps the plain day.
    /// </summary>
    [Fact]
    public async Task EndToEnd_ConvergeCompressionSchedule_StaggersHeavyTables_AndSettles_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live compress_after stagger converge test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        /* Two REAL collector hypertables in TimescaleSupport.HeavyCompressAfterOffsetHours, and a real one
           that is NOT — the exclusion is meaningless against a throwaway name a converge would skip for a
           different reason entirely. */
        const string HeavyA = "query_store_stats";
        const string HeavyB = "wait_stats";
        const string NonHeavy = "memory_stats";

        Assert.True(TimescaleSupport.HeavyOffsetHoursFor(HeavyA) > 0);
        Assert.True(TimescaleSupport.HeavyOffsetHoursFor(HeavyB) > 0);
        Assert.Equal(0, TimescaleSupport.HeavyOffsetHoursFor(NonHeavy));

        await ExecAsync(connection, $"SELECT remove_compression_policy('collect.{HeavyA}', if_exists => true)", ct);
        await ExecAsync(connection, $"SELECT remove_compression_policy('collect.{HeavyB}', if_exists => true)", ct);
        await ExecAsync(connection, $"SELECT remove_compression_policy('collect.{NonHeavy}', if_exists => true)", ct);

        /* Idempotent, and needed before add_compression_policy will accept any of these three tables at
           all — the product runs this on every start. */
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql($"collect.{HeavyA}"), ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql($"collect.{HeavyB}"), ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql($"collect.{NonHeavy}"), ct);

        var bodySucceeded = false;
        try
        {
            /* (a) THE OLD WAY: every table on the shared, unstaggered day — the pre-#4510 state every
               deployed store is in. */
            await ExecAsync(connection,
                $"SELECT add_compression_policy('collect.{HeavyA}', compress_after => INTERVAL '{TimescaleSupport.CompressAfterDays} days', if_not_exists => true)", ct);
            await ExecAsync(connection,
                $"SELECT add_compression_policy('collect.{HeavyB}', compress_after => INTERVAL '{TimescaleSupport.CompressAfterDays} days', if_not_exists => true)", ct);
            await ExecAsync(connection,
                $"SELECT add_compression_policy('collect.{NonHeavy}', compress_after => INTERVAL '{TimescaleSupport.CompressAfterDays} days', if_not_exists => true)", ct);

            var heavyAJobIdBefore = await JobIdAsync(connection, HeavyA, ct);
            var heavyBJobIdBefore = await JobIdAsync(connection, HeavyB, ct);

            var convergeLog = new CapturingTestLogger();
            var converged = await TimescaleSupport.ConvergeCompressionScheduleAsync(connection, convergeLog, ct);
            Assert.True(converged >= 2,
                $"expected both heavy policies to be retuned onto their own stagger; {convergeLog.Joined}");

            /* Each job's compress_after now equals its own CompressAfterFor — read back from the catalog, not
               from the converge's return value. */
            Assert.Equal(
                TimeSpan.FromDays(TimescaleSupport.CompressAfterDays) + TimeSpan.FromHours(TimescaleSupport.HeavyOffsetHoursFor(HeavyA)),
                await CompressAfterAsync(connection, HeavyA, ct));
            Assert.Equal(
                TimeSpan.FromDays(TimescaleSupport.CompressAfterDays) + TimeSpan.FromHours(TimescaleSupport.HeavyOffsetHoursFor(HeavyB)),
                await CompressAfterAsync(connection, HeavyB, ct));

            /* THE SAME job id, before and after — a remove/re-add would lose the job's stats history and
               hand the hypertable a new id, which is the mistake this design rejects. */
            Assert.Equal(heavyAJobIdBefore, await JobIdAsync(connection, HeavyA, ct));
            Assert.Equal(heavyBJobIdBefore, await JobIdAsync(connection, HeavyB, ct));

            /* (c) THE NON-HEAVY TABLE keeps the plain day — untouched by a converge that demonstrably moved
               two other tables, so "it was left alone" is not a sweep that looked at nothing. */
            Assert.Equal(
                TimeSpan.FromDays(TimescaleSupport.CompressAfterDays),
                await CompressAfterAsync(connection, NonHeavy, ct));

            /* (b) A SECOND PASS issues no further alter_job for compress_after: the config is already what
               the stagger wants, so nothing here is stale. */
            var secondLog = new CapturingTestLogger();
            var secondConverged = await TimescaleSupport.ConvergeCompressionScheduleAsync(connection, secondLog, ct);
            Assert.Equal(0, secondConverged);
            Assert.DoesNotContain("eligibility delay (#4510)", secondLog.Joined, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await ExecAsync(cleanup, $"SELECT add_compression_policy('collect.{HeavyA}', compress_after => INTERVAL '{TimescaleSupport.CompressAfterFor(HeavyA)}', if_not_exists => true)", cleanupCt);
                await ExecAsync(cleanup, $"SELECT add_compression_policy('collect.{HeavyB}', compress_after => INTERVAL '{TimescaleSupport.CompressAfterFor(HeavyB)}', if_not_exists => true)", cleanupCt);
                await ExecAsync(cleanup, $"SELECT add_compression_policy('collect.{NonHeavy}', compress_after => INTERVAL '{TimescaleSupport.CompressAfterFor(NonHeavy)}', if_not_exists => true)", cleanupCt);
            });
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int> JobIdAsync(NpgsqlConnection connection, string table, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand($@"
SELECT job_id
FROM timescaledb_information.jobs
WHERE hypertable_schema = 'collect' AND hypertable_name = '{table}'
AND   (proc_name LIKE '%compression%' OR proc_name LIKE '%columnstore%')", connection);
        return Convert.ToInt32((await command.ExecuteScalarAsync(ct))!, CultureInfo.InvariantCulture);
    }

    private static async Task<TimeSpan?> CompressAfterAsync(NpgsqlConnection connection, string table, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand($@"
SELECT (j.config->>'compress_after')::interval
FROM timescaledb_information.jobs AS j
WHERE j.hypertable_schema = 'collect' AND j.hypertable_name = '{table}'
AND   (j.proc_name LIKE '%compression%' OR j.proc_name LIKE '%columnstore%')", connection);
        return await command.ExecuteScalarAsync(ct) is TimeSpan interval ? interval : null;
    }
}
