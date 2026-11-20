/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9089: the migration applier gives a rung in <see cref="PgMigrations.DeclaredRungCommandTimeoutSeconds"/> its
/// declared command timeout, and every other rung the default one.
///
/// <para><b>What is run.</b> The shipped applier, through <see cref="PgMigrations.ApplyLadderForTestsAsync"/>, over a
/// synthetic ladder whose rungs only sleep. The default bound is shrunk to <see cref="ShrunkDefaultSeconds"/> so a
/// rung that outlasts it takes seconds. The declared bound is not shrunk: it is the applier's own entry for V281.</para>
///
/// <para><b>No timing race.</b> Neither test compares a duration to a threshold. A rung sleeps longer than the shrunk
/// default and far less than any bound that could apply by mistake, so the bound in force decides the outcome by
/// itself: the rung completes and is stamped, or it is cancelled and throws.</para>
///
/// <para><b>#1776 own-store.</b> Each test mints a scratch database and leaves it unmigrated, so the synthetic ladder is the
/// only ladder that store has seen, no other migrator can reach it, and the class is not serialized against the
/// <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class MigrationRungCommandTimeoutLiveTests
{
    /// <summary>The default rung bound these tests run with, in place of the shipped
    /// <see cref="PgMigrations.MigrationCommandTimeoutSeconds"/>.</summary>
    private const int ShrunkDefaultSeconds = 1;

    /// <summary>How long a silent rung sleeps when it has to outlast <see cref="ShrunkDefaultSeconds"/> and complete.</summary>
    private const int PastTheShrunkDefaultSeconds = 4;

    /// <summary>The version the synthetic declared rung carries: the applier's declared rung.</summary>
    private const int DeclaredVersion = 281;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// A silent rung with a declared bound sleeps past the default bound, completes and is stamped. The same sleep in
    /// a rung with no declared bound, under the same default, is cancelled: the declaration is the only difference
    /// between the two halves.
    /// </summary>
    [Fact]
    public async Task ASilentRungWithADeclaredTimeout_RunsPastTheDefaultBound_CompletesAndIsStamped()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the declared rung timeout test.");

        var ct = TestContext.Current.CancellationToken;

        Assert.True(
            PgMigrations.DeclaredRungCommandTimeoutSeconds.TryGetValue(DeclaredVersion, out var declaredSeconds),
            "the applier no longer declares a command timeout for V281, so this test has no declared rung to run");
        Assert.True(
            declaredSeconds > PastTheShrunkDefaultSeconds,
            $"the declared bound ({declaredSeconds} s) must outlast the rung's {PastTheShrunkDefaultSeconds} s sleep");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);

        var bodySucceeded = false;
        try
        {
            var logger = new CapturingTestLogger();
            var sleep = "SELECT pg_sleep(" + PastTheShrunkDefaultSeconds.ToString(CultureInfo.InvariantCulture) + ");";

            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);

                var applied = await PgMigrations.ApplyLadderForTestsAsync(
                    connection,
                    logger,
                    [new PgMigrations.Migration(DeclaredVersion, "declared-silent-rung", sleep)],
                    ShrunkDefaultSeconds,
                    ct);

                Assert.True(applied == 1, $"the declared rung was not applied. Log: {logger.Joined}");
            }

            Assert.Equal(new[] { DeclaredVersion }, await StampedVersionsAsync(scratch.ConnectionString, ct));

            /* The applier says which rung is running and for how long it may run, once. */
            var announcements = logger.Lines
                .Where(line => line.Contains("Applying migration V281", StringComparison.Ordinal))
                .ToList();
            var announcement = Assert.Single(announcements);
            Assert.Contains(declaredSeconds.ToString(CultureInfo.InvariantCulture) + "s", announcement, StringComparison.Ordinal);

            /* The matched half: the same sleep, the same default bound, no declaration. It is cancelled, so the
               default bound was in force above and the declaration is what let the rung finish. */
            var undeclaredVersion = DeclaredVersion + 1000;
            Assert.False(PgMigrations.DeclaredRungCommandTimeoutSeconds.ContainsKey(undeclaredVersion));

            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);

                var thrown = await Assert.ThrowsAnyAsync<NpgsqlException>(() => PgMigrations.ApplyLadderForTestsAsync(
                    connection,
                    logger,
                    [new PgMigrations.Migration(undeclaredVersion, "undeclared-silent-rung", sleep)],
                    ShrunkDefaultSeconds,
                    ct));

                Assert.IsType<TimeoutException>(thrown.InnerException);
            }

            Assert.Equal(new[] { DeclaredVersion }, await StampedVersionsAsync(scratch.ConnectionString, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                /* The scratch database is dropped by ScratchPostgres.DisposeAsync. */
                await Task.CompletedTask;
            });
        }
    }

    /// <summary>
    /// A silent rung with no declared bound is cancelled at the default bound and is not stamped. A quick rung before
    /// it in the same ladder is applied and stamped under the same bound, so the bound cancels a rung that outlasts it
    /// and nothing else. The slow rung sleeps for a tenth of the shipped default: were that the bound in force, the
    /// rung would complete and the test would fail.
    /// </summary>
    [Fact]
    public async Task ASilentRungWithNoDeclaredTimeout_IsCancelledAtTheDefaultBound_AndNothingIsStamped()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the default rung timeout test.");

        var ct = TestContext.Current.CancellationToken;

        const int QuickVersion = 1;
        const int SlowVersion = 2;
        Assert.False(PgMigrations.DeclaredRungCommandTimeoutSeconds.ContainsKey(QuickVersion));
        Assert.False(PgMigrations.DeclaredRungCommandTimeoutSeconds.ContainsKey(SlowVersion));

        var slowSleepSeconds = PgMigrations.MigrationCommandTimeoutSeconds / 10;
        Assert.True(
            slowSleepSeconds > ShrunkDefaultSeconds,
            $"the slow rung's sleep ({slowSleepSeconds} s) must outlast the shrunk default ({ShrunkDefaultSeconds} s)");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);

        var bodySucceeded = false;
        try
        {
            var logger = new CapturingTestLogger();

            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);

                var thrown = await Assert.ThrowsAnyAsync<NpgsqlException>(() => PgMigrations.ApplyLadderForTestsAsync(
                    connection,
                    logger,
                    [
                        new PgMigrations.Migration(QuickVersion, "quick-rung", "SELECT 1;"),
                        new PgMigrations.Migration(
                            SlowVersion,
                            "undeclared-silent-rung",
                            "CREATE TABLE public.rung_timeout_probe (a int);\n"
                            + "SELECT pg_sleep(" + slowSleepSeconds.ToString(CultureInfo.InvariantCulture) + ");"),
                    ],
                    ShrunkDefaultSeconds,
                    ct));

                Assert.IsType<TimeoutException>(thrown.InnerException);
            }

            /* The quick rung committed with its stamp. The slow rung left no stamp and none of its work: the table it
               created before it slept rolled back with it. */
            Assert.Equal(new[] { QuickVersion }, await StampedVersionsAsync(scratch.ConnectionString, ct));

            await using (var probe = new NpgsqlConnection(scratch.ConnectionString))
            {
                await probe.OpenAsync(ct);
                using var exists = new NpgsqlCommand("SELECT to_regclass('public.rung_timeout_probe') IS NOT NULL", probe);
                Assert.False((bool)(await exists.ExecuteScalarAsync(ct))!, "the cancelled rung's table survived its rollback");
            }

            /* No declared rung ran, so the applier announced none. */
            Assert.DoesNotContain(logger.Lines, line => line.Contains("Applying migration V", StringComparison.Ordinal));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                /* The scratch database is dropped by ScratchPostgres.DisposeAsync. */
                await Task.CompletedTask;
            });
        }
    }

    /// <summary>
    /// The versions stamped in the store, in order, read on a connection of its own so the answer does not depend on
    /// the state a cancelled command left the applier's connection in. The applier sets its search path per session,
    /// so this sets the same one before it names the stamp table.
    /// </summary>
    private static async Task<List<int>> StampedVersionsAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        using (var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection))
        {
            await setPath.ExecuteNonQueryAsync(ct);
        }

        var versions = new List<int>();
        using var read = new NpgsqlCommand("SELECT version FROM darling_schema_version ORDER BY version", connection);
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            versions.Add(reader.GetInt32(0));
        }

        return versions;
    }
}
