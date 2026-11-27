/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5773: the migration applier warns once when the store it reads at start is stamped above this build's top rung, and
/// applies nothing. Run through the shipped applier (<see cref="PgMigrations.ApplyLadderForTestsAsync"/>) on a scratch
/// store, so the test reaches the check the way a start does. The wording and the range the warning names are covered
/// without a store by <see cref="NewerStoreWarningTests"/>.
///
/// <para><b>#1776 own-store.</b> The test mints a scratch database and leaves it unmigrated, so the synthetic ladder is the
/// only ladder that store has seen and no other migrator can reach it.</para>
/// </summary>
public sealed class NewerStoreWarningLiveTests
{
    private const string WarningText = "this build knows up to V";

    private const string SkippedTable = "newer_store_warning_skipped_rung";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// A rung above this build's top is stamped first, with no warning, because the store was below the build when it
    /// was read. The next start reads that stamp, warns once, and skips a rung at or below it, so the rung's table is
    /// never created.
    /// </summary>
    [Fact]
    public async Task AStoreStampedAboveTheBuildsTopRung_GetsOneWarning_AndNoRungIsApplied()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the newer-store warning test.");

        var ct = TestContext.Current.CancellationToken;
        var aboveTheBuild = PgMigrations.Scripts[^1].Version + 1;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);

        var bodySucceeded = false;
        try
        {
            var stampLogger = new CapturingTestLogger();
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);

                var stamped = await PgMigrations.ApplyLadderForTestsAsync(
                    connection,
                    stampLogger,
                    [new PgMigrations.Migration(aboveTheBuild, "stamp-above-the-build", "SELECT 1;")],
                    PgMigrations.MigrationCommandTimeoutSeconds,
                    ct);

                Assert.True(stamped == 1, $"the stamping rung was not applied. Log: {stampLogger.Joined}");
            }

            /* The matched half: the store was empty when this start read it, so nothing warned. */
            Assert.DoesNotContain(stampLogger.Lines, line => line.Contains(WarningText, StringComparison.Ordinal));

            var logger = new CapturingTestLogger();
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);

                var applied = await PgMigrations.ApplyLadderForTestsAsync(
                    connection,
                    logger,
                    [new PgMigrations.Migration(1, "would-create-a-table", "CREATE TABLE " + SkippedTable + " (id integer);")],
                    PgMigrations.MigrationCommandTimeoutSeconds,
                    ct);

                Assert.True(applied == 0, $"a rung was applied to a store above this build. Log: {logger.Joined}");
            }

            var warning = Assert.Single(logger.Lines, line => line.Contains(WarningText, StringComparison.Ordinal));
            Assert.StartsWith("Warning:", warning, StringComparison.Ordinal);
            Assert.Contains(
                "The store is at V" + aboveTheBuild.ToString(CultureInfo.InvariantCulture) + ";",
                warning,
                StringComparison.Ordinal);

            Assert.False(
                await TableExistsAsync(scratch.ConnectionString, SkippedTable, ct),
                "the skipped rung's table exists, so a rung below the stamp ran");

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
    /// Whether the table exists on the applier's search path, read on a connection of its own. The applier sets its
    /// search path per session, so this sets the same one before it resolves the name.
    /// </summary>
    private static async Task<bool> TableExistsAsync(string connectionString, string table, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        using (var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection))
        {
            await setPath.ExecuteNonQueryAsync(ct);
        }

        using var read = new NpgsqlCommand("SELECT to_regclass(@name) IS NOT NULL", connection);
        read.Parameters.AddWithValue("name", table);
        return (bool)(await read.ExecuteScalarAsync(ct))!;
    }
}
