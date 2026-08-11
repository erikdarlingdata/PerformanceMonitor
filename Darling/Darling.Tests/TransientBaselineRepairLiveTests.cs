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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2189 gated-live contract for <see cref="DarlingAlertReadAdapter.RepairTransientDatabaseStateBaselineSql"/>.
///
/// <para>The bug: the seed excluded only SUSPECT / RECOVERY_PENDING / EMERGENCY, so a database observed
/// mid-restore had RESTORING recorded as its ACCEPTED baseline and then deviated permanently by being
/// healthy. Measured on the production fleet: 636 alerts in 24 hours from 5 databases, every one reading
/// "Expected: RESTORING, Current: ONLINE". Excluding those states from the seed stops new ones; it cannot
/// repair rows already written, because the seed is insert-if-absent by design.</para>
///
/// <para>Live rather than harness-level because the whole statement IS the behaviour — there is no C# to
/// exercise. Runs against a real Postgres gated on <c>DARLING_TEST_PG</c>, on the serialized
/// "live-postgres" collection, against a negative sentinel server_id, cleaning up in finally.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class TransientBaselineRepairLiveTests
{
    private const int LiveServerId = -915759;

    [Fact]
    public async Task Repair_DropsGuessedTransientBaselines_KeepsSteadyStatesAndOperatorIntent()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live transient-baseline repair.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteLiveRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            /* The bug's signature: a guessed baseline naming a state nobody would choose as "expected". */
            await ExpectedAsync(connection, ct, "MidRestore", "RESTORING", userOverride: false);
            await ExpectedAsync(connection, ct, "MidRecovery", "RECOVERING", userOverride: false);
            await ExpectedAsync(connection, ct, "FirstSeenSuspect", "SUSPECT", userOverride: false);

            /* Steady states a baseline SHOULD hold. STANDBY is the one worth pinning explicitly: a
               log-shipping secondary sits there permanently, so it is a legitimate expectation and must not
               be swept along with the transient states it superficially resembles. */
            await ExpectedAsync(connection, ct, "Healthy", "ONLINE", userOverride: false);
            await ExpectedAsync(connection, ct, "LogShipped", "STANDBY", userOverride: false);

            /* Operator intent, even when it names a transient state: somebody who deliberately expects a
               database to sit in RESTORING means it, and their row must survive. */
            await ExpectedAsync(connection, ct, "DeliberatelyRestoring", "RESTORING", userOverride: true);

            using (var repair = new NpgsqlCommand(DarlingAlertReadAdapter.RepairTransientDatabaseStateBaselineSql, connection))
            {
                repair.Parameters.AddWithValue(LiveServerId);
                Assert.Equal(3, await repair.ExecuteNonQueryAsync(ct));
            }

            Assert.Null(await ExpectedStateAsync(connection, ct, "MidRestore"));
            Assert.Null(await ExpectedStateAsync(connection, ct, "MidRecovery"));
            Assert.Null(await ExpectedStateAsync(connection, ct, "FirstSeenSuspect"));

            Assert.Equal("ONLINE", await ExpectedStateAsync(connection, ct, "Healthy"));
            Assert.Equal("STANDBY", await ExpectedStateAsync(connection, ct, "LogShipped"));
            Assert.Equal("RESTORING", await ExpectedStateAsync(connection, ct, "DeliberatelyRestoring"));

            /* Idempotent: it runs on every evaluation of every server, so a second pass over an already
               repaired store must change nothing. */
            using (var again = new NpgsqlCommand(DarlingAlertReadAdapter.RepairTransientDatabaseStateBaselineSql, connection))
            {
                again.Parameters.AddWithValue(LiveServerId);
                Assert.Equal(0, await again.ExecuteNonQueryAsync(ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteLiveRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public void Seed_ExcludesEveryStateTheRepairDeletes()
    {
        /* The two statements have to agree, or the pair oscillates: the seed writes a baseline the repair
           deletes on the next evaluation, forever, churning the table and re-announcing the database. This
           pins that the seed's exclusion list covers everything the repair treats as unfit. */
        foreach (var state in new[] { "SUSPECT", "RECOVERY_PENDING", "EMERGENCY", "RESTORING", "RECOVERING" })
        {
            Assert.Contains($"'{state}'", DarlingAlertReadAdapter.RepairTransientDatabaseStateBaselineSql, StringComparison.Ordinal);
            Assert.Contains($"'{state}'", DarlingAlertReadAdapter.SeedDatabaseStateExpectedSql, StringComparison.Ordinal);
        }

        /* And the inverse: a steady state must appear in NEITHER list. STANDBY is the trap. */
        Assert.DoesNotContain("'STANDBY'", DarlingAlertReadAdapter.RepairTransientDatabaseStateBaselineSql, StringComparison.Ordinal);
        Assert.DoesNotContain("'ONLINE'", DarlingAlertReadAdapter.RepairTransientDatabaseStateBaselineSql, StringComparison.Ordinal);

        /* Operator intent is the repair's only exemption, so its absence would be silent data loss. */
        Assert.Contains("is_user_override = false", DarlingAlertReadAdapter.RepairTransientDatabaseStateBaselineSql, StringComparison.Ordinal);
    }

    private static async Task ExpectedAsync(
        NpgsqlConnection connection, CancellationToken ct, string database, string expected, bool userOverride)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO config.database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
VALUES ($1, $2, $3, $4, (now() AT TIME ZONE 'UTC'))", connection);
        command.Parameters.AddWithValue(LiveServerId);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(expected);
        command.Parameters.AddWithValue(userOverride);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ExpectedStateAsync(
        NpgsqlConnection connection, CancellationToken ct, string database)
    {
        using var command = new NpgsqlCommand(
            "SELECT expected_state FROM config.database_state_expected WHERE server_id = $1 AND database_name = $2",
            connection);
        command.Parameters.AddWithValue(LiveServerId);
        command.Parameters.AddWithValue(database);
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull or null ? null : (string)value;
    }

    private static async Task DeleteLiveRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM config.database_state_expected WHERE server_id = {LiveServerId.ToString(CultureInfo.InvariantCulture)};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
