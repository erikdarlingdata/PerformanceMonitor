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
/// Gated-live contracts for the maintenance statements the database-state alert runs against the store
/// beside its deviation read — the parts of this alert whose correctness lives in SQL rather than in the
/// engine: <see cref="DarlingAlertReadAdapter.ClearRecoveredDatabaseStateAlertsSql"/> (#2166, the
/// store-derived half of the edge trigger), <see cref="DarlingAlertReadAdapter.SeedDatabaseStateExpectedSql"/>
/// and <see cref="DarlingAlertReadAdapter.HealDatabaseStateBaselineToOnlineSql"/> (#2189, what a baseline is
/// allowed to be learned from and what un-learns a stale one).
///
/// <para>Why these have to be LIVE rather than harness tests: each guards a bug that only a store can
/// exhibit. #2166's clear depended on the engine's in-memory active set, so a restart between an alert and
/// the recovery left <c>last_alerted_state</c> sticky forever — a test that drives the engine can only prove
/// the path a running process takes, and the restart gap is invisible to it by construction. #2189's pair
/// decide what rows EXIST, which no amount of engine stubbing observes.</para>
///
/// <para>Runs against a real Postgres gated on <c>DARLING_TEST_PG</c>, on the serialized "live-postgres"
/// collection, against a negative sentinel server_id, cleaning up in finally — the house pattern.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DatabaseStateAlertMemoryLiveTests
{
    private const int LiveServerId = -915758;
    private const string Name = "DBSTATE-MEMORY-SRV";

    [Fact]
    public async Task ClearRecovered_ForgetsOnlyDatabasesBackAtExpected_WithNothingHeldInMemory()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live database-state memory clear.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteLiveRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var newest = new DateTime(2026, 08, 11, 9, 0, 0, DateTimeKind.Unspecified);

            /* Recovered: alerted OFFLINE, now back ONLINE == expected. MUST be cleared — this is the
               restart-gap case, where no process ever witnessed the falling edge. */
            await StateAsync(connection, ct, "BackOnline", "ONLINE", standby: false, at: newest);
            await ExpectedAsync(connection, ct, "BackOnline", expected: "ONLINE", lastAlerted: "OFFLINE");

            /* Still deviating: alerted OFFLINE and still OFFLINE. MUST be kept, or the repetition this alert
               went quiet about starts over on the next cycle. */
            await StateAsync(connection, ct, "StillParked", "OFFLINE", standby: false, at: newest);
            await ExpectedAsync(connection, ct, "StillParked", expected: "ONLINE", lastAlerted: "OFFLINE");

            /* Deviating DIFFERENTLY: alerted OFFLINE, now SUSPECT. Not at expected, so the memory stays —
               the engine's own state comparison is what fires this one again, not a cleared memory. */
            await StateAsync(connection, ct, "TurnedSuspect", "SUSPECT", standby: false, at: newest);
            await ExpectedAsync(connection, ct, "TurnedSuspect", expected: "ONLINE", lastAlerted: "OFFLINE");

            /* Standby: expected STANDBY and currently in standby, which the effective-state CASE resolves to
               STANDBY rather than the raw state_desc. Pins that this statement reads the same effective
               state the deviation query does — comparing against state_desc would leave it uncleared. */
            await StateAsync(connection, ct, "LogShipped", "RESTORING", standby: true, at: newest);
            await ExpectedAsync(connection, ct, "LogShipped", expected: "STANDBY", lastAlerted: "RESTORING");

            /* The (ignore) sentinel: an operator silenced it, so a memory must not outlive the silence. */
            await StateAsync(connection, ct, "Silenced", "OFFLINE", standby: false, at: newest);
            await ExpectedAsync(connection, ct, "Silenced", expected: "(ignore)", lastAlerted: "OFFLINE");

            using (var clear = new NpgsqlCommand(DarlingAlertReadAdapter.ClearRecoveredDatabaseStateAlertsSql, connection))
            {
                clear.Parameters.AddWithValue(LiveServerId);
                await clear.ExecuteNonQueryAsync(ct);
            }

            Assert.Null(await MemoryAsync(connection, ct, "BackOnline"));
            Assert.Null(await MemoryAsync(connection, ct, "LogShipped"));
            Assert.Null(await MemoryAsync(connection, ct, "Silenced"));

            Assert.Equal("OFFLINE", await MemoryAsync(connection, ct, "StillParked"));
            Assert.Equal("OFFLINE", await MemoryAsync(connection, ct, "TurnedSuspect"));

            /* Idempotent: a second sweep over an already-clear store is a no-op, which matters because this
               runs on EVERY evaluation of every server. */
            using (var again = new NpgsqlCommand(DarlingAlertReadAdapter.ClearRecoveredDatabaseStateAlertsSql, connection))
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
    public async Task Seed_RefusesToLearnATransientState_SoAMidRestoreOnboardingStaysPending()
    {
        /* #2189: the seed's exclusion list is what decides a database's accepted normal FOREVER, and it used
           to exclude only the integrity states. A database observed mid-restore therefore learned RESTORING
           as expected and deviated by being healthy ever after. RESTORING and RECOVERING are databases in
           the middle of an operation, not steady states, and are now refused the same way SUSPECT is. */
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live database-state seed pins.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteLiveRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var newest = new DateTime(2026, 08, 11, 10, 0, 0, DateTimeKind.Unspecified);

            await StateAsync(connection, ct, "MidRestore", "RESTORING", standby: false, at: newest);
            await StateAsync(connection, ct, "ComingUp", "RECOVERING", standby: false, at: newest);
            await StateAsync(connection, ct, "Corrupt", "SUSPECT", standby: false, at: newest);
            await StateAsync(connection, ct, "Healthy", "ONLINE", standby: false, at: newest);
            await StateAsync(connection, ct, "Parked", "OFFLINE", standby: false, at: newest);
            /* A standby secondary's effective state is the synthetic STANDBY, which IS stable by construction
               and so is exactly the kind of state worth learning — the raw RESTORING underneath it is not. */
            await StateAsync(connection, ct, "LogShipped", "RESTORING", standby: true, at: newest);

            using (var seed = new NpgsqlCommand(DarlingAlertReadAdapter.SeedDatabaseStateExpectedSql, connection))
            {
                seed.Parameters.AddWithValue(LiveServerId);
                await seed.ExecuteNonQueryAsync(ct);
            }

            Assert.Null(await ExpectedStateAsync(connection, ct, "MidRestore"));
            Assert.Null(await ExpectedStateAsync(connection, ct, "ComingUp"));
            Assert.Null(await ExpectedStateAsync(connection, ct, "Corrupt"));

            Assert.Equal("ONLINE", await ExpectedStateAsync(connection, ct, "Healthy"));
            Assert.Equal("OFFLINE", await ExpectedStateAsync(connection, ct, "Parked"));
            Assert.Equal("STANDBY", await ExpectedStateAsync(connection, ct, "LogShipped"));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteLiveRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task Heal_RelearnsOnline_OnlyForInferredBaselinesTheSeedWouldHaveRefused()
    {
        /* #2189's other half: the widened seed governs rows that do not exist yet, and cannot touch the ones
           already written — five databases on the reporting fleet were baselined RESTORING and then alerted
           ~127 times each in 24 hours for being ONLINE. The heal applies the seed's own rule after the fact:
           a baseline recording a state the seed would REFUSE to learn is not a baseline anyone chose, so once
           the database is demonstrably healthy the steady state is learned instead.

           Every row below the first is one that must NOT be healed. The failure mode on this side is silence,
           and there are more ways to cause it than to fix it. */
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live database-state baseline heal.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteLiveRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var newest = new DateTime(2026, 08, 11, 11, 0, 0, DateTimeKind.Unspecified);

            /* The reported row: baselined mid-restore, restore finished, now permanently "deviating" by being
               healthy. Healed — and its alerted-state memory goes with the baseline it described. */
            await StateAsync(connection, ct, "Poisoned", "ONLINE", standby: false, at: newest);
            await ExpectedAsync(connection, ct, "Poisoned", expected: "RESTORING", lastAlerted: "ONLINE");

            /* An operator's declaration. #2166's composition contract says a database parked at expected
               OFFLINE stays quiet while parked and still alerts the moment it comes back ONLINE — which only
               works if the heal leaves overrides alone. Rewriting this to ONLINE would silently delete the
               operator's intent AND the alert they set it up to get. */
            await StateAsync(connection, ct, "Parked", "ONLINE", standby: false, at: newest);
            await ExpectedAsync(connection, ct, "Parked", expected: "OFFLINE", lastAlerted: "", isOverride: true);

            /* The trap: a standby secondary reports state_desc = 'ONLINE' with is_in_standby set. Matching the
               RAW column would re-baseline every log-shipping secondary from STANDBY to ONLINE and then alert
               it forever for being STANDBY — #2189 recreated for the family #1986 works hardest to keep quiet. */
            await StateAsync(connection, ct, "LogShipped", "ONLINE", standby: true, at: newest);
            await ExpectedAsync(connection, ct, "LogShipped", expected: "STANDBY", lastAlerted: "");

            /* Not ONLINE yet: a database that moved from one un-settled state to another has not settled, so
               its illegitimate baseline stays exactly as illegitimate as it was. */
            await StateAsync(connection, ct, "StillDown", "OFFLINE", standby: false, at: newest);
            await ExpectedAsync(connection, ct, "StillDown", expected: "RESTORING", lastAlerted: "");

            /* A STANDBY secondary that turns up TRULY online (is_in_standby now 0) has been recovered out of
               standby: log shipping is broken, and that deviation is the alert's entire job. Healing it would
               replace the alert with silence and then fire when the operator put standby BACK. STANDBY is a
               steady state the seed learns on purpose, so it is not on the heal's list. */
            await StateAsync(connection, ct, "RecoveredSecondary", "ONLINE", standby: false, at: newest);
            await ExpectedAsync(connection, ct, "RecoveredSecondary", expected: "STANDBY", lastAlerted: "");

            /* Same reasoning for the other steady state. An auto-baselined OFFLINE database brought up for an
               hour of maintenance must not have its baseline rewritten, or re-parking it leaves it deviating
               forever against a baseline it never had - this bug, inverted, by the fix for it. */
            await StateAsync(connection, ct, "WasParked", "ONLINE", standby: false, at: newest);
            await ExpectedAsync(connection, ct, "WasParked", expected: "OFFLINE", lastAlerted: "");

            /* The sentinel. Not on the list either, so un-ignoring a database by accident cannot happen even
               if some future path writes "(ignore)" without the override flag. */
            await StateAsync(connection, ct, "Silenced", "ONLINE", standby: false, at: newest);
            await ExpectedAsync(connection, ct, "Silenced", expected: "(ignore)", lastAlerted: "");

            using (var heal = new NpgsqlCommand(DarlingAlertReadAdapter.HealDatabaseStateBaselineToOnlineSql, connection))
            {
                heal.Parameters.AddWithValue(LiveServerId);
                Assert.Equal(1, await heal.ExecuteNonQueryAsync(ct));
            }

            Assert.Equal("ONLINE", await ExpectedStateAsync(connection, ct, "Poisoned"));
            Assert.Null(await MemoryAsync(connection, ct, "Poisoned"));

            Assert.Equal("OFFLINE", await ExpectedStateAsync(connection, ct, "Parked"));
            Assert.Equal("STANDBY", await ExpectedStateAsync(connection, ct, "LogShipped"));
            Assert.Equal("RESTORING", await ExpectedStateAsync(connection, ct, "StillDown"));
            Assert.Equal("STANDBY", await ExpectedStateAsync(connection, ct, "RecoveredSecondary"));
            Assert.Equal("OFFLINE", await ExpectedStateAsync(connection, ct, "WasParked"));
            Assert.Equal("(ignore)", await ExpectedStateAsync(connection, ct, "Silenced"));

            /* Idempotent, which matters because this runs on EVERY evaluation of every server: a store with
               nothing left to heal must cost zero writes, not rewrite the same rows and churn updated_at. */
            using (var again = new NpgsqlCommand(DarlingAlertReadAdapter.HealDatabaseStateBaselineToOnlineSql, connection))
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

    private static async Task StateAsync(
        NpgsqlConnection connection, CancellationToken ct, string database, string stateDesc, bool standby, DateTime at)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collect.database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby)
VALUES (0, $1, $2, $3, $4, 5, $5, $6)", connection);
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(LiveServerId);
        command.Parameters.AddWithValue(Name);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(stateDesc);
        command.Parameters.AddWithValue(standby);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Plants an expectation row. <paramref name="isOverride"/> is the load-bearing one for #2189: it is the
    /// only thing separating a baseline the machine INFERRED (heal-able) from one an operator DECLARED
    /// (never second-guessed), so a test that cannot set it cannot tell the two apart.
    /// </summary>
    private static async Task ExpectedAsync(
        NpgsqlConnection connection, CancellationToken ct, string database, string expected, string lastAlerted,
        bool isOverride = false)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO config.database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at, last_alerted_state, last_alerted_at)
VALUES ($1, $2, $3, $5, (now() AT TIME ZONE 'UTC'), $4, (now() AT TIME ZONE 'UTC'))", connection);
        command.Parameters.AddWithValue(LiveServerId);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(expected);
        command.Parameters.AddWithValue(lastAlerted);
        command.Parameters.AddWithValue(isOverride);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The stored expected state, or null when no row exists (a database still pending a baseline).</summary>
    private static async Task<string?> ExpectedStateAsync(NpgsqlConnection connection, CancellationToken ct, string database)
    {
        using var command = new NpgsqlCommand(
            "SELECT expected_state FROM config.database_state_expected WHERE server_id = $1 AND database_name = $2",
            connection);
        command.Parameters.AddWithValue(LiveServerId);
        command.Parameters.AddWithValue(database);
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull or null ? null : (string)value;
    }

    private static async Task<string?> MemoryAsync(NpgsqlConnection connection, CancellationToken ct, string database)
    {
        using var command = new NpgsqlCommand(
            "SELECT last_alerted_state FROM config.database_state_expected WHERE server_id = $1 AND database_name = $2",
            connection);
        command.Parameters.AddWithValue(LiveServerId);
        command.Parameters.AddWithValue(database);
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull or null ? null : (string)value;
    }

    private static async Task DeleteLiveRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var id = LiveServerId.ToString(CultureInfo.InvariantCulture);
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collect.database_states WHERE server_id = {id};" +
            $"DELETE FROM config.database_state_expected WHERE server_id = {id};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
