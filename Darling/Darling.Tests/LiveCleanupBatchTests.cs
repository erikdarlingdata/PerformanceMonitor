/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The seam tests for <see cref="LiveCleanupBatch"/> (#1873) — the helper the live suite's teardowns now go
/// through instead of a swallow.
///
/// <para><b>Why these run against a real store.</b> The behaviour under test is entirely about what the
/// PostgreSQL catalog says after a statement ran, and every one of the bugs in the shape it replaces was
/// invisible to anything that did not ask a real database: a probe that never matches always answers "gone",
/// and a swallow always answers "fine". A fake connection would reproduce both faults perfectly and pass.</para>
///
/// <para>The negative cases construct the batch with <c>publishResidue: false</c>. They fail a removal on
/// purpose, and a deliberate failure filed to the run-wide ledger would fail every run at collection
/// teardown — the coverage for the alarm would trip the alarm.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class LiveCleanupBatchTests
{
    private const string SkipReason =
        "Set DARLING_TEST_PG to a Postgres connection string to run the live cleanup-batch seam tests.";

    private const string Probe = "cleanup_batch_1873_probe";

    /// <summary>
    /// The ordinary path: the object is there, the statement removes it, the probe agrees, nothing is
    /// recorded. Establishes that a clean removal does NOT manufacture residue — without this, a helper that
    /// reported residue unconditionally would pass every negative test below.
    /// </summary>
    [Fact]
    public async Task RemovingSomethingThatGoes_LeavesNoResidue_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;

        await using var connection = await OpenAsync(connectionString!, ct);

        using (var create = new NpgsqlCommand(
            $"CREATE TABLE IF NOT EXISTS collect.{Probe} (id integer)", connection))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        var batch = new LiveCleanupBatch(connection);
        await batch.DropTableAsync(Probe, ct);

        Assert.Empty(batch.Residue);
        Assert.False(await ExistsAsync(connection, Probe, ct),
            $"collect.{Probe} should have been dropped by the batch.");
    }

    /// <summary>
    /// The whole point: a removal that does not remove is REPORTED, rather than reported as success.
    ///
    /// <para>The statement here succeeds and changes nothing, which is the precise shape of the bug — a
    /// <c>DROP</c> that lost its race raises an error, but a helper that only watched for errors would also
    /// have to be right about which errors count. This one watches the object.</para>
    /// </summary>
    [Fact]
    public async Task AStatementThatDoesNotRemoveTheObject_IsRecordedAsResidue_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;

        await using var connection = await OpenAsync(connectionString!, ct);

        using (var create = new NpgsqlCommand(
            $"CREATE TABLE IF NOT EXISTS collect.{Probe} (id integer)", connection))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        var bodySucceeded = false;
        try
        {
            var batch = new LiveCleanupBatch(connection, publishResidue: false, maxAttempts: 2);

            await batch.RemoveAsync(
                $"table collect.{Probe}",
                "SELECT 1" /* succeeds, removes nothing */,
                "SELECT EXISTS (SELECT 1 FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace "
                + $"WHERE n.nspname = 'collect' AND c.relname = '{Probe}')",
                ct);

            var entry = Assert.Single(batch.Residue);
            Assert.Contains($"table collect.{Probe}", entry, StringComparison.Ordinal);
            Assert.Contains("the removal statement reported success but the object is still there", entry,
                StringComparison.Ordinal);

            /* The attribution half — the catalog knows WHAT survived, and only this knows WHOSE cleanup
               could not remove it. */
            Assert.Contains(nameof(AStatementThatDoesNotRemoveTheObject_IsRecordedAsResidue_AgainstDevPostgres),
                entry, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            /* Through LiveStoreCleanup like every other live teardown (#1902). This one already opened its own
               connection with CancellationToken.None, so both halves were right by hand — but "right by hand"
               is exactly what the ratchet cannot tell apart from wrong, and an exemption carved for a correct
               site is an exemption a later incorrect site inherits. Going through the helper costs nothing and
               lets the count reach zero honestly. */
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).DropTableAsync(Probe, cleanupCt));
        }
    }

    /// <summary>
    /// A removal that THROWS but leaves the object gone is a success, not a fault.
    ///
    /// <para>This is not a corner case invented for the test — it is the pre-test call in
    /// <c>DarlingSecuritySplitLiveTests</c>, where <c>DROP OWNED BY</c> (which has no <c>IF EXISTS</c> form)
    /// raises <c>42704</c> on every run because the roles have not been created yet. Judging the postcondition
    /// rather than the exception is what lets that stay quiet while a role that genuinely survives does not.
    /// </para>
    /// </summary>
    [Fact]
    public async Task AThrowingRemovalWhoseObjectIsAlreadyGone_IsNotResidue_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;

        await using var connection = await OpenAsync(connectionString!, ct);

        var batch = new LiveCleanupBatch(connection, publishResidue: false, maxAttempts: 2);

        /* 42P01 every time: the relation does not exist, which is exactly why nothing needs removing. */
        await batch.RemoveAsync(
            $"table collect.{Probe}",
            $"DROP TABLE collect.{Probe}_never_created",
            "SELECT EXISTS (SELECT 1 FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace "
            + $"WHERE n.nspname = 'collect' AND c.relname = '{Probe}_never_created')",
            ct);

        Assert.Empty(batch.Residue);
    }

    /// <summary>
    /// A failing statement does not poison the statements after it.
    ///
    /// <para>The swallow this replaces existed partly for this: one broken statement must not cascade into
    /// every cleanup statement behind it, leaving renames and snapshots stranded (#1794's shape). The batch
    /// reopens the pooled session, so a session-breaking failure costs one object's removal, not the rest.</para>
    /// </summary>
    [Fact]
    public async Task AFailedRemoval_DoesNotStrandTheRemovalsAfterIt_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;

        await using var connection = await OpenAsync(connectionString!, ct);

        using (var create = new NpgsqlCommand(
            $"CREATE TABLE IF NOT EXISTS collect.{Probe} (id integer)", connection))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        var batch = new LiveCleanupBatch(connection, publishResidue: false, maxAttempts: 2);

        await batch.RemoveAsync(
            "a deliberately unremovable thing",
            "SELECT 1",
            "SELECT true",
            ct);

        /* The real removal, queued behind the failure. */
        await batch.DropTableAsync(Probe, ct);

        Assert.Single(batch.Residue);
        Assert.False(await ExistsAsync(connection, Probe, ct),
            $"collect.{Probe} should still have been dropped after an earlier removal failed.");
    }

    /// <summary>
    /// A three-level aggregate hierarchy is dropped whole, even when the ROOT is offered first.
    ///
    /// <para>This is the second #1873 mechanism, and the residue check found it in the field rather than
    /// reasoning predicting it — on the second run against a reused database, naming
    /// <c>query_store_stats_interval_hourly</c> and the test that stranded it.</para>
    ///
    /// <para><b>Three levels, not two, and that is the whole point.</b> <c>CASCADE</c> handles a two-level
    /// hierarchy by itself: dropping an hourly whose daily has no dependents of its own takes both, so a test
    /// built that way passes with or without the fix — verified by mutation, which is how this test came to be
    /// three levels deep. The real shape is <c>query_store_stats_interval_hourly</c> → <c>interval_daily</c> →
    /// <c>day_grain_daily</c> (#1849 and #1869), and there TimescaleDB refuses to cascade THROUGH the middle
    /// aggregate because it has a dependent: <c>2BP01 cannot drop view ... because other objects depend on
    /// it</c>. Retrying the root cannot ever succeed, because what has to change is a different relation - so
    /// the helper sweeps the whole set per round, and the leaf goes in the round the root fails.</para>
    /// </summary>
    [Fact]
    public async Task AThreeLevelAggregateHierarchy_IsDroppedWhole_EvenWhenTheRootIsOfferedFirst_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        const string Raw = "cagg_1873_raw";
        const string Hourly = "cagg_1873_raw_hourly";
        const string Daily = "cagg_1873_raw_daily";
        const string DayGrain = "cagg_1873_raw_day_grain";

        await using var connection = await OpenAsync(connectionString!, ct);

        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection,
                $"CREATE TABLE IF NOT EXISTS collect.{Raw} (collection_time timestamp NOT NULL, server_id integer NOT NULL)", ct);
            await ExecAsync(connection, TimescaleSupport.CreateHypertableSql($"collect.{Raw}", "collection_time"), ct);

            await ExecAsync(connection, $@"
CREATE MATERIALIZED VIEW collect.{Hourly}
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket(INTERVAL '1 hour', collection_time) AS bucket, server_id, count(*) AS samples
FROM collect.{Raw}
GROUP BY 1, 2
WITH NO DATA", ct);

            await ExecAsync(connection, $@"
CREATE MATERIALIZED VIEW collect.{Daily}
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket(INTERVAL '1 day', bucket) AS bucket, server_id, sum(samples) AS samples
FROM collect.{Hourly}
GROUP BY 1, 2
WITH NO DATA", ct);

            await ExecAsync(connection, $@"
CREATE MATERIALIZED VIEW collect.{DayGrain}
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket(INTERVAL '1 day', bucket) AS bucket, server_id, sum(samples) AS samples
FROM collect.{Daily}
GROUP BY 1, 2
WITH NO DATA", ct);

            var batch = new LiveCleanupBatch(connection);

            /* Root FIRST — the order in which no amount of retrying one aggregate can succeed. */
            await batch.DropContinuousAggregatesAsync([Hourly, Daily, DayGrain], ct);

            Assert.Empty(batch.Residue);
            Assert.False(await AggregateExistsAsync(connection, Hourly, ct), $"collect.{Hourly} should be gone.");
            Assert.False(await AggregateExistsAsync(connection, Daily, ct), $"collect.{Daily} should be gone.");
            Assert.False(await AggregateExistsAsync(connection, DayGrain, ct), $"collect.{DayGrain} should be gone.");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.DropContinuousAggregatesAsync([DayGrain, Daily, Hourly], cleanupCt);
                await batch.DropTableAsync(Raw, cleanupCt);
            });
        }
    }

    /// <summary>
    /// Opens a store connection and SETS the search path, never inherits it — the same rule
    /// <see cref="LiveStoreCleanup"/> documents, and for the same reason. Npgsql pools physical sessions, and
    /// the path a pooled one carries depends on when it was first opened. It bites here because
    /// <see cref="TimescaleSupport.CreateHypertableSql"/> calls <c>by_range(...)</c> UNQUALIFIED and the
    /// TimescaleDB helpers live in <c>public</c>: a session whose path omits it dies
    /// <c>42883 function by_range(unknown, interval) does not exist</c>, naming the inner function because it
    /// is an argument to <c>create_hypertable</c> and resolves first. It passed locally and failed on CI,
    /// because a connection string that pins its own <c>SearchPath</c> hides exactly this.
    /// </summary>
    private static async Task<NpgsqlConnection> OpenAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        using var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection);
        await setPath.ExecuteNonQueryAsync(ct);

        return connection;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<bool> AggregateExistsAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM timescaledb_information.continuous_aggregates "
            + $"WHERE view_schema = 'collect' AND view_name = '{view}')", connection);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>
    /// <see cref="LiveCleanupBatch.RemoveRolesAsync"/> takes a role that owns an object and holds privileges out of the
    /// cluster, and a role it cannot drop is reported by name, in the ledger entry and in the thrown exception (#5654).
    /// The blocker here is a privilege in ANOTHER database (<c>postgres</c>, which every cluster has), which the cleanup's own database cannot revoke.
    /// </summary>
    [Fact]
    public async Task RemovingRoles_TakesAnOwnerAway_AndNamesTheRoleItCannotDrop_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var goes = "rolegoes_" + suffix;
        var stays = "rolestays_" + suffix;
        const string OtherDatabase = "postgres";
        var bodySucceeded = false;

        await using var connection = await OpenAsync(connectionString!, ct);
        try
        {
            /* The first role owns a table and holds a privilege in this database: both must go. */
            await ExecAsync(connection, $"CREATE ROLE {goes} NOLOGIN; GRANT CREATE ON SCHEMA collect TO {goes}", ct);
            await ExecAsync(connection, $"CREATE TABLE collect.owned_{suffix} (id integer); ALTER TABLE collect.owned_{suffix} OWNER TO {goes}", ct);
            var batch = new LiveCleanupBatch(connection, publishResidue: false, maxAttempts: 2);
            await batch.RemoveRolesAsync([goes], suffix, ct);
            Assert.Empty(batch.Residue);
            await ExecAsync(connection, $"DROP TABLE collect.owned_{suffix}", ct);

            /* The second holds a privilege inside another database, where this connection's DROP OWNED cannot reach. */
            await ExecAsync(connection, $"CREATE ROLE {stays} NOLOGIN", ct);
            var other = new NpgsqlConnectionStringBuilder(connectionString!) { Database = OtherDatabase, Pooling = false }.ConnectionString;
            await using (var inOther = await OpenAsync(other, ct))
            {
                await ExecAsync(inOther, $"GRANT CREATE ON SCHEMA public TO {stays}", ct);
            }

            var blocked = new LiveCleanupBatch(connection, publishResidue: false, maxAttempts: 2);
            var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => blocked.RemoveRolesAsync([stays], suffix, ct));
            Assert.Contains(stays, failure.Message, StringComparison.Ordinal);
            Assert.Contains(stays, Assert.Single(blocked.Residue), StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var other = new NpgsqlConnectionStringBuilder(connectionString!) { Database = OtherDatabase, Pooling = false }.ConnectionString;
                await using (var inOther = await OpenAsync(other, cleanupCt))
                {
                    try
                    {
                        await ExecAsync(inOther, $"DROP OWNED BY {stays}", cleanupCt);
                    }
                    catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedObject)
                    {
                        /* The body never made the role. */
                    }
                }

                await ExecAsync(cleanup, $"DROP TABLE IF EXISTS collect.owned_{suffix}", cleanupCt);
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync([goes, stays], suffix, cleanupCt);
            });
        }
    }

    private static async Task<bool> ExistsAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace "
            + $"WHERE n.nspname = 'collect' AND c.relname = '{table}')", connection);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }
}
