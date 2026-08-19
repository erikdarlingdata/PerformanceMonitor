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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #2188 prune against a REAL store, driving
/// <see cref="DarlingCollectorRunner.PruneOrphanedQueryStoreDatabaseStateAsync"/> — the production method,
/// not a re-implementation. Split from the pure <see cref="QueryStoreStatePruneTests"/> per the shape
/// <c>LivePostgresCollectionHygieneTests</c> asks for, so the source and policy pins do not serialize behind
/// the shared store.
///
/// <para>The statement's whole risk lives in how PostgreSQL evaluates two guards and an anti-join together —
/// a NULL-valued aggregate over an empty snapshot, a timestamp comparison, and a correlated NOT EXISTS — and
/// no source pin can speak to any of it.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStoreStatePruneLivePostgresTests
{
    /// <summary>Distinctive fake ids — a real server_id is a storage-name hash, never these.</summary>
    private const int LiveServerId = -218800;
    private const int NeighborServerId = -218801;
    private const string ServerName = "PLANWM-PRUNE-SRV";

    /// <summary>The snapshot's collection_time in every case below; state rows are dated relative to it.</summary>
    private static readonly DateTime Newest = new(2026, 8, 11, 9, 0, 0, DateTimeKind.Unspecified);

    /// <summary>Old enough to be judged by <see cref="Newest"/> — the ordinary case for a real state row.</summary>
    private static readonly DateTime BeforeNewest = Newest.AddHours(-1);

    private static string Planwm(string database) => QueryStorePlanXmlState.WatermarkKeyPrefix + database;
    private static string Done(string database) => QueryStoreBackfillState.DoneKeyPrefix + database;
    private static string Hole(string database) => QueryStoreBackfillState.HoleKeyPrefix + database;

    private static string EncodedHole() => QueryStoreBackfillState.EncodeHole(
        new DateTime(2026, 8, 10, 0, 0, 0, DateTimeKind.Utc),
        new DateTime(2026, 8, 10, 6, 0, 0, DateTimeKind.Utc));

    /// <summary>
    /// One live pass over every case that separates a correct prune from a destructive one.
    /// </summary>
    [Fact]
    public async Task Prune_RetiresOnlyDroppedDatabases_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live query_store state prune test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteLiveRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var logger = new CapturingTestLogger();
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator(), logger);

        var bodySucceeded = false;
        try
        {
            /* The snapshot: what sys.databases still holds. "Parked" is the case the whole design turns on —
               a database that EXISTS but that query_store's enumeration would never return (it screens
               state_desc = ONLINE), so a prune keyed on the enumeration deletes it and a prune keyed on
               sys.databases keeps it.

               "App" is present and "AppArchive" is not, which is the name-shape trap: writing the anti-join
               as starts_with(state_key, prefix || ds.database_name) instead of an equality is a very
               plausible variant, and it would spare planwm:AppArchive forever because "planwm:App" is a
               prefix of it. For an issue whose subject is database name churn, that case has to be here. */
            await SnapshotAsync(connection, ct, Newest, "Live", "Parked", "App");

            /* An OLDER snapshot still naming the dropped databases. If the prune read any snapshot but the
               newest, nothing would ever be retired. */
            await SnapshotAsync(connection, ct, Newest.AddMinutes(-15), "Live", "Parked", "App", "Dropped", "AppArchive");

            await StateAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("Live"), "900000:1786449600");
            await StateAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("Parked"), "800000:1786449600");
            await StateAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("Dropped"), "700000:1786449600");
            await StateAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("App"), "500000:1786449600");
            await StateAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("AppArchive"), "400000:1786449600");

            /* The backfill worker's per-database keys, which orphan identically. Both prefixes get a
               survivor as well as a casualty: with only a delete case, a statement that deleted
               unconditionally would still pass. */
            await StateAsync(connection, ct, LiveServerId, QueryStoreBackfillState.StateCollectorName, Done("Live"), "2026-08-11T09:00:00.0000000Z");
            await StateAsync(connection, ct, LiveServerId, QueryStoreBackfillState.StateCollectorName, Done("Dropped"), "2026-08-11T09:00:00.0000000Z");
            await StateAsync(connection, ct, LiveServerId, QueryStoreBackfillState.StateCollectorName, Hole("Live"), EncodedHole());
            await StateAsync(connection, ct, LiveServerId, QueryStoreBackfillState.StateCollectorName, Hole("Dropped"), EncodedHole());

            /* A key under the SAME owner that is not database-keyed. The prefix filter is what protects it;
               a prune written as "every key of this collector" would take it. */
            await StateAsync(connection, ct, LiveServerId, QueryStoreBackfillState.StateCollectorName, "unrelated-bookkeeping", "keep me");

            /* Another collector's state entirely, and a NEIGHBOUR SERVER whose database really was dropped
               here — server scoping is the difference between pruning one server and pruning the fleet. */
            await StateAsync(connection, ct, LiveServerId, DefaultTraceEventsCollector.Instance.Name,
                DefaultTraceEventsCollector.LastTraceFilePathStateKey, @"S:\MSSQL\Log\log_766.trc");
            await StateAsync(connection, ct, NeighborServerId, QueryStorePlanXmlState.StateCollectorName,
                Planwm("Dropped"), "600000:1786449600");

            await runner.PruneOrphanedQueryStoreDatabaseStateAsync(LiveServerId, ct);

            /* Retired: gone from the newest snapshot, on every prefix it could have left behind. */
            Assert.Null(await ValueAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("Dropped")));
            Assert.Null(await ValueAsync(connection, ct, LiveServerId, QueryStoreBackfillState.StateCollectorName, Done("Dropped")));
            Assert.Null(await ValueAsync(connection, ct, LiveServerId, QueryStoreBackfillState.StateCollectorName, Hole("Dropped")));

            /* Retired even though a LIVE database's name is a prefix of it. */
            Assert.Null(await ValueAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("AppArchive")));

            /* Kept: still collected. */
            Assert.Equal("900000:1786449600",
                await ValueAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("Live")));
            Assert.Equal("500000:1786449600",
                await ValueAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("App")));
            Assert.Equal("2026-08-11T09:00:00.0000000Z",
                await ValueAsync(connection, ct, LiveServerId, QueryStoreBackfillState.StateCollectorName, Done("Live")));
            Assert.Equal(EncodedHole(),
                await ValueAsync(connection, ct, LiveServerId, QueryStoreBackfillState.StateCollectorName, Hole("Live")));

            /* Kept, and this is the assertion the change exists for: present in sys.databases, absent from
               every enumeration query_store runs. Pruning it costs a full plan-XML refetch of a database that
               never went anywhere, on precisely the servers that keep databases parked. */
            Assert.Equal("800000:1786449600",
                await ValueAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("Parked")));

            /* Kept: not database-keyed, not this collector, not this server. */
            Assert.Equal("keep me",
                await ValueAsync(connection, ct, LiveServerId, QueryStoreBackfillState.StateCollectorName, "unrelated-bookkeeping"));
            Assert.Equal(@"S:\MSSQL\Log\log_766.trc",
                await ValueAsync(connection, ct, LiveServerId, DefaultTraceEventsCollector.Instance.Name,
                    DefaultTraceEventsCollector.LastTraceFilePathStateKey));
            Assert.Equal("600000:1786449600",
                await ValueAsync(connection, ct, NeighborServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("Dropped")));

            /* The DIAGNOSTIC, which is the only thing that could ever make a wrong delete visible — the other
               symptom is a silent refetch. It comes from the statement's RETURNING clause, so if that ever
               stopped yielding rows the deletes would still happen and the log would simply go quiet: no
               assertion on the store's contents can see that, which is why it is asserted on the log. */
            Assert.Contains("Dropped", logger.Joined, StringComparison.Ordinal);
            Assert.Contains("AppArchive", logger.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain("Parked", logger.Joined, StringComparison.Ordinal);

            /* Idempotent — it runs on every query_store cycle of every server, so a second pass over a clean
               store must touch nothing. Seven survivors: planwm for Live, Parked and App; done and hole for
               Live; the non-database-keyed bookkeeping row; and the other collector's key. */
            await runner.PruneOrphanedQueryStoreDatabaseStateAsync(LiveServerId, ct);
            Assert.Equal(7, await CountAsync(connection, ct, LiveServerId));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteLiveRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The freshness guard, which is the difference between correct and merely usually-correct.
    ///
    /// <para>A snapshot that EXISTS is not a snapshot that is CURRENT. If database_states stops collecting
    /// for a server, its newest snapshot freezes, and every database created after that instant is missing
    /// from it while being perfectly alive. Pruning on presence alone would delete such a database's
    /// watermark on every cycle forever — paying a full plan-XML refetch each time, which is the exact cost
    /// #2164 exists to remove, while logging that a live database is gone. A snapshot cannot judge a row
    /// written after it was taken.</para>
    /// </summary>
    [Fact]
    public async Task Prune_LeavesStateWrittenAfterTheSnapshot_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live prune freshness test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteLiveRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        var bodySucceeded = false;
        try
        {
            /* A frozen snapshot: database_states stopped collecting at Newest and names only OldDb. */
            await SnapshotAsync(connection, ct, Newest, "OldDb");

            /* Created after the snapshot froze — absent from it, and entirely alive. */
            await StateAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName,
                Planwm("BornAfterTheSnapshot"), "10:1786449600", updatedAt: Newest.AddMinutes(30));

            /* Dropped before the snapshot froze: absent from it, and its last state write PRECEDES it, which
               is what still makes it prunable. Without this the test would pass for a prune that had simply
               stopped working. */
            await StateAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName,
                Planwm("DroppedLongAgo"), "20:1786449600", updatedAt: BeforeNewest);

            await runner.PruneOrphanedQueryStoreDatabaseStateAsync(LiveServerId, ct);

            Assert.Equal("10:1786449600",
                await ValueAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName,
                    Planwm("BornAfterTheSnapshot")));
            Assert.Null(await ValueAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName,
                Planwm("DroppedLongAgo")));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteLiveRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The empty-snapshot guard, isolated. A server with NO database_states snapshot must lose nothing —
    /// this is the case that turns a hygiene sweep into a fleet-wide data event, and ordinary configurations
    /// reach it: Azure SQL DB never collects database_states at all
    /// (<c>DatabaseStateCollector.AppliesTo</c>), and a server whose rows have aged out of the raw retention
    /// tier looks identical from here.
    /// </summary>
    [Fact]
    public async Task Prune_WithNoDatabaseSnapshot_RetiresNothing_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live prune guard test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteLiveRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        var bodySucceeded = false;
        try
        {
            /* No snapshot for THIS server. A neighbour's snapshot exists and names none of these databases,
               so a prune that forgot to scope the snapshot read by server would wipe every row here. */
            await SnapshotAsync(connection, ct, Newest, NeighborServerId, "SomeOtherServersDatabase");

            await StateAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("Alpha"), "1:1786449600");
            await StateAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("Beta"), "2:1786449600");

            await runner.PruneOrphanedQueryStoreDatabaseStateAsync(LiveServerId, ct);

            Assert.Equal("1:1786449600",
                await ValueAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("Alpha")));
            Assert.Equal("2:1786449600",
                await ValueAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("Beta")));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteLiveRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The race the issue asks about, driven in the order that would lose data if the write-back were not an
    /// upsert: a cycle loads state, the prune deletes that key underneath it, and the cycle then persists
    /// what it observed. The row must come back.
    ///
    /// <para>The prune cannot actually target a live database — its predicate is absence from the newest
    /// sys.databases snapshot — so this drives the adversarial case DELIBERATELY, by pruning while the name
    /// is missing from the snapshot. That is what makes the consequence a measured fact instead of an
    /// argument: even a prune that fires on a database it should not have costs one refetch, never a
    /// row.</para>
    /// </summary>
    [Fact]
    public async Task Prune_RacingAnInFlightCycle_CannotLoseTheWatermark_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live prune race test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteLiveRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        var bodySucceeded = false;
        try
        {
            /* A snapshot that does NOT name Racer, and a state row old enough to be judged by it — the
               adversarial setup, since neither is true of a real live database. */
            await SnapshotAsync(connection, ct, Newest, "Live");
            await StateAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName,
                Planwm("Racer"), "900000:1786449600", updatedAt: BeforeNewest);

            /* Cycle start: the collection pass reads its state. */
            var loaded = await runner.GetCollectorStateAsync(LiveServerId, QueryStorePlanXmlState.StateCollectorName, ct);
            Assert.Equal("900000:1786449600", Assert.Contains(Planwm("Racer"), loaded));

            /* Mid-flight: the prune fires and takes the row this cycle is still working from. */
            await runner.PruneOrphanedQueryStoreDatabaseStateAsync(LiveServerId, ct);
            Assert.Null(await ValueAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("Racer")));

            /* Cycle end: the write-back is an INSERT ... ON CONFLICT, so it restores rather than failing on
               a row that is no longer there. The database keeps collecting; the delete cost nothing. */
            await runner.SaveCollectorStateAsync(
                LiveServerId, QueryStorePlanXmlState.StateCollectorName,
                new Dictionary<string, string>(StringComparer.Ordinal) { [Planwm("Racer")] = "950000:1786449600" },
                ct);

            Assert.Equal("950000:1786449600",
                await ValueAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("Racer")));

            /* And it stays: the restored row is stamped NOW, which is after the snapshot, so the freshness
               guard keeps the next cycle's prune off it too. Without that the two would fight forever. */
            await runner.PruneOrphanedQueryStoreDatabaseStateAsync(LiveServerId, ct);
            Assert.Equal("950000:1786449600",
                await ValueAsync(connection, ct, LiveServerId, QueryStorePlanXmlState.StateCollectorName, Planwm("Racer")));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteLiveRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ---------------- helpers ---------------- */

    private static Task SnapshotAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, params string[] databases)
        => SnapshotAsync(connection, ct, at, LiveServerId, databases);

    private static async Task SnapshotAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, int serverId, params string[] databases)
    {
        foreach (var database in databases)
        {
            /* state_desc is deliberately never read by the prune — existence is the only question it asks —
               and "Parked" carrying OFFLINE is what makes that testable: the assertion that it survives
               fails the moment anyone adds a state filter to the anti-join. */
            using var command = new NpgsqlCommand(@"
INSERT INTO collect.database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby)
VALUES (0, $1, $2, $3, $4, 5, $5, false)", connection);
            command.Parameters.AddWithValue(at);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(database);
            command.Parameters.AddWithValue(string.Equals(database, "Parked", StringComparison.Ordinal) ? "OFFLINE" : "ONLINE");
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task StateAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string owner, string key, string value,
        DateTime? updatedAt = null)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collect.collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(owner);
        command.Parameters.AddWithValue(key);
        command.Parameters.AddWithValue(value);
        command.Parameters.AddWithValue(updatedAt ?? BeforeNewest);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ValueAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string owner, string key)
    {
        using var command = new NpgsqlCommand(
            "SELECT state_value FROM collect.collector_state WHERE server_id = $1 AND collector_name = $2 AND state_key = $3",
            connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(owner);
        command.Parameters.AddWithValue(key);
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull or null ? null : (string)value;
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, CancellationToken ct, int serverId)
    {
        using var command = new NpgsqlCommand(
            "SELECT COUNT(*) FROM collect.collector_state WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(serverId);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task DeleteLiveRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var live = LiveServerId.ToString(CultureInfo.InvariantCulture);
        var neighbor = NeighborServerId.ToString(CultureInfo.InvariantCulture);
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collect.collector_state WHERE server_id IN ({live}, {neighbor});" +
            $"DELETE FROM collect.database_states WHERE server_id IN ({live}, {neighbor});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
