/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>pg_index_bloat</c>'s rotation cursors are RETIRED when their database stops being enumerated
/// (#3153) — and the assertions are about rows REMOVED, never about the prune having run.
///
/// <para><b>Why removal and not execution.</b> The registry's own rule is that a prefix pruned under the
/// wrong <c>collector_name</c> "silently deletes nothing, which is indistinguishable from having nothing to
/// prune". A pin that asserted the statement executed would pass over exactly that, and over the sharper
/// version of it here: the query_store prune anti-joins <c>collect.database_states</c>, which is
/// <c>sys.databases</c> from a SQL Server collector and therefore EMPTY for a PostgreSQL
/// <c>server_id</c>, so wiring this prefix into that statement would have deleted nothing forever while
/// looking like a fix. <see cref="TheWrongOwnerDeletesNothing_WhichIsWhyRemovalIsWhatIsAsserted"/> is the
/// falsifier for that: it runs the real prune under a name nothing wrote and asserts the rows survive, so
/// "rows disappeared" cannot be satisfied by a prune that deletes indiscriminately either.</para>
///
/// <para><b>Why this is not merely untidy.</b> <c>BuildQuery</c> is called ONCE per cycle, outside the
/// per-database loop, and the host reuses that statement for every live database — so an unpruned cursor
/// list costs <c>O(databases ever seen) x O(live databases)</c> in text and bound parameters on every
/// cycle, permanently. At three parameters per cursor that ends at PostgreSQL's 65,535-parameter limit,
/// where the statement throws before it is sent and the cycle fails for every database.
/// <see cref="PgIndexBloatCollector.MaxSplicedCursors"/> is the graceful floor under that; this prune is
/// what stops the count growing in the first place.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgRotationCursorPruneLivePostgresTests
{
    /// <summary>NEGATIVE, per the convention every live class here follows.</summary>
    private const int ServerId = -993_154;

    private const string ServerName = "pg-rotation-prune-probe";

    private static string Key(string database) =>
        PgIndexBloatCollector.RotationCursorKeyPrefix + database;

    [Fact]
    public async Task ACursorWhoseDatabaseIsGone_IsRemoved_AndTheLiveOnesSurvive()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the rotation-cursor prune test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

            await SeedAsync(runner, ct, "live_one", "dead_one", "live_two", "dead_two");

            /* The sweep enumerated two of the four. The other two are the databases that went away. */
            var pruned = await runner.PrunePgPerDatabaseStateAsync(
                ServerId, PgIndexBloatCollector.Instance.Name, new[] { "live_one", "live_two" }, ct);

            /* Removal, named: the keys the prune reports are the keys it deleted. */
            Assert.Equal(
                new[] { Key("dead_one"), Key("dead_two") },
                pruned.OrderBy(k => k, StringComparer.Ordinal).ToArray());

            /* And the store agrees — the report and the table cannot disagree about what is gone. */
            Assert.Equal(
                new[] { Key("live_one"), Key("live_two") },
                await StoredKeysAsync(runner, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /// <summary>
    /// An empty enumeration removes NOTHING. That is not a tidiness choice: an empty database list is how a
    /// login that cannot read <c>pg_database</c> presents, and every cursor on the server would look
    /// orphaned against it. Refused by the caller AND by the statement, so a future caller that forgets the
    /// check gets a no-op rather than a wipe.
    /// </summary>
    [Fact]
    public async Task AnEmptyEnumerationRemovesNothing_BecauseItIsAPermissionsFailureNotAnEmptyServer()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the rotation-cursor prune test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

            await SeedAsync(runner, ct, "live_one", "live_two");

            var pruned = await runner.PrunePgPerDatabaseStateAsync(
                ServerId, PgIndexBloatCollector.Instance.Name, Array.Empty<string>(), ct);

            Assert.Empty(pruned);
            Assert.Equal(new[] { Key("live_one"), Key("live_two") }, await StoredKeysAsync(runner, ct));

            /* The statement's own guard, exercised directly rather than through the caller that already
               refuses: this is the arm that has to hold if a future caller stops checking. */
            await using var direct = new NpgsqlCommand(
                DarlingCollectorRunner.PrunePgPerDatabaseStateKeysSql, connection);
            direct.Parameters.AddWithValue(ServerId);
            direct.Parameters.AddWithValue(PgIndexBloatCollector.Instance.Name);
            direct.Parameters.AddWithValue(PgIndexBloatCollector.RotationCursorKeyPrefix);
            direct.Parameters.AddWithValue(Array.Empty<string>());

            await using (var reader = await direct.ExecuteReaderAsync(ct))
            {
                Assert.False(await reader.ReadAsync(ct), "the statement deleted a row on an empty database list");
            }

            Assert.Equal(new[] { Key("live_one"), Key("live_two") }, await StoredKeysAsync(runner, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /// <summary>
    /// The prune under a <c>collector_name</c> nothing wrote deletes NOTHING — the failure the registry
    /// warns about, made visible. This is what makes the removal assertions above meaningful rather than
    /// satisfiable by a delete that is simply too broad.
    /// </summary>
    [Fact]
    public async Task TheWrongOwnerDeletesNothing_WhichIsWhyRemovalIsWhatIsAsserted()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the rotation-cursor prune test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

            await SeedAsync(runner, ct, "dead_one");

            /* The statement, under the wrong owner, with a database list that makes the row an orphan. */
            await using var direct = new NpgsqlCommand(
                DarlingCollectorRunner.PrunePgPerDatabaseStateKeysSql, connection);
            direct.Parameters.AddWithValue(ServerId);
            direct.Parameters.AddWithValue("query_store");
            direct.Parameters.AddWithValue(PgIndexBloatCollector.RotationCursorKeyPrefix);
            direct.Parameters.AddWithValue(new[] { "live_one" });

            await using (var reader = await direct.ExecuteReaderAsync(ct))
            {
                Assert.False(await reader.ReadAsync(ct), "a prune under the wrong owner deleted a row");
            }

            /* The orphan is still there. A pin that asserted "the prune ran" would be green right now. */
            Assert.Equal(new[] { Key("dead_one") }, await StoredKeysAsync(runner, ct));

            /* Under the right owner the same row goes, which is what proves the survival above is about the
               OWNER and not about the row being unreachable for some other reason. */
            var pruned = await runner.PrunePgPerDatabaseStateAsync(
                ServerId, PgIndexBloatCollector.Instance.Name, new[] { "live_one" }, ct);

            Assert.Equal(new[] { Key("dead_one") }, pruned.ToArray());
            Assert.Empty(await StoredKeysAsync(runner, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /// <summary>
    /// Seeds one cursor per database through the runner's own writer, so the rows land under exactly the
    /// <c>collector_name</c> production writes them under — a hand-rolled INSERT could agree with a prune
    /// that looks for the wrong owner.
    /// </summary>
    private static async Task SeedAsync(
        DarlingCollectorRunner runner, CancellationToken ct, params string[] databases)
    {
        var state = new Dictionary<string, string>(StringComparer.Ordinal);

        for (var i = 0; i < databases.Length; i++)
        {
            state[Key(databases[i])] = PgIndexBloatCollector.FormatCursor(16_384 + i, 20_000 + i);
        }

        await runner.SaveCollectorStateAsync(
            ServerId, PgIndexBloatCollector.Instance.Name, state, ct);
    }

    /// <summary>The rotation keys actually in the store, ordered, read back through the runner's loader.</summary>
    private static async Task<string[]> StoredKeysAsync(DarlingCollectorRunner runner, CancellationToken ct)
    {
        var loaded = await runner.GetCollectorStateAsync(
            ServerId, PgIndexBloatCollector.Instance.Name, ct);

        return loaded.Keys
            .Where(k => k.StartsWith(PgIndexBloatCollector.RotationCursorKeyPrefix, StringComparison.Ordinal))
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToArray();
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var delete = new NpgsqlCommand(
            "DELETE FROM collect.collector_state WHERE server_id = $1", connection);
        delete.Parameters.AddWithValue(ServerId);
        await delete.ExecuteNonQueryAsync(ct);
    }
}
