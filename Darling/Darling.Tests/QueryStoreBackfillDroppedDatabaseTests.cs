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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5483: a database dropped from the monitored server must stop being a backfill candidate.
///
/// <para>What a user saw: after four temporary databases were dropped, every backfill tick still tried each one
/// and logged "database does not exist", for as long as the store held rows for it. The cause is not the
/// <c>collector_state</c> keys. A failed slice writes no state, and the hourly orphan prune retires the
/// <c>done:</c> and <c>hole:</c> keys of a dropped database (pinned below). The candidate list is read from the
/// COLLECTED ROWS in <c>query_store_stats</c> (plus any database a hole names), so a database that shipped rows
/// inside the horizon is a candidate until those rows age out, whatever the state table says. The fix reads the
/// newest <c>database_states</c> snapshot: a candidate absent from it, with no stored row at or after the
/// snapshot, is gone.</para>
///
/// <para><b>#1776 own-store</b> — mints its own scratch database (via <see cref="ScratchPostgres"/>). The slice
/// body is replaced through <see cref="QueryStoreBackfill.SliceOverrideForTests"/> and throws what the server
/// throws for a missing database, so no SQL Server is needed; the tick loop, the candidate read, the stored-floor
/// read and the real orphan prune are the production ones.</para>
/// </summary>
public sealed class QueryStoreBackfillDroppedDatabaseTests
{
    private const int TestServerId = -548301;
    private const string Kept = "aaa_kept_db";
    private const string Dropped = "bbb_dropped_db";
    private const string Created = "ccc_created_after_snapshot_db";
    private const string MissingMessage = "Cannot open database requested by the login. The login failed. (database does not exist)";

    private static DateTime Ago(TimeSpan span) => DateTime.SpecifyKind(DateTime.UtcNow - span, DateTimeKind.Unspecified);

    [Fact]
    public async Task ADroppedDatabase_StopsBeingTried_OnceTheNewestSnapshotLeavesItOut_AndItsStateIsPruned()
    {
        await using var fixture = await DroppedBackfillStore.CreateAsync();
        var ct = TestContext.Current.CancellationToken;

        /* Two databases with a young stored history, so each has a pending tail slice. The dropped one also carries
           a hole key, written 20 minutes ago. */
        await fixture.SeedRowAsync(Kept, TimeSpan.FromHours(1), ct);
        await fixture.SeedRowAsync(Dropped, TimeSpan.FromHours(1), ct);
        await fixture.SeedStateAsync(Dropped, TimeSpan.FromMinutes(20), ct);

        /* The first snapshot names both: the drop has not happened yet, so the dropped database is still worked. */
        await fixture.SeedSnapshotAsync(TimeSpan.FromMinutes(30), [Kept, Dropped], ct);
        await fixture.RunTicksAsync(4, ct);
        Assert.Contains(Dropped, fixture.Attempts);

        /* The drop: the newest snapshot (5 minutes ago, slower than the 5-minute backfill tick) no longer names it. */
        await fixture.SeedSnapshotAsync(TimeSpan.FromMinutes(5), [Kept], ct);
        fixture.Attempts.Clear();

        Assert.True(await fixture.Runner.PruneOrphanedQueryStoreDatabaseStateAsync(TestServerId, ct));
        Assert.Null(await fixture.HoleAsync(Dropped, ct));

        await fixture.RunTicksAsync(6, ct);

        Assert.DoesNotContain(Dropped, fixture.Attempts);
        Assert.Null(await fixture.HoleAsync(Dropped, ct));
    }

    [Fact]
    public async Task ADroppedDatabaseWhoseHistoryReachesTheHorizon_IsNotMarkedDoneAgain_AfterThePruneRetiredItsMarker()
    {
        await using var fixture = await DroppedBackfillStore.CreateAsync();
        var ct = TestContext.Current.CancellationToken;

        /* A row inside the horizon makes the dropped database a candidate; a row past it makes the tick mark it done
           without a slice. Before the fix the hourly prune deleted that marker and the next tick wrote it back, so
           the database was never retired. */
        await fixture.SeedRowAsync(Kept, TimeSpan.FromHours(1), ct);
        await fixture.SeedRowAsync(Dropped, TimeSpan.FromHours(1), ct);
        await fixture.SeedRowAsync(Dropped, QueryStoreBackfill.PlainStoreHorizon + TimeSpan.FromDays(1), ct);
        await fixture.SeedSnapshotAsync(TimeSpan.FromMinutes(30), [Kept, Dropped], ct);
        await fixture.RunTicksAsync(4, ct);
        Assert.NotNull(await fixture.StateAsync(QueryStoreBackfillState.DoneKeyPrefix, Dropped, ct));

        /* A snapshot a second ahead of the marker's own write, so the prune judges it (a marker is stamped "now"). */
        await fixture.SeedSnapshotAsync(TimeSpan.FromSeconds(-1), [Kept], ct);
        Assert.True(await fixture.Runner.PruneOrphanedQueryStoreDatabaseStateAsync(TestServerId, ct));
        Assert.Null(await fixture.StateAsync(QueryStoreBackfillState.DoneKeyPrefix, Dropped, ct));

        await fixture.RunTicksAsync(4, ct);

        Assert.Null(await fixture.StateAsync(QueryStoreBackfillState.DoneKeyPrefix, Dropped, ct));
    }

    [Fact]
    public void AbsentFromSnapshot_ReturnsOnlyTheCandidatesTheSnapshotDoesNotName_ComparedOrdinally()
    {
        var snapshot = new HashSet<string>(StringComparer.Ordinal) { "App", "Live" };

        Assert.Equal(
            ["AppArchive", "app"],
            QueryStoreBackfillState.AbsentFromSnapshot(["App", "AppArchive", "Live", "app"], snapshot));
        Assert.Empty(QueryStoreBackfillState.AbsentFromSnapshot([], snapshot));
    }

    [Fact]
    public async Task ADatabaseWithRowsNewerThanTheSnapshot_IsStillTried()
    {
        await using var fixture = await DroppedBackfillStore.CreateAsync();
        var ct = TestContext.Current.CancellationToken;

        /* Created after the newest snapshot: absent from it, but its rows are newer than it. Dropping it would be
           the very mistake the prune's own freshness guard exists to avoid. */
        await fixture.SeedRowAsync(Kept, TimeSpan.FromHours(1), ct);
        await fixture.SeedRowAsync(Created, TimeSpan.FromMinutes(2), ct);
        await fixture.SeedSnapshotAsync(TimeSpan.FromMinutes(10), [Kept], ct);

        await fixture.RunTicksAsync(4, ct);

        Assert.Contains(Created, fixture.Attempts);
    }

    [Fact]
    public async Task WithNoSnapshotAtAll_NothingIsDropped()
    {
        await using var fixture = await DroppedBackfillStore.CreateAsync();
        var ct = TestContext.Current.CancellationToken;

        /* No database_states row for this server (Azure SQL DB never collects one; so does a server that has not
           collected yet): the list is not known, so every candidate stays. */
        await fixture.SeedRowAsync(Kept, TimeSpan.FromHours(1), ct);
        await fixture.SeedRowAsync(Dropped, TimeSpan.FromHours(1), ct);

        await fixture.RunTicksAsync(4, ct);

        Assert.Contains(Kept, fixture.Attempts);
        Assert.Contains(Dropped, fixture.Attempts);
    }

    /// <summary>A scratch store, a real runner and backfill, and a slice body that completes for every database
    /// except <see cref="Dropped"/>, which throws what a missing database throws.</summary>
    private sealed class DroppedBackfillStore : IAsyncDisposable
    {
        private ScratchPostgres _scratch = null!;
        private NpgsqlDataSource _postgres = null!;
        private QueryStoreBackfill _backfill = null!;
        private ServerRuntime _server = null!;
        private long _collectionId = 548300;

        public DarlingCollectorRunner Runner { get; private set; } = null!;

        public List<string> Attempts { get; } = [];

        public static async Task<DroppedBackfillStore> CreateAsync()
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live dropped-database backfill tests (they mint their own scratch database).");

            var ct = TestContext.Current.CancellationToken;
            var fixture = new DroppedBackfillStore
            {
                _scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct),
            };
            await using (var connection = new NpgsqlConnection(fixture._scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
            }

            fixture._postgres = NpgsqlDataSource.Create(fixture._scratch.ConnectionString);
            fixture.Runner = new DarlingCollectorRunner(fixture._postgres, new CollectorDeltaCalculator());
            fixture._backfill = new QueryStoreBackfill(fixture._postgres, fixture.Runner, new CollectorDeltaCalculator(), null);
            fixture._backfill.SliceOverrideForTests = async (database, span) =>
            {
                fixture.Attempts.Add(database);
                if (string.Equals(database, Dropped, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(MissingMessage);
                }

                await fixture.Runner.SaveCollectorStateAsync(
                    TestServerId,
                    QueryStoreBackfill.StateCollectorName,
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        [QueryStoreBackfillState.DoneKeyPrefix + database] = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                    },
                    TestContext.Current.CancellationToken);
            };
            fixture._server = new ServerRuntime
            {
                Config = new MonitoredServer { Name = "backfill-dropped-test", Host = "backfill-dropped-test" },
                ConnectionString = "Server=backfill-dropped-test",
                Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
                StorageName = "backfill-dropped-test",
                ServerId = TestServerId,
                EngineEdition = 3,
            };
            return fixture;
        }

        public async Task RunTicksAsync(int ticks, CancellationToken ct)
        {
            for (var tick = 0; tick < ticks; tick++)
            {
                try
                {
                    await _backfill.RunServerSliceAsync(_server, ct);
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("database does not exist", StringComparison.Ordinal))
                {
                    /* The worker's outer catch logs a failed slice and carries on to the next tick. */
                }
            }
        }

        /// <summary>One stored row <paramref name="age"/> old whose last_execution_time is an hour older: inside the
        /// horizon, so the database is a candidate, and newer than the floor, so a tail slice is pending.</summary>
        public async Task SeedRowAsync(string databaseName, TimeSpan age, CancellationToken ct)
        {
            const string sql = @"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, min_duration_us, max_duration_us)
VALUES
    ($1, $2, $3, 'SQL01', $4, 'dbo.GetOrders', '0xABCD', 91, 111, 'Regular', 'Primary',
     1, $2, $5, $5, 1, 100, 100, 100, 100)";

            await using var connection = await _postgres.OpenConnectionAsync(ct);
            await using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.AddWithValue(++_collectionId);
            command.Parameters.AddWithValue(Ago(age));
            command.Parameters.AddWithValue(TestServerId);
            command.Parameters.AddWithValue(databaseName);
            command.Parameters.AddWithValue(Ago(age + TimeSpan.FromHours(1)));
            await command.ExecuteNonQueryAsync(ct);
        }

        /// <summary>A hole key for <paramref name="databaseName"/>, its updated_at set explicitly so it can sit
        /// before a snapshot (a state save stamps "now", which no past snapshot could judge).</summary>
        public async Task SeedStateAsync(string databaseName, TimeSpan age, CancellationToken ct)
        {
            await using var connection = await _postgres.OpenConnectionAsync(ct);
            await using var command = new NpgsqlCommand(@"
INSERT INTO collect.collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)", connection);
            command.Parameters.AddWithValue(TestServerId);
            command.Parameters.AddWithValue(QueryStoreBackfill.StateCollectorName);
            command.Parameters.AddWithValue(QueryStoreBackfillState.HoleKeyPrefix + databaseName);
            command.Parameters.AddWithValue(QueryStoreBackfillState.EncodeHole(
                DateTime.UtcNow.AddDays(-2), DateTime.UtcNow.AddHours(-5)));
            command.Parameters.AddWithValue(Ago(age));
            await command.ExecuteNonQueryAsync(ct);
        }

        public async Task SeedSnapshotAsync(TimeSpan age, string[] databases, CancellationToken ct)
        {
            await using var connection = await _postgres.OpenConnectionAsync(ct);
            foreach (var database in databases)
            {
                await using var command = new NpgsqlCommand(@"
INSERT INTO collect.database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby)
VALUES (0, $1, $2, 'SQL01', $3, 5, 'ONLINE', false)", connection);
                command.Parameters.AddWithValue(Ago(age));
                command.Parameters.AddWithValue(TestServerId);
                command.Parameters.AddWithValue(database);
                await command.ExecuteNonQueryAsync(ct);
            }
        }

        public Task<string?> HoleAsync(string databaseName, CancellationToken ct)
            => StateAsync(QueryStoreBackfillState.HoleKeyPrefix, databaseName, ct);

        public async Task<string?> StateAsync(string prefix, string databaseName, CancellationToken ct)
        {
            await using var connection = await _postgres.OpenConnectionAsync(ct);
            await using var command = new NpgsqlCommand(
                "SELECT state_value FROM collect.collector_state WHERE server_id = $1 AND collector_name = $2 AND state_key = $3", connection);
            command.Parameters.AddWithValue(TestServerId);
            command.Parameters.AddWithValue(QueryStoreBackfill.StateCollectorName);
            command.Parameters.AddWithValue(prefix + databaseName);
            return await command.ExecuteScalarAsync(ct) as string;
        }

        public async ValueTask DisposeAsync()
        {
            await _postgres.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}
