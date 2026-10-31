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
/// #7017: six more alert-pass latest-row reads start at a floor taken from the newest row they last returned (#6968's
/// <see cref="NewestRowFloor"/>). For each read: the bounded statement really is bounded (a floor past the row finds nothing),
/// the read after the first rides on the floor and returns the same rows, a newer row is found, rows purged under the floor fall
/// back to the unbounded read (which finds an older row), and a server with no rows finds its first row.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertReadFloorLiveTests
{
    /// <summary>Sentinel server ids, negative so they cannot collide with a registered server.</summary>
    private const int Pvs = -7011, Volume = -7012, States = -7013, Agent = -7014, Replica = -7015, DbReplica = -7016;

    private const int LiveTimeoutSeconds = 60;

    private static readonly string[] SentinelTables =
    {
        "pvs_stats", "database_size_stats", "database_states", "agent_status", "ag_replica_states", "ag_database_replica_states"
    };

    private static string Key(int serverId) => serverId.ToString(CultureInfo.InvariantCulture);

    /// <summary>Whole seconds, so the store's microseconds round-trip exactly.</summary>
    private static DateTime Now() => new(DateTime.UtcNow.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, DateTimeKind.Utc);

    private static DateTime Utc(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Utc);

    private static (DateTime? CollectionTimeUtc, bool? AgentRunning) Status(DateTime? time, bool? running) => (time, running);

    private static async Task<NpgsqlConnection?> OpenAsync(string? connectionString, CancellationToken ct)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live alert-read floor test.");
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSentinelsAsync(connection, ct);
        return connection;
    }

    private static async Task DeleteSentinelsAsync(NpgsqlConnection connection, CancellationToken ct, int? only = null)
    {
        var filter = only is { } one
            ? string.Create(CultureInfo.InvariantCulture, $"server_id = {one}")
            : string.Create(CultureInfo.InvariantCulture, $"server_id IN ({Pvs}, {Volume}, {States}, {Agent}, {Replica}, {DbReplica})");
        foreach (var table in SentinelTables)
        {
            if (only is { } id && !TableFor(id).Equals(table, StringComparison.Ordinal))
            {
                continue;
            }

            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE {filter};", connection) { CommandTimeout = LiveTimeoutSeconds };
            await cleanup.ExecuteNonQueryAsync(ct);
        }

        if (only is null)
        {
            using var expected = new NpgsqlCommand($"DELETE FROM config.database_state_expected WHERE {filter};", connection) { CommandTimeout = LiveTimeoutSeconds };
            await expected.ExecuteNonQueryAsync(ct);
        }
    }

    private static string TableFor(int serverId) => serverId switch
    {
        Pvs => "pvs_stats",
        Volume => "database_size_stats",
        States => "database_states",
        Agent => "agent_status",
        Replica => "ag_replica_states",
        _ => "ag_database_replica_states"
    };

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] values)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = LiveTimeoutSeconds };
        foreach (var value in values)
        {
            command.Parameters.AddWithValue(value is DateTime stamp ? DateTime.SpecifyKind(stamp, DateTimeKind.Unspecified) : value);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>How many rows a bounded statement returns for a floor (the statements' own $2).</summary>
    private static async Task<int> RowsAsync(NpgsqlConnection connection, string sql, int serverId, DateTime floor, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = LiveTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(floor, DateTimeKind.Unspecified) });
        using var reader = await command.ExecuteReaderAsync(ct);
        var rows = 0;
        while (await reader.ReadAsync(ct))
        {
            rows++;
        }

        return rows;
    }

    private static Task InsertPvsAsync(NpgsqlConnection connection, DateTime at, double pvsMb, CancellationToken ct) => ExecAsync(connection, @"
INSERT INTO pvs_stats (collection_id, collection_time, server_id, server_name, database_name, is_accelerated_database_recovery_on, persistent_version_store_size_mb, database_data_size_mb)
VALUES ($1, $2, $3, 'sentinel', 'AdrDb', true, $4, 100), ($1, $2, $3, 'sentinel', 'NoAdrDb', false, 1, 100)",
        ct, at.Ticks, at, Pvs, pvsMb);

    private static Task InsertVolumeAsync(NpgsqlConnection connection, DateTime at, double freeMb, CancellationToken ct) => ExecAsync(connection, @"
INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, volume_mount_point, volume_total_mb, volume_free_mb)
VALUES ($1, $2, $3, 'sentinel', 'C:\', 1000, $4)",
        ct, at.Ticks, at, Volume, freeMb);

    private static Task InsertStateAsync(NpgsqlConnection connection, DateTime at, string database, string state, CancellationToken ct) => ExecAsync(connection, @"
INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, state_desc, is_in_standby)
VALUES ($1, $2, $3, 'sentinel', $4, $5, false)",
        ct, at.Ticks, at, States, database, state);

    private static Task InsertAgentAsync(NpgsqlConnection connection, DateTime at, bool running, CancellationToken ct) => ExecAsync(connection, @"
INSERT INTO agent_status (collection_id, collection_time, server_id, server_name, agent_running)
VALUES ($1, $2, $3, 'sentinel', $4)",
        ct, at.Ticks, at, Agent, running);

    private static Task InsertReplicaAsync(NpgsqlConnection connection, DateTime at, string node, string role, CancellationToken ct) => ExecAsync(connection, @"
INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, connected_state_desc, is_local)
VALUES ($1, $2, $3, 'sentinel', 'AG1', $4, $5, 'CONNECTED', false)",
        ct, at.Ticks, at, Replica, node, role);

    private static Task InsertDbReplicaAsync(NpgsqlConnection connection, DateTime at, string database, long lagSeconds, CancellationToken ct) => ExecAsync(connection, @"
INSERT INTO ag_database_replica_states (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name, secondary_lag_seconds, is_suspended)
VALUES ($1, $2, $3, 'sentinel', 'AG1', $4, 'NODE2', $5, false)",
        ct, at.Ticks, at, DbReplica, database, lagSeconds);

    [Fact]
    public async Task PvsRead_RidesOnTheFloor_FindsNewerRows_AndFallsBackWhenTheFloorRowIsGone()
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        using var connection = (await OpenAsync(connectionString, ct))!;
        await using var dataSource = NpgsqlDataSource.Create(connectionString!);
        var adapter = new DarlingAlertReadAdapter(dataSource);
        var bodySucceeded = false;
        try
        {
            var now = Now();
            Assert.Empty(await adapter.GetPvsPressureAsync(Key(Pvs), ct));

            await InsertPvsAsync(connection, now.AddMinutes(-10), 50, ct);
            var first = Assert.Single(await adapter.GetPvsPressureAsync(Key(Pvs), ct));
            Assert.Equal("AdrDb", first.DatabaseName);
            Assert.Equal(Utc(now.AddMinutes(-10)), first.NewestSampleUtc);

            /* The bounded statement is bounded: a floor past the snapshot finds nothing, a floor at it finds it. */
            Assert.Equal(0, await RowsAsync(connection, DarlingAlertReadAdapter.PvsPressureSinceSql, Pvs, now.AddMinutes(-5), ct));
            Assert.Equal(1, await RowsAsync(connection, DarlingAlertReadAdapter.PvsPressureSinceSql, Pvs, now.AddMinutes(-10), ct));

            /* The second read rides on the floor and returns the same row. */
            var second = Assert.Single(await adapter.GetPvsPressureAsync(Key(Pvs), ct));
            Assert.Equal(first.NewestSampleUtc, second.NewestSampleUtc);
            Assert.Equal(50, second.PvsSizeMb);

            /* A newer snapshot is found, and only it. */
            await InsertPvsAsync(connection, now.AddMinutes(-1), 60, ct);
            var third = Assert.Single(await adapter.GetPvsPressureAsync(Key(Pvs), ct));
            Assert.Equal(Utc(now.AddMinutes(-1)), third.NewestSampleUtc);
            Assert.Equal(60, third.PvsSizeMb);

            /* The snapshot under the floor is purged and an older one remains: the bounded read finds nothing, so the read asks again
               without a bound and gets the older snapshot, as the unbounded read always did. */
            await DeleteSentinelsAsync(connection, ct, only: Pvs);
            await InsertPvsAsync(connection, now.AddMinutes(-40), 40, ct);
            var older = Assert.Single(await adapter.GetPvsPressureAsync(Key(Pvs), ct));
            Assert.Equal(Utc(now.AddMinutes(-40)), older.NewestSampleUtc);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (c, token) => DeleteSentinelsAsync(c, token));
        }
    }

    [Fact]
    public async Task VolumeRead_RidesOnTheFloor_FindsNewerRows_AndFallsBackWhenTheFloorRowIsGone()
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        using var connection = (await OpenAsync(connectionString, ct))!;
        await using var dataSource = NpgsqlDataSource.Create(connectionString!);
        var adapter = new DarlingAlertReadAdapter(dataSource);
        var bodySucceeded = false;
        try
        {
            var now = Now();
            Assert.Empty(await adapter.GetVolumeFreeSpaceAsync(Key(Volume), ct));

            await InsertVolumeAsync(connection, now.AddMinutes(-10), 400, ct);
            var first = Assert.Single(await adapter.GetVolumeFreeSpaceAsync(Key(Volume), ct));
            Assert.Equal(400, first.FreeMb);
            Assert.Equal(Utc(now.AddMinutes(-10)), first.NewestSampleUtc);

            Assert.Equal(0, await RowsAsync(connection, DarlingAlertReadAdapter.VolumeFreeSpaceSinceSql, Volume, now.AddMinutes(-5), ct));
            Assert.Equal(1, await RowsAsync(connection, DarlingAlertReadAdapter.VolumeFreeSpaceSinceSql, Volume, now.AddMinutes(-10), ct));

            var second = Assert.Single(await adapter.GetVolumeFreeSpaceAsync(Key(Volume), ct));
            Assert.Equal(first.NewestSampleUtc, second.NewestSampleUtc);
            Assert.Equal(400, second.FreeMb);

            await InsertVolumeAsync(connection, now.AddMinutes(-1), 300, ct);
            var third = Assert.Single(await adapter.GetVolumeFreeSpaceAsync(Key(Volume), ct));
            Assert.Equal(300, third.FreeMb);
            Assert.Equal(Utc(now.AddMinutes(-1)), third.NewestSampleUtc);

            await DeleteSentinelsAsync(connection, ct, only: Volume);
            await InsertVolumeAsync(connection, now.AddMinutes(-40), 200, ct);
            var older = Assert.Single(await adapter.GetVolumeFreeSpaceAsync(Key(Volume), ct));
            Assert.Equal(200, older.FreeMb);
            Assert.Equal(Utc(now.AddMinutes(-40)), older.NewestSampleUtc);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (c, token) => DeleteSentinelsAsync(c, token));
        }
    }

    /// <summary>
    /// The database state read judges the newest collection against the one before it, so its floor sits one collection back. The
    /// case the issue names: no new collection arrives between two ticks. A floor at the newest collection would hide the previous
    /// state and report nothing, so the second tick must return what the unbounded read returns.
    /// </summary>
    [Fact]
    public async Task DatabaseStateRead_KeepsThePreviousCollectionVisible_WhenNoNewRowArrivesBetweenTicks()
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        using var connection = (await OpenAsync(connectionString, ct))!;
        await using var dataSource = NpgsqlDataSource.Create(connectionString!);
        var adapter = new DarlingAlertReadAdapter(dataSource);
        var bodySucceeded = false;
        try
        {
            var now = Now();
            var t0 = now.AddMinutes(-10);
            var t1 = now.AddMinutes(-5);

            /* No collection at all: no deviation, and the next read finds the first rows. */
            Assert.Empty(await adapter.GetDatabaseStatesAsync(Key(States), ct));

            /* Two collections with one database in a critical state in both (a pending first observation) and one healthy. */
            foreach (var at in new[] { t0, t1 })
            {
                await InsertStateAsync(connection, at, "CritDb", "SUSPECT", ct);
                await InsertStateAsync(connection, at, "OkDb", "ONLINE", ct);
            }

            var first = Assert.Single(await adapter.GetDatabaseStatesAsync(Key(States), ct));
            Assert.Equal("CritDb", first.DatabaseName);
            Assert.Equal("SUSPECT", first.StateDesc);
            Assert.Equal(Utc(t1), first.NewestSampleUtc);

            /* Why the floor is one collection back: bounded at the newest collection the previous state is gone and the rule finds
               nothing, bounded at the previous one it still finds the deviation. */
            Assert.Equal(0, await DeviationRowsAsync(connection, t1, ct));
            Assert.Equal(1, await DeviationRowsAsync(connection, t0, ct));

            /* Second tick, no new collection: the same answer as the unbounded read. */
            var second = Assert.Single(await adapter.GetDatabaseStatesAsync(Key(States), ct));
            Assert.Equal("CritDb", second.DatabaseName);
            Assert.Equal(Utc(t1), second.NewestSampleUtc);
            var third = Assert.Single(await adapter.GetDatabaseStatesAsync(Key(States), ct));
            Assert.Equal(Utc(t1), third.NewestSampleUtc);

            /* A new collection arrives: the old newest becomes the previous one, and the deviation carries on at the new time. */
            var t2 = now.AddMinutes(-1);
            await InsertStateAsync(connection, t2, "CritDb", "SUSPECT", ct);
            await InsertStateAsync(connection, t2, "OkDb", "ONLINE", ct);
            var fourth = Assert.Single(await adapter.GetDatabaseStatesAsync(Key(States), ct));
            Assert.Equal(Utc(t2), fourth.NewestSampleUtc);

            /* The database recovers: one sample at a good state ends the deviation. */
            var t3 = now;
            await InsertStateAsync(connection, t3, "CritDb", "ONLINE", ct);
            await InsertStateAsync(connection, t3, "OkDb", "ONLINE", ct);
            Assert.Empty(await adapter.GetDatabaseStatesAsync(Key(States), ct));
            Assert.Empty(await adapter.GetDatabaseStatesAsync(Key(States), ct));

            /* The collections under the floor are purged and older ones remain: the bounded read finds nothing, so the read asks
               again without a bound and gets the deviation the unbounded read finds. */
            await DeleteSentinelsAsync(connection, ct, only: States);
            await ExecAsync(connection, "DELETE FROM config.database_state_expected WHERE server_id = $1", ct, States);
            foreach (var at in new[] { now.AddMinutes(-50), now.AddMinutes(-45) })
            {
                await InsertStateAsync(connection, at, "CritDb", "SUSPECT", ct);
            }

            var older = Assert.Single(await adapter.GetDatabaseStatesAsync(Key(States), ct));
            Assert.Equal("CritDb", older.DatabaseName);
            Assert.Equal(Utc(now.AddMinutes(-45)), older.NewestSampleUtc);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (c, token) => DeleteSentinelsAsync(c, token));
        }
    }

    /// <summary>The deviation rows (not the row that carries the times) the bounded statement returns for a floor.</summary>
    private static async Task<int> DeviationRowsAsync(NpgsqlConnection connection, DateTime floor, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(DarlingAlertReadAdapter.DatabaseStateDeviationsSinceSql, connection) { CommandTimeout = LiveTimeoutSeconds };
        command.Parameters.AddWithValue(States);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(floor, DateTimeKind.Unspecified) });
        using var reader = await command.ExecuteReaderAsync(ct);
        var rows = 0;
        while (await reader.ReadAsync(ct))
        {
            if (!reader.IsDBNull(0))
            {
                rows++;
            }
        }

        return rows;
    }

    [Fact]
    public async Task AgentStatusRead_RidesOnTheFloor_FindsNewerRows_AndFallsBackWhenTheFloorRowIsGone()
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        using var connection = (await OpenAsync(connectionString, ct))!;
        await using var dataSource = NpgsqlDataSource.Create(connectionString!);
        var evaluator = DarlingSelfAlertTests.LiveReadEvaluatorForTests();
        var bodySucceeded = false;
        try
        {
            var now = Now();
            Assert.Equal(Status(null, null), await evaluator.ReadLatestAgentStatusAsync(dataSource, Agent, ct));

            await InsertAgentAsync(connection, now.AddMinutes(-10), true, ct);
            Assert.Equal(Status(Utc(now.AddMinutes(-10)), true), await evaluator.ReadLatestAgentStatusAsync(dataSource, Agent, ct));

            Assert.Equal(0, await RowsAsync(connection, DarlingSelfAlertEvaluator.LatestAgentStatusSinceSql, Agent, now.AddMinutes(-5), ct));
            Assert.Equal(1, await RowsAsync(connection, DarlingSelfAlertEvaluator.LatestAgentStatusSinceSql, Agent, now.AddMinutes(-10), ct));

            Assert.Equal(Status(Utc(now.AddMinutes(-10)), true), await evaluator.ReadLatestAgentStatusAsync(dataSource, Agent, ct));

            await InsertAgentAsync(connection, now.AddMinutes(-1), false, ct);
            Assert.Equal(Status(Utc(now.AddMinutes(-1)), false), await evaluator.ReadLatestAgentStatusAsync(dataSource, Agent, ct));

            await DeleteSentinelsAsync(connection, ct, only: Agent);
            await InsertAgentAsync(connection, now.AddMinutes(-40), true, ct);
            Assert.Equal(Status(Utc(now.AddMinutes(-40)), true), await evaluator.ReadLatestAgentStatusAsync(dataSource, Agent, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (c, token) => DeleteSentinelsAsync(c, token));
        }
    }

    [Fact]
    public async Task AgReplicaRead_RidesOnTheFloor_FindsNewerSnapshots_AndFallsBackWhenTheFloorRowIsGone()
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        using var connection = (await OpenAsync(connectionString, ct))!;
        await using var dataSource = NpgsqlDataSource.Create(connectionString!);
        var evaluator = DarlingSelfAlertTests.LiveReadEvaluatorForTests();
        var bodySucceeded = false;
        try
        {
            var now = Now();
            var (emptyTime, emptyReplicas) = await evaluator.ReadLatestAgReplicaStatesAsync(dataSource, Replica, ct);
            Assert.Null(emptyTime);
            Assert.Empty(emptyReplicas);

            await InsertReplicaAsync(connection, now.AddMinutes(-10), "NODE1", "PRIMARY", ct);
            await InsertReplicaAsync(connection, now.AddMinutes(-10), "NODE2", "SECONDARY", ct);
            var (firstTime, firstReplicas) = await evaluator.ReadLatestAgReplicaStatesAsync(dataSource, Replica, ct);
            Assert.Equal(Utc(now.AddMinutes(-10)), firstTime);
            Assert.Equal(2, firstReplicas.Count);

            Assert.Equal(0, await RowsAsync(connection, DarlingSelfAlertEvaluator.LatestAgReplicaStatesSinceSql, Replica, now.AddMinutes(-5), ct));
            Assert.Equal(2, await RowsAsync(connection, DarlingSelfAlertEvaluator.LatestAgReplicaStatesSinceSql, Replica, now.AddMinutes(-10), ct));

            var (secondTime, secondReplicas) = await evaluator.ReadLatestAgReplicaStatesAsync(dataSource, Replica, ct);
            Assert.Equal(firstTime, secondTime);
            Assert.Equal(2, secondReplicas.Count);

            await InsertReplicaAsync(connection, now.AddMinutes(-1), "NODE1", "SECONDARY", ct);
            await InsertReplicaAsync(connection, now.AddMinutes(-1), "NODE2", "PRIMARY", ct);
            var (thirdTime, thirdReplicas) = await evaluator.ReadLatestAgReplicaStatesAsync(dataSource, Replica, ct);
            Assert.Equal(Utc(now.AddMinutes(-1)), thirdTime);
            Assert.Contains(thirdReplicas, r => r.ReplicaServerName == "NODE2" && r.RoleDesc == "PRIMARY");

            await DeleteSentinelsAsync(connection, ct, only: Replica);
            await InsertReplicaAsync(connection, now.AddMinutes(-40), "NODE1", "PRIMARY", ct);
            var (olderTime, olderReplicas) = await evaluator.ReadLatestAgReplicaStatesAsync(dataSource, Replica, ct);
            Assert.Equal(Utc(now.AddMinutes(-40)), olderTime);
            Assert.Single(olderReplicas);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (c, token) => DeleteSentinelsAsync(c, token));
        }
    }

    [Fact]
    public async Task AgDatabaseReplicaRead_RidesOnTheFloor_FindsNewerSnapshots_AndFallsBackWhenTheFloorRowIsGone()
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        using var connection = (await OpenAsync(connectionString, ct))!;
        await using var dataSource = NpgsqlDataSource.Create(connectionString!);
        var evaluator = DarlingSelfAlertTests.LiveReadEvaluatorForTests();
        var bodySucceeded = false;
        try
        {
            var now = Now();
            var (emptyTime, emptyDatabases) = await evaluator.ReadLatestAgDatabaseReplicaStatesAsync(dataSource, DbReplica, ct);
            Assert.Null(emptyTime);
            Assert.Empty(emptyDatabases);

            await InsertDbReplicaAsync(connection, now.AddMinutes(-10), "Sales", 10, ct);
            await InsertDbReplicaAsync(connection, now.AddMinutes(-10), "Orders", 20, ct);
            var (firstTime, firstDatabases) = await evaluator.ReadLatestAgDatabaseReplicaStatesAsync(dataSource, DbReplica, ct);
            Assert.Equal(Utc(now.AddMinutes(-10)), firstTime);
            Assert.Equal(2, firstDatabases.Count);

            Assert.Equal(0, await RowsAsync(connection, DarlingSelfAlertEvaluator.LatestAgDatabaseReplicaStatesSinceSql, DbReplica, now.AddMinutes(-5), ct));
            Assert.Equal(2, await RowsAsync(connection, DarlingSelfAlertEvaluator.LatestAgDatabaseReplicaStatesSinceSql, DbReplica, now.AddMinutes(-10), ct));

            var (secondTime, secondDatabases) = await evaluator.ReadLatestAgDatabaseReplicaStatesAsync(dataSource, DbReplica, ct);
            Assert.Equal(firstTime, secondTime);
            Assert.Equal(2, secondDatabases.Count);

            await InsertDbReplicaAsync(connection, now.AddMinutes(-1), "Sales", 900, ct);
            var (thirdTime, thirdDatabases) = await evaluator.ReadLatestAgDatabaseReplicaStatesAsync(dataSource, DbReplica, ct);
            Assert.Equal(Utc(now.AddMinutes(-1)), thirdTime);
            var sales = Assert.Single(thirdDatabases);
            Assert.Equal(900, sales.SecondaryLagSeconds);

            await DeleteSentinelsAsync(connection, ct, only: DbReplica);
            await InsertDbReplicaAsync(connection, now.AddMinutes(-40), "Sales", 5, ct);
            var (olderTime, olderDatabases) = await evaluator.ReadLatestAgDatabaseReplicaStatesAsync(dataSource, DbReplica, ct);
            Assert.Equal(Utc(now.AddMinutes(-40)), olderTime);
            Assert.Equal(5, Assert.Single(olderDatabases).SecondaryLagSeconds);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (c, token) => DeleteSentinelsAsync(c, token));
        }
    }
}
