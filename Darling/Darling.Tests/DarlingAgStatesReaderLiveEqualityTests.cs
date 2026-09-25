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
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4228: the live proof beside <see cref="DarlingAgStatesReaderTests"/>'s string-level pins. This class seeds
/// ten server shapes (current, stopped 2 days ago, stopped 2 hours ago, disabled-with-current-rows, and a
/// server removed from <c>config_monitored_servers</c> but still present in <c>servers</c>) across several
/// daily chunks, then proves two things against a real store: (1) every entry point the move touches — the
/// viewer's AG tab, <c>/api/ag</c>, <c>/api/ag/count</c> and <c>get_ag_health</c>, with and without a server
/// filter — returns exactly the rows the pre-#4228 unbounded statements returned; and (2) the floor-bounded
/// step two plans meaningfully fewer chunks than the unbounded oracle for both the shared-floor batch and a
/// stale server's own point read.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingAgStatesReaderLiveEqualityTests
{
    private const string AgName = "AG4228";

    private static readonly string[] CurrentNames =
    {
        "ag4228-cur-1", "ag4228-cur-2", "ag4228-cur-3", "ag4228-cur-4", "ag4228-cur-5", "ag4228-cur-6",
    };

    private const string StaleName = "ag4228-stale";
    private const string InsideHorizonName = "ag4228-inside-horizon";
    private const string DisabledName = "ag4228-disabled";
    private const string RemovedName = "ag4228-removed";

    private static readonly string[] AllNames = CurrentNames
        .Concat(new[] { StaleName, InsideHorizonName, DisabledName, RemovedName })
        .ToArray();

    private static int Id(string name) => ServerIdHelper.GetDeterministicHashCode(name);

    /* ── the pre-#4228 shapes, kept here as the row-correctness oracle (bare table names — equivalent to the
       Service's shipped collect.-qualified text under this store's search_path, like DarlingAgStatesReader's
       own doc explains) ── */

    private const string OldReplicaStatesSql = """
        SELECT
            r.server_id, r.server_name, r.collection_time, r.ag_name, r.replica_server_name, r.role_desc,
            r.is_local, r.operational_state_desc, r.connected_state_desc, r.recovery_health_desc,
            r.synchronization_health_desc, r.availability_mode_desc, r.failover_mode_desc, r.endpoint_url
        FROM ag_replica_states AS r
        JOIN
        (
            SELECT server_id, MAX(collection_time) AS max_collection_time
            FROM ag_replica_states
            WHERE ($1::integer IS NULL OR server_id = $1)
            GROUP BY server_id
        ) AS latest
            ON r.server_id = latest.server_id
            AND r.collection_time = latest.max_collection_time
        JOIN servers AS s
            ON s.server_id = r.server_id
            AND s.is_enabled
        WHERE ($1::integer IS NULL OR r.server_id = $1)
        ORDER BY r.server_name, r.ag_name, r.replica_server_name
        """;

    private const string OldDatabaseReplicaStatesSql = """
        SELECT
            d.server_id, d.server_name, d.collection_time, d.ag_name, d.database_name, d.replica_server_name,
            d.is_local, d.synchronization_state_desc, d.last_hardened_lsn, d.last_commit_lsn,
            d.log_send_queue_size, d.redo_queue_size, d.log_send_rate, d.redo_rate, d.is_suspended,
            d.suspend_reason_desc, d.availability_mode_desc, d.secondary_lag_seconds
        FROM ag_database_replica_states AS d
        JOIN
        (
            SELECT server_id, MAX(collection_time) AS max_collection_time
            FROM ag_database_replica_states
            WHERE ($1::integer IS NULL OR server_id = $1)
            GROUP BY server_id
        ) AS latest
            ON d.server_id = latest.server_id
            AND d.collection_time = latest.max_collection_time
        JOIN servers AS s
            ON s.server_id = d.server_id
            AND s.is_enabled
        WHERE ($1::integer IS NULL OR d.server_id = $1)
        ORDER BY d.server_name, d.ag_name, d.database_name, d.replica_server_name
        """;

    private const string OldAvailabilityGroupCountSql = """
        SELECT COUNT(DISTINCT (r.server_id, UPPER(COALESCE(r.ag_name, ''))))
        FROM ag_replica_states AS r
        JOIN
        (
            SELECT server_id, MAX(collection_time) AS max_collection_time
            FROM ag_replica_states
            WHERE ($1::integer IS NULL OR server_id = $1)
            GROUP BY server_id
        ) AS latest
            ON r.server_id = latest.server_id
            AND r.collection_time = latest.max_collection_time
        JOIN servers AS s
            ON s.server_id = r.server_id
            AND s.is_enabled
        WHERE ($1::integer IS NULL OR r.server_id = $1)
        """;

    /* ── seed statements: $1 collection_id, $2 collection_time, $3 server_id, $4 server_name, $5 ag_name ── */

    private const string ReplicaNode1Sql = """
        INSERT INTO ag_replica_states
            (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc,
             operational_state_desc, connected_state_desc, recovery_health_desc, synchronization_health_desc,
             availability_mode_desc, failover_mode_desc, endpoint_url)
        VALUES ($1, $2, $3, $4, $5, 'NODE1', 'PRIMARY', 'ONLINE', 'CONNECTED', 'ONLINE', 'HEALTHY',
                'SYNCHRONOUS_COMMIT', 'AUTOMATIC', 'TCP://NODE1:5022')
        """;

    private const string ReplicaNode2Sql = """
        INSERT INTO ag_replica_states
            (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc,
             operational_state_desc, connected_state_desc, recovery_health_desc, synchronization_health_desc,
             availability_mode_desc, failover_mode_desc, endpoint_url)
        VALUES ($1, $2, $3, $4, $5, 'NODE2', 'SECONDARY', NULL, 'CONNECTED', 'ONLINE', 'HEALTHY',
                'SYNCHRONOUS_COMMIT', 'AUTOMATIC', 'TCP://NODE2:5022')
        """;

    private const string DatabaseDb1Sql = """
        INSERT INTO ag_database_replica_states
            (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name,
             is_local, synchronization_state_desc, last_hardened_lsn, last_commit_lsn, log_send_queue_size,
             redo_queue_size, log_send_rate, redo_rate, is_suspended, suspend_reason_desc, availability_mode_desc,
             secondary_lag_seconds)
        VALUES ($1, $2, $3, $4, $5, 'DB1', 'NODE2', false, 'SYNCHRONIZED', '0x01', '0x02', 500, 0, 100, 0, false,
                NULL, 'SYNCHRONOUS_COMMIT', 1)
        """;

    private const string DatabaseDb2Sql = """
        INSERT INTO ag_database_replica_states
            (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name,
             is_local, synchronization_state_desc, last_hardened_lsn, last_commit_lsn, log_send_queue_size,
             redo_queue_size, log_send_rate, redo_rate, is_suspended, suspend_reason_desc, availability_mode_desc,
             secondary_lag_seconds)
        VALUES ($1, $2, $3, $4, $5, 'DB2', 'NODE2', false, 'SYNCHRONIZED', '0x03', '0x04', 200, 0, 100, 0, false,
                NULL, 'SYNCHRONOUS_COMMIT', 2)
        """;

    [Fact]
    public async Task AgStatesReader_MatchesTheOldUnboundedStatements_AcrossEveryServerShape_AgainstDevPostgres()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #4228 AG read-equality test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSentinelRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            var now = Micro(DateTime.UtcNow);
            await SeedAsync(connection, now, ct);

            var sentinelIds = new HashSet<int>(AllNames.Select(Id));
            var disabledId = Id(DisabledName);
            var staleId = Id(StaleName);
            var insideHorizonId = Id(InsideHorizonName);
            var removedId = Id(RemovedName);
            var cur1Id = Id(CurrentNames[0]);

            /* Unfiltered, scoped to this test's own servers: one equality assertion over all nine visible
               shapes' rows proves, in one shot, that disabled exclusion, stale/inside-horizon/removed
               inclusion at their true instants, and the tie at each server's newest snapshot all still match
               what the unbounded oracle returns — a wrong row anywhere breaks set equality. */
            var oldReplica = Scoped(await ReadOracleRowsAsync(connection, OldReplicaStatesSql, null, ct), sentinelIds);
            var newReplicaTyped = await DarlingAgStatesReader.GetReplicaStatesAsync(postgres, null, now, ct);
            var newReplica = newReplicaTyped.Where(r => sentinelIds.Contains(r.ServerId))
                .Select(RenderReplica).OrderBy(s => s, StringComparer.Ordinal).ToList();
            Assert.Equal(oldReplica, newReplica);
            Assert.Equal(18, newReplica.Count);

            var oldDatabase = Scoped(await ReadOracleRowsAsync(connection, OldDatabaseReplicaStatesSql, null, ct), sentinelIds);
            var newDatabaseTyped = await DarlingAgStatesReader.GetDatabaseReplicaStatesAsync(postgres, null, now, ct);
            var newDatabase = newDatabaseTyped.Where(r => sentinelIds.Contains(r.ServerId))
                .Select(RenderDatabase).OrderBy(s => s, StringComparer.Ordinal).ToList();
            Assert.Equal(oldDatabase, newDatabase);
            Assert.Equal(18, newDatabase.Count);

            /* The tie: both replicas of cur-1's newest snapshot share the identical collection_time — the
               normal shape of every AG snapshot, not an edge case. collection_time is NOT NULL on both AG
               tables (schema-enforced, V34), so a tie is the only ordering edge case reachable here. */
            var cur1Replicas = newReplicaTyped.Where(r => r.ServerId == cur1Id).ToList();
            Assert.Equal(2, cur1Replicas.Count);
            Assert.Equal(cur1Replicas[0].CollectionTime, cur1Replicas[1].CollectionTime);
            Assert.Equal(now, cur1Replicas[0].CollectionTime);

            /* Stale: its own read returns its own true newest instant (2 days old), not the shared floor's. */
            var staleReplicas = newReplicaTyped.Where(r => r.ServerId == staleId).ToList();
            Assert.Equal(2, staleReplicas.Count);
            Assert.Equal(now.AddDays(-2), staleReplicas[0].CollectionTime);

            /* The serverIdFilter path end to end, for the shapes most likely to break it. */
            foreach (var (_, id) in new[] { (StaleName, staleId), (InsideHorizonName, insideHorizonId), (RemovedName, removedId) })
            {
                var oldRows = (await ReadOracleRowsAsync(connection, OldReplicaStatesSql, id, ct))
                    .Select(r => r.Rendered).OrderBy(s => s, StringComparer.Ordinal).ToList();
                var newRows = (await DarlingAgStatesReader.GetReplicaStatesAsync(postgres, id, now, ct))
                    .Select(RenderReplica).OrderBy(s => s, StringComparer.Ordinal).ToList();
                Assert.Equal(oldRows, newRows);
                Assert.Equal(2, newRows.Count);
            }

            /* Disabled, filtered directly to it: zero rows even though its rows are the newest in the table —
               the output-side is_enabled join excludes it exactly as today. */
            var disabledOld = await ReadOracleRowsAsync(connection, OldReplicaStatesSql, disabledId, ct);
            var disabledNew = await DarlingAgStatesReader.GetReplicaStatesAsync(postgres, disabledId, now, ct);
            Assert.Empty(disabledOld);
            Assert.Empty(disabledNew);

            /* Group count (backs GetAvailabilityGroupCountAsync / /api/ag/count): one AG per non-disabled
               server, zero for disabled. */
            foreach (var name in AllNames)
            {
                var id = Id(name);
                var expected = name == DisabledName ? 0 : 1;
                Assert.Equal(expected, await ReadOracleScalarAsync(connection, OldAvailabilityGroupCountSql, id, ct));
                Assert.Equal(expected, await DarlingAgStatesReader.GetReplicaGroupCountAsync(postgres, id, now, ct));
            }

            /* Entry points: the viewer read, /api/ag, /api/ag/count and get_ag_health, with and without a
               server filter — wiring coverage on top of the row-level proof above, not a second oracle. */
            await using (var viewer = new ViewerDataService(cs!))
            {
                var cards = (await viewer.GetAvailabilityGroupsAsync(ct)).Where(c => sentinelIds.Contains(c.ServerId)).ToList();
                Assert.Equal(9, cards.Count);
                Assert.DoesNotContain(cards, c => c.ServerId == disabledId);
                var staleCard = Assert.Single(cards, c => c.ServerId == staleId);
                Assert.Equal(now.AddDays(-2), staleCard.CollectionTime);
                Assert.Equal(2, staleCard.Replicas.Count);
                var removedCard = Assert.Single(cards, c => c.ServerId == removedId);
                Assert.Equal(2, removedCard.Databases.Count);
            }

            var fleetHealth = await DarlingAgReader.GetAgHealthAsync(postgres, null, now, ct);
            var fleetGroups = fleetHealth.AvailabilityGroups.Where(g => sentinelIds.Contains(g.ServerId)).ToList();
            Assert.Equal(9, fleetGroups.Count);
            Assert.DoesNotContain(fleetGroups, g => g.ServerId == disabledId);

            var fleetCount = await DarlingAgReader.GetAvailabilityGroupCountAsync(postgres, null, ct);
            Assert.Equal(fleetHealth.AvailabilityGroupCount, fleetCount);

            var scopedHealth = await DarlingAgReader.GetAgHealthAsync(postgres, staleId, now, ct);
            Assert.Equal(StaleName, Assert.Single(scopedHealth.AvailabilityGroups).ServerName);

            var fleetJson = await DarlingMcpAgTools.GetAgHealth(postgres, null);
            Assert.False(McpHelpers.IsErrorEnvelope(fleetJson), $"tool returned an error: {fleetJson}");
            using (var doc = JsonDocument.Parse(fleetJson))
            {
                var groups = doc.RootElement.GetProperty("availability_groups").EnumerateArray()
                    .Where(g => sentinelIds.Contains(g.GetProperty("server_id").GetInt32())).ToList();
                Assert.Equal(9, groups.Count);
            }

            var namedJson = await DarlingMcpAgTools.GetAgHealth(postgres, CurrentNames[0]);
            Assert.False(McpHelpers.IsErrorEnvelope(namedJson), $"tool returned an error: {namedJson}");
            using (var doc = JsonDocument.Parse(namedJson))
            {
                var group = Assert.Single(doc.RootElement.GetProperty("availability_groups").EnumerateArray());
                Assert.Equal(CurrentNames[0], group.GetProperty("server_name").GetString());
                Assert.Equal(2, group.GetProperty("replicas").GetArrayLength());
                Assert.Equal(2, group.GetProperty("databases").GetArrayLength());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteSentinelRowsAsync);
        }
    }

    [Fact]
    public async Task AgStatesReader_StepTwoStaysBounded_ForCurrentAndStaleServers_AgainstDevPostgres()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #4228 AG plan test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var timescale = await PrepareHypertablesAsync(cs!, connection, ct);
        Assert.SkipUnless(timescale, "TimescaleDB is not available on this store: there are no chunks to exclude.");

        await DeleteSentinelRowsAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            var now = Micro(DateTime.UtcNow);
            await SeedAsync(connection, now, ct);
            var sentinelIds = new HashSet<int>(AllNames.Select(Id));

            /* The real step-one + split, exactly as GetReplicaStatesAsync runs it (scoped to this test's own
               servers so the floor used below is not perturbed by whatever else the shared store holds). */
            var instants = (await ReadInstantsAsync(connection, DarlingAgStatesReader.ReplicaNewestInstantSql, ct))
                .Where(i => sentinelIds.Contains(i.ServerId)).ToList();
            var (floor, stale) = DarlingAgStatesReader.SplitCurrentAndStale(instants, now);
            Assert.NotNull(floor);
            var staleServer = stale.Single(s => s.ServerId == Id(StaleName));

            /* Shared floor vs the unbounded oracle. Relative reduction, not an absolute ceiling: darlingtest
               is shared with the rest of the suite, and an unrelated class's own near-"now" seed can add a
               chunk this seed did not create (FleetReadsAreBoundedByTheFleetTests and
               EventWindowedReadsAreBoundedLivePostgresTests hit the same thing first, same fix). Confirmed
               once by hand against a freshly created database — see the PR body's measured table — that the
               floored plan touches at most 2 chunks for this exact seed when every server is current. */
            var oldPlan = await ExplainAsync(connection, "EXPLAIN (COSTS OFF) " + OldReplicaStatesSql, new[] { ServerFilterParam(null) }, ct);
            var newPlan = await ExplainAsync(connection, "EXPLAIN (COSTS OFF) " + DarlingAgStatesReader.ReplicaStatesSql,
                new[] { TimestampParam(floor!.Value), ServerFilterParam(null) }, ct);
            var oldChunks = PlanChunkScans.Count(oldPlan);
            var newChunks = PlanChunkScans.Count(newPlan);
            Assert.True(oldChunks - newChunks >= 3,
                $"expected the floor to exclude several of the seeded old-day chunks (old={oldChunks}, new={newChunks}):\nOLD:\n{oldPlan}\nNEW:\n{newPlan}");

            /* The stale server's own point read is bounded BELOW by its own newest instant, so it cannot scan
               older than that — cheaper than the unbounded oracle's whole-table scan, but not necessarily a
               single chunk: the WHERE clause has no upper bound, and the shared table keeps gaining new
               chunks every day regardless of this one server's staleness, so a chunk that opened after this
               server's own floor cannot be excluded from an open-ended ">=" predicate. */
            var oldStalePlan = await ExplainAsync(connection, "EXPLAIN (COSTS OFF) " + OldReplicaStatesSql,
                new[] { ServerFilterParam(staleServer.ServerId) }, ct);
            var newStalePlan = await ExplainAsync(connection, "EXPLAIN (COSTS OFF) " + DarlingAgStatesReader.ReplicaStatesSql,
                new[] { TimestampParam(staleServer.Instant), ServerFilterParam(staleServer.ServerId) }, ct);
            var oldStaleChunks = PlanChunkScans.Count(oldStalePlan);
            var newStaleChunks = PlanChunkScans.Count(newStalePlan);
            Assert.True(oldStaleChunks - newStaleChunks >= 2,
                $"expected the stale server's own floor to exclude chunks older than its own newest instant (old={oldStaleChunks}, new={newStaleChunks}):\nOLD:\n{oldStalePlan}\nNEW:\n{newStalePlan}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteSentinelRowsAsync);
        }
    }

    /* ─────────────────────────── seeding ─────────────────────────── */

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        foreach (var name in CurrentNames)
        {
            await RegisterServerAsync(connection, Id(name), name, enabled: true, ct);
            await SeedServerAsync(connection, Id(name), name, Instants(now, new[] { 4, 3, 2, 1 }, now), ct);
        }

        await RegisterServerAsync(connection, Id(StaleName), StaleName, enabled: true, ct);
        await SeedServerAsync(connection, Id(StaleName), StaleName, Instants(now, new[] { 5, 4, 3 }, now.AddDays(-2)), ct);

        await RegisterServerAsync(connection, Id(InsideHorizonName), InsideHorizonName, enabled: true, ct);
        await SeedServerAsync(connection, Id(InsideHorizonName), InsideHorizonName, Instants(now, new[] { 4, 3, 2, 1 }, now.AddHours(-2)), ct);

        await RegisterServerAsync(connection, Id(DisabledName), DisabledName, enabled: false, ct);
        await SeedServerAsync(connection, Id(DisabledName), DisabledName, Instants(now, new[] { 1 }, now), ct);

        await RegisterServerAsync(connection, Id(RemovedName), RemovedName, enabled: true, ct);
        await SeedServerAsync(connection, Id(RemovedName), RemovedName, Instants(now, new[] { 1 }, now), ct);
        await SimulateRemoveServerAsync(connection, Id(RemovedName), RemovedName, ct);
    }

    /// <summary>Several days of history (2 rows/day, several distinct chunks) plus a "collected every few
    /// minutes" recent window ending at <paramref name="newest"/> — the server's newest snapshot.</summary>
    private static List<DateTime> Instants(DateTime now, int[] daysBack, DateTime newest)
    {
        var list = new List<DateTime>();
        foreach (var d in daysBack)
        {
            var day = now.AddDays(-d);
            list.Add(day.AddHours(3));
            list.Add(day.AddHours(15));
        }

        for (var minutesAgo = 20; minutesAgo >= 5; minutesAgo -= 5)
        {
            list.Add(newest.AddMinutes(-minutesAgo));
        }

        list.Add(newest);
        return list;
    }

    private static async Task SeedServerAsync(NpgsqlConnection connection, int serverId, string serverName, IReadOnlyList<DateTime> instants, CancellationToken ct)
    {
        foreach (var at in instants)
        {
            await ExecAsync(connection, ReplicaNode1Sql, ct, CollectionIdGenerator.Next(), at, serverId, serverName, AgName);
            await ExecAsync(connection, ReplicaNode2Sql, ct, CollectionIdGenerator.Next(), at, serverId, serverName, AgName);
            await ExecAsync(connection, DatabaseDb1Sql, ct, CollectionIdGenerator.Next(), at, serverId, serverName, AgName);
            await ExecAsync(connection, DatabaseDb2Sql, ct, CollectionIdGenerator.Next(), at, serverId, serverName, AgName);
        }
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, int serverId, string serverName, bool enabled, CancellationToken ct)
    {
        using var command = new NpgsqlCommand("""
            INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
            VALUES ($1, $2, $3, $4, 15, $5, $5)
            ON CONFLICT (server_id) DO UPDATE SET is_enabled = $4, sql_major_version = 15
            """, connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(enabled);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Mirrors add_servers' write, then remove_server's own <c>DELETE FROM config_monitored_servers
    /// WHERE server_id = $1</c> — the servers registry row and all collected history are untouched, exactly as
    /// remove_server documents (#4228 PR body).</summary>
    private static async Task SimulateRemoveServerAsync(NpgsqlConnection connection, int serverId, string serverName, CancellationToken ct)
    {
        using (var insert = new NpgsqlCommand(
            "INSERT INTO config_monitored_servers (server_id, name, host) VALUES ($1, $2, $3) ON CONFLICT (server_id) DO NOTHING", connection))
        {
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(serverName);
            await insert.ExecuteNonQueryAsync(ct);
        }

        using var delete = new NpgsqlCommand("DELETE FROM config_monitored_servers WHERE server_id = $1", connection);
        delete.Parameters.AddWithValue(serverId);
        await delete.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object?[] args)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.AddWithValue(arg is DateTime dt ? DateTime.SpecifyKind(dt, DateTimeKind.Unspecified) : (arg ?? DBNull.Value));
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    /* ─────────────────────────── oracle + reader row rendering ─────────────────────────── */

    private static string Cell(object? value) => value switch
    {
        null => "null",
        DateTime dt => dt.ToString("O", CultureInfo.InvariantCulture),
        _ => Convert.ToString(value, CultureInfo.InvariantCulture)!,
    };

    private static string RenderReplica(DarlingAgStatesReader.ReplicaStateRow r) => string.Join("|",
        Cell(r.ServerId), Cell(r.ServerName), Cell(r.CollectionTime), Cell(r.AgName), Cell(r.ReplicaServerName),
        Cell(r.RoleDesc), Cell(r.IsLocal), Cell(r.OperationalStateDesc), Cell(r.ConnectedStateDesc),
        Cell(r.RecoveryHealthDesc), Cell(r.SynchronizationHealthDesc), Cell(r.AvailabilityModeDesc),
        Cell(r.FailoverModeDesc), Cell(r.EndpointUrl));

    private static string RenderDatabase(DarlingAgStatesReader.DatabaseReplicaStateRow r) => string.Join("|",
        Cell(r.ServerId), Cell(r.ServerName), Cell(r.CollectionTime), Cell(r.AgName), Cell(r.DatabaseName),
        Cell(r.ReplicaServerName), Cell(r.IsLocal), Cell(r.SynchronizationStateDesc), Cell(r.LastHardenedLsn),
        Cell(r.LastCommitLsn), Cell(r.LogSendQueueSize), Cell(r.RedoQueueSize), Cell(r.LogSendRate), Cell(r.RedoRate),
        Cell(r.IsSuspended), Cell(r.SuspendReasonDesc), Cell(r.AvailabilityModeDesc), Cell(r.SecondaryLagSeconds));

    private static async Task<List<(int ServerId, string Rendered)>> ReadOracleRowsAsync(NpgsqlConnection connection, string sql, int? serverIdFilter, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(ServerFilterParam(serverIdFilter));
        var rows = new List<(int, string)>();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var cells = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                cells[i] = Cell(reader.IsDBNull(i) ? null : reader.GetValue(i));
            }

            rows.Add((reader.GetInt32(0), string.Join("|", cells)));
        }

        return rows;
    }

    private static async Task<int> ReadOracleScalarAsync(NpgsqlConnection connection, string sql, int? serverIdFilter, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(ServerFilterParam(serverIdFilter));
        var result = await command.ExecuteScalarAsync(ct);
        return Convert.ToInt32(result ?? 0L);
    }

    private static async Task<List<(int ServerId, DateTime Instant)>> ReadInstantsAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(ServerFilterParam(null));
        var rows = new List<(int, DateTime)>();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetInt32(0), reader.GetDateTime(1)));
        }

        return rows;
    }

    private static List<string> Scoped(List<(int ServerId, string Rendered)> rows, HashSet<int> ids) =>
        rows.Where(r => ids.Contains(r.ServerId)).Select(r => r.Rendered).OrderBy(s => s, StringComparer.Ordinal).ToList();

    /* ─────────────────────────── plumbing ─────────────────────────── */

    /// <summary>Binds the optional server filter exactly as <c>DarlingAgStatesReader.AddServerFilter</c> does.</summary>
    private static NpgsqlParameter ServerFilterParam(int? value) => new NpgsqlParameter
    {
        NpgsqlDbType = NpgsqlDbType.Integer,
        Value = (object?)value ?? DBNull.Value,
    };

    /// <summary>Binds a floor exactly as <c>DarlingAgStatesReader.AddTimestamp</c> does.</summary>
    private static NpgsqlParameter TimestampParam(DateTime value) =>
        new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Unspecified) };

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, string sql, NpgsqlParameter[] args, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddRange(args);
        var plan = new StringBuilder();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            plan.AppendLine(reader.GetString(0));
        }

        return plan.ToString();
    }

    /// <summary>The store the way the service builds it — duplicated rather than shared across test files per
    /// this suite's convention (see EventWindowedReadsAreBoundedLivePostgresTests).</summary>
    private static async Task<bool> PrepareHypertablesAsync(string connectionString, NpgsqlConnection connection, CancellationToken ct)
    {
        var timescale = await LiveTimescaleProbe.TryEnableAsync(connectionString, ct);
        if (timescale)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct);
        }

        return timescale;
    }

    /// <summary>PostgreSQL <c>timestamp</c> is microsecond-resolution; .NET ticks are 100 ns.</summary>
    private static DateTime Micro(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % 10)), DateTimeKind.Unspecified);

    private static async Task DeleteSentinelRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", AllNames.Select(n => Id(n).ToString(CultureInfo.InvariantCulture)));
        foreach (var table in new[] { "ag_replica_states", "ag_database_replica_states" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ({ids})", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }

        using (var cleanupConfig = new NpgsqlCommand($"DELETE FROM config_monitored_servers WHERE server_id IN ({ids})", connection))
        {
            await cleanupConfig.ExecuteNonQueryAsync(ct);
        }

        using var cleanupServers = new NpgsqlCommand($"DELETE FROM servers WHERE server_id IN ({ids})", connection);
        await cleanupServers.ExecuteNonQueryAsync(ct);
    }
}
