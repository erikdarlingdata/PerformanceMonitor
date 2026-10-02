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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V151 (#4475): <c>group_id</c>, a text column on both <c>collect.ag_replica_states</c>
/// and <c>collect.ag_database_replica_states</c> that carries <c>sys.availability_groups.group_id</c>, the
/// same GUID (stored as a 36-character string) on every replica of one physical Availability Group. This
/// file is the RUNG (ladder, viewer probe) and the schema-after-migrate proof: both columns exist on a
/// fresh migrate, the ALTER succeeds on a store whose chunks are already compressed, a pre-V151 row reads
/// <c>group_id IS NULL</c> rather than erroring, and an INSERT in the collector's own column order round
/// trips a GUID string end to end.
///
/// <para>This file's "I am the top rung" claim moved to <c>CaggGroupIndexDropRungTests</c> (V152) now
/// that V152 has landed; this file's own rung/probe facts below keep asserting what stays true forever
/// (present, in-order, gated behind the arm above it) rather than "is exactly the top".</para>
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the
   shared store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class AgGroupIdRungTests
{
    private const int RungVersion = 151;
    private const int PreviousVersion = 150;

    /// <summary>This rung's sentinel ordinal in the viewer probe — no longer the newest, since V152
    /// landed above it.</summary>
    private const int ProbeOrdinal = 126;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The rung is registered, and the ladder stays dense above the historical gap — the claim this class
    /// took over from <c>CollectionLogWatermarkAndJobHistoryIndexesRungTests</c> (V150) moved on again to
    /// <c>CaggGroupIndexDropRungTests</c> (V152) now that V152 has landed.
    /// </summary>
    [Fact]
    public void TheRungIsRegistered_AndTheLadderIsDenseAboveTheHistoricalGap()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("ag-group-id", PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);

        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// The viewer probe's sentinel carries this rung, and the map treats it as an arm gated below the
    /// current top's arm — a missing arm maps a fully-migrated store one rung short, permanently, because
    /// <see cref="ViewerDataService.RequiredStoreSchemaVersion"/> is <see cref="StorageVersion.SchemaVersion"/>.
    /// </summary>
    [Fact]
    public void TheProbeCarriesThisRungsSentinel_AndTheArmSitsBelowTheCurrentTop()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("table_name = 'ag_replica_states'", probe, StringComparison.Ordinal);
        Assert.Contains("column_name = 'group_id'", probe, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.Equal("hasAgGroupId", method.GetParameters()[ProbeOrdinal].Name);

        /* Every rung above this one (V152's hasCaggGroupIndexDrop) must also be false, or the map finds
           the newer arm first and this assertion is checking the wrong rung's fallthrough. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        var behind = (object[])all.Clone();
        for (var i = ProbeOrdinal; i < arity; i++)
        {
            behind[i] = false;
        }
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* V152 (#4503) is now the top rung, so this arm no longer needs to be the LAST one — it only has to
           sit below the current top's arm, which is what the ladder-dense invariant above already
           guarantees is registered ahead of it. */
        var thisArm = viewer.IndexOf("if (hasAgGroupId)", StringComparison.Ordinal);
        var topArm = viewer.IndexOf("if (hasCaggGroupIndexDrop)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V151 sentinel arm — a fully-migrated store would map one rung short");
        Assert.True(topArm >= 0 && topArm < thisArm, "the current top rung's arm must sit above the V151 arm");
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..viewer.IndexOf("if (hasCollectionLogWatermarkAndJobHistoryIndexes)", StringComparison.Ordinal)], StringComparison.Ordinal);
    }

    /// <summary>
    /// The LIVE schema after migrate: both columns exist. Run against <c>origin/dev</c> (pre-V151) this is
    /// RED — neither column exists — proving the pin actually checks the rung rather than a tautology.
    /// </summary>
    [Fact]
    public async Task AfterMigrate_BothGroupIdColumnsExist()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V151 schema pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await ColumnExistsAsync(connection, "ag_replica_states", "group_id", ct),
            "V151 adds ag_replica_states.group_id");
        Assert.True(await ColumnExistsAsync(connection, "ag_database_replica_states", "group_id", ct),
            "V151 adds ag_database_replica_states.group_id");
    }

    /// <summary>
    /// Rerunning the migration ladder (as startup does on an already-migrated store) is idempotent: the
    /// two <c>ADD COLUMN IF NOT EXISTS</c> statements do not error and both columns still exist.
    /// </summary>
    [Fact]
    public async Task MigrateAsync_RunTwice_IsIdempotent()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V151 idempotency pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await ColumnExistsAsync(connection, "ag_replica_states", "group_id", ct));
        Assert.True(await ColumnExistsAsync(connection, "ag_database_replica_states", "group_id", ct));
    }

    /// <summary>
    /// The live migration round trip on COMPRESSED hypertables: climb to V150, plant one row in each of
    /// <c>ag_replica_states</c> and <c>ag_database_replica_states</c>, compress their chunks the product
    /// way (<c>create_hypertable</c> + <c>timescaledb.compress</c> + <c>compress_chunk</c> — the same
    /// three statements <see cref="TimescaleSupport.CreateHypertableSql(ICollectorSchemaInfo)"/> and
    /// <see cref="TimescaleSupport.EnableCompressionSql(ICollectorSchemaInfo)"/> issue at startup), then
    /// roll back JUST V151's version stamp and re-apply it. The ALTER must succeed against a hypertable
    /// whose chunks are already compressed — a TimescaleDB compressed chunk rejects some DDL forms outright
    /// — and the planted pre-V151 rows must read <c>group_id IS NULL</c> rather than erroring or losing the
    /// row. A closing INSERT in the collector's own column order (<see cref="AgReplicaStatesCollector"/>'s
    /// <c>WritePayload</c> order, group_id last) carries a GUID string and reads back unchanged.
    /// </summary>
    [Fact]
    public async Task AStoreAtV150WithCompressedChunks_MigratesToV151_AndOldRowsReadGroupIdNull()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V151 compressed-hypertable round trip.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        /* Climb all the way to the top first — the compressed-hypertable state below needs the rest of
           the schema (servers, config) present, and a fresh scratch database has none of it yet. */
        await PgMigrations.MigrateAsync(connection, ct);

        var oldTime = DateTime.SpecifyKind(new DateTime(2026, 1, 1, 0, 0, 0), DateTimeKind.Unspecified);

        await InsertReplicaRowAsync(connection, ct, collectionId: 1, collectionTime: oldTime, groupIdColumn: false);
        await InsertDatabaseReplicaRowAsync(connection, ct, collectionId: 1, collectionTime: oldTime, groupIdColumn: false);

        /* This test MUST run on CI (CI has TimescaleDB); the compressed-hypertable state is the whole
           point. A fresh ScratchPostgres database does not inherit the extension, so enable it here on the
           scratch connection before the create_hypertable calls below, the same way
           DarlingWatermarkFloorScanBoundLiveTests / CaptureDownChunkOrderTests do. */
        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        Assert.True(timescaleEnabled, "TimescaleDB must be available on CI for the compressed-hypertable round trip");

        /* Convert both tables to hypertables and compress their one chunk each, the product's own way —
           this is what "V150's ALTER succeeded on the compressed hypertable" actually needs to test
           against, rather than an ordinary heap table. */
        await ExecAsync(connection, ct,
            "SELECT create_hypertable('collect.ag_replica_states', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true, migrate_data => true)");
        await ExecAsync(connection, ct,
            "SELECT create_hypertable('collect.ag_database_replica_states', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true, migrate_data => true)");
        await ExecAsync(connection, ct,
            "ALTER TABLE collect.ag_replica_states SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')");
        await ExecAsync(connection, ct,
            "ALTER TABLE collect.ag_database_replica_states SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')");
        await CompressAllChunksAsync(connection, ct, "collect.ag_replica_states");
        await CompressAllChunksAsync(connection, ct, "collect.ag_database_replica_states");

        Assert.True(await AnyChunkCompressedAsync(connection, ct, "collect.ag_replica_states"),
            "the seed row's chunk did not compress — the ALTER below would prove nothing about a compressed hypertable");
        Assert.True(await AnyChunkCompressedAsync(connection, ct, "collect.ag_database_replica_states"));

        /* Roll back JUST V151's version stamp, so the store looks exactly like a V150 store whose chunks
           are already compressed. */
        await ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= " + RungVersion);

        using (var version = new NpgsqlCommand("SELECT MAX(version) FROM darling_schema_version", connection))
        {
            Assert.Equal(PreviousVersion, Convert.ToInt32(await version.ExecuteScalarAsync(ct)));
        }

        /* The upgrade: apply every rung above V150, which is exactly V151 on this build. */
        var applied = await PgMigrations.MigrateAsync(connection, ct);
        Assert.Equal(PgMigrations.Scripts.Count(m => m.Version >= RungVersion), applied);

        Assert.True(await ColumnExistsAsync(connection, "ag_replica_states", "group_id", ct));
        Assert.True(await ColumnExistsAsync(connection, "ag_database_replica_states", "group_id", ct));

        Assert.Null(await ReadGroupIdAsync(connection, ct, "ag_replica_states", collectionId: 1));
        Assert.Null(await ReadGroupIdAsync(connection, ct, "ag_database_replica_states", collectionId: 1));

        /* A closing INSERT in the collector's own column order (group_id last), carrying a real GUID
           string, round trips end to end on the now-widened, compressed hypertable. */
        var newTime = DateTime.SpecifyKind(new DateTime(2026, 9, 27, 0, 0, 0), DateTimeKind.Unspecified);
        const string groupId = "3F2504E0-4F89-11D3-9A0C-0305E82C3301";

        await InsertReplicaRowAsync(connection, ct, collectionId: 2, collectionTime: newTime, groupIdColumn: true, groupId);
        await InsertDatabaseReplicaRowAsync(connection, ct, collectionId: 2, collectionTime: newTime, groupIdColumn: true, groupId);

        Assert.Equal(groupId, await ReadGroupIdAsync(connection, ct, "ag_replica_states", collectionId: 2));
        Assert.Equal(groupId, await ReadGroupIdAsync(connection, ct, "ag_database_replica_states", collectionId: 2));
    }

    private static async Task InsertReplicaRowAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, long collectionId, DateTime collectionTime,
        bool groupIdColumn, string? groupId = null)
    {
        var columns = "collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, " +
            "operational_state_desc, connected_state_desc, recovery_health_desc, synchronization_health_desc, " +
            "availability_mode_desc, failover_mode_desc, endpoint_url, is_local" + (groupIdColumn ? ", group_id" : string.Empty);
        var placeholders = groupIdColumn
            ? "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16"
            : "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15";

        await using var command = new NpgsqlCommand(
            $"INSERT INTO collect.ag_replica_states ({columns}) VALUES ({placeholders})", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(1);
        command.Parameters.AddWithValue("server1");
        command.Parameters.AddWithValue("AG1");
        command.Parameters.AddWithValue("NODE1");
        command.Parameters.AddWithValue("PRIMARY");
        command.Parameters.AddWithValue("ONLINE");
        command.Parameters.AddWithValue("CONNECTED");
        command.Parameters.AddWithValue("ONLINE");
        command.Parameters.AddWithValue("HEALTHY");
        command.Parameters.AddWithValue("SYNCHRONOUS_COMMIT");
        command.Parameters.AddWithValue("AUTOMATIC");
        command.Parameters.AddWithValue("TCP://NODE1:5022");
        command.Parameters.AddWithValue(true);
        if (groupIdColumn)
        {
            command.Parameters.AddWithValue(groupId!);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertDatabaseReplicaRowAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, long collectionId, DateTime collectionTime,
        bool groupIdColumn, string? groupId = null)
    {
        var columns = "collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name, " +
            "is_local, synchronization_state_desc, last_hardened_lsn, last_commit_lsn, log_send_queue_size, " +
            "redo_queue_size, log_send_rate, redo_rate, is_suspended, suspend_reason_desc, availability_mode_desc, " +
            "secondary_lag_seconds" + (groupIdColumn ? ", group_id" : string.Empty);
        var placeholders = groupIdColumn
            ? "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20"
            : "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19";

        await using var command = new NpgsqlCommand(
            $"INSERT INTO collect.ag_database_replica_states ({columns}) VALUES ({placeholders})", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(1);
        command.Parameters.AddWithValue("server1");
        command.Parameters.AddWithValue("AG1");
        command.Parameters.AddWithValue("db1");
        command.Parameters.AddWithValue("NODE1");
        command.Parameters.AddWithValue(true);
        command.Parameters.AddWithValue("SYNCHRONIZED");
        command.Parameters.AddWithValue("0/0");
        command.Parameters.AddWithValue("0/0");
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(false);
        command.Parameters.AddWithValue(DBNull.Value);
        command.Parameters.AddWithValue("SYNCHRONOUS_COMMIT");
        command.Parameters.AddWithValue(0L);
        if (groupIdColumn)
        {
            command.Parameters.AddWithValue(groupId!);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ReadGroupIdAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, string table, long collectionId)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT group_id FROM collect.{table} WHERE collection_id = $1", connection);
        command.Parameters.AddWithValue(collectionId);
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull or null ? null : (string)value;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<bool> ColumnExistsAsync(
        NpgsqlConnection connection, string table, string column, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = $1 AND column_name = $2)",
            connection);
        command.Parameters.AddWithValue(table);
        command.Parameters.AddWithValue(column);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task CompressAllChunksAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, string table)
    {
        var chunks = new List<string>();
        await using (var chunkList = new NpgsqlCommand($"SELECT show_chunks('{table}')::text", connection))
        await using (var reader = await chunkList.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                chunks.Add(reader.GetString(0));
            }
        }

        foreach (var chunk in chunks)
        {
            await using var compress = new NpgsqlCommand($"SELECT compress_chunk('{chunk}', if_not_compressed => true)", connection);
            await compress.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task<bool> AnyChunkCompressedAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, string table)
    {
        await using var command = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM timescaledb_information.chunks WHERE hypertable_name = " +
            "split_part($1, '.', 2) AND is_compressed)", connection);
        command.Parameters.AddWithValue(table);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }
}
