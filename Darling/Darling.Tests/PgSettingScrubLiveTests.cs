/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live, end-to-end proof of the #4348 S1b one-time scrub against a REAL compressed TimescaleDB chunk.
///
/// <para><b>#4348 own-store</b> — mints its own scratch database (<see cref="ScratchPostgres"/>) rather than
/// sharing the live fixture, so it is deliberately NOT in the <c>live-postgres</c> collection: it creates its
/// own hypertable/compression shape on <c>collect.pg_server_config</c>, which the shared fixture must never
/// inherit from a test.</para>
/// </summary>
public sealed class PgSettingScrubLiveTests
{
    /// <summary>
    /// Pins round 2's H1 fix: batches are grouped by (server, day), not day alone, and every UPDATE carries a
    /// constant <c>server_id</c> predicate alongside the day range — not just the join equality — so
    /// TimescaleDB can exclude every other server's compressed segment in that day's chunk. Two servers each
    /// get a secret-bearing row on the SAME day, in the SAME compressed chunk. The connection's
    /// <c>Options=-c timescaledb.max_tuples_decompressed_per_dml_transaction=N</c> is set to one target's row
    /// count plus one: a day-only batch (the pre-H1 shape) would decompress BOTH servers' rows in the shared
    /// chunk and trip the limit with 53400; the (server, day) batch decompresses only the one server's
    /// segment and stays under it.
    /// </summary>
    [Fact]
    public async Task TheScrubBatchesPerServerAndDay_NotDayAlone()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4351 H1 batching pin (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        /* Seed and shape the chunk using an UNCAPPED connection: capping max_tuples_decompressed at
           connection scope would also block the seeding compress_chunk() call itself. */
        await using (var setupConnection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setupConnection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setupConnection, ct);

            Assert.True(await TimescaleSupport.TryEnableAsync(setupConnection, null, ct),
                "the dev fixture is expected to have TimescaleDB installed");
            await ExecAsync(setupConnection, "SELECT create_hypertable('collect.pg_server_config', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true)", ct);
            await ExecAsync(setupConnection, "ALTER TABLE collect.pg_server_config SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

            const int serverA = -444445;
            const int serverB = -444446;

            /* One secret-bearing row per server, same day, same chunk. */
            await InsertRowAsync(setupConnection, serverA, OldTime, "primary_conninfo", Secret, databaseName: null, roleName: null, ct);
            await InsertRowAsync(setupConnection, serverB, OldTime, "primary_conninfo", Secret, databaseName: null, roleName: null, ct);

            await ExecAsync(setupConnection, "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('collect.pg_server_config') c", ct);

            Assert.True(await ContainsSecretAsync(setupConnection, ct), "seeding failed to plant the secret this test exists to catch");
        }

        /* One target's own day has 1 candidate row. Cap at 1 (target-row-count) so a day-only batch, which
           would decompress both servers' rows sharing this chunk, trips 53400 with room to spare; a
           (server, day) batch touches only 1 row and stays at the cap. */
        var cappedConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            Options = "-c timescaledb.max_tuples_decompressed_per_dml_transaction=1",
        }.ConnectionString;

        await using var postgres = NpgsqlDataSource.Create(cappedConnectionString);

        var summary = await PgSettingScrub.RunAsync(postgres, logger: null, ct);

        Assert.False(summary.AlreadyDone);
        Assert.Equal(2, summary.RowsUpdated);

        await using var verifyConnection = new NpgsqlConnection(scratch.ConnectionString);
        await verifyConnection.OpenAsync(ct);
        Assert.False(await ContainsSecretAsync(verifyConnection, ct), "the raw password survived the scrub under a tight decompress cap");
    }

    private const int ServerId = -444444;
    private const string ServerName = "darling-setting-scrub-e2e";
    private const string Secret = "host=replica1.internal port=5432 user=replicator password=hunter2 sslmode=require";
    private static readonly DateTime OldTime = DateTime.SpecifyKind(new DateTime(2026, 1, 1, 0, 0, 0), DateTimeKind.Unspecified);
    private static readonly DateTime NewTime = DateTime.SpecifyKind(new DateTime(2026, 1, 1, 1, 0, 0), DateTimeKind.Unspecified);

    [Fact]
    public async Task TheScrubRedactsOldRows_LeavesHistoryUnchanged_AndIsIdempotent()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4348 scrub test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await ExecAsync(connection, "SELECT create_hypertable('collect.pg_server_config', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true)", ct);
        await ExecAsync(connection, "ALTER TABLE collect.pg_server_config SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

        /* Arm 1: a server-wide pg_settings row (database_name/role_name NULL), the OLD unredacted shape. */
        await InsertRowAsync(connection, ServerId, OldTime, "primary_conninfo", Secret, databaseName: null, roleName: null, ct);
        /* Arm 2: a database_name override row (V138) — the standby's replication setting scoped to one database. */
        await InsertRowAsync(connection, ServerId, OldTime, "primary_conninfo", Secret, databaseName: "appdb", roleName: null, ct);
        /* A NEWER row S1a's redactor already masked — same underlying value, already-redacted shape, server-wide. */
        await InsertRowAsync(connection, ServerId, NewTime, "primary_conninfo", "host=replica1.internal port=5432 user=replicator password=******** sslmode=require", databaseName: null, roleName: null, ct);
        /* A control row with no secret at all — must come out byte-identical. */
        await InsertRowAsync(connection, ServerId, OldTime, "shared_buffers", "128MB", databaseName: null, roleName: null, ct);

        await ExecAsync(connection, "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('collect.pg_server_config') c", ct);

        /* ── Prove the assertion below actually depends on the job: the secret is really there, compressed, before it runs. ── */
        Assert.True(await ContainsSecretAsync(connection, ct), "seeding failed to plant the secret this test exists to catch");

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var first = await PgSettingScrub.RunAsync(postgres, logger: null, ct);
        Assert.False(first.AlreadyDone);
        Assert.True(first.RowsUpdated >= 2, $"expected at least the two secret-bearing rows to be updated, got {first.RowsUpdated}");

        /* ── No column of any row contains the secret. ── */
        Assert.False(await ContainsSecretAsync(connection, ct), "the raw password survived the scrub");

        /* ── The control row is byte-identical. ── */
        var controlValue = await ScalarTextAsync(connection,
            "SELECT setting FROM collect.pg_server_config WHERE server_id = $1 AND collection_time = $2 AND name = 'shared_buffers'",
            ServerId, OldTime, ct);
        Assert.Equal("128MB", controlValue);

        /* ── The change history shows no CHANGE between the old (now-scrubbed) row and the newer (already-redacted) row. ── */
        await using (var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            var changes = await DarlingPgServerConfigReader.GetConfigChangesAsync(
                dataSource, ServerId, OldTime.AddHours(-1), NewTime.AddHours(1), 50, ct);
            foreach (var change in changes)
            {
                if (change.Name == "primary_conninfo" && change.DatabaseName is null)
                {
                    Assert.NotEqual(DarlingPgServerConfigReader.PgConfigChangeKind.Changed, change.ChangeKind);
                }
            }
        }

        /* ── A second start does nothing: the marker holds. ── */
        var second = await PgSettingScrub.RunAsync(postgres, logger: null, ct);
        Assert.True(second.AlreadyDone);

        /* ── An older marker (standing in for a future RulesVersion bump) runs the scrub again. ── */
        await ExecAsync(connection,
            "UPDATE collect.collector_state SET state_value = '0' WHERE collector_name = 'pg_setting_scrub' AND state_key = 'rules_version'", ct);
        var third = await PgSettingScrub.RunAsync(postgres, logger: null, ct);
        Assert.False(third.AlreadyDone);
    }

    private static async Task<bool> ContainsSecretAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT count(*) FROM collect.pg_server_config WHERE setting LIKE '%hunter2%' OR boot_val LIKE '%hunter2%' OR reset_val LIKE '%hunter2%'",
            connection)
        { CommandTimeout = 60 };
        var count = (long)(await cmd.ExecuteScalarAsync(ct))!;
        return count > 0;
    }

    private static async Task<string?> ScalarTextAsync(NpgsqlConnection connection, string sql, int serverId, DateTime collectionTime, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(collectionTime);
        return (await cmd.ExecuteScalarAsync(ct)) as string;
    }

    private static async Task InsertRowAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTime, string name, string value,
        string? databaseName, string? roleName, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO collect.pg_server_config
  (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype,
   source, boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc, database_name, role_name)
VALUES
  ($1, $2, $3, $4, $5, $6, NULL, 'category', 'sighup', 'string', 'configuration file', $6, $6, NULL, NULL, false, 'desc', $7, $8)", connection)
        { CommandTimeout = 60 };
        cmd.Parameters.AddWithValue((long)(collectionTime.Ticks ^ (long)name.GetHashCode() ^ (databaseName ?? "").GetHashCode()));
        cmd.Parameters.AddWithValue(collectionTime);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(ServerName);
        cmd.Parameters.AddWithValue(name);
        cmd.Parameters.AddWithValue(value);
        cmd.Parameters.AddWithValue((object?)databaseName ?? DBNull.Value);
        cmd.Parameters.AddWithValue((object?)roleName ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
