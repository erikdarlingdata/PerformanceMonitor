/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite v77 / Darling V201 (#6159): <c>ag_replica_states</c> gains <c>primary_replica</c>, the name of the replica that was the
/// primary when the row was collected. The column is VARCHAR, nullable, and the LAST payload column, because the DuckDB appender
/// is positional. A fresh file gets it from the generator; an existing file gets it from the shared added-columns list. The
/// collector side is <see cref="AgCollectorDefinitionTests"/>; the reader is <see cref="AgPrimaryReplicaReadTests"/>.
/// </summary>
public sealed class AgPrimaryReplicaRungTests : IDisposable
{
    private const string Column = "primary_replica";

    private readonly string _tempDir;

    public AgPrimaryReplicaRungTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "AgPrimaryReplicaRungTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task AFreshFile_EndsAgReplicaStatesWithThePrimary_AfterTheGroupId()
    {
        var dbPath = Path.Combine(_tempDir, "lite-fresh.duckdb");
        using (var initializer = new DuckDbInitializer(dbPath))
        {
            await initializer.InitializeAsync();
        }

        using var conn = new DuckDBConnection($"Data Source={dbPath}");
        await conn.OpenAsync();

        Assert.True(DuckDbInitializer.CurrentSchemaVersion >= 77);
        var rows = await ColumnsAsync(conn);
        Assert.Equal((Column, "VARCHAR", "YES"), rows[^1]);
        Assert.Equal("group_id", rows[^2].Name);
        var entry = Assert.Single(DuckDbInitializer.AddedColumnsForVersion(77));
        Assert.Equal(("ag_replica_states", Column, "VARCHAR"), (entry.Table, entry.Column, entry.Type));
    }

    [Fact]
    public async Task AV76Database_ClimbsToTheNewestVersion_AndItsOldRowsReadNull()
    {
        var dbPath = Path.Combine(_tempDir, "lite-v76.duckdb");

        using (var initializer = new DuckDbInitializer(dbPath))
        {
            await initializer.InitializeAsync();
        }

        string[] freshOrder;
        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();
            freshOrder = (await ColumnsAsync(conn)).Select(c => c.Name).ToArray();

            /* DuckDB refuses to DROP a column a view or index depends on; the dependents go first for the fixture's sake. */
            foreach (var view in await ListAsync(conn, "SELECT view_name FROM duckdb_views() WHERE NOT internal AND sql ILIKE '%ag_replica_states%'"))
                await ExecAsync(conn, $"DROP VIEW IF EXISTS {view}");
            foreach (var index in await ListAsync(conn, "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'ag_replica_states'"))
                await ExecAsync(conn, $"DROP INDEX IF EXISTS {index}");

            await ExecAsync(conn, $"ALTER TABLE ag_replica_states DROP COLUMN {Column}");
            await ExecAsync(conn, "INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc) "
                + "VALUES (-1, TIMESTAMP '2026-10-19 12:00:00', 1, 'srv', 'AG1', 'NODE2', 'SECONDARY')");
            await ExecAsync(conn, "DELETE FROM schema_version");
            await ExecAsync(conn, "INSERT INTO schema_version (version) VALUES (76)");

            Assert.Equal(0L, await CountAsync(conn, $"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'ag_replica_states' AND column_name = '{Column}'"));
        }

        using var upgraded = new DuckDbInitializer(dbPath);
        await upgraded.InitializeAsync();

        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();

            Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, await CountAsync(conn, "SELECT MAX(version) FROM schema_version"));

            /* The upgraded table has the same columns in the same order as a fresh one. */
            Assert.Equal(freshOrder, (await ColumnsAsync(conn)).Select(c => c.Name).ToArray());

            /* The pre-rung row reads NULL; a post-rung row keeps its value. */
            await ExecAsync(conn, "INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, primary_replica) "
                + "VALUES (-2, TIMESTAMP '2026-10-19 13:00:00', 1, 'srv', 'AG1', 'NODE2', 'SECONDARY', 'NODE1')");
            Assert.Equal(DBNull.Value, await ScalarAsync(conn, $"SELECT {Column} FROM ag_replica_states WHERE collection_id = -1"));
            Assert.Equal("NODE1", await ScalarAsync(conn, $"SELECT {Column} FROM ag_replica_states WHERE collection_id = -2"));
        }
    }

    private static async Task<List<(string Name, string Type, string Nullable)>> ColumnsAsync(DuckDBConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'ag_replica_states' ORDER BY ordinal_position";
        using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<(string Name, string Type, string Nullable)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        }

        return rows;
    }

    private static async Task<List<string>> ListAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = await cmd.ExecuteReaderAsync();
        var values = new List<string>();
        while (await reader.ReadAsync())
        {
            values.Add(reader.GetString(0));
        }

        return values;
    }

    private static async Task ExecAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<object?> ScalarAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync();
    }

    private static async Task<long> CountAsync(DuckDBConnection conn, string sql) =>
        Convert.ToInt64(await ScalarAsync(conn, sql));
}

/// <summary>
/// #6159: Lite's AG read carries the stored primary onto the card. The secondary's own list shows only itself, so the card names the primary
/// the engine reported, by display name when that primary is a monitored server (found by its own local row), and keeps the "not visible"
/// note for a row stored before the column existed.
/// </summary>
public sealed class AgPrimaryReplicaReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public AgPrimaryReplicaReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task ASecondarysCard_NamesTheStoredPrimary_ByDisplayName_AndAnOldRowKeepsTheNote()
    {
        var at = new DateTime(2026, 10, 19, 12, 0, 0, DateTimeKind.Utc);
        await SeedAsync(1, "SRV1", "AG1", "NODE1", "PRIMARY", "NODE1", at);
        await SeedAsync(2, "SRV2", "AG1", "NODE2", "SECONDARY", "NODE1", at);
        await SeedAsync(3, "SRV3", "AG2", "NODE3", "SECONDARY", null, at);

        var service = new LocalDataService(_duckDb)
        {
            DisplayNames = () => new Dictionary<int, string> { [1] = "Orders primary" },
        };
        var cards = await service.GetAgTopologyAsync();

        Assert.Equal("Primary: Orders primary", Assert.Single(cards, c => c.ServerId == 2).PrimaryDisplay);
        Assert.Equal("Primary: NODE1", Assert.Single(cards, c => c.ServerId == 1).PrimaryDisplay);
        Assert.Equal("Primary not visible from this secondary", Assert.Single(cards, c => c.ServerId == 3).PrimaryDisplay);

        /* No display-name map: the replica name. */
        var plain = await new LocalDataService(_duckDb).GetAgTopologyAsync();
        Assert.Equal("Primary: NODE1", Assert.Single(plain, c => c.ServerId == 2).PrimaryDisplay);
    }

    private async Task SeedAsync(int serverId, string serverName, string ag, string replica, string role, string? primary, DateTime at)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open)
        {
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO ag_replica_states
    (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc,
     operational_state_desc, connected_state_desc, recovery_health_desc, synchronization_health_desc,
     availability_mode_desc, failover_mode_desc, endpoint_url, is_local, primary_replica)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'ONLINE', 'CONNECTED', 'ONLINE', 'HEALTHY', 'SYNCHRONOUS_COMMIT', 'AUTOMATIC', 'tcp://x', true, $8)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = ag });
        cmd.Parameters.Add(new DuckDBParameter { Value = replica });
        cmd.Parameters.Add(new DuckDBParameter { Value = role });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)primary ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }
}
