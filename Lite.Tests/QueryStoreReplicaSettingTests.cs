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
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6254, the Lite setting half: a server's <c>QueryStoreReplica</c> builds a replica connection string from the server's own
/// (host swapped, read-only intent, everything else kept), it saves and loads with the server list, and
/// <c>collection_log.read_source</c> reaches an existing store and its archive. The runner that opens the replica is a later part.
/// </summary>
[Collection("credential-manager")]
public sealed class QueryStoreReplicaConnectionStringTests
{
    private static ServerConnection SqlAuthServer(string? replica) => new()
    {
        ServerName = "primary.example.com,1433",
        DatabaseName = "AppDb",
        AuthenticationType = AuthenticationTypes.SqlServer,
        EncryptMode = "Strict",
        TrustServerCertificate = true,
        MultiSubnetFailover = true,
        QueryStoreReplica = replica
    };

    [Fact]
    public void TheReplicaHostReplacesThePrimary_WithReadOnlyIntent_AndTheLoginAndEncryptionKept()
    {
        var server = SqlAuthServer("replica.example.com");

        var replica = new SqlConnectionStringBuilder(server.BuildQueryStoreReplicaConnectionString("monitor", "p@ss")!);
        var primary = new SqlConnectionStringBuilder(server.BuildConnectionString("monitor", "p@ss"));

        Assert.Equal("replica.example.com", replica.DataSource);
        Assert.Equal(ApplicationIntent.ReadOnly, replica.ApplicationIntent);
        Assert.Equal(ApplicationIntent.ReadWrite, primary.ApplicationIntent);
        Assert.Equal("monitor", replica.UserID);
        Assert.Equal("p@ss", replica.Password);
        Assert.Equal(SqlConnectionEncryptOption.Strict, replica.Encrypt);
        Assert.True(replica.TrustServerCertificate);
        Assert.Equal("AppDb", replica.InitialCatalog);
        Assert.Equal(primary.ConnectTimeout, replica.ConnectTimeout);
        Assert.Equal(primary.CommandTimeout, replica.CommandTimeout);
        Assert.Equal(primary.ApplicationName, replica.ApplicationName);
        Assert.True(replica.MultiSubnetFailover);
    }

    [Fact]
    public void AHostAndPort_AndASurroundingBlank_ReachTheDataSourceAsTyped()
    {
        var server = SqlAuthServer("  replica.example.com,14331 ");

        var replica = new SqlConnectionStringBuilder(server.BuildQueryStoreReplicaConnectionString("monitor", "pw")!);

        Assert.Equal("replica.example.com,14331", replica.DataSource);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void AnEmptySetting_BuildsNothing_SoTheCallerReadsThePrimary(string? setting)
    {
        var server = SqlAuthServer(setting);

        Assert.Null(server.BuildQueryStoreReplicaConnectionString("monitor", "pw"));
        Assert.Null(ServerConnection.ResolveQueryStoreReplicaConnectionString(server, new CredentialService(), profileLookup: null));
    }

    [Fact]
    public void TheResolvedString_KeepsWindowsAuth_AndSwapsTheHost()
    {
        var server = new ServerConnection
        {
            ServerName = "primary.example.com",
            AuthenticationType = AuthenticationTypes.Windows,
            QueryStoreReplica = "listener.example.com"
        };

        var replica = new SqlConnectionStringBuilder(
            ServerConnection.ResolveQueryStoreReplicaConnectionString(server, new CredentialService(), profileLookup: null)!);

        Assert.Equal("listener.example.com", replica.DataSource);
        Assert.True(replica.IntegratedSecurity);
        Assert.Equal(ApplicationIntent.ReadOnly, replica.ApplicationIntent);
    }
}

public sealed class QueryStoreReplicaPersistenceTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "QsReplicaPersist_" + Guid.NewGuid().ToString("N")[..8]);

    public QueryStoreReplicaPersistenceTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    [Fact]
    public void AServerWithAReplica_SavesAndLoadsIt()
    {
        var server = new ServerConnection { ServerName = "primary-a", DisplayName = "A", QueryStoreReplica = "replica-a,14332" };
        new ServerManager(_dir).AddServer(server);

        var loaded = Assert.Single(new ServerManager(_dir).GetAllServers());

        Assert.Equal("replica-a,14332", loaded.QueryStoreReplica);
    }

    [Fact]
    public void AServerSavedWithoutAReplica_LoadsItEmpty()
    {
        new ServerManager(_dir).AddServer(new ServerConnection { ServerName = "primary-b", DisplayName = "B" });

        var loaded = Assert.Single(new ServerManager(_dir).GetAllServers());

        Assert.Null(loaded.QueryStoreReplica);
    }

    [Fact]
    public void AnOldServerList_WithNoSuchField_LoadsWithItEmpty()
    {
        File.WriteAllText(Path.Combine(_dir, "servers.json"),
            """{"Servers":[{"Id":"11111111-1111-1111-1111-111111111111","ServerName":"old-primary","DisplayName":"Old","ReadOnlyIntent":true,"MultiSubnetFailover":true}]}""");

        var loaded = Assert.Single(new ServerManager(_dir).GetAllServers());

        Assert.Equal("old-primary", loaded.ServerName);
        Assert.True(loaded.MultiSubnetFailover);
        Assert.Null(loaded.QueryStoreReplica);
        Assert.Null(ServerConnection.ResolveQueryStoreReplicaConnectionString(loaded, new CredentialService(), profileLookup: null));
    }
}

/// <summary>#6254, v78: <c>collection_log.read_source</c>, nullable text, on an existing store and across the archive union.</summary>
public sealed class CollectionLogReadSourceRungTests : IDisposable
{
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "CollectionLogReadSource_" + Guid.NewGuid().ToString("N")[..8]);

    public CollectionLogReadSourceRungTests() => Directory.CreateDirectory(Path.Combine(_tempDir, "archive"));

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private const string InsertOld =
        "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ({0}, 1, 'srv', 'query_store', TIMESTAMP '{1}', 'SUCCESS')";

    [Fact]
    public async Task AFreshFile_EndsCollectionLogWithNullableReadSourceText()
    {
        var dbPath = Path.Combine(_tempDir, "fresh.duckdb");
        using (var initializer = new DuckDbInitializer(dbPath))
            await initializer.InitializeAsync();

        using var conn = new DuckDBConnection($"Data Source={dbPath}");
        await conn.OpenAsync();

        var columns = await ColumnsAsync(conn);
        Assert.Equal(("read_source", "VARCHAR", "YES"), columns[^1]);
        Assert.Equal("slowest_item_ms", columns[^2].Name);
        Assert.True(DuckDbInitializer.CurrentSchemaVersion >= 78);
    }

    [Fact]
    public async Task AV77Store_ClimbsToTheNewestVersion_OldRowsReadNull_AndTheArchiveUnionStillReads()
    {
        var dbPath = Path.Combine(_tempDir, "v77.duckdb");
        using (var initializer = new DuckDbInitializer(dbPath))
            await initializer.InitializeAsync();

        string[] freshOrder;
        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();
            freshOrder = (await ColumnsAsync(conn)).Select(c => c.Name).ToArray();

            /* Put the table back as a v77 store had it: no column, an old row, and an old-shape parquet archive. DuckDB refuses to drop a
               column that a view or index depends on, so the dependents go first. */
            foreach (var view in await ListAsync(conn, "SELECT view_name FROM duckdb_views() WHERE NOT internal AND sql ILIKE '%collection_log%'"))
                await ExecAsync(conn, $"DROP VIEW IF EXISTS {view}");
            foreach (var index in await ListAsync(conn, "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'collection_log'"))
                await ExecAsync(conn, $"DROP INDEX IF EXISTS {index}");
            await ExecAsync(conn, "ALTER TABLE collection_log DROP COLUMN read_source");

            await ExecAsync(conn, string.Format(InsertOld, -1, "2026-10-01 12:00:00"));
            var parquet = Path.Combine(_tempDir, "archive", "20261001_0000_collection_log.parquet").Replace("\\", "/");
            await ExecAsync(conn, $"COPY collection_log TO '{parquet}' (FORMAT PARQUET)");
            await ExecAsync(conn, "DELETE FROM collection_log");
            await ExecAsync(conn, string.Format(InsertOld, -2, "2026-10-20 12:00:00"));
            await ExecAsync(conn, "DELETE FROM schema_version");
            await ExecAsync(conn, "INSERT INTO schema_version (version) VALUES (77)");
        }

        using var upgraded = new DuckDbInitializer(dbPath);
        await upgraded.InitializeAsync();
        await upgraded.CreateArchiveViewsAsync();

        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();

            Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, Convert.ToInt64(await ScalarAsync(conn, "SELECT MAX(version) FROM schema_version")));
            Assert.Equal(freshOrder, (await ColumnsAsync(conn)).Select(c => c.Name).ToArray());

            await ExecAsync(conn, "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, read_source) "
                + "VALUES (-3, 1, 'srv', 'query_store', TIMESTAMP '2026-10-21 12:00:00', 'SUCCESS', 'replica-b')");

            /* The row stored before the upgrade reads NULL; the new row keeps its source. */
            Assert.Equal(DBNull.Value, await ScalarAsync(conn, "SELECT read_source FROM collection_log WHERE log_id = -2"));
            Assert.Equal("replica-b", await ScalarAsync(conn, "SELECT read_source FROM collection_log WHERE log_id = -3"));

            /* The archive view unions the old-shape parquet by name: all three rows read, the archived one with NULL. */
            Assert.Equal(3L, Convert.ToInt64(await ScalarAsync(conn, "SELECT COUNT(*) FROM v_collection_log WHERE log_id IN (-1, -2, -3)")));
            Assert.Equal(DBNull.Value, await ScalarAsync(conn, "SELECT read_source FROM v_collection_log WHERE log_id = -1"));
            Assert.Equal("replica-b", await ScalarAsync(conn, "SELECT read_source FROM v_collection_log WHERE log_id = -3"));
        }
    }

    private static async Task<List<(string Name, string Type, string Nullable)>> ColumnsAsync(DuckDBConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'collection_log' ORDER BY ordinal_position";
        using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<(string, string, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        return rows;
    }

    private static async Task<List<string>> ListAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = await cmd.ExecuteReaderAsync();
        var values = new List<string>();
        while (await reader.ReadAsync())
            values.Add(reader.GetString(0));
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
}
