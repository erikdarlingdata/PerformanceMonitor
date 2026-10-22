/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6254: Lite's REAL definition runner against a REAL availability group (the Lite twin of Darling's
/// <c>QueryStoreReplicaAgFixtureLiveTests</c>). The server is registered on the AG primary, <c>QueryStoreReplica</c> names
/// the readable secondary, and the secondary's Query Store rows must land under the PRIMARY's server id with
/// <c>collection_log.read_source</c> saying a replica served the database. Needs the docker AG fixture
/// (<c>tools/ag-fixture</c>): set <c>DARLING_TEST_SQL</c> (the primary, <c>localhost,14331</c>), <c>DARLING_TEST_SQL_REPLICA</c>
/// (the secondary, <c>localhost,14332</c>), <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c>; skips without
/// them, and when this session cannot write to Windows Credential Manager (Lite keeps the login there). The fixture's
/// secondary captures Query Store by default and a capturing secondary routes <c>not_mirror</c> (the primary is read), so the
/// test turns secondary capture off for its run and puts it back afterwards.
/// </summary>
[Collection("credential-manager")]
public sealed class QueryStoreReplicaAgFixtureLiveTests : IDisposable
{
    private const string FixtureDatabase = "AgFixtureDb";

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
    private DuckDbInitializer? _duckDb;

    public void Dispose()
    {
        _duckDb?.Dispose();
        try
        {
            if (Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, recursive: true);
            }
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private static string? Env(string name) => Environment.GetEnvironmentVariable(name);

    private static string ConnectionString(string host, string database) => new SqlConnectionStringBuilder
    {
        DataSource = host,
        InitialCatalog = database,
        UserID = Env("DARLING_TEST_SQL_USER") ?? string.Empty,
        Password = Env("DARLING_TEST_SQL_PASSWORD") ?? string.Empty,
        TrustServerCertificate = true,
        Encrypt = SqlConnectionEncryptOption.Optional,
        Pooling = false,
    }.ConnectionString;

    private static async Task<string?> ScalarAsync(string host, string database, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(ConnectionString(host, database));
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 120 };
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private static Task<string?> SecondaryStateAsync(string replicaHost, CancellationToken ct) =>
        ScalarAsync(replicaHost, FixtureDatabase, "SELECT actual_state_desc FROM sys.database_query_store_options", ct);

    private static async Task WaitForSecondaryStateAsync(string replicaHost, Func<string?, bool> wanted, CancellationToken ct)
    {
        for (var attempt = 0; attempt < 60; attempt++)
        {
            if (wanted(await SecondaryStateAsync(replicaHost, ct)))
            {
                return;
            }

            await Task.Delay(1000, ct);
        }

        Assert.Fail("the secondary's Query Store state did not reach the wanted state within 60 s");
    }

    [Fact]
    public async Task TheRunnerReadsTheSecondarysQueryStore_AndTheRowsLandUnderThePrimarysServerId()
    {
        var primaryHost = Env("DARLING_TEST_SQL");
        var replicaHost = Env("DARLING_TEST_SQL_REPLICA");
        var user = Env("DARLING_TEST_SQL_USER");
        var password = Env("DARLING_TEST_SQL_PASSWORD");
        Assert.SkipWhen(
            string.IsNullOrEmpty(primaryHost) || string.IsNullOrEmpty(replicaHost) || string.IsNullOrEmpty(user) || string.IsNullOrEmpty(password),
            "Set DARLING_TEST_SQL (AG primary), DARLING_TEST_SQL_REPLICA (readable secondary), DARLING_TEST_SQL_USER and DARLING_TEST_SQL_PASSWORD to run the #6254 AG fixture test.");

        var ct = TestContext.Current.CancellationToken;
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        var serverManager = new ServerManager(configDir);
        var server = new ServerConnection
        {
            ServerName = primaryHost!,
            DisplayName = "ag-fixture-6254",
            AuthenticationType = AuthenticationTypes.SqlServer,
            EncryptMode = "Optional",
            TrustServerCertificate = true,
            QueryStoreReplica = replicaHost,
        };

        var credentialSaved = false;
        var wasCapturing = false;
        try
        {
            try
            {
                serverManager.AddServer(server, user, password);
                credentialSaved = true;
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("Credential Manager", StringComparison.Ordinal))
            {
                Assert.Skip("This session cannot write to Windows Credential Manager.");
            }

            wasCapturing = string.Equals(await SecondaryStateAsync(replicaHost!, ct), "READ_CAPTURE_SECONDARY", StringComparison.Ordinal);
            if (wasCapturing)
            {
                await ScalarAsync(primaryHost!, "master", $"ALTER DATABASE [{FixtureDatabase}] FOR SECONDARY SET QUERY_STORE = OFF", ct);
            }

            await WaitForSecondaryStateAsync(replicaHost!, state => !string.Equals(state, "READ_CAPTURE_SECONDARY", StringComparison.Ordinal), ct);

            _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
            await _duckDb.InitializeFromTemplateAsync();
            var status = serverManager.GetConnectionStatus(server.Id);
            status.SqlEngineEdition = 3;
            status.SqlMajorVersion = int.Parse(
                await ScalarAsync(primaryHost!, "master", "SELECT CONVERT(int, SERVERPROPERTY('ProductMajorVersion'))", ct) ?? "0",
                CultureInfo.InvariantCulture);

            var service = new RemoteCollectorService(_duckDb, serverManager, new ScheduleManager(configDir));
            await service.RunCollectorAsync(server, QueryStoreCollector.Instance.Name, ct);

            /* The replica was opened once, and it served the fixture database, and nothing else. */
            Assert.Equal(1, service.QueryStoreReplicaConnectionsOpened);
            var serverId = RemoteCollectorService.GetServerId(server);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(ct);

            using var log = connection.CreateCommand();
            log.CommandText = "SELECT read_source FROM collection_log WHERE server_id = $1 AND collector_name = 'query_store' ORDER BY collection_time DESC LIMIT 1";
            log.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            var readSource = Convert.ToString(await log.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
            Assert.NotNull(readSource);
            Assert.StartsWith("replica 1, primary 0", readSource, StringComparison.Ordinal);

            /* The rows are the primary's: one server id (the primary's) and the fixture database. */
            using var rows = connection.CreateCommand();
            rows.CommandText = "SELECT COUNT(*), COUNT(DISTINCT database_name), MIN(database_name) FROM query_store_stats";
            using var reader = await rows.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.True(reader.GetInt64(0) > 0, "the fixture's primary has Query Store intervals; a replica read must ship them");
            Assert.Equal(1L, reader.GetInt64(1));
            Assert.Equal(FixtureDatabase, reader.GetString(2));

            using var servers = connection.CreateCommand();
            servers.CommandText = "SELECT COUNT(*) FROM query_store_stats WHERE server_id <> $1";
            servers.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            Assert.Equal(0L, Convert.ToInt64(await servers.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));
        }
        finally
        {
            if (wasCapturing)
            {
                await ScalarAsync(primaryHost!, "master", $"ALTER DATABASE [{FixtureDatabase}] FOR SECONDARY SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE)", CancellationToken.None);
            }

            if (credentialSaved)
            {
                new CredentialService().DeleteCredential(server.Id);
            }
        }
    }
}
