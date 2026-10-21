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
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6254 T7: the REAL Darling runner, a REAL availability group. The runner connects to the AG primary, <c>query_store_replica</c>
/// names the readable secondary, and the secondary's Query Store rows must land under the PRIMARY's server id with
/// <c>read_source</c> saying a replica served the database. Needs the docker AG fixture (<c>tools/ag-fixture</c>): set
/// <c>DARLING_TEST_PG</c>, <c>DARLING_TEST_SQL</c> (the primary, <c>localhost,14331</c>), <c>DARLING_TEST_SQL_REPLICA</c> (the
/// secondary, <c>localhost,14332</c>), <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c>; skips without them. The
/// fixture's secondary captures Query Store by default and a capturing secondary routes <c>not_mirror</c> (the primary is read),
/// so the test turns secondary capture off for its run and puts it back afterwards.
///
/// <para><b>#1776 own-store</b> - the test creates and drops its own scratch database through <see cref="ScratchPostgres"/>,
/// so it is not in the <c>live-postgres</c> collection.</para>
/// </summary>
[Collection("remediation-live-sql")]
public sealed class QueryStoreReplicaAgFixtureLiveTests
{
    private const string FixtureDatabase = "AgFixtureDb";

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
        return value is null or DBNull ? null : Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture);
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
        var pg = Env("DARLING_TEST_PG");
        var primaryHost = Env("DARLING_TEST_SQL");
        var replicaHost = Env("DARLING_TEST_SQL_REPLICA");
        var user = Env("DARLING_TEST_SQL_USER");
        Assert.SkipWhen(
            string.IsNullOrEmpty(pg) || string.IsNullOrEmpty(primaryHost) || string.IsNullOrEmpty(replicaHost) || string.IsNullOrEmpty(user),
            "Set DARLING_TEST_PG, DARLING_TEST_SQL (AG primary), DARLING_TEST_SQL_REPLICA (readable secondary), DARLING_TEST_SQL_USER and DARLING_TEST_SQL_PASSWORD to run the #6254 AG fixture test.");

        var ct = TestContext.Current.CancellationToken;
        var wasCapturing = string.Equals(await SecondaryStateAsync(replicaHost!, ct), "READ_CAPTURE_SECONDARY", StringComparison.Ordinal);
        try
        {
            if (wasCapturing)
            {
                await ScalarAsync(primaryHost!, "master", $"ALTER DATABASE [{FixtureDatabase}] FOR SECONDARY SET QUERY_STORE = OFF", ct);
            }

            await WaitForSecondaryStateAsync(replicaHost!, state => !string.Equals(state, "READ_CAPTURE_SECONDARY", StringComparison.Ordinal), ct);

            var config = new MonitoredServer
            {
                Name = "ag-fixture-6254",
                Host = primaryHost!,
                Auth = "sql",
                Username = user,
                Password = Env("DARLING_TEST_SQL_PASSWORD"),
                TrustServerCertificate = true,
                QueryStoreReplica = replicaHost,
            };
            var runtime = await DarlingServerConnector.ConnectAsync(config, null, ct);

            await using var scratch = await ScratchPostgres.CreateAsync(pg!, ct);
            await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
            {
                await migrate.OpenAsync(ct);
                await PgMigrations.MigrateAsync(migrate, ct);
            }

            await using var source = new NpgsqlDataSourceBuilder(scratch.ConnectionString).Build();
            var runner = new DarlingCollectorRunner(source, new CollectorDeltaCalculator());
            var result = await runner.RunAsync(QueryStoreCollector.Instance, runtime, ct);

            /* The fixture database was served by the replica, and by nothing else. */
            Assert.NotNull(result.ReadSource);
            Assert.StartsWith("replica 1, primary 0", result.ReadSource, StringComparison.Ordinal);
            Assert.True(result.Rows > 0, "the fixture's primary has Query Store intervals; a replica read must ship them");

            /* The rows are the primary's: one server id, the primary's, and nothing under any other. */
            var perServer = new Dictionary<int, long>();
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await using var command = new NpgsqlCommand(
                    "SELECT server_id, count(*) FROM collect.query_store_stats WHERE database_name = $1 GROUP BY server_id", connection);
                command.Parameters.AddWithValue(FixtureDatabase);
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    perServer[reader.GetInt32(0)] = reader.GetInt64(1);
                }
            }

            Assert.Equal(new[] { runtime.ServerId }, perServer.Keys);
            Assert.Equal(result.Rows, perServer[runtime.ServerId]);
        }
        finally
        {
            if (wasCapturing)
            {
                await ScalarAsync(primaryHost!, "master", $"ALTER DATABASE [{FixtureDatabase}] FOR SECONDARY SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE)", CancellationToken.None);
            }
        }
    }
}
