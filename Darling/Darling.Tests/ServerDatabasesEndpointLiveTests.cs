/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5245: <c>GET /api/server-databases</c> through a real host over a live store. The list is the user databases the
/// store has collected for ONE server, from BOTH views the desktop picker reads (<c>v_database_config</c> and
/// <c>v_database_size_stats</c>), de-duplicated, with the system databases removed, and a name that is markup travels as
/// JSON data. Skipped without <c>DARLING_TEST_PG</c>, like its siblings.
/// </summary>
/* #1776: seeds and reads registry + database rows on the SHARED store, so it serializes with the other live classes
   that do. */
[Collection("live-postgres")]
public sealed class ServerDatabasesEndpointLiveTests
{
    private const int ServerId = 5_245_001;
    private const string ServerName = "srv5245-endpoint-live";
    private const int OtherServerId = 5_245_002;
    private const string OtherServerName = "srv5245-endpoint-other";

    /// <summary>A name that is markup: it must come back as a string value, never be interpreted.</summary>
    private const string MarkupName = "<img src=x onerror=alert(1)>";

    [Fact]
    public async Task TheRoute_AnswersTheUserDatabasesOfOneServer_FromBothViews_WithSystemDatabasesRemoved_AndMarkupAsData()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #5245 server-databases route test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await InsertServerAsync(connection, ServerId, ServerName, ct);
            await InsertServerAsync(connection, OtherServerId, OtherServerName, ct);
            var now = DateTime.UtcNow;

            /* v_database_config only, v_database_size_stats only, and both (AppB); two system databases. */
            await InsertAsync(connection, "database_config", "config_id", "capture_time", ServerId, ServerName, "AppA", now, ct);
            await InsertAsync(connection, "database_config", "config_id", "capture_time", ServerId, ServerName, "AppB", now, ct);
            await InsertAsync(connection, "database_config", "config_id", "capture_time", ServerId, ServerName, "master", now, ct);
            await InsertAsync(connection, "database_size_stats", "collection_id", "collection_time", ServerId, ServerName, "AppB", now, ct);
            await InsertAsync(connection, "database_size_stats", "collection_id", "collection_time", ServerId, ServerName, "AppC", now, ct);
            await InsertAsync(connection, "database_size_stats", "collection_id", "collection_time", ServerId, ServerName, "tempdb", now, ct);
            await InsertAsync(connection, "database_size_stats", "collection_id", "collection_time", ServerId, ServerName, MarkupName, now, ct);
            /* Another server's database must not leak into this server's list. */
            await InsertAsync(connection, "database_config", "config_id", "capture_time", OtherServerId, OtherServerName, "OtherOnly", now, ct);

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            await using var app = builder.Build();
            DarlingServerDatabasesEndpoint.Map(app, postgres, app.Logger);
            await app.StartAsync(ct);
            using var client = app.GetTestClient();

            using var response = await client.GetAsync("/api/server-databases?server=" + Uri.EscapeDataString(ServerName), ct);
            var text = await response.Content.ReadAsStringAsync(ct);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("application/json", response.Content.Headers.ContentType?.MediaType);

            using var doc = JsonDocument.Parse(text);
            Assert.Equal(ServerName, doc.RootElement.GetProperty("server").GetString());
            var names = doc.RootElement.GetProperty("databases").EnumerateArray().Select(e => e.GetString()!).ToArray();

            /* Both views, de-duplicated, system databases gone, the other server's database absent. The ORDER BY is
               the database collation's, so the set is compared in ordinal order. */
            Assert.Equal(
                new[] { "AppA", "AppB", "AppC", MarkupName }.OrderBy(n => n, StringComparer.Ordinal).ToArray(),
                names.OrderBy(n => n, StringComparer.Ordinal).ToArray());

            /* Markup is data: the angle brackets are escaped in the body, so the raw text holds no tag at all. */
            Assert.DoesNotContain("<img", text, StringComparison.Ordinal);
            Assert.DoesNotContain("<", text, StringComparison.Ordinal);

            /* Only GET is mapped, and an unknown server is the shared 400 refusal, not an empty list. */
            using var post = await client.PostAsync("/api/server-databases", new System.Net.Http.StringContent("{}"), ct);
            Assert.Equal(HttpStatusCode.MethodNotAllowed, post.StatusCode);
            using var unknown = await client.GetAsync("/api/server-databases?server=" + Uri.EscapeDataString("no-such-server-5245"), ct);
            Assert.Equal(HttpStatusCode.BadRequest, unknown.StatusCode);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertServerAsync(NpgsqlConnection connection, int serverId, string serverName, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO servers (server_id, server_name, display_name, is_enabled) VALUES ($1, $2, $3, TRUE)", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, string table, string idColumn, string timeColumn,
        int serverId, string serverName, string databaseName, DateTime whenUtc, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            $"INSERT INTO {table} ({idColumn}, {timeColumn}, server_id, server_name, database_name) VALUES ($1, $2, $3, $4, $5)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(whenUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(databaseName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "database_config", "database_size_stats", "servers" })
        {
            using var command = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = ANY($1)", connection);
            command.Parameters.AddWithValue(new[] { ServerId, OtherServerId });
            await command.ExecuteNonQueryAsync(ct);
        }
    }
}

/// <summary>
/// #5245 L7: the inventory SQL deduplicates inside EACH arm. Over 500 databases, hourly for 90 days (2.2 million
/// <c>v_database_size_stats</c> rows) the bare <c>UNION</c> shape sorted every row and took a 4.4 s median; hashing each
/// arm down first took 0.26 s with the same rows. A pin, because the shape is invisible in the result.
/// </summary>
public sealed class CollectedDatabasesSqlTests
{
    [Fact]
    public void NamesSql_DeduplicatesInsideEachArm_SoTheUnionSortsNamesNotRows()
    {
        var sql = string.Join(" ", CollectedDatabases.NamesSql.Split(['\r', '\n', ' '], StringSplitOptions.RemoveEmptyEntries));

        Assert.Contains("SELECT DISTINCT database_name FROM v_database_config WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT DISTINCT database_name FROM v_database_size_stats WHERE server_id = $1", sql, StringComparison.Ordinal);
    }
}