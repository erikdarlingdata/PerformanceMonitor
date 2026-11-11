/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8573 gated live test (DARLING_TEST_PG): the <c>pg-connection-saturation</c> starter reads the connected backend
/// count from <c>pg_database_stats.numbackends</c>, summed across the databases of ONE capture, so it has a value on a
/// capture that stored no <c>pg_session_states</c> row (that table gets a row only while a session holds a transaction
/// old enough to capture). The rule goes through the same evaluator <c>test_custom_alert_rule</c> runs.
/// Two databases and two captures are seeded so a sum across captures (37) or a per-row maximum (20) would show.
/// </summary>
[Collection("live-postgres")]
public sealed class PgConnectionSaturationTemplateLiveTests
{
    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the saturation template live test.");
        return connectionString!;
    }

    private static string TemplateScopedTo(string storageName, string aggregate)
    {
        var template = CustomAlertTemplates.All.Single(t => t.Key == "pg-connection-saturation");
        var node = (JsonObject)JsonNode.Parse(template.DefinitionJson)!;
        ((JsonObject)node["metric"]!)["aggregate"] = aggregate;
        node["scope"] = new JsonObject
        {
            ["mode"] = "servers",
            ["servers"] = new JsonArray(storageName),
        };
        return node.ToJsonString();
    }

    [Fact]
    public async Task TheSaturationTemplate_ReadsTheBackendSumOfOneCapture_WithNoSessionRow()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;

        await using (var migrate = new NpgsqlConnection(connectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        var dataSourceConnectionString = new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString;
        await using var dataSource = NpgsqlDataSource.Create(dataSourceConnectionString);

        const int serverId = 991573;
        var storageName = "sat_tpl_srv_" + Guid.NewGuid().ToString("N");
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        var bodySucceeded = false;
        try
        {
            await using (var insert = dataSource.CreateCommand(
                "INSERT INTO servers (server_id, server_name, display_name) VALUES ($1, $2, $2)"))
            {
                insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = storageName });
                await insert.ExecuteNonQueryAsync(ct);
            }

            // Capture one (10 minutes ago): 3 + 4 backends. Capture two (5 minutes ago): 10 + 20. No pg_session_states row at all.
            var captures = new[]
            {
                (Minutes: 10, First: 3, Second: 4),
                (Minutes: 5, First: 10, Second: 20),
            };
            var collectionId = 9915730L;
            foreach (var capture in captures)
            {
                foreach (var (database, backends) in new[] { ("db_one", capture.First), ("db_two", capture.Second) })
                {
                    await using var seed = dataSource.CreateCommand(
                        "INSERT INTO pg_database_stats (collection_id, collection_time, server_id, server_name, database_name, numbackends) "
                        + "VALUES ($1, $2, $3, $4, $5, $6)");
                    seed.Parameters.Add(new NpgsqlParameter<long> { TypedValue = collectionId++ });
                    seed.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.AddMinutes(-capture.Minutes) });
                    seed.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                    seed.Parameters.Add(new NpgsqlParameter<string> { TypedValue = storageName });
                    seed.Parameters.Add(new NpgsqlParameter<string> { TypedValue = database });
                    seed.Parameters.Add(new NpgsqlParameter<int> { TypedValue = backends });
                    await seed.ExecuteNonQueryAsync(ct);
                }
            }

            // The template as shipped (max): the larger capture's sum, 10 + 20.
            var maxRow = await RunAsync(dataSource, TemplateScopedTo(storageName, "max"));
            Assert.False((bool)maxRow["no_data"]!, "no session row is stored, but the backend count is");
            Assert.Equal(30d, (double)maxRow["current_value"]!, 6);

            // Average over the captures: the mean of the per-capture sums, (7 + 30) / 2, not of the four per-database rows (9.25).
            var avgRow = await RunAsync(dataSource, TemplateScopedTo(storageName, "avg"));
            Assert.Equal(18.5d, (double)avgRow["current_value"]!, 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using (var setPath = new NpgsqlCommand("SET search_path TO collect, config, public", cleanup))
                {
                    await setPath.ExecuteNonQueryAsync(cleanupCt);
                }

                using (var stats = new NpgsqlCommand("DELETE FROM pg_database_stats WHERE server_id = $1", cleanup))
                {
                    stats.Parameters.AddWithValue(serverId);
                    await stats.ExecuteNonQueryAsync(cleanupCt);
                }

                using var server = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", cleanup);
                server.Parameters.AddWithValue(serverId);
                await server.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task<JsonObject> RunAsync(NpgsqlDataSource dataSource, string definition)
    {
        var json = await DarlingMcpCustomAlertTools.TestCustomAlertRule(dataSource, rule_id: null, definition: definition);
        var response = (JsonObject)JsonNode.Parse(json)!;
        var results = Assert.IsType<JsonArray>(response["results"], exactMatch: false);
        return (JsonObject)Assert.Single(results)!;
    }
}
