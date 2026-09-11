/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3299 gated live round-trip (DARLING_TEST_PG) for the evaluate-now tool's PLUMBING: server resolution
/// (<see cref="DarlingServerResolver.LoadEnabledAsync"/>) + scope filtering + the shared
/// <see cref="CustomAlertEvaluator.EvaluateScalarNowAsync"/> seam compiling and running the rule's metric on
/// the store + the JSON shape. It cannot be a pure test (the seam runs real compose SQL). On a freshly-migrated
/// store the collect tables are empty, so the metric reads no-data — which is exactly the wiring this proves:
/// the query runs against a real (empty) table and comes back null rather than throwing.
/// </summary>
[Collection("live-postgres")]
public sealed class CustomAlertEvaluateNowLiveTests
{
    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the evaluate-now live test.");
        return connectionString!;
    }

    [Fact]
    public async Task TestCustomAlertRule_RunsTheMetricPerInScopeServer_AndReportsNoDataOnAnEmptyStore()
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

        const int serverId = 991301;
        var storageName = "car_eval_srv_" + Guid.NewGuid().ToString("N");
        var ruleName = "car_eval_" + Guid.NewGuid().ToString("N");
        var definition =
            "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":0.25}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}," +
            "\"scope\":{\"mode\":\"servers\",\"servers\":[\"" + storageName + "\"]}}";

        var bodySucceeded = false;
        long ruleId = 0;
        try
        {
            // Register the synthetic server so the resolver's in-scope filter finds exactly it (the rule is
            // scoped to this storage name, so no other server on the store is in scope — deterministic).
            await using (var insert = dataSource.CreateCommand(
                "INSERT INTO servers (server_id, server_name, display_name) VALUES ($1, $2, $3)"))
            {
                insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = storageName });
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "Eval Now Test Server" });
                await insert.ExecuteNonQueryAsync(ct);
            }

            var created = Assert.IsType<CustomAlertRuleResult.Ok>(
                await new CustomAlertRuleStore(dataSource).CreateAsync(ruleName, null, definition, true, "test", ct));
            ruleId = created.Rule!.Id;

            var json = await DarlingMcpCustomAlertTools.TestCustomAlertRule(dataSource, ruleId, definition: null);
            var response = (JsonObject)JsonNode.Parse(json)!;

            Assert.Equal(ruleId, (long?)response["rule_id"]);
            Assert.Equal(ruleName, (string?)response["name"]);
            Assert.False(string.IsNullOrEmpty((string?)response["note"])); // the hysteresis caveat is always present

            var results = (JsonArray)response["results"]!;
            var only = Assert.Single(results);
            var row = (JsonObject)only!;
            Assert.Equal("Eval Now Test Server", (string?)row["server"]);
            Assert.Null(row["current_value"]);          // empty store -> no data
            Assert.True((bool)row["no_data"]!);
            Assert.False((bool)row["breaching"]!);       // no-data is never a breach
            Assert.Null(row["severity"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                // The cleanup connection has no search_path; `servers` is created unqualified (its schema
                // depends on the migrate session's path), so set the store's path and use bare names — one
                // statement per command since Npgsql positional params can't ride a multi-statement batch.
                using (var setPath = new NpgsqlCommand("SET search_path TO collect, config, public", cleanup))
                {
                    await setPath.ExecuteNonQueryAsync(cleanupCt);
                }

                using (var rule = new NpgsqlCommand("DELETE FROM custom_alert_rules WHERE name = $1", cleanup))
                {
                    rule.Parameters.AddWithValue(ruleName);
                    await rule.ExecuteNonQueryAsync(cleanupCt);
                }

                using var server = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", cleanup);
                server.Parameters.AddWithValue(serverId);
                await server.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
