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

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres. The managed roles it provisions carry fixed,
   cluster-wide names (admin, viewer, mcp), so the class runs in the pg-cluster-roles collection with the other provisioning tests. */

/// <summary>
/// The query tag rule tools (#5982) as the provisioned, NOSUPERUSER <c>mcp</c> role: the grants are the shipped
/// <c>DarlingManagedRoles.BuildProvisioningSql</c> batch run whole, so a missing grant (the <c>config.query_tag_state</c> read every
/// write makes, the rule table writes) fails here and not on a superuser owner connection.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class DarlingMcpQueryTagRuleToolsRoleLiveTests
{
    private const int ServerA = 7301;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonNode Parse(string json) => JsonNode.Parse(json)!;

    private static string Status(string json) => (string)Parse(json)["status"]!;

    [Fact]
    public async Task TheProvisionedMcpRole_RunsEveryRuleTool_IncludingTheRulesVersionReadEachWriteMakes()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the query tag rule tool pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            /* The managed roles carry fixed names and a role is cluster-wide: never adopt or drop a set that belongs to something else. */
            await using var probe = new NpgsqlCommand("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", owner);
            Assert.SkipWhen((bool)(await probe.ExecuteScalarAsync(ct))!, "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
        }

        var bodySucceeded = false;
        try
        {
            await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
            {
                await owner.OpenAsync(ct);
                var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";

                // The shipped batch, run whole: every grant the managed mcp role really holds.
                var batch = DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName));
                await using (var provision = new NpgsqlCommand(batch, owner))
                {
                    await provision.ExecuteNonQueryAsync(ct);
                }

                await using var seed = new NpgsqlCommand(
                    $"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES ({ServerA}, 'srv{ServerA}', 'srv{ServerA}', TRUE, 16, now(), now()); " +
                    "INSERT INTO config.query_tags (name) VALUES ('Billing')", owner);
                await seed.ExecuteNonQueryAsync(ct);
            }

            var mcpString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = "mcp",
                Password = ProvisioningTestSecrets.McpPassword,
                SearchPath = PgSchemaGenerator.SearchPath,
                Pooling = false,
            }.ConnectionString;
            await using var mcp = NpgsqlDataSource.Create(mcpString);

            var created = Parse(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(mcp, "application", "billing", 1, server_id: ServerA, cancellationToken: ct));
            Assert.Equal("created", (string)created["status"]!);
            Assert.True((long)created["rules_version"]! > 0);
            var id = (int)created["rule"]!["id"]!;

            Assert.Equal("updated", Status(await DarlingMcpQueryTagRuleTools.UpdateQueryTagRule(mcp, id, "{\"enabled\":false,\"pattern\":\"billing*\"}", ct)));
            Assert.Equal("ok", Status(await DarlingMcpQueryTagRuleTools.ListQueryTagRules(mcp, cancellationToken: ct)));
            Assert.Equal("empty", Status(await DarlingMcpQueryTagRuleTools.TestQueryTagRule(mcp, rule_id: id, cancellationToken: ct)));
            Assert.Equal("deleted", Status(await DarlingMcpQueryTagRuleTools.DeleteQueryTagRule(mcp, id, ct)));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand("DROP OWNED BY admin, viewer, web, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS web; DROP ROLE IF EXISTS mcp", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
