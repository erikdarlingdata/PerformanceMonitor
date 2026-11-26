/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the facts mint their own scratch database and a role with a per-run name, and touch nothing on the shared one, so the
   class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #9728: the agent action gate reads the notification routes on the <c>mcp</c> role's connection. That role is refused the destination and
/// signing key columns on purpose (#6231, #6394), so a route read that names one failed every agent action with 42501. The grants are taken from
/// <see cref="DarlingManagedRoles.BuildProvisioningSql"/> (through <see cref="ViewerGrantReplay"/>), so a grant added to or dropped from the
/// product's SQL changes these facts.
/// </summary>
public sealed class AgentActionGateMcpRoleLiveTests
{
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    /// <summary>Every column the gate's route read names is one the <c>mcp</c> role may SELECT, so it cannot name a secret column.</summary>
    [Fact]
    public void TheGatesRouteColumns_AreAllNonSecretColumnsOfTheRoutesTable()
    {
        var acl = DarlingManagedRoles.ViewerRestrictedConfigTables.Single(t => t.Table == "config_notification_routes");

        Assert.Empty(StoreConfigProvider.NotificationRoutesAgentColumns.Except(acl.NonSecretColumns));
        Assert.Empty(StoreConfigProvider.NotificationRoutesAgentColumns.Intersect(acl.SecretColumns));
    }

    /// <summary>
    /// The gate's route read as the managed <c>mcp</c> role answers the routes' agent settings with their destinations empty, and the full read
    /// (the destination columns) stays refused, so the fix did not widen the role's grants.
    /// </summary>
    [Fact]
    public async Task TheGatesRouteRead_AsTheMcpRole_ReturnsTheAgentSettings_WhileTheFullReadStaysRefused()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            await SeedRoutesAsync(owner, ct);

            await using (var connection = await asMcp.OpenConnectionAsync(ct))
            {
                var resolve = await DarlingAgentActionGate.LoadRoutesAsync(connection, NullLogger.Instance, ct);
                var route = resolve("Blocking", 7, null);
                Assert.NotNull(route);
                Assert.Equal(2, route!.AgentLevel);
                Assert.Equal("required", route.ActionApproval);
                Assert.Equal("", route.SlackUrl + route.TeamsUrl + route.GenericUrl + route.PagerDutyRoutingKey);
            }

            await using var denied = await asMcp.OpenConnectionAsync(ct);
            var refused = await Assert.ThrowsAsync<PostgresException>(() => StoreConfigProvider.ReadNotificationRoutesAsync(denied, null, ct));
            Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, refused.SqlState);
        });
    }

    /// <summary>
    /// One real agent action path as the <c>mcp</c> role: the gate runs its checks on the claimed request, reading the routes on the mcp
    /// connection, and lets the change through.
    /// </summary>
    [Fact]
    public async Task AnAgentAction_OnAClaimedRequest_RunsAsTheMcpRole()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            await SeedRoutesAsync(owner, ct);

            /* The service opens the request on its own connection. The agent claims and acts through the mcp tools, so on the mcp connection. */
            var opened = await new InvestigationRequestStore(owner, 30).OpenOrJoinAsync(
                new InvestigationAlert(1, 7, "srv-7", "blocking", "Blocking", DateTime.UtcNow, Array.Empty<string>(), null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240)
                { AutonomyLevel = 2 }, ct);
            var request = opened.RequestId!.Value;
            var store = new InvestigationRequestStore(asMcp, 30);
            var claim = (await store.ClaimAsync(request, "agent-a", ct)).ClaimId!.Value;

            var writes = 0;
            var hooks = new AgentActionHooks
            {
                LoadRoutes = (connection, _, token) => DarlingAgentActionGate.LoadRoutesAsync(connection, NullLogger.Instance, token),
                ReadBefore = (_, _, _) => Task.FromResult<string?>(null),
                Write = (_, _, _) => { writes++; return Task.FromResult<string?>("{}"); },
            };
            var call = new AgentActionCall("create_mute_rule", "mute", new AgentAction(request, claim, "agent-a", "known batch",
                "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"12\"}]"), new AgentActionTarget(7, "srv-7", null, null));

            var done = await store.RunAgentActionAsync(call, hooks, ct);

            Assert.True(done.Done, done.Refusal?.Message);
            Assert.Equal(1, writes);
        });
    }

    /// <summary>A server-scoped route (so the scope read runs) holding a destination, and a fleet route with none. The owner seeds them.</summary>
    private static async Task SeedRoutesAsync(NpgsqlDataSource owner, CancellationToken ct)
    {
        await ExecAsync(owner, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'host-a') ON CONFLICT DO NOTHING", ct);
        await ExecAsync(owner,
            "INSERT INTO config.config_notification_routes (metric_match, slack_url, scope_server_id, agent_level, action_approval) "
            + "VALUES ('Blocking', 'https://hooks.example.test/secret', 7, 2, 'required')", ct);
    }

    private static async Task WithMcpRoleAsync(Func<NpgsqlDataSource, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the agent gate mcp role live tests (they mint their own scratch database and role).");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "gate_mcp_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var owner = NpgsqlDataSource.Create(
            new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, "mcp"))
            {
                await ExecAsync(owner, statement, ct);
            }

            await using var asMcp = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = RolePassword,
                Pooling = false,
                SearchPath = PgSchemaGenerator.SearchPath,
            }.ConnectionString);

            await body(owner, asMcp, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}
