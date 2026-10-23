/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database and a role with a per-run name, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// #6542 part 2: the route agent fields over MCP, AS the <c>mcp</c> role with the grants taken from
/// <see cref="DarlingManagedRoles.BuildProvisioningSql"/> and the functions and the journal trigger built by the owner, as in a
/// deployment. A named operator token sets levels 0 to 2 and journals <c>route_agent_changed</c> as <c>operator:&lt;name&gt;</c>. Level 3
/// is refused, and so is any agent-field change to a route already at level 3. The legacy token still sets no agent field.
/// </summary>
public sealed class McpRouteAgentFieldsLiveTests
{
    private const string Operator = "ops-lead";
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    private static DarlingMcpServedPolicy OperatorPolicy() =>
        new(new DarlingMcpWritePolicy.Effective(true, false, false, Operator));

    /// <summary>The legacy single token with writes on: no token name.</summary>
    private static DarlingMcpServedPolicy LegacyPolicy() => new(new DarlingMcpWritePolicy.Effective(true, false));

    /// <summary>The level 3 answers follow the one ceiling constant, so moving the ceiling flips them with no other test edit.</summary>
    private static readonly bool Level3OverMcp = DarlingMcpAlertTools.MaxMcpAgentLevel >= 3;

    private static string Status(string answer) => JsonDocument.Parse(answer).RootElement.GetProperty("status").GetString()!;

    private static int RouteIdOf(string answer) =>
        JsonDocument.Parse(answer).RootElement.GetProperty("route").GetProperty("route_id").GetInt32();

    private static async Task<string?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task AnOperatorToken_CreatesARouteAtLevel1_AndTheJournalNamesTheToken()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var created = await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking", agent_level: 1, agent_max_per_hour: 5, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("created", Status(created));
            var routeId = RouteIdOf(created);

            Assert.Equal("1|5", await ScalarAsync(owner,
                $"SELECT agent_level::text || '|' || agent_max_per_hour::text FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal("1", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND event = '{NotificationRouteWriter.AgentChangedEvent}' " +
                $"AND actor = 'operator:{Operator}' AND (detail -> 'after' ->> 'agent_level') = '1' AND detail -> 'before' = 'null'::jsonb", ct));

            /* An edit of the limits journals again, with the values before and after. */
            var edited = await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_level: 2, agent_lease_minutes: 30, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("updated", Status(edited));
            Assert.Equal("2|30", await ScalarAsync(owner,
                $"SELECT agent_level::text || '|' || agent_lease_minutes::text FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal("1", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND event = 'route_agent_changed' " +
                $"AND actor = 'operator:{Operator}' AND reason = 'changed agent_level, agent_lease_minutes' " +
                "AND (detail -> 'before' ->> 'agent_level') = '1' AND (detail -> 'after' ->> 'agent_level') = '2'", ct));

            /* The Viewer's rules hold: a limit out of range is invalid and nothing changes. */
            var outOfRange = await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_max_per_hour: 500, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("invalid", Status(outOfRange));
            Assert.Equal("5", await ScalarAsync(owner, $"SELECT agent_max_per_hour FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
        });
    }

    [Fact]
    public async Task Level3_IsRefusedOverMcp_AndARouteAlreadyAtLevel3_IsNotChanged_LoweringIncluded()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            /* Level 3 asked for on a create: refused, and no route is left behind. */
            var create = await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Deadlocks", agent_level: 3, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal(Level3OverMcp ? "created" : "refused", Status(create));
            Assert.Equal(Level3OverMcp ? "1" : "0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_notification_routes WHERE metric_match = 'Deadlocks'", ct));

            var routeId = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking", agent_level: 2, served: OperatorPolicy(), cancellationToken: ct));
            var raise = await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_level: 3, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal(Level3OverMcp ? "updated" : "refused", Status(raise));
            Assert.Equal(Level3OverMcp ? "3" : "2", await ScalarAsync(owner, $"SELECT agent_level FROM config.config_notification_routes WHERE route_id = {routeId}", ct));

            /* A person set level 3 in the Viewer: MCP cannot lower it, change a limit or move its fallback. */
            await using (var person = owner.CreateCommand($"UPDATE config.config_notification_routes SET agent_level = 3 WHERE route_id = {routeId}"))
            {
                await person.ExecuteNonQueryAsync(ct);
            }

            Assert.Equal(Level3OverMcp ? "updated" : "refused", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_max_per_day: 50, served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal(Level3OverMcp ? "3|50" : "3|100", await ScalarAsync(owner,
                $"SELECT agent_level::text || '|' || agent_max_per_day::text FROM config.config_notification_routes WHERE route_id = {routeId}", ct));

            Assert.Equal(Level3OverMcp ? "updated" : "refused", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_level: 1, served: OperatorPolicy(), cancellationToken: ct)));
            if (Level3OverMcp)
            {
                return;
            }

            /* The store function refuses it too, so a caller that skips the tool's own check is held as well. */
            var direct = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var command = asMcp.CreateCommand(
                    $"SELECT config.set_notification_route_agent({routeId}, 'operator:{Operator}', ARRAY['agent_level'], 1, NULL, NULL, NULL, NULL, NULL, false)");
                await command.ExecuteNonQueryAsync(ct);
            });
            Assert.Equal("DL403", direct.SqlState);
            Assert.Equal("0", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND (detail -> 'after' ->> 'agent_level') = '3'", ct));
        });
    }

    [Fact]
    public async Task TheLegacyToken_StillSetsNoAgentField_AndStillCannotEditAnAgentRoute()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var create = await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking", agent_level: 1, served: LegacyPolicy(), cancellationToken: ct);
            Assert.Equal("refused", Status(create));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_notification_routes", ct));

            var routeId = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(asMcp, "Blocking", served: LegacyPolicy(), cancellationToken: ct));
            Assert.Equal("refused", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_level: 1, served: LegacyPolicy(), cancellationToken: ct)));

            /* A route a person made an agent route: the legacy token is refused on any field, as before part 2. */
            await using (var person = owner.CreateCommand($"UPDATE config.config_notification_routes SET agent_level = 1 WHERE route_id = {routeId}"))
            {
                await person.ExecuteNonQueryAsync(ct);
            }

            Assert.Equal("refused", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, send_resolutions: "always", served: LegacyPolicy(), cancellationToken: ct)));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'route_agent_changed'", ct));
        });
    }

    private static async Task WithMcpRoleAsync(Func<NpgsqlDataSource, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the route agent field live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "route_agent_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            /* The functions and the journal trigger are created by the owner, as the managed batch does; the batch's own grants, retargeted. */
            await ExecAsync(owner, DarlingManagedRoles.BuildNotificationRouteFunctionsSql("config"), ct);
            await ExecAsync(owner, DarlingManagedRoles.BuildJournalEventRulesSql("config"), ct);
            await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, "mcp"))
            {
                await ExecAsync(owner, statement, ct);
            }

            var executes = ViewerGrantReplay.BatchStatements().Where(s =>
                s.StartsWith("GRANT EXECUTE ON FUNCTION config.create_notification_route", StringComparison.Ordinal)
                || s.StartsWith("GRANT EXECUTE ON FUNCTION config.update_notification_route", StringComparison.Ordinal)
                || s.StartsWith("GRANT EXECUTE ON FUNCTION config.set_notification_route_agent", StringComparison.Ordinal)).ToList();
            Assert.Equal(3, executes.Count);
            foreach (var execute in executes)
            {
                var at = execute.LastIndexOf(" TO ", StringComparison.Ordinal);
                Assert.Equal("mcp", execute[(at + 4)..].Trim());
                await ExecAsync(owner, execute[..at] + " TO " + roleName, ct);
            }

            await using var asMcp = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = RolePassword,
                Pooling = false,
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
