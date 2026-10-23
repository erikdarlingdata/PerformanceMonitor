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
/// deployment. An operator-class token (a named operator, or the legacy token with writes) sets levels 0 to 3 and journals
/// <c>route_agent_changed</c> as <c>operator:</c> and its name or identity.
/// </summary>
public sealed class McpRouteAgentFieldsLiveTests
{
    private const string Operator = "ops-lead";
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    private static DarlingMcpServedPolicy OperatorPolicy() =>
        new(new DarlingMcpWritePolicy.Effective(true, false, false, Operator));

    /// <summary>The legacy single token with writes on: no token name, operator-class.</summary>
    private static DarlingMcpServedPolicy LegacyPolicy() => new(new DarlingMcpWritePolicy.Effective(true, false));

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

    /// <summary>Erik, 10-23: MCP is full admin, so an operator token sets level 3 and changes a route already at level 3, journaled each time.</summary>
    [Fact]
    public async Task AnOperatorToken_SetsLevel3_AndChangesARouteAtLevel3_EachJournaled()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var created = await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Deadlocks", agent_level: 3, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("created", Status(created));
            var routeId = RouteIdOf(created);
            Assert.Equal("3", await ScalarAsync(owner, $"SELECT agent_level FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal("1", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND event = 'route_agent_changed' " +
                $"AND actor = 'operator:{Operator}' AND (detail -> 'after' ->> 'agent_level') = '3'", ct));

            /* A route at level 3: a limit changes, then the level is lowered, each with its own row. */
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_max_per_day: 50, served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_level: 1, served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("1|50", await ScalarAsync(owner,
                $"SELECT agent_level::text || '|' || agent_max_per_day::text FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal("1", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND actor = 'operator:{Operator}' " +
                "AND reason = 'changed agent_level' AND (detail -> 'before' ->> 'agent_level') = '3' AND (detail -> 'after' ->> 'agent_level') = '1'", ct));
            Assert.Equal("3", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND event = 'route_agent_changed'", ct));

            /* The store function holds the actor to operator:<name>, so no caller of it journals as anyone else. */
            var direct = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var command = asMcp.CreateCommand(
                    $"SELECT config.set_notification_route_agent({routeId}, 'agent:{Operator}', ARRAY['agent_level'], 2, NULL, NULL, NULL, NULL, NULL, false)");
                await command.ExecuteNonQueryAsync(ct);
            });
            Assert.Equal("DL400", direct.SqlState);
        });
    }

    /// <summary>Agreed with the coordinator, 10-23: the legacy single token served the writes is operator-class, so it sets agent levels 1 and 3
    /// and edits an agent route, each change journaled under operator: and the token identity the route write logs carry. A token served the
    /// investigations instead (writes off) is not operator-class and sets no agent field.</summary>
    [Fact]
    public async Task TheLegacyWritesToken_SetsLevels1And3_AndEachIsJournaled_ButAnInvestigationsTokenSetsNone()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var created = await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking", agent_level: 1, served: LegacyPolicy(), cancellationToken: ct);
            Assert.Equal("created", Status(created));
            var routeId = RouteIdOf(created);

            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_level: 3, served: LegacyPolicy(), cancellationToken: ct)));
            Assert.Equal("3", await ScalarAsync(owner, $"SELECT agent_level FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal("2", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND event = 'route_agent_changed' " +
                "AND actor LIKE 'operator:%' AND actor NOT LIKE 'operator:" + Operator + "'", ct));
            Assert.Equal("1", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND (detail -> 'after' ->> 'agent_level') = '3'", ct));

            /* The agent route's other fields are no longer refused for it either. */
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, send_resolutions: "always", served: LegacyPolicy(), cancellationToken: ct)));

            /* Investigations on, writes off: not operator-class, so no agent field. */
            var investigations = new DarlingMcpServedPolicy(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true));
            Assert.Equal("refused", Status(await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Deadlocks", agent_level: 1, served: investigations, cancellationToken: ct)));
            Assert.Equal("refused", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_level: 1, served: investigations, cancellationToken: ct)));
            Assert.Equal("3", await ScalarAsync(owner, $"SELECT agent_level FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
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
