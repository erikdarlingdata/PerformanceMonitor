/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database and a role with a per-run name, and touches nothing on the
   shared one, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The MCP route tools (#6231) AS the <c>mcp</c> role. The grants are TAKEN FROM <see cref="DarlingManagedRoles.BuildProvisioningSql"/>
/// (through <see cref="ViewerGrantReplay"/>, the two functions' EXECUTE grants from the batch's own statements), so a grant added to
/// or dropped from the product's SQL changes these facts. The functions are built by the owner, as in a deployment. They show that
/// create and update work without the role holding an INSERT or any destination-column privilege, that the role cannot read a
/// destination or write one directly, and that a route made this way delivers through a parent channel.
/// </summary>
public sealed class McpRouteRoleLiveTests
{
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    [Fact]
    public async Task TheMcpRole_CreatesAndEditsARoute_WithoutAnyGrantOnTheTableOrADestinationColumn()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var created = await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking", send_resolutions: "never", cancellationToken: ct);
            var routeId = JsonDocument.Parse(created).RootElement.GetProperty("route").GetProperty("route_id").GetInt32();
            Assert.Equal("created", Status(created));

            var edited = await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, metric_match: "Deadlocks", enabled: false, send_resolutions: "always", cancellationToken: ct);
            Assert.Equal("updated", Status(edited));

            await using var read = owner.CreateCommand(
                "SELECT metric_match, enabled, send_resolutions, teams_url || slack_url || generic_url || pagerduty_routing_key || smtp_recipients " +
                "FROM config.config_notification_routes WHERE route_id = $1");
            read.Parameters.AddWithValue(routeId);
            await using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal("Deadlocks", reader.GetString(0));
            Assert.False(reader.GetBoolean(1));
            Assert.True(reader.GetBoolean(2));
            Assert.Equal("", reader.GetString(3));
        });
    }

    [Theory]
    [InlineData("SELECT teams_url FROM config.config_notification_routes")]
    [InlineData("SELECT slack_url, generic_url FROM config.config_notification_routes")]
    [InlineData("SELECT pagerduty_routing_key FROM config.config_notification_routes")]
    [InlineData("SELECT signing_key FROM config.config_notification_routes")]
    [InlineData("SELECT * FROM config.config_notification_routes")]
    [InlineData("UPDATE config.config_notification_routes SET generic_url = 'http://127.0.0.1:1/x'")]
    [InlineData("UPDATE config.config_notification_routes SET teams_url = 'http://127.0.0.1:1/x', smtp_recipients = 'a@example.com'")]
    [InlineData("UPDATE config.config_notification_routes SET metric_match = 'x'")]
    [InlineData("INSERT INTO config.config_notification_routes (metric_match, generic_url) VALUES ('Blocking', 'http://127.0.0.1:1/x')")]
    [InlineData("INSERT INTO config.config_notification_routes (metric_match) VALUES ('Blocking')")]
    public async Task TheMcpRole_CannotReadASealedColumn_OrWriteADestinationOrInsertDirectly(string statement)
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            await ExecAsync(owner, "INSERT INTO config.config_notification_routes (metric_match, generic_url) VALUES ('Blocking', 'sealed-text')", ct);
            var denied = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var command = asMcp.CreateCommand(statement);
                await command.ExecuteNonQueryAsync(ct);
            });
            Assert.Equal("42501", denied.SqlState);
        });
    }

    [Fact]
    public async Task TheUpdateFunction_RechecksTheAgentRefusalUnderTheRowLock_AndNeverTouchesADestination()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            await ExecAsync(owner,
                "INSERT INTO config.config_notification_routes (metric_match, generic_url, smtp_recipients, agent_level) " +
                "VALUES ('Blocking', 'sealed-text', 'ops@example.com', 1), ('Deadlocks', 'sealed-text', 'ops@example.com', NULL)", ct);

            async Task<PostgresException> CallAsync(int routeId, string[] columns)
            {
                return await Assert.ThrowsAsync<PostgresException>(async () =>
                {
                    await using var call = asMcp.CreateCommand("SELECT config.update_notification_route($1, $2, 'x', true, NULL, NULL, NULL, NULL)");
                    call.Parameters.AddWithValue(routeId);
                    call.Parameters.AddWithValue(columns);
                    await call.ExecuteNonQueryAsync(ct);
                });
            }

            var agent = await CallAsync(1, ["metric_match"]);
            Assert.Equal("DL403", agent.SqlState);
            Assert.Equal("DL400", (await CallAsync(2, ["generic_url"])).SqlState);
            Assert.Equal("DL404", (await CallAsync(99, ["metric_match"])).SqlState);

            /* The enabled flag is still open on a route with a destination of its own. */
            await using (var call = asMcp.CreateCommand("SELECT config.update_notification_route(2, ARRAY['enabled'], 'x', false, NULL, NULL, NULL, NULL)"))
            {
                await call.ExecuteNonQueryAsync(ct);
            }

            await using var read = owner.CreateCommand(
                "SELECT metric_match, enabled, generic_url, smtp_recipients FROM config.config_notification_routes WHERE route_id = 2");
            await using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal(("Deadlocks", false, "sealed-text", "ops@example.com"),
                (reader.GetString(0), reader.GetBoolean(1), reader.GetString(2), reader.GetString(3)));
        });
    }

    /// <summary>A route with an explicit agent setting (Off) keeps its match and scope, because they decide which servers an agent route covers; a route
    /// with a destination of its own does not (MCP sets destinations too). The same run shows what stays open: the enabled flag on the Off route.
    /// The role is the mcp role, so a raw login gets the same answer.</summary>
    [Fact]
    public async Task TheUpdateFunction_RefusesAMatchOrScopeChange_OnAnAgentOffRoute_AndAllowsItOnARouteWithADestination()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            await ExecAsync(owner,
                "INSERT INTO config.config_notification_routes (metric_match, slack_url, scope_tag_id) VALUES ('Blocking', 'sealed-text', NULL), ('Deadlocks', '', NULL), ('Blocking', '', NULL);" +
                "UPDATE config.config_notification_routes SET agent_level = 0 WHERE route_id = 2;", ct);
            await ExecAsync(owner, "INSERT INTO config.server_tags (id, name) OVERRIDING SYSTEM VALUE VALUES (9001, 'prod')", ct);

            async Task CallAsync(int routeId, string columns, string match = "x", bool enabled = true, string tag = "NULL")
            {
                await using var call = asMcp.CreateCommand(
                    $"SELECT config.update_notification_route($1, ARRAY[{columns}]::text[], $2, $3, NULL, {tag}, NULL, NULL)");
                call.Parameters.AddWithValue(routeId);
                call.Parameters.AddWithValue(match);
                call.Parameters.AddWithValue(enabled);
                await call.ExecuteNonQueryAsync(ct);
            }

            /* A route with a destination of its own: a scope or a match change is allowed. */
            await CallAsync(1, "'scope'", tag: "9001");
            await CallAsync(1, "'metric_match'", match: "Deadlocks");

            /* A route whose agent setting is Off: the same refusal, with no destination on it. */
            var off = await Assert.ThrowsAsync<PostgresException>(() => CallAsync(2, "'scope'", tag: "9001"));
            Assert.Equal("DL403", off.SqlState);
            Assert.Contains("Viewer", off.MessageText, StringComparison.Ordinal);
            Assert.Equal("DL403", (await Assert.ThrowsAsync<PostgresException>(() => CallAsync(2, "'metric_match'", match: "Blocking"))).SqlState);

            /* What stays open: the enabled flag on the Off route, and a full edit of a route that has no agent setting. */
            await CallAsync(1, "'enabled'", enabled: false);
            await CallAsync(2, "'enabled'", enabled: false);
            await CallAsync(1, "'scope'", tag: "NULL");
            await CallAsync(3, "'metric_match', 'scope', 'enabled'", match: "Deadlocks", enabled: false, tag: "9001");

            await using var read = owner.CreateCommand(
                "SELECT route_id, metric_match, enabled, scope_tag_id FROM config.config_notification_routes ORDER BY route_id");
            await using var reader = await read.ExecuteReaderAsync(ct);
            var rows = new List<(int, string, bool, int?)>();
            while (await reader.ReadAsync(ct))
            {
                rows.Add((reader.GetInt32(0), reader.GetString(1), reader.GetBoolean(2), reader.IsDBNull(3) ? null : reader.GetInt32(3)));
            }

            Assert.Equal([(1, "Deadlocks", false, (int?)null), (2, "Deadlocks", false, (int?)null), (3, "Deadlocks", false, (int?)9001)], rows);
        });
    }

    /// <summary>The MCP tool's answer for a refused match change on an agent Off route says why in plain words, points at the Viewer and leaves the route as it was;
    /// the same change on a route with a destination is made.</summary>
    [Fact]
    public async Task TheUpdateTool_AnswersARefusedScopeChange_OnAnAgentOffRoute_InPlainWords_AndMakesItOnARouteWithADestination()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            await ExecAsync(owner, "INSERT INTO config.config_notification_routes (metric_match, generic_url) VALUES ('Blocking', 'sealed-text'), ('Blocking', '')", ct);
            await ExecAsync(owner, "UPDATE config.config_notification_routes SET agent_level = 0 WHERE route_id = 2", ct);

            var answer = JsonDocument.Parse(await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, 2, metric_match: "Deadlocks", fleet_wide: true, cancellationToken: ct)).RootElement;
            Assert.Equal("refused", answer.GetProperty("status").GetString());
            var message = answer.GetProperty("message").GetString()!;
            Assert.Contains("agent setting", message, StringComparison.Ordinal);
            Assert.Contains("Viewer", message, StringComparison.Ordinal);

            /* The enabled flag is still changed over MCP on the same route, and a route with a destination takes a match change. */
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, 2, enabled: false, cancellationToken: ct)));
            var edited = await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, 1, metric_match: "Deadlocks", fleet_wide: true, cancellationToken: ct);
            Assert.Equal("updated", Status(edited));
            Assert.DoesNotContain("sealed-text", edited, StringComparison.Ordinal);
        });
    }

    /// <summary>A route the mcp role made has every channel empty, so an alert it matches is delivered through the parent channel
    /// configured in Settings: the local listener behind the parent's Slack URL receives the post.</summary>
    [Fact]
    public async Task ARouteMadeByTheMcpRole_DeliversThroughAParentChannel()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            using var parent = new CapturingWebhookEndpoint();
            var created = await DarlingMcpAlertTools.CreateNotificationRoute(asMcp, "High CPU", cancellationToken: ct);
            Assert.Equal("created", Status(created));

            await using var connection = new NpgsqlConnection(owner.ConnectionString);
            await connection.OpenAsync(ct);
            var routes = await StoreConfigProvider.ReadNotificationRoutesAsync(connection, null, ct);
            var route = Assert.Single(routes);
            Assert.Equal("", route.SlackUrl + route.TeamsUrl + route.GenericUrl + route.PagerDutyRoutingKey + route.SmtpRecipients);

            var config = new DarlingConfig();
            config.Webhooks.SlackUrl = parent.Url;
            config.NotificationRoutes = routes;
            var settings = new DarlingAlertSettings(config);
            var history = new CapturingHistoryStore();
            var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
            var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance, utcNow: () => new DateTime(2026, 10, 15, 12, 0, 0, DateTimeKind.Utc));
            await deliverer.DeliverAsync(
                new AlertOutcome("7", "SQL01", "High CPU", "97%", "90%", Context: null, DetailText: null,
                    NumericCurrentValue: 97, NumericThresholdValue: 90, Muted: false, Severity: null), ct);

            Assert.Single(parent.Bodies);
        });
    }

    private static string Status(string answer) => JsonDocument.Parse(answer).RootElement.GetProperty("status").GetString()!;

    private static async Task WithMcpRoleAsync(Func<NpgsqlDataSource, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the MCP route role live tests (they mint their own scratch database and role).");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "route_mcp_" + Guid.NewGuid().ToString("N")[..8];
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
            /* The functions are created by the owner, as the managed batch does; the batch's own EXECUTE grants, retargeted. */
            await ExecAsync(owner, DarlingManagedRoles.BuildNotificationRouteFunctionsSql("config"), ct);
            await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, "mcp"))
            {
                await ExecAsync(owner, statement, ct);
            }

            var executes = ViewerGrantReplay.BatchStatements().Where(s =>
                s.StartsWith("GRANT EXECUTE ON FUNCTION config.create_notification_route", StringComparison.Ordinal)
                || s.StartsWith("GRANT EXECUTE ON FUNCTION config.update_notification_route", StringComparison.Ordinal)).ToList();
            Assert.Equal(2, executes.Count);
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
