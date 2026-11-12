/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres. The managed roles it provisions carry fixed,
   cluster-wide names (admin, viewer, mcp), so the class runs in the pg-cluster-roles collection with the other provisioning tests. */

/// <summary>
/// The route tools' own destinations (#6231) as the provisioned, NOSUPERUSER <c>mcp</c> role: the grants are the shipped
/// <c>DarlingManagedRoles.BuildProvisioningSql</c> batch run whole. A route made over MCP with a webhook opens only as its own route,
/// the Viewer reads and edits what MCP set, a value that cannot be sent to is refused at save, an answer holds no URL or key, and the role cannot touch a sealed column except through the store's function.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class McpRouteDestinationsLiveTests
{
    private const string SlackUrl = "https://hooks.example.test/services/T000/B000/slack-secret-token";
    private const string TeamsUrl = "https://teams.example.test/webhook/teams-secret-token";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    private static string Status(string json) => Parse(json).GetProperty("status").GetString()!;

    private static async Task<long> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<string> TextAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

    [Fact]
    public async Task AMcpRoute_WithDestinations_IsSealedForItsOwnRow_TheViewerEditsIt_AndNoAnswerCarriesAValue()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the MCP route destination checks.");
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
                var batch = DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName));
                await using var provision = new NpgsqlCommand(batch, owner);
                await provision.ExecuteNonQueryAsync(ct);
            }

            var mcpString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = "mcp",
                Password = ProvisioningTestSecrets.McpPassword,
                SearchPath = PgSchemaGenerator.SearchPath,
                Pooling = false,
            }.ConnectionString;
            await using var mcp = NpgsqlDataSource.Create(mcpString);
            await using var owned = NpgsqlDataSource.Create(scratch.ConnectionString);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            using var key = PasswordPrivateKey.Generate();
            var ring = DarlingPasswordKey.FromPrivateKey(key);

            /* A route made over MCP with a Slack webhook, a Teams webhook and two recipients. */
            var created = await DarlingMcpAlertTools.CreateNotificationRouteAsync(
                mcp, "Blocking", true, null, null, null, null,
                new DarlingMcpAlertTools.RouteDestinationRequest(TeamsUrl, SlackUrl, null, null, "ops@example.com, dba@example.org"),
                ring, null, ct);
            Assert.Equal("created", Status(created));
            foreach (var secret in new[] { "slack-secret-token", "teams-secret-token", SlackUrl, "sealed:" })
            {
                Assert.DoesNotContain(secret, created, StringComparison.Ordinal);
            }

            var doc = Parse(created);
            var routeId = doc.GetProperty("route").GetProperty("route_id").GetInt32();
            var now = doc.GetProperty("destinations").GetProperty("now");
            Assert.Equal("slack webhook at hooks.example.test", now.GetProperty("slack").GetString());
            Assert.Equal("teams webhook at teams.example.test", now.GetProperty("teams").GetString());
            Assert.Equal("ops@example.com, dba@example.org", now.GetProperty("email").GetString());

            /* The stored value is sealed for this route's own row: it opens there and nowhere else. */
            var sealedSlack = await TextAsync(owned, $"SELECT slack_url FROM config_notification_routes WHERE route_id = {routeId}", ct);
            Assert.StartsWith("sealed:", sealedSlack, StringComparison.Ordinal);
            Assert.Equal(SlackUrl, ring.Open(sealedSlack, PasswordBinding.ForWebhook("slack", PasswordBinding.WebhookRouteRow(routeId), "", null)));
            Assert.Throws<PasswordSealException>(() => ring.Open(sealedSlack, PasswordBinding.ForWebhook("slack", PasswordBinding.WebhookRouteRow(routeId + 1), "", null)));
            Assert.Throws<PasswordSealException>(() => ring.Open(sealedSlack, PasswordBinding.ForWebhook("teams", PasswordBinding.WebhookRouteRow(routeId), "", null)));

            /* Copying the sealed value to another route leaves it unopenable there. */
            var other = Parse(await DarlingMcpAlertTools.CreateNotificationRouteAsync(
                mcp, "Deadlocks", true, null, null, null, null, new DarlingMcpAlertTools.RouteDestinationRequest(null, null, null, null, "x@example.com"), ring, null, ct))
                .GetProperty("route").GetProperty("route_id").GetInt32();
            await using (var copy = owned.CreateCommand("UPDATE config_notification_routes SET slack_url = (SELECT slack_url FROM config_notification_routes WHERE route_id = $1) WHERE route_id = $2"))
            {
                copy.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
                copy.Parameters.Add(new NpgsqlParameter<int> { TypedValue = other });
                await copy.ExecuteNonQueryAsync(ct);
            }

            var copied = await TextAsync(owned, $"SELECT slack_url FROM config_notification_routes WHERE route_id = {other}", ct);
            Assert.Throws<PasswordSealException>(() => ring.Open(copied, PasswordBinding.ForWebhook("slack", PasswordBinding.WebhookRouteRow(other), "", null)));

            /* The Viewer reads what MCP set and edits the route (a blank box keeps the sealed value): the webhook still opens afterwards. */
            var rows = await viewer.GetNotificationRoutesAsync(ct);
            var storedRow = rows.Single(r => r.RouteId == routeId);
            Assert.Equal("ops@example.com, dba@example.org", storedRow.SmtpRecipients);
            var typed = storedRow.Clone();
            typed.TeamsUrl = ViewerWebhookSealing.CarryKept("", storedRow.TeamsUrl);
            typed.SlackUrl = ViewerWebhookSealing.CarryKept("", storedRow.SlackUrl);
            typed.SmtpRecipients = "ops@example.com";
            await viewer.SaveNotificationRouteSealedAsync(typed, storedRow, NotificationRow.Defaults(), new ViewerPasswordSealer(key.PublicKey), null, ct);
            var afterViewer = await TextAsync(owned, $"SELECT slack_url FROM config_notification_routes WHERE route_id = {routeId}", ct);
            Assert.Equal(SlackUrl, ring.Open(afterViewer, PasswordBinding.ForWebhook("slack", PasswordBinding.WebhookRouteRow(routeId), "", null)));
            Assert.Equal("ops@example.com", await TextAsync(owned, $"SELECT smtp_recipients FROM config_notification_routes WHERE route_id = {routeId}", ct));

            /* An edit over MCP changes the generic URL and removes Slack: the answer says saved or none (no host for the generic URL), never a value, and the undo says to pass the earlier value again. */
            var edited = await DarlingMcpAlertTools.UpdateNotificationRouteAsync(
                mcp, routeId, null, null, null, null, null, null, null, null,
                new DarlingMcpAlertTools.RouteDestinationRequest(null, "-", "https://generic.example.test/in/generic-secret-token", null, null), ring, null, ct);
            Assert.Equal("updated", Status(edited));
            foreach (var secret in new[] { "generic-secret-token", "slack-secret-token", "sealed:" })
            {
                Assert.DoesNotContain(secret, edited, StringComparison.Ordinal);
            }

            var editDoc = Parse(edited);
            var destinations = editDoc.GetProperty("destinations");
            Assert.Equal("none", destinations.GetProperty("now").GetProperty("slack").GetString());
            Assert.Equal("slack webhook saved", destinations.GetProperty("previous").GetProperty("slack").GetString());
            Assert.Equal("generic webhook saved", destinations.GetProperty("now").GetProperty("generic").GetString());
            Assert.Contains("pass its earlier value again", editDoc.GetProperty("undo").GetProperty("destinations").GetString(), StringComparison.Ordinal);
            Assert.Equal("", await TextAsync(owned, $"SELECT slack_url FROM config_notification_routes WHERE route_id = {routeId}", ct));

            /* A match change on a route with its own destination works now; the destinations stay. */
            var rematched = await DarlingMcpAlertTools.UpdateNotificationRouteAsync(
                mcp, routeId, "Deadlocks", null, null, null, true, null, null, null,
                new DarlingMcpAlertTools.RouteDestinationRequest(null, null, null, null, null), ring, null, ct);
            Assert.Equal("updated", Status(rematched));
            Assert.Equal("Deadlocks", await TextAsync(owned, $"SELECT metric_match FROM config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.StartsWith("sealed:", await TextAsync(owned, $"SELECT generic_url FROM config_notification_routes WHERE route_id = {routeId}", ct), StringComparison.Ordinal);

            /* A route with an explicit agent setting keeps its match and scope, and an agent route keeps everything. */
            await using (var seed = owned.CreateCommand(
                "INSERT INTO config_notification_routes (metric_match, generic_url, agent_level) VALUES ('Blocking', '', 0), ('Blocking', '', 1)"))
            {
                await seed.ExecuteNonQueryAsync(ct);
            }

            var offId = (int)await ScalarAsync(owned, "SELECT route_id FROM config_notification_routes WHERE agent_level = 0", ct);
            var agentId = (int)await ScalarAsync(owned, "SELECT route_id FROM config_notification_routes WHERE agent_level = 1", ct);
            var none = new DarlingMcpAlertTools.RouteDestinationRequest(null, null, null, null, null);
            Assert.Equal("refused", Status(await DarlingMcpAlertTools.UpdateNotificationRouteAsync(
                mcp, offId, "Deadlocks", null, null, null, null, null, null, null, none, ring, null, ct)));
            Assert.Equal("refused", Status(await DarlingMcpAlertTools.UpdateNotificationRouteAsync(
                mcp, agentId, null, false, null, null, null, null, null, null,
                new DarlingMcpAlertTools.RouteDestinationRequest(null, null, null, null, "a@example.com"), ring, null, ct)));
            Assert.Equal("", await TextAsync(owned, $"SELECT smtp_recipients FROM config_notification_routes WHERE route_id = {agentId}", ct));

            /* A URL that cannot be sent to is refused at save, and a refused create leaves no route behind. */
            var before = await ScalarAsync(owned, "SELECT count(*) FROM config_notification_routes", ct);
            foreach (var bad in new[] { "not a url", "ftp://hooks.example.test/x", "http://169.254.169.254/latest/meta-data", "https://user:pw@hooks.example.test/x" })
            {
                var refused = await DarlingMcpAlertTools.CreateNotificationRouteAsync(
                    mcp, "Blocking", true, null, null, null, null, new DarlingMcpAlertTools.RouteDestinationRequest(null, bad, null, null, null), ring, null, ct);
                Assert.Equal("invalid", Status(refused));
                Assert.DoesNotContain("169.254.169.254", refused, StringComparison.Ordinal);
            }

            var badRecipients = await DarlingMcpAlertTools.CreateNotificationRouteAsync(
                mcp, "Blocking", true, null, null, null, null, new DarlingMcpAlertTools.RouteDestinationRequest(null, null, null, null, "not an address"), ring, null, ct);
            Assert.Equal("invalid", Status(badRecipients));
            Assert.DoesNotContain("not an address", badRecipients, StringComparison.Ordinal);
            Assert.Equal(before, await ScalarAsync(owned, "SELECT count(*) FROM config_notification_routes", ct));

            /* The role itself cannot read or write a sealed column, or insert a route; the function stores only a sealed value or an empty one. */
            foreach (var statement in new[]
            {
                "UPDATE config_notification_routes SET slack_url = 'sealed:v1:x'",
                "UPDATE config_notification_routes SET generic_url = ''",
                "INSERT INTO config_notification_routes (metric_match, slack_url) VALUES ('Blocking', 'sealed:v1:x')",
                "SELECT slack_url FROM config_notification_routes",
            })
            {
                await using var command = mcp.CreateCommand(statement);
                var denied = await Assert.ThrowsAsync<PostgresException>(() => command.ExecuteNonQueryAsync(ct));
                Assert.Equal("42501", denied.SqlState);
            }

            await using (var plain = mcp.CreateCommand(
                $"SELECT * FROM config.set_notification_route_destinations({routeId}, NULL, ARRAY['slack_url'], '', 'https://hooks.example.test/plain', '', '', '')"))
            {
                var refusedPlain = await Assert.ThrowsAsync<PostgresException>(() => plain.ExecuteNonQueryAsync(ct));
                Assert.Equal("DL400", refusedPlain.SqlState);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand($"{LiveCleanupBatch.DropOwnedByExistingProductRolesSql} DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS web; DROP ROLE IF EXISTS mcp", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
