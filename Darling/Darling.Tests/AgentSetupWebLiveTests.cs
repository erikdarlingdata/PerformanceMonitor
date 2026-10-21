/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// The web dashboard's agent setup writes (#6264) on a live store, through the web routes: a route's agent channel created, edited and
/// deleted, and the environment profiles saved and removed, each with the Viewer's rules and the Viewer's journal rows. The routes run as the
/// managed <c>admin</c> role (they write) and as the managed <c>viewer</c> role (they list, and every write answers 403 and changes nothing),
/// the grants replayed from the shipped provisioning SQL.
/// </summary>
public sealed class AgentSetupWebLiveTests
{
    private const int ServerId = 7_626_401;
    private const string ServerName = "LcAgentSetupSrv";
    private const string RolePassword = "agent-setup-live-pw";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private delegate Task<(int Status, JsonNode? Body)> Sender(string method, string path, string? contentType, string? json = null);

    private static async Task<string?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<NpgsqlDataSource> MigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        return NpgsqlDataSource.Create(scratch.ConnectionString);
    }

    /// <summary>A route with one saved email destination, written by the same code the Viewer saves through.</summary>
    private static Task<int> SeedRouteAsync(NpgsqlDataSource owner, string match, CancellationToken ct) =>
        NotificationRouteWriter.SaveAsync(
            owner, new NotificationRouteDraft { MetricMatch = match, SmtpRecipients = "ops@example.com" }, null,
            new WebhookProxies(null, null, null, null), null, null, 30, ct);

    private static async Task WithWebRoutesAsync(
        string managedRole, Func<NpgsqlDataSource, NpgsqlDataSource, Sender, CancellationToken, Task> body, IPasswordKeyRing? ring = null)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the agent setup web live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "setup_seat_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = await MigratedAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, managedRole))
            {
                await ExecAsync(owner, statement, ct);
            }

            var asSeat = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = RolePassword,
                SearchPath = "config,public",
                Pooling = false,
            }.ConnectionString;
            await using var webLogin = NpgsqlDataSource.Create(asSeat);

            var builder = WebApplication.CreateBuilder(new WebApplicationOptions
            {
                ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
                WebRootPath = "wwwroot",
            });
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton(webLogin);
            await using var app = builder.Build();
            DarlingWebEndpoints.MapAll(app, webLogin, new CollectorRuntimeState(), new CapturingTestLogger(), passwordKeys: ring);
            await app.StartAsync(ct);
            using var server = app.GetTestServer();

            async Task<(int Status, JsonNode? Body)> Send(string method, string path, string? contentType, string? json = null)
            {
                var context = await server.SendAsync(request =>
                {
                    request.Request.Method = method;
                    request.Request.Path = path;
                    request.Request.Headers.Host = "localhost";
                    if (contentType is not null)
                    {
                        request.Request.ContentType = contentType;
                    }

                    request.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(json ?? ""));
                });
                using var reader = new StreamReader(context.Response.Body);
                var text = await reader.ReadToEndAsync(ct);
                return (context.Response.StatusCode, string.IsNullOrWhiteSpace(text) ? null : JsonNode.Parse(text));
            }

            await body(owner, webLogin, Send, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (connection, token) =>
            {
                NpgsqlConnection.ClearAllPools();
                await using var drop = new NpgsqlCommand($"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName}", connection);
                await drop.ExecuteNonQueryAsync(token);
            });
        }
    }

    private const string Json = "application/json";

    [Fact]
    public async Task AgentRoutes_CreateEditDelete_RoundTrip_KeepTheRoutesDestinations_AndDeleteClearsFallbacks()
    {
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            var access = await Send("GET", "/api/agent-setup/access", null);
            Assert.True((bool)access.Body!["can_edit_routes"]!);
            Assert.True((bool)access.Body["can_edit_profiles"]!);

            /* Create: an agent-only route (no destination) at Level 1 with its limits, scoped to one server. */
            var created = await Send("POST", "/api/notification-routes", Json,
                $"{{\"metric_match\":\"blocking\",\"scope_server_id\":{ServerId},\"agent_level\":1,\"agent_max_per_hour\":5,\"agent_max_per_day\":40,\"agent_lease_minutes\":20,\"agent_expiry_minutes\":300}}");
            Assert.Equal(200, created.Status);
            Assert.Equal("created", (string)created.Body!["status"]!);
            var id = (int)created.Body["route_id"]!;
            Assert.Equal($"blocking|{ServerId}|1|5|40|20|300|true", await ScalarAsync(owner,
                $"SELECT metric_match || '|' || scope_server_id || '|' || agent_level || '|' || agent_max_per_hour || '|' || agent_max_per_day || '|' || agent_lease_minutes || '|' || agent_expiry_minutes || '|' || enabled::text FROM config_notification_routes WHERE route_id = {id}", ct));

            /* Edit: only the fields sent change, and a route's saved email destination is carried forward untouched. */
            var seeded = await SeedRouteAsync(owner, "Analysis: Waits", ct);
            var edited = await Send("PUT", $"/api/notification-routes/{seeded}", Json,
                "{\"agent_level\":3,\"agent_max_per_hour\":60,\"agent_max_per_day\":500,\"enabled\":false}");
            Assert.Equal(200, edited.Status);
            Assert.Equal("updated", (string)edited.Body!["status"]!);
            Assert.Equal("Analysis: Waits|ops@example.com|3|60|500|false", await ScalarAsync(owner,
                $"SELECT metric_match || '|' || smtp_recipients || '|' || agent_level || '|' || agent_max_per_hour || '|' || agent_max_per_day || '|' || enabled::text FROM config_notification_routes WHERE route_id = {seeded}", ct));

            /* Level 0 (Off) and Inherit are both settable. */
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"agent_level\":0}")).Status);
            Assert.Equal("0", await ScalarAsync(owner, $"SELECT agent_level FROM config_notification_routes WHERE route_id = {seeded}", ct));
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"agent_level\":null}")).Status);
            Assert.Equal("", await ScalarAsync(owner, $"SELECT agent_level FROM config_notification_routes WHERE route_id = {seeded}", ct));

            /* Delete clears every other route's fallback to it, then removes it; a second delete is a 404. */
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, $"{{\"fallback_route_id\":{id}}}")).Status);
            Assert.Equal(id.ToString(CultureInfo.InvariantCulture), await ScalarAsync(owner, $"SELECT fallback_route_id FROM config_notification_routes WHERE route_id = {seeded}", ct));
            var deleted = await Send("DELETE", $"/api/notification-routes/{id}", null);
            Assert.Equal(200, deleted.Status);
            Assert.Equal("deleted", (string)deleted.Body!["status"]!);
            Assert.Equal("0", await ScalarAsync(owner, $"SELECT count(*) FROM config_notification_routes WHERE route_id = {id}", ct));
            Assert.Equal("", await ScalarAsync(owner, $"SELECT fallback_route_id FROM config_notification_routes WHERE route_id = {seeded}", ct));
            Assert.Equal(404, (await Send("DELETE", $"/api/notification-routes/{id}", null)).Status);
            Assert.Equal(404, (await Send("PUT", $"/api/notification-routes/{id}", Json, "{\"agent_level\":1}")).Status);
        });
    }

    [Fact]
    public async Task AgentRoutes_Destinations_AreSealedAndNeverShown_KeptOnOmit_ClearedOnDash_AndARefusedValueIsNotEchoed()
    {
        const string TeamsUrl = "https://teams.example.test/webhook/teams-secret-token";
        const string SlackUrl = "https://hooks.example.test/services/T000/B000/slack-secret-token";
        const string GenericUrl = "https://generic.example.test/in/generic-secret-token";
        const string RoutingKey = "pagerduty-secret-routing-key-0001";
        using var key = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            async Task<string> Stored(int id, string column) => await ScalarAsync(owner, $"SELECT {column} FROM config_notification_routes WHERE route_id = {id}", ct) ?? "";
            async Task<System.Text.Json.JsonElement> PageRoute(int id) =>
                System.Text.Json.JsonDocument.Parse(await DarlingMcpAlertTools.GetNotificationRoutes(owner, ct)).RootElement
                    .GetProperty("routes").EnumerateArray().Single(r => r.GetProperty("route_id").GetInt32() == id);

            /* Set: all five destinations on a create. Each webhook is stored sealed for the route (it opens to what was sent) and the
               answer carries no value. */
            var created = await Send("POST", "/api/notification-routes", Json,
                $"{{\"metric_match\":\"blocking\",\"teams_webhook_url\":\"{TeamsUrl}\",\"slack_webhook_url\":\"{SlackUrl}\",\"generic_webhook_url\":\"{GenericUrl}\",\"pagerduty_routing_key\":\"{RoutingKey}\",\"email_recipients\":\"ops@example.com, oncall@example.com\"}}");
            Assert.Equal(200, created.Status);
            var id = (int)created.Body!["route_id"]!;
            foreach (var secret in new[] { TeamsUrl, SlackUrl, GenericUrl, RoutingKey })
            {
                Assert.DoesNotContain(secret, created.Body.ToJsonString(), StringComparison.Ordinal);
            }

            var proxies = await NotificationRouteWriter.ReadProxiesAsync(owner, 30, ct);
            var where = PasswordBinding.WebhookRouteRow(id);
            Assert.Equal(TeamsUrl, ring.Open(await Stored(id, "teams_url"), PasswordBinding.ForWebhook("teams", where, proxies.TeamsProxy, null)));
            Assert.Equal(SlackUrl, ring.Open(await Stored(id, "slack_url"), PasswordBinding.ForWebhook("slack", where, proxies.SlackProxy, null)));
            Assert.Equal(GenericUrl, ring.Open(await Stored(id, "generic_url"), PasswordBinding.ForWebhook("generic", where, proxies.GenericProxy, null)));
            Assert.Equal(RoutingKey, ring.Open(await Stored(id, "pagerduty_routing_key"), PasswordBinding.ForWebhook("pagerduty", where, proxies.PagerDutyProxy, null)));
            Assert.Equal("ops@example.com, oncall@example.com", await Stored(id, "smtp_recipients"));

            /* The page's read says which destinations are set, and shows no webhook value (the recipients are shown, as the read always shows them). */
            var pageText = await DarlingMcpAlertTools.GetNotificationRoutes(owner, ct);
            foreach (var secret in new[] { TeamsUrl, SlackUrl, GenericUrl, RoutingKey, "teams-secret-token", "slack-secret-token", "generic-secret-token", "secret-routing-key" })
            {
                Assert.DoesNotContain(secret, pageText, StringComparison.Ordinal);
            }

            var route = await PageRoute(id);
            Assert.Equal(new[] { "Teams", "Slack", "Generic", "PagerDuty", "Email" }, route.GetProperty("configured_channels").EnumerateArray().Select(c => c.GetString()).ToArray());
            Assert.Equal("ops@example.com, oncall@example.com", route.GetProperty("smtp_recipients").GetString());

            /* Omit keeps: an edit that names no destination, or names one blank or null, leaves every saved value byte for byte. */
            string[] columns = ["teams_url", "slack_url", "generic_url", "pagerduty_routing_key", "smtp_recipients"];
            async Task<string> Snapshot()
            {
                var parts = new List<string>();
                foreach (var column in columns)
                {
                    parts.Add(await Stored(id, column));
                }

                return string.Join("|", parts);
            }

            var before = await Snapshot();
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{id}", Json, "{\"enabled\":false}")).Status);
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{id}", Json, "{\"agent_level\":1,\"teams_webhook_url\":\"  \",\"slack_webhook_url\":null,\"email_recipients\":\"\"}")).Status);
            Assert.Equal(before, await Snapshot());
            Assert.Equal("false", await Stored(id, "enabled::text"));

            /* A new value replaces only its own destination. */
            var replaced = "https://teams.example.test/webhook/replacement-token";
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{id}", Json, $"{{\"teams_webhook_url\":\"{replaced}\"}}")).Status);
            Assert.Equal(replaced, ring.Open(await Stored(id, "teams_url"), PasswordBinding.ForWebhook("teams", where, proxies.TeamsProxy, null)));
            Assert.Equal(before.Split('|')[1], await Stored(id, "slack_url"));

            /* An explicit - clears that destination only. */
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{id}", Json, "{\"slack_webhook_url\":\"-\",\"email_recipients\":\"-\",\"pagerduty_routing_key\":\"-\"}")).Status);
            Assert.Equal("", await Stored(id, "slack_url"));
            Assert.Equal("", await Stored(id, "smtp_recipients"));
            Assert.Equal("", await Stored(id, "pagerduty_routing_key"));
            Assert.NotEqual("", await Stored(id, "teams_url"));
            Assert.NotEqual("", await Stored(id, "generic_url"));
            Assert.Equal(new[] { "Teams", "Generic" }, (await PageRoute(id)).GetProperty("configured_channels").EnumerateArray().Select(c => c.GetString()).ToArray());

            /* A refused value is named by its field and never echoed, and nothing is written. */
            var snapshot = await Snapshot();
            foreach (var (json, contains, secret) in new[]
            {
                ("{\"teams_webhook_url\":\"ftp://secret-host.example/secret-token\"}", "Teams webhook URL", "secret-token"),
                ("{\"slack_webhook_url\":\"not-a-url-secret-token\"}", "Slack webhook URL", "secret-token"),
                ("{\"generic_webhook_url\":\"http://169.254.169.254/latest/secret-token\"}", "generic webhook URL", "secret-token"),
                ("{\"email_recipients\":\"Secret Name <secret-person@example.com>\"}", "plain addresses", "secret-person"),
                ("{\"teams_webhook_url\":12345}", "teams_webhook_url must be text", "12345"),
            })
            {
                var refused = await Send("PUT", $"/api/notification-routes/{id}", Json, json);
                Assert.Equal(400, refused.Status);
                Assert.Contains(contains, (string)refused.Body!["error"]!, StringComparison.Ordinal);
                Assert.DoesNotContain(secret, refused.Body.ToJsonString(), StringComparison.Ordinal);
                Assert.Equal(snapshot, await Snapshot());
            }

            /* The stored sealed text is not a value a caller may send back, and the create of a refused value leaves no route behind. */
            var sealedText = await Stored(id, "teams_url");
            var sealedBack = await Send("PUT", $"/api/notification-routes/{id}", Json, $"{{\"slack_webhook_url\":\"{sealedText}\"}}");
            Assert.Equal(400, sealedBack.Status);
            Assert.DoesNotContain(sealedText, sealedBack.Body!.ToJsonString(), StringComparison.Ordinal);
            var routesBefore = await ScalarAsync(owner, "SELECT count(*) FROM config_notification_routes", ct);
            Assert.Equal(400, (await Send("POST", "/api/notification-routes", Json, "{\"metric_match\":\"deadlocks\",\"teams_webhook_url\":\"ftp://x.example/secret-token\"}")).Status);
            Assert.Equal(routesBefore, await ScalarAsync(owner, "SELECT count(*) FROM config_notification_routes", ct));
            Assert.Equal(snapshot, await Snapshot());
        }, ring);
    }

    [Fact]
    public async Task AgentRoutes_RefuseWhatTheViewerRefuses_WithTheSharedSentence_AndWriteNothing()
    {
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            var seeded = await SeedRouteAsync(owner, "performance", ct);
            var before = await ScalarAsync(owner, "SELECT count(*) || '|' || coalesce(max(modified_at)::text, '') FROM config_notification_routes", ct);

            async Task Refused(string method, string path, string json, string contains)
            {
                var answer = await Send(method, path, Json, json);
                Assert.Equal(400, answer.Status);
                Assert.True(((string)answer.Body!["error"]!).Contains(contains, StringComparison.Ordinal), contains + " not in: " + answer.Body.ToJsonString());
            }

            await Refused("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"agent_level\":4}", "Inherit, Off, Investigate, Act or Act on servers");
            await Refused("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"agent_level\":1,\"agent_max_per_hour\":61}", "Requests per hour");
            await Refused("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"agent_level\":1,\"agent_max_per_day\":0}", "Requests per day");
            await Refused("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"agent_level\":1,\"agent_lease_minutes\":4}", "Lease minutes");
            await Refused("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"agent_level\":1,\"agent_expiry_minutes\":1441}", "Expiry minutes");
            await Refused("POST", "/api/notification-routes", "{\"metric_match\":\"  \",\"agent_level\":1}", "Choose a family");
            await Refused("POST", "/api/notification-routes", $"{{\"metric_match\":\"*\",\"agent_level\":1}}", "Every metric (*) needs a server or a tag scope");
            await Refused("POST", "/api/notification-routes", $"{{\"metric_match\":\"blocking\",\"agent_level\":1,\"scope_server_id\":{ServerId},\"scope_tag_id\":3}}", "one server or one tag, not both");
            await Refused("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\"}", "at least one destination or the agent channel");
            await Refused("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"agent_level\":1,\"fallback_route_id\":99999}", "Fallback route 99999 does not exist");
            await Refused("PUT", $"/api/notification-routes/{seeded}", $"{{\"fallback_route_id\":{seeded}}}", "its own fallback");
            await Refused("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"agent_level\":\"one\"}", "agent_level must be a whole number");
            await Refused("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"agent_level\":1,\"agent_max_per_hour\":1.5}", "agent_max_per_hour must be a whole number");

            await Refused("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"agent_level\":1,\"bogus\":1}", "Unknown field");

            /* Not JSON: refused before the store is touched. A non-object body is a 400. */
            Assert.Equal(415, (await Send("POST", "/api/notification-routes", null, "{}")).Status);
            Assert.Equal(400, (await Send("POST", "/api/notification-routes", Json, "[1]")).Status);

            Assert.Equal(before, await ScalarAsync(owner, "SELECT count(*) || '|' || coalesce(max(modified_at)::text, '') FROM config_notification_routes", ct));
        });
    }

    [Fact]
    public async Task AgentSetup_AsTheManagedViewerRole_Lists_ButEveryWriteIs403_AndChangesNothing()
    {
        await WithWebRoutesAsync("viewer", async (owner, viewer, Send, ct) =>
        {
            var seeded = await SeedRouteAsync(owner, "performance", ct);
            await new InvestigationRequestStore(owner, 30).SaveProfileAsync(null, null, "fleet: OLTP", "tester", ct);

            var access = await Send("GET", "/api/agent-setup/access", null);
            Assert.False((bool)access.Body!["can_edit_routes"]!);
            Assert.False((bool)access.Body["can_edit_profiles"]!);

            var profiles = await Send("GET", "/api/environment-profiles", null);
            Assert.Equal(200, profiles.Status);
            Assert.False((bool)profiles.Body!["can_edit"]!);
            Assert.Equal("fleet: OLTP", (string)profiles.Body["profiles"]![0]!["profile_text"]!);

            foreach (var (method, path, json) in new[]
            {
                ("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"agent_level\":1}"),
                ("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"teams_webhook_url\":\"https://teams.example.test/hook\",\"email_recipients\":\"a@example.com\"}"),
                ("PUT", $"/api/notification-routes/{seeded}", "{\"agent_level\":1}"),
                ("PUT", $"/api/notification-routes/{seeded}", "{\"slack_webhook_url\":\"https://hooks.example.test/slack\"}"),
                ("DELETE", $"/api/notification-routes/{seeded}", null),
                ("PUT", "/api/environment-profiles", "{\"profile_text\":\"changed\"}"),
            })
            {
                var answer = await Send(method, path, method == "DELETE" ? null : Json, json);
                Assert.Equal(403, answer.Status);
                Assert.Equal(DarlingWebAgentSetup.RefusedMessage, (string)answer.Body!["error"]!);
            }

            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config_notification_routes", ct));
            Assert.Equal("", await ScalarAsync(owner, $"SELECT agent_level FROM config_notification_routes WHERE route_id = {seeded}", ct));
            Assert.Equal("fleet: OLTP", await ScalarAsync(owner, "SELECT profile_text FROM config.agent_environment_profiles", ct));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'profile_%'", ct));
        }, DarlingPasswordKey.FromPrivateKey(PasswordPrivateKey.Generate()));
    }

    [Fact]
    public async Task EnvironmentProfiles_SaveReplaceAndRemove_WithTheViewersRulesAndJournalRows()
    {
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            Assert.Empty((await Send("GET", "/api/environment-profiles", null)).Body!["profiles"]!.AsArray());

            var fleet = await Send("PUT", "/api/environment-profiles", Json, "{\"profile_text\":\"  fleet: OLTP, business hours 08-18  \"}");
            Assert.Equal(200, fleet.Status);
            Assert.Equal("saved", (string)fleet.Body!["status"]!);
            var server = await Send("PUT", "/api/environment-profiles", Json, $"{{\"scope_server_id\":{ServerId},\"profile_text\":\"reporting replica\"}}");
            Assert.Equal("saved", (string)server.Body!["status"]!);
            Assert.Equal("saved", (string)(await Send("PUT", "/api/environment-profiles", Json, "{\"profile_text\":\"fleet: OLTP, 24x7\"}")).Body!["status"]!);

            var listed = await Send("GET", "/api/environment-profiles", null);
            Assert.True((bool)listed.Body!["can_edit"]!);
            Assert.Equal(4000, (int)listed.Body["max_length"]!);
            var rows = listed.Body["profiles"]!.AsArray();
            Assert.Equal(2, rows.Count);
            Assert.Null(rows[0]!["scope_server_id"]);
            Assert.Equal("fleet: OLTP, 24x7", (string)rows[0]!["profile_text"]!);
            Assert.Equal("person:web", (string)rows[0]!["modified_by"]!);
            Assert.Equal(ServerId, (int)rows[1]!["scope_server_id"]!);

            /* The same journal rows the Viewer writes: profile_saved by person:<login>, request NULL, the reason naming the scope. */
            Assert.Equal("fleet profile|server " + ServerId.ToString(CultureInfo.InvariantCulture) + " profile|fleet profile", await ScalarAsync(owner,
                "SELECT string_agg(reason, '|' ORDER BY journal_id) FROM config.investigation_journal WHERE event = 'profile_saved' AND actor = 'person:web' AND request_id IS NULL", ct));

            /* Blank removes the scope's profile and journals profile_removed; blank on a scope with none changes and journals nothing. */
            Assert.Equal("removed", (string)(await Send("PUT", "/api/environment-profiles", Json, $"{{\"scope_server_id\":{ServerId},\"profile_text\":\"   \"}}")).Body!["status"]!);
            Assert.Equal("unchanged", (string)(await Send("PUT", "/api/environment-profiles", Json, $"{{\"scope_server_id\":{ServerId},\"profile_text\":\"\"}}")).Body!["status"]!);
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'profile_removed' AND reason = 'server " + ServerId.ToString(CultureInfo.InvariantCulture) + " profile'", ct));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.agent_environment_profiles", ct));

            /* The rules: a server and a tag together, and text over 4,000 characters, are refused and write nothing. */
            var both = await Send("PUT", "/api/environment-profiles", Json, "{\"scope_server_id\":1,\"scope_tag_id\":2,\"profile_text\":\"x\"}");
            Assert.Equal(400, both.Status);
            Assert.Equal("A profile is for the fleet, one server or one tag, not a server and a tag.", (string)both.Body!["error"]!);
            var tooLong = await Send("PUT", "/api/environment-profiles", Json, $"{{\"profile_text\":\"{new string('p', 4001)}\"}}");
            Assert.Equal(400, tooLong.Status);
            Assert.Equal("A profile must be 4,000 characters or fewer.", (string)tooLong.Body!["error"]!);
            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles", Json, $"{{\"scope_tag_id\":3,\"profile_text\":\"{new string('p', 4000)}\"}}")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/environment-profiles", Json, "{\"scope_server_id\":\"x\",\"profile_text\":\"x\"}")).Status);
            Assert.Equal(415, (await Send("PUT", "/api/environment-profiles", null, "{}")).Status);
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.agent_environment_profiles", ct));
        });
    }

    /* The write gate is the host's, by method: a read-only seat (an OIDC viewer) is refused every unsafe method, so each new write
       route is born gated; a seat that can edit passes. */
    [Theory]
    [InlineData("POST", "/api/notification-routes")]
    [InlineData("PUT", "/api/notification-routes/7")]
    [InlineData("DELETE", "/api/notification-routes/7")]
    [InlineData("PUT", "/api/environment-profiles")]
    public void TheHostsWriteGate_RefusesAReadOnlySeat_ThisSetupsWrites_AndLetsAnEditingSeatThrough(string method, string path)
    {
        Assert.False(DarlingWebSeat.IsRequestAllowed(new DarlingWebSeat("viewer-person", false), method, path));
        Assert.True(DarlingWebSeat.IsRequestAllowed(new DarlingWebSeat("admin-person", true), method, path));
    }
}
