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
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
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

    /// <summary>The fleet profile's version as a page would have read it.</summary>
    private static async Task<DateTime?> ProfileVersionAsync(NpgsqlDataSource owner, CancellationToken ct) =>
        (await new InvestigationRequestStore(owner, 30).ListProfilesAsync(ct)).FirstOrDefault(p => p.ScopeServerId is null && p.ScopeTagId is null)?.ModifiedAtUtc;

    private static async Task<NpgsqlDataSource> MigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        /* #6400: the server a profile may name is one the operator defined (config_monitored_servers, what every picker lists), not one the worker
           has observed (collect.servers, written on a first connection and never pruned), so the fixture's server is defined as well. */
        await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES ($1, $2, 'host-a')", ServerId, ServerName);
        return NpgsqlDataSource.Create(scratch.ConnectionString);
    }

    /// <summary>A route with one saved email destination, written by the same code the Viewer saves through.</summary>
    private static Task<int> SeedRouteAsync(NpgsqlDataSource owner, string match, CancellationToken ct) =>
        NotificationRouteWriter.SaveAsync(
            owner, new NotificationRouteDraft { MetricMatch = match, SmtpRecipients = "ops@example.com" }, null,
            new WebhookProxies(null, null, null, null), null, null, 30, ct);

    private static async Task WithWebRoutesAsync(
        string managedRole, Func<NpgsqlDataSource, NpgsqlDataSource, Sender, CancellationToken, Task> body, IPasswordKeyRing? ring = null,
        DarlingWebSeat? seat = null, bool signedIn = true, bool managedStore = false, AdminArmShape arm = AdminArmShape.None)
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

            /* #6600: the managed-shaped store. The web logs in as the read-only role above; the arm signs in as a second role that holds the admin
               role's grants (the managed `admin` login), or, for Refused, as a login the store rejects, so a request that opens it fails. */
            using var adminArm = await AdminArmLive.BuildAsync(owner, scratch, roleName, arm, ct);

            var builder = WebApplication.CreateBuilder(new WebApplicationOptions
            {
                ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
                WebRootPath = "wwwroot",
            });
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton(webLogin);
            await using var app = builder.Build();

            /* The host's seat step (#6274), as DarlingWebHostService runs it for a signed-in request: the resolved seat rides HttpContext.Items and
               a read-only seat is refused every mutating request by the real DarlingWebSeat.IsRequestAllowed with the host's sentence. With
               signedIn false nothing is stored, exactly as a loopback dashboard with no sign-in: the default seat FromContext hands back is the
               shared token's, and only IsSignedInAdmin can tell the two apart. */
            app.Use(async (context, next) =>
            {
                if (signedIn)
                {
                    var resolved = seat ?? DarlingWebSeat.SharedToken;
                    context.Items[DarlingWebSeat.HttpContextItemKey] = resolved;
                    if (!DarlingWebSeat.IsRequestAllowed(resolved, context.Request.Method, context.Request.Path.Value ?? "/"))
                    {
                        context.Response.StatusCode = StatusCodes.Status403Forbidden;
                        context.Response.ContentType = "application/json; charset=utf-8";
                        await context.Response.WriteAsync("{\"error\": \"This account has read-only access.\"}");
                        return;
                    }
                }

                await next(context);
            });
            DarlingWebEndpoints.MapAll(app, webLogin, new CollectorRuntimeState(), new CapturingTestLogger(), passwordKeys: ring,
                postgresConfig: managedStore ? new PostgresConfig { Managed = true } : null, adminArm: adminArm);
            await app.StartAsync(ct);
            using var server = app.GetTestServer();

            async Task<(int Status, JsonNode? Body)> Send(string method, string path, string? contentType, string? json = null)
            {
                /* A route edit carries the modified_at the page read (#6274). These tests read it just before the send, so a test that wants a
                   STALE one writes expected_modified_at itself, and one that wants it missing adds ?nostamp=1 to the path. */
                if (method == "PUT" && json is not null && json.StartsWith('{') && !json.Contains("expected_modified_at", StringComparison.Ordinal)
                    && !path.Contains("nostamp", StringComparison.Ordinal)
                    && path.StartsWith("/api/notification-routes/", StringComparison.Ordinal)
                    && int.TryParse(path["/api/notification-routes/".Length..], out var editedId))
                {
                    var current = await ScalarAsync(owner, $"SELECT to_char(modified_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.US') FROM config_notification_routes WHERE route_id = {editedId}", ct);
                    json = "{\"expected_modified_at\":\"" + (string.IsNullOrEmpty(current) ? "2000-01-01T00:00:00" : current) + "\"" + (json.Length > 2 ? "," : "") + json[1..];
                }

                /* A profile save carries the modified_at the page read for its scope, when the scope has a profile (#6274). */
                if (method == "PUT" && json is not null && path.StartsWith("/api/environment-profiles", StringComparison.Ordinal)
                    && !json.Contains("expected_modified_at", StringComparison.Ordinal) && !path.Contains("nostamp", StringComparison.Ordinal)
                    && JsonNode.Parse(json) is JsonObject profileBody
                    && (profileBody["scope_server_id"] is null || profileBody["scope_server_id"]!.GetValueKind() == JsonValueKind.Number)
                    && (profileBody["scope_tag_id"] is null || profileBody["scope_tag_id"]!.GetValueKind() == JsonValueKind.Number))
                {
                    var serverScope = profileBody["scope_server_id"]?.GetValue<int>() ?? 0;
                    var tagScope = profileBody["scope_tag_id"]?.GetValue<int>() ?? 0;
                    var current = await ScalarAsync(owner, $"SELECT to_char(modified_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.US') FROM config.agent_environment_profiles WHERE coalesce(scope_server_id, 0) = {serverScope} AND coalesce(scope_tag_id, 0) = {tagScope}", ct);
                    if (!string.IsNullOrEmpty(current))
                    {
                        profileBody["expected_modified_at"] = current;
                        json = profileBody.ToJsonString();
                    }
                }

                path = path.Replace("?nostamp=1", "", StringComparison.Ordinal);
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
                await using var drop = new NpgsqlCommand(AdminArmLive.DropRolesSql(roleName), connection);
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

            /* #6928: the severity filter. A new route is Both, an edit sets it (any case), an edit that leaves it out keeps it. */
            Assert.Equal("both", await ScalarAsync(owner, $"SELECT severities FROM config_notification_routes WHERE route_id = {seeded}", ct));
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"severities\":\"Critical\"}")).Status);
            Assert.Equal("critical", await ScalarAsync(owner, $"SELECT severities FROM config_notification_routes WHERE route_id = {seeded}", ct));
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"enabled\":true}")).Status);
            Assert.Equal("critical", await ScalarAsync(owner, $"SELECT severities FROM config_notification_routes WHERE route_id = {seeded}", ct));
            var warningOnly = await Send("POST", "/api/notification-routes", Json, "{\"metric_match\":\"blocking\",\"agent_level\":1,\"severities\":\"warning\"}");
            Assert.Equal(200, warningOnly.Status);
            Assert.Equal("warning", await ScalarAsync(owner, $"SELECT severities FROM config_notification_routes WHERE route_id = {(int)warningOnly.Body!["route_id"]!}", ct));
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"severities\":\"both\"}")).Status);

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
            await Refused("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"agent_level\":1,\"severities\":\"major\"}", "severities is critical, warning or both");
            await Refused("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"agent_level\":1,\"severities\":3}", "severities must be text");
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
            await new InvestigationRequestStore(owner, 30).SaveProfileAsync(null, null, "fleet: OLTP", "tester", null, ct);

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
                Assert.Equal(DarlingWebAgentSetup.RefusedMessage(false), (string)answer.Body!["error"]!);
            }

            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config_notification_routes", ct));
            Assert.Equal("", await ScalarAsync(owner, $"SELECT agent_level FROM config_notification_routes WHERE route_id = {seeded}", ct));
            Assert.Equal("fleet: OLTP", await ScalarAsync(owner, "SELECT profile_text FROM config.agent_environment_profiles", ct));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'profile_%'", ct));
        }, DarlingPasswordKey.FromPrivateKey(PasswordPrivateKey.Generate()));
    }

    /// <summary>#6445: the same viewer-role 403, but the web host runs on the managed store. That store refuses <c>postgres.webConnectionString</c>,
    /// so the sentence must not send the user to it or to "the admin login"; a store you bring yourself keeps that advice. Both are real
    /// refusals from the store's own grants, and nothing is written.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TheViewerRoles403_NamesTheAdminLoginOption_OnlyOnAStoreYouBringYourself(bool managedStore)
    {
        await WithWebRoutesAsync("viewer", async (owner, viewer, Send, ct) =>
        {
            var seeded = await SeedRouteAsync(owner, "performance", ct);
            foreach (var (method, path, json) in new[]
            {
                ("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"agent_level\":1}"),
                ("DELETE", $"/api/notification-routes/{seeded}", null),
                ("PUT", "/api/environment-profiles", "{\"profile_text\":\"changed\"}"),
            })
            {
                var answer = await Send(method, path, method == "DELETE" ? null : Json, json);
                Assert.Equal(403, answer.Status);
                var said = (string)answer.Body!["error"]!;
                Assert.Equal(DarlingWebAgentSetup.RefusedMessage(managedStore, adminWritesOff: managedStore), said);
                Assert.Equal(managedStore, said.Contains("web.allowAdminWrites", StringComparison.Ordinal));
                Assert.Contains("Viewer app", said, StringComparison.Ordinal);
                Assert.Equal(!managedStore, said.Contains("webConnectionString", StringComparison.Ordinal));
                Assert.Equal(!managedStore, said.Contains("admin store login", StringComparison.OrdinalIgnoreCase));
            }

            Assert.Equal(managedStore, (bool)(await Send("GET", "/api/session", null)).Body!["managed_store"]!);
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config_notification_routes", ct));
        }, DarlingPasswordKey.FromPrivateKey(PasswordPrivateKey.Generate()), managedStore: managedStore);
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
            await ExecAsync(owner, "INSERT INTO config.server_tags (id, name) OVERRIDING SYSTEM VALUE VALUES (3, 'dev')", ct);
            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles", Json, $"{{\"scope_tag_id\":3,\"profile_text\":\"{new string('p', 4000)}\"}}")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/environment-profiles", Json, "{\"scope_server_id\":\"x\",\"profile_text\":\"x\"}")).Status);
            Assert.Equal(415, (await Send("PUT", "/api/environment-profiles", null, "{}")).Status);
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.agent_environment_profiles", ct));
        });
    }

    /* #6600: the managed store's shape: the web reads as the viewer role and a signed-in admin's writes use the arm, a second small pool on the
       admin login. The write succeeds, the journal names the person, and the access flags are true. */
    [Fact]
    public async Task OnTheManagedStore_ASignedInAdminSeat_WritesThroughTheArm_AndTheAccessFlagsAreTrue()
    {
        await WithWebRoutesAsync("viewer", async (owner, _, Send, ct) =>
        {
            var seeded = await SeedRouteAsync(owner, "performance", ct);

            var access = await Send("GET", "/api/agent-setup/access", null);
            Assert.True((bool)access.Body!["can_edit_routes"]!);
            Assert.True((bool)access.Body["can_edit_profiles"]!);
            Assert.True((bool)(await Send("GET", "/api/environment-profiles", null)).Body!["can_edit"]!);

            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles", Json, "{\"profile_text\":\"fleet: OLTP\"}")).Status);
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"agent_level\":3}")).Status);
            var created = await Send("POST", "/api/notification-routes", Json, "{\"metric_match\":\"blocking\",\"agent_level\":1}");
            Assert.Equal(200, created.Status);
            Assert.Equal(200, (await Send("DELETE", $"/api/notification-routes/{(int)created.Body!["route_id"]!}", null)).Status);

            Assert.Equal("person:alice", await ScalarAsync(owner, "SELECT modified_by FROM config.agent_environment_profiles", ct));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'route_agent_changed' AND actor = 'person:alice' AND route_id = " + seeded, ct));
        }, DarlingPasswordKey.FromPrivateKey(PasswordPrivateKey.Generate()), seat: new DarlingWebSeat("alice", true), managedStore: true, arm: AdminArmShape.Admin);
    }

    /* #6600: a seat that is not a signed-in admin never opens the arm. Its login here is refused by the store, so a request that opened it would
       fail; instead the reads answer as they do today (the viewer role: list only) and the host's write gate answers the write. */
    [Fact]
    public async Task OnTheManagedStore_AReadOnlySeat_NeverOpensTheArm_AndAnswersAsToday()
    {
        await WithWebRoutesAsync("viewer", async (owner, _, Send, ct) =>
        {
            var seeded = await SeedRouteAsync(owner, "performance", ct);

            var access = await Send("GET", "/api/agent-setup/access", null);
            Assert.Equal(200, access.Status);
            Assert.False((bool)access.Body!["can_edit_routes"]!);
            Assert.False((bool)access.Body["can_edit_profiles"]!);

            Assert.Equal(403, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"agent_level\":3}")).Status);
            Assert.Equal("", await ScalarAsync(owner, $"SELECT agent_level FROM config_notification_routes WHERE route_id = {seeded}", ct));
        }, DarlingPasswordKey.FromPrivateKey(PasswordPrivateKey.Generate()), seat: new DarlingWebSeat("viewer-person", false), managedStore: true, arm: AdminArmShape.Refused);
    }

    /* #6600: with the switch off (no arm on a managed store) the answers are today's: list-only flags, a 403 on every write that now names the
       switch, and nothing written. */
    [Fact]
    public async Task OnTheManagedStore_WithTheSwitchOff_TheSignedInAdminStillListsOnly()
    {
        await WithWebRoutesAsync("viewer", async (owner, _, Send, ct) =>
        {
            var access = await Send("GET", "/api/agent-setup/access", null);
            Assert.False((bool)access.Body!["can_edit_routes"]!);

            var refused = await Send("POST", "/api/notification-routes", Json, "{\"metric_match\":\"blocking\",\"agent_level\":1}");
            Assert.Equal(403, refused.Status);
            Assert.Contains("web.allowAdminWrites", (string)refused.Body!["error"]!, StringComparison.Ordinal);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config_notification_routes", ct));
        }, DarlingPasswordKey.FromPrivateKey(PasswordPrivateKey.Generate()), seat: new DarlingWebSeat("alice", true), managedStore: true);
    }

    /* The one person-name rule (#6265) reaches the web's profile and route writes: a signed-in name with a space and an apostrophe is a real
       name, stamped exactly as a note decision stamps it (person:<name>) on the profile row and in both journal rows. */
    [Fact]
    public async Task APersonNameWithASpaceAndAnApostrophe_SavesAProfileAndARouteChange_StampedAsANoteDecisionStampsIt()
    {
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            var seeded = await SeedRouteAsync(owner, "performance", ct);

            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles", Json, "{\"profile_text\":\"fleet: OLTP\"}")).Status);
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"agent_level\":3}")).Status);

            Assert.Equal("person:Alice O'Neil", await ScalarAsync(owner, "SELECT modified_by FROM config.agent_environment_profiles", ct));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'profile_saved' AND actor = 'person:Alice O''Neil'", ct));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'route_agent_changed' AND actor = 'person:Alice O''Neil'", ct));
        }, seat: new DarlingWebSeat("Alice O'Neil", true));
    }

    /* A signed-in name the shared rule refuses (here an angle bracket) never reaches a store write: every route and profile write answers
       400 with the fixed sentence the web's note decisions answer, never the name, and nothing is saved or journaled. */
    [Fact]
    public async Task APersonNameTheSharedRuleRefuses_IsRefusedWithTheNoteDecisionSentence_ByEveryProfileAndRouteWrite_WritingNothing()
    {
        const string sentence = "The signed-in name cannot be recorded as the person who made this change.";
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            var seeded = await SeedRouteAsync(owner, "performance", ct);

            var profile = await Send("PUT", "/api/environment-profiles", Json, "{\"profile_text\":\"fleet: OLTP\"}");
            var edit = await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"agent_level\":3}");
            var create = await Send("POST", "/api/notification-routes", Json, "{\"metric_match\":\"blocking\"}");
            var delete = await Send("DELETE", $"/api/notification-routes/{seeded}", null);
            foreach (var answer in new[] { profile, edit, create, delete })
            {
                Assert.Equal(400, answer.Status);
                Assert.Equal(sentence, (string)answer.Body!["error"]!);
            }

            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.agent_environment_profiles", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE actor LIKE 'person:%'", ct));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config_notification_routes", ct));
        }, seat: new DarlingWebSeat("Eve <b>", true));
    }

    /* The write gate is the host's, by method, and the setup's own gate is the signed-in admin (#6274). A read-only seat (an OIDC viewer) is
       refused every unsafe method by the host before routing, so each new write route is born gated; this runs the four writes through the
       host's seat step and tells the host's refusal from the handler's by its sentence, so it fails when the gate is removed. */
    [Fact]
    public async Task TheHostsWriteGate_RefusesAReadOnlySeat_EveryWrite_WithTheHostsSentence_AndChangesNothing()
    {
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            var seeded = await SeedRouteAsync(owner, "performance", ct);
            foreach (var (method, path, json) in new[]
            {
                ("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"agent_level\":3}"),
                ("PUT", $"/api/notification-routes/{seeded}", "{\"agent_level\":3}"),
                ("DELETE", $"/api/notification-routes/{seeded}", null),
                ("PUT", "/api/environment-profiles", "{\"profile_text\":\"changed\"}"),
            })
            {
                var answer = await Send(method, path, method == "DELETE" ? null : Json, json);
                Assert.Equal(403, answer.Status);
                Assert.Equal("This account has read-only access.", (string)answer.Body!["error"]!);
            }

            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config_notification_routes WHERE agent_level IS NULL", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.agent_environment_profiles", ct));
        }, seat: new DarlingWebSeat("viewer-person", false));
    }

    /* A loopback dashboard with no sign-in stores no seat: every write is refused for want of a signed-in admin, the access flags say the page
       lists only, and nothing changes. A signed-in admin (the shared token's seat or an OIDC admin) writes. */
    [Fact]
    public async Task AgentSetup_WithoutASignedInAdmin_EveryWriteIs403_TheFlagsAreFalse_AndNothingChanges()
    {
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            var seeded = await SeedRouteAsync(owner, "performance", ct);
            await new InvestigationRequestStore(owner, 30).SaveProfileAsync(null, null, "fleet: OLTP", "tester", null, ct);
            var routeBefore = await ScalarAsync(owner, $"SELECT agent_level || '|' || agent_max_per_hour || '|' || enabled || '|' || smtp_recipients || '|' || modified_at FROM config_notification_routes WHERE route_id = {seeded}", ct);

            foreach (var (method, path, json) in new[]
            {
                ("POST", "/api/notification-routes", "{\"metric_match\":\"blocking\",\"agent_level\":3}"),
                ("PUT", $"/api/notification-routes/{seeded}", "{\"agent_level\":3,\"agent_max_per_hour\":60}"),
                ("PUT", $"/api/notification-routes/{seeded}", "{\"generic_webhook_url\":\"https://elsewhere.example.test/hook\"}"),
                ("PUT", $"/api/notification-routes/{seeded}", "{\"email_recipients\":\"-\"}"),
                ("DELETE", $"/api/notification-routes/{seeded}", null),
                ("PUT", "/api/environment-profiles", "{\"profile_text\":\"changed\"}"),
            })
            {
                var answer = await Send(method, path, method == "DELETE" ? null : Json, json);
                Assert.Equal(403, answer.Status);
                Assert.Equal("Sign in to the web as an admin to make this change, or use the Viewer.", (string)answer.Body!["error"]!);
            }

            var access = await Send("GET", "/api/agent-setup/access", null);
            Assert.False((bool)access.Body!["can_edit_routes"]!);
            Assert.False((bool)access.Body["can_edit_profiles"]!);
            Assert.Equal(DarlingWebEndpoints.InvestigationWriteSignInMessage, (string)access.Body["sign_in_message"]!);
            var profiles = await Send("GET", "/api/environment-profiles", null);
            Assert.False((bool)profiles.Body!["can_edit"]!);

            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config_notification_routes", ct));
            Assert.Equal(routeBefore, await ScalarAsync(owner, $"SELECT agent_level || '|' || agent_max_per_hour || '|' || enabled || '|' || smtp_recipients || '|' || modified_at FROM config_notification_routes WHERE route_id = {seeded}", ct));
            Assert.Equal("fleet: OLTP", await ScalarAsync(owner, "SELECT profile_text FROM config.agent_environment_profiles", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'route_agent_changed'", ct));
        }, signedIn: false);

        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            var access = await Send("GET", "/api/agent-setup/access", null);
            Assert.True((bool)access.Body!["can_edit_routes"]!);
            Assert.True((bool)access.Body["can_edit_profiles"]!);
            Assert.Null(access.Body["sign_in_message"]);
            Assert.Equal(200, (await Send("POST", "/api/notification-routes", Json, "{\"metric_match\":\"blocking\",\"agent_level\":1}")).Status);
            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles", Json, "{\"profile_text\":\"fleet: OLTP\"}")).Status);
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config_notification_routes", ct));
        }, seat: new DarlingWebSeat("alice", true));
    }

    /* A page that loaded before an operator changed the route must not write over the change: the edit carries the modified_at the page read,
       and a save over a newer row is a 409 that writes nothing. The Viewer's save is the same writer. */
    [Fact]
    public async Task AgentRoutes_AStaleSave_Is409_AndWritesNothing_ViaTheWebAndTheSharedWriter()
    {
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            var seeded = await SeedRouteAsync(owner, "performance", ct);
            var pageRead = await ScalarAsync(owner, $"SELECT to_char(modified_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.US') FROM config_notification_routes WHERE route_id = {seeded}", ct);

            /* Missing is a 400, not a pass. */
            var missing = await Send("PUT", $"/api/notification-routes/{seeded}?nostamp=1", Json, "{\"agent_level\":1}");
            Assert.Equal(400, missing.Status);

            /* An operator turns the agent off on the route after the page loaded. */
            await ExecAsync(owner, $"UPDATE config_notification_routes SET agent_level = 0, modified_at = (now() AT TIME ZONE 'UTC') + interval '1 second' WHERE route_id = {seeded}", ct);

            var stale = await Send("PUT", $"/api/notification-routes/{seeded}", Json, $"{{\"expected_modified_at\":\"{pageRead}\",\"agent_level\":2,\"enabled\":false}}");
            Assert.Equal(409, stale.Status);
            Assert.Equal(NotificationRouteWriter.ConflictText, (string)stale.Body!["error"]!);
            Assert.Equal("0|true", await ScalarAsync(owner, $"SELECT agent_level || '|' || enabled FROM config_notification_routes WHERE route_id = {seeded}", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'route_agent_changed'", ct));

            /* Reloaded, the same edit goes through. */
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"agent_level\":2,\"enabled\":false}")).Status);
            Assert.Equal("2|false", await ScalarAsync(owner, $"SELECT agent_level || '|' || enabled FROM config_notification_routes WHERE route_id = {seeded}", ct));

            /* The Viewer's path is the same writer: a draft whose version is old is refused; a route removed since is refused too. */
            var stored = (await NotificationRouteWriter.ReadStoredAsync(owner, seeded, 30, ct))!;
            await ExecAsync(owner, $"UPDATE config_notification_routes SET enabled = true, modified_at = (now() AT TIME ZONE 'UTC') + interval '2 seconds' WHERE route_id = {seeded}", ct);
            var typed = stored.Clone();
            typed.AgentLevel = 3;
            var viewerSave = await Assert.ThrowsAsync<NotificationRouteRefusedException>(() => NotificationRouteWriter.SaveAsync(
                owner, typed, stored, new WebhookProxies(null, null, null, null), null, null, 30, ct, "tester"));
            Assert.Equal(NotificationRouteRefusalKind.Conflict, viewerSave.Kind);
            Assert.Equal("2", await ScalarAsync(owner, $"SELECT agent_level FROM config_notification_routes WHERE route_id = {seeded}", ct));

            await ExecAsync(owner, $"DELETE FROM config_notification_routes WHERE route_id = {seeded}", ct);
            var gone = await Assert.ThrowsAsync<NotificationRouteRefusedException>(() => NotificationRouteWriter.SaveAsync(
                owner, typed, stored, new WebhookProxies(null, null, null, null), null, null, 30, ct, "tester"));
            Assert.Equal(NotificationRouteRefusalKind.Conflict, gone.Kind);
        });
    }

    /* A person's change to a route's agent level, limits, scope or match is journaled with the route, the person, and the values before and
       after, from the web and from the shared writer the Viewer saves through; a change to anything else is not. */
    [Fact]
    public async Task AgentRoutes_AnAgentLevelLimitOrScopeChange_WritesAJournalRow_BeforeAndAfter_ByThePerson()
    {
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            var seeded = await SeedRouteAsync(owner, "performance", ct);

            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"agent_level\":3,\"agent_max_per_hour\":60}")).Status);
            Assert.Equal($"person:alice|{seeded}|changed agent_level, agent_max_per_hour", await ScalarAsync(owner,
                "SELECT actor || '|' || route_id || '|' || reason FROM config.investigation_journal WHERE event = 'route_agent_changed'", ct));
            Assert.Equal("|3|20|60", await ScalarAsync(owner,
                "SELECT coalesce(detail->'before'->>'agent_level','') || '|' || (detail->'after'->>'agent_level') || '|' || (detail->'before'->>'agent_max_per_hour') || '|' || (detail->'after'->>'agent_max_per_hour') FROM config.investigation_journal WHERE event = 'route_agent_changed'", ct));

            /* The scope is part of it; a destination is not. Turning an agent route off is (#6274 round 2: it drops out of the lookup). */
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, $"{{\"scope_server_id\":{ServerId}}}")).Status);
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"email_recipients\":\"ops2@example.com\"}")).Status);
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'route_agent_changed'", ct));
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"enabled\":false}")).Status);
            Assert.Equal("changed enabled", await ScalarAsync(owner, "SELECT reason FROM config.investigation_journal WHERE event = 'route_agent_changed' ORDER BY journal_id DESC LIMIT 1", ct));
            Assert.Equal("3", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'route_agent_changed'", ct));
            Assert.Equal(ServerId.ToString(CultureInfo.InvariantCulture), await ScalarAsync(owner, "SELECT server_id FROM config.investigation_journal WHERE event = 'route_agent_changed' ORDER BY journal_id DESC LIMIT 1", ct));

            /* The Viewer's writer: the same row from the same code, by the login it names. */
            var stored = (await NotificationRouteWriter.ReadStoredAsync(owner, seeded, 30, ct))!;
            var typed = stored.Clone();
            typed.AgentLevel = 1;
            await NotificationRouteWriter.SaveAsync(owner, typed, stored, new WebhookProxies(null, null, null, null), null, null, 30, ct, "bob");
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'route_agent_changed' AND actor = 'person:bob' AND reason = 'changed agent_level'", ct));

            /* No login, no row (the MCP tools never reach this writer with a person). */
            var again = (await NotificationRouteWriter.ReadStoredAsync(owner, seeded, 30, ct))!;
            var third = again.Clone();
            third.AgentLevel = 2;
            await NotificationRouteWriter.SaveAsync(owner, third, again, new WebhookProxies(null, null, null, null), null, null, 30, ct);
            Assert.Equal("4", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'route_agent_changed'", ct));
        }, seat: new DarlingWebSeat("alice", true));
    }

    /* #6274 round 2: a route set to agent Off stops the agent lookup for its servers, and a disabled or deleted route is dropped from it, so
       disabling, enabling or deleting such a route hands the servers to a broader agent route. Each of those is journaled with the person, in
       the same transaction, from the web (the PUT's enabled flag and the DELETE) and from the shared writer the Viewer's toggle and delete use.
       A route with no agent level takes no part in the lookup: turning it off or deleting it writes nothing. */
    [Fact]
    public async Task AgentRoutes_DisablingEnablingOrDeleting_ARouteWithAnAgentLevel_IsJournaled_AndOneWithoutIsNot()
    {
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            var off = await SeedRouteAsync(owner, "performance", ct);
            await ExecAsync(owner, $"UPDATE config_notification_routes SET agent_level = 0, scope_server_id = {ServerId} WHERE route_id = {off}", ct);
            var plain = await SeedRouteAsync(owner, "blocking", ct);
            const string Journal = "SELECT count(*) FROM config.investigation_journal WHERE event = 'route_agent_changed'";

            /* Web: disable, enable, delete of the Off route. */
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{off}", Json, "{\"enabled\":false}")).Status);
            Assert.Equal($"person:alice|{off}|changed enabled|{ServerId}|true|false", await ScalarAsync(owner,
                "SELECT actor || '|' || route_id || '|' || reason || '|' || server_id || '|' || (detail->'before'->>'enabled') || '|' || (detail->'after'->>'enabled') "
                + "FROM config.investigation_journal WHERE event = 'route_agent_changed' ORDER BY journal_id DESC LIMIT 1", ct));
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{off}", Json, "{\"enabled\":true}")).Status);
            Assert.Equal("2", await ScalarAsync(owner, Journal, ct));
            Assert.Equal(200, (await Send("DELETE", $"/api/notification-routes/{off}", null)).Status);
            Assert.Equal($"person:alice|{off}|deleted route|{ServerId}|0", await ScalarAsync(owner,
                "SELECT actor || '|' || route_id || '|' || reason || '|' || server_id || '|' || (detail->'before'->>'agent_level') "
                + "FROM config.investigation_journal WHERE event = 'route_agent_changed' ORDER BY journal_id DESC LIMIT 1", ct));
            Assert.Equal("0", await ScalarAsync(owner, $"SELECT count(*) FROM config_notification_routes WHERE route_id = {off}", ct));

            /* A route with no agent level: nothing is written, from the web or the shared writer. */
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{plain}", Json, "{\"enabled\":false}")).Status);
            Assert.True(await NotificationRouteWriter.SetEnabledAsync(owner, plain, true, 30, ct, "bob"));
            Assert.True(await NotificationRouteWriter.DeleteAsync(owner, plain, 30, ct, "bob"));
            Assert.Equal("3", await ScalarAsync(owner, Journal, ct));

            /* The Viewer's writer: toggle and delete of an agent route journal by the login they name; with no login (the MCP tools) they do not. */
            var again = await SeedRouteAsync(owner, "performance", ct);
            await ExecAsync(owner, $"UPDATE config_notification_routes SET agent_level = 2 WHERE route_id = {again}", ct);
            Assert.True(await NotificationRouteWriter.SetEnabledAsync(owner, again, false, 30, ct));
            Assert.Equal("3", await ScalarAsync(owner, Journal, ct));
            Assert.True(await NotificationRouteWriter.SetEnabledAsync(owner, again, true, 30, ct, "bob"));
            Assert.True(await NotificationRouteWriter.SetEnabledAsync(owner, again, true, 30, ct, "bob"));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE actor = 'person:bob' AND reason = 'changed enabled'", ct));
            Assert.True(await NotificationRouteWriter.DeleteAsync(owner, again, 30, ct, "bob"));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE actor = 'person:bob' AND reason = 'deleted route'", ct));
            Assert.False(await NotificationRouteWriter.DeleteAsync(owner, again, 30, ct, "bob"));
            Assert.Equal("5", await ScalarAsync(owner, Journal, ct));
        }, seat: new DarlingWebSeat("alice", true));
    }

    /* #6274 round 2: profiles refuse a stale save the way routes do. The page's copy of the profile carries the modified_at it read; a profile
       saved or removed since is a 409 with the reload sentence, the text stays the other admin's, and no journal row is written. A first
       save needs no version. The Viewer's save is the same store method. */
    [Fact]
    public async Task EnvironmentProfiles_AStaleSave_Is409_AndLeavesTheTextAndTheJournalAlone_ViaTheWebAndTheStore()
    {
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            const string Journal = "SELECT count(*) FROM config.investigation_journal WHERE event IN ('profile_saved', 'profile_removed')";
            var store = new InvestigationRequestStore(owner, 30);

            /* A first save has no version; a second first-save finds the profile there and is refused. */
            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles?nostamp=1", Json, "{\"profile_text\":\"SQLPROD01 runs payroll 01:00-03:00, change nothing\"}")).Status);
            var late = await Send("PUT", "/api/environment-profiles?nostamp=1", Json, "{\"profile_text\":\"a second first save\"}");
            Assert.Equal(409, late.Status);
            Assert.Equal(InvestigationProfiles.ConflictText, (string)late.Body!["error"]!);

            /* The page loads this profile (the version it shows is the modified_at_utc it was sent), and another admin changes it. */
            var pageRead = (string)(await Send("GET", "/api/environment-profiles", null)).Body!["profiles"]![0]!["modified_at_utc"]!;
            await Task.Delay(20, ct);
            Assert.True(await store.SaveProfileAsync(null, null, "SQLPROD01 runs payroll 01:00-03:00 and 22:00-23:00", "carol", DateTime.Parse(pageRead, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal), ct));
            Assert.Equal("2", await ScalarAsync(owner, Journal, ct));

            var stale = await Send("PUT", "/api/environment-profiles", Json, $"{{\"expected_modified_at\":\"{pageRead}\",\"profile_text\":\"a typo fix on an hour-old page\"}}");
            Assert.Equal(409, stale.Status);
            Assert.Equal(InvestigationProfiles.ConflictText, (string)stale.Body!["error"]!);
            Assert.Equal("SQLPROD01 runs payroll 01:00-03:00 and 22:00-23:00|person:carol", await ScalarAsync(owner, "SELECT profile_text || '|' || modified_by FROM config.agent_environment_profiles", ct));
            Assert.Equal("2", await ScalarAsync(owner, Journal, ct));

            /* A stale blank save is refused too: it does not remove the other admin's text. */
            Assert.Equal(409, (await Send("PUT", "/api/environment-profiles", Json, $"{{\"expected_modified_at\":\"{pageRead}\",\"profile_text\":\"\"}}")).Status);
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.agent_environment_profiles", ct));

            /* The Viewer's call: a version that is old, and one for a profile that was removed since, are both refused. */
            var oldVersion = DateTime.Parse(pageRead, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
            await Assert.ThrowsAsync<InvestigationProfileConflictException>(() => store.SaveProfileAsync(null, null, "an out-of-date edit", "dave", oldVersion, ct));
            await Assert.ThrowsAsync<InvestigationProfileConflictException>(() => store.SaveProfileAsync(null, null, "added over a profile the screen did not show", "dave", null, ct));
            var current = (await store.ListProfilesAsync(ct))[0].ModifiedAtUtc;
            Assert.True(await store.SaveProfileAsync(null, null, "   ", "dave", current, ct));
            await Assert.ThrowsAsync<InvestigationProfileConflictException>(() => store.SaveProfileAsync(null, null, "edit of a removed profile", "erin", current, ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.agent_environment_profiles", ct));
            Assert.Equal("3", await ScalarAsync(owner, Journal, ct));

            /* A version of the wrong shape is a 400, and the default date is not a pass. */
            Assert.Equal(400, (await Send("PUT", "/api/environment-profiles", Json, "{\"expected_modified_at\":17,\"profile_text\":\"x\"}")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/environment-profiles", Json, "{\"expected_modified_at\":\"not a date\",\"profile_text\":\"x\"}")).Status);
        }, seat: new DarlingWebSeat("alice", true));
    }

    /* #6400: a profile for tag 0 (or below), or for a tag that does not exist, is a 400 with one sentence and writes nothing. The store
       matches a scope on coalesce(scope_tag_id, 0), so a tag-0 row was never read by any brief, made every later fleet save a conflict, and a
       blank tag-0 save carrying the fleet row's version deleted the fleet profile. The Viewer's save is the same store method. */
    [Fact]
    public async Task EnvironmentProfiles_ATagThatIs0OrBelowOrNamesNoTag_Is400_AndWritesNothing_ViaTheWebAndTheStore()
    {
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            const string Rows = "SELECT count(*) FROM config.agent_environment_profiles";
            const string Journal = "SELECT count(*) FROM config.investigation_journal WHERE event IN ('profile_saved', 'profile_removed')";
            var store = new InvestigationRequestStore(owner, 30);
            var tag = await ScalarAsync(owner, "INSERT INTO config.server_tags (name) VALUES ('oltp-6400') RETURNING id", ct);
            var unknown = (await ScalarAsync(owner, "SELECT coalesce(max(id), 0) + 1 FROM config.server_tags", ct))!;

            /* No fleet profile yet: tag 0, a negative tag and an unknown tag are all refused, and the fleet can still be saved afterwards. */
            foreach (var refused in new[] { "0", "-1", unknown })
            {
                var answer = await Send("PUT", "/api/environment-profiles", Json, $"{{\"scope_tag_id\":{refused},\"profile_text\":\"for no tag\"}}");
                Assert.Equal(400, answer.Status);
                Assert.Contains($"{refused}", (string)answer.Body!["error"]!, StringComparison.Ordinal);
                await Assert.ThrowsAsync<ArgumentException>(() => store.SaveProfileAsync(null, int.Parse(refused, CultureInfo.InvariantCulture), "for no tag", "dave", null, ct));
            }

            Assert.Equal("0", await ScalarAsync(owner, Rows, ct));
            Assert.Equal("0", await ScalarAsync(owner, Journal, ct));
            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles", Json, "{\"profile_text\":\"fleet: OLTP\"}")).Status);

            /* A fleet profile is there: a blank tag-0 save that carries the fleet row's version (the page stamps it so) removes nothing. */
            var fleetStamp = await ScalarAsync(owner, "SELECT modified_at::text FROM config.agent_environment_profiles", ct);
            Assert.Equal(400, (await Send("PUT", "/api/environment-profiles", Json, "{\"scope_tag_id\":0,\"profile_text\":\"\"}")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/environment-profiles", Json, "{\"scope_tag_id\":0,\"profile_text\":\"for no tag\"}")).Status);
            Assert.Equal("1|fleet: OLTP", await ScalarAsync(owner, "SELECT count(*) || '|' || max(profile_text) FROM config.agent_environment_profiles WHERE scope_server_id IS NULL AND scope_tag_id IS NULL", ct));
            Assert.Equal(fleetStamp, await ScalarAsync(owner, "SELECT modified_at::text FROM config.agent_environment_profiles", ct));
            Assert.Equal("1", await ScalarAsync(owner, Rows, ct));
            Assert.Equal("1", await ScalarAsync(owner, Journal, ct));

            /* An existing tag still saves; once its tag is deleted the orphan profile can still be removed by a blank save. */
            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles", Json, $"{{\"scope_tag_id\":{tag},\"profile_text\":\"tag: OLTP\"}}")).Status);
            Assert.Equal("2", await ScalarAsync(owner, Rows, ct));
            await ExecAsync(owner, $"DELETE FROM config.server_tags WHERE id = {tag}", ct);
            Assert.Equal(400, (await Send("PUT", "/api/environment-profiles", Json, $"{{\"scope_tag_id\":{tag},\"profile_text\":\"tag: OLTP, edited\"}}")).Status);
            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles", Json, $"{{\"scope_tag_id\":{tag},\"profile_text\":\"\"}}")).Status);
            Assert.Equal("1", await ScalarAsync(owner, Rows, ct));
        }, seat: new DarlingWebSeat("alice", true));
    }

    /* #6400: the server side of the tag test above. The profile table has no foreign key to a server either, so a profile for server 0 (or
       below), or for a server id that is not registered (no row in config.config_monitored_servers), is a 400 with one sentence and writes
       nothing; a registered server still saves, and a blank save still removes the profile of a server that has gone. */
    [Fact]
    public async Task EnvironmentProfiles_AServerThatIs0OrBelowOrIsNotRegistered_Is400_AndWritesNothing_ViaTheWebAndTheStore()
    {
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            const string Rows = "SELECT count(*) FROM config.agent_environment_profiles";
            const string Journal = "SELECT count(*) FROM config.investigation_journal WHERE event IN ('profile_saved', 'profile_removed')";
            var store = new InvestigationRequestStore(owner, 30);
            var unregistered = ServerId + 1000;
            var neverConnected = ServerId + 2000;

            /* The registered/observed split (#6400): `unregistered` was observed (it has a collect.servers row, as a removed server keeps one) but
               has no definition; `neverConnected` is defined and has never connected (no collect.servers row, as a server just added). */
            await using (var observed = await owner.OpenConnectionAsync(ct))
            {
                await DarlingMcpTestData.RegisterServerAsync(observed, unregistered, "LcAgentSetupRemoved", ct);
            }

            await ExecAsync(owner, $"INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES ({neverConnected}, 'LcAgentSetupNew', 'host-new')", ct);
            Assert.Equal("0", await ScalarAsync(owner, $"SELECT count(*) FROM collect.servers WHERE server_id = {neverConnected}", ct));
            Assert.Equal("0", await ScalarAsync(owner, $"SELECT count(*) FROM config.config_monitored_servers WHERE server_id = {unregistered}", ct));

            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles", Json, "{\"profile_text\":\"fleet: OLTP\"}")).Status);
            var fleetStamp = await ScalarAsync(owner, "SELECT modified_at::text FROM config.agent_environment_profiles", ct);
            Assert.Equal("1", await ScalarAsync(owner, Journal, ct));

            /* Server 0, a negative server and an unregistered server are refused, with text and blank, and the fleet profile is not touched. */
            foreach (var refused in new[] { 0, -1, unregistered })
            {
                var answer = await Send("PUT", "/api/environment-profiles", Json, $"{{\"scope_server_id\":{refused},\"profile_text\":\"for no server\"}}");
                Assert.Equal(400, answer.Status);
                var sentence = (string)answer.Body!["error"]!;
                Assert.Contains($"{refused}", sentence, StringComparison.Ordinal);
                Assert.DoesNotContain("Parameter", sentence, StringComparison.Ordinal);
                await Assert.ThrowsAsync<ArgumentException>(() => store.SaveProfileAsync(refused, null, "for no server", "dave", null, ct));
            }

            Assert.Equal(400, (await Send("PUT", "/api/environment-profiles", Json, "{\"scope_server_id\":0,\"profile_text\":\"\"}")).Status);
            Assert.Equal("1|fleet: OLTP", await ScalarAsync(owner, "SELECT count(*) || '|' || max(profile_text) FROM config.agent_environment_profiles WHERE scope_server_id IS NULL AND scope_tag_id IS NULL", ct));
            Assert.Equal(fleetStamp, await ScalarAsync(owner, "SELECT modified_at::text FROM config.agent_environment_profiles", ct));
            Assert.Equal("1", await ScalarAsync(owner, Rows, ct));
            Assert.Equal("1", await ScalarAsync(owner, Journal, ct));

            /* A registered server still saves; an orphan profile (its server gone) can still be removed by a blank save. */
            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles", Json, $"{{\"scope_server_id\":{ServerId},\"profile_text\":\"server: reporting\"}}")).Status);
            Assert.Equal("2", await ScalarAsync(owner, Rows, ct));

            /* A defined server that has never connected saves, through the web and through the store. */
            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles", Json, $"{{\"scope_server_id\":{neverConnected},\"profile_text\":\"server: just added\"}}")).Status);
            Assert.Equal("3", await ScalarAsync(owner, Rows, ct));
            await store.SaveProfileAsync(neverConnected, null, "server: just added, edited", "dave",
                (await store.ListProfilesAsync(ct)).First(p => p.ScopeServerId == neverConnected).ModifiedAtUtc, ct);
            Assert.Equal("server: just added, edited", await ScalarAsync(owner, $"SELECT profile_text FROM config.agent_environment_profiles WHERE scope_server_id = {neverConnected}", ct));

            await ExecAsync(owner, $"INSERT INTO config.agent_environment_profiles (scope_server_id, profile_text, modified_by) VALUES ({unregistered}, 'orphan', 'test')", ct);
            Assert.Equal(400, (await Send("PUT", "/api/environment-profiles", Json, $"{{\"scope_server_id\":{unregistered},\"profile_text\":\"orphan, edited\"}}")).Status);
            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles", Json, $"{{\"scope_server_id\":{unregistered},\"profile_text\":\"\"}}")).Status);
            Assert.Equal("3", await ScalarAsync(owner, Rows, ct));
        }, seat: new DarlingWebSeat("alice", true));
    }

    /* The page's round trip: the modified_at_utc a GET answers is sent back verbatim as expected_modified_at and is accepted, for routes and
       for profiles. The other tests stamp their PUTs from SQL; this one uses only what the page is given. */
    [Fact]
    public async Task TheModifiedAtTheListAnswers_SentBackVerbatim_IsAcceptedAsTheVersion_ForRoutesAndProfiles()
    {
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            var seeded = await SeedRouteAsync(owner, "performance", ct);
            /* The route page reads its routes through get_notification_routes; modified_at_utc is what it is given. */
            var routeVersion = System.Text.Json.JsonDocument.Parse(await DarlingMcpAlertTools.GetNotificationRoutes(owner, ct)).RootElement
                .GetProperty("routes").EnumerateArray().Single(r => r.GetProperty("route_id").GetInt32() == seeded).GetProperty("modified_at_utc").GetString()!;
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, $"{{\"expected_modified_at\":\"{routeVersion}\",\"agent_level\":1}}")).Status);
            Assert.Equal(409, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, $"{{\"expected_modified_at\":\"{routeVersion}\",\"agent_level\":2}}")).Status);

            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles", Json, "{\"profile_text\":\"fleet: OLTP\"}")).Status);
            var profileVersion = (string)(await Send("GET", "/api/environment-profiles", null)).Body!["profiles"]![0]!["modified_at_utc"]!;
            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles", Json, $"{{\"expected_modified_at\":\"{profileVersion}\",\"profile_text\":\"fleet: OLTP, 24x7\"}}")).Status);
            Assert.Equal(409, (await Send("PUT", "/api/environment-profiles", Json, $"{{\"expected_modified_at\":\"{profileVersion}\",\"profile_text\":\"fleet: stale\"}}")).Status);
            Assert.Equal("fleet: OLTP, 24x7", await ScalarAsync(owner, "SELECT profile_text FROM config.agent_environment_profiles", ct));
        }, seat: new DarlingWebSeat("alice", true));
    }

    /* Round 1's remaining cases, over the web: a signing key blocks a change of the URL text (RemoveKeyFirstText), a padded "-" clears like a
       bare one (and the clear is not mistaken for a value), and a profile body cannot name who saved it. */
    [Fact]
    public async Task AgentRoutes_RemoveKeyFirst_APaddedDashClears_AndAProfileBodyCannotNameItsAuthor()
    {
        using var key = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            async Task<string> Stored(int id, string column) => await ScalarAsync(owner, $"SELECT {column} FROM config_notification_routes WHERE route_id = {id}", ct) ?? "";
            var created = await Send("POST", "/api/notification-routes", Json,
                "{\"metric_match\":\"blocking\",\"slack_webhook_url\":\"https://hooks.example.test/services/T000/B000/slack-token\",\"generic_webhook_url\":\"https://generic.example.test/in/one\"}");
            Assert.Equal(200, created.Status);
            var seeded = (int)created.Body!["route_id"]!;

            /* A signing key sealed for the generic URL's stored text: a change of that text, or clearing it, is refused with the shared sentence
               and writes nothing. */
            await ExecAsync(owner, $"UPDATE config_notification_routes SET signing_key = 'seal:v1:key' WHERE route_id = {seeded}", ct);
            var generic = await Stored(seeded, "generic_url");
            var changed = await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"generic_webhook_url\":\"https://generic.example.test/in/two\"}");
            Assert.Equal(400, changed.Status);
            Assert.Equal(NotificationRouteWriter.RemoveKeyFirstText, (string)changed.Body!["error"]!);
            var padded = await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"generic_webhook_url\":\"  -  \"}");
            Assert.Equal(400, padded.Status);
            Assert.Equal(NotificationRouteWriter.RemoveKeyFirstText, (string)padded.Body!["error"]!);
            Assert.Equal(generic, await Stored(seeded, "generic_url"));
            await ExecAsync(owner, $"UPDATE config_notification_routes SET signing_key = '' WHERE route_id = {seeded}", ct);

            /* Without a key, a padded dash clears the destination, as a bare one does, and only that destination. */
            Assert.Equal(200, (await Send("PUT", $"/api/notification-routes/{seeded}", Json, "{\"slack_webhook_url\":\" - \"}")).Status);
            Assert.Equal("", await Stored(seeded, "slack_url"));
            Assert.Equal(generic, await Stored(seeded, "generic_url"));

            /* A profile body names no author: modified_by is refused by name and nothing is written. */
            var forged = await Send("PUT", "/api/environment-profiles", Json, "{\"profile_text\":\"fleet: OLTP\",\"modified_by\":\"person:somebody-else\"}");
            Assert.Equal(400, forged.Status);
            Assert.Equal("Unknown field: modified_by.", (string)forged.Body!["error"]!);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.agent_environment_profiles", ct));
            Assert.Equal(200, (await Send("PUT", "/api/environment-profiles", Json, "{\"profile_text\":\"fleet: OLTP\"}")).Status);
            Assert.Equal("person:alice", await ScalarAsync(owner, "SELECT modified_by FROM config.agent_environment_profiles", ct));
        }, ring, seat: new DarlingWebSeat("alice", true));
    }

    /* The profile journal row is dated by the store, not the process that wrote it: a store given a clock years off still writes a row dated
       by now() in SQL, as the Viewer's own insert did before the web shared it. */
    [Fact]
    public async Task EnvironmentProfiles_TheJournalRow_IsDatedByTheStoresClock_NotTheCallers()
    {
        await WithWebRoutesAsync("admin", async (owner, _, _, ct) =>
        {
            var skewed = new InvestigationRequestStore(owner, 30, () => new DateTime(2001, 1, 1, 0, 0, 0, DateTimeKind.Utc));
            Assert.True(await skewed.SaveProfileAsync(null, null, "fleet: OLTP", "alice", null, ct));
            Assert.True(await skewed.SaveProfileAsync(null, null, "   ", "alice", await ProfileVersionAsync(owner, ct), ct));
            Assert.Equal("2|0", await ScalarAsync(owner,
                "SELECT count(*) || '|' || count(*) FILTER (WHERE abs(extract(epoch FROM now() - event_time)) > 120) FROM config.investigation_journal WHERE event IN ('profile_saved', 'profile_removed')", ct));
        });
    }
}
