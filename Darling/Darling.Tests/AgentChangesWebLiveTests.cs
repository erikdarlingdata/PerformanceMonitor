/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5917: the Agent changes web routes against a live store, the web host logging in as a role that holds what the managed
/// <c>viewer</c> or <c>admin</c> role holds. The viewer role lists (and the body says it cannot decide), and an undo and the
/// switch answer 403 with the fixed sentence and change nothing; the admin role undoes and switches; a call with no result
/// row is listed as unknown.
/// </summary>
public sealed class AgentChangesWebLiveTests
{
    private const string RolePassword = "agent-changes-live-pw";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AsTheManagedViewerRole_TheListWorks_AndUndoAndTheSwitchAnswer403_ChangingNothing()
    {
        await WithWebRoutesAsync("web", async (owner, Send, ct) =>
        {
            var list = await Send("GET", "/api/agent-changes", null);
            Assert.Equal(200, list.Status);
            Assert.False((bool)list.Body!["can_decide"]!);
            Assert.True((bool)list.Body["enabled"]!);
            Assert.Equal(3, list.Body["changes"]!.AsArray().Count);
            var results = string.Join(",", list.Body["changes"]!.AsArray().Select(c => (string)c!["result"]!));
            Assert.Contains("unknown", results, StringComparison.Ordinal);
            Assert.Contains("done", results, StringComparison.Ordinal);

            var planCall = await ScalarAsync(owner, "SELECT journal_id FROM config.investigation_journal WHERE detail->>'kind' = 'plan_force'", ct);
            var planRow = list.Body["changes"]!.AsArray().Single(c => (string?)c!["kind"] == "plan_force")!;
            Assert.True((bool)planRow["can_undo"]!);
            var planUndo = await Send("PUT", "/api/agent-changes/" + planCall + "/undo", "application/json", "{\"confirm\":true}");
            Assert.Equal(403, planUndo.Status);
            Assert.Equal(DarlingWebEndpoints.AgentChangeRefusedMessage(false), (string)planUndo.Body!["error"]!);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE actor LIKE 'person:%'", ct));

            var undone = await Send("PUT", "/api/agent-changes/" + await CalledIdAsync(owner, "m-web", ct) + "/undo", "application/json");
            Assert.Equal(403, undone.Status);
            Assert.Equal(DarlingWebEndpoints.AgentChangeRefusedMessage(false), (string)undone.Body!["error"]!);
            Assert.Equal(
                "This dashboard's store login cannot undo or switch off agent changes. Use the Viewer app to make the change, or give the web an admin store login (set postgres.webConnectionString to an admin login).",
                (string)undone.Body["error"]!);

            var off = await Send("PUT", "/api/agent-changes/switch", "application/json", "{\"enabled\":false}");
            Assert.Equal(403, off.Status);
            Assert.Equal(DarlingWebEndpoints.AgentChangeRefusedMessage(false), (string)off.Body!["error"]!);

            Assert.Equal("agent says", await ScalarAsync(owner, "SELECT reason FROM config.config_mute_rules WHERE id = 'm-web'", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_service WHERE NOT agent_actions_enabled", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event IN ('action_undone', 'agent_actions_switched')", ct));
        });
    }

    [Fact]
    public async Task OnAnUnsignedLoopbackDashboard_APlanUndo_Answers403WithTheSignInSentence_AndWritesAndJournalsNothing()
    {
        await WithWebRoutesAsync("admin", async (owner, Send, ct) =>
        {
            var planCall = await ScalarAsync(owner, "SELECT journal_id FROM config.investigation_journal WHERE detail->>'kind' = 'plan_force'", ct);
            var before = await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct);
            foreach (var payload in new[] { "{}", "{\"confirm\":true}" })
            {
                var refused = await Send("PUT", "/api/agent-changes/" + planCall + "/undo", "application/json", payload);
                Assert.Equal(403, refused.Status);
                Assert.Equal(DarlingWebEndpoints.InvestigationWriteSignInMessage, (string)refused.Body!["error"]!);
            }

            Assert.Equal(before, await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));
        }, signedInAs: () => null);
    }

    [Fact]
    public async Task AsTheManagedAdminRole_APlanUndo_IsRefusedWith409WhenTheServersSwitchIsOff_JournaledWithThePerson_AndNeverWritesWithoutTheSwitch()
    {
        await WithWebRoutesAsync("admin", async (owner, Send, ct) =>
        {
            var list = await Send("GET", "/api/agent-changes", null);
            var planRow = list.Body!["changes"]!.AsArray().Single(c => (string?)c!["kind"] == "plan_force")!;
            Assert.True((bool)planRow["can_undo"]!);
            Assert.Equal("unknown", (string)planRow["result"]!);

            var planCall = await ScalarAsync(owner, "SELECT journal_id FROM config.investigation_journal WHERE detail->>'kind' = 'plan_force'", ct);
            /* A refusal is journaled once per call, code and HOUR, keyed on the stamp the store took (its own clock, which the web route
               cannot be given), so two refusals journal one row inside an hour and two rows across an hour boundary. The journal's own
               stamps own both ends of that: one row per distinct UTC hour, and one or two rows, wherever the hour happened to fall. */
            foreach (var payload in new[] { "{}", "{\"confirm\":true}" })
            {
                var refused = await Send("PUT", "/api/agent-changes/" + planCall + "/undo", "application/json", payload);
                Assert.Equal(409, refused.Status);
                Assert.Equal("server_action_off", (string)refused.Body!["code"]!);
                Assert.Contains("config_server_actions", (string)refused.Body["error"]!, StringComparison.Ordinal);
            }

            var refusals = await ScalarAsync(owner, "SELECT count(*) || ':' || count(DISTINCT date_trunc('hour', event_time AT TIME ZONE 'UTC')) FROM config.investigation_journal WHERE event = 'refused' AND actor LIKE 'person:%' AND reason = 'server_action_off'", ct);
            Assert.True(refusals is "1:1" or "2:2", $"two refusals journal one row per distinct UTC hour, so 1 or 2 rows (rows:hours), got {refusals}");
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'action_undone'", ct));
            Assert.Equal(400, (await Send("PUT", "/api/agent-changes/" + planCall + "/undo", "application/json", "[1]")).Status);
            Assert.Equal(415, (await Send("PUT", "/api/agent-changes/" + planCall + "/undo", "text/plain", "{}")).Status);
        });
    }

    [Fact]
    public async Task AsTheManagedAdminRole_AnUndoRestoresTheMute_TheSwitchTurnsOff_AndASecondClickOrABadIdOrBodyIsRefused()
    {
        await WithWebRoutesAsync("admin", async (owner, Send, ct) =>
        {
            var list = await Send("GET", "/api/agent-changes", null);
            Assert.True((bool)list.Body!["can_decide"]!);

            /* No service row yet (#6401): the switch is refused with 409 and the row is not created. */
            var notSetUp = await Send("PUT", "/api/agent-changes/switch", "application/json", "{\"enabled\":false}");
            Assert.Equal(409, notSetUp.Status);
            Assert.Equal("store_not_set_up", (string)notSetUp.Body!["code"]!);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_service", ct));
            await ExecAsync(owner, "INSERT INTO config.config_service (id) VALUES (1)", ct);

            var called = await CalledIdAsync(owner, "m-web", ct);
            var undone = await Send("PUT", "/api/agent-changes/" + called + "/undo", "application/json");
            Assert.Equal(200, undone.Status);
            Assert.Equal("undone", (string)undone.Body!["status"]!);
            Assert.Equal("old", await ScalarAsync(owner, "SELECT reason FROM config.config_mute_rules WHERE id = 'm-web'", ct));

            /* A second click finds it undone (409), an unknown call is a 404, and a bad switch body is a 400. */
            Assert.Equal(409, (await Send("PUT", "/api/agent-changes/" + called + "/undo", "application/json")).Status);
            Assert.Equal(404, (await Send("PUT", "/api/agent-changes/999999/undo", "application/json")).Status);

            var off = await Send("PUT", "/api/agent-changes/switch", "application/json", "{\"enabled\":false}");
            Assert.Equal(200, off.Status);
            Assert.Equal("False", await ScalarAsync(owner, "SELECT agent_actions_enabled FROM config.config_service", ct));
            Assert.Equal(400, (await Send("PUT", "/api/agent-changes/switch", "application/json", "{\"enabled\":\"yes\"}")).Status);
        });
    }

    /* #6265 round 2: the undo and the service-wide switch are a person's acts. A dashboard with no sign-in answers 403 with the sign-in
       sentence, for a real call, an unknown one and a plan change alike, before any store read, and changes nothing. */
    [Fact]
    public async Task WithNoSignedInAdmin_UndoAndTheSwitchAnswer403_ChangingNothing()
    {
        await WithWebRoutesAsync("admin", async (owner, Send, ct) =>
        {
            var called = await CalledIdAsync(owner, "m-web", ct);
            var planCall = await ScalarAsync(owner, "SELECT journal_id FROM config.investigation_journal WHERE detail->>'kind' = 'plan_force'", ct);
            var journalBefore = await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct);

            foreach (var (path, payload) in new[]
            {
                ("/api/agent-changes/" + called + "/undo", "{}"),
                ("/api/agent-changes/" + planCall + "/undo", "{\"confirm\":true}"),
                ("/api/agent-changes/999999/undo", "{}"),
                ("/api/agent-changes/switch", "{\"enabled\":false}"),
                ("/api/agent-changes/switch", "{\"enabled\":\"yes\"}"),
            })
            {
                var answer = await Send("PUT", path, "application/json", payload);
                Assert.True(answer.Status == 403, $"{path} answered {answer.Status}");
                Assert.Equal(SignInSentence, (string)answer.Body!["error"]!);
            }

            Assert.Equal("agent says", await ScalarAsync(owner, "SELECT reason FROM config.config_mute_rules WHERE id = 'm-web'", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_service", ct));
            Assert.Equal(journalBefore, await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));
        }, () => null);
    }

    /* The undo and the switch stamp the signed-in person's exact name (a name with a space, as a Viewer or a web sign-in can hold),
       once: person:<name>, never person:person:<name>. */
    [Fact]
    public async Task TheUndoAndTheSwitch_StampTheSignedInPersonsExactName()
    {
        await WithWebRoutesAsync("admin", async (owner, Send, ct) =>
        {
            await ExecAsync(owner, "INSERT INTO config.config_service (id) VALUES (1)", ct);
            var called = await CalledIdAsync(owner, "m-web", ct);
            var undone = await Send("PUT", "/api/agent-changes/" + called + "/undo", "application/json");
            Assert.Equal(200, undone.Status);
            Assert.Equal("person:John Smith", (string)undone.Body!["undone_by"]!);
            Assert.Equal("person:John Smith", await ScalarAsync(owner,
                $"SELECT actor FROM config.investigation_journal WHERE event = 'action_undone' AND detail->>'called_id' = '{called}'", ct));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'action_undone'", ct));

            Assert.Equal(200, (await Send("PUT", "/api/agent-changes/switch", "application/json", "{\"enabled\":false}")).Status);
            Assert.Equal("person:John Smith", await ScalarAsync(owner, "SELECT updated_by FROM config.config_service", ct));
            Assert.Equal("person:John Smith", await ScalarAsync(owner, "SELECT actor FROM config.investigation_journal WHERE event = 'agent_actions_switched'", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE actor LIKE 'person:person:%'", ct));
        }, () => "John Smith");
    }

    /* #6600: the managed store's shape. The web reads as the viewer role; a signed-in admin's access check, undo and switch go through the arm and
       record the person. A plan undo that the server's switch refuses is still journaled (a viewer-role insert would fail). */
    [Fact]
    public async Task OnTheManagedStore_ASignedInAdminSeat_UndoesAndSwitchesThroughTheArm_AndCanDecideIsTrue()
    {
        await WithWebRoutesAsync("web", async (owner, Send, ct) =>
        {
            var list = await Send("GET", "/api/agent-changes", null);
            Assert.Equal(200, list.Status);
            Assert.True((bool)list.Body!["can_decide"]!);

            var planCall = await ScalarAsync(owner, "SELECT journal_id FROM config.investigation_journal WHERE detail->>'kind' = 'plan_force'", ct);
            var planUndo = await Send("PUT", "/api/agent-changes/" + planCall + "/undo", "application/json", "{\"confirm\":true}");
            Assert.Equal(409, planUndo.Status);
            Assert.Equal("server_action_off", (string)planUndo.Body!["code"]!);
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND actor = 'person:alice'", ct));

            var called = await CalledIdAsync(owner, "m-web", ct);
            var undone = await Send("PUT", "/api/agent-changes/" + called + "/undo", "application/json");
            Assert.Equal(200, undone.Status);
            Assert.Equal("person:alice", (string)undone.Body!["undone_by"]!);
            Assert.Equal("person:alice", await ScalarAsync(owner,
                $"SELECT actor FROM config.investigation_journal WHERE event = 'action_undone' AND detail->>'called_id' = '{called}'", ct));

            /* #6401: the switch updates the service row and never creates it, so the store needs its row first. */
            await ExecAsync(owner, "INSERT INTO config.config_service (id) VALUES (1)", ct);
            Assert.Equal(200, (await Send("PUT", "/api/agent-changes/switch", "application/json", "{\"enabled\":false}")).Status);
            Assert.Equal("person:alice", await ScalarAsync(owner, "SELECT updated_by FROM config.config_service", ct));
            Assert.Equal("person:alice", await ScalarAsync(owner, "SELECT actor FROM config.investigation_journal WHERE event = 'agent_actions_switched'", ct));
        }, () => "alice", arm: AdminArmShape.Admin, managedStore: true);
    }

    /* #6600: an OIDC viewer seat never opens the arm (its login is refused here, so opening it would fail): the list says it cannot decide and the
       writes answer with the sign-in sentence, as they do today. */
    [Fact]
    public async Task OnTheManagedStore_AViewerSeat_NeverOpensTheArm_AndAnswersAsToday()
    {
        await WithWebRoutesAsync("web", async (owner, Send, ct) =>
        {
            var list = await Send("GET", "/api/agent-changes", null);
            Assert.Equal(200, list.Status);
            Assert.False((bool)list.Body!["can_decide"]!);

            var undone = await Send("PUT", "/api/agent-changes/" + await CalledIdAsync(owner, "m-web", ct) + "/undo", "application/json");
            Assert.Equal(403, undone.Status);
            Assert.Equal(SignInSentence, (string)undone.Body!["error"]!);
            Assert.Equal(403, (await Send("PUT", "/api/agent-changes/switch", "application/json", "{\"enabled\":false}")).Status);
            Assert.Equal("agent says", await ScalarAsync(owner, "SELECT reason FROM config.config_mute_rules WHERE id = 'm-web'", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE actor LIKE 'person:%'", ct));
        }, () => "viewer-person", arm: AdminArmShape.Refused, managedStore: true, seatCanEdit: false);
    }

    /* #6600: with the switch off (no arm on a managed store) the list-only answers are today's, and the 403 names the switch. */
    [Fact]
    public async Task OnTheManagedStore_WithTheSwitchOff_TheSignedInAdminStillListsOnly_NamingTheSwitch()
    {
        await WithWebRoutesAsync("web", async (owner, Send, ct) =>
        {
            Assert.False((bool)(await Send("GET", "/api/agent-changes", null)).Body!["can_decide"]!);
            var off = await Send("PUT", "/api/agent-changes/switch", "application/json", "{\"enabled\":false}");
            Assert.Equal(403, off.Status);
            Assert.Equal(DarlingWebEndpoints.AgentChangeRefusedMessage(true, adminWritesOff: true), (string)off.Body!["error"]!);
            Assert.Contains("web.allowAdminWrites", (string)off.Body["error"]!, StringComparison.Ordinal);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_service WHERE NOT agent_actions_enabled", ct));
        }, () => "alice", managedStore: true);
    }

    private delegate Task<(int Status, JsonNode? Body)> Sender(string method, string path, string? contentType, string? payload = null);

    private static async Task<string?> CalledIdAsync(NpgsqlDataSource owner, string target, CancellationToken ct) =>
        await ScalarAsync(owner,
            $"SELECT j.journal_id FROM config.investigation_journal j WHERE j.event = 'action_called' AND j.detail->'target'->>'id' = '{target}' AND EXISTS (SELECT 1 FROM config.investigation_journal d WHERE d.event = 'action_done' AND d.detail->>'called_id' = j.journal_id::text) ORDER BY j.journal_id LIMIT 1", ct);

    private const string SignInSentence = "Sign in to the web as an admin to make this change, or use the Viewer.";

    /// <param name="signedInAs">The signed-in admin's name read on every request (default a real email), or null for a dashboard with
    /// no sign-in configured (no seat is stored, as the host does when no web token is set).</param>
    private static async Task WithWebRoutesAsync(
        string managedRole, Func<NpgsqlDataSource, Sender, CancellationToken, Task> body, Func<string?>? signedInAs = null,
        AdminArmShape arm = AdminArmShape.None, bool managedStore = false, bool seatCanEdit = true)
    {
        signedInAs ??= () => "erik@example.com";
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the agent changes web live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "agent_seat_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        NpgsqlDataSource owner;
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        }

        await using var ownerSource = owner;
        var bodySucceeded = false;
        try
        {
            /* A mute the agent changed (done) and one call that never got a result row. */
            await ExecAsync(owner, "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason) VALUES ('m-web', true, now() at time zone 'utc', 'agent says')", ct);
            var request = Guid.NewGuid();
            var called = await ScalarAsync(owner, $@"INSERT INTO config.investigation_journal (request_id, server_id, event, actor, detail)
VALUES ('{request}', 7, 'action_called', 'agent:a', '{{""tool"":""t"",""kind"":""mute"",""target"":{{""id"":""m-web"",""server_id"":7,""database"":null}},""before"":{{""reason"":""old"",""enabled"":true}}}}') RETURNING journal_id", ct);
            await ExecAsync(owner, $@"INSERT INTO config.investigation_journal (request_id, server_id, event, actor, detail)
VALUES ('{request}', 7, 'action_done', 'agent:a', '{{""called_id"":{called},""after"":{{""status"":""updated"",""mute_rule"":{{""reason"":""agent says"",""enabled"":true}}}}}}')", ct);
            await ExecAsync(owner, $@"INSERT INTO config.investigation_journal (request_id, server_id, event, actor, detail)
VALUES ('{Guid.NewGuid()}', 7, 'action_called', 'agent:a', '{{""tool"":""t"",""kind"":""mute"",""target"":{{""id"":""lost"",""server_id"":7,""database"":null}},""before"":null}}')", ct);
            /* A plan force whose call has no result row (#5632, step 9): a plan row is offered an undo whatever its result. */
            await ExecAsync(owner, $@"INSERT INTO config.investigation_journal (request_id, server_id, event, actor, detail)
VALUES ('{Guid.NewGuid()}', 7, 'action_called', 'agent:a', '{{""tool"":""force_query_store_plan"",""kind"":""plan_force"",""target"":{{""id"":""20"",""server_id"":7,""database"":""d""}},""before"":{{""database"":""d"",""query_id"":10,""plan_id"":20,""forced_plan"":null}}}}')", ct);

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
            app.Use(async (http, next) =>
            {
                if (signedInAs() is { } name)
                {
                    http.Items[PerformanceMonitor.Darling.Service.Hosting.DarlingWebSeat.HttpContextItemKey] = new PerformanceMonitor.Darling.Service.Hosting.DarlingWebSeat(name, seatCanEdit);
                }

                await next(http);
            });
            var ownerSeam = new PerformanceMonitor.Darling.Service.Mcp.StoreOwnerSource();
            ownerSeam.Publish(owner);
            DarlingWebEndpoints.MapAll(app, webLogin, new CollectorRuntimeState(), new CapturingTestLogger(), ownerSource: ownerSeam,
                postgresConfig: managedStore ? new PostgresConfig { Managed = true } : null, adminArm: adminArm);
            await app.StartAsync(ct);
            using var server = app.GetTestServer();

            async Task<(int Status, JsonNode? Body)> Send(string method, string path, string? contentType, string? payload = null)
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

                    request.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(payload ?? (method == "PUT" ? "{}" : "")));
                });
                using var reader = new StreamReader(context.Response.Body);
                var text = await reader.ReadToEndAsync(ct);
                return (context.Response.StatusCode, string.IsNullOrWhiteSpace(text) ? null : JsonNode.Parse(text));
            }

            await body(owner, new Sender(Send), ct);
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

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }
}
