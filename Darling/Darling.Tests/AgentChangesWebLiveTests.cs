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
        await WithWebRoutesAsync("viewer", async (owner, Send, ct) =>
        {
            var list = await Send("GET", "/api/agent-changes", null);
            Assert.Equal(200, list.Status);
            Assert.False((bool)list.Body!["can_decide"]!);
            Assert.True((bool)list.Body["enabled"]!);
            Assert.Equal(2, list.Body["changes"]!.AsArray().Count);
            var results = string.Join(",", list.Body["changes"]!.AsArray().Select(c => (string)c!["result"]!));
            Assert.Contains("unknown", results, StringComparison.Ordinal);
            Assert.Contains("done", results, StringComparison.Ordinal);

            var undone = await Send("PUT", "/api/agent-changes/" + await CalledIdAsync(owner, "m-web", ct) + "/undo", "application/json");
            Assert.Equal(403, undone.Status);
            Assert.Equal(DarlingWebEndpoints.AgentChangeRefusedMessage, (string)undone.Body!["error"]!);
            Assert.Equal(
                "This dashboard signs in to the store as the viewer role, so it cannot undo or switch off agent changes. Use the Viewer app, or run the web under the admin login.",
                (string)undone.Body["error"]!);

            var off = await Send("PUT", "/api/agent-changes/switch", "application/json", "{\"enabled\":false}");
            Assert.Equal(403, off.Status);
            Assert.Equal(DarlingWebEndpoints.AgentChangeRefusedMessage, (string)off.Body!["error"]!);

            Assert.Equal("agent says", await ScalarAsync(owner, "SELECT reason FROM config.config_mute_rules WHERE id = 'm-web'", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_service WHERE NOT agent_actions_enabled", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event IN ('action_undone', 'agent_actions_switched')", ct));
        });
    }

    [Fact]
    public async Task AsTheManagedAdminRole_AnUndoRestoresTheMute_TheSwitchTurnsOff_AndASecondClickOrABadIdOrBodyIsRefused()
    {
        await WithWebRoutesAsync("admin", async (owner, Send, ct) =>
        {
            var list = await Send("GET", "/api/agent-changes", null);
            Assert.True((bool)list.Body!["can_decide"]!);

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

    private delegate Task<(int Status, JsonNode? Body)> Sender(string method, string path, string? contentType, string? payload = null);

    private static async Task<string?> CalledIdAsync(NpgsqlDataSource owner, string target, CancellationToken ct) =>
        await ScalarAsync(owner,
            $"SELECT j.journal_id FROM config.investigation_journal j WHERE j.event = 'action_called' AND j.detail->'target'->>'id' = '{target}' AND EXISTS (SELECT 1 FROM config.investigation_journal d WHERE d.event = 'action_done' AND d.detail->>'called_id' = j.journal_id::text) ORDER BY j.journal_id LIMIT 1", ct);

    private static async Task WithWebRoutesAsync(
        string managedRole, Func<NpgsqlDataSource, Sender, CancellationToken, Task> body)
    {
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
            DarlingWebEndpoints.MapAll(app, webLogin, new CollectorRuntimeState(), new CapturingTestLogger());
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
                await using var drop = new NpgsqlCommand($"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName}", connection);
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
