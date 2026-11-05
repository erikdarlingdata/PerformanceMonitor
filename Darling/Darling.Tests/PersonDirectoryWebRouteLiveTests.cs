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
/// #7107: the Admin People tab's web routes. <c>GET /api/people</c> lists people with the session's own sign-in key, and
/// <c>PUT /api/people/{command}</c> runs one people write on the store owner connection, after the switch route's checks in the switch route's
/// order: the signed-in admin check, then the store login's admin check, then the owner connection. The shared web token is an admin session.
/// </summary>
public sealed class PersonDirectoryWebRouteLiveTests
{
    private const string RolePassword = "people-web-live-pw";
    private const string AliceKey = "AliceKeyAliceKeyAliceKeyAliceKeyAliceKey123";
    private const string ViewerKey = "ViewerKeyViewerKeyViewerKeyViewerKeyViewer1";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private delegate Task<(int Status, JsonNode? Body)> Sender(string method, string path, string? contentType, string? payload = null);

    [Fact]
    public async Task ASignedInAdmin_SeesItsOwnKey_AddsAPerson_AndLinksItsOwnSignIn_JournaledAsThePerson()
    {
        await WithWebAsync("admin", async (owner, Send, ct) =>
        {
            var list = await Send("GET", "/api/people", null);
            Assert.Equal(200, list.Status);
            Assert.True((bool)list.Body!["can_decide"]!);
            Assert.True((bool)list.Body["signed_in_admin"]!);
            Assert.Equal(AliceKey, (string?)list.Body["sign_in_key"]);
            Assert.Equal(0, (int)list.Body["people_returned"]!);

            var added = await Send("PUT", "/api/people/add_person", "application/json", """{"display_name":"Ann"}""");
            Assert.Equal(200, added.Status);
            Assert.Equal("added", (string?)added.Body!["status"]);
            var personId = (long)added.Body["person_id"]!;

            var linked = await Send("PUT", "/api/people/add_person_identity", "application/json",
                "{\"person_id\":" + personId + ",\"kind\":\"web\",\"external_id\":\"" + AliceKey + "\"}");
            Assert.Equal(200, linked.Status);
            var granted = await Send("PUT", "/api/people/add_person_grant", "application/json",
                "{\"person_id\":" + personId + ",\"capability\":\"approve\",\"kinds\":[\"kill_session\"]}");
            Assert.Equal(200, granted.Status);

            var after = (await Send("GET", "/api/people", null)).Body!;
            var person = after["people"]![0]!;
            Assert.Equal("Ann", (string?)person["display_name"]);
            Assert.Equal(AliceKey, (string?)person["identities"]![0]!["external_id"]);
            Assert.Equal("fleet", (string?)person["grants"]![0]!["scope"]);

            Assert.Equal("3", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE actor = 'person:alice'", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE actor <> 'person:alice'", ct));
        }, seat: new DarlingWebSeat("alice", true, AliceKey));
    }

    [Fact]
    public async Task TheSharedWebToken_IsAnAdminSession_ThatChangesPeople_AndHasNoKey()
    {
        await WithWebAsync("admin", async (owner, Send, ct) =>
        {
            var list = await Send("GET", "/api/people", null);
            Assert.True((bool)list.Body!["can_decide"]!);
            Assert.True((bool)list.Body["signed_in_admin"]!);
            Assert.Null((string?)list.Body["sign_in_key"]);

            Assert.Equal(200, (await Send("PUT", "/api/people/add_person", "application/json", """{"display_name":"Ann"}""")).Status);
            Assert.Equal("person:web", await ScalarAsync(owner, "SELECT actor FROM config.investigation_journal WHERE event = 'person_added'", ct));
        }, seat: DarlingWebSeat.SharedToken);
    }

    [Fact]
    public async Task AnUnsignedRequest_IsRefusedBeforeTheStore_EvenWithNoOwnerConnection_AndSeesNoKey()
    {
        /* The web's store login is the admin role and no owner connection is published: a 403 rather than a 503 shows the sign-in check runs first. */
        await WithWebAsync("admin", async (owner, Send, ct) =>
        {
            var refused = await Send("PUT", "/api/people/add_person", "application/json", """{"display_name":"Ann"}""");
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.InvestigationWriteSignInMessage, (string?)refused.Body!["error"]);

            var list = await Send("GET", "/api/people", null);
            Assert.Equal(200, list.Status);
            Assert.False((bool)list.Body!["can_decide"]!);
            Assert.False((bool)list.Body["signed_in_admin"]!);
            Assert.Null((string?)list.Body["sign_in_key"]);
            Assert.False(list.Body.AsObject().ContainsKey("people"));
            await AssertNothingWrittenAsync(owner, ct);
        }, signedIn: false, publishOwner: false);
    }

    [Fact]
    public async Task AViewerSeat_SeesOnlyItsOwnKey_AndEveryWriteIsRefusedBeforeTheStore()
    {
        await WithWebAsync("admin", async (owner, Send, ct) =>
        {
            /* A viewer session gets no list at all, only its own key (#7107). The old answer carried an empty list here. */
            var list = await Send("GET", "/api/people", null);
            Assert.False((bool)list.Body!["can_decide"]!);
            Assert.Equal(ViewerKey, (string?)list.Body["sign_in_key"]);
            Assert.False(list.Body.AsObject().ContainsKey("people"));
            Assert.False(list.Body.AsObject().ContainsKey("people_returned"));

            foreach (var command in new[] { "add_person", "rename_person", "set_person_enabled", "add_person_identity", "remove_person_identity", "add_person_grant", "remove_person_grant" })
            {
                var refused = await Send("PUT", "/api/people/" + command, "application/json", """{"display_name":"Ann","person_id":1}""");
                Assert.Equal(403, refused.Status);
                Assert.Equal(DarlingWebEndpoints.InvestigationWriteSignInMessage, (string?)refused.Body!["error"]);
            }

            await AssertNothingWrittenAsync(owner, ct);
        }, seat: new DarlingWebSeat("viewer-person", false, ViewerKey), publishOwner: false);
    }

    [Fact]
    public async Task UnderTheViewerStoreRole_ASignedInAdmin_IsRefused403ByTheAdminCheck_BeforeTheOwnerConnection()
    {
        await WithWebAsync("web", async (owner, Send, ct) =>
        {
            Assert.False((bool)(await Send("GET", "/api/people", null)).Body!["can_decide"]!);
            var refused = await Send("PUT", "/api/people/add_person", "application/json", """{"display_name":"Ann"}""");
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.PeopleRefusedMessage(false), (string?)refused.Body!["error"]);
            await AssertNothingWrittenAsync(owner, ct);
        }, seat: new DarlingWebSeat("alice", true, AliceKey), publishOwner: false);
    }

    [Fact]
    public async Task ASignedInAdmin_Gets415WithoutJson_400ForANonObject_And503BeforeTheOwnerConnection()
    {
        await WithWebAsync("admin", async (owner, Send, ct) =>
        {
            Assert.Equal(415, (await Send("PUT", "/api/people/add_person", "text/plain", """{"display_name":"Ann"}""")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/people/add_person", "application/json", "[1]")).Status);
            Assert.Equal(503, (await Send("PUT", "/api/people/add_person", "application/json", """{"display_name":"Ann"}""")).Status);
            await AssertNothingWrittenAsync(owner, ct);
        }, seat: new DarlingWebSeat("alice", true, AliceKey), publishOwner: false);
    }

    [Fact]
    public async Task ARefusedWrite_AnswersItsStatus_AndChangesNothing()
    {
        await WithWebAsync("admin", async (owner, Send, ct) =>
        {
            var unknownCommand = await Send("PUT", "/api/people/delete_person", "application/json", """{"person_id":1}""");
            Assert.Equal(400, unknownCommand.Status);
            Assert.Equal("invalid_args", (string?)unknownCommand.Body!["code"]);

            var unknownPerson = await Send("PUT", "/api/people/rename_person", "application/json", """{"person_id":99,"display_name":"Bo"}""");
            Assert.Equal(404, unknownPerson.Status);
            Assert.Equal("unknown_person", (string?)unknownPerson.Body!["code"]);

            var personId = (long)(await Send("PUT", "/api/people/add_person", "application/json", """{"display_name":"Ann"}""")).Body!["person_id"]!;
            var identity = "{\"person_id\":" + personId + ",\"kind\":\"slack\",\"external_id\":\"T1:U1\"}";
            Assert.Equal(200, (await Send("PUT", "/api/people/add_person_identity", "application/json", identity)).Status);
            var otherId = (long)(await Send("PUT", "/api/people/add_person", "application/json", """{"display_name":"Bo"}""")).Body!["person_id"]!;
            var taken = await Send("PUT", "/api/people/add_person_identity", "application/json", identity.Replace("\"person_id\":" + personId, "\"person_id\":" + otherId, StringComparison.Ordinal));
            Assert.Equal(409, taken.Status);
            Assert.Equal("identity_taken", (string?)taken.Body!["code"]);

            Assert.Equal("3", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));
        }, seat: new DarlingWebSeat("alice", true, AliceKey));
    }

    private static async Task AssertNothingWrittenAsync(NpgsqlDataSource owner, CancellationToken ct)
    {
        Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.person", ct));
        Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));
    }

    private static async Task WithWebAsync(string managedRole, Func<NpgsqlDataSource, Sender, CancellationToken, Task> body, bool signedIn = true,
        DarlingWebSeat? seat = null, bool publishOwner = true)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the people web route live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "people_seat_" + Guid.NewGuid().ToString("N")[..8];
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
            await ExecAsync(owner, DarlingManagedRoles.BuildJournalEventRulesSql("config"), ct);
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
            if (signedIn)
            {
                /* What the sign-in middleware stores for a request that holds a valid session. A dashboard with no sign-in stores nothing. */
                app.Use((context, next) =>
                {
                    context.Items[DarlingWebSeat.HttpContextItemKey] = seat ?? DarlingWebSeat.SharedToken;
                    return next(context);
                });
            }

            var ownerSeam = new StoreOwnerSource();
            if (publishOwner)
            {
                ownerSeam.Publish(owner);
            }

            DarlingWebEndpoints.MapAll(app, webLogin, new CollectorRuntimeState(), new CapturingTestLogger(), ownerSource: ownerSeam);
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
