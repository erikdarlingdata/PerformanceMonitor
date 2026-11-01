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
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The web proposal routes of #7109 on a throwaway PostgreSQL target: the agent's kill call stores a proposal, and the web's two press routes
/// run it. The approver comes from the request's seat, never from the body. Also the reads with each <c>mcp.dataDetail</c> value, the refusals
/// a write route answers before the store is touched, and the route check behind the route page's nobody-can-approve sentence.
/// </summary>
/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres and its own roles on the cluster. */
public sealed class ProposalWebRouteLiveTests(PostgresKillLiveTests.ScramTlsCluster target) : IClassFixture<PostgresKillLiveTests.ScramTlsCluster>
{
    private static string? Cluster => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string VictimPassword = "Victim-1x";

    private sealed class SeatBox
    {
        public DarlingWebSeat Seat { get; set; } = DarlingWebSeat.SharedToken;
    }

    private sealed record Ctx(NpgsqlDataSource Source, NpgsqlConnection Store, NpgsqlConnection Admin, string Victim, string Arg, Guid RequestId,
        int RouteId, SeatBox Seats, CancellationToken Ct)
    {
        /// <summary>Opens a victim backend and has the agent propose its kill. Answers the backend and the proposal id.</summary>
        public async Task<(NpgsqlConnection Connection, int Pid, Guid ProposalId)> ProposeKillAsync(PostgresKillLiveTests.ScramTlsCluster target)
        {
            var builder = new NpgsqlConnectionStringBuilder(target.ConnectionString(Victim, VictimPassword)) { ApplicationName = "pm7109-web-victim" };
            var connection = new NpgsqlConnection(builder.ConnectionString);
            await connection.OpenAsync(Ct);
            await using var pidCommand = new NpgsqlCommand("SELECT pg_backend_pid()", connection);
            var pid = Convert.ToInt32(await pidCommand.ExecuteScalarAsync(Ct), CultureInfo.InvariantCulture);
            await using var read = new NpgsqlCommand(PostgresKillLiveTests.CollectorBackendIdSql, Admin);
            read.Parameters.Add(new NpgsqlParameter { Value = pid, NpgsqlDbType = NpgsqlDbType.Integer });
            var backendId = (string)(await read.ExecuteScalarAsync(Ct))!;
            var answer = JsonNode.Parse(await DarlingMcpPlanForceTools.RunKillAsync(Source, Source, null, "pg-7", pid, null, backendId, Arg, false, Ct))!;
            Assert.Equal("proposed", (string?)answer["status"]);
            return (connection, pid, Guid.Parse((string)answer["proposal_id"]!));
        }

        /// <summary>A person with one web identity (the seat's owner key) and, unless <paramref name="grant"/> is false, <c>approve</c> for
        /// <c>kill_session</c> on server 7. Answers the person id and a signed-in seat whose key is that identity: an admin seat, or a
        /// viewer-role seat when <paramref name="canEdit"/> is false.</summary>
        public async Task<(long PersonId, DarlingWebSeat Seat)> PersonAsync(string name, bool grant = true, bool enabled = true, bool canEdit = true)
        {
            var personId = (long)(await ScalarAsync(
                $"INSERT INTO config.person (display_name, enabled, created_by, updated_by) VALUES ('{name}', {(enabled ? "TRUE" : "FALSE")}, 'test', 'test') RETURNING person_id"))!;
            var key = NewKey();
            await ScalarAsync($"INSERT INTO config.person_identity (person_id, kind, external_id, created_by) VALUES ({personId}, 'web', '{key}', 'test') RETURNING identity_id");
            if (grant)
                await ScalarAsync($"INSERT INTO config.person_grant (person_id, scope_server_id, capability, kinds, created_by) VALUES ({personId}, 7, 'approve', ARRAY['kill_session'], 'test') RETURNING grant_id");
            return (personId, new DarlingWebSeat(name.ToLowerInvariant().Replace(' ', '.'), canEdit, key));
        }

        public async Task<object?> ScalarAsync(string sql)
        {
            await using var command = new NpgsqlCommand(sql, Store);
            return await command.ExecuteScalarAsync(Ct);
        }

        public async Task<string> StateAsync(Guid proposalId) => (string)(await ScalarAsync($"SELECT state FROM config.agent_action_proposals WHERE proposal_id = '{proposalId}'"))!;

        public async Task<string?> FirstPressByAsync(Guid proposalId) =>
            await ScalarAsync($"SELECT first_press_by FROM config.agent_action_proposals WHERE proposal_id = '{proposalId}'") as string;

        /// <summary>Up to <paramref name="polls"/> x 100 ms for the backend to leave pg_stat_activity: false the moment it does.</summary>
        public async Task<bool> AliveAsync(int pid, int polls = 100)
        {
            for (var i = 0; i < polls; i++)
            {
                await using var command = new NpgsqlCommand($"SELECT count(*) FROM pg_stat_activity WHERE pid = {pid}", Admin);
                if (await command.ExecuteScalarAsync(Ct) is 0L)
                    return false;
                await Task.Delay(100, Ct);
            }

            return true;
        }

        /// <summary>Starts the web routes over this store at <paramref name="dataDetail"/>, with the seat in <see cref="Seats"/> on every request,
        /// behind the host's real write gate (<see cref="DarlingWebSeat.IsRequestAllowed"/>), which answers 403 as the host does.</summary>
        public async Task<Web> WebAsync(McpDataDetail dataDetail = McpDataDetail.Full)
        {
            var builder = WebApplication.CreateBuilder(new WebApplicationOptions
            {
                ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
                WebRootPath = "wwwroot",
            });
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            var app = builder.Build();
            var seats = Seats;
            app.Use((context, next) =>
            {
                context.Items[DarlingWebSeat.HttpContextItemKey] = seats.Seat;
                if (!DarlingWebSeat.IsRequestAllowed(seats.Seat, context.Request.Method, context.Request.Path.Value ?? "/"))
                {
                    context.Response.StatusCode = 403;
                    return context.Response.WriteAsync("{\"error\":\"" + GateRefusal + "\"}");
                }

                return next(context);
            });
            var owner = new StoreOwnerSource();
            owner.Publish(Source);
            DarlingWebEndpoints.MapAll(app, Source, new CollectorRuntimeState(), new CapturingTestLogger(), ownerSource: owner, dataDetail: dataDetail);
            await app.StartAsync(Ct);
            return new Web(app, Ct);
        }
    }

    private sealed class Web(WebApplication app, CancellationToken ct) : IAsyncDisposable
    {
        public async Task<(int Status, JsonNode? Body)> SendAsync(string method, string path, string? contentType = "application/json", string? payload = null)
        {
            var context = await app.GetTestServer().SendAsync(request =>
            {
                request.Request.Method = method;
                request.Request.Path = path;
                request.Request.Headers.Host = "localhost";
                if (contentType is not null)
                    request.Request.ContentType = contentType;
                request.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(payload ?? (method == "POST" ? "{}" : "")));
            });
            using var reader = new StreamReader(context.Response.Body);
            var text = await reader.ReadToEndAsync(ct);
            return (context.Response.StatusCode, string.IsNullOrWhiteSpace(text) ? null : JsonNode.Parse(text));
        }

        public Task<(int Status, JsonNode? Body)> PressAsync(Guid proposalId, string verb, string payload = "{}") =>
            SendAsync("POST", $"/api/proposals/{proposalId}/{verb}", payload: payload);

        public async ValueTask DisposeAsync() => await app.DisposeAsync();
    }

    /// <summary>What this test's copy of the host gate answers, so a test can tell a gate refusal from an endpoint refusal.</summary>
    private const string GateRefusal = "refused by the gate";

    private static string NewKey() => (Guid.NewGuid().ToString("N") + "ABCDEFGHIJK")[..43];

    private async Task RunAsync(bool singlePerson, Func<Ctx, Task> body)
    {
        Assert.SkipWhen(target.SkipReason is not null, target.SkipReason ?? "");
        var ct = TestContext.Current.CancellationToken;
        var tag = Guid.NewGuid().ToString("N")[..12];
        var victim = "pmw_vic_" + tag;
        var remediation = "pmw_rem_" + tag;
        var secret = "Aa1" + Guid.NewGuid().ToString("N");
        var envName = "PM_PGW_" + tag;
        await using var scratch = await ScratchPostgres.CreateAsync(Cluster!, ct, emptyStore: true);
        var bodySucceeded = false;
        await using var admin = new NpgsqlConnection(target.SuperuserConnectionString);
        await admin.OpenAsync(ct);
        try
        {
            await ExecAsync(admin, $"CREATE ROLE \"{victim}\" LOGIN PASSWORD '{VictimPassword}'", ct);
            await ExecAsync(admin, $"CREATE ROLE \"{remediation}\" LOGIN PASSWORD '{secret}'", ct);
            await ExecAsync(admin, $"GRANT pg_signal_backend TO \"{remediation}\"", ct);
            await ExecAsync(admin, $"GRANT pg_read_all_stats TO \"{remediation}\"", ct);
            Environment.SetEnvironmentVariable(envName, secret);

            await using var store = new NpgsqlConnection(scratch.ConnectionString);
            await store.OpenAsync(ct);
            await PgMigrations.MigrateAsync(store, ct);
            await ExecAsync(store,
                "INSERT INTO config.config_monitored_servers (server_id, name, host, port, encrypt_mode, trust_server_certificate, remediation_username, remediation_encrypted_password) "
                + $"VALUES (7, 'pg-7', '127.0.0.1', {target.Port}, 'Optional', TRUE,'{remediation}', 'env:{envName}')", ct);
            await ExecAsync(store, "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, engine_kind, created_date, modified_date) "
                + "VALUES (7, 'pg-7', 'pg-7', TRUE, NULL, 'postgres', now(), now())", ct);
            await ExecAsync(store, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            await using var routeCommand = new NpgsqlCommand("INSERT INTO config.config_notification_routes (metric_match, agent_level, action_approval, approval_single_person) "
                + $"VALUES ('Blocking', 3, 'required', {(singlePerson ? "TRUE" : "FALSE")}) RETURNING route_id", store);
            var routeId = Convert.ToInt32(await routeCommand.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
            await ExecAsync(store, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (7, 'kill_session', true, 'test')", ct);

            var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
            await using var source = NpgsqlDataSource.Create(builder.ConnectionString);
            var requests = new InvestigationRequestStore(source, 30);
            var alert = new InvestigationAlert(1, 7, "pg-7", "blocking", "Blocking", DateTime.UtcNow, Array.Empty<string>(), null, 20, 100,
                LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = 3 };
            var opened = await requests.OpenOrJoinAsync(alert, ct);
            var claim = await requests.ClaimAsync(opened.RequestId!.Value, "agent-a", ct);
            var arg = AgentActionArgumentTests.Arg(opened.RequestId.Value.ToString(), claim.ClaimId!.Value.ToString(), reason: "the blocker holds a transaction open");
            await body(new Ctx(source, store, admin, victim, arg, opened.RequestId.Value, routeId, new SeatBox(), ct));
            bodySucceeded = true;
        }
        finally
        {
            Environment.SetEnvironmentVariable(envName, null);
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                await using var cleanup = new NpgsqlConnection(target.SuperuserConnectionString);
                await cleanup.OpenAsync(CancellationToken.None);
                try
                {
                    await ExecAsync(cleanup, $"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename IN ('{victim}', '{remediation}')", CancellationToken.None);
                    await ExecAsync(cleanup, $"DROP ROLE IF EXISTS \"{victim}\"", CancellationToken.None);
                    await ExecAsync(cleanup, $"DROP ROLE IF EXISTS \"{remediation}\"", CancellationToken.None);
                }
                catch (PostgresException)
                {
                    /* A role that cannot be dropped is named for this test alone; the assertions already reported what went wrong. */
                }
            });

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>End to end over the web: a mapped person's first press and confirm run the kill, and the proposal ends <c>run</c>. Each press body
    /// names another person and an administrator, and the press is recorded for the seat's person all the same.</summary>
    [Fact]
    public async Task AMappedPerson_PressesTwiceOverTheWeb_TheKillRuns_AndAPersonNamedInTheBodyIsIgnored()
    {
        await RunAsync(singlePerson: false, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync(target);
            await using var victim = connection;
            var (personId, seat) = await c.PersonAsync("Approver A");
            var (otherId, _) = await c.PersonAsync("Approver B");
            await using var web = await c.WebAsync();
            var forged = $"{{\"person_id\":{otherId},\"identity_id\":1,\"approver\":\"admin:web\",\"by\":\"person:{otherId}\"}}";

            c.Seats.Seat = seat;
            var list = await web.SendAsync("GET", "/api/proposals");
            Assert.Equal(200, list.Status);
            Assert.True((bool)list.Body!["can_press"]!);
            Assert.Equal("person", (string?)list.Body["presses_as"]);
            Assert.Equal("Approver A", (string?)list.Body["person_name"]);
            var row = Assert.Single(list.Body["proposals"]!.AsArray());
            Assert.Equal("kill_session", (string?)row!["kind"]);
            Assert.Equal("pg-7", (string?)row["server_name"]);
            Assert.Equal("pending", (string?)row["state"]);
            Assert.Equal("the blocker holds a transaction open", (string?)row["agent_reason"]);
            Assert.Equal(c.RequestId.ToString(), (string?)row["request_id"]);

            var first = await web.PressAsync(proposalId, "first-press", forged);
            Assert.True(first.Status == 200, first.Body?.ToJsonString());
            Assert.Equal("first_pressed", (string?)first.Body!["status"]);
            Assert.Equal(pid, (int)first.Body["facts"]!["pid"]!);
            Assert.Equal("person:" + personId, await c.FirstPressByAsync(proposalId));
            Assert.True(await c.AliveAsync(pid, polls: 5));

            var confirmed = await web.PressAsync(proposalId, "confirm", forged);
            Assert.True(confirmed.Status == 200, confirmed.Body?.ToJsonString());
            Assert.Equal("run", (string?)confirmed.Body!["status"]);
            Assert.False(await c.AliveAsync(pid));
            Assert.Equal("run", await c.StateAsync(proposalId));
            Assert.Equal(personId.ToString(CultureInfo.InvariantCulture), (string?)await c.ScalarAsync(
                "SELECT detail->'approved_by'->>'person_id' FROM config.investigation_journal WHERE event = 'action_called'"));
        });
    }

    /// <summary>On a two-person route, every seat with no mapped person presses as an administrator and is refused: the shared web token, a
    /// signed-in admin whose key maps to nobody, and one whose person is disabled. Nothing is recorded and the backend stays.</summary>
    [Fact]
    public async Task OnATwoPersonRoute_TheSharedToken_AnUnmappedSignIn_AndADisabledPerson_AreRefused()
    {
        await RunAsync(singlePerson: false, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync(target);
            await using var victim = connection;
            var (_, disabled) = await c.PersonAsync("Disabled D", enabled: false);
            await using var web = await c.WebAsync();

            foreach (var seat in new[] { DarlingWebSeat.SharedToken, new DarlingWebSeat("nobody", CanEdit: true, NewKey()), disabled })
            {
                c.Seats.Seat = seat;
                var read = await web.SendAsync("GET", $"/api/proposals/{proposalId}");
                Assert.Equal("administrator", (string?)read.Body!["presses_as"]);
                Assert.Contains("needs two people", (string?)read.Body["press_note"]);

                var first = await web.PressAsync(proposalId, "first-press");
                Assert.Equal(409, first.Status);
                Assert.Equal("refused", (string?)first.Body!["status"]);
                Assert.Equal(InvestigationRequestStore.ProposalPressCodes.AdministratorNeedsSinglePerson, (string?)first.Body["code"]);
            }

            Assert.Null(await c.FirstPressByAsync(proposalId));
            Assert.Equal("pending", await c.StateAsync(proposalId));
            Assert.True(await c.AliveAsync(pid, polls: 5));
        });
    }

    /// <summary>On a single-person route, the shared web token presses as an administrator, and the kill runs.</summary>
    [Fact]
    public async Task OnASinglePersonRoute_TheSharedToken_PressesAsAnAdministrator_AndTheKillRuns()
    {
        await RunAsync(singlePerson: true, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync(target);
            await using var victim = connection;
            await using var web = await c.WebAsync();
            c.Seats.Seat = DarlingWebSeat.SharedToken;

            var first = await web.PressAsync(proposalId, "first-press");
            Assert.True(first.Status == 200, first.Body?.ToJsonString());
            Assert.Equal("admin:web", await c.FirstPressByAsync(proposalId));
            var confirmed = await web.PressAsync(proposalId, "confirm");
            Assert.True(confirmed.Status == 200, confirmed.Body?.ToJsonString());
            Assert.Equal("run", (string?)confirmed.Body!["status"]);
            Assert.False(await c.AliveAsync(pid));
        });
    }

    /// <summary>A confirm from a different seat than the first press is refused: another mapped person, and the shared web token.</summary>
    [Fact]
    public async Task AConfirmFromADifferentSeatThanTheFirstPress_IsRefused_AndNothingRuns()
    {
        await RunAsync(singlePerson: false, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync(target);
            await using var victim = connection;
            var (_, first) = await c.PersonAsync("Approver A");
            var (_, second) = await c.PersonAsync("Approver B");
            await using var web = await c.WebAsync();

            c.Seats.Seat = first;
            Assert.Equal(200, (await web.PressAsync(proposalId, "first-press")).Status);

            c.Seats.Seat = second;
            var other = await web.PressAsync(proposalId, "confirm");
            Assert.Equal(409, other.Status);
            Assert.Equal(InvestigationRequestStore.ProposalPressCodes.OtherApprover, (string?)other.Body!["code"]);

            c.Seats.Seat = DarlingWebSeat.SharedToken;
            var token = await web.PressAsync(proposalId, "confirm");
            Assert.Equal(409, token.Status);
            Assert.Equal("refused", (string?)token.Body!["status"]);

            Assert.Equal("pending", await c.StateAsync(proposalId));
            Assert.True(await c.AliveAsync(pid, polls: 5));
        });
    }

    /// <summary>The refusals a write route answers before the store is touched: a press a cross-site page could send (a form post, plain text,
    /// no content type) is 415, a body that is not an object is 400, and a session that cannot edit is 403. That session still reads the
    /// proposal, read-only, with the sentence that says how to press.</summary>
    [Fact]
    public async Task ACrossSitePost_AndASessionThatCannotEdit_AreRefused_BeforeThePressRuns()
    {
        await RunAsync(singlePerson: true, async c =>
        {
            var (connection, _, proposalId) = await c.ProposeKillAsync(target);
            await using var victim = connection;
            await using var web = await c.WebAsync();
            c.Seats.Seat = DarlingWebSeat.SharedToken;
            var path = $"/api/proposals/{proposalId}/first-press";

            Assert.Equal(415, (await web.SendAsync("POST", path, "application/x-www-form-urlencoded", "a=1")).Status);
            Assert.Equal(415, (await web.SendAsync("POST", path, "text/plain", "{}")).Status);
            Assert.Equal(415, (await web.SendAsync("POST", path, "multipart/form-data; boundary=x", "{}")).Status);
            Assert.Equal(415, (await web.SendAsync("POST", path, null, "{}")).Status);
            Assert.Equal(400, (await web.SendAsync("POST", path, "application/json", "[]")).Status);

            /* The reads-only seat names nobody, so the gate refuses it. A viewer-role sign-in passes the gate on a press route, and the
               endpoint refuses it because its key maps to no person. */
            foreach (var (seat, note, error) in new[]
            {
                (DarlingWebSeat.ReadsOnly, DarlingWebEndpoints.ProposalPressSignInNote, GateRefusal),
                (new DarlingWebSeat("viewer.v", CanEdit: false, NewKey()), DarlingWebEndpoints.ProposalPressViewerNotMappedNote,
                    DarlingWebEndpoints.ProposalPressViewerNotMappedNote),
            })
            {
                c.Seats.Seat = seat;
                var press = await web.PressAsync(proposalId, "first-press");
                Assert.Equal(403, press.Status);
                Assert.Equal(error, (string?)press.Body!["error"]);
                var read = await web.SendAsync("GET", $"/api/proposals/{proposalId}");
                Assert.Equal(200, read.Status);
                Assert.False((bool)read.Body!["can_press"]!);
                Assert.Null(read.Body["presses_as"]);
                Assert.Equal(note, (string?)read.Body["press_note"]);
            }

            Assert.Null(await c.FirstPressByAsync(proposalId));
            Assert.Equal(404, (await web.SendAsync("GET", $"/api/proposals/{Guid.NewGuid()}")).Status);
        });
    }

    /// <summary>A viewer-role sign-in mapped to a person who holds <c>approve</c> presses twice over the web, through the host's gate, and the kill
    /// runs: a grant gives the power to the person whatever the role of the sign-in. Both presses are recorded for that person.</summary>
    [Fact]
    public async Task AViewerRoleSignIn_MappedToAPersonWithApprove_PressesTwice_AndTheKillRuns()
    {
        await RunAsync(singlePerson: false, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync(target);
            await using var victim = connection;
            var (personId, seat) = await c.PersonAsync("Viewer V", canEdit: false);
            Assert.False(seat.CanEdit);
            await using var web = await c.WebAsync();
            c.Seats.Seat = seat;

            var read = await web.SendAsync("GET", $"/api/proposals/{proposalId}");
            Assert.True((bool)read.Body!["can_press"]!);
            Assert.Equal("person", (string?)read.Body["presses_as"]);
            Assert.Equal("Viewer V", (string?)read.Body["person_name"]);

            var first = await web.PressAsync(proposalId, "first-press");
            Assert.True(first.Status == 200, first.Body?.ToJsonString());
            Assert.Equal("first_pressed", (string?)first.Body!["status"]);
            Assert.Equal("person:" + personId, await c.FirstPressByAsync(proposalId));

            var confirmed = await web.PressAsync(proposalId, "confirm");
            Assert.True(confirmed.Status == 200, confirmed.Body?.ToJsonString());
            Assert.Equal("run", (string?)confirmed.Body!["status"]);
            Assert.False(await c.AliveAsync(pid));
            Assert.Equal("run", await c.StateAsync(proposalId));
            Assert.Equal(personId.ToString(CultureInfo.InvariantCulture), (string?)await c.ScalarAsync(
                "SELECT detail->'approved_by'->>'person_id' FROM config.investigation_journal WHERE event = 'action_called'"));

            /* The owner key opens the presses only: the same seat is still refused every other write. */
            Assert.Equal(GateRefusal, (string?)(await web.SendAsync("POST", "/api/people", payload: "{}")).Body!["error"]);
        });
    }

    /// <summary>A viewer-role sign-in mapped to a person with no <c>approve</c> grant passes the gate and the endpoint, and the press API refuses
    /// it on the grant. Nothing is recorded and the backend stays.</summary>
    [Fact]
    public async Task AViewerRoleSignIn_MappedToAPersonWithoutTheGrant_IsRefusedByThePressApi()
    {
        await RunAsync(singlePerson: false, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync(target);
            await using var victim = connection;
            var (_, seat) = await c.PersonAsync("Viewer N", grant: false, canEdit: false);
            await using var web = await c.WebAsync();
            c.Seats.Seat = seat;

            var first = await web.PressAsync(proposalId, "first-press");
            Assert.Equal(409, first.Status);
            Assert.Equal("refused", (string?)first.Body!["status"]);
            Assert.Equal(InvestigationRequestStore.ProposalPressCodes.NotApprover, (string?)first.Body["code"]);

            Assert.Null(await c.FirstPressByAsync(proposalId));
            Assert.Equal("pending", await c.StateAsync(proposalId));
            Assert.True(await c.AliveAsync(pid, polls: 5));
        });
    }

    /// <summary>A viewer-role sign-in that maps to no enabled person never presses as an administrator, even on a single-person route where an
    /// administrator press would run: an unmapped key and a disabled person are refused by the endpoint, before the press API, with the
    /// sentence that says how to be mapped. Nothing is recorded and the backend stays.</summary>
    [Fact]
    public async Task AViewerRoleSignIn_MappedToNobody_IsRefusedBeforeThePressApi_EvenOnASinglePersonRoute()
    {
        await RunAsync(singlePerson: true, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync(target);
            await using var victim = connection;
            var (_, disabled) = await c.PersonAsync("Viewer D", enabled: false, canEdit: false);
            await using var web = await c.WebAsync();

            foreach (var seat in new[] { new DarlingWebSeat("viewer.u", CanEdit: false, NewKey()), disabled })
            {
                c.Seats.Seat = seat;
                foreach (var verb in new[] { "first-press", "confirm" })
                {
                    var press = await web.PressAsync(proposalId, verb);
                    Assert.Equal(403, press.Status);
                    Assert.Equal(DarlingWebEndpoints.ProposalPressViewerNotMappedNote, (string?)press.Body!["error"]);
                }
            }

            Assert.Null(await c.FirstPressByAsync(proposalId));
            Assert.Equal("pending", await c.StateAsync(proposalId));
            Assert.True(await c.AliveAsync(pid, polls: 5));
        });
    }

    /// <summary>The statement in a proposal follows <c>mcp.dataDetail</c>: whole at <c>full</c>, its literals replaced at <c>maskLiterals</c>, and
    /// withheld at <c>metadataOnly</c>, in the list and in the one-proposal read.</summary>
    [Fact]
    public async Task TheStatement_FollowsEachDataDetailValue()
    {
        await RunAsync(singlePerson: false, async c =>
        {
            const string statement = "SELECT * FROM dbo.orders WHERE customer_id = 48151623";
            var created = await new InvestigationRequestStore(c.Source, 30).CreateProposalAsync(new ActionProposalDraft(c.RequestId, 1, "agent-a",
                "force_query_store_plan", 7, new JsonObject { ["query_id"] = 5, ["plan_id"] = 9 },
                new JsonObject { ["statement"] = statement, ["forced_plan_id"] = null, ["is_forced"] = false }, "plan 9 is the fast one"), c.Ct);

            foreach (var (detail, check) in new (McpDataDetail, Action<string?>)[]
                     {
                         (McpDataDetail.Full, s => Assert.Equal(statement, s)),
                         (McpDataDetail.MaskLiterals, s =>
                         {
                             Assert.DoesNotContain("48151623", s);
                             Assert.Contains("dbo.orders", s);
                         }),
                         (McpDataDetail.MetadataOnly, s =>
                         {
                             Assert.DoesNotContain("48151623", s ?? "");
                             Assert.DoesNotContain("dbo.orders", s ?? "");
                         }),
                     })
            {
                await using var web = await c.WebAsync(detail);
                var one = await web.SendAsync("GET", $"/api/proposals/{created.ProposalId}");
                Assert.Equal(200, one.Status);
                Assert.Equal("force_plan", (string?)one.Body!["proposal"]!["kind"]);
                check((string?)one.Body["proposal"]!["binding"]!["statement"]);
                var list = await web.SendAsync("GET", "/api/proposals");
                check((string?)list.Body!["proposals"]!.AsArray().Single()!["binding"]!["statement"]);
            }
        });
    }

    /// <summary>The route check behind the route page's sentence: a Level 3 route that needs two people is named while nobody can approve on
    /// it, and not once a mapped, enabled person holds <c>approve</c>. A person with no sign-in does not count.</summary>
    [Fact]
    public async Task TheRouteCheck_NamesARouteNobodyCanApproveOn_UntilAMappedPersonHoldsApprove()
    {
        await RunAsync(singlePerson: false, async c =>
        {
            var off = Convert.ToInt32(await c.ScalarAsync("INSERT INTO config.config_notification_routes (metric_match, agent_level, action_approval) VALUES ('Deadlocks', 3, 'off') RETURNING route_id"), CultureInfo.InvariantCulture);
            var single = Convert.ToInt32(await c.ScalarAsync("INSERT INTO config.config_notification_routes (metric_match, agent_level, approval_single_person) VALUES ('High CPU', 3, TRUE) RETURNING route_id"), CultureInfo.InvariantCulture);
            await using var web = await c.WebAsync();

            async Task<int[]> NamedAsync()
            {
                var answer = await web.SendAsync("GET", DarlingWebEndpoints.NobodyCanApproveRoutesRoute);
                Assert.Equal(200, answer.Status);
                Assert.Equal("Nobody can approve this. Map a person, or turn on single-person approval.", (string?)answer.Body!["message"]);
                return answer.Body["route_ids"]!.AsArray().Select(n => (int)n!).ToArray();
            }

            var before = await NamedAsync();
            Assert.Contains(c.RouteId, before);
            Assert.DoesNotContain(off, before);
            Assert.DoesNotContain(single, before);

            var noSignIn = (long)(await c.ScalarAsync("INSERT INTO config.person (display_name, created_by, updated_by) VALUES ('No Sign In', 'test', 'test') RETURNING person_id"))!;
            await c.ScalarAsync($"INSERT INTO config.person_grant (person_id, capability, kinds, created_by) VALUES ({noSignIn}, 'approve', ARRAY['kill_session'], 'test') RETURNING grant_id");
            Assert.Contains(c.RouteId, await NamedAsync());

            await c.PersonAsync("Approver A");
            Assert.DoesNotContain(c.RouteId, await NamedAsync());
        });
    }
}
