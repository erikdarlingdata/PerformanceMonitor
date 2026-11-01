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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The pending-notes decisions (#5903) on a live store. An agent's proposed note is absent from the brief composer's
/// approved-note read until a person approves it; rejecting it keeps it out and keeps the row, with who and when;
/// a note cannot be decided twice. The web-route facts run the routes as the managed <c>admin</c> role (it decides, as on dev) and as the
/// managed <c>viewer</c> role (it lists, and the decide routes answer 403 and change nothing), the grants replayed from the shipped provisioning SQL.
/// </summary>
public sealed class InvestigationNoteDecisionLiveTests
{
    private const int ServerId = 7_590_301;
    private const string ServerName = "LcNoteDecisionSrv";
    private const string RolePassword = "note-decision-live-pw";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static InvestigationAlert Alert(DateTime at, string metric = "Blocking") =>
        new(1, ServerId, ServerName, "blocking", metric, at, new[] { ServerId + ":" + metric }, null,
            20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);

    /* A request an agent claims and reports, with its proposed note; the note lands pending. A later alert after a report
       opens a NEW request, so each call gives its own note. */
    private static async Task<Guid> ProposeAsync(InvestigationRequestStore store, DateTime at, string note, CancellationToken ct)
    {
        var requestId = (await store.OpenOrJoinAsync(Alert(at), ct)).RequestId!.Value;
        var claim = await store.ClaimAsync(requestId, "agent-a", ct);
        Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
            requestId, claim.ClaimId!.Value, "agent-a", 1, "real", "root cause", true, "summary",
            "[{\"tool\":\"get_blocking\"}]", ProposedNote: note), ct));
        return requestId;
    }

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

    [Fact]
    public async Task APendingNote_IsAbsentFromTheBriefRead_UntilApproved_AndARejectedNoteStaysStoredButOut()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation note decision live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = await MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var reader = new PgInvestigationBriefSourceReader(owner, utcNow: () => DateTime.UtcNow);

        var approvedRequest = await ProposeAsync(store, DateTime.UtcNow.AddHours(-3), "approve me: the nightly load blocks for a minute", ct);
        var rejectedRequest = await ProposeAsync(store, DateTime.UtcNow.AddHours(-2), "reject me: a wrong guess", ct);

        /* Both wait, oldest first, with the request's server, family and metric and the agent that proposed them. */
        var pending = await store.ListPendingNotesAsync(200, ct);
        Assert.Equal(2, pending.Count);
        Assert.Equal("approve me: the nightly load blocks for a minute", pending[0].NoteText);
        Assert.Equal(ServerName, pending[0].ServerName);
        Assert.Equal("blocking", pending[0].Family);
        Assert.Equal("Blocking", pending[0].MetricName);
        Assert.Equal("agent:agent-a", pending[0].Author);
        Assert.Equal(approvedRequest, pending[0].SourceRequestId);
        Assert.Equal(rejectedRequest, pending[1].SourceRequestId);

        /* The composer's approved read does not carry a pending note. */
        var next = (await store.OpenOrJoinAsync(Alert(DateTime.UtcNow), ct)).RequestId!.Value;
        Assert.Empty((await reader.ReadAsync(next, ct))!.Notes);

        /* Approve: the note joins the brief read, stamped with the approver, and leaves the pending list. */
        var before = DateTime.UtcNow.AddSeconds(-5);
        Assert.True(await store.ApproveNoteAsync(pending[0].NoteId, "person:tester", ct));
        var notes = (await reader.ReadAsync(next, ct))!.Notes;
        var carried = Assert.Single(notes);
        Assert.Equal("approve me: the nightly load blocks for a minute", carried.Text);
        Assert.Equal("person:tester", carried.DecidedBy);
        Assert.Equal("1", await ScalarAsync(owner, $@"
SELECT count(*) FROM config.investigation_notes
WHERE note_id = {pending[0].NoteId} AND state = 'approved' AND decided_by = 'person:tester'
  AND decided_at BETWEEN '{before:O}'::timestamptz AND now() + interval '5 seconds'", ct));
        Assert.Equal("1", await ScalarAsync(owner, $@"
SELECT count(*) FROM config.investigation_journal
WHERE event = 'note_approved' AND actor = 'person:tester' AND request_id = '{approvedRequest}'", ct));
        Assert.Equal(pending[1].NoteId, Assert.Single(await store.ListPendingNotesAsync(200, ct)).NoteId);

        /* Reject: the note stays out of the brief read and stays stored, with its verdict, who and when. */
        Assert.True(await store.RejectNoteAsync(pending[1].NoteId, "person:tester", ct));
        Assert.Single((await reader.ReadAsync(next, ct))!.Notes);
        Assert.DoesNotContain((await reader.ReadAsync(next, ct))!.Notes, n => n.Text.StartsWith("reject me", StringComparison.Ordinal));
        Assert.Empty(await store.ListPendingNotesAsync(200, ct));
        Assert.Equal("rejected|person:tester|reject me: a wrong guess|true", await ScalarAsync(owner, $@"
SELECT state || '|' || decided_by || '|' || note_text || '|' || (decided_at >= '{before:O}'::timestamptz)::text
FROM config.investigation_notes WHERE note_id = {pending[1].NoteId}", ct));
        Assert.Equal("1", await ScalarAsync(owner, $@"
SELECT count(*) FROM config.investigation_journal
WHERE event = 'note_rejected' AND actor = 'person:tester' AND request_id = '{rejectedRequest}'", ct));

        /* A decided note cannot be decided again either way, and an unknown id changes nothing. */
        Assert.False(await store.ApproveNoteAsync(pending[1].NoteId, "person:other", ct));
        Assert.False(await store.RejectNoteAsync(pending[0].NoteId, "person:other", ct));
        Assert.False(await store.ApproveNoteAsync(long.MaxValue, "person:other", ct));
        Assert.Equal("approved|person:tester,rejected|person:tester", await ScalarAsync(owner,
            "SELECT string_agg(state || '|' || decided_by, ',' ORDER BY note_id) FROM config.investigation_notes", ct));
        Assert.Equal("2", await ScalarAsync(owner,
            "SELECT count(*) FROM config.investigation_journal WHERE event IN ('note_approved', 'note_rejected')", ct));
    }

    /// <summary>Runs <paramref name="body"/> against the web routes (a test server over the endpoints) with the web host's data
    /// source logging in as a role holding exactly what the managed <paramref name="managedRole"/> holds, on a scratch store with
    /// two pending notes (the older one first). The body gets the owner source, the web login's source and a sender for the routes.</summary>
    private static async Task WithWebRoutesAsync(
        string managedRole, Func<NpgsqlDataSource, NpgsqlDataSource, Func<string, string, string?, Task<(int Status, JsonNode? Body)>>, CancellationToken, Task> body,
        AdminArmShape arm = AdminArmShape.None, bool managedStore = false, PerformanceMonitor.Darling.Service.Hosting.DarlingWebSeat? seat = null)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation note decision live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "note_seat_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = await MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        await ProposeAsync(store, DateTime.UtcNow.AddHours(-3), "first proposal", ct);
        await ProposeAsync(store, DateTime.UtcNow.AddHours(-2), "second proposal", ct);

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
            /* #6265: a decision is made by a signed-in admin. The shared token's seat (no subject) is stamped person:web. */
            app.Use(async (http, next) =>
            {
                http.Items[PerformanceMonitor.Darling.Service.Hosting.DarlingWebSeat.HttpContextItemKey] =
                    seat ?? new PerformanceMonitor.Darling.Service.Hosting.DarlingWebSeat(null, true);
                await next(http);
            });
            DarlingWebEndpoints.MapAll(app, webLogin, new CollectorRuntimeState(), new CapturingTestLogger(),
                postgresConfig: managedStore ? new PostgresConfig { Managed = true } : null, adminArm: adminArm);
            await app.StartAsync(ct);
            using var server = app.GetTestServer();

            async Task<(int Status, JsonNode? Body)> Send(string method, string path, string? contentType)
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

                    request.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(method == "PUT" ? "{}" : ""));
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

    [Fact]
    public async Task TheWebRoutes_ListApproveAndReject_AsTheManagedAdminRole()
    {
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            var list = await Send("GET", "/api/investigation-notes/pending", null);
            Assert.Equal(200, list.Status);
            Assert.Equal(2, (int)list.Body!["notes_returned"]!);
            var first = list.Body["notes"]![0]!;
            Assert.Equal("first proposal", (string)first["note_text"]!);
            Assert.Equal(ServerName, (string)first["server_name"]!);
            Assert.Equal("blocking", (string)first["family"]!);
            Assert.Equal("agent:agent-a", (string)first["author"]!);
            var firstId = (long)first["note_id"]!;
            var secondId = (long)list.Body["notes"]![1]!["note_id"]!;

            /* A write without the JSON content type is refused before the store is touched. */
            Assert.Equal(415, (await Send("PUT", $"/api/investigation-notes/{firstId}/approve", null)).Status);
            Assert.Equal(2, (int)(await Send("GET", "/api/investigation-notes/pending", null)).Body!["notes_returned"]!);

            var approved = await Send("PUT", $"/api/investigation-notes/{firstId}/approve", "application/json");
            Assert.Equal(200, approved.Status);
            Assert.Equal("approved", (string)approved.Body!["status"]!);
            Assert.Equal("person:web", (string)approved.Body["decided_by"]!);

            var rejected = await Send("PUT", $"/api/investigation-notes/{secondId}/reject", "application/json");
            Assert.Equal(200, rejected.Status);
            Assert.Equal("rejected", (string)rejected.Body!["status"]!);

            /* Deciding again, or an unknown id, is a 404 with a plain sentence and changes nothing. */
            var again = await Send("PUT", $"/api/investigation-notes/{firstId}/reject", "application/json");
            Assert.Equal(404, again.Status);
            Assert.Contains("not pending", (string)again.Body!["error"]!, StringComparison.Ordinal);
            Assert.Equal(404, (await Send("PUT", "/api/investigation-notes/999999999/approve", "application/json")).Status);

            Assert.Equal(0, (int)(await Send("GET", "/api/investigation-notes/pending", null)).Body!["notes_returned"]!);
            Assert.Equal("approved|person:web,rejected|person:web", await ScalarAsync(owner,
                "SELECT string_agg(state || '|' || decided_by, ',' ORDER BY note_id) FROM config.investigation_notes", ct));
            Assert.Equal("2", await ScalarAsync(owner,
                "SELECT count(*) FROM config.investigation_journal WHERE event IN ('note_approved', 'note_rejected') AND actor = 'person:web'", ct));
        });
    }

    /* #6600: the managed store's shape. The web reads as the viewer role; a signed-in admin's decision goes through the arm and the journal names
       the person. */
    [Fact]
    public async Task OnTheManagedStore_ASignedInAdminSeat_DecidesThroughTheArm_AsThePerson()
    {
        await WithWebRoutesAsync("web", async (owner, _, Send, ct) =>
        {
            var list = await Send("GET", "/api/investigation-notes/pending", null);
            Assert.Equal(2, (int)list.Body!["notes_returned"]!);
            var firstId = (long)list.Body["notes"]![0]!["note_id"]!;
            var secondId = (long)list.Body["notes"]![1]!["note_id"]!;

            var approved = await Send("PUT", $"/api/investigation-notes/{firstId}/approve", "application/json");
            Assert.Equal(200, approved.Status);
            Assert.Equal("person:alice", (string)approved.Body!["decided_by"]!);
            Assert.Equal(200, (await Send("PUT", $"/api/investigation-notes/{secondId}/reject", "application/json")).Status);

            Assert.Equal("approved|person:alice,rejected|person:alice", await ScalarAsync(owner,
                "SELECT string_agg(state || '|' || decided_by, ',' ORDER BY note_id) FROM config.investigation_notes", ct));
            Assert.Equal("2", await ScalarAsync(owner,
                "SELECT count(*) FROM config.investigation_journal WHERE event IN ('note_approved', 'note_rejected') AND actor = 'person:alice'", ct));
        }, arm: AdminArmShape.Admin, managedStore: true, seat: new PerformanceMonitor.Darling.Service.Hosting.DarlingWebSeat("alice", true));
    }

    /* #6600: a seat that is not a signed-in admin never opens the arm (its login is refused here, so opening it would fail); the answer is today's. */
    [Fact]
    public async Task OnTheManagedStore_AViewerSeat_NeverOpensTheArm_AndAnswersAsToday()
    {
        await WithWebRoutesAsync("web", async (owner, _, Send, ct) =>
        {
            var firstId = (long)(await Send("GET", "/api/investigation-notes/pending", null)).Body!["notes"]![0]!["note_id"]!;
            var refused = await Send("PUT", $"/api/investigation-notes/{firstId}/approve", "application/json");
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.InvestigationWriteSignInMessage, (string)refused.Body!["error"]!);
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_notes WHERE state = 'pending'", ct));
        }, arm: AdminArmShape.Refused, managedStore: true, seat: new PerformanceMonitor.Darling.Service.Hosting.DarlingWebSeat("viewer-person", false));
    }

    /* #6600: with the switch off (no arm on a managed store) the answer is today's list-only 403, and it names the switch. */
    [Fact]
    public async Task OnTheManagedStore_WithTheSwitchOff_TheSignedInAdminStillGets403_NamingTheSwitch()
    {
        await WithWebRoutesAsync("web", async (owner, _, Send, ct) =>
        {
            var firstId = (long)(await Send("GET", "/api/investigation-notes/pending", null)).Body!["notes"]![0]!["note_id"]!;
            var refused = await Send("PUT", $"/api/investigation-notes/{firstId}/approve", "application/json");
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.NoteDecisionRefusedMessage(true, adminWritesOff: true), (string)refused.Body!["error"]!);
            Assert.Contains("web.allowAdminWrites", (string)refused.Body["error"]!, StringComparison.Ordinal);
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_notes WHERE state = 'pending'", ct));
        }, managedStore: true, seat: new PerformanceMonitor.Darling.Service.Hosting.DarlingWebSeat("alice", true));
    }

    [Fact]
    public async Task TheWebRoutes_AsTheManagedViewerRole_ListNotes_ButRefuseADecision_With403_AndLeaveEveryNotePending()
    {
        await WithWebRoutesAsync("web", async (owner, viewer, Send, ct) =>
        {
            var list = await Send("GET", "/api/investigation-notes/pending", null);
            Assert.Equal(200, list.Status);
            Assert.Equal(2, (int)list.Body!["notes_returned"]!);
            var firstId = (long)list.Body["notes"]![0]!["note_id"]!;
            var secondId = (long)list.Body["notes"]![1]!["note_id"]!;

            /* The store refuses the viewer role any note write: a decision, a rewrite, a delete. */
            foreach (var statement in new[]
            {
                $"UPDATE config.investigation_notes SET state = 'approved' WHERE note_id = {firstId}",
                "UPDATE config.investigation_notes SET note_text = 'rewritten'",
                "DELETE FROM config.investigation_notes",
            })
            {
                var refused = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(viewer, statement, ct));
                Assert.Equal("42501", refused.SqlState);
            }

            /* Both decide routes pass the refusal through as a 403 whose sentence says which login is needed. */
            foreach (var route in new[] { $"/api/investigation-notes/{firstId}/approve", $"/api/investigation-notes/{secondId}/reject" })
            {
                var answer = await Send("PUT", route, "application/json");
                Assert.Equal(403, answer.Status);
                Assert.Equal(DarlingWebEndpoints.NoteDecisionRefusedMessage(false), (string)answer.Body!["error"]!);
            }

            Assert.Equal("pending|pending", await ScalarAsync(owner,
                "SELECT string_agg(state, '|' ORDER BY note_id) FROM config.investigation_notes", ct));
            Assert.Equal("0", await ScalarAsync(owner,
                "SELECT count(*) FROM config.investigation_journal WHERE event IN ('note_approved', 'note_rejected')", ct));
            Assert.Equal(2, (int)(await Send("GET", "/api/investigation-notes/pending", null)).Body!["notes_returned"]!);
        });
    }

    /* #5903 round 1: a note is born with a 90-day expires_at and the brief read drops one past it, so approving an expired
       pending note would do nothing. The list leaves it out and shows the expiry of the ones it lists; the one decide
       statement refuses a decided_by that is not a person (#6265). */
    [Fact]
    public async Task AnExpiredPendingNote_IsNotListed_AListedOneShowsItsExpiry_AndOnlyAPersonDecides()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation note decision live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = await MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        await ProposeAsync(store, DateTime.UtcNow.AddHours(-3), "stale: pending past its expiry", ct);
        await ProposeAsync(store, DateTime.UtcNow.AddHours(-2), "fresh", ct);
        await ExecAsync(owner, "UPDATE config.investigation_notes SET expires_at = now() - interval '1 day' WHERE note_text LIKE 'stale%'", ct);

        var pending = Assert.Single(await store.ListPendingNotesAsync(200, ct));
        Assert.Equal("fresh", pending.NoteText);
        Assert.Equal(DateTimeKind.Utc, pending.ExpiresAtUtc.Kind);
        Assert.InRange(pending.ExpiresAtUtc, DateTime.UtcNow.AddDays(90).AddMinutes(-5), DateTime.UtcNow.AddDays(90).AddMinutes(5));

        /* #6265: the one decide statement holds the same person guard as close and note: only person:<name> decides. */
        foreach (var actor in new[] { "agent:x", "mcp:x", "service", "web", "person:", "  ", "x", "person:erik\u200B", "person:erik  smith", " person:erik" })
        {
            await Assert.ThrowsAsync<ArgumentException>(() => store.ApproveNoteAsync(pending.NoteId, actor, ct));
            await Assert.ThrowsAsync<ArgumentException>(() => store.RejectNoteAsync(pending.NoteId, actor, ct));
        }

        Assert.Equal("0", await ScalarAsync(owner,
            "SELECT count(*) FROM config.investigation_journal WHERE event IN ('note_approved', 'note_rejected')", ct));
        Assert.Equal("pending", await ScalarAsync(owner, $"SELECT state FROM config.investigation_notes WHERE note_id = {pending.NoteId}", ct));

        /* A name of exactly the longest length decides, and is recorded whole. */
        var longest = new string('x', AgentChangeUndo.LoginMaxLength);
        Assert.True(await store.ApproveNoteAsync(pending.NoteId, InvestigationPersonWrites.PersonActor(longest), ct));
        Assert.Equal("person:" + longest, await ScalarAsync(owner,
            $"SELECT decided_by FROM config.investigation_notes WHERE note_id = {pending.NoteId}", ct));
    }

    /* #6265: a decision carries the store's clock, not the caller's. A desktop whose clock is 400 days slow still stamps decided_at
       and the journal row with the store's time, and the two agree. */
    [Fact]
    public async Task ADecision_UsesTheStoresClock_NotTheCallersClock()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation note decision live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = await MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        await ProposeAsync(store, DateTime.UtcNow.AddHours(-3), "approve from a slow clock", ct);
        await ProposeAsync(store, DateTime.UtcNow.AddHours(-2), "reject from a slow clock", ct);
        var pending = await store.ListPendingNotesAsync(200, ct);
        var skewed = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow.AddDays(-400));

        Assert.True(await skewed.ApproveNoteAsync(pending[0].NoteId, "person:tester", ct));
        Assert.True(await skewed.RejectNoteAsync(pending[1].NoteId, "person:tester", ct));

        Assert.Equal("2", await ScalarAsync(owner, @"
SELECT count(*) FROM config.investigation_notes AS n
JOIN config.investigation_journal AS j ON j.event = 'note_' || n.state AND j.reason = 'note ' || n.note_id
WHERE abs(extract(epoch FROM now() - n.decided_at)) < 120 AND j.event_time = n.decided_at", ct));
    }

    /* #6265 round 2: the Viewer's login is Environment.UserName, which can hold a space or an accented letter. The Viewer's own decide
       records it, in normal form C, where it once refused anything outside ASCII letters, digits and a few marks. */
    [Fact]
    public async Task TheViewerDecide_RecordsANameWithASpaceOrAnAccent()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation note decision live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = await MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        await ProposeAsync(store, DateTime.UtcNow.AddHours(-3), "approve as a name with a space", ct);
        await ProposeAsync(store, DateTime.UtcNow.AddHours(-2), "reject as a name with an accent", ct);
        var pending = await store.ListPendingNotesAsync(200, ct);

        await using var viewer = new PerformanceMonitor.Darling.Viewer.ViewerDataService(scratch.ConnectionString);
        Assert.True(await viewer.DecideInvestigationNoteAsync(pending[0].NoteId, true, "John Smith", ct));
        /* e + a combining acute accent: stored as the one composed letter. */
        Assert.True(await viewer.DecideInvestigationNoteAsync(pending[1].NoteId, false, "Jose\u0301", ct));

        Assert.Equal("approved|person:John Smith,rejected|person:Jos\u00E9", await ScalarAsync(owner,
            "SELECT string_agg(state || '|' || decided_by, ',' ORDER BY note_id) FROM config.investigation_notes", ct));
        Assert.Equal("2", await ScalarAsync(owner,
            "SELECT count(*) FROM config.investigation_journal WHERE event IN ('note_approved', 'note_rejected') AND actor IN ('person:John Smith', 'person:Jos\u00E9')", ct));
    }

    /* #5903 round 1: the cap. 201 pending notes (199 planted older than the two the fixture proposes) answer 200, truncated,
       oldest first; an expired one is not among them; deciding the oldest reveals the one that was cut. */
    [Fact]
    public async Task TheWebList_AtTheCap_ReturnsTheOldest200_SaysTruncated_AndDecidingTheOldestRevealsTheRest()
    {
        await WithWebRoutesAsync("admin", async (owner, _, Send, ct) =>
        {
            await ExecAsync(owner, @"
INSERT INTO config.investigation_notes (server_id, note_text, author, created_at)
SELECT 7590301, 'planted ' || g, 'agent:planted', now() - interval '1 day' - make_interval(mins => 400 - g)
FROM generate_series(1, 199) AS g;
INSERT INTO config.investigation_notes (server_id, note_text, author, created_at, expires_at)
VALUES (7590301, 'planted expired', 'agent:planted', now() - interval '2 days', now() - interval '1 day')", ct);

            var list = await Send("GET", "/api/investigation-notes/pending", null);
            Assert.Equal(200, list.Status);
            Assert.Equal(200, (int)list.Body!["notes_returned"]!);
            Assert.True((bool)list.Body["truncated"]!);
            Assert.Null(list.Body["count"]);
            var notes = list.Body["notes"]!.AsArray();
            Assert.Equal(200, notes.Count);
            Assert.Equal("planted 1", (string)notes[0]!["note_text"]!);
            Assert.DoesNotContain(notes, n => (string)n!["note_text"]! == "planted expired");
            Assert.DoesNotContain(notes, n => (string)n!["note_text"]! == "second proposal");
            Assert.False(string.IsNullOrEmpty((string)notes[0]!["expires_at_utc"]!));

            var approved = await Send("PUT", $"/api/investigation-notes/{(long)notes[0]!["note_id"]!}/approve", "application/json");
            Assert.Equal(200, approved.Status);
            var after = await Send("GET", "/api/investigation-notes/pending", null);
            Assert.Equal(200, (int)after.Body!["notes_returned"]!);
            Assert.False((bool)after.Body["truncated"]!);
            Assert.Contains(after.Body["notes"]!.AsArray(), n => (string)n!["note_text"]! == "second proposal");
        });
    }
}
