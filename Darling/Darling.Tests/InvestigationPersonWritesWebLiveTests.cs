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
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The web's three investigation writes (#6265): close a request, write a note, start a practice request. Each runs the real web route
/// over a scratch store, as the managed <c>admin</c> role (it writes, with the journal rows the Viewer writes) and as the managed
/// <c>viewer</c> role (every write answers 403 and changes nothing). A note a person writes reaches a brief; an agent's proposed note
/// does not until somebody approves it.
/// </summary>
public sealed class InvestigationPersonWritesWebLiveTests
{
    private const int ServerId = 7_626_501;
    private const string ServerName = "LcPersonWriteSrv";
    private const string RolePassword = "person-write-live-pw";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static InvestigationAlert Alert(DateTime at) =>
        new(1, ServerId, ServerName, "blocking", "Blocking", at, new[] { ServerId + ":Blocking" }, null,
            20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);

    private static async Task<Guid> OpenAsync(InvestigationRequestStore store, DateTime at, CancellationToken ct) =>
        (await store.OpenOrJoinAsync(Alert(at), ct)).RequestId!.Value;

    /* An open request an agent claims and reports; a later alert after the report opens a NEW request. */
    private static async Task<Guid> ReportedAsync(InvestigationRequestStore store, DateTime at, string? proposedNote, CancellationToken ct)
    {
        var requestId = await OpenAsync(store, at, ct);
        var claim = await store.ClaimAsync(requestId, "agent-a", ct);
        Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
            requestId, claim.ClaimId!.Value, "agent-a", 1, "real", "root cause", true, "summary",
            "[{\"tool\":\"get_blocking\"}]", ProposedNote: proposedNote), ct));
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

    /// <summary>Runs <paramref name="body"/> against the web routes with the web host's data source logging in as a role holding exactly
    /// what the managed <paramref name="managedRole"/> holds. The body gets the owner source, the request store and a sender
    /// (method, path, content type, body text). <paramref name="signedInAs"/> is read on every request: the signed-in admin's name, or null
    /// for a dashboard with no sign-in configured (no seat is stored, as the host does when no web token is set).</summary>
    private static async Task WithWebRoutesAsync(
        string managedRole,
        Func<string?> signedInAs,
        Func<NpgsqlDataSource, InvestigationRequestStore, Func<string, string, string?, string?, Task<(int Status, JsonNode? Body)>>, CancellationToken, Task> body,
        AdminArmShape arm = AdminArmShape.None, bool managedStore = false, bool seatCanEdit = true)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation person-write live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "person_seat_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);

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
            app.Use(async (http, next) =>
            {
                if (signedInAs() is { } name)
                {
                    http.Items[DarlingWebSeat.HttpContextItemKey] = new DarlingWebSeat(name, seatCanEdit);
                }

                await next(http);
            });
            DarlingWebEndpoints.MapAll(app, webLogin, new CollectorRuntimeState(), new CapturingTestLogger(),
                postgresConfig: managedStore ? new PostgresConfig { Managed = true } : null, adminArm: adminArm);
            await app.StartAsync(ct);
            using var server = app.GetTestServer();

            async Task<(int Status, JsonNode? Body)> Send(string method, string path, string? contentType, string? text)
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

                    request.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(text ?? "{}"));
                });
                using var reader = new StreamReader(context.Response.Body);
                var answer = await reader.ReadToEndAsync(ct);
                return (context.Response.StatusCode, string.IsNullOrWhiteSpace(answer) ? null : JsonNode.Parse(answer));
            }

            await body(owner, store, Send, ct);
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

    /// <summary>A real signed-in name. The routes record exactly person:erik@example.com, never a constant and never anything the body says.</summary>
    private const string Lee = "erik@example.com";

    private const string SignInSentence = "Sign in to the web as an admin to make this change, or use the Viewer.";

    [Fact]
    public async Task CloseRoute_ClosesAnOpenAndAReportedRequest_RefusesTheRest_AndJournalsClosedByThePerson()
    {
        await WithWebRoutesAsync("admin", () => Lee, async (owner, store, Send, ct) =>
        {
            var reported = await ReportedAsync(store, DateTime.UtcNow.AddHours(-3), null, ct);
            var open = await OpenAsync(store, DateTime.UtcNow.AddHours(-1), ct);

            /* Refused before the store is touched: no JSON content type, a bad id, no reason, a reason over 500 characters. */
            Assert.Equal(415, (await Send("PUT", $"/api/investigation-requests/{open}/close", null, "{\"reason\":\"x\"}")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/investigation-requests/not-a-guid/close", Json, "{\"reason\":\"x\"}")).Status);
            var noReason = await Send("PUT", $"/api/investigation-requests/{open}/close", Json, "{\"reason\":\"   \"}");
            Assert.Equal(400, noReason.Status);
            Assert.Equal(InvestigationPersonWrites.ValidateCloseReason("  ")!, (string)noReason.Body!["error"]!);
            Assert.Equal(400, (await Send("PUT", $"/api/investigation-requests/{open}/close", Json, "[1]")).Status);
            Assert.Equal(400, (await Send("PUT", $"/api/investigation-requests/{open}/close", Json,
                "{\"reason\":\"" + new string('r', 501) + "\"}")).Status);
            Assert.Equal("open", await ScalarAsync(owner, $"SELECT state FROM config.investigation_requests WHERE request_id = '{open}'", ct));

            var closed = await Send("PUT", $"/api/investigation-requests/{open}/close", Json, "{\"reason\":\"  duplicate of the nightly batch  \",\"closed_by\":\"x\",\"author\":\"agent:x\"}");
            Assert.Equal(200, closed.Status);
            Assert.Equal("closed", (string)closed.Body!["status"]!);
            Assert.Equal("person:erik@example.com", (string)closed.Body["closed_by"]!);
            Assert.Equal("closed|person:erik@example.com|duplicate of the nightly batch|true", await ScalarAsync(owner, $@"
SELECT state || '|' || closed_by || '|' || closed_reason || '|' || (closed_at IS NOT NULL)::text
FROM config.investigation_requests WHERE request_id = '{open}'", ct));
            Assert.Equal($"closed|person:erik@example.com|duplicate of the nightly batch|{ServerId}", await ScalarAsync(owner, $@"
SELECT event || '|' || actor || '|' || reason || '|' || server_id
FROM config.investigation_journal WHERE request_id = '{open}' AND event = 'closed'", ct));

            /* A reported request closes too. */
            Assert.Equal(200, (await Send("PUT", $"/api/investigation-requests/{reported}/close", Json, "{\"reason\":\"reviewed\"}")).Status);
            Assert.Equal("closed", await ScalarAsync(owner, $"SELECT state FROM config.investigation_requests WHERE request_id = '{reported}'", ct));

            /* Already closed, or gone: 409, nothing written (one journal row per closed request). */
            var again = await Send("PUT", $"/api/investigation-requests/{open}/close", Json, "{\"reason\":\"again\"}");
            Assert.Equal(409, again.Status);
            Assert.Contains("not open", (string)again.Body!["error"]!, StringComparison.Ordinal);
            Assert.Equal(409, (await Send("PUT", $"/api/investigation-requests/{Guid.NewGuid()}/close", Json, "{\"reason\":\"gone\"}")).Status);
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'closed'", ct));
            Assert.Equal("duplicate of the nightly batch", await ScalarAsync(owner, $"SELECT closed_reason FROM config.investigation_requests WHERE request_id = '{open}'", ct));
        });
    }

    [Fact]
    public async Task NoteRoute_WritesAnApprovedNoteThatReachesABrief_WhileAnAgentsProposedNoteDoesNotUntilApproved()
    {
        await WithWebRoutesAsync("admin", () => Lee, async (owner, store, Send, ct) =>
        {
            /* An agent proposes a note with its report: it waits, and no brief carries it. */
            await ReportedAsync(store, DateTime.UtcNow.AddHours(-3), "agent proposal: pending until a person approves", ct);
            var reader = new PgInvestigationBriefSourceReader(owner, utcNow: () => DateTime.UtcNow);
            var next = await OpenAsync(store, DateTime.UtcNow, ct);
            Assert.Empty((await reader.ReadAsync(next, ct))!.Notes);

            /* Refused: no JSON, a blank or over-long note, days outside 1 to 365, a bad or unregistered server. */
            Assert.Equal(415, (await Send("PUT", "/api/investigation-notes", null, "{\"note_text\":\"x\"}")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/investigation-notes", Json, "{\"note_text\":\"  \"}")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/investigation-notes", Json, "{\"note_text\":\"" + new string('n', 2001) + "\"}")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/investigation-notes", Json, "{\"note_text\":\"x\",\"days\":0}")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/investigation-notes", Json, "{\"note_text\":\"x\",\"days\":366}")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/investigation-notes", Json, "{\"note_text\":\"x\",\"server_id\":\"abc\"}")).Status);
            Assert.Equal(404, (await Send("PUT", "/api/investigation-notes", Json, "{\"note_text\":\"x\",\"server_id\":999999}")).Status);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_notes WHERE author LIKE 'person:%'", ct));

            /* A person's note is approved as written, by its author, and the journal says so (no request). */
            var written = await Send("PUT", "/api/investigation-notes", Json,
                "{\"note_text\":\"  the nightly load blocks for a minute  \",\"server_id\":" + ServerId + ",\"metric_name\":\" Blocking \",\"days\":30,\"author\":\"agent:x\",\"decided_by\":\"x\"}");
            Assert.Equal(200, written.Status);
            Assert.Equal("approved", (string)written.Body!["status"]!);
            Assert.Equal("person:erik@example.com", (string)written.Body["author"]!);
            var noteId = (long)written.Body["note_id"]!;
            Assert.Equal($"approved|person:erik@example.com|person:erik@example.com|the nightly load blocks for a minute|Blocking|{ServerId}|true", await ScalarAsync(owner, $@"
SELECT state || '|' || author || '|' || decided_by || '|' || note_text || '|' || metric_name || '|' || server_id
       || '|' || (expires_at BETWEEN now() + interval '29 days' AND now() + interval '31 days')::text
FROM config.investigation_notes WHERE note_id = {noteId}", ct));
            Assert.Equal($"person:erik@example.com|note {noteId}|{ServerId}|", await ScalarAsync(owner, $@"
SELECT actor || '|' || reason || '|' || server_id || '|' || coalesce(request_id::text, '')
FROM config.investigation_journal WHERE event = 'note_written'", ct));

            /* A fleet note (no server) takes the default 90 days. */
            var fleet = await Send("PUT", "/api/investigation-notes", Json, "{\"note_text\":\"fleet-wide: patching happens on Sundays\"}");
            Assert.Equal(200, fleet.Status);
            Assert.Equal(90, (int)fleet.Body!["days"]!);

            /* The approved note reaches a brief; the agent's pending proposal still does not. */
            var notes = (await reader.ReadAsync(next, ct))!.Notes;
            Assert.Contains(notes, n => n.Text == "the nightly load blocks for a minute" && n.DecidedBy == "person:erik@example.com");
            Assert.DoesNotContain(notes, n => n.Text.StartsWith("agent proposal", StringComparison.Ordinal));
            Assert.Equal("pending", await ScalarAsync(owner, "SELECT state FROM config.investigation_notes WHERE author LIKE 'agent:%'", ct));

            /* Approving the proposal is the existing route, and only then does it reach the brief. */
            var pendingId = (long)(await Send("GET", "/api/investigation-notes/pending", null, "")).Body!["notes"]![0]!["note_id"]!;
            var approved = await Send("PUT", $"/api/investigation-notes/{pendingId}/approve", Json, "{}");
            Assert.Equal(200, approved.Status);
            Assert.Equal("person:erik@example.com", (string)approved.Body!["decided_by"]!);
            Assert.Equal("person:erik@example.com", await ScalarAsync(owner, $"SELECT decided_by FROM config.investigation_notes WHERE note_id = {pendingId}", ct));
            Assert.Equal("person:erik@example.com", await ScalarAsync(owner,
                $"SELECT actor FROM config.investigation_journal WHERE event = 'note_approved'", ct));
            Assert.Contains((await reader.ReadAsync(next, ct))!.Notes, n => n.Text.StartsWith("agent proposal", StringComparison.Ordinal));
        });
    }

    [Fact]
    public async Task PracticeRoute_OpensAJudgedCase_RefusesTheRest_AndJournalsOpenedByThePerson()
    {
        await WithWebRoutesAsync("admin", () => Lee, async (owner, store, Send, ct) =>
        {
            var judged = await ReportedAsync(store, DateTime.UtcNow.AddHours(-5), null, ct);
            var noFeedback = await ReportedAsync(store, DateTime.UtcNow.AddHours(-3), null, ct);
            var open = await OpenAsync(store, DateTime.UtcNow.AddHours(-1), ct);
            Assert.Equal(InvestigationFeedbackOutcome.Recorded, (await new InvestigationFeedbackStore(owner, 30).RecordAsync(
                new InvestigationFeedbackSubmission(judged, "wrong", "false_positive", "missed the batch window", null, "lee"), ct)).Outcome);

            Assert.Equal(415, (await Send("PUT", $"/api/investigation-practice-requests/{judged}", null, "{}")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/investigation-practice-requests/not-a-guid", Json, "{}")).Status);

            /* The refusals the Viewer shows: unknown, no report, no feedback. Nothing is opened. */
            Assert.Equal(404, (await Send("PUT", $"/api/investigation-practice-requests/{Guid.NewGuid()}", Json, "{}")).Status);
            var noReport = await Send("PUT", $"/api/investigation-practice-requests/{open}", Json, "{}");
            Assert.Equal(409, noReport.Status);
            Assert.Equal("NoReport", (string)noReport.Body!["outcome"]!);
            var none = await Send("PUT", $"/api/investigation-practice-requests/{noFeedback}", Json, "{}");
            Assert.Equal(409, none.Status);
            Assert.Equal("NoFeedback", (string)none.Body!["outcome"]!);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_requests WHERE practice_of IS NOT NULL", ct));

            var opened = await Send("PUT", $"/api/investigation-practice-requests/{judged}", Json, "{}");
            Assert.Equal(200, opened.Status);
            Assert.Equal("opened", (string)opened.Body!["status"]!);
            Assert.Equal("person:erik@example.com", (string)opened.Body["opened_by"]!);
            var practiceId = Guid.Parse((string)opened.Body["request_id"]!);
            Assert.Equal($"{judged}", await ScalarAsync(owner, $"SELECT practice_of FROM config.investigation_requests WHERE request_id = '{practiceId}'", ct));
            Assert.Equal("person:erik@example.com", await ScalarAsync(owner, $"SELECT actor FROM config.investigation_journal WHERE request_id = '{practiceId}' AND event = 'opened'", ct));

            /* The same case again while its practice request is live, and a practice request itself: refused. */
            var live = await Send("PUT", $"/api/investigation-practice-requests/{judged}", Json, "{}");
            Assert.Equal(409, live.Status);
            Assert.Equal("AlreadyLive", (string)live.Body!["outcome"]!);
            Assert.Equal("IsPractice", (string)(await Send("PUT", $"/api/investigation-practice-requests/{practiceId}", Json, "{}")).Body!["outcome"]!);
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_requests WHERE practice_of IS NOT NULL", ct));
        });
    }

    /* #6600: the managed store's shape. The web reads as the viewer role; a signed-in admin's close, note and practice request go through the arm
       and each records the person. */
    [Fact]
    public async Task OnTheManagedStore_ASignedInAdminSeat_ClosesWritesAndPractisesThroughTheArm_AsThePerson()
    {
        await WithWebRoutesAsync("viewer", () => "alice", async (owner, store, Send, ct) =>
        {
            var judged = await ReportedAsync(store, DateTime.UtcNow.AddHours(-5), null, ct);
            var open = await OpenAsync(store, DateTime.UtcNow.AddHours(-1), ct);
            await new InvestigationFeedbackStore(owner, 30).RecordAsync(new InvestigationFeedbackSubmission(judged, "right", null, null, null, "lee"), ct);

            var closed = await Send("PUT", $"/api/investigation-requests/{open}/close", Json, "{\"reason\":\"duplicate\"}");
            Assert.Equal(200, closed.Status);
            Assert.Equal("person:alice", (string)closed.Body!["closed_by"]!);

            var note = await Send("PUT", "/api/investigation-notes", Json, "{\"note_text\":\"a person wrote this\",\"server_id\":" + ServerId + "}");
            Assert.Equal(200, note.Status);
            Assert.Equal("person:alice", (string)note.Body!["author"]!);

            var practice = await Send("PUT", $"/api/investigation-practice-requests/{judged}", Json, "{}");
            Assert.Equal(200, practice.Status);
            Assert.Equal("person:alice", (string)practice.Body!["opened_by"]!);

            Assert.Equal("3", await ScalarAsync(owner,
                "SELECT count(*) FROM config.investigation_journal WHERE actor = 'person:alice' AND event IN ('closed', 'note_written', 'opened')", ct));
        }, arm: AdminArmShape.Admin, managedStore: true);
    }

    /* #6600: an OIDC viewer seat never opens the arm (its login is refused here, so opening it would fail): the writes answer as they do today. */
    [Fact]
    public async Task OnTheManagedStore_AViewerSeat_NeverOpensTheArm_AndAnswersAsToday()
    {
        await WithWebRoutesAsync("viewer", () => "viewer-person", async (owner, store, Send, ct) =>
        {
            var open = await OpenAsync(store, DateTime.UtcNow.AddHours(-1), ct);
            var refused = await Send("PUT", $"/api/investigation-requests/{open}/close", Json, "{\"reason\":\"viewer tries\"}");
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.InvestigationWriteSignInMessage, (string)refused.Body!["error"]!);
            Assert.Equal("open", await ScalarAsync(owner, $"SELECT state FROM config.investigation_requests WHERE request_id = '{open}'", ct));
        }, arm: AdminArmShape.Refused, managedStore: true, seatCanEdit: false);
    }

    /* #6600: with the switch off (no arm on a managed store) the signed-in admin gets today's 403, which now names the switch. */
    [Fact]
    public async Task OnTheManagedStore_WithTheSwitchOff_TheSignedInAdminGets403_NamingTheSwitch()
    {
        await WithWebRoutesAsync("viewer", () => "alice", async (owner, store, Send, ct) =>
        {
            var open = await OpenAsync(store, DateTime.UtcNow.AddHours(-1), ct);
            var refused = await Send("PUT", $"/api/investigation-requests/{open}/close", Json, "{\"reason\":\"no switch\"}");
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.InvestigationWriteRefusedMessage(true, adminWritesOff: true), (string)refused.Body!["error"]!);
            Assert.Contains("web.allowAdminWrites", (string)refused.Body["error"]!, StringComparison.Ordinal);
            Assert.Equal("open", await ScalarAsync(owner, $"SELECT state FROM config.investigation_requests WHERE request_id = '{open}'", ct));
        }, managedStore: true);
    }

    [Fact]
    public async Task AllThreeRoutes_AsTheManagedViewerRole_Answer403_AndChangeNothing()
    {
        await WithWebRoutesAsync("viewer", () => Lee, async (owner, store, Send, ct) =>
        {
            var judged = await ReportedAsync(store, DateTime.UtcNow.AddHours(-5), null, ct);
            var open = await OpenAsync(store, DateTime.UtcNow.AddHours(-1), ct);
            await new InvestigationFeedbackStore(owner, 30).RecordAsync(new InvestigationFeedbackSubmission(judged, "right", null, null, null, "lee"), ct);
            var journalBefore = await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct);

            foreach (var (path, text) in new[]
            {
                ($"/api/investigation-requests/{open}/close", "{\"reason\":\"viewer tries\"}"),
                ("/api/investigation-notes", "{\"note_text\":\"viewer tries\",\"server_id\":" + ServerId + "}"),
                ($"/api/investigation-practice-requests/{judged}", "{}"),
            })
            {
                var answer = await Send("PUT", path, Json, text);
                Assert.Equal(403, answer.Status);
                Assert.Equal(DarlingWebEndpoints.InvestigationWriteRefusedMessage(false), (string)answer.Body!["error"]!);
            }

            Assert.Equal("open", await ScalarAsync(owner, $"SELECT state FROM config.investigation_requests WHERE request_id = '{open}'", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_notes", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_requests WHERE practice_of IS NOT NULL", ct));
            Assert.Equal(journalBefore, await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));
        });
    }

    [Fact]
    public async Task AllFiveWrites_WithNoSignedInAdmin_Answer403WithTheSignInSentence_AndWriteNothing()
    {
        await WithWebRoutesAsync("admin", () => null, async (owner, store, Send, ct) =>
        {
            var judged = await ReportedAsync(store, DateTime.UtcNow.AddHours(-5), "agent proposal: stays pending", ct);
            var open = await OpenAsync(store, DateTime.UtcNow.AddHours(-1), ct);
            await new InvestigationFeedbackStore(owner, 30).RecordAsync(new InvestigationFeedbackSubmission(judged, "right", null, null, null, "lee"), ct);
            var pendingId = await ScalarAsync(owner, "SELECT note_id FROM config.investigation_notes WHERE state = 'pending'", ct);
            var journalBefore = await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct);
            var requestsBefore = await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_requests", ct);

            foreach (var (path, text) in new[]
            {
                ($"/api/investigation-requests/{open}/close", "{\"reason\":\"no sign-in\"}"),
                ("/api/investigation-notes", "{\"note_text\":\"no sign-in\"}"),
                ($"/api/investigation-practice-requests/{judged}", "{}"),
                ($"/api/investigation-notes/{pendingId}/approve", "{}"),
                ($"/api/investigation-notes/{pendingId}/reject", "{}"),
            })
            {
                var answer = await Send("PUT", path, Json, text);
                Assert.Equal(403, answer.Status);
                Assert.Equal(SignInSentence, (string)answer.Body!["error"]!);
            }

            /* Ids that do not exist answer the same 403, not 404: the gate is the first statement, before any store read. */
            foreach (var (path, text) in new[]
            {
                ($"/api/investigation-requests/{Guid.NewGuid()}/close", "{\"reason\":\"no sign-in\"}"),
                ($"/api/investigation-practice-requests/{Guid.NewGuid()}", "{}"),
                ("/api/investigation-notes/9999999999/approve", "{}"),
                ("/api/investigation-notes/9999999999/reject", "{}"),
            })
            {
                var answer = await Send("PUT", path, Json, text);
                Assert.Equal(403, answer.Status);
                Assert.Equal(SignInSentence, (string)answer.Body!["error"]!);
            }

            Assert.Equal("open", await ScalarAsync(owner, $"SELECT state FROM config.investigation_requests WHERE request_id = '{open}'", ct));
            Assert.Equal("pending", await ScalarAsync(owner, $"SELECT state FROM config.investigation_notes WHERE note_id = {pendingId}", ct));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_notes", ct));
            Assert.Equal(requestsBefore, await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_requests", ct));
            Assert.Equal(journalBefore, await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));
            Assert.False((bool)(await Send("GET", "/api/session", null, "")).Body!["signed_in_admin"]!);
        });
    }

    [Fact]
    public async Task AName_ThatCannotBeRecorded_Answers400_OnEveryWrite_AndWritesNothing()
    {
        string? current = Lee;
        await WithWebRoutesAsync("admin", () => current, async (owner, store, Send, ct) =>
        {
            var judged = await ReportedAsync(store, DateTime.UtcNow.AddHours(-5), "agent proposal: stays pending", ct);
            var open = await OpenAsync(store, DateTime.UtcNow.AddHours(-1), ct);
            await new InvestigationFeedbackStore(owner, 30).RecordAsync(new InvestigationFeedbackSubmission(judged, "right", null, null, null, "lee"), ct);
            var pendingId = await ScalarAsync(owner, "SELECT note_id FROM config.investigation_notes WHERE state = 'pending'", ct);
            var journalBefore = await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct);

            /* One more than the longest name the shape holds, a double space, and names with invisible, format, bidi or filler characters
               that would pass for another person's, or characters that no sink should have to encode. */
            foreach (var name in new[]
            {
                new string('a', AgentChangeUndo.LoginMaxLength + 1), "erik  smith",
                "erik\u200B@example.com", "erik\u202E@example.com", "erik\u2066@example.com", "erik\uFEFF@example.com", "erik\u3164",
                "erik<x>@example.com", "erik;smith",
            })
            {
                current = name;
                foreach (var (path, text) in new[]
                {
                    ($"/api/investigation-requests/{open}/close", "{\"reason\":\"bad name\"}"),
                    ("/api/investigation-notes", "{\"note_text\":\"bad name\"}"),
                    ($"/api/investigation-practice-requests/{judged}", "{}"),
                    ($"/api/investigation-notes/{pendingId}/approve", "{}"),
                    ($"/api/investigation-notes/{pendingId}/reject", "{}"),
                })
                {
                    var answer = await Send("PUT", path, Json, text);
                    Assert.True(answer.Status == 400, $"{path} with a {name.Length}-character name answered {answer.Status}");
                }
            }

            Assert.Equal("open", await ScalarAsync(owner, $"SELECT state FROM config.investigation_requests WHERE request_id = '{open}'", ct));
            Assert.Equal("pending", await ScalarAsync(owner, $"SELECT state FROM config.investigation_notes WHERE note_id = {pendingId}", ct));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_notes", ct));
            Assert.Equal(journalBefore, await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));

            /* The longest name the shape holds (exactly LoginMaxLength) is recorded in full. */
            current = new string('a', AgentChangeUndo.LoginMaxLength - 6) + "@x.com";
            Assert.Equal(AgentChangeUndo.LoginMaxLength, current.Length);
            var closed = await Send("PUT", $"/api/investigation-requests/{open}/close", Json, "{\"reason\":\"long but fine\"}");
            Assert.Equal(200, closed.Status);
            Assert.Equal("person:" + current, (string)closed.Body!["closed_by"]!);
            Assert.Equal("person:" + current, await ScalarAsync(owner, $"SELECT closed_by FROM config.investigation_requests WHERE request_id = '{open}'", ct));
        });
    }

    /* #6265 round 2: a real sign-in name or Windows user name is recorded as the person, whatever it holds besides letters: a space, an
       accent, an email's plus or quote, an Auth0 pipe, a domain and a backslash. The note route and the approve route both stamp it,
       in normal form C, and the journal's actor agrees. */
    [Fact]
    public async Task ARealName_WithASpaceAnAccentOrAnEmailsPunctuation_IsRecordedExactlyAsThePerson()
    {
        string current = Lee;
        await WithWebRoutesAsync("admin", () => current, async (owner, store, Send, ct) =>
        {
            await ReportedAsync(store, DateTime.UtcNow.AddHours(-5), "agent proposal: stays pending", ct);
            var pendingId = await ScalarAsync(owner, "SELECT note_id FROM config.investigation_notes WHERE state = 'pending'", ct);

            var names = new[]
            {
                "John Smith", "Jos\u00E9", "erik+ops@example.com", "o'brien@example.com", "auth0|abc123", "CORP\\jsmith",
                new string('z', AgentChangeUndo.LoginMaxLength),
            };
            foreach (var name in names)
            {
                current = name;
                var written = await Send("PUT", "/api/investigation-notes", Json, "{\"note_text\":\"written as " + name.Length + "\"}");
                Assert.True(written.Status == 200, $"a note as {name.Length}-character name answered {written.Status}");
                Assert.Equal("person:" + name, (string)written.Body!["author"]!);
                Assert.Equal("person:" + name, await ScalarAsync(owner,
                    $"SELECT author FROM config.investigation_notes WHERE note_id = {(long)written.Body["note_id"]!}", ct));
            }

            /* A name given in decomposed form (e and a combining acute) is stored once, composed. */
            current = "Jose\u0301";
            var approved = await Send("PUT", $"/api/investigation-notes/{pendingId}/approve", Json, "{}");
            Assert.Equal(200, approved.Status);
            Assert.Equal("person:Jos\u00E9", (string)approved.Body!["decided_by"]!);
            Assert.Equal("person:Jos\u00E9", await ScalarAsync(owner, $"SELECT decided_by FROM config.investigation_notes WHERE note_id = {pendingId}", ct));
            Assert.Equal("person:Jos\u00E9", await ScalarAsync(owner, "SELECT actor FROM config.investigation_journal WHERE event = 'note_approved'", ct));
            Assert.Equal(names.Length.ToString(CultureInfo.InvariantCulture), await ScalarAsync(owner,
                "SELECT count(*) FROM config.investigation_journal WHERE event = 'note_written'", ct));
        });
    }

    [Fact]
    public async Task ABodyWithADuplicateKey_Answers400_AndAMetricNameOver128_IsRefused()
    {
        await WithWebRoutesAsync("admin", () => Lee, async (owner, store, Send, ct) =>
        {
            var open = await OpenAsync(store, DateTime.UtcNow.AddHours(-1), ct);
            var close = await Send("PUT", $"/api/investigation-requests/{open}/close", Json, "{\"reason\":\"a\",\"reason\":\"b\"}");
            Assert.Equal(400, close.Status);
            Assert.Equal("The body must be a JSON object.", (string)close.Body!["error"]!);
            Assert.Equal(400, (await Send("PUT", "/api/investigation-notes", Json, "{\"note_text\":\"a\",\"note_text\":\"b\"}")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/investigation-notes", Json, "{\"note_text\":\"a\",\"days\":5,\"days\":6}")).Status);
            Assert.Equal("open", await ScalarAsync(owner, $"SELECT state FROM config.investigation_requests WHERE request_id = '{open}'", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_notes", ct));

            var tooLong = await Send("PUT", "/api/investigation-notes", Json, "{\"note_text\":\"a\",\"metric_name\":\"" + new string('m', 129) + "\"}");
            Assert.Equal(400, tooLong.Status);
            Assert.Equal("The metric name must be 128 characters or fewer.", (string)tooLong.Body!["error"]!);
            Assert.Equal(400, (await Send("PUT", "/api/investigation-notes", Json, "{\"note_text\":\"a\",\"metric_name\":\"a\\u0007b\"}")).Status);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_notes", ct));
            Assert.Equal(200, (await Send("PUT", "/api/investigation-notes", Json, "{\"note_text\":\"a\",\"metric_name\":\"" + new string('m', 128) + "\"}")).Status);
        });
    }

    [Fact]
    public async Task TheStorageWrites_RefuseAnActorThatIsNotAPerson_AndWriteNothing()
    {
        await WithWebRoutesAsync("admin", () => Lee, async (owner, store, Send, ct) =>
        {
            var open = await OpenAsync(store, DateTime.UtcNow.AddHours(-1), ct);
            var journalBefore = await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct);

            foreach (var actor in new[] { "agent:x", "mcp:x", "service", "web", "person:", "person:erik\u200B", "Person:erik", " person:erik", "person: erik", "person:erik  smith" })
            {
                await Assert.ThrowsAsync<ArgumentException>(() => store.CloseRequestAsync(open, actor, "reason", ct));
                await Assert.ThrowsAsync<ArgumentException>(() => store.WriteNoteAsync(null, null, "an approved note", actor, 30, ct));
            }

            Assert.Equal("open", await ScalarAsync(owner, $"SELECT state FROM config.investigation_requests WHERE request_id = '{open}'", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_notes", ct));
            Assert.Equal(journalBefore, await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));

            /* The note decision and the practice request hold the same person guard (#6265). */
            var judged = await ReportedAsync(store, DateTime.UtcNow.AddHours(-5), "agent proposal: stays pending", ct);
            await new InvestigationFeedbackStore(owner, 30).RecordAsync(new InvestigationFeedbackSubmission(judged, "right", null, null, null, "lee"), ct);
            var pendingId = long.Parse(await ScalarAsync(owner, "SELECT note_id FROM config.investigation_notes WHERE state = 'pending'", ct) ?? "0", CultureInfo.InvariantCulture);
            var practice = new InvestigationPracticeStore(owner, 30);
            var journalAfterReport = await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct);
            foreach (var actor in new[] { "agent:x", "mcp:x", "service", "web", "person:", "person:erik\u200B", "Person:erik", " person:erik", "person:erik  smith" })
            {
                await Assert.ThrowsAsync<ArgumentException>(() => store.ApproveNoteAsync(pendingId, actor, ct));
                await Assert.ThrowsAsync<ArgumentException>(() => store.RejectNoteAsync(pendingId, actor, ct));
                await Assert.ThrowsAsync<ArgumentException>(() => practice.OpenAsync(judged, actor, ct));
            }

            Assert.Equal("pending", await ScalarAsync(owner, $"SELECT state FROM config.investigation_notes WHERE note_id = {pendingId}", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_requests WHERE practice_of IS NOT NULL", ct));
            Assert.Equal(journalAfterReport, await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));

            /* A person's actor, built the one way, writes. */
            var actorOk = InvestigationPersonWrites.PersonActor(Lee);
            Assert.True(await store.CloseRequestAsync(open, actorOk, "reviewed", ct));
            Assert.True(await store.WriteNoteAsync(null, null, "a person's note", actorOk, 30, ct) > 0);
        });
    }

    [Fact]
    public async Task ACloseAndANoteWrite_UseTheStoresClock_NotTheCallersClock()
    {
        await WithWebRoutesAsync("admin", () => Lee, async (owner, store, Send, ct) =>
        {
            var open = await OpenAsync(store, DateTime.UtcNow.AddHours(-1), ct);
            /* A caller whose clock is 400 days slow (a desktop with a wrong date): the store's rows still carry the store's time. */
            var skewed = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow.AddDays(-400));
            var actor = InvestigationPersonWrites.PersonActor(Lee);
            Assert.True(await skewed.CloseRequestAsync(open, actor, "closed from a desktop with a wrong clock", ct));
            var noteId = await skewed.WriteNoteAsync(null, null, "written from a desktop with a wrong clock", actor, 30, ct);

            Assert.Equal("true|true", await ScalarAsync(owner, $@"
SELECT (abs(extract(epoch FROM now() - closed_at)) < 120)::text || '|' ||
       (closed_at = (SELECT event_time FROM config.investigation_journal WHERE request_id = '{open}' AND event = 'closed'))::text
FROM config.investigation_requests WHERE request_id = '{open}'", ct));
            Assert.Equal("true|true|true|true", await ScalarAsync(owner, $@"
SELECT (abs(extract(epoch FROM now() - created_at)) < 120)::text || '|' ||
       (abs(extract(epoch FROM now() - decided_at)) < 120)::text || '|' ||
       (expires_at BETWEEN now() + interval '29 days' AND now() + interval '31 days')::text || '|' ||
       (created_at = (SELECT event_time FROM config.investigation_journal WHERE event = 'note_written'))::text
FROM config.investigation_notes WHERE note_id = {noteId}", ct));
        });
    }
}
