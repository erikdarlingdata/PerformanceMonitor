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
/// People's feedback on investigation reports (#5913) on a live store: a verdict outside right, partly or wrong is refused
/// with plain words; feedback moves the false-positive counts of that alert family and of no other; a second record by the
/// same author, in any letter case, replaces the first; deleting a request deletes its feedback; and the web records
/// feedback under the managed <c>admin</c> role and answers 403 under the managed <c>viewer</c> role (the grants replayed from
/// the shipped provisioning SQL), while listing for both.
/// </summary>
public sealed class InvestigationFeedbackLiveTests
{
    internal const int ServerId = 7_591_301;
    internal const string ServerName = "LcFeedbackSrv";
    private const string RolePassword = "feedback-live-pw";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    internal static InvestigationAlert Alert(DateTime at, string family) =>
        new(1, ServerId, ServerName, family, family == "blocking" ? "Blocking" : "Cpu", at, new[] { ServerId + ":" + family }, null,
            50, 200, LeaseMinutes: 15, ExpiryMinutes: 240);

    /* A request an agent claims and reports with the given verdict. */
    internal static async Task<Guid> ReportedAsync(InvestigationRequestStore store, DateTime at, string family, string verdict, CancellationToken ct)
    {
        var requestId = (await store.OpenOrJoinAsync(Alert(at, family), ct)).RequestId!.Value;
        var claim = await store.ClaimAsync(requestId, "agent-a", ct);
        Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
            requestId, claim.ClaimId!.Value, "agent-a", 1, verdict, "root cause", true, "summary",
            "[{\"tool\":\"get_blocking\"}]"), ct));
        return requestId;
    }

    private static async Task<string?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    internal static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    internal static async Task<NpgsqlDataSource> MigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        return NpgsqlDataSource.Create(scratch.ConnectionString);
    }

    internal static InvestigationFeedbackSubmission Feedback(Guid request, string verdict, string author, string? corrected = null,
        string? missed = null, string? comment = null) => new(request, verdict, corrected, missed, comment, author);

    [Fact]
    public async Task AVerdictOutsideRightPartlyOrWrong_IsRefusedInPlainWords_AndNothingIsWritten()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation feedback live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = await MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var feedback = new InvestigationFeedbackStore(owner, 30);
        var request = await ReportedAsync(store, DateTime.UtcNow.AddHours(-2), "blocking", "real", ct);

        foreach (var bad in new[] { "good", "RIGHT", "real", "", "  " })
        {
            var refused = await feedback.RecordAsync(Feedback(request, bad, "erik"), ct);
            Assert.Equal(InvestigationFeedbackOutcome.Refused, refused.Outcome);
            Assert.Contains("right, partly or wrong", refused.Message!, StringComparison.Ordinal);
        }

        var badCorrected = await feedback.RecordAsync(Feedback(request, "wrong", "erik", corrected: "fine"), ct);
        Assert.Equal(InvestigationFeedbackOutcome.Refused, badCorrected.Outcome);
        Assert.Contains("real, false_positive or needs_person", badCorrected.Message!, StringComparison.Ordinal);

        var tooLong = await feedback.RecordAsync(Feedback(request, "wrong", "erik", comment: new string('x', 4001)), ct);
        Assert.Equal(InvestigationFeedbackOutcome.Refused, tooLong.Outcome);
        Assert.Contains("comment", tooLong.Message!, StringComparison.Ordinal);

        var badAuthor = await feedback.RecordAsync(Feedback(request, "wrong", "bad author!"), ct);
        Assert.Equal(InvestigationFeedbackOutcome.Refused, badAuthor.Outcome);

        Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_feedback", ct));

        /* A request with no report, and one that does not exist, are told apart. */
        var open = (await store.OpenOrJoinAsync(Alert(DateTime.UtcNow.AddHours(-1), "resources"), ct)).RequestId!.Value;
        Assert.Equal(InvestigationFeedbackOutcome.NoReport, (await feedback.RecordAsync(Feedback(open, "right", "erik"), ct)).Outcome);
        Assert.Equal(InvestigationFeedbackOutcome.NotFound, (await feedback.RecordAsync(Feedback(Guid.NewGuid(), "right", "erik"), ct)).Outcome);
        Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_feedback", ct));
    }

    [Fact]
    public async Task ASecondRecordByTheSameAuthor_InAnyLetterCase_ReplacesTheFirst_AndAnotherAuthorAddsOne()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation feedback live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = await MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var feedback = new InvestigationFeedbackStore(owner, 30);
        var request = await ReportedAsync(store, DateTime.UtcNow.AddHours(-2), "blocking", "real", ct);

        var first = await feedback.RecordAsync(Feedback(request, "partly", "Erik", missed: "check the lead blocker", comment: "first"), ct);
        Assert.Equal(InvestigationFeedbackOutcome.Recorded, first.Outcome);
        var second = await feedback.RecordAsync(Feedback(request, "wrong", "erik", corrected: "false_positive", comment: "second"), ct);
        Assert.Equal(InvestigationFeedbackOutcome.Replaced, second.Outcome);

        var rows = await feedback.ReadAsync(new[] { request }, ct);
        var only = Assert.Single(rows);
        Assert.Equal("wrong", only.Verdict);
        Assert.Equal("false_positive", only.CorrectedVerdict);
        Assert.Null(only.MissedStep);
        Assert.Equal("second", only.Comment);
        Assert.Equal("erik", only.Author);
        Assert.Equal(DateTimeKind.Utc, only.CreatedAtUtc.Kind);

        Assert.Equal(InvestigationFeedbackOutcome.Recorded, (await feedback.RecordAsync(Feedback(request, "right", "dana"), ct)).Outcome);
        Assert.Equal(2, (await feedback.ReadAsync(new[] { request }, ct)).Count);
        Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_feedback", ct));
    }

    [Fact]
    public async Task Feedback_ChangesTheFalsePositiveCounts_OfThatAlertFamilyOnly_InBothDirections()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation feedback live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = await MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var feedback = new InvestigationFeedbackStore(owner, 30);
        var since = DateTime.UtcNow.AddDays(-1);

        var blockingReal = await ReportedAsync(store, DateTime.UtcNow.AddHours(-6), "blocking", "real", ct);
        var resourcesFalse = await ReportedAsync(store, DateTime.UtcNow.AddHours(-5), "resources", "false_positive", ct);
        await ReportedAsync(store, DateTime.UtcNow.AddHours(-4), "resources", "real", ct);

        async Task<InvestigationFamilyCounts> CountsOf(string family) =>
            (await feedback.FamilyCountsAsync(since, null, ct)).Single(c => c.Family == family);

        /* Before any feedback the counts are the reports' own. */
        Assert.Equal(new InvestigationFamilyCounts("blocking", 1, 0, 0, 0, 0), await CountsOf("blocking"));
        Assert.Equal(new InvestigationFamilyCounts("resources", 2, 0, 0, 1, 1), await CountsOf("resources"));

        /* A person says the blocking report was wrong and the alert was a false positive: that family's count rises, the other's does not. */
        await feedback.RecordAsync(Feedback(blockingReal, "wrong", "erik", corrected: "false_positive"), ct);
        Assert.Equal(new InvestigationFamilyCounts("blocking", 1, 1, 1, 0, 1), await CountsOf("blocking"));
        Assert.Equal(new InvestigationFamilyCounts("resources", 2, 0, 0, 1, 1), await CountsOf("resources"));

        /* Another person marks a false_positive report right (no correction): the settled verdict is the report's, so the count stays. */
        await feedback.RecordAsync(Feedback(resourcesFalse, "right", "erik"), ct);
        Assert.Equal(new InvestigationFamilyCounts("resources", 2, 1, 0, 1, 1), await CountsOf("resources"));

        /* Correcting that false_positive report to real lowers the count; the agent's own count is unchanged. */
        await feedback.RecordAsync(Feedback(resourcesFalse, "wrong", "erik", corrected: "real"), ct);
        Assert.Equal(new InvestigationFamilyCounts("resources", 2, 1, 1, 1, 0), await CountsOf("resources"));

        /* A replacement without the correction puts the report's verdict back. */
        await feedback.RecordAsync(Feedback(blockingReal, "partly", "ERIK"), ct);
        Assert.Equal(new InvestigationFamilyCounts("blocking", 1, 1, 0, 0, 0), await CountsOf("blocking"));

        /* The window is by when the request opened: a window that starts after them holds none. */
        Assert.Empty(await feedback.FamilyCountsAsync(DateTime.UtcNow.AddMinutes(1), null, ct));
    }

    [Fact]
    public async Task DeletingARequest_DeletesItsFeedback_AndTheReportsReadCarriesFeedbackAndCounts()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation feedback live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = await MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var feedback = new InvestigationFeedbackStore(owner, 30);
        var kept = await ReportedAsync(store, DateTime.UtcNow.AddHours(-6), "blocking", "real", ct);
        var doomed = await ReportedAsync(store, DateTime.UtcNow.AddHours(-5), "resources", "real", ct);
        await feedback.RecordAsync(Feedback(kept, "wrong", "erik", corrected: "false_positive", missed: "look at the head blocker", comment: "never real"), ct);
        await feedback.RecordAsync(Feedback(doomed, "right", "erik"), ct);

        /* A window with no requests still answers as a store that keeps feedback: empty lists, never null (a null reads as a store that keeps none). */
        var none = await store.ReadReportsAsync(null, null, DateTime.UtcNow.AddMinutes(1), 100, 200, ct);
        Assert.Empty(none.Requests);
        Assert.Empty(none.Feedback!);
        Assert.Empty(none.FamilyCounts!);
        Assert.Null((await store.ReadReportsAsync(kept, null, DateTime.UtcNow.AddMinutes(1), 100, 200, ct)).FamilyCounts);

        /* The reports list: each report carries its feedback, and the window read carries the counts. */
        var read = await store.ReadReportsAsync(null, null, DateTime.UtcNow.AddDays(-1), 100, 200, ct);
        Assert.Equal(2, read.Feedback!.Count);
        var answer = JsonNode.Parse(InvestigationToolCore.ReportsAnswer(read, 24, McpDataDetail.Full))!;
        var keptReport = answer["reports"]!.AsArray().Single(r => (string)r!["request_id"]! == kept.ToString())!;
        var keptFeedback = Assert.Single(keptReport["feedback"]!.AsArray())!;
        Assert.Equal("wrong", (string)keptFeedback["verdict"]!);
        Assert.Equal("false_positive", (string)keptFeedback["corrected_verdict"]!);
        Assert.Equal("never real", (string)keptFeedback["comment"]!);
        Assert.Equal("erik", (string)keptFeedback["author"]!);
        var blocking = answer["family_counts"]!.AsArray().Single(c => (string)c!["family"]! == "blocking")!;
        Assert.Equal(1, (int)blocking["false_positives"]!);
        Assert.Equal(0, (int)blocking["agent_false_positives"]!);

        /* One request by id reads its feedback and no window counts. */
        var one = await store.ReadReportsAsync(kept, null, DateTime.UtcNow, 100, 200, ct);
        Assert.Single(one.Feedback!);
        Assert.Null(one.FamilyCounts);

        /* Retention deletes a request with a DELETE; its feedback goes with it. */
        await ExecAsync(owner, $"DELETE FROM config.investigation_requests WHERE request_id = '{doomed}'", ct);
        Assert.Equal("0", await ScalarAsync(owner, $"SELECT count(*) FROM config.investigation_feedback WHERE request_id = '{doomed}'", ct));
        Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_feedback", ct));
        Assert.Empty(await feedback.ReadAsync(new[] { doomed }, ct));
    }

    /// <summary>Runs <paramref name="body"/> against the web routes (a test server over the endpoints) with the web host's data
    /// source logging in as a role holding exactly what the managed <paramref name="managedRole"/> holds, on a scratch store with
    /// one reported request. The body gets the owner source, the reported request, and a sender for the routes.</summary>
    internal static Task WithWebRoutesAsync(
        string managedRole, Func<NpgsqlDataSource, Guid, Func<string, string, string?, string?, Task<(int Status, JsonNode? Body)>>, CancellationToken, Task> body,
        DarlingWebSeat? seat = null, AdminArmShape arm = AdminArmShape.None, bool managedStore = false) =>
        WithRawWebRoutesAsync(managedRole, seat, McpDataDetail.Full, null, (owner, request, sendRaw, ct) => body(owner, request,
            async (method, path, contentType, payload) =>
            {
                var (status, bytes) = await sendRaw(method, path, contentType, payload);
                var text = Encoding.UTF8.GetString(bytes);
                return (status, string.IsNullOrWhiteSpace(text) ? null : JsonNode.Parse(text));
            }, ct), arm: arm, managedStore: managedStore);

    /// <summary>The same harness with the answer's raw bytes. <paramref name="seat"/> is what the sign-in middleware would have stored for the
    /// request (null: no sign-in is configured, so nothing is stored); <paramref name="dataDetail"/> is the level the dashboard started with;
    /// <paramref name="configureStore"/> runs on the scratch store before the routes start.</summary>
    internal static async Task WithRawWebRoutesAsync(
        string managedRole, DarlingWebSeat? seat, McpDataDetail dataDetail, Func<NpgsqlDataSource, Task>? configureStore,
        Func<NpgsqlDataSource, Guid, Func<string, string, string?, string?, Task<(int Status, byte[] Raw)>>, CancellationToken, Task> body,
        InvestigationExportGate? exportGate = null, ReadAdmission? readAdmission = null,
        Action<Microsoft.AspNetCore.Http.HttpContext>? onRequest = null, AdminArmShape arm = AdminArmShape.None, bool managedStore = false)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation feedback live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "fb_seat_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = await MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var request = await ReportedAsync(store, DateTime.UtcNow.AddHours(-3), "blocking", "real", ct);

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
            if (seat is not null)
            {
                app.Use(async (http, next) =>
                {
                    http.Items[DarlingWebSeat.HttpContextItemKey] = seat;
                    await next(http);
                });
            }

            var ownerSource = new StoreOwnerSource();
            ownerSource.Publish(owner);
            using var keys = new PseudonymKeySource(ownerSource);
            DarlingWebEndpoints.MapAll(app, webLogin, new CollectorRuntimeState(), new CapturingTestLogger(), dataDetail: dataDetail, pseudonymKeys: keys,
                readAdmission: readAdmission, postgresConfig: managedStore ? new PostgresConfig { Managed = true } : null, adminArm: adminArm, investigationExportGate: exportGate ?? new InvestigationExportGate(Path.Combine(Path.GetTempPath(), "darling-export-test-" + Guid.NewGuid().ToString("N"))));
            if (configureStore is not null)
            {
                await configureStore(owner);
            }

            await app.StartAsync(ct);
            using var server = app.GetTestServer();

            async Task<(int Status, byte[] Raw)> Send(string method, string path, string? contentType, string? payload)
            {
                var context = await server.SendAsync(http =>
                {
                    http.Request.Method = method;
                    var split = path.IndexOf('?', StringComparison.Ordinal);
                    http.Request.Path = split < 0 ? path : path[..split];
                    http.Request.QueryString = new Microsoft.AspNetCore.Http.QueryString(split < 0 ? "" : path[split..]);
                    http.Request.Headers.Host = "localhost";
                    onRequest?.Invoke(http);
                    if (contentType is not null)
                    {
                        http.Request.ContentType = contentType;
                    }

                    http.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(payload ?? ""));
                });
                using var bytes = new MemoryStream();
                await context.Response.Body.CopyToAsync(bytes, ct);
                return (context.Response.StatusCode, bytes.ToArray());
            }

            await body(owner, request, Send, ct);
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
    public async Task TheWebRecordsFeedback_AsTheManagedAdminRole_ReplacesOnASecondRecord_AndRefusesBadInput()
    {
        await WithWebRoutesAsync("admin", async (owner, request, Send, ct) =>
        {
            var route = "/api/investigation-feedback/" + request;
            const string Json = "application/json";

            Assert.Equal(415, (await Send("PUT", route, null, "{\"verdict\":\"right\"}")).Status);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_feedback", ct));

            var recorded = await Send("PUT", route, Json, "{\"verdict\":\"wrong\",\"corrected_verdict\":\"false_positive\",\"missed_step\":\"check jobs\",\"comment\":\"first\"}");
            Assert.Equal(200, recorded.Status);
            Assert.Equal("recorded", (string)recorded.Body!["status"]!);
            Assert.Equal("web", (string)recorded.Body["author"]!);

            var replaced = await Send("PUT", route, Json, "{\"verdict\":\"partly\",\"comment\":\"second\"}");
            Assert.Equal(200, replaced.Status);
            Assert.Equal("replaced", (string)replaced.Body!["status"]!);
            Assert.Equal("partly|second|1", await ScalarAsync(owner,
                "SELECT verdict || '|' || comment || '|' || (SELECT count(*) FROM config.investigation_feedback) FROM config.investigation_feedback", ct));

            var badVerdict = await Send("PUT", route, Json, "{\"verdict\":\"great\"}");
            Assert.Equal(400, badVerdict.Status);
            Assert.Contains("right, partly or wrong", (string)badVerdict.Body!["error"]!, StringComparison.Ordinal);
            Assert.Equal(400, (await Send("PUT", route, Json, "[]")).Status);
            Assert.Equal(400, (await Send("PUT", route, Json, "{\"verdict\":5}")).Status);
            Assert.Equal(404, (await Send("PUT", "/api/investigation-feedback/" + Guid.NewGuid(), Json, "{\"verdict\":\"right\"}")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/investigation-feedback/not-a-guid", Json, "{\"verdict\":\"right\"}")).Status);

            var list = await Send("GET", "/api/investigation-feedback", null, null);
            Assert.Equal(200, list.Status);
            var listed = Assert.Single(list.Body!["requests"]!.AsArray())!;
            Assert.True((bool)list.Body["can_record"]!, "the admin login holds INSERT and UPDATE, so the page may show its form");
            Assert.Equal(request.ToString(), (string)listed["request_id"]!);
            Assert.Equal("real", (string)listed["report_verdict"]!);
            var shown = Assert.Single(listed["feedback"]!.AsArray())!;
            Assert.Equal("partly", (string)shown["verdict"]!);
            Assert.Equal("second", (string)shown["comment"]!);
            var family = Assert.Single(list.Body["families"]!.AsArray())!;
            Assert.Equal("blocking", (string)family["family"]!);
            Assert.Equal(1, (int)family["with_feedback"]!);
            Assert.Equal(400, (await Send("GET", "/api/investigation-feedback?hours_back=0", null, null)).Status);
        });
    }

    /* #6600: the managed store's shape. The web reads as the viewer role; a signed-in admin's record and its capability probe go through the arm,
       so the page may show its form, and the author is the person. */
    [Fact]
    public async Task OnTheManagedStore_ASignedInAdminSeat_RecordsThroughTheArm_AndCanRecordIsTrue()
    {
        await WithWebRoutesAsync("viewer", async (owner, request, Send, ct) =>
        {
            var list = await Send("GET", "/api/investigation-feedback", null, null);
            Assert.Equal(200, list.Status);
            Assert.True((bool)list.Body!["can_record"]!);

            var recorded = await Send("PUT", "/api/investigation-feedback/" + request, "application/json", "{\"verdict\":\"wrong\",\"comment\":\"via the arm\"}");
            Assert.Equal(200, recorded.Status);
            Assert.Equal("alice", (string)recorded.Body!["author"]!);
            Assert.Equal("wrong|via the arm|alice", await ScalarAsync(owner, "SELECT verdict || '|' || comment || '|' || author FROM config.investigation_feedback", ct));
        }, new DarlingWebSeat("alice", true), AdminArmShape.Admin, managedStore: true);
    }

    /* #6600: an OIDC viewer seat never opens the arm (its login is refused here, so opening it would fail): the list says it cannot record and a
       record answers 403 as today. */
    [Fact]
    public async Task OnTheManagedStore_AViewerSeat_NeverOpensTheArm_AndAnswersAsToday()
    {
        await WithWebRoutesAsync("viewer", async (owner, request, Send, ct) =>
        {
            Assert.False((bool)(await Send("GET", "/api/investigation-feedback", null, null)).Body!["can_record"]!);
            var refused = await Send("PUT", "/api/investigation-feedback/" + request, "application/json", "{\"verdict\":\"wrong\"}");
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.FeedbackRefusedMessage(true), (string)refused.Body!["error"]!);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_feedback", ct));
        }, new DarlingWebSeat("viewer-person", false), AdminArmShape.Refused, managedStore: true);
    }

    /* #6600: with the switch off (no arm on a managed store) the signed-in admin gets today's 403, which now names the switch. */
    [Fact]
    public async Task OnTheManagedStore_WithTheSwitchOff_TheSignedInAdminGets403_NamingTheSwitch()
    {
        await WithWebRoutesAsync("viewer", async (owner, request, Send, ct) =>
        {
            Assert.False((bool)(await Send("GET", "/api/investigation-feedback", null, null)).Body!["can_record"]!);
            var refused = await Send("PUT", "/api/investigation-feedback/" + request, "application/json", "{\"verdict\":\"wrong\"}");
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.FeedbackRefusedMessage(true, adminWritesOff: true), (string)refused.Body!["error"]!);
            Assert.Contains("web.allowAdminWrites", (string)refused.Body["error"]!, StringComparison.Ordinal);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_feedback", ct));
        }, new DarlingWebSeat("alice", true), managedStore: true);
    }

    [Fact]
    public async Task TheWebRoutes_AsTheManagedViewerRole_ListFeedback_ButRefuseARecord_With403_AndWriteNothing()
    {
        await WithWebRoutesAsync("viewer", async (owner, request, Send, ct) =>
        {
            var answer = await Send("PUT", "/api/investigation-feedback/" + request, "application/json", "{\"verdict\":\"wrong\"}");
            Assert.Equal(403, answer.Status);
            Assert.Equal(DarlingWebEndpoints.FeedbackRefusedMessage(false), (string)answer.Body!["error"]!);
            Assert.Contains("Viewer", (string)answer.Body["error"]!, StringComparison.Ordinal);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_feedback", ct));

            /* An owner-recorded feedback is listed under the viewer login. */
            await new InvestigationFeedbackStore(owner, 30).RecordAsync(Feedback(request, "right", "dana"), ct);
            var list = await Send("GET", "/api/investigation-feedback", null, null);
            Assert.Equal(200, list.Status);
            Assert.Equal("right", (string)Assert.Single(Assert.Single(list.Body!["requests"]!.AsArray())!["feedback"]!.AsArray())!["verdict"]!);
            /* The list says the viewer login cannot record, so the page shows no form (the promise made before the click). */
            Assert.False((bool)list.Body["can_record"]!);
        });
    }
}
