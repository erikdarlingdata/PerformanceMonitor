/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Admin page's Pending Notes tab (#5903), run under Node through the Admin page harness: the rows the list read
/// becomes, the words each answer to Approve or Reject gets, and who gets the buttons. Plus the route's pure pieces: who a
/// decision is stamped as, and the list body's shape.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class AdminPendingNotesTests
{
    private static JsonElement Pure(string fn, params object?[] args) =>
        AdminServerEditBehaviourTests.Run("pure", JsonSerializer.Serialize(new { fn, args })).GetProperty("result");

    [Fact]
    public void TheRows_NameTheServerFamilyAndProposer_AndAFleetNoteSaysFleet_Pure()
    {
        var data = JsonNode.Parse("""
            {"notes_returned":2,"truncated":false,"notes":[
              {"note_id":7,"server_id":3,"server_name":"srv-a","metric_name":"Blocking","family":"blocking","story_path_hash":"ABCDEF0123456789","note_text":"the 02:00 ETL blocks","author":"agent:agent-a","source_request_id":"0f8fad5b-d9cb-469f-a165-70867728950e","proposed_at_utc":"2026-10-15T01:02:03.0000000Z","expires_at_utc":"2026-12-30T00:00:00Z"},
              {"note_id":8,"server_id":null,"server_name":null,"metric_name":null,"family":null,"story_path_hash":null,"note_text":"a fleet-wide guess","author":"agent:agent-b","source_request_id":null,"proposed_at_utc":"2026-10-15T02:00:00.0000000Z"}
            ]}
            """);
        var rows = Pure("pendingNoteRows", data).EnumerateArray().ToArray();

        Assert.Equal(2, rows.Length);
        Assert.Equal("srv-a", rows[0].GetProperty("server_name").GetString());
        Assert.Equal("blocking", rows[0].GetProperty("family").GetString());
        Assert.Equal("ABCDEF01", rows[0].GetProperty("story").GetString());
        Assert.Equal("0f8fad5b", rows[0].GetProperty("request").GetString());
        Assert.Equal("the 02:00 ETL blocks", rows[0].GetProperty("note_text").GetString());
        Assert.Equal("agent:agent-a", rows[0].GetProperty("author").GetString());
        Assert.Equal("2026-12-30T00:00:00Z", rows[0].GetProperty("expires_at_utc").GetString());
        Assert.Equal("Fleet", rows[1].GetProperty("server_name").GetString());
        Assert.Equal(JsonValueKind.Null, rows[1].GetProperty("request").ValueKind);
        Assert.Empty(Pure("pendingNoteRows", JsonNode.Parse("""{"notes_returned":0,"notes":[]}""")).EnumerateArray());
        Assert.Empty(Pure("pendingNoteRows", (object?)null).EnumerateArray());
    }

    [Theory]
    [InlineData("approve", 200, """{"status":"approved","note_id":7}""", "Note 7 approved.", false, true)]
    [InlineData("reject", 200, """{"status":"rejected","note_id":7}""", "Note 7 rejected.", false, true)]
    [InlineData("approve", 404, """{"error":"That note is not pending: it was already decided, or it is gone."}""",
        "Note 7 was not changed. That note is not pending: it was already decided, or it is gone.", true, true)]
    [InlineData("approve", 403, """{"error":"Deciding a note needs a store login with the admin role. Managed and compose stores connect the web dashboard as the viewer role."}""",
        "Deciding a note needs a store login with the admin role. Managed and compose stores connect the web dashboard as the viewer role.", true, false)]
    [InlineData("reject", 403, null, "This sign-in is read-only, so it cannot reject notes.", true, false)]
    [InlineData("approve", 500, """{"error":"The store did not answer."}""",
        "Note 7 was not approved. The service answered 500. The store did not answer.", true, false)]
    [InlineData("approve", 200, """{"status":"rejected","note_id":7}""", "Note 7 was not approved. The service answered 200.", true, false)]
    public void EachAnswer_GetsPlainWords_AndOnlyAStaleListIsReadAgain_Pure(
        string verb, int status, string? body, string message, bool isError, bool reread)
    {
        var res = new { status, body = body is null ? null : JsonNode.Parse(body) };
        var outcome = Pure("noteDecisionOutcome", verb, 7, res);

        Assert.Equal(message, outcome.GetProperty("message").GetString());
        Assert.Equal(isError, outcome.GetProperty("isError").GetBoolean());
        Assert.Equal(reread, outcome.GetProperty("reread").GetBoolean());
    }

    [Fact]
    public void ANetworkFailure_ShowsItsMessage_AndAnExpiredSessionShowsNone_Pure()
    {
        var offline = Pure("noteDecisionOutcome", "approve", 7, new { status = 0, body = (object?)null, message = "Network error: offline" });
        Assert.Equal("Network error: offline", offline.GetProperty("message").GetString());
        Assert.True(offline.GetProperty("isError").GetBoolean());

        var expired = Pure("noteDecisionOutcome", "approve", 7, new { status = 401, body = (object?)null, expired = true });
        Assert.Equal(JsonValueKind.Null, expired.GetProperty("message").ValueKind);
    }

    /* An empty Write note says "Enter the note's text." at once, as the service would, and sends no change: before, the click made a
       round trip, logged a 400 in the console and only then showed the sentence. */
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void WritingABlankNote_SaysSoWithoutSendingARequest_Pure(string noteText)
    {
        var run = AdminServerEditBehaviourTests.Run("pure", JsonSerializer.Serialize(new
        {
            fn = "writeNote",
            args = new object[] { new { server_id = "", metric_name = "", note_text = noteText, days = "90" } },
        }));
        var result = run.GetProperty("result");

        Assert.False(result.GetProperty("ok").GetBoolean());
        Assert.Equal("The note was not written. Enter the note's text.", result.GetProperty("message").GetString());
        Assert.Equal(0, run.GetProperty("writes").GetInt32());
    }

    /* An empty Add route says what the service says (a family or an exact metric name is needed) before anything is sent: the save
       shows an error from routePayload without a request. A route with a match builds its body. */
    [Fact]
    public void AddingARouteWithNoMatch_IsRefusedByTheFormBeforeARequest_Pure()
    {
        var blank = Pure("routePayload", new { id = (int?)null, match = "  ", perHour = "10", perDay = "50", lease = "30", expiry = "120", fallback = "", scope = "", level = "", resolutions = "", enabled = true });
        var named = Pure("routePayload", new { id = (int?)null, match = "blocking", perHour = "10", perDay = "50", lease = "30", expiry = "120", fallback = "", scope = "", level = "", resolutions = "", enabled = true });

        Assert.Equal("Choose a family or enter an exact metric name.", blank.GetProperty("error").GetString());
        Assert.False(blank.TryGetProperty("payload", out _));
        Assert.False(named.TryGetProperty("error", out _));
        Assert.Equal("blocking", named.GetProperty("payload").GetProperty("metric_match").GetString());
    }

    /* #6928: the Routes list names the severity filter in the desktop window's words (Both when the field is missing or unknown), the editor
       reads it back from a route and the save sends it, and a word the page does not know is sent as both. */
    [Fact]
    public void TheRoutesList_ShowsTheSeverityBesideTheScope_AndTheEditorRoundTripsIt_Pure()
    {
        var data = JsonNode.Parse("""
            {"routes":[
              {"route_id":1,"metric_match":"blocking","severities":"critical","enabled":true},
              {"route_id":2,"metric_match":"performance","severities":"warning","enabled":true},
              {"route_id":3,"metric_match":"reports","severities":"both","enabled":true},
              {"route_id":4,"metric_match":"agent-jobs","enabled":true},
              {"route_id":5,"metric_match":"waits","severities":"someday","enabled":true}
            ]}
            """);
        var rows = Pure("routeRows", data).EnumerateArray().ToArray();
        Assert.Equal(new[] { "Critical only", "Warning only", "Both", "Both", "Both" }, rows.Select(r => r.GetProperty("severities").GetString()));

        var columns = Pure("ROUTE_COLUMNS").EnumerateArray().Select(c => c.GetProperty("key").GetString()).ToList();
        Assert.Equal(columns.IndexOf("applies_to") + 1, columns.IndexOf("severities"));

        Assert.Equal("critical", Pure("entriesFromRoute", new { route_id = 1, metric_match = "blocking", severities = "critical" }).GetProperty("severities").GetString());
        Assert.Equal("both", Pure("entriesFromRoute", new { route_id = 1, metric_match = "blocking" }).GetProperty("severities").GetString());
        Assert.Equal("both", Pure("entriesFromRoute", (object?)null).GetProperty("severities").GetString());

        string Sent(string severities) => Pure("routePayload", new { id = (int?)null, match = "blocking", perHour = "10", perDay = "50", lease = "30", expiry = "120", fallback = "", scope = "", level = "", resolutions = "", enabled = true, severities })
            .GetProperty("payload").GetProperty("severities").GetString()!;
        Assert.Equal("warning", Sent("warning"));
        Assert.Equal("critical", Sent("critical"));
        Assert.Equal("both", Sent("both"));
        Assert.Equal("both", Sent("someday"));
    }

    /* #6979: the Routes list names the report notice setting (Off when the field is missing or unknown), the editor reads it back from a
       route, and the save sends it. A word the page does not know is sent as off, so it never turns a notice on. */
    [Fact]
    public void TheRoutesList_ShowsTheReportNotice_AndTheEditorRoundTripsIt_Pure()
    {
        var data = JsonNode.Parse("""
            {"routes":[
              {"route_id":1,"metric_match":"blocking","report_notice":"needs_person_or_change","enabled":true},
              {"route_id":2,"metric_match":"performance","report_notice":"all","enabled":true},
              {"route_id":3,"metric_match":"reports","report_notice":"off","enabled":true},
              {"route_id":4,"metric_match":"agent-jobs","enabled":true},
              {"route_id":5,"metric_match":"waits","report_notice":"someday","enabled":true}
            ]}
            """);
        var rows = Pure("routeRows", data).EnumerateArray().ToArray();
        Assert.Equal(new[] { "Needs a person or made a change", "All reports", "Off", "Off", "Off" }, rows.Select(r => r.GetProperty("report_notice").GetString()));

        var columns = Pure("ROUTE_COLUMNS").EnumerateArray().Select(c => c.GetProperty("key").GetString()).ToList();
        Assert.Equal(columns.IndexOf("severities") + 1, columns.IndexOf("report_notice"));

        Assert.Equal("all", Pure("entriesFromRoute", new { route_id = 1, metric_match = "blocking", report_notice = "all" }).GetProperty("reportNotice").GetString());
        Assert.Equal("off", Pure("entriesFromRoute", new { route_id = 1, metric_match = "blocking" }).GetProperty("reportNotice").GetString());
        Assert.Equal("off", Pure("entriesFromRoute", (object?)null).GetProperty("reportNotice").GetString());

        string Sent(string reportNotice) => Pure("routePayload", new { id = (int?)null, match = "blocking", perHour = "10", perDay = "50", lease = "30", expiry = "120", fallback = "", scope = "", level = "", resolutions = "", enabled = true, severities = "both", reportNotice })
            .GetProperty("payload").GetProperty("report_notice").GetString()!;
        Assert.Equal("needs_person_or_change", Sent("needs_person_or_change"));
        Assert.Equal("all", Sent("all"));
        Assert.Equal("off", Sent("off"));
        Assert.Equal("off", Sent("someday"));
    }

    /* #6979: the editor opens on a stored route with its report notice showing, and Save sends the choice made in the box. */
    [Fact]
    public void TheRouteEditor_ShowsTheStoredReportNotice_AndSavingSendsTheChosenOne()
    {
        var result = AdminServerEditBehaviourTests.Run("routeEdit:warning,all");

        Assert.Equal("needs_person_or_change", result.GetProperty("reportNoticeShown").GetString());
        var write = Assert.Single(result.GetProperty("writes").EnumerateArray().ToArray());
        Assert.Equal("all", write.GetProperty("body").GetProperty("report_notice").GetString());
    }

    /* #6979: the report notice box is disabled with its hint showing while the route's agent level is Inherit or Off, and enabled with the
       hint gone once a level is chosen. The hint must be hidden by display as well as by the hidden attribute, because the .mute-label
       rule would otherwise keep it on screen next to an enabled box. */
    [Fact]
    public void TheReportNoticeBox_IsDisabledWithItsHintUntilTheRouteHasAnAgentLevel()
    {
        var result = AdminServerEditBehaviourTests.Run("routeEdit:warning");

        foreach (var name in new[] { "noticeAtInherit", "noticeAtOff" })
        {
            var state = result.GetProperty(name);
            Assert.True(state.GetProperty("disabled").GetBoolean(), name);
            Assert.False(state.GetProperty("hidden").GetBoolean(), name);
            Assert.Equal("", state.GetProperty("display").GetString());
        }

        var investigate = result.GetProperty("noticeAtInvestigate");
        Assert.False(investigate.GetProperty("disabled").GetBoolean());
        Assert.True(investigate.GetProperty("hidden").GetBoolean());
        Assert.Equal("none", investigate.GetProperty("display").GetString());
    }

    /* #6936: Edit then Save on the Routes tab sends the version the form read (expected_modified_at), because the service refuses an edit
       without it with a 400. The test drives the page itself: it opens the editor on a stored Critical only route, picks Warning only,
       presses Save and reads the write the page made. */
    [Fact]
    public void SavingAnEditedRoute_SendsTheVersionTheFormRead_AndTheChosenSeverity()
    {
        var writes = AdminServerEditBehaviourTests.Run("routeEdit:warning").GetProperty("writes").EnumerateArray().ToArray();

        var write = Assert.Single(writes);
        Assert.Equal("/api/notification-routes/7", write.GetProperty("url").GetString());
        var body = write.GetProperty("body");
        Assert.Equal("2026-01-01T00:00:00.1234567Z", body.GetProperty("expected_modified_at").GetString());
        Assert.Equal("warning", body.GetProperty("severities").GetString());
        Assert.Equal("blocking", body.GetProperty("metric_match").GetString());
    }

    [Fact]
    public void OnlyASeatThatCanEdit_GetsTheDecideColumn_Pure()
    {
        var readOnly = Pure("noteColumns", false).EnumerateArray().Select(c => c.GetProperty("key").GetString()).ToArray();
        var editor = Pure("noteColumns", true).EnumerateArray().Select(c => c.GetProperty("key").GetString()).ToArray();

        Assert.DoesNotContain("decide", readOnly);
        Assert.Equal(readOnly.Concat(new[] { "decide" }), editor);
        Assert.Contains("note_text", readOnly);
    }

    /* #5903 round 1: the page acts on the answer's reread flag, not only on the flag being set. An approval and a 404 (the
       list is stale) read the list again; a 403 refusal and a 500 do not, and the person is told in the status line. */
    [Theory]
    [InlineData("approved", 2, "7|8", "8", "Note 7 approved.")]
    [InlineData("gone", 2, "7|8", "8", "Note 7 was not changed. That note is not pending: it was already decided, or it is gone.")]
    [InlineData("refused", 1, "7|8", "7|8", "Deciding a note needs a store login with the admin role.")]
    [InlineData("broken", 1, "7|8", "7|8", "Note 7 was not approved. The service answered 500. The store did not answer.")]
    [InlineData("signin", 1, "7|8", "7|8", "Sign in to the web as an admin to make this change, or use the Viewer.")]
    public void TheDecideButton_ReadsTheListAgainOnlyWhenTheAnswerSaysItIsStale(
        string kind, int listReadsAfter, string rowsBefore, string rowsAfter, string status)
    {
        var result = AdminServerEditBehaviourTests.Run("decide:" + kind);

        Assert.Equal(1, result.GetProperty("listReadsBefore").GetInt32());
        Assert.Equal(listReadsAfter, result.GetProperty("listReadsAfter").GetInt32());
        Assert.Equal(new[] { "/api/investigation-notes/7/approve" }, result.GetProperty("writes").EnumerateArray().Select(w => w.GetString()).ToArray());
        Assert.Equal(rowsBefore, string.Join("|", result.GetProperty("rowsBefore").EnumerateArray().Select(w => w.GetString())));
        Assert.Equal(rowsAfter, string.Join("|", result.GetProperty("rowsAfter").EnumerateArray().Select(w => w.GetString())));
        Assert.Equal(new[] { status }, result.GetProperty("status").EnumerateArray().Select(w => w.GetString()).ToArray());
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("practice")]
    public void ASeatThatCanEditButIsNotSignedIn_SeesTheList_TheSignInSentence_AndNoWriteControls(string tab)
    {
        var page = AdminServerEditBehaviourTests.Run("signedout:" + tab);

        Assert.True(page.GetProperty("sentence").GetBoolean());
        Assert.Equal(0, page.GetProperty("approveButtons").GetInt32());
        Assert.Equal(0, page.GetProperty("noteForm").GetInt32());
        Assert.Equal(0, page.GetProperty("practiceForm").GetInt32());
        Assert.Equal(0, page.GetProperty("writes").GetInt32());
    }

    [Fact]
    public void ACutList_TellsTheOperatorThatDecidingTheOldestRevealsMore_Pure()
    {
        var cut = Pure("pendingNotesTruncatedNotice", JsonNode.Parse("""{"notes_returned":200,"truncated":true,"notes":[]}"""));
        Assert.Equal("This list shows the oldest 200 pending notes and more are waiting. Approve or reject some and the next ones appear.", cut.GetString());
        Assert.Equal(JsonValueKind.Null, Pure("pendingNotesTruncatedNotice", JsonNode.Parse("""{"notes_returned":200,"truncated":false,"notes":[]}""")).ValueKind);
        Assert.Equal(JsonValueKind.Null, Pure("pendingNotesTruncatedNotice", (object?)null).ValueKind);
    }

    [Fact]
    public void ADecisionIsStampedAsAPerson_TheSignedInSubjectOrTheWebConstant()
    {
        /* #6265: the Viewer stamps person:<login>; the web's approve and reject stamp the same shape, not the bare name. */
        Assert.Equal("person:web", DarlingWebEndpoints.NoteDecider(new DarlingWebSeat(null, true)));
        Assert.Equal("person:erik@example.com", DarlingWebEndpoints.NoteDecider(new DarlingWebSeat("erik@example.com", true)));
    }

    /* #5903 round 1: a subject the store's one rule refuses answers a sentence the route turns into a 400, never a 500. */
    [Theory]
    [InlineData("erik@example.com", false)]
    [InlineData("128-chars", false)]
    [InlineData("129-chars", true)]
    [InlineData("   ", true)]
    public void ADecidedByTheStoreRefuses_IsNamedInPlainWords(string subject, bool refused)
    {
        var name = subject switch { "128-chars" => new string('x', 128), "129-chars" => new string('x', 129), _ => subject };
        var message = InvestigationNoteRules.ValidateDecidedBy(name);
        Assert.Equal(refused, message is not null);
        if (refused)
        {
            Assert.DoesNotContain("Exception", message, StringComparison.Ordinal);
        }
    }

    /* #5903 round 1: 201 planted rows. The route reads one past its cap, returns the first 200 and says the list was cut;
       200 rows are a whole list. The count is the page's, named so. */
    [Theory]
    [InlineData(201, 200, true)]
    [InlineData(200, 200, false)]
    [InlineData(0, 0, false)]
    public void TheListBody_AtTheCap_CutsToTheLimit_AndSaysSo(int planted, int returned, bool truncated)
    {
        var rows = Enumerable.Range(1, planted).Select(i => new PendingInvestigationNote(
            i, 3, "srv-a", "Blocking", "blocking", null, "note " + i, "agent:a", null,
            new DateTime(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2026, 12, 30, 0, 0, 0, DateTimeKind.Utc))).ToList();

        var body = DarlingWebEndpoints.PendingNotesBody(rows);

        Assert.Equal(DarlingWebEndpoints.PendingNotesLimit, 200);
        Assert.Equal(returned, body["notes_returned"]!.GetValue<int>());
        Assert.Equal(truncated, body["truncated"]!.GetValue<bool>());
        Assert.Null(body["count"]);
        var notes = body["notes"]!.AsArray();
        Assert.Equal(returned, notes.Count);
        if (returned > 0)
        {
            Assert.Equal(1L, notes[0]!["note_id"]!.GetValue<long>());
            Assert.Equal(returned, notes[^1]!["note_id"]!.GetValue<long>());
            Assert.Equal("2026-12-30T00:00:00.0000000Z", notes[0]!["expires_at_utc"]!.GetValue<string>());
        }
    }

    [Theory]
    [InlineData(true, "PUT", "/api/investigation-notes/7/approve", true)]
    [InlineData(true, "PUT", "/api/investigation-notes/7/reject", true)]
    [InlineData(false, "PUT", "/api/investigation-notes/7/approve", false)]
    [InlineData(false, "PUT", "/api/investigation-notes/7/reject", false)]
    [InlineData(false, "GET", "/api/investigation-notes/pending", true)]
    public void TheWriteGate_RefusesAReadOnlySeatTheDecisions_AndLetsItList(bool canEdit, string method, string path, bool expected) =>
        Assert.Equal(expected, DarlingWebSeat.IsRequestAllowed(new DarlingWebSeat("who", canEdit), method, path));
}
