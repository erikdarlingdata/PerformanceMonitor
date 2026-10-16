/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
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
            {"count":2,"notes":[
              {"note_id":7,"server_id":3,"server_name":"srv-a","metric_name":"Blocking","family":"blocking","story_path_hash":"ABCDEF0123456789","note_text":"the 02:00 ETL blocks","author":"agent:agent-a","source_request_id":"0f8fad5b-d9cb-469f-a165-70867728950e","proposed_at_utc":"2026-10-15T01:02:03.0000000Z"},
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
        Assert.Equal("Fleet", rows[1].GetProperty("server_name").GetString());
        Assert.Equal(JsonValueKind.Null, rows[1].GetProperty("request").ValueKind);
        Assert.Empty(Pure("pendingNoteRows", JsonNode.Parse("""{"count":0,"notes":[]}""")).EnumerateArray());
        Assert.Empty(Pure("pendingNoteRows", (object?)null).EnumerateArray());
    }

    [Theory]
    [InlineData("approve", 200, """{"status":"approved","note_id":7}""", "Note 7 approved.", false, true)]
    [InlineData("reject", 200, """{"status":"rejected","note_id":7}""", "Note 7 rejected.", false, true)]
    [InlineData("approve", 404, """{"error":"That note is not pending: it was already decided, or it is gone."}""",
        "Note 7 was not changed. That note is not pending: it was already decided, or it is gone.", true, true)]
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

    [Fact]
    public void OnlyASeatThatCanEdit_GetsTheDecideColumn_Pure()
    {
        var readOnly = Pure("noteColumns", false).EnumerateArray().Select(c => c.GetProperty("key").GetString()).ToArray();
        var editor = Pure("noteColumns", true).EnumerateArray().Select(c => c.GetProperty("key").GetString()).ToArray();

        Assert.DoesNotContain("decide", readOnly);
        Assert.Equal(readOnly.Concat(new[] { "decide" }), editor);
        Assert.Contains("note_text", readOnly);
    }

    [Fact]
    public void ADecisionIsStampedAsAPerson_TheSignedInSubjectOrViewer()
    {
        Assert.Equal("person:viewer", DarlingWebEndpoints.NoteDecider(new DarlingWebSeat(null, true)));
        Assert.Equal("person:viewer", DarlingWebEndpoints.NoteDecider(new DarlingWebSeat("  ", true)));
        Assert.Equal("person:erik@example.com", DarlingWebEndpoints.NoteDecider(new DarlingWebSeat(" erik@example.com ", true)));
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
