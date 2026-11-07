/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The web Investigations page (#5904), run in Node against a recording fetch: what it asks the two reads for, the list in
/// the order the read gave, a picked request's report, stored brief and journal, markup in the data drawn as text, the
/// poll's rebuild keeping the open request, and the empty, error and truncation lines. Behavior only; the spelling of
/// the page's source is not pinned.
/// </summary>
public sealed class InvestigationsPageTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "investigations-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the investigations harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the investigations harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void TheListReadsTheRequestsInTheOrderTheReadGave_WithTheViewerColumns()
    {
        var first = Run().GetProperty("first");
        var call = Assert.Single(first.GetProperty("listCalls").EnumerateArray());
        Assert.Equal("100", call.GetProperty("limit").GetString());
        Assert.False(call.TryGetProperty("state", out _));
        Assert.Empty(first.GetProperty("reportCalls").EnumerateArray());
        Assert.Equal("Opened,State,Server,Metric,Family,Alerts,Agent,Lease until,Reported,Route", string.Join(",", Strings(first.GetProperty("headers"))));
        var rows = first.GetProperty("rows").EnumerateArray().Select(Strings).ToArray();
        Assert.Equal(new[] { "srv-new", "srv-old" }, rows.Select(r => r[2]));
        Assert.Equal(new[] { "reported", "open" }, rows.Select(r => r[1]));
        Assert.Equal("agent-a", rows[0][6]);
        Assert.Equal(",open,claimed,reported,closed,expired", string.Join(",", Strings(first.GetProperty("stateChoices"))));
    }

    [Fact]
    public void PickingARequest_ReadsThatOneRequest_AndDrawsItsReportBriefAndJournal_AsText()
    {
        var picked = Run().GetProperty("picked");
        var read = Assert.Single(picked.GetProperty("reportCalls").EnumerateArray());
        Assert.Equal("11111111-1111-1111-1111-111111111111", read.GetProperty("request_id").GetString());
        var text = picked.GetProperty("text").GetString()!;
        Assert.Contains("Verdict: needs_person", text);
        Assert.Contains("agent-a", text);
        Assert.Contains("unreviewed and self-attributed", text);
        Assert.Contains("Stored brief (data detail: maskLiterals)", text);
        // Markup in the stored data is drawn as its characters, never built into elements.
        Assert.Contains("SUMMARY <img src=x onerror=alert(1)>", text);
        Assert.Contains("<script>x</script>", text);
        Assert.Equal(0, picked.GetProperty("createdImgOrScript").GetInt32());
        // The journal keeps the read's order (newest first). The claim time's mapping (the claimed event, not the row) is pinned zone-independently by claimedAt below; the rendered text depends on the runner's zone, so it is not asserted.
        var journal = picked.GetProperty("journalRows").EnumerateArray().Select(Strings).ToArray();
        Assert.Equal(new[] { "reported", "claimed", "opened" }, journal.Select(r => r[1]));
    }

    /// <summary>A seat that can edit but has no signed-in admin (a dashboard with no sign-in, or a signed-in viewer's twin) is told to
    /// sign in, and is given no Close form: the service would answer 403 to it.</summary>
    [Fact]
    public void ASeatThatCanEditButIsNotSignedInAsAnAdmin_SeesTheSignInSentence_InsteadOfTheCloseForm()
    {
        var picked = Run().GetProperty("picked");
        var lines = Strings(picked.GetProperty("signInLines"));
        Assert.Equal(new[] { "Sign in to the web as an admin to make this change, or use the Viewer." }, lines);
        Assert.Equal(0, picked.GetProperty("closeInputs").GetInt32());
        Assert.Equal(0, picked.GetProperty("closeButtons").GetInt32());
    }

    /// <summary>#7700: a signed-in admin on a store with web.allowAdminWrites off gets no Close form (the service refuses it), and the
    /// section says why; with the switch on the form is back.</summary>
    [Fact]
    public void WithTheSwitchOff_TheCloseForm_IsNotDrawn_AndTheSectionNamesTheSwitch()
    {
        var run = Run();
        var off = run.GetProperty("writesOff");
        Assert.Equal(0, off.GetProperty("closeInputs").GetInt32());
        Assert.Equal(0, off.GetProperty("closeButtons").GetInt32());
        Assert.Equal(0, off.GetProperty("signInLines").GetInt32());
        var line = Assert.Single(Strings(off.GetProperty("offLines")));
        Assert.Contains("set web.allowAdminWrites to true in darling.json", line, StringComparison.Ordinal);
        Assert.Equal(1, run.GetProperty("writesOn").GetProperty("closeInputs").GetInt32());
    }

    /// <summary>#7700: the single-request view (#/investigations?request=id) says the same, for a signed-in admin and for a seat that is
    /// not signed in (the switch comes first: signing in would not help, so the sign-in sentence is not shown).</summary>
    [Theory]
    [InlineData("oneWritesOff")]
    [InlineData("oneWritesOffSignedOut")]
    public void TheSingleRequestView_WithTheSwitchOff_NamesTheSwitch_AndDrawsNoCloseForm_AndNoSignInSentence(string phase)
    {
        var off = Run().GetProperty(phase);
        Assert.Equal(0, off.GetProperty("closeInputs").GetInt32());
        Assert.Equal(0, off.GetProperty("closeButtons").GetInt32());
        Assert.Equal(0, off.GetProperty("signInLines").GetInt32());
        var line = Assert.Single(Strings(off.GetProperty("offLines")));
        Assert.Contains("set web.allowAdminWrites to true in darling.json", line, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRebuild_KeepsTheOpenRequest_AndCloseEmptiesThePane()
    {
        var run = Run();
        var rebuilt = run.GetProperty("rebuilt");
        Assert.True(rebuilt.GetProperty("hasDetail").GetBoolean());
        Assert.Single(rebuilt.GetProperty("reportCalls").EnumerateArray());
        Assert.Equal("", run.GetProperty("closed").GetString());
    }

    /// <summary>#7956: the poll's rebuild of the page draws the Close form again with the reason as typed so far, in one box, and a
    /// successful close sends that reason and clears it.</summary>
    [Fact]
    public void TheRebuild_KeepsTheCloseReasonBeingTyped_AndASuccessfulClose_SendsAndClearsIt()
    {
        var run = Run();
        var draft = run.GetProperty("draft");
        Assert.Equal("The blocker was a one-off import", draft.GetProperty("kept").GetString());
        Assert.Equal(1, draft.GetProperty("boxes").GetInt32());
        var closing = run.GetProperty("closing");
        Assert.Equal("The blocker was a one-off import", closing.GetProperty("reasonSent").GetString());
        Assert.Equal("", closing.GetProperty("draftAfterClose").GetString());
    }

    /// <summary>#7956: a close whose PUT is still out when the poll redraws the form: the new button is disabled, a press on it sends
    /// nothing, one PUT reaches the service, and the button is enabled again once the service has answered.</summary>
    [Fact]
    public void ACloseInFlight_KeepsTheRebuiltButtonDisabled_AndOnePutIsSent()
    {
        var closing = Run().GetProperty("closing");
        Assert.Equal("/api/investigation-requests/11111111-1111-1111-1111-111111111111/close", closing.GetProperty("url").GetString());
        Assert.Equal(1, closing.GetProperty("putsBeforeReply").GetInt32());
        Assert.True(closing.GetProperty("rebuiltDisabled").GetBoolean());
        Assert.Equal(1, closing.GetProperty("putsAfterSecondPress").GetInt32());
        Assert.Equal(1, closing.GetProperty("putsAfterReply").GetInt32());
        Assert.True(closing.GetProperty("buttonEnabledAfter").GetBoolean());
    }

    /// <summary>#7957: a Close under a State filter. The closed request no longer matches the filter, so the re-read's list does not hold
    /// it, and the pane that showed it is emptied: with another request still listed, with the read answering empty, and with the read
    /// answering no rows. One PUT goes out and the list is read again with the filter.</summary>
    [Theory]
    [InlineData("closedUnderFilter", "srv-third", false)]
    [InlineData("closedLastUnderFilter", "", true)]
    [InlineData("closedNoRowsUnderFilter", "", false)]
    public void ACloseUnderAStateFilter_EmptiesThePane_WhenTheClosedRequestLeftTheList(string phase, string rowsLeft, bool emptyLine)
    {
        var closed = Run().GetProperty(phase);
        Assert.Contains("Request on srv-old", closed.GetProperty("paneBefore").GetString());
        Assert.Equal(1, closed.GetProperty("puts").GetInt32());
        Assert.Equal("/api/investigation-requests/22222222-2222-2222-2222-222222222222/close", closed.GetProperty("closedUrl").GetString());
        var read = Assert.Single(closed.GetProperty("listCalls").EnumerateArray());
        Assert.Equal("open", read.GetProperty("state").GetString());
        Assert.Equal(rowsLeft, string.Join(",", Strings(closed.GetProperty("rows"))));
        Assert.Equal(emptyLine, closed.GetProperty("emptyLine").GetBoolean());
        Assert.Equal("", closed.GetProperty("pane").GetString());
    }

    /// <summary>#8007: request A's detail read is still out when A is dropped, by a State pick and by a list re-read that no longer holds
    /// it. The read then answers: the pane stays empty, no Close form is drawn for A, and the read was aborted when A was dropped.</summary>
    [Theory]
    [InlineData("droppedByFilter")]
    [InlineData("droppedByReRead")]
    public void ADetailReadStillOutWhenItsRequestIsDropped_IsAborted_AndDoesNotRedrawThePane(string phase)
    {
        var dropped = Run().GetProperty(phase);
        Assert.Contains("Loading this request", dropped.GetProperty("paneWhileOut").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, dropped.GetProperty("detailReads").GetInt32());
        Assert.True(Assert.Single(dropped.GetProperty("abortedBeforeAnswer").EnumerateArray()).GetBoolean(), "the detail read still out was not aborted when its request was dropped");
        Assert.Equal("", dropped.GetProperty("pane").GetString());
        Assert.Equal(0, dropped.GetProperty("closeButtons").GetInt32());
    }

    /// <summary>#8007: request A's detail read has answered, but the session read the pane waits on with it is still out, when a State pick
    /// drops A. The fetch is past aborting, so only the detail ticket stops the answer: once the session read answers, the pane is still empty.</summary>
    [Fact]
    public void ADetailReadThatAnsweredBeforeItsRequestWasDropped_WhileTheSessionReadIsStillOut_DoesNotRedrawThePane()
    {
        var dropped = Run().GetProperty("droppedWhileSessionOut");
        Assert.Equal(1, dropped.GetProperty("detailReads").GetInt32());
        Assert.Equal(1, dropped.GetProperty("answeredBeforeDrop").GetInt32());
        Assert.Contains("Loading this request", dropped.GetProperty("paneWhileSessionOut").GetString(), StringComparison.Ordinal);
        Assert.Equal("", dropped.GetProperty("paneAfterDrop").GetString());
        Assert.Equal("", dropped.GetProperty("pane").GetString());
    }

    /// <summary>#7987: request A's close is out when request B is picked and its reason typed. A's answer re-reads the list, and B's form
    /// comes back with the reason as typed: only the closed request's own draft is dropped.</summary>
    [Fact]
    public void AnotherRequestsCloseSucceeding_KeepsTheReasonBeingTyped()
    {
        var other = Run().GetProperty("otherClose");
        Assert.Equal("/api/investigation-requests/11111111-1111-1111-1111-111111111111/close", other.GetProperty("closedUrl").GetString());
        Assert.Equal(1, other.GetProperty("puts").GetInt32());
        Assert.Equal("22222222-2222-2222-2222-222222222222", other.GetProperty("paneRequest").GetString());
        Assert.Equal("The CPU spike was one report run by hand", other.GetProperty("reasonKept").GetString());
    }

    /// <summary>#7987: a close the service refuses (409 with its sentence) after the poll redrew the form: the rebuilt button is enabled
    /// again with the sentence under it, the reason stays in the box, and the list is not re-read.</summary>
    [Fact]
    public void ACloseRefused_EnablesTheRebuiltButton_ShowsTheRefusal_AndKeepsTheReason()
    {
        var refused = Run().GetProperty("refused");
        Assert.True(refused.GetProperty("rebuiltDisabled").GetBoolean());
        Assert.Equal("The CPU spike was one report run by hand", refused.GetProperty("reasonSent").GetString());
        Assert.Equal(1, refused.GetProperty("puts").GetInt32());
        Assert.True(refused.GetProperty("buttonEnabledAfter").GetBoolean());
        Assert.Contains("This request was closed by someone else a moment ago.", refused.GetProperty("status").GetString(), StringComparison.Ordinal);
        Assert.Equal("The CPU spike was one report run by hand", refused.GetProperty("reasonKept").GetString());
        Assert.Equal(1, refused.GetProperty("listReads").GetInt32());
    }

    /// <summary>#7987: leaving the page while the reason box has the focus, then opening it again, keeps the reason and does not move
    /// the focus into the box. The poll's rebuild on the page still gives the box its focus back (#7956).</summary>
    [Fact]
    public void LeavingThePageWithTheReasonBoxFocused_DoesNotMoveTheFocusIntoTheBoxOnReturn()
    {
        var left = Run().GetProperty("leftFocused");
        Assert.Equal(1, left.GetProperty("pollRefocused").GetInt32());
        Assert.Equal(0, left.GetProperty("returnRefocused").GetInt32());
        Assert.Equal("The CPU spike was one report run by hand", left.GetProperty("reasonKept").GetString());
    }

    [Fact]
    public void AStateAndFamilyPick_AreSentToTheRead_AndDropTheOpenRequest()
    {
        var filtered = Run().GetProperty("filtered");
        var calls = filtered.GetProperty("listCalls").EnumerateArray().ToArray();
        Assert.Equal("claimed", calls[0].GetProperty("state").GetString());
        Assert.Equal("claimed", calls[1].GetProperty("state").GetString());
        Assert.Equal("blocking", calls[1].GetProperty("family").GetString());
        Assert.Empty(filtered.GetProperty("reportCalls").EnumerateArray());
        Assert.Equal("", filtered.GetProperty("pane").GetString());
    }

    [Fact]
    public void ARequestWithNoReport_SaysSo_AndTheEmptyErrorAndTruncationLinesAreDrawn()
    {
        var run = Run();
        var none = run.GetProperty("noReport").GetString()!;
        Assert.Contains("No agent has reported on this request.", none);
        Assert.Contains("never claimed", none);
        Assert.DoesNotContain("unreviewed", none);
        Assert.True(run.GetProperty("truncated").GetBoolean());
        Assert.True(run.GetProperty("empty").GetBoolean());
        Assert.True(run.GetProperty("error").GetBoolean());
        Assert.Equal("2026-10-14T10:05:00Z", run.GetProperty("claimedAt").GetString());
        Assert.Equal(JsonValueKind.Null, run.GetProperty("claimedAtNever").ValueKind);
        Assert.False(run.GetProperty("params").GetProperty("none").TryGetProperty("state", out _));
    }

    /// <summary>#6979: <c>#/investigations?request=&lt;id&gt;</c> opens one request on its own, with its report and its own agent changes only.</summary>
    [Fact]
    public void TheRequestRoute_OpensOneRequest_WithItsReportAndOnlyItsOwnAgentChanges()
    {
        var one = Run().GetProperty("one");
        Assert.Empty(one.GetProperty("listCalls").EnumerateArray());
        var read = Assert.Single(one.GetProperty("reportCalls").EnumerateArray());
        Assert.Equal("11111111-1111-1111-1111-111111111111", read.GetProperty("request_id").GetString());
        Assert.Equal(1, one.GetProperty("agentChangeReads").GetInt32());
        /* The page asks the service for this request's changes by id: it does not filter the newest 200 in the browser (#6979). */
        Assert.Equal("/api/agent-changes?request=11111111-1111-1111-1111-111111111111", Assert.Single(Strings(one.GetProperty("agentChangeUrls"))));
        var text = one.GetProperty("text").GetString()!;
        Assert.Contains("Request on srv-new", text);
        Assert.Contains("Verdict: needs_person", text);
        Assert.Contains("Agent changes (1)", text);
        Assert.Contains("set_config", text);
        Assert.DoesNotContain("other_tool", text);
        Assert.Contains("#/admin/agent-changes", Strings(one.GetProperty("links")));
        Assert.Equal(0, one.GetProperty("closeButtons").GetInt32());
    }

    [Fact]
    public void TheRequestRoute_SaysPlainlyWhenTheIdIsNotARequest()
    {
        var text = Run().GetProperty("oneMissing").GetString()!;
        Assert.Contains("No investigation request has id 99999999-9999-9999-9999-999999999999.", text);
        Assert.DoesNotContain("Verdict", text);
    }

    [Fact]
    public void TheRequestRoute_SaysSoWhenARequestHasNoAgentChange_AndStillLinksToTheTab()
    {
        var none = Run().GetProperty("oneNoChange");
        Assert.Contains("Request on srv-old", none.GetProperty("text").GetString());
        Assert.Contains("No agent change on this request", none.GetProperty("text").GetString());
        Assert.Contains("#/admin/agent-changes", Strings(none.GetProperty("links")));
    }

    [Fact]
    public void TheRequestId_IsReadFromTheHashQuery_AndBlankWhenThereIsNone()
    {
        var ids = Strings(Run().GetProperty("queryIds"));
        Assert.Equal(new[] { "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222", "", "", "" }, ids);
    }
}
