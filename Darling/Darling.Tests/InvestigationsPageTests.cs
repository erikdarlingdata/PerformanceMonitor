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

    [Fact]
    public void TheRebuild_KeepsTheOpenRequest_AndCloseEmptiesThePane()
    {
        var run = Run();
        var rebuilt = run.GetProperty("rebuilt");
        Assert.True(rebuilt.GetProperty("hasDetail").GetBoolean());
        Assert.Single(rebuilt.GetProperty("reportCalls").EnumerateArray());
        Assert.Equal("", run.GetProperty("closed").GetString());
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
