/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text.Json;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Admin page's Report Feedback tab (#5913) run under Node through the admin harness: a sign-in that can make changes sees a
/// form on each report and its record goes to the feedback route as JSON; the page reads the list again after a record, and
/// does not after the service refuses (403, the store login is not an admin one) or rejects a value (400), where it shows the
/// service's sentence instead; a sign-in that cannot make changes sees the list and no form.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class AdminReportFeedbackBehaviourTests
{
    private const string Id = "11111111-1111-1111-1111-111111111111";

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void ARecord_GoesToTheFeedbackRoute_AndThePageReadsTheListAgain()
    {
        var page = AdminServerEditBehaviourTests.Run("feedback:recorded");

        Assert.Equal(1, page.GetProperty("formsBefore").GetInt32());
        Assert.Contains("Report Feedback", Strings(page.GetProperty("tabs")));
        var write = Assert.Single(page.GetProperty("writes").EnumerateArray());
        Assert.Equal("/api/investigation-feedback/" + Id, write.GetProperty("url").GetString());
        Assert.Equal("application/json", write.GetProperty("contentType").GetString());
        var body = write.GetProperty("body");
        Assert.Equal("wrong", body.GetProperty("verdict").GetString());
        Assert.Equal("false_positive", body.GetProperty("corrected_verdict").GetString());
        Assert.Equal("never a real block", body.GetProperty("comment").GetString());
        Assert.Equal(JsonValueKind.Null, body.GetProperty("missed_step").ValueKind);
        Assert.Equal(page.GetProperty("readsBefore").GetInt32() + 1, page.GetProperty("readsAfter").GetInt32());
        Assert.Contains(Strings(page.GetProperty("lines")), l => l.Contains("web: wrong", System.StringComparison.Ordinal) && l.Contains("never a real block", System.StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("refused", "Recording feedback needs a store login with the admin role.")]
    [InlineData("invalid", "The feedback was not recorded. verdict: 'great' is not right, partly or wrong.")]
    public void ARefusedOrInvalidRecord_ShowsTheServicesSentence_AndDoesNotReadTheListAgain(string kind, string sentence)
    {
        var page = AdminServerEditBehaviourTests.Run("feedback:" + kind);

        Assert.Equal(page.GetProperty("readsBefore").GetInt32(), page.GetProperty("readsAfter").GetInt32());
        Assert.Equal(sentence, Assert.Single(Strings(page.GetProperty("status"))));
        Assert.Empty(Strings(page.GetProperty("lines")));
    }

    [Fact]
    public void AMissingRequest_ReadsTheListAgain_BecauseItIsStale()
    {
        var page = AdminServerEditBehaviourTests.Run("feedback:gone");

        Assert.Equal(page.GetProperty("readsBefore").GetInt32() + 1, page.GetProperty("readsAfter").GetInt32());
        Assert.Equal("No investigation request has that id.", Assert.Single(Strings(page.GetProperty("status"))));
    }

    [Fact]
    public void AStoreLoginThatCannotRecord_ShowsNoForm_AndSaysTheViewerIsWhereFeedbackIsGiven()
    {
        // The seat can make changes, but the list's own answer says the dashboard's store login may not write (a managed store).
        var page = AdminServerEditBehaviourTests.Run("feedback:nostore");

        Assert.Equal(0, page.GetProperty("formsBefore").GetInt32());
        Assert.Contains(Strings(page.GetProperty("strips")),
            t => t.Contains("this page is list-only", System.StringComparison.Ordinal) && t.Contains("To record feedback, use the Viewer app", System.StringComparison.Ordinal));
    }

    [Fact]
    public void OnTheManagedStoreWithTheSwitchOff_TheListOnlyNote_NamesTheSwitch_InTheWordsOfThe403()
    {
        var page = AdminServerEditBehaviourTests.Run("feedback:nostoreoff");

        Assert.Equal(0, page.GetProperty("formsBefore").GetInt32());
        var note = Assert.Single(Strings(page.GetProperty("strips")), t => t.Contains("this page is list-only", System.StringComparison.Ordinal));
        const string SwitchSentence = " To let a signed-in admin make it here, set web.allowAdminWrites to true in darling.json and restart the service.";
        Assert.Equal("This dashboard lists only on this store, so this page is list-only. To record feedback, use the Viewer app." + SwitchSentence, note);
        Assert.EndsWith(SwitchSentence, PerformanceMonitor.Darling.Service.DarlingWebEndpoints.FeedbackRefusedMessage(true, adminWritesOff: true), System.StringComparison.Ordinal);
    }

    [Fact]
    public void AStoreLoginThatCanRecord_ShowsTheForm_AndNotTheListOnlySentence()
    {
        var page = AdminServerEditBehaviourTests.Run("feedback:recorded");

        Assert.Equal(1, page.GetProperty("formsBefore").GetInt32());
        Assert.DoesNotContain(Strings(page.GetProperty("strips")), t => t.Contains("list-only", System.StringComparison.Ordinal));
    }

    [Fact]
    public void TheReportedLine_ShowsTheInstantInTheReadersClock_AndTheWindowOfTheCountsIsStated()
    {
        // The harness runs in UTC; the stamp is localized (toLocaleString), never the route's ISO text.
        var page = AdminServerEditBehaviourTests.Run("feedback:recorded");

        var reported = Assert.Single(Strings(page.GetProperty("reported")));
        Assert.StartsWith("Reported ", reported, System.StringComparison.Ordinal);
        Assert.EndsWith(" by agent-a: real", reported, System.StringComparison.Ordinal);
        Assert.DoesNotContain("2026-10-01T", reported, System.StringComparison.Ordinal);
        Assert.DoesNotContain(".0000000Z", reported, System.StringComparison.Ordinal);
        Assert.Contains("2026", reported, System.StringComparison.Ordinal);
        Assert.Equal("Counts and reports cover the requests opened in the last 7 days that have a report.", Assert.Single(Strings(page.GetProperty("window"))));
    }

    [Fact]
    public void ASeatThatCannotMakeChanges_SeesTheList_AndNoForm()
    {
        var page = AdminServerEditBehaviourTests.Run("feedback:listonly");

        Assert.Equal(0, page.GetProperty("formsBefore").GetInt32());
        Assert.Contains("Report Feedback", Strings(page.GetProperty("tabs")));
    }
}
