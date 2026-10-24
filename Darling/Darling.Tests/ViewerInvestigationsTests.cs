/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The viewer's investigation writes say what is wrong before a statement runs (#5632 Phase A): a close needs a reason of
/// at most 500 characters, a note is 1 to 2,000 characters kept 1 to 365 days, a profile is at most 4,000 characters for
/// one scope, and every write names the person. The bounds are the V184 CHECKs' twins.
/// </summary>
public sealed class ViewerInvestigationsValidationTests
{
    private const string UnreachableStore = "Host=127.0.0.1;Port=1;Username=nobody;Database=nothing;Timeout=3;Pooling=false";

    [Fact]
    public void TheCloseReason_IsRequired_AndBounded()
    {
        Assert.Equal("Enter a reason for closing the request.", ViewerDataService.ValidateInvestigationCloseReason("  "));
        Assert.Equal("The reason must be 500 characters or fewer.", ViewerDataService.ValidateInvestigationCloseReason(new string('x', 501)));
        Assert.Null(ViewerDataService.ValidateInvestigationCloseReason(new string('x', 500)));
    }

    [Fact]
    public void ANote_IsOneTo2000Characters_KeptOneTo365Days()
    {
        Assert.Equal("Enter the note's text.", ViewerDataService.ValidateInvestigationNote("", 90));
        Assert.Equal("A note must be 2,000 characters or fewer.", ViewerDataService.ValidateInvestigationNote(new string('n', 2001), 90));
        Assert.Equal("A note is kept from 1 to 365 days.", ViewerDataService.ValidateInvestigationNote("ok", 366));
        Assert.Equal("A note is kept from 1 to 365 days.", ViewerDataService.ValidateInvestigationNote("ok", 0));
        Assert.Null(ViewerDataService.ValidateInvestigationNote(new string('n', 2000), 365));
        Assert.Equal(90, ViewerDataService.InvestigationNoteDefaultDays);
    }

    [Fact]
    public void AProfile_IsAtMost4000Characters_ForOneScope_AndBlankIsAllowed()
    {
        Assert.Equal("A profile is for the fleet, one server or one tag, not a server and a tag.",
            ViewerDataService.ValidateInvestigationProfile(1, 2, "text"));
        Assert.Equal("A profile must be 4,000 characters or fewer.", ViewerDataService.ValidateInvestigationProfile(null, null, new string('p', 4001)));
        Assert.Null(ViewerDataService.ValidateInvestigationProfile(null, 3, new string('p', 4000)));
        Assert.Null(ViewerDataService.ValidateInvestigationProfile(5, null, ""));
    }

    /* #6400: the store matches a scope on coalesce(scope_tag_id, 0), so tag 0 would stand in the fleet's place. */
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void AProfile_ForATagThatIs0OrBelow_IsRefused_NamingTheId(int tagId)
    {
        var refusal = ViewerDataService.ValidateInvestigationProfile(null, tagId, "text");
        Assert.NotNull(refusal);
        Assert.Contains($"Tag id {tagId} ", refusal, StringComparison.Ordinal);
        Assert.Equal(refusal, ViewerDataService.ValidateInvestigationProfile(null, tagId, ""));
        Assert.Null(ViewerDataService.ValidateInvestigationProfile(null, 1, "text"));
    }

    /* #6400: the same for the server scope, which shares the coalesce(scope_server_id, 0) match. */
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void AProfile_ForAServerThatIs0OrBelow_IsRefused_NamingTheId(int serverId)
    {
        var refusal = ViewerDataService.ValidateInvestigationProfile(serverId, null, "text");
        Assert.NotNull(refusal);
        Assert.Contains($"Server id {serverId} ", refusal, StringComparison.Ordinal);
        Assert.Equal(refusal, ViewerDataService.ValidateInvestigationProfile(serverId, null, ""));
        Assert.Null(ViewerDataService.ValidateInvestigationProfile(1, null, "text"));
    }

    [Fact]
    public void EveryWrite_NamesThePerson_AsPersonLogin()
    {
        Assert.Equal("person:erik", ViewerDataService.InvestigationPersonActor(" erik "));
        Assert.NotNull(ViewerDataService.ValidateInvestigationLogin(" "));
        Assert.NotNull(ViewerDataService.ValidateInvestigationLogin(new string('l', 257)));
        Assert.Null(ViewerDataService.ValidateInvestigationLogin(new string('l', 256)));
        Assert.Null(ViewerDataService.ValidateInvestigationLogin("erik"));
    }

    /// <summary>The Viewer's login is the Windows user name (<c>Environment.UserName</c>, no domain), which can hold a space or an accented
    /// letter. The Viewer's write accepts it, in normal form C, and refuses the characters that would pass for another person's name.</summary>
    [Fact]
    public void TheViewersLogin_MayHoldASpaceOrAnAccent_ButNotAnInvisibleOrBidiCharacter()
    {
        Assert.Equal("person:John Smith", ViewerDataService.InvestigationPersonActor(" John Smith "));
        Assert.Equal("person:Jos\u00E9", ViewerDataService.InvestigationPersonActor("Jose\u0301"));
        Assert.Null(ViewerDataService.ValidateInvestigationLogin("John Smith"));
        Assert.Null(ViewerDataService.ValidateInvestigationLogin("Jose\u0301"));
        Assert.Null(ViewerDataService.ValidateInvestigationLogin("CORP\\jsmith"));
        foreach (var refused in new[] { "John  Smith", "erik\u200B", "erik\u202E", "erik\uFEFF", "erik\u3164", "erik<x>", "erik;x", "erik\tsmith" })
        {
            Assert.NotNull(ViewerDataService.ValidateInvestigationLogin(refused));
        }
    }

    /// <summary>Each write refuses bad input with the same words before it opens a connection: the store here listens on
    /// nothing, so reaching it would fail with a socket error instead.</summary>
    [Fact]
    public async Task EachWrite_RefusesBadInput_BeforeItReachesTheStore()
    {
        await using var viewer = new ViewerDataService(UnreachableStore);
        var close = await Assert.ThrowsAsync<ArgumentException>(() => viewer.CloseInvestigationRequestAsync(Guid.NewGuid(), "erik", " "));
        Assert.StartsWith("Enter a reason for closing the request.", close.Message, StringComparison.Ordinal);
        var decide = await Assert.ThrowsAsync<ArgumentException>(() => viewer.DecideInvestigationNoteAsync(1, true, ""));
        Assert.StartsWith("The viewer could not tell who you are", decide.Message, StringComparison.Ordinal);
        var note = await Assert.ThrowsAsync<ArgumentException>(() => viewer.WriteInvestigationNoteAsync(null, null, "text", "erik", 400));
        Assert.StartsWith("A note is kept from 1 to 365 days.", note.Message, StringComparison.Ordinal);
        var profile = await Assert.ThrowsAsync<ArgumentException>(() => viewer.SaveInvestigationProfileAsync(1, 2, "text", "erik", null));
        Assert.StartsWith("A profile is for the fleet, one server or one tag", profile.Message, StringComparison.Ordinal);

        /* #5913: feedback is refused in the rules' words, and a blank login before any store call. */
        var feedbackLogin = await Assert.ThrowsAsync<ArgumentException>(
            () => viewer.RecordInvestigationFeedbackAsync(Guid.NewGuid(), "wrong", null, null, null, " "));
        Assert.StartsWith("The viewer could not tell who you are", feedbackLogin.Message, StringComparison.Ordinal);
        var feedbackVerdict = await Assert.ThrowsAsync<ArgumentException>(
            () => viewer.RecordInvestigationFeedbackAsync(Guid.NewGuid(), "fine", null, null, null, "erik"));
        Assert.StartsWith("The feedback was not recorded. verdict:", feedbackVerdict.Message, StringComparison.Ordinal);
    }
}

/// <summary>
/// The Investigations window's four commands on the shared STA thread (#5632 Phase A): each checks its input and says
/// what is missing in the status line without reaching the store (it listens on nothing here), and a read-only seat gets
/// none of the write controls.
/// </summary>
public sealed class InvestigationsWindowStaTests
{
    private const string UnreachableStore = "Host=127.0.0.1;Port=1;Username=nobody;Database=nothing;Timeout=3;Pooling=false";

    private static void OnWindow(bool readOnly, Func<InvestigationsWindow, Task> body) =>
        StaTestThread.Run(async () =>
        {
            await using var viewer = new ViewerDataService(UnreachableStore);
            var window = new InvestigationsWindow(viewer, readOnly, "tester");
            try
            {
                await body(window);
            }
            finally
            {
                window.Close();
            }
        });

    [Fact]
    public void Close_WithNoRequestSelected_SaysSo()
    {
        OnWindow(false, async window =>
        {
            await window.CloseRequestCommandAsync();
            Assert.Equal("Select a request to close.", window.StatusText.Text);
        });
    }

    [Fact]
    public void CloseError_RefusesAFinalRequest_AndABlankReason()
    {
        var expired = new InvestigationRequestViewRow { State = "expired" };
        Assert.Equal("This request is expired already; only an open, claimed or reported request can be closed.",
            InvestigationsWindow.CloseError(expired, "done"));
        var reported = new InvestigationRequestViewRow { State = "reported" };
        Assert.Equal("Enter a reason for closing the request.", InvestigationsWindow.CloseError(reported, ""));
        Assert.Null(InvestigationsWindow.CloseError(reported, "reviewed"));
    }

    [Fact]
    public void Decide_WithNoNoteSelected_SaysSo_AndADecidedNoteIsRefused()
    {
        OnWindow(false, async window =>
        {
            await window.DecideNoteCommandAsync(approve: true);
            Assert.Equal("Select a note to approve or reject.", window.StatusText.Text);
        });
        Assert.Equal("This note is approved already; only a pending note can be approved or rejected.",
            InvestigationsWindow.DecideError(new InvestigationNoteViewRow { State = "approved" }));
        Assert.Null(InvestigationsWindow.DecideError(new InvestigationNoteViewRow { State = "pending" }));
    }

    [Fact]
    public void WriteNote_WithABadLifetime_OrNoText_SaysSo()
    {
        OnWindow(false, async window =>
        {
            window.NoteTextBox.Text = "the nightly ETL blocks here; expected";
            window.NoteDaysBox.Text = "forever";
            await window.WriteNoteCommandAsync();
            Assert.Equal("Keep for days must be a whole number from 1 to 365.", window.StatusText.Text);

            window.NoteTextBox.Text = " ";
            window.NoteDaysBox.Text = "90";
            await window.WriteNoteCommandAsync();
            Assert.Equal("Enter the note's text.", window.StatusText.Text);
        });
    }

    [Fact]
    public void SaveProfile_TooLong_SaysSo_AndTheFleetIsTheFirstScope()
    {
        OnWindow(false, async window =>
        {
            var fleet = Assert.IsType<InvestigationsWindow.ScopeChoice>(window.ProfileScopeBox.SelectedItem);
            Assert.Null(fleet.ServerId);
            Assert.Null(fleet.TagId);
            window.ProfileTextBox.Text = new string('p', 4001);
            await window.SaveProfileCommandAsync();
            Assert.Equal("A profile must be 4,000 characters or fewer.", window.StatusText.Text);
        });
    }

    [Fact]
    public void SaveFeedback_NeedsAReport_AndTheRulesWords_BeforeTheStore()
    {
        OnWindow(false, async window =>
        {
            await window.SaveFeedbackCommandAsync();
            Assert.Equal("Select a report to give feedback on.", window.StatusText.Text);

            /* The first choice of each box is "right" and "keep the report's verdict" (no correction). */
            Assert.Null(InvestigationsWindow.CorrectedVerdictOf(((System.Windows.Controls.ComboBoxItem)window.FeedbackCorrectedBox.SelectedItem).Content as string));
            Assert.Equal("false_positive", InvestigationsWindow.CorrectedVerdictOf("false_positive"));

            var item = new InvestigationsWindow.FeedbackReportItem(new PerformanceMonitor.Common.InvestigationFeedbackListRow(
                Guid.NewGuid(), "srv", "blocking", "Blocking", DateTime.UtcNow, DateTime.UtcNow, "agent-a", "real", "root", null,
                Array.Empty<PerformanceMonitor.Common.InvestigationFeedbackRow>()));
            Assert.Null(InvestigationsWindow.FeedbackError(item, "wrong", "false_positive", "", ""));
            Assert.StartsWith("The feedback was not recorded. verdict:", InvestigationsWindow.FeedbackError(item, "", null, null, null), StringComparison.Ordinal);
            Assert.Contains("comment: 4001 characters", InvestigationsWindow.FeedbackError(item, "wrong", null, null, new string('c', 4001)), StringComparison.Ordinal);
            Assert.Equal("none", item.FeedbackSummary);

            /* The tab says what its counts are taken over, and the rule that settles a request's verdict (#5913). */
            Assert.Equal(30, window.FeedbackDays);
            var label = InvestigationsWindow.FeedbackWindowLabel(window.FeedbackDays);
            Assert.Contains("last 30 days", label, StringComparison.Ordinal);
            Assert.Contains("a later one that gives none does not displace it", label, StringComparison.Ordinal);
            Assert.Equal(item.Row.ReportedAtUtc, item.ReportedAtUtc);
        });
    }

    [Fact]
    public void AReadOnlySeat_HasNoWriteControls_AndSaysWhy()
    {
        OnWindow(true, window =>
        {
            Assert.False(window.IsEditable);
            Assert.Equal(Visibility.Collapsed, window.CloseRequestPanel.Visibility);
            Assert.Equal(Visibility.Collapsed, window.DecideNotePanel.Visibility);
            Assert.Equal(Visibility.Collapsed, window.WriteNotePanel.Visibility);
            Assert.Equal(Visibility.Collapsed, window.SaveProfilePanel.Visibility);
            Assert.Equal(Visibility.Collapsed, window.GiveFeedbackPanel.Visibility);
            Assert.True(window.ProfileTextBox.IsReadOnly);
            Assert.Equal(InvestigationsWindow.ReadOnlyMessage, window.StatusText.Text);
            return Task.CompletedTask;
        });
    }
}
