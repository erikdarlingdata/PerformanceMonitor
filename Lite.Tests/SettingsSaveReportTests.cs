/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2433. Lite's Settings window said "Settings saved." on a save that wrote nothing.
///
/// <para>Ten writers backed that one button and each rewrote the whole of settings.json behind its own
/// catch. Seven returned <c>void</c> and could not report a write failure by construction; the three that
/// returned something returned whether the BOXES validated, which is a different question. So the toast
/// was shown whenever nothing objected, and "nothing objected" was never about whether a byte reached
/// disk.</para>
///
/// <para>The repair was to remove the question rather than answer it: one read, ten mutators on one
/// document, one write. That leaves a single ordering rule, which is what these pin — and the rule that
/// matters is that a failed write outranks a validation objection. A writer that rejected a value has
/// already raised its own dialog naming it; what the user has not been told, and can find out nowhere
/// else, is that none of it was saved.</para>
/// </summary>
public sealed class SettingsSaveReportTests
{
    [Fact]
    public void AnOrdinarySave_Reports_Saved()
    {
        Assert.Equal(
            SettingsSaveOutcome.Saved,
            SettingsSaveReport.Classify(documentWritten: true, mcpChanged: false,
                alertsValid: true, mcpValid: true, webhooksValid: true));
    }

    [Fact]
    public void AnMcpChange_AsksForARestart()
    {
        Assert.Equal(
            SettingsSaveOutcome.SavedAndMcpNeedsRestart,
            SettingsSaveReport.Classify(documentWritten: true, mcpChanged: true,
                alertsValid: true, mcpValid: true, webhooksValid: true));
    }

    /// <summary>
    /// The headline case. Everything on the page validated, so on dev this is exactly the state that
    /// produced "Settings saved." over a save that wrote nothing at all.
    /// </summary>
    [Fact]
    public void AFailedWrite_IsNeverReportedAsSaved()
    {
        Assert.Equal(
            SettingsSaveOutcome.NothingWritten,
            SettingsSaveReport.Classify(documentWritten: false, mcpChanged: false,
                alertsValid: true, mcpValid: true, webhooksValid: true));
    }

    /// <summary>
    /// And it stays the headline case when a writer ALSO objected. The objection has its own dialog; the
    /// failed write does not, and it is the one that describes what happened to the other nine writers'
    /// work. Ordering the other way round would leave the most important fact of the save unsaid.
    /// </summary>
    [Theory]
    [InlineData(false, true, true)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    public void AFailedWrite_OutranksAValidationObjection(bool alertsValid, bool mcpValid, bool webhooksValid)
    {
        Assert.Equal(
            SettingsSaveOutcome.NothingWritten,
            SettingsSaveReport.Classify(documentWritten: false, mcpChanged: true,
                alertsValid, mcpValid, webhooksValid));
    }

    /// <summary>
    /// A rejected value suppresses the toast without claiming nothing was saved — because the rest of the
    /// document DID reach disk. This is the arm that would be a lie under any of the three options #2433
    /// listed that kept the writes separate.
    /// </summary>
    [Theory]
    [InlineData(false, true, true)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    public void AnObjection_SuppressesTheToast_WithoutClaimingNothingWasSaved(
        bool alertsValid, bool mcpValid, bool webhooksValid)
    {
        Assert.Equal(
            SettingsSaveOutcome.WrittenWithObjections,
            SettingsSaveReport.Classify(documentWritten: true, mcpChanged: false,
                alertsValid, mcpValid, webhooksValid));
    }

    /// <summary>
    /// #5772: a password, webhook URL or routing key that Credential Manager refused is an objection too:
    /// the page reached disk, so the report is "written with objections" and the success toast is suppressed.
    /// </summary>
    [Fact]
    public void ACredentialThatWasNotStored_SuppressesTheToast_WhenThePageWasWritten()
    {
        Assert.Equal(
            SettingsSaveOutcome.WrittenWithObjections,
            SettingsSaveReport.Classify(documentWritten: true, mcpChanged: true,
                alertsValid: true, mcpValid: true, webhooksValid: true, credentialsSaved: false));
    }

    /// <summary>
    /// And when the page was not written either, the failed write still outranks it: the answer is that
    /// nothing was saved, and the credential failures ride along in that one dialog.
    /// </summary>
    [Fact]
    public void ACredentialThatWasNotStored_DoesNotOutrankAFailedWrite()
    {
        Assert.Equal(
            SettingsSaveOutcome.NothingWritten,
            SettingsSaveReport.Classify(documentWritten: false, mcpChanged: false,
                alertsValid: true, mcpValid: true, webhooksValid: true, credentialsSaved: false));
    }

    private const string TeamsFailure = "The Teams webhook URL was not saved because it is 1,400 characters long, "
        + "and Windows Credential Manager stores at most 1,280.";

    private const string SmtpFailure = "The SMTP password was not saved because Windows returned error 5 (Access is denied).";

    /// <summary>The page was written but a webhook was not stored: the one dialog says the rest was saved.</summary>
    [Fact]
    public void CredentialFailureText_PageWritten_NamesTheValue_AndSaysTheRestWasSaved()
    {
        var text = SettingsSaveReport.CredentialFailureText(true, new[] { TeamsFailure }, webhookValueHeld: true);

        Assert.Equal(
            TeamsFailure + "\n\nThe rest of this page was saved. Lite keeps using the value you typed until it "
            + "restarts, then goes back to the value saved before, if there was one.",
            text);
        Assert.DoesNotContain("could not be written", text, StringComparison.Ordinal);
    }

    /// <summary>
    /// Nothing was written: the dialog never says the rest was saved. It says the opposite, in the same
    /// dialog as the failed values, so one click gets one true answer.
    /// </summary>
    [Fact]
    public void CredentialFailureText_NothingWritten_NamesTheValue_AndSaysNothingElseWasSaved()
    {
        var text = SettingsSaveReport.CredentialFailureText(false, new[] { TeamsFailure }, webhookValueHeld: true);

        Assert.Equal(
            TeamsFailure + "\n\n" + SettingsSaveReport.NothingWrittenText
            + " Lite keeps using the value you typed until it restarts, then goes back to the value saved "
            + "before, if there was one.",
            text);
        Assert.Contains("nothing was saved", text, StringComparison.Ordinal);
        Assert.DoesNotContain("The rest of this page was saved", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Your other settings were saved", text, StringComparison.Ordinal);
    }

    /// <summary>The SMTP password is read from Credential Manager every time, so the in-memory note is left off.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void CredentialFailureText_SmtpOnly_LeavesOutTheNoteAboutTheTypedValue(bool documentWritten)
    {
        var text = SettingsSaveReport.CredentialFailureText(documentWritten, new[] { SmtpFailure }, webhookValueHeld: false);

        Assert.StartsWith(SmtpFailure, text, StringComparison.Ordinal);
        Assert.DoesNotContain("Lite keeps using", text, StringComparison.Ordinal);
    }

    [Fact]
    public void CredentialFailureText_SeveralFailures_ListsEachOnItsOwnParagraph()
    {
        var text = SettingsSaveReport.CredentialFailureText(true, new[] { TeamsFailure, SmtpFailure }, webhookValueHeld: true);

        Assert.StartsWith(TeamsFailure + "\n\n" + SmtpFailure + "\n\n", text, StringComparison.Ordinal);
    }

    [Fact]
    public void CredentialFailureText_NoFailures_IsNull()
    {
        Assert.Null(SettingsSaveReport.CredentialFailureText(true, Array.Empty<string>(), webhookValueHeld: true));
        Assert.Null(SettingsSaveReport.CredentialFailureText(false, Array.Empty<string>(), webhookValueHeld: true));
    }
}

/// <summary>
/// The category behind #2433 rather than the instance, and the companion to
/// <see cref="SettingsWriterQuarantineWiringTests"/>'s one-writer pin: the Save button must ask whether the
/// write happened before it is allowed to say anything, and none of its writers may go behind its back to
/// settings.json.
///
/// <para>Source-parsing because the defect was a wiring one. Every individual writer was correct about the
/// thing it knew; what was missing was any path from "the write failed" to the sentence the user reads.</para>
/// </summary>
public sealed class SettingsSaveButtonHonestyTests
{
    private static string SettingsWindowSource() =>
        File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Windows", "SettingsWindow.xaml.cs")));

    /// <summary>
    /// The toast is reachable only through the classifier, so a future edit cannot restore the old
    /// "nothing objected, therefore say it saved" shortcut without deleting this.
    /// </summary>
    [Fact]
    public void TheSaveButton_AsksTheClassifierBeforeItSaysAnything()
    {
        var body = SavePath(SettingsWindowSource());

        Assert.Contains("App.WriteSettingsDocument(", body, StringComparison.Ordinal);
        Assert.Contains("SettingsSaveReport.Classify(", body, StringComparison.Ordinal);

        /* Every "Settings saved" string in the method must sit under a classifier arm; the only way to
           check that cheaply is that the classify call comes first. */
        var classify = body.IndexOf("SettingsSaveReport.Classify(", StringComparison.Ordinal);
        var firstToast = body.IndexOf("Settings saved", StringComparison.Ordinal);
        Assert.True(classify < firstToast,
            "The Save path can reach \"Settings saved.\" without going through SettingsSaveReport.Classify, " +
            "which is #2433: the toast is about whether the boxes validated rather than whether anything " +
            "reached disk.");
    }

    /// <summary>
    /// #5772: the credential failures are reported from the Save button, after the settings.json write is
    /// known. A mutator that raised its own dialog before the write could only guess at the rest of the page,
    /// and once said "Your other settings were saved." over a save that then failed.
    /// </summary>
    [Fact]
    public void CredentialFailures_AreReportedAfterTheWrite_AndNoMutatorClaimsTheRestWasSaved()
    {
        var source = SettingsWindowSource();
        var body = SavePath(source);

        var write = body.IndexOf("App.WriteSettingsDocument(", StringComparison.Ordinal);
        var report = body.IndexOf("SettingsSaveReport.CredentialFailureText(", StringComparison.Ordinal);
        Assert.True(write >= 0 && report > write,
            "The Save path must build the credential failure text after the settings.json write is known.");
        Assert.Contains("credentialsSaved:", body, StringComparison.Ordinal);

        Assert.DoesNotContain("Your other settings were saved", source, StringComparison.Ordinal);
        Assert.DoesNotContain("The rest of this page was saved", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// Each Save* the button calls takes the shared document. A writer that opened settings.json for itself
    /// would be back to its own read, its own write and its own swallowed failure — one write is what makes
    /// one answer possible.
    /// </summary>
    [Fact]
    public void EveryWriterTheSaveButtonCalls_TakesTheSharedDocument()
    {
        var source = SettingsWindowSource();
        var body = SavePath(source);

        var called = Regex.Matches(body, @"\b(Save[A-Za-z]+)\(root\)")
            .Select(m => m.Groups[1].Value)
            .ToList();

        Assert.True(called.Count >= 9,
            $"The Save path hands the shared document to only {called.Count} writer(s) — the rest are " +
            "opening settings.json for themselves again.");

        foreach (var name in called)
        {
            Assert.Matches(new Regex($@"\b{name}\(JsonNode root\)"), source);
        }
    }

    /// <summary>
    /// The window's other claim about a save, and the one that used to contradict the dialog. The theme
    /// selector applies its choice LIVE, and <c>_saved</c> is what stops the close handlers reverting that
    /// preview — so setting it on the click rather than on the write left an unpersisted theme applied for
    /// the rest of the run while the dialog said nothing had been saved. It has to track the write.
    /// </summary>
    [Fact]
    public void TheLiveThemePreview_OnlySurvivesASaveThatWrote()
    {
        var source = SettingsWindowSource();
        var body = SavePath(source);

        Assert.Contains("_saved = written;", body, StringComparison.Ordinal);
        Assert.DoesNotContain("_saved = true;", body, StringComparison.Ordinal);

        /* And the gate it feeds is still there, so this cannot quietly stop meaning anything. */
        Assert.Contains("if (!_saved)", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The restart is a bigger claim than the toast, so it needs the same gate. MainWindow reads
    /// <c>McpSettingsChanged</c> after <c>ShowDialog</c> and, when it is set, stops and restarts the MCP
    /// server — dropping every connected client — then reloads the port from settings.json on DISK. Set on
    /// the click rather than the write, a failed save produced a disruptive restart back onto the OLD
    /// configuration, moments after the app had said nothing was saved.
    /// </summary>
    [Fact]
    public void TheMcpRestart_OnlyFollowsASaveThatWrote()
    {
        var body = SavePath(SettingsWindowSource());

        Assert.Contains("if (mcpChanged && written) McpSettingsChanged = true;", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every mutator sits inside the guarded region, not just the read and the write.
    ///
    /// <para>Before the consolidation each writer carried its own try, so an exception thrown while BUILDING
    /// a value — not only on the disk I/O — was caught, logged, and the remaining writers still ran.
    /// Guarding only the ends would leave the mutators between them able to escape into the generic
    /// "An error occurred" dispatcher dialog, which is precisely the class of silence this PR is about.
    /// Nothing is written in that case either, and that is what the user should be told.</para>
    /// </summary>
    [Fact]
    public void EveryMutator_SitsInsideTheGuardedRegion()
    {
        var body = SavePath(SettingsWindowSource());

        var openTry = body.IndexOf("try", StringComparison.Ordinal);
        var firstMutator = body.IndexOf("SaveMcpSettingsAsync(root)", StringComparison.Ordinal);
        var lastMutator = body.IndexOf("SaveWebhookSettings(root)", StringComparison.Ordinal);
        var theCatch = body.IndexOf("catch (Exception", StringComparison.Ordinal);

        Assert.True(openTry >= 0 && firstMutator >= 0 && lastMutator >= 0 && theCatch >= 0,
            "SaveCoreAsync no longer has the shape this guard describes — its anchors moved and it is " +
            "testing nothing.");
        Assert.True(openTry < firstMutator && lastMutator < theCatch,
            "A mutator sits outside SaveCoreAsync's try, so an exception building a settings value " +
            "escapes into App's generic unhandled-exception dialog instead of the honest \"Nothing was " +
            "saved\" this method owes the user.");
    }

    /// <summary>
    /// The body of the Save path. #5846 split the button: <c>SaveButton_Click</c> now only awaits
    /// <c>SaveCoreAsync</c> and then selects the leftmost tab holding an invalid value, so the confirm, the
    /// guarded mutators, the write and the classifier all live in <c>SaveCoreAsync</c>. The click is checked to
    /// still call it, so these rules cannot pass over a method the button no longer runs.
    /// </summary>
    private static string SavePath(string source)
    {
        Assert.Contains("await SaveCoreAsync();", MethodBody(source, "SaveButton_Click"), StringComparison.Ordinal);
        return MethodBody(source, "Task SaveCoreAsync");
    }

    private static string MethodBody(string source, string methodName)
    {
        var signature = source.IndexOf(methodName + "(", StringComparison.Ordinal);
        Assert.True(signature >= 0, $"No method named {methodName} — this guard's anchor moved and it is testing nothing.");

        var open = source.IndexOf('{', signature);
        Assert.True(open >= 0, $"{methodName} has no body.");

        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{') depth++;
            else if (source[i] == '}' && --depth == 0) return source[open..i];
        }

        throw new InvalidOperationException($"{methodName}'s body is unbalanced.");
    }

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }

        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
