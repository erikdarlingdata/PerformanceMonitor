/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7700: on a managed store with web.allowAdminWrites off, the service refuses every admin write, so Alert Settings, Practice Scores
/// and Pending Notes draw their rows and no form, and say why (the shipped page scripts, run under Node). With the switch on, the same
/// signed-in admin still gets the forms. The Investigations Close form is covered in InvestigationsPageTests.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class AdminWritesOffBehaviourTests
{
    private const string SwitchSentence = "set web.allowAdminWrites to true in darling.json";

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void AlertSettings_WithTheSwitchOff_DrawsTheRowsAndNoForm_AndNamesTheSwitch()
    {
        var page = AdminServerEditBehaviourTests.Run("writesOff:settings/off");

        Assert.Equal(0, page.GetProperty("saveButtons").GetInt32());
        Assert.Contains("overdue", page.GetProperty("rows").GetString() ?? "", StringComparison.OrdinalIgnoreCase);
        // #7558: the switch is named once, in the line under the Admin heading; the tab's own line says only what the page lists.
        var strips = Strings(page.GetProperty("strips"));
        Assert.Contains(strips, t => t.Contains("This page lists the settings only", StringComparison.Ordinal) && !t.Contains(SwitchSentence, StringComparison.Ordinal));
        Assert.Single(strips, t => t.Contains(SwitchSentence, StringComparison.Ordinal));
    }

    [Fact]
    public void AlertSettings_WithTheSwitchOn_DrawsBothForms()
    {
        var page = AdminServerEditBehaviourTests.Run("writesOff:settings/on");

        Assert.Equal(2, page.GetProperty("saveButtons").GetInt32());
        Assert.DoesNotContain(Strings(page.GetProperty("strips")), t => t.Contains(SwitchSentence, StringComparison.Ordinal));
    }

    [Fact]
    public void TheAlertSettingsNote_NamesBothForms_AndSaysThePageIsReadOnlyWhenChangesAreOff()
    {
        var page = AdminServerEditBehaviourTests.Run("writesOff:settings/on");

        var note = Strings(page.GetProperty("strips")).First(t => t.StartsWith("Read only", StringComparison.Ordinal));
        Assert.Contains("Database Protection overdue ages", note, StringComparison.Ordinal);
        Assert.Contains("Daily Health Report", note, StringComparison.Ordinal);
        Assert.Contains("turned off, this page is read only", note, StringComparison.Ordinal);
    }

    [Fact]
    public void PracticeScores_WithTheSwitchOff_DrawsNoStartForm_AndNamesTheSwitch()
    {
        var off = AdminServerEditBehaviourTests.Run("writesOff:practice/off");
        var on = AdminServerEditBehaviourTests.Run("writesOff:practice/on");

        Assert.Equal(0, off.GetProperty("practiceForm").GetInt32());
        Assert.Contains(Strings(off.GetProperty("strips")), t => t.Contains(SwitchSentence, StringComparison.Ordinal));
        Assert.Equal(1, on.GetProperty("practiceForm").GetInt32());
    }

    /// <summary>The Daily Health Report's Preview only reads, so with the switch off it is still drawn and still works (Save and the
    /// inputs that only feed Save are not); with the switch on the full editor is drawn.</summary>
    [Fact]
    public void DailyReportPreview_WithTheSwitchOff_StaysAndWorks_WithNoSaveAndNoInputs()
    {
        var off = AdminServerEditBehaviourTests.Run("writesOffPreview:off");
        var on = AdminServerEditBehaviourTests.Run("writesOffPreview:on");

        Assert.Equal(1, off.GetProperty("previewButtons").GetInt32());
        Assert.Equal(0, off.GetProperty("saveButtons").GetInt32());
        Assert.Equal(0, off.GetProperty("inputs").GetInt32());
        Assert.Contains("PREVIEW BODY 123", off.GetProperty("previewText").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, off.GetProperty("reads").GetInt32());
        Assert.Equal(0, off.GetProperty("writes").GetInt32());
        Assert.Equal(1, on.GetProperty("previewButtons").GetInt32());
        Assert.Equal(1, on.GetProperty("saveButtons").GetInt32());
        Assert.True(on.GetProperty("inputs").GetInt32() > 0);
    }

    /// <summary>A seat that can edit but is not signed in, on a store with the switch off, gets the switch sentence and not the sign-in
    /// one: signing in would not help.</summary>
    [Fact]
    public void PracticeScores_ASeatNotSignedIn_WithTheSwitchOff_GetsTheSwitchSentence_NotTheSignInSentence()
    {
        var page = AdminServerEditBehaviourTests.Run("writesOff:practice/offSignedOut");

        Assert.Equal(0, page.GetProperty("practiceForm").GetInt32());
        Assert.Contains(Strings(page.GetProperty("strips")), t => t.Contains(SwitchSentence, StringComparison.Ordinal));
        Assert.DoesNotContain("Sign in to the web as an admin", page.GetProperty("text").GetString() ?? "", StringComparison.Ordinal);
    }

    /// <summary>#7950: the switch never covered adding and editing servers (they follow the edit seat), so the line under the Admin
    /// heading names the changes that are off and says servers still save, over the Servers tab and over a list-only tab alike. The
    /// Servers tab keeps its Edit buttons.</summary>
    [Fact]
    public void TheAdminHeading_WithTheSwitchOff_NamesTheChangesThatAreOff_AndSaysServersStillSave()
    {
        var servers = AdminServerEditBehaviourTests.Run("writesOffServers:off");
        var settings = AdminServerEditBehaviourTests.Run("writesOff:settings/off");

        var heading = servers.GetProperty("heading").GetString();
        Assert.NotNull(heading);
        Assert.Contains("Adding and editing servers", heading, StringComparison.Ordinal);
        Assert.Contains("still work", heading, StringComparison.Ordinal);
        Assert.Contains(SwitchSentence, heading, StringComparison.Ordinal);
        Assert.DoesNotContain("Changes from the web are turned off", heading, StringComparison.Ordinal);
        Assert.True(servers.GetProperty("editButtons").GetInt32() > 0);
        Assert.Contains(Strings(settings.GetProperty("strips")), t => t.Contains("Adding and editing servers", StringComparison.Ordinal));
    }

    [Fact]
    public void TheAdminHeading_WithTheSwitchOn_DrawsNoLine_AndServersKeepsEdit()
    {
        var on = AdminServerEditBehaviourTests.Run("writesOffServers:on");

        Assert.Equal(JsonValueKind.Null, on.GetProperty("heading").ValueKind);
        Assert.True(on.GetProperty("editButtons").GetInt32() > 0);
    }

    [Fact]
    public void PendingNotes_WithTheSwitchOff_ListTheNotesWithNoWriteFormAndNoDecisionButtons_AndNameTheSwitch()
    {
        var off = AdminServerEditBehaviourTests.Run("writesOff:pending/off");
        var on = AdminServerEditBehaviourTests.Run("writesOff:pending/on");

        Assert.Equal(0, off.GetProperty("noteForm").GetInt32());
        Assert.Equal(0, off.GetProperty("decideButtons").GetInt32());
        Assert.Contains("note 7", off.GetProperty("rows").GetString(), StringComparison.Ordinal);
        Assert.Contains(Strings(off.GetProperty("strips")), t => t.Contains(SwitchSentence, StringComparison.Ordinal));
        Assert.Equal(1, on.GetProperty("noteForm").GetInt32());
        Assert.True(on.GetProperty("decideButtons").GetInt32() > 0);
    }
}
