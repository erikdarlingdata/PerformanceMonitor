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
        Assert.Contains(Strings(page.GetProperty("strips")), t => t.Contains("this page lists the settings only", StringComparison.Ordinal) && t.Contains(SwitchSentence, StringComparison.Ordinal));
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
