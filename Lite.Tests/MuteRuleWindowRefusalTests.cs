/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Darling.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Source pins for #4889: while a restore marker is pending, the shared MuteRuleService swallows the store's
/// refusal and still updates its in-memory rules, so every window path must refuse (and undo) on its own.
/// </summary>
public class MuteRuleWindowRefusalTests
{
    private const string Window = "Lite/Windows/ManageMuteRulesWindow.xaml.cs";
    private const string Tab = "Lite/Controls/AlertsHistoryTab.xaml.cs";

    [Fact]
    public void RefusedCheckBoxToggle_PutsTheRuleAndTheBoxBack()
    {
        var body = MethodBody(Window, "EnabledCheckBox_Click");
        var refuse = body.IndexOf("PendingRestoreNotice.Refuse(", StringComparison.Ordinal);
        Assert.True(refuse >= 0, "EnabledCheckBox_Click lost its pending-restore check.");
        var ret = body.IndexOf("return", refuse, StringComparison.Ordinal);
        Assert.True(ret >= 0, "the refusal branch no longer returns.");
        var branch = body[refuse..ret];
        Assert.True(branch.Contains("rule.Enabled =", StringComparison.Ordinal),
            "the two-way binding already flipped the shared rule; a refused toggle must set rule.Enabled back before it returns.");
        Assert.True(branch.Contains("cb.IsChecked =", StringComparison.Ordinal),
            "MuteRule raises no change events; a refused toggle must set cb.IsChecked back before it returns.");
    }

    [Fact]
    public void EveryServiceWriteInTheWindow_ChecksItsResult()
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(ParitySource.ReadFile(Window));
        var calls = System.Text.RegularExpressions.Regex.Matches(source, @"_muteRuleService\.(\w+)");
        var checkedWrites = 0;
        foreach (System.Text.RegularExpressions.Match m in calls)
        {
            if (m.Groups[1].Value is "PurgeExpiredRulesAsync" or "GetRules") continue;
            Assert.True(m.Index >= 18 && source[(m.Index - 18)..m.Index] == "var saved = await ",
                $"{Window}: _muteRuleService.{m.Groups[1].Value} must be written as 'var saved = await ...' so its result is acted on.");
            checkedWrites++;
        }
        Assert.Equal(5, checkedWrites);
    }

    [Fact]
    public void CheckBoxToggle_NotSaved_RevertsAndSaysSo()
    {
        var body = MethodBody(Window, "EnabledCheckBox_Click");
        var saved = body.IndexOf("var saved = await", StringComparison.Ordinal);
        Assert.True(saved >= 0, "EnabledCheckBox_Click no longer captures the service result.");
        var branch = body[saved..];
        Assert.True(branch.Contains("rule.Enabled =", StringComparison.Ordinal), "a not-saved toggle must set rule.Enabled back.");
        Assert.True(branch.Contains("cb.IsChecked =", StringComparison.Ordinal), "a not-saved toggle must set cb.IsChecked back.");
        Assert.True(branch.Contains("PendingRestoreNotice.SaveFailed(", StringComparison.Ordinal), "a not-saved toggle must tell the user.");
    }

    [Theory]
    [InlineData(Window, "AddRule_Click", "ShowDialog", "_muteRuleService.")]
    [InlineData(Window, "EditRule_Click", "ShowDialog", "_muteRuleService.")]
    [InlineData(Window, "DeleteRule_Click", "MessageBox.Show", "_muteRuleService.")]
    [InlineData(Tab, "MuteThisAlert_Click", "ShowDialog", "MuteRuleService.AddRuleAsync")]
    [InlineData(Tab, "MuteSimilarAlerts_Click", "ShowDialog", "MuteRuleService.AddRuleAsync")]
    public void MuteRuleWrite_ChecksAgainAfterItsDialog_BeforeTheServiceCall(string file, string method, string dialog, string call)
    {
        var body = MethodBody(file, method);
        var dialogAt = body.IndexOf(dialog, StringComparison.Ordinal);
        var callAt = body.IndexOf(call, StringComparison.Ordinal);
        Assert.True(dialogAt >= 0 && callAt > dialogAt, $"{method}: anchors moved ({dialog} / {call}).");
        var between = body[dialogAt..callAt];
        Assert.True(between.Contains("PendingRestoreNotice.Refuse(", StringComparison.Ordinal),
            $"{method}: a reset can start while the dialog is open; re-check PendingRestoreNotice.Refuse after it and before the service call.");
    }

    [Fact]
    public void SnoozeBalloon_NotSaved_TellsTheUserBeforeClosing()
    {
        var body = MethodBody("Lite/Controls/SnoozeBalloon.xaml.cs", "Snooze");
        var saved = body.IndexOf("var saved = await _muteRuleService.AddRuleAsync", StringComparison.Ordinal);
        Assert.True(saved >= 0, "Snooze no longer captures the AddRuleAsync result.");
        var branch = body[saved..];
        var notice = branch.IndexOf("PendingRestoreNotice.SaveFailed(", StringComparison.Ordinal);
        var close = branch.IndexOf("CloseBalloon()", StringComparison.Ordinal);
        Assert.True(notice >= 0 && close >= 0 && notice < close, "a not-saved snooze must call PendingRestoreNotice.SaveFailed before CloseBalloon.");
    }

    private static string MethodBody(string file, string method)
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(ParitySource.ReadFile(file));
        var at = source.IndexOf(" " + method + "(", StringComparison.Ordinal);
        Assert.True(at >= 0, $"{file}: no method {method}; the pin's anchor moved.");
        var open = source.IndexOf('{', at);
        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{') depth++;
            else if (source[i] == '}' && --depth == 0) return source[open..i];
        }
        throw new InvalidOperationException($"{file}: unbalanced braces in {method}.");
    }
}
