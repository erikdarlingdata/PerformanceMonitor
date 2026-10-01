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
