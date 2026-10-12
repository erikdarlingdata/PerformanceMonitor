/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5641 (Lane 2b): the Darling Viewer's Runnable Queue settings. The row carries the engine's defaults, compares and
/// saves all three columns, and the Settings window and the mute dialog name the alert. The MCP and web halves are
/// pinned in <c>DarlingMcpAlertToolsSurfaceAndSqlTests</c> and <c>AdminPageTests</c>.
/// </summary>
public sealed class RunnableQueueSettingsSurfaceTests
{
    [Fact]
    public void TheRow_StartsAtTheEnginesDefaults()
    {
        var row = new AlertSettingsRow();
        Assert.True(row.RunnableQueueEnabled);
        Assert.Equal(RunnableQueueEvaluator.DefaultWarningPerScheduler, row.RunnableQueueWarningPerScheduler);
        Assert.Equal(RunnableQueueEvaluator.DefaultCriticalPerScheduler, row.RunnableQueueCriticalPerScheduler);
        Assert.Equal(8, row.RunnableQueueWarningPerScheduler);
        Assert.Equal(16, row.RunnableQueueCriticalPerScheduler);
    }

    [Fact]
    public void ValueEquals_NoticesEachOfTheThreeColumns()
    {
        Assert.True(new AlertSettingsRow().ValueEquals(new AlertSettingsRow()));
        Assert.False(new AlertSettingsRow().ValueEquals(new AlertSettingsRow { RunnableQueueEnabled = false }));
        Assert.False(new AlertSettingsRow().ValueEquals(new AlertSettingsRow { RunnableQueueWarningPerScheduler = 9 }));
        Assert.False(new AlertSettingsRow().ValueEquals(new AlertSettingsRow { RunnableQueueCriticalPerScheduler = 17 }));
    }

    [Fact]
    public void TheUpsertAndTheSelect_CarryTheThreeColumns_InTheOrdinalsTheReaderUses()
    {
        foreach (var column in new[] { "runnable_queue_enabled", "runnable_queue_warning_per_scheduler", "runnable_queue_critical_per_scheduler" })
        {
            Assert.Contains(column, ViewerDataService.AlertSettingsSelectSql, StringComparison.Ordinal);
            Assert.Contains(column + " = EXCLUDED." + column, ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
        }

        // Appended last, so every older ordinal keeps its place: the select ends with the three columns in order.
        Assert.EndsWith(
            "runnable_queue_enabled, runnable_queue_warning_per_scheduler, runnable_queue_critical_per_scheduler FROM config_alert_settings WHERE id = 1",
            ViewerDataService.AlertSettingsSelectSql, StringComparison.Ordinal);
        Assert.Contains("$73,", ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
        Assert.DoesNotContain("$74", ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSettingsWindow_ShowsTheSwitchAndBothBars_AndSavesThemThroughTheEnginesRule()
    {
        var xaml = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml"));
        foreach (var name in new[] { "AlertRunnableQueueCheckBox", "AlertRunnableQueueWarningBox", "AlertRunnableQueueCriticalBox" })
        {
            Assert.Contains("x:Name=\"" + name + "\"", xaml, StringComparison.Ordinal);
        }

        var code = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml.cs"));
        Assert.Contains("RunnableQueueEnabled = AlertRunnableQueueCheckBox.IsChecked == true", code, StringComparison.Ordinal);
        Assert.Contains("row.RunnableQueueWarningPerScheduler = rqWarning", code, StringComparison.Ordinal);
        Assert.Contains("row.RunnableQueueCriticalPerScheduler = rqCritical", code, StringComparison.Ordinal);
        Assert.Contains("rqCritical >= rqWarning", code, StringComparison.Ordinal);
        Assert.Contains("r.RunnableQueueWarningPerScheduler", code, StringComparison.Ordinal);
    }

    [Fact]
    public void TheMuteDialog_OffersRunnableQueue_BesidePoisonWait_WithoutAWaitTypeField()
    {
        var xaml = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", "MuteRuleEditDialog.xaml"));
        var poison = xaml.IndexOf("<ComboBoxItem Content=\"Poison Wait\"/>", StringComparison.Ordinal);
        var runnable = xaml.IndexOf("<ComboBoxItem Content=\"" + RunnableQueueEvaluator.MetricName + "\"/>", StringComparison.Ordinal);
        Assert.True(poison >= 0 && runnable > poison, "the metric list names Runnable Queue after Poison Wait");

        // The alert carries no wait type, so the dialog must not offer one for it (the wait-type arm lists only Poison Wait and Long-Running Query).
        var code = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", "MuteRuleEditDialog.xaml.cs"));
        Assert.DoesNotContain("\"Runnable Queue\"", code, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")) && !File.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
