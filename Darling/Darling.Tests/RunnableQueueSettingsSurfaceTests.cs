/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service.Mcp;
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

    /// <summary>
    /// V177's three columns sit at the FIXED ordinals 70, 71 and 72 (parameters $71 to $73) on every settings-row
    /// surface that is positional, and the row is 73 columns wide. This rung is the newest append, so it owns the
    /// total: the earlier knobs pin their own column's ordinal and no longer claim the end
    /// (<c>UncorroboratedRouteStoreKnobTests</c>, whose column is 69). The next rung to append moves this total on
    /// purpose and pins its own columns the same way.
    /// </summary>
    [Fact]
    public void TheThreeColumns_SitAtOrdinals70To72_OnEveryPositionalSurface_OfA73ColumnRow()
    {
        var expected = new[] { "runnable_queue_enabled", "runnable_queue_warning_per_scheduler", "runnable_queue_critical_per_scheduler" };

        /* The viewer's select, and the service's own read. */
        var viewerColumns = ViewerDataService.AlertSettingsSelectSql
            .Replace("SELECT ", "", StringComparison.Ordinal).Split(" FROM ")[0]
            .Split(',').Select(c => c.Trim()).ToList();
        Assert.Equal(73, viewerColumns.Count);
        Assert.Equal(expected, viewerColumns.Skip(70).ToArray());

        var service = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs"));
        var start = service.IndexOf("ReadAlertSettingsAsync(NpgsqlConnection connection", StringComparison.Ordinal);
        Assert.True(start > 0, "the service's alert-settings read could not be located");
        var select = service[start..];
        select = select[(select.IndexOf("SELECT", StringComparison.Ordinal) + 6)..select.IndexOf("FROM config_alert_settings WHERE id = 1", StringComparison.Ordinal)];
        var serviceColumns = select.Split(',').Select(c => c.Trim()).Where(c => c.Length > 0).ToList();
        Assert.Equal(73, serviceColumns.Count);
        Assert.Equal(expected, serviceColumns.Skip(70).ToArray());
        Assert.Contains("RunnableQueueEnabled = reader.GetBoolean(70),", service, StringComparison.Ordinal);
        Assert.Contains("RunnableQueueWarningPerScheduler = reader.GetInt32(71),", service, StringComparison.Ordinal);
        Assert.Contains("RunnableQueueCriticalPerScheduler = reader.GetInt32(72),", service, StringComparison.Ordinal);

        /* The MCP read and its positional record. */
        var mcpColumns = DarlingAlertReader.AlertSettingsSelectSql
            .Split("FROM config_alert_settings")[0].Replace("SELECT", "", StringComparison.Ordinal)
            .Split(',').Select(c => c.Trim()).Where(c => c.Length > 0).ToList();
        Assert.Equal(73, mcpColumns.Count);
        Assert.Equal(expected, mcpColumns.Skip(70).ToArray());
        var parameters = typeof(DarlingAlertReader.AlertSettingsReadRow).GetConstructors().Single().GetParameters();
        Assert.Equal(73, parameters.Length);
        Assert.Equal(new[] { "RunnableQueueEnabled", "RunnableQueueWarningPerScheduler", "RunnableQueueCriticalPerScheduler" },
            parameters.Skip(70).Select(p => p.Name!).ToArray());
        Assert.Equal(typeof(bool), parameters[70].ParameterType);
        Assert.Equal(typeof(int), parameters[71].ParameterType);
        Assert.Equal(typeof(int), parameters[72].ParameterType);

        /* The viewer's bind supplies $71 to $73 at indexes 70 to 72, in column order, and nothing after them. */
        var bind = typeof(ViewerDataService).GetMethod("BindAlertSettings", BindingFlags.NonPublic | BindingFlags.Static)!;
        var row = new AlertSettingsRow { RunnableQueueEnabled = false, RunnableQueueWarningPerScheduler = 5, RunnableQueueCriticalPerScheduler = 11 };
        using var command = new NpgsqlCommand();
        bind.Invoke(null, new object[] { command, row });
        Assert.Equal(73, command.Parameters.Count);
        Assert.Equal(false, command.Parameters[70].Value);
        Assert.Equal(5, command.Parameters[71].Value);
        Assert.Equal(11, command.Parameters[72].Value);
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
