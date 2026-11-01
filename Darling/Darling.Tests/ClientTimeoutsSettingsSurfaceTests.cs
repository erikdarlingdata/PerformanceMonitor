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
/// #5640 (Lane E1): the Darling Viewer's Client Timeouts settings, the V179 twin of
/// <c>RunnableQueueSettingsSurfaceTests</c>. The row carries the engine's defaults (ON, 10, 60), compares and saves
/// all three columns through the engine's clamp, and the Settings window names the alert. V179 is the newest append, so
/// this class owns the settings row's total width (96). The viewer needs no per-version gate for the columns: it refuses a
/// store below <c>StorageVersion.SchemaVersion</c> at connect, as it did for V177's.
/// </summary>
public sealed class ClientTimeoutsSettingsSurfaceTests
{
    private static readonly string[] Expected =
    {
        "client_timeouts_enabled", "client_timeouts_warning_per_minute", "client_timeouts_critical_per_minute",
    };

    [Fact]
    public void TheRow_StartsAtTheEnginesDefaults_WithTheAlertOn()
    {
        var row = new AlertSettingsRow();
        Assert.True(row.ClientTimeoutsEnabled);
        Assert.Equal(AttentionEvaluator.DefaultWarningPerMinute, row.ClientTimeoutsWarningPerMinute);
        Assert.Equal(AttentionEvaluator.DefaultCriticalPerMinute, row.ClientTimeoutsCriticalPerMinute);
        Assert.Equal(10, row.ClientTimeoutsWarningPerMinute);
        Assert.Equal(60, row.ClientTimeoutsCriticalPerMinute);
    }

    [Fact]
    public void ValueEquals_NoticesEachOfTheThreeColumns()
    {
        Assert.True(new AlertSettingsRow().ValueEquals(new AlertSettingsRow()));
        Assert.False(new AlertSettingsRow().ValueEquals(new AlertSettingsRow { ClientTimeoutsEnabled = false }));
        Assert.False(new AlertSettingsRow().ValueEquals(new AlertSettingsRow { ClientTimeoutsWarningPerMinute = 11 }));
        Assert.False(new AlertSettingsRow().ValueEquals(new AlertSettingsRow { ClientTimeoutsCriticalPerMinute = 61 }));
    }

    [Fact]
    public void TheUpsertAndTheSelect_CarryTheThreeColumns_AtTheEnd()
    {
        foreach (var column in Expected)
        {
            Assert.Contains(column, ViewerDataService.AlertSettingsSelectSql, StringComparison.Ordinal);
            Assert.Contains(column + " = EXCLUDED." + column, ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
        }

        /* The three columns sit together, in this order (their ordinals are pinned below). Columns appended after them (V200's
           excluded_volumes) do not move them. */
        Assert.Contains(
            "client_timeouts_enabled, client_timeouts_warning_per_minute, client_timeouts_critical_per_minute",
            ViewerDataService.AlertSettingsSelectSql, StringComparison.Ordinal);
        Assert.Contains("$76,", ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// V179's three columns sit at the FIXED ordinals 73, 74 and 75 (parameters $74 to $76) on every positional
    /// settings-row surface, and the row is 96 columns wide (V232 appended the four Daily Health Report columns at 92 to 95, V230 appended the two blocking band snapshot wait tiers at 90 and 91, V228 appended the four blocking band tiers at 86 to 89, V200 appended the excluded volumes at 76, V211 the two multiples at 77 and 78, V222 the Agent alert gaps switch at 79, V223 the three database protection switches at 80 to 82, V224 the three identity capacity columns at 83 to 85): the viewer's select and upsert, the service's own read and
    /// its seed, and the MCP read with its positional record. This is the test that was red while the seed had 76 columns
    /// and the viewer 73.
    /// </summary>
    [Fact]
    public void TheThreeColumns_SitAtOrdinals73To75_OnEveryPositionalSurface_OfA86ColumnRow()
    {
        var viewerColumns = ViewerDataService.AlertSettingsSelectSql
            .Replace("SELECT ", "", StringComparison.Ordinal).Split(" FROM ")[0]
            .Split(',').Select(c => c.Trim()).ToList();
        Assert.Equal(96, viewerColumns.Count);
        Assert.Equal(Expected, viewerColumns.Skip(73).Take(3).ToArray());

        var service = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs"));
        var start = service.IndexOf("ReadAlertSettingsAsync(NpgsqlConnection connection", StringComparison.Ordinal);
        Assert.True(start > 0, "the service's alert-settings read could not be located");
        var select = service[start..];
        select = select[(select.IndexOf("SELECT", StringComparison.Ordinal) + 6)..select.IndexOf("FROM config_alert_settings WHERE id = 1", StringComparison.Ordinal)];
        var serviceColumns = select.Split(',').Select(c => c.Trim()).Where(c => c.Length > 0).ToList();
        Assert.Equal(96, serviceColumns.Count);
        Assert.Equal(Expected, serviceColumns.Skip(73).Take(3).ToArray());
        Assert.Contains("ClientTimeoutsEnabled = reader.GetBoolean(73),", service, StringComparison.Ordinal);
        Assert.Contains("ClientTimeoutsWarningPerMinute = reader.GetInt32(74),", service, StringComparison.Ordinal);
        Assert.Contains("ClientTimeoutsCriticalPerMinute = reader.GetInt32(75),", service, StringComparison.Ordinal);

        /* The viewer's own read of its select. */
        var viewer = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.AlertSettings.cs"));
        Assert.Contains("ClientTimeoutsEnabled = reader.GetBoolean(73),", viewer, StringComparison.Ordinal);
        Assert.Contains("ClientTimeoutsWarningPerMinute = reader.GetInt32(74),", viewer, StringComparison.Ordinal);
        Assert.Contains("ClientTimeoutsCriticalPerMinute = reader.GetInt32(75),", viewer, StringComparison.Ordinal);

        /* The MCP read and its positional record. */
        var mcpColumns = DarlingAlertReader.AlertSettingsSelectSql
            .Split("FROM config_alert_settings")[0].Replace("SELECT", "", StringComparison.Ordinal)
            .Split(',').Select(c => c.Trim()).Where(c => c.Length > 0).ToList();
        Assert.Equal(96, mcpColumns.Count);
        Assert.Equal(Expected, mcpColumns.Skip(73).Take(3).ToArray());
        var parameters = typeof(DarlingAlertReader.AlertSettingsReadRow).GetConstructors().Single().GetParameters();
        Assert.Equal(96, parameters.Length);
        Assert.Equal(new[] { "ClientTimeoutsEnabled", "ClientTimeoutsWarningPerMinute", "ClientTimeoutsCriticalPerMinute" },
            parameters.Skip(73).Take(3).Select(p => p.Name!).ToArray());
        Assert.Equal(typeof(bool), parameters[73].ParameterType);
        Assert.Equal(typeof(int), parameters[74].ParameterType);
        Assert.Equal(typeof(int), parameters[75].ParameterType);

        /* The viewer's bind supplies $74 to $76 at indexes 73 to 75, in column order (V200's excluded_volumes follows at 76). */
        var row = new AlertSettingsRow { ClientTimeoutsEnabled = true, ClientTimeoutsWarningPerMinute = 5, ClientTimeoutsCriticalPerMinute = 11 };
        var command = Bind(row);
        Assert.Equal(96, command.Parameters.Count);
        Assert.Equal(true, command.Parameters[73].Value);
        Assert.Equal(5, command.Parameters[74].Value);
        Assert.Equal(11, command.Parameters[75].Value);

        /* #6874: the Agent Alert Gaps switch sits at $80 (index 79, off by default) and takes part in the row's equality. */
        Assert.Equal(false, command.Parameters[79].Value);
        Assert.Equal(true, Bind(new AlertSettingsRow { AgentAlertGapsEnabled = true }).Parameters[79].Value);
        Assert.False(new AlertSettingsRow().ValueEquals(new AlertSettingsRow { AgentAlertGapsEnabled = true }));
    }

    /// <summary>The save goes through the engine's clamp (Warning at least 1, Critical never below Warning), so the store never
    /// holds a pair the engine would rewrite on read.</summary>
    [Theory]
    [InlineData(0, 5, 1, 5)]
    [InlineData(-3, 60, 1, 60)]
    [InlineData(20, 5, 20, 20)]
    [InlineData(10, 60, 10, 60)]
    public void TheBind_ClampsTheBars_WithTheEnginesRule(int warning, int critical, int storedWarning, int storedCritical)
    {
        var command = Bind(new AlertSettingsRow { ClientTimeoutsWarningPerMinute = warning, ClientTimeoutsCriticalPerMinute = critical });
        Assert.Equal(storedWarning, command.Parameters[74].Value);
        Assert.Equal(storedCritical, command.Parameters[75].Value);
        /* #5736: a new row's switch defaults to on, and the clamp leaves it alone. */
        Assert.Equal(true, command.Parameters[73].Value);
    }

    [Fact]
    public void TheSettingsWindow_ShowsTheSwitchAndBothBars_AndSavesThemThroughTheEnginesRule()
    {
        var xaml = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml"));
        foreach (var name in new[] { "AlertClientTimeoutsCheckBox", "AlertClientTimeoutsWarningBox", "AlertClientTimeoutsCriticalBox" })
        {
            Assert.Contains("x:Name=\"" + name + "\"", xaml, StringComparison.Ordinal);
        }

        var code = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml.cs"));
        Assert.Contains("ClientTimeoutsEnabled = AlertClientTimeoutsCheckBox.IsChecked == true", code, StringComparison.Ordinal);
        Assert.Contains("row.ClientTimeoutsWarningPerMinute = ctWarning", code, StringComparison.Ordinal);
        Assert.Contains("row.ClientTimeoutsCriticalPerMinute = ctCritical", code, StringComparison.Ordinal);
        Assert.Contains("ctCritical >= ctWarning", code, StringComparison.Ordinal);
        Assert.Contains("r.ClientTimeoutsWarningPerMinute", code, StringComparison.Ordinal);
        Assert.Contains("AttentionEvaluator.DefaultWarningPerMinute", code, StringComparison.Ordinal);

        /* The switch ships unchecked in the XAML too, so a window that has not loaded a row never shows the alert on. */
        var checkbox = xaml.IndexOf("x:Name=\"AlertClientTimeoutsCheckBox\"", StringComparison.Ordinal);
        var element = xaml.Substring(checkbox, xaml.IndexOf("/>", checkbox, StringComparison.Ordinal) - checkbox);
        Assert.DoesNotContain("IsChecked=\"True\"", element, StringComparison.Ordinal);
    }

    private static NpgsqlCommand Bind(AlertSettingsRow row)
    {
        var bind = typeof(ViewerDataService).GetMethod("BindAlertSettings", BindingFlags.NonPublic | BindingFlags.Static)!;
        var command = new NpgsqlCommand();
        bind.Invoke(null, new object[] { command, row });
        return command;
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
