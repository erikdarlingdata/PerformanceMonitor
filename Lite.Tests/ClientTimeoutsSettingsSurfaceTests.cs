/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitorLite;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Lite's Client Timeouts settings surfaces (#5640), the twin of <c>RunnableQueueSettingsSurfaceTests</c>: the Settings
/// window's save rules, the three settings-file keys the window writes and <c>App</c> loads, the sample file (alert ON by
/// default), the engine settings the statics project onto, and the MCP <c>get_alert_settings</c> group. Shares the
/// "app-alert-statics" collection because two tests read and write the App statics.
/// </summary>
[Collection("app-alert-statics")]
public sealed class ClientTimeoutsSettingsSurfaceTests
{
    private static readonly string[] Keys =
    {
        "alert_client_timeouts_enabled",
        "alert_client_timeouts_warning_per_minute",
        "alert_client_timeouts_critical_per_minute",
    };

    /* ---------------- the save rules ---------------- */

    [Theory]
    [InlineData("10", "60")]
    [InlineData("1", "1")]
    [InlineData(" 4 ", "40")]
    public void AValidPair_IsAccepted_AndReturnedAsTyped(string warning, string critical)
    {
        var errors = new List<string>();

        var (w, c) = ClientTimeoutsBarsInput.Validate(warning, critical, errors);

        Assert.Empty(errors);
        Assert.Equal(int.Parse(warning.Trim()), w);
        Assert.Equal(int.Parse(critical.Trim()), c);
    }

    [Theory]
    [InlineData("0", "60", ClientTimeoutsBarsInput.WarningInvalidMessage)]
    [InlineData("-3", "60", ClientTimeoutsBarsInput.WarningInvalidMessage)]
    [InlineData("abc", "60", ClientTimeoutsBarsInput.WarningInvalidMessage)]
    [InlineData("", "60", ClientTimeoutsBarsInput.WarningInvalidMessage)]
    [InlineData("10", "0", ClientTimeoutsBarsInput.CriticalInvalidMessage)]
    [InlineData("10", "1.5", ClientTimeoutsBarsInput.CriticalInvalidMessage)]
    [InlineData("10", "9", ClientTimeoutsBarsInput.CriticalBelowWarningMessage)]
    public void ABadPair_IsRefusedWithAMessage(string warning, string critical, string expectedMessage)
    {
        var errors = new List<string>();

        ClientTimeoutsBarsInput.Validate(warning, critical, errors);

        Assert.Contains(expectedMessage, errors);
    }

    [Fact]
    public void BothBarsBad_ReportsBothMessages_AndNoOrderingMessage()
    {
        var errors = new List<string>();

        ClientTimeoutsBarsInput.Validate("x", "y", errors);

        Assert.Equal(
            new[] { ClientTimeoutsBarsInput.WarningInvalidMessage, ClientTimeoutsBarsInput.CriticalInvalidMessage },
            errors.ToArray());
    }

    /// <summary>The window refuses what the engine would rewrite, so a pair the window accepts is a pair the engine reads back unchanged.</summary>
    [Theory]
    [InlineData(1, 1)]
    [InlineData(10, 60)]
    [InlineData(25, 25)]
    public void APairTheWindowAccepts_IsOneTheEngineClampLeavesAlone(int warning, int critical)
    {
        var errors = new List<string>();
        var (w, c) = ClientTimeoutsBarsInput.Validate(warning.ToString(), critical.ToString(), errors);

        Assert.Empty(errors);
        Assert.Equal((w, c), AttentionEvaluator.ClampBars(w, c));
    }

    /* ---------------- the settings file ---------------- */

    [Fact]
    public void TheWindowWritesTheThreeKeysAppLoads_AndTheSampleFileCarriesThemAtTheEvaluatorsDefaults_WithTheAlertOn()
    {
        var window = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Windows", "SettingsWindow.xaml.cs")));
        var app = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "App.xaml.cs")));
        var sample = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "config", "settings.sample.json")));

        foreach (var key in Keys)
        {
            if (key == "alert_client_timeouts_enabled")
            {
                /* #5736: the switch is written beside its marker, by App.WriteClientTimeoutsSwitch, and
                   ClientTimeoutsOnByDefaultSettingsTests round-trips that write through the loader. */
                Assert.Contains($"root[\"{key}\"]", app, StringComparison.Ordinal);
            }
            else
            {
                Assert.Contains($"root[\"{key}\"]", window, StringComparison.Ordinal);
            }

            Assert.Contains($"\"{key}\"", app, StringComparison.Ordinal);
            Assert.Contains($"\"{key}\"", sample, StringComparison.Ordinal);
        }

        Assert.Matches(new Regex(@"""alert_client_timeouts_enabled"":\s*true"), sample);
        Assert.Matches(
            new Regex($@"""alert_client_timeouts_warning_per_minute"":\s*{AttentionEvaluator.DefaultWarningPerMinute}\b"), sample);
        Assert.Matches(
            new Regex($@"""alert_client_timeouts_critical_per_minute"":\s*{AttentionEvaluator.DefaultCriticalPerMinute}\b"), sample);

        /* The load takes the whole-number bars from 1 up, and the switch ships on since #5736. */
        Assert.Contains("AlertClientTimeoutsWarningPerMinute = v.WholeNumber(AlertClientTimeoutsWarningPerMinute, 1, int.MaxValue)", app, StringComparison.Ordinal);
        Assert.Contains("public static bool AlertClientTimeoutsEnabled { get; set; } = true;", app, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWindowHasTheThreeControls_GatedByTheMasterSwitch_AndInThePreview()
    {
        var xaml = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Windows", "SettingsWindow.xaml")));
        var code = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Windows", "SettingsWindow.xaml.cs")));

        foreach (var name in new[] { "AlertClientTimeoutsCheckBox", "AlertClientTimeoutsWarningBox", "AlertClientTimeoutsCriticalBox" })
        {
            Assert.Contains($"x:Name=\"{name}\"", xaml, StringComparison.Ordinal);
            Assert.Contains($"{name}.IsEnabled = enabled;", code, StringComparison.Ordinal);
        }

        Assert.Contains("AlertClientTimeoutsWarningBox.Text = AttentionEvaluator.DefaultWarningPerMinute", code, StringComparison.Ordinal);
        Assert.Contains("AlertClientTimeoutsCriticalBox.Text = AttentionEvaluator.DefaultCriticalPerMinute", code, StringComparison.Ordinal);
        Assert.Contains("App.AlertClientTimeoutsEnabled = AlertClientTimeoutsCheckBox.IsChecked == true", code, StringComparison.Ordinal);
        Assert.Contains("ClientTimeoutsBarsInput.Validate(", code, StringComparison.Ordinal);
        Assert.Contains("client timeouts >=", code, StringComparison.Ordinal);
    }

    [Fact]
    public void TheMuteDialog_OffersClientTimeouts_ShowingDatabaseAndClientApp_FromTheSharedMetricConstant()
    {
        var xaml = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Windows", "MuteRuleDialog.xaml"))).Replace("\r\n", "\n", StringComparison.Ordinal);
        var code = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Windows", "MuteRuleDialog.xaml.cs"))).Replace("\r\n", "\n", StringComparison.Ordinal);

        /* The item takes its text from the one declaration the engine's evaluator also takes, so the dialog cannot drift from the alert's name. */
        Assert.Equal(AttentionEvaluator.MetricName, PerformanceMonitor.Notifications.AlertFamily.ClientTimeoutsMetric);
        Assert.Contains("xmlns:n=\"clr-namespace:PerformanceMonitor.Notifications;assembly=PerformanceMonitor.Notifications\"", xaml, StringComparison.Ordinal);
        var runnable = xaml.IndexOf("<ComboBoxItem Content=\"Runnable Queue\"/>", StringComparison.Ordinal);
        var item = xaml.IndexOf("<ComboBoxItem Content=\"{x:Static n:AlertFamily.ClientTimeoutsMetric}\"/>", StringComparison.Ordinal);
        Assert.True(runnable >= 0 && item > runnable, "the metric list names Client Timeouts after Runnable Queue");
        Assert.DoesNotContain("\"Client Timeouts\"", code, StringComparison.Ordinal);

        /* Both pattern fields show for it, so the alert's top database and top client application (the pre-fill) are saved, not dropped by the visibility gate. */
        Assert.Matches(@"bool showDatabase = [^;]*or AlertFamily\.ClientTimeoutsMetric;", code);
        Assert.Contains("bool showClientApp = metric is null or \"(any)\" or AlertFamily.ClientTimeoutsMetric;", code, StringComparison.Ordinal);
        Assert.Contains("DatabasePatternBox.Text = context.DatabaseName;", code, StringComparison.Ordinal);
        Assert.Contains("ClientAppPatternBox.Text = context.ClientAppName;", code, StringComparison.Ordinal);
        Assert.Contains("Rule.DatabasePattern = DatabasePatternBox.Visibility == Visibility.Visible", code, StringComparison.Ordinal);
        Assert.Contains("Rule.ClientAppPattern = ClientAppPatternBox.Visibility == Visibility.Visible", code, StringComparison.Ordinal);
    }

    /* ---------------- the engine's view of the statics ---------------- */

    [Fact]
    public void TheEngineSettings_ReadTheStatics_ThroughTheClamp()
    {
        var (enabled, warning, critical) = (App.AlertClientTimeoutsEnabled, App.AlertClientTimeoutsWarningPerMinute, App.AlertClientTimeoutsCriticalPerMinute);
        try
        {
            App.AlertClientTimeoutsEnabled = true;
            App.AlertClientTimeoutsWarningPerMinute = 0;
            App.AlertClientTimeoutsCriticalPerMinute = 0;
            var floor = new AppAlertEngineSettings();
            Assert.True(floor.ClientTimeoutsEnabled);
            Assert.Equal(1, floor.ClientTimeoutsWarningPerMinute);
            Assert.Equal(1, floor.ClientTimeoutsCriticalPerMinute);

            App.AlertClientTimeoutsWarningPerMinute = 30;
            App.AlertClientTimeoutsCriticalPerMinute = 12;
            var crossed = new AppAlertEngineSettings();
            Assert.Equal(30, crossed.ClientTimeoutsWarningPerMinute);
            Assert.Equal(30, crossed.ClientTimeoutsCriticalPerMinute);
        }
        finally
        {
            App.AlertClientTimeoutsEnabled = enabled;
            App.AlertClientTimeoutsWarningPerMinute = warning;
            App.AlertClientTimeoutsCriticalPerMinute = critical;
        }
    }

    /* ---------------- MCP ---------------- */

    [Fact]
    public async Task GetAlertSettings_ReportsTheClientTimeoutsGroup_WithTheBarsTheEngineActuallyUses()
    {
        var (enabled, warning, critical) = (App.AlertClientTimeoutsEnabled, App.AlertClientTimeoutsWarningPerMinute, App.AlertClientTimeoutsCriticalPerMinute);
        try
        {
            App.AlertClientTimeoutsEnabled = true;
            App.AlertClientTimeoutsWarningPerMinute = 12;
            App.AlertClientTimeoutsCriticalPerMinute = 5;     /* below Warning: the engine raises it */

            var json = await McpAlertTools.GetAlertSettings();
            var group = JsonDocument.Parse(json).RootElement.GetProperty("client_timeouts");

            Assert.Equal(
                new[] { "enabled", "warning_per_minute", "critical_per_minute", "warning_multiple", "critical_multiple" },
                group.EnumerateObject().Select(p => p.Name).ToArray());
            Assert.True(group.GetProperty("enabled").GetBoolean());
            Assert.Equal(12, group.GetProperty("warning_per_minute").GetInt32());
            Assert.Equal(12, group.GetProperty("critical_per_minute").GetInt32());
        }
        finally
        {
            App.AlertClientTimeoutsEnabled = enabled;
            App.AlertClientTimeoutsWarningPerMinute = warning;
            App.AlertClientTimeoutsCriticalPerMinute = critical;
        }
    }

    [Fact]
    public void TheGetAlertSettingsGuideText_NamesTheClientTimeoutsAlertAndItsKeys()
    {
        var method = typeof(McpAlertTools).GetMethod(nameof(McpAlertTools.GetAlertSettings))!;
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;

        Assert.Contains("runnable queue, client timeouts,", description, StringComparison.Ordinal);
        Assert.Contains("client_timeouts.enabled", description, StringComparison.Ordinal);
        Assert.Contains("client_timeouts.warning_per_minute", description, StringComparison.Ordinal);
        Assert.Contains("client_timeouts.critical_per_minute", description, StringComparison.Ordinal);
        Assert.Contains("ON by default", description, StringComparison.Ordinal);
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
