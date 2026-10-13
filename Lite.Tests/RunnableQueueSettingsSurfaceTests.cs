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
/// Lite's Runnable Queue settings surfaces (#5641): the Settings window's save rules, the three settings-file keys the
/// window writes and <c>App</c> loads, the sample file, the MCP <c>get_alert_settings</c> group, and the mute dialog's
/// metric list. Shares the "app-alert-statics" collection because the MCP test reads and writes the App statics.
/// </summary>
[Collection("app-alert-statics")]
public sealed class RunnableQueueSettingsSurfaceTests
{
    private static readonly string[] Keys =
    {
        "alert_runnable_queue_enabled",
        "alert_runnable_queue_warning_per_scheduler",
        "alert_runnable_queue_critical_per_scheduler",
    };

    /* ---------------- the save rules ---------------- */

    [Theory]
    [InlineData("8", "16")]
    [InlineData("1", "1")]
    [InlineData(" 4 ", "40")]
    public void AValidPair_IsAccepted_AndReturnedAsTyped(string warning, string critical)
    {
        var errors = new List<string>();

        var (w, c) = RunnableQueueBarsInput.Validate(warning, critical, errors);

        Assert.Empty(errors);
        Assert.Equal(int.Parse(warning.Trim()), w);
        Assert.Equal(int.Parse(critical.Trim()), c);
    }

    [Theory]
    [InlineData("0", "16", RunnableQueueBarsInput.WarningInvalidMessage)]
    [InlineData("-3", "16", RunnableQueueBarsInput.WarningInvalidMessage)]
    [InlineData("abc", "16", RunnableQueueBarsInput.WarningInvalidMessage)]
    [InlineData("", "16", RunnableQueueBarsInput.WarningInvalidMessage)]
    [InlineData("8", "0", RunnableQueueBarsInput.CriticalInvalidMessage)]
    [InlineData("8", "1.5", RunnableQueueBarsInput.CriticalInvalidMessage)]
    [InlineData("8", "7", RunnableQueueBarsInput.CriticalBelowWarningMessage)]
    public void ABadPair_IsRefusedWithAMessage(string warning, string critical, string expectedMessage)
    {
        var errors = new List<string>();

        RunnableQueueBarsInput.Validate(warning, critical, errors);

        Assert.Contains(expectedMessage, errors);
    }

    [Fact]
    public void BothBarsBad_ReportsBothMessages_AndNoOrderingMessage()
    {
        var errors = new List<string>();

        RunnableQueueBarsInput.Validate("x", "y", errors);

        Assert.Equal(
            new[] { RunnableQueueBarsInput.WarningInvalidMessage, RunnableQueueBarsInput.CriticalInvalidMessage },
            errors.ToArray());
    }

    /* ---------------- the settings file ---------------- */

    [Fact]
    public void TheWindowWritesTheThreeKeysAppLoads_AndTheSampleFileCarriesThemAtTheEvaluatorsDefaults()
    {
        var window = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Windows", "SettingsWindow.xaml.cs")));
        var app = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "App.xaml.cs")));
        var sample = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "config", "settings.sample.json")));

        foreach (var key in Keys)
        {
            Assert.Contains($"root[\"{key}\"]", window, StringComparison.Ordinal);
            Assert.Contains($"\"{key}\"", app, StringComparison.Ordinal);
            Assert.Contains($"\"{key}\"", sample, StringComparison.Ordinal);
        }

        Assert.Matches(new Regex(@"""alert_runnable_queue_enabled"":\s*true"), sample);
        Assert.Matches(
            new Regex($@"""alert_runnable_queue_warning_per_scheduler"":\s*{RunnableQueueEvaluator.DefaultWarningPerScheduler}\b"), sample);
        Assert.Matches(
            new Regex($@"""alert_runnable_queue_critical_per_scheduler"":\s*{RunnableQueueEvaluator.DefaultCriticalPerScheduler}\b"), sample);
    }

    [Fact]
    public void TheWindowHasTheThreeControls_GatedByTheMasterSwitch_AndInThePreview()
    {
        var xaml = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Windows", "SettingsWindow.xaml")));
        var code = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Windows", "SettingsWindow.xaml.cs")));

        foreach (var name in new[] { "AlertRunnableQueueCheckBox", "AlertRunnableQueueWarningBox", "AlertRunnableQueueCriticalBox" })
        {
            Assert.Contains($"x:Name=\"{name}\"", xaml, StringComparison.Ordinal);
            Assert.Contains($"{name}.IsEnabled = enabled;", code, StringComparison.Ordinal);
        }

        Assert.Contains("AlertRunnableQueueWarningBox.Text = RunnableQueueEvaluator.DefaultWarningPerScheduler", code, StringComparison.Ordinal);
        Assert.Contains("AlertRunnableQueueCriticalBox.Text = RunnableQueueEvaluator.DefaultCriticalPerScheduler", code, StringComparison.Ordinal);
        Assert.Contains("runnable tasks per scheduler >=", code, StringComparison.Ordinal);
    }

    /* ---------------- MCP ---------------- */

    [Fact]
    public async Task GetAlertSettings_ReportsTheRunnableQueueGroup_WithTheBarsTheEngineActuallyUses()
    {
        var (enabled, warning, critical) = (App.AlertRunnableQueueEnabled, App.AlertRunnableQueueWarningPerScheduler, App.AlertRunnableQueueCriticalPerScheduler);
        try
        {
            App.AlertRunnableQueueEnabled = false;
            App.AlertRunnableQueueWarningPerScheduler = 12;
            App.AlertRunnableQueueCriticalPerScheduler = 5;     /* below Warning: the engine raises it */

            var json = await McpAlertTools.GetAlertSettings();
            var group = JsonDocument.Parse(json).RootElement.GetProperty("runnable_queue");

            Assert.Equal(
                new[] { "enabled", "warning_per_scheduler", "critical_per_scheduler" },
                group.EnumerateObject().Select(p => p.Name).ToArray());
            Assert.False(group.GetProperty("enabled").GetBoolean());
            Assert.Equal(12, group.GetProperty("warning_per_scheduler").GetInt32());
            Assert.Equal(12, group.GetProperty("critical_per_scheduler").GetInt32());
        }
        finally
        {
            App.AlertRunnableQueueEnabled = enabled;
            App.AlertRunnableQueueWarningPerScheduler = warning;
            App.AlertRunnableQueueCriticalPerScheduler = critical;
        }
    }

    [Fact]
    public void TheGetAlertSettingsGuideText_NamesTheRunnableQueueAlertAndItsKeys()
    {
        var method = typeof(McpAlertTools).GetMethod(nameof(McpAlertTools.GetAlertSettings))!;
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;

        Assert.Contains("poison waits, runnable queue,", description, StringComparison.Ordinal);
        Assert.Contains("runnable_queue.enabled", description, StringComparison.Ordinal);
        Assert.Contains("runnable_queue.warning_per_scheduler", description, StringComparison.Ordinal);
        Assert.Contains("runnable_queue.critical_per_scheduler", description, StringComparison.Ordinal);
    }

    /* ---------------- the mute dialog ---------------- */

    [Fact]
    public void TheMuteDialogOffersRunnableQueue_AndShowsNoPatternFieldForIt()
    {
        var xaml = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Windows", "MuteRuleDialog.xaml")));
        var code = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Windows", "MuteRuleDialog.xaml.cs")));

        Assert.Contains("<ComboBoxItem Content=\"Runnable Queue\"/>", xaml, StringComparison.Ordinal);
        Assert.Equal(RunnableQueueEvaluator.MetricName, "Runnable Queue");

        /* The alert's mute context carries no wait type, database, query text or job name, so it appears in none of
           the pattern-field predicates and choosing it collapses the pattern grid. */
        Assert.DoesNotContain("\"Runnable Queue\"", code, StringComparison.Ordinal);
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
