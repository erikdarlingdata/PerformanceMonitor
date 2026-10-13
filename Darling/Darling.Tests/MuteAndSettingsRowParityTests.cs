/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5640 round 2: "Mute this alert" on a Client Timeouts alert makes the SAME rule (database + client application) in
/// Lite, the viewer and the web page. The two desktop dialogs hid the Database field for the metric, so the pre-fill
/// from the alert was dropped on save; this pins the metric item, both visibility gates and the save path in both,
/// and the web page's side of the same click.
/// </summary>
public sealed class MuteDialogClientTimeoutsParityTests
{
    private static readonly DateTime Now = new(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc);

    private static readonly (string App, string[] Xaml, string[] Code)[] Dialogs =
    {
        ("viewer", new[] { "Darling", "PerformanceMonitor.Darling.Viewer", "MuteRuleEditDialog.xaml" },
            new[] { "Darling", "PerformanceMonitor.Darling.Viewer", "MuteRuleEditDialog.xaml.cs" }),
        ("Lite", new[] { "Lite", "Windows", "MuteRuleDialog.xaml" },
            new[] { "Lite", "Windows", "MuteRuleDialog.xaml.cs" }),
    };

    [Fact]
    public void BothDialogs_ListClientTimeouts_AfterRunnableQueue_FromTheSharedConstant()
    {
        Assert.Equal(AttentionEvaluator.MetricName, AlertFamily.ClientTimeoutsMetric);

        foreach (var (app, xamlPath, codePath) in Dialogs)
        {
            var xaml = RepoFile.ReadRepoFileLf(xamlPath);
            var code = RepoFile.ReadRepoFileLf(codePath);

            Assert.Contains("xmlns:n=\"clr-namespace:PerformanceMonitor.Notifications;assembly=PerformanceMonitor.Notifications\"", xaml, StringComparison.Ordinal);
            var runnable = xaml.IndexOf("<ComboBoxItem Content=\"Runnable Queue\"/>", StringComparison.Ordinal);
            var item = xaml.IndexOf("<ComboBoxItem Content=\"{x:Static n:AlertFamily.ClientTimeoutsMetric}\"/>", StringComparison.Ordinal);
            Assert.True(runnable >= 0 && item > runnable, $"{app}: the metric list names Client Timeouts after Runnable Queue");

            /* No new literal: the code names the metric only through the constant. */
            Assert.DoesNotContain("\"Client Timeouts\"", code, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void BothDialogs_ShowTheDatabaseAndClientApplicationFields_ForClientTimeouts_AndSaveThePrefill()
    {
        foreach (var (app, _, codePath) in Dialogs)
        {
            var code = RepoFile.ReadRepoFileLf(codePath);

            Assert.True(Regex.IsMatch(code, @"bool showDatabase = [^;]*or AlertFamily\.ClientTimeoutsMetric;"), $"{app}: showDatabase lists the metric");
            Assert.Contains("bool showClientApp = metric is null or \"(any)\" or AlertFamily.ClientTimeoutsMetric;", code, StringComparison.Ordinal);

            /* The pre-fill from the alert's context, and the save that keeps each pattern only while its box is visible. */
            Assert.Contains("DatabasePatternBox.Text = context.DatabaseName;", code, StringComparison.Ordinal);
            Assert.Contains("ClientAppPatternBox.Text = context.ClientAppName;", code, StringComparison.Ordinal);
            Assert.Contains("Rule.DatabasePattern = DatabasePatternBox.Visibility == Visibility.Visible", code, StringComparison.Ordinal);
            Assert.Contains("Rule.ClientAppPattern = ClientAppPatternBox.Visibility == Visibility.Visible", code, StringComparison.Ordinal);
            Assert.Contains("SelectMetric(context.MetricName);", code, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheSavedRule_IsDatabasePlusApp_AndMutesThatAlertOnly()
    {
        /* What the dialogs save once both boxes are visible and pre-filled from the alert's top database and top application. */
        var alert = new AlertMuteContext { ServerName = "sql01", MetricName = AlertFamily.ClientTimeoutsMetric, DatabaseName = "sales", ClientAppName = "etl-job" };
        var saved = new MuteRule
        {
            ServerName = alert.ServerName,
            MetricName = alert.MetricName,
            DatabasePattern = alert.DatabaseName,
            ClientAppPattern = alert.ClientAppName,
        };

        Assert.True(saved.MatchesAt(alert, Now));
        Assert.False(saved.MatchesAt(new AlertMuteContext { ServerName = "sql01", MetricName = alert.MetricName, DatabaseName = "hr", ClientAppName = "etl-job" }, Now));
        Assert.False(saved.MatchesAt(new AlertMuteContext { ServerName = "sql01", MetricName = alert.MetricName, DatabaseName = "sales", ClientAppName = "ssms" }, Now));
        Assert.False(saved.MatchesEveryAlert);
    }

    [Fact]
    public void TheWebPage_SavesTheSameTwoPatterns_ForTheSameAlert()
    {
        var context = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "mute-context.js");
        Assert.Contains("database_pattern: d.database,", context, StringComparison.Ordinal);
        Assert.Contains("client_app_pattern: d.clientApp,", context, StringComparison.Ordinal);

        var rules = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "mute-rules.js");
        Assert.Contains("key: \"database_pattern\"", rules, StringComparison.Ordinal);
        Assert.Contains("key: \"client_app_pattern\"", rules, StringComparison.Ordinal);
    }
}

/// <summary>
/// #5640 round 2 / #5709: the Settings rows for Client Timeouts and Runnable Queue read the same in Lite and the viewer:
/// the same label wording and the same per-box tooltips. Ground-truth wording, the unit said once, no "attentions"
/// jargon in visible text (the tooltip is where the sqlserver.attention event is named).
/// </summary>
public sealed class SettingsRowParityTests
{
    private static readonly string[] Lite = { "Lite", "Windows", "SettingsWindow.xaml" };
    private static readonly string[] Viewer = { "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml" };

    /// <summary>The attribute value of <paramref name="attr"/> on the element named <paramref name="name"/>, or null.</summary>
    private static string? Attr(string xaml, string name, string attr)
    {
        var start = xaml.IndexOf($"x:Name=\"{name}\"", StringComparison.Ordinal);
        Assert.True(start >= 0, $"{name} is in the window");
        var end = xaml.IndexOf("/>", start, StringComparison.Ordinal);
        var element = xaml.Substring(start, end - start);
        var match = Regex.Match(element, attr + "=\"([^\"]*)\"");
        return match.Success ? match.Groups[1].Value : null;
    }

    [Theory]
    [InlineData("AlertRunnableQueueCheckBox", "Content")]
    [InlineData("AlertRunnableQueueCheckBox", "ToolTip")]
    [InlineData("AlertRunnableQueueWarningBox", "ToolTip")]
    [InlineData("AlertRunnableQueueCriticalBox", "ToolTip")]
    [InlineData("AlertClientTimeoutsCheckBox", "Content")]
    [InlineData("AlertClientTimeoutsCheckBox", "ToolTip")]
    [InlineData("AlertClientTimeoutsWarningBox", "ToolTip")]
    [InlineData("AlertClientTimeoutsCriticalBox", "ToolTip")]
    public void LiteAndTheViewer_SayTheSameThing_OnTheRowsControls(string control, string attr)
    {
        var lite = Attr(RepoFile.ReadRepoFileLf(Lite), control, attr);
        var viewer = Attr(RepoFile.ReadRepoFileLf(Viewer), control, attr);

        Assert.False(string.IsNullOrWhiteSpace(lite), $"Lite {control} has a {attr}");
        Assert.Equal(lite, viewer);
    }

    [Fact]
    public void TheClientTimeoutsRow_SaysThePerMinuteUnitOnce_AndUsesNoJargonInVisibleText()
    {
        foreach (var path in new[] { Lite, Viewer })
        {
            var xaml = RepoFile.ReadRepoFileLf(path);
            var label = Attr(xaml, "AlertClientTimeoutsCheckBox", "Content")!;
            Assert.Equal("Client timeouts and cancels per minute — Warning at", label);
            Assert.DoesNotContain("attention", label, StringComparison.OrdinalIgnoreCase);

            /* The row ends at the Critical box: no trailing "per minute" TextBlock repeats the unit. */
            var row = xaml.Substring(xaml.IndexOf("x:Name=\"AlertClientTimeoutsCheckBox\"", StringComparison.Ordinal));
            row = row.Substring(0, row.IndexOf("</StackPanel>", StringComparison.Ordinal));
            Assert.DoesNotContain("Text=\"per minute\"", row, StringComparison.Ordinal);

            /* The tooltip is where the event is named. */
            Assert.Contains("sqlserver.attention", Attr(xaml, "AlertClientTimeoutsCheckBox", "ToolTip"), StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheViewersClientTimeoutsCheckBox_IsVerticallyCentred_LikeItsNeighbours()
    {
        var xaml = RepoFile.ReadRepoFileLf(Viewer);
        Assert.Equal("Center", Attr(xaml, "AlertClientTimeoutsCheckBox", "VerticalAlignment"));
        Assert.Equal("Center", Attr(xaml, "AlertRunnableQueueCheckBox", "VerticalAlignment"));
    }
}
