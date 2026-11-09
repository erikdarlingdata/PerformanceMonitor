/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8350 item 1: "Mute This Alert" on an alert whose type the dialog's Metric list did not name (Failed Agent Job, Server
/// Unreachable, Server Restored, Database State) opened with Metric "(any)", so Save wrote a rule that muted every alert
/// from that server. The dialog's list names every alert type the engines check mute rules for, and the alert's own type
/// is selected when the dialog opens from it, so the saved rule mutes that type and nothing else from the server.
/// </summary>
public sealed class MuteThisAlertScopeTests
{
    private static readonly DateTime Now = new(2026, 11, 8, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void MuteThisAlert_OnAFailedAgentJob_SavesARuleForThatMetric_NotTheWholeServer()
    {
        var rule = SaveFrom(new AlertMuteContext { ServerName = "sql01", MetricName = "Failed Agent Job" });

        Assert.Equal("Failed Agent Job", rule.MetricName);
        Assert.Equal("sql01", rule.ServerName);
        Assert.True(rule.MatchesAt(new AlertMuteContext { ServerName = "sql01", MetricName = "Failed Agent Job", JobName = "nightly" }, Now));
        Assert.False(rule.MatchesAt(new AlertMuteContext { ServerName = "sql01", MetricName = "High CPU" }, Now),
            "a rule saved from a Failed Agent Job alert must not mute the same server's High CPU alerts");
        Assert.False(rule.MatchesAt(new AlertMuteContext { ServerName = "sql01", MetricName = "Server Unreachable" }, Now));
    }

    [Theory]
    [InlineData("Server Unreachable")]
    [InlineData("Server Restored")]
    [InlineData("Database State")]
    [InlineData("Failed Agent Job")]
    public void MuteSimilarAlerts_OnAConnectionOrStateAlert_SavesARuleForThatMetricAlone(string metric)
    {
        var rule = SaveFrom(new AlertMuteContext { MetricName = metric });

        Assert.Equal(metric, rule.MetricName);
        Assert.Null(rule.ServerName);
        Assert.True(rule.MatchesAt(new AlertMuteContext { ServerName = "any", MetricName = metric }, Now));
        Assert.False(rule.MatchesAt(new AlertMuteContext { ServerName = "any", MetricName = "Deadlocks Detected" }, Now));
    }

    [Fact]
    public void TheMetricList_NamesEveryAlertTypeTheEnginesCheckMuteRulesFor()
    {
        var listed = StaTestThread.Run(() => WithTheme(() =>
        {
            var dialog = new MuteRuleDialog();
            try
            {
                return ((ComboBox)dialog.FindName("MetricCombo")).Items.Cast<ComboBoxItem>()
                    .Select(i => i.Content?.ToString() ?? "").ToArray();
            }
            finally
            {
                dialog.Close();
            }
        }));

        /* Lite raises the performance and agent-job alerts (the connection and AG names among them) and no PostgreSQL alert. */
        var missing = AlertFamily.MetricFamilies
            .Where(pair => pair.Value is AlertFamily.Performance or AlertFamily.AgentJobs && !pair.Key.StartsWith("PostgreSQL ", StringComparison.Ordinal))
            .Select(pair => pair.Key)
            .Where(metric => !listed.Contains(metric, StringComparer.OrdinalIgnoreCase)).ToArray();
        Assert.True(missing.Length == 0, "the Metric list lacks: " + string.Join(", ", missing));
        Assert.Equal("(any)", listed[0]);
        Assert.Equal(listed.Length, listed.Distinct(StringComparer.OrdinalIgnoreCase).Count());
    }

    [Fact]
    public void MuteThisAlert_OnAnAnalysisFinding_KeepsTheFindingsOwnMetricName()
    {
        const string metric = "Analysis: Missing Index [0a1b2c3d]";
        var rule = SaveFrom(new AlertMuteContext { ServerName = "sql01", MetricName = metric });

        Assert.Equal(metric, rule.MetricName);
        Assert.False(rule.MatchesAt(new AlertMuteContext { ServerName = "sql01", MetricName = "Analysis: Parameter Sniffing [ffff0000]" }, Now));
    }

    /// <summary>Opens the dialog the way Alert History's Mute This / Mute Similar do and clicks Save without touching a field.</summary>
    private static MuteRule SaveFrom(AlertMuteContext context) =>
        StaTestThread.Run(() => WithTheme(() =>
        {
            var dialog = new MuteRuleDialog(context);
            try
            {
                /* The dialog opens on the alert's own type. Checked before Save: a dialog left on "(any)" with no server would ask
                   "mute ALL alerts?" in a native box, which would hold the shared STA thread instead of failing here. */
                var combo = (ComboBox)dialog.FindName("MetricCombo");
                Assert.Equal(context.MetricName, (combo.SelectedItem as ComboBoxItem)?.Content?.ToString());

                var save = typeof(MuteRuleDialog).GetMethod("Save_Click", BindingFlags.Instance | BindingFlags.NonPublic)!;
                try
                {
                    save.Invoke(dialog, new object?[] { null, new RoutedEventArgs() });
                }
                catch (TargetInvocationException ex) when (ex.InnerException is InvalidOperationException)
                {
                    /* A dialog that was never shown cannot take a DialogResult: that is the last statement of a valid save. */
                }

                return dialog.Rule;
            }
            finally
            {
                dialog.Close();
            }
        }));

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("AccentButton"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            return body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }
}
