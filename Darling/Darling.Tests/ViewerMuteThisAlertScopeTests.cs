/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8350 item 1, the viewer's twin of Lite's <c>MuteThisAlertScopeTests</c>: "Mute This Alert" on an alert whose type the
/// Metric list did not name opened on "(any)" and saved a rule that muted every alert from the server. The list names every
/// alert type the engines check mute rules for, and the alert's own type is selected when the dialog opens from it.
/// </summary>
public sealed class ViewerMuteThisAlertScopeTests
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
    [InlineData("Collection Stopped")]
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
        var listed = StaTestThread.Run(() =>
        {
            var dialog = new MuteRuleEditDialog(scheduleSupported: false);
            try
            {
                return ((ComboBox)dialog.FindName("MetricCombo")).Items.Cast<ComboBoxItem>()
                    .Select(i => i.Content?.ToString() ?? "").ToArray();
            }
            finally
            {
                dialog.Close();
            }
        });

        var missing = AlertFamily.MetricFamilies.Keys
            .Where(metric => !listed.Contains(metric, StringComparer.OrdinalIgnoreCase)).ToArray();
        Assert.True(missing.Length == 0, "the Metric list lacks: " + string.Join(", ", missing));
        Assert.Equal("(any)", listed[0]);
        Assert.Equal(listed.Length, listed.Distinct(StringComparer.OrdinalIgnoreCase).Count());
    }

    [Fact]
    public void MuteThisAlert_OnACustomRuleAlert_KeepsTheRulesOwnMetricName()
    {
        const string metric = "Custom:42";
        var rule = SaveFrom(new AlertMuteContext { ServerName = "sql01", MetricName = metric });

        Assert.Equal(metric, rule.MetricName);
        Assert.False(rule.MatchesAt(new AlertMuteContext { ServerName = "sql01", MetricName = "Custom:43" }, Now));
    }

    /// <summary>Opens the dialog the way Alert History's Mute This / Mute Similar do and clicks Save without touching a field.</summary>
    private static MuteRule SaveFrom(AlertMuteContext context) =>
        StaTestThread.Run(() =>
        {
            var dialog = new MuteRuleEditDialog(context, scheduleSupported: false);
            try
            {
                /* The dialog opens on the alert's own type. Checked before Save: a dialog left on "(any)" with no server would ask
                   "mute ALL alerts?" in a native box, which would hold the shared STA thread instead of failing here. */
                var combo = (ComboBox)dialog.FindName("MetricCombo");
                Assert.Equal(context.MetricName, (combo.SelectedItem as ComboBoxItem)?.Content?.ToString());

                var save = typeof(MuteRuleEditDialog).GetMethod("Save_Click", BindingFlags.Instance | BindingFlags.NonPublic)!;
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
        });
}
