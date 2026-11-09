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
/// The mute rule dialog keeps the dimensions the engine honours for the alert's type, and Lite's Metric list names only the
/// types Lite raises. A Database State alert carries its database, so the rule it saves is for that database alone; a Failed
/// Agent Job alert offers the Job Name box the engine's check reads; and a name only the Darling service raises is not offered
/// in Lite, where a rule for it could never match.
/// </summary>
public sealed class MuteRuleReviewRound1Tests
{
    private static readonly DateTime Now = new(2026, 11, 8, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void MuteThisAlert_OnADatabaseStateAlert_KeepsTheDatabase()
    {
        var rule = SaveFrom(new AlertMuteContext { ServerName = "S", MetricName = "Database State", DatabaseName = "Sales" });

        Assert.Equal("Sales", rule.DatabasePattern);
        Assert.True(rule.MatchesAt(new AlertMuteContext { ServerName = "S", MetricName = "Database State", DatabaseName = "Sales" }, Now));
        Assert.False(rule.MatchesAt(new AlertMuteContext { ServerName = "S", MetricName = "Database State", DatabaseName = "master" }, Now),
            "a rule from a Database State alert on Sales must not mute Database State for master");
    }

    [Fact]
    public void AFailedAgentJobAlert_OffersTheJobNameBox()
    {
        var shown = StaTestThread.Run(() => WithTheme(() =>
        {
            var dialog = new MuteRuleDialog(new AlertMuteContext { ServerName = "S", MetricName = "Failed Agent Job", JobName = "nightly" });
            try
            {
                return ((TextBox)dialog.FindName("JobNamePatternBox")).Visibility;
            }
            finally
            {
                dialog.Close();
            }
        }));

        Assert.Equal(Visibility.Visible, shown);
    }

    [Fact]
    public void TheMetricList_HoldsNoNameOnlyTheDarlingServiceRaises()
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

        var serviceOnly = AlertFamily.MetricFamilies
            .Where(pair => pair.Value is AlertFamily.SelfMonitor or AlertFamily.Reports or AlertFamily.External
                || pair.Key.StartsWith("PostgreSQL ", StringComparison.Ordinal))
            .Select(pair => pair.Key)
            .Where(name => listed.Contains(name, StringComparer.OrdinalIgnoreCase))
            .ToArray();
        Assert.True(serviceOnly.Length == 0, "Lite's Metric list offers names only the Darling service raises: " + string.Join(", ", serviceOnly));
        Assert.Contains("Failed Agent Job", listed);
        Assert.Contains("Server Unreachable", listed);
    }

    private static MuteRule SaveFrom(AlertMuteContext context) =>
        StaTestThread.Run(() => WithTheme(() =>
        {
            var dialog = new MuteRuleDialog(context);
            try
            {
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
        if (!app.Resources.Contains("NumericCell"))
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
            if (added is not null) app.Resources.MergedDictionaries.Remove(added);
        }
    }
}
