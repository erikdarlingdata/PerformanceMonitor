/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Darling.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's twin of Lite's dialog dimension test: a Database State alert carries its database, so the rule it saves is for
/// that database alone, and a Failed Agent Job alert offers the Job Name box the engine's check reads.
/// </summary>
public sealed class ViewerMuteRuleReviewRound1Tests
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
        var shown = StaTestThread.Run(() =>
        {
            var dialog = new MuteRuleEditDialog(new AlertMuteContext { ServerName = "S", MetricName = "Failed Agent Job", JobName = "nightly" }, scheduleSupported: false);
            try
            {
                return ((TextBox)dialog.FindName("JobNamePatternBox")).Visibility;
            }
            finally
            {
                dialog.Close();
            }
        });

        Assert.Equal(Visibility.Visible, shown);
    }

    private static MuteRule SaveFrom(AlertMuteContext context) =>
        StaTestThread.Run(() =>
        {
            var dialog = new MuteRuleEditDialog(context, scheduleSupported: false);
            try
            {
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
