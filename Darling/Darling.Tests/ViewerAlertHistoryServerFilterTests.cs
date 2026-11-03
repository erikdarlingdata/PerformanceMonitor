/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7211: the Alert History Server filter only grows. A read narrowed to the picked server, or to a window where it has no
/// alerts, used to rebuild the combo from that read alone: other servers could not be picked, and a picked server with no alerts
/// vanished, leaving the combo blank while the grid (and Dismiss All) covered every server.
/// </summary>
public sealed class ViewerAlertHistoryServerFilterTests
{
    private static ViewerAlertRow Row(int serverId, string name) => new()
    {
        AlertTime = DateTime.UtcNow, ServerId = serverId, ServerName = name, MetricName = "CPU", CurrentValue = 1, ThresholdValue = 1, AlertSent = true,
        NotificationType = "email", Muted = false,
    };

    private static string[] Names(AlertsHistoryTab tab) =>
        tab.ServerFilterComboBox.Items.OfType<ComboBoxItem>().Select(i => i.Content?.ToString() ?? "").ToArray();

    [Fact]
    public void ARead_NarrowedToThePickedServer_KeepsTheOtherServersOnOffer()
    {
        StaTestThread.Run(() =>
        {
            var tab = new AlertsHistoryTab();
            tab.PopulateServerFilter([Row(1, "alpha"), Row(2, "beta")]);
            tab.ServerFilterComboBox.SelectedIndex = 2;
            Assert.Equal(2, tab.GetSelectedServerId());

            tab.PopulateServerFilter([Row(2, "beta")]);

            Assert.Equal(["All Servers", "alpha", "beta"], Names(tab));
            Assert.Equal(2, tab.GetSelectedServerId());
        });
    }

    [Fact]
    public void AWindow_WhereThePickedServerHasNoAlerts_KeepsItPickedAndListed()
    {
        StaTestThread.Run(() =>
        {
            var tab = new AlertsHistoryTab();
            tab.PopulateServerFilter([Row(1, "alpha"), Row(2, "beta")]);
            tab.ServerFilterComboBox.SelectedIndex = 2;

            tab.PopulateServerFilter([]);

            Assert.Equal(["All Servers", "alpha", "beta"], Names(tab));
            Assert.Equal(2, tab.ServerFilterComboBox.SelectedIndex);
            Assert.Equal(2, tab.GetSelectedServerId());
        });
    }

    [Fact]
    public void ANewServer_JoinsTheList_AndTheSelectionStays()
    {
        StaTestThread.Run(() =>
        {
            var tab = new AlertsHistoryTab();
            tab.PopulateServerFilter([Row(2, "beta")]);
            tab.ServerFilterComboBox.SelectedIndex = 1;

            tab.PopulateServerFilter([Row(1, "alpha"), Row(2, "beta")]);

            Assert.Equal(["All Servers", "alpha", "beta"], Names(tab));
            Assert.Equal(2, tab.GetSelectedServerId());
        });
    }
}
