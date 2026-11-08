/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Alert History's clock and toolbar, with the real tab rendered: a list that has shown several servers keeps one clock in its header and its range label when a later read is empty;
/// and Refresh wraps together with the Dismiss buttons.
/// </summary>
[Collection("credential-manager")]
public sealed class AlertHistoryMenusAndPopupsTests
{
    [Fact]
    public void AListThatHasShownSeveralServers_KeepsOneClockInItsHeaderAndRangeLabel_WhenAReadIsEmpty()
    {
        var rememberedMode = ServerTimeHelper.CurrentDisplayMode;
        var rememberedClock = ServerTimeHelper.ActiveServerClock;
        try
        {
            var (afterRows, afterEmpty) = StaTestThread.Run(() => DialogWalkTestHost.WithTheme(() =>
            {
                ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
                ServerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(-480);
                var tab = new AlertsHistoryTab();
                var window = new Window { Content = tab, Width = 1000, Height = 600 };
                DialogWalkTestHost.Show(window);
                try
                {
                    var header = (TextBlock)tab.FindName("AlertTimeHeaderText");
                    var picker = (TimeRangePicker)tab.FindName("RangePicker");
                    tab.ShowAlerts(new List<AlertHistoryRow>
                    {
                        new() { AlertTime = new DateTime(2026, 11, 8, 12, 0, 0), ServerId = 1, ServerName = "sql-01", MetricName = "cpu" },
                        new() { AlertTime = new DateTime(2026, 11, 8, 12, 5, 0), ServerId = 2, ServerName = "sql-02", MetricName = "cpu" },
                    });
                    var first = (header.Text, picker.Zone.Id);
                    tab.ShowAlerts(new List<AlertHistoryRow>());
                    return (first, (header.Text, picker.Zone.Id));
                }
                finally
                {
                    window.Close();
                }
            }));

            Assert.Equal(("Time (local)", TimeZoneInfo.Local.Id), afterRows);
            Assert.Equal(afterRows, afterEmpty);
        }
        finally
        {
            ServerTimeHelper.CurrentDisplayMode = rememberedMode;
            ServerTimeHelper.ActiveServerClock = rememberedClock;
        }
    }

    [Fact]
    public void RefreshAndTheDismissButtons_WrapAsOneGroup()
    {
        var tops = StaTestThread.Run(() => DialogWalkTestHost.WithTheme(() =>
        {
            var tab = new AlertsHistoryTab();
            var host = new Grid { Width = 1900, HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top, Children = { tab } };
            var window = new Window { Content = new ScrollViewer { Content = host, HorizontalScrollBarVisibility = ScrollBarVisibility.Hidden }, Width = 1000, Height = 700 };
            DialogWalkTestHost.Show(window);
            try
            {
                var refresh = ClippedTextProbe.Descendants(tab).OfType<Button>().First(b => b.Content as string == "Refresh");
                var serverGroup = (FrameworkElement)((FrameworkElement)tab.FindName("ServerFilterComboBox")).Parent;
                /* Room for Refresh at the end of the first row and for nothing after it: the width that split the buttons. */
                host.Width = Math.Ceiling(DialogWalkTestHost.RightIn(serverGroup, tab) + 12 + refresh.ActualWidth + 30);
                ClippedTextProbe.Settle(window);
                var buttons = new[] { refresh, (FrameworkElement)tab.FindName("DismissSelectedButton"), (FrameworkElement)tab.FindName("DismissAllButton") };
                return buttons.Select(b => (b.Name.Length == 0 ? "Refresh" : b.Name, Math.Round(DialogWalkTestHost.TopIn(b, tab)))).ToList();
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(tops.Select(t => t.Item2).Distinct().Count() == 1, "the buttons split across rows: " + string.Join(", ", tops.Select(t => $"{t.Item1} at y={t.Item2}")));
    }
}
