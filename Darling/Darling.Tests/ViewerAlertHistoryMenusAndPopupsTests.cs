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
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's Alert History popups, clock and toolbar, with the real tab rendered: a filter popup opened by a click closes on
/// Escape in its window; a list that has shown several servers keeps one clock in its header and its range label when a later read
/// is empty; and Refresh wraps together with the Dismiss buttons.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerAlertHistoryMenusAndPopupsTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const string ThemeUri = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml";

    [Fact]
    public void AFilterPopupOpenedByAClick_ClosesOnEscapeInItsWindow()
    {
        var openAfterEscape = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = new AlertsHistoryTab();
            var window = new Window { Content = tab, Width = 1000, Height = 600 };
            StaTestThread.ShowOffScreen(window);
            ClippedTextProbe.Settle(window);
            try
            {
                var button = ClippedTextProbe.Descendants(tab).OfType<Button>().Single(b => b.Tag is string tag && tag == "ServerName");
                button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
                StaTestThread.WaitForIdle(window.Dispatcher, TimeSpan.FromSeconds(10));
                var popup = (Popup)typeof(AlertsHistoryTab).GetField("_filterPopup", Instance)!.GetValue(tab)!;
                Assert.True(popup.IsOpen, "the click did not open the filter popup");

                /* A test window is shown unactivated, so keyboard focus stays where a mouse click leaves it in the app: in the window. */
                var source = PresentationSource.FromVisual(window)!;
                window.RaiseEvent(new KeyEventArgs(Keyboard.PrimaryDevice, source, 0, Key.Escape) { RoutedEvent = Keyboard.PreviewKeyDownEvent });
                StaTestThread.WaitForIdle(window.Dispatcher, TimeSpan.FromSeconds(10));
                var open = popup.IsOpen;
                popup.IsOpen = false;
                return open;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.False(openAfterEscape, "Escape in the window left the filter popup open");
    }

    [Fact]
    public void AListThatHasShownSeveralServers_KeepsOneClockInItsHeaderAndRangeLabel_WhenAReadIsEmpty()
    {
        var rememberedMode = ViewerTimeHelper.CurrentDisplayMode;
        var rememberedClock = ViewerTimeHelper.ActiveServerClock;
        try
        {
            var (afterRows, afterEmpty) = StaTestThread.Run(() => WithTheme(() =>
            {
                ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
                ViewerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(-480);
                var tab = new AlertsHistoryTab();
                var window = new Window { Content = tab, Width = 1000, Height = 600 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    ClippedTextProbe.Settle(window);
                    var header = (TextBlock)tab.FindName("AlertTimeHeaderText");
                    var picker = (TimeRangePicker)tab.FindName("TimeRangePickerControl");
                    tab.ShowAlerts(new List<ViewerAlertRow> { Row(1), Row(2) });
                    var first = (header.Text, picker.Zone.Id);
                    tab.ShowAlerts(new List<ViewerAlertRow>());
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
            ViewerTimeHelper.CurrentDisplayMode = rememberedMode;
            ViewerTimeHelper.ActiveServerClock = rememberedClock;
        }
    }

    [Fact]
    public void RefreshAndTheDismissButtons_WrapAsOneGroup()
    {
        var tops = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = new AlertsHistoryTab();
            var host = new Grid { Width = 1900, HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top, Children = { tab } };
            var window = new Window { Content = new ScrollViewer { Content = host, HorizontalScrollBarVisibility = ScrollBarVisibility.Hidden }, Width = 1000, Height = 700 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                ClippedTextProbe.Settle(window);
                var refresh = ClippedTextProbe.Descendants(tab).OfType<Button>().First(b => b.Content as string == "Refresh");
                var serverGroup = (FrameworkElement)((FrameworkElement)tab.FindName("ServerFilterComboBox")).Parent;
                host.Width = Math.Ceiling(serverGroup.TransformToAncestor(tab).Transform(new Point(serverGroup.ActualWidth, 0)).X + 12 + refresh.ActualWidth + 30);
                ClippedTextProbe.Settle(window);
                var buttons = new[] { refresh, (FrameworkElement)tab.FindName("DismissSelectedButton"), (FrameworkElement)tab.FindName("DismissAllButton") };
                return buttons.Select(b => (b.Name.Length == 0 ? "Refresh" : b.Name, Math.Round(b.TransformToAncestor(tab).Transform(new Point(0, 0)).Y))).ToList();
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(tops.Select(t => t.Item2).Distinct().Count() == 1, "the buttons split across rows: " + string.Join(", ", tops.Select(t => $"{t.Item1} at y={t.Item2}")));
    }

    private static ViewerAlertRow Row(int serverId) => new()
    {
        AlertTime = new DateTime(2026, 11, 8, 12, serverId, 0),
        ServerId = serverId,
        ServerName = "sql-0" + serverId,
        MetricName = "cpu",
        CurrentValue = 90,
        ThresholdValue = 80,
        AlertSent = true,
        NotificationType = "email",
        Muted = false,
    };

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("BackgroundLightBrush"))
        {
            added = new ResourceDictionary { Source = new Uri(ThemeUri) };
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
