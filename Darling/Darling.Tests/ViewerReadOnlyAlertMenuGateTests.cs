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
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8547: a Viewer connected with the read-only role disables the alert write controls in the row context menus, the way the toolbar
/// buttons already were. Alert History's row menu (Mute This Alert, Mute Similar Alerts, Dismiss Selected) and the server row menu
/// (Remove, Silence, Unsilence) are built for real; the mute handler is invoked on a read-only seat to prove no Create Mute Rule dialog
/// opens.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerReadOnlyAlertMenuGateTests
{
    private const string ThemeUri = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml";
    private static readonly string[] WriteHeaders = ["_Mute This Alert...", "Mute _Similar Alerts...", "Dismiss Se_lected"];

    private static int s_dialogsOpened;

    static ViewerReadOnlyAlertMenuGateTests()
    {
        /* Counts every Create Mute Rule dialog that loads, and closes it so a dialog that does open never blocks the test. */
        EventManager.RegisterClassHandler(typeof(MuteRuleEditDialog), FrameworkElement.LoadedEvent, new RoutedEventHandler((sender, _) =>
        {
            Interlocked.Increment(ref s_dialogsOpened);
            var window = (Window)sender;
            window.Dispatcher.BeginInvoke(new Action(window.Close));
        }));
    }

    [Fact]
    public void OnAReadOnlySeat_TheAlertHistoryRowMenuDisablesItsThreeWriteItems_WithTheReadOnlyTooltip()
    {
        var items = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = BuildTab(readOnly: true);
            return Describe(RowMenu(tab));
        }));

        foreach (var header in WriteHeaders)
        {
            var (enabled, tip, showOnDisabled) = items[header];
            Assert.False(enabled, header + " stayed enabled on a read-only seat");
            Assert.Contains("read-only", tip, StringComparison.Ordinal);
            Assert.True(showOnDisabled, header + " would not show its tooltip while disabled");
        }

        Assert.True(items["Copy _Row"].Enabled, "a read-only seat can still copy a row");
    }

    [Fact]
    public void OnAWritableSeat_TheAlertHistoryRowMenuKeepsItsWriteItemsEnabled()
    {
        var items = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = BuildTab(readOnly: false);
            return Describe(RowMenu(tab));
        }));

        foreach (var header in WriteHeaders)
        {
            Assert.True(items[header].Enabled, header + " was disabled on a writable seat");
        }
    }

    [Fact]
    public void OnAReadOnlySeat_InvokingMuteThisAlertOrMuteSimilarAlerts_OpensNoDialog()
    {
        var opened = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = BuildTab(readOnly: true);
            var window = new Window { Content = tab, Width = 1000, Height = 600 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                ClippedTextProbe.Settle(window);
                var grid = (DataGrid)tab.FindName("AlertsDataGrid");
                tab.ShowAlerts(new List<ViewerAlertRow> { Row(1) });
                grid.SelectedItem = grid.Items[0];

                var menu = RowMenu(tab);
                menu.PlacementTarget = grid;
                window.Dispatcher.UnhandledException += (_, e) => e.Handled = true;
                Interlocked.Exchange(ref s_dialogsOpened, 0);
                foreach (var handler in new[] { "MuteThisAlert_Click", "MuteSimilarAlerts_Click" })
                {
                    var item = menu.Items.OfType<MenuItem>().Single(m => (m.Header as string) == (handler == "MuteThisAlert_Click" ? WriteHeaders[0] : WriteHeaders[1]));
                    typeof(AlertsHistoryTab).GetMethod(handler, BindingFlags.Instance | BindingFlags.NonPublic)!
                        .Invoke(tab, [item, new RoutedEventArgs()]);
                }

                StaTestThread.WaitForIdle(window.Dispatcher, TimeSpan.FromSeconds(10));
                return Volatile.Read(ref s_dialogsOpened);
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.Equal(0, opened);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public void TheServerRowMenu_DisablesRemoveSilenceAndUnsilenceOnAReadOnlySeat_AndKeepsTheSilencePairExclusiveOtherwise(bool readOnly, bool silenced)
    {
        var (remove, silence, unsilence) = StaTestThread.Run(() =>
        {
            var menu = new ContextMenu();
            menu.Items.Add(new MenuItem { Header = "_Remove", Tag = "RemoveServer" });
            menu.Items.Add(new MenuItem { Header = "_Silence This Server", Tag = "SilenceServer" });
            menu.Items.Add(new MenuItem { Header = "_Unsilence", Tag = "UnsilenceServer" });
            MainWindow.ApplyServerRowWriteGate(menu, silenced, readOnly);
            var items = menu.Items.OfType<MenuItem>().ToList();
            return (items[0].IsEnabled, items[1].IsEnabled, items[2].IsEnabled);
        });

        Assert.Equal(!readOnly, remove);
        Assert.Equal(!readOnly && !silenced, silence);
        Assert.Equal(!readOnly && silenced, unsilence);
    }

    /* The service is never connected: the probe that sets IsReadOnly needs a live store, so the test sets it. */
    private static AlertsHistoryTab BuildTab(bool readOnly)
    {
        var service = new ViewerDataService("Host=localhost;Port=1;Database=darling;Timeout=1");
        typeof(ViewerDataService).GetProperty(nameof(ViewerDataService.IsReadOnly))!.SetValue(service, readOnly);
        /* A known schema version, so a handler that did reach the dialog would not wait on a store read first. */
        typeof(ViewerDataService).GetField("_cachedStoreSchemaVersion", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(service, 9999);
        var tab = new AlertsHistoryTab();
        tab.Initialize(service);
        return tab;
    }

    private static ContextMenu RowMenu(AlertsHistoryTab tab) => (ContextMenu)tab.FindResource("DataGridContextMenu");

    private static Dictionary<string, (bool Enabled, string Tip, bool ShowOnDisabled)> Describe(ContextMenu menu) =>
        menu.Items.OfType<MenuItem>().ToDictionary(
            m => (string)m.Header,
            m => (m.IsEnabled, m.ToolTip as string ?? "", ToolTipService.GetShowOnDisabled(m)));

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
