/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8332: the status bar keeps one line per top-level tab. A load that fails stores its failure under the tab whose load it was, not under
/// the tab selected when the failure lands (the fleet tabs and a server tab report their own read failures through their status events, so the Recommendations findings read is the one that reaches the window's catch); a server tab's slot holds the line the tab itself raises, "refresh failed: …" when its read fails, so
/// the host writes nothing over it; and an empty bar opens no tooltip. The real main window is built on the shared STA thread with the Dark theme and hosted at
/// 1000x700, inside a 1024x768 screen; a store at a closed local port makes every read fail without a server.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerStatusBarLinesTests
{
    private const string ThemeUri = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml";

    /// <summary>A store nothing listens on: every read fails after its connection attempt, asynchronously.</summary>
    private const string ClosedStore = "Host=127.0.0.1;Port=1;Database=darling;Username=darling;Timeout=5";

    private static readonly BindingFlags Instance = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    [Fact]
    public void AFailedLoad_IsStoredUnderTheTabWhoseLoadItWas_NotTheTabSelectedWhenItLanded()
    {
        var findings = new List<string>();
        StaTestThread.Run(async () =>
        {
            await WithThemeAsync(async () =>
            {
                var w = Shell();
                var host = Host(w, 1000, 700);
                try
                {
                    var tabs = (TabControl)w.FindName("MainTabs");
                    var recommendations = (TabItem)w.FindName("RecommendationsTab");
                    var alerts = (TabItem)w.FindName("AlertsTab");
                    var status = (TextBlock)w.FindName("StatusText");
                    ((UIElement)w.FindName("EmptyStatePanel")).Visibility = Visibility.Collapsed;
                    tabs.Visibility = Visibility.Visible;

                    /* Recommendations is selected with a server picked and no store, so nothing loads yet; then the store arrives and
                       its findings read starts. That read is the one load that throws to the window rather than reporting through a
                       tab's own status event. */
                    var picker = (ComboBox)w.FindName("RecommendationsServerSelector");
                    var server = new DarlingServer(4, "srv-d", "srv-d", true, null);
                    picker.ItemsSource = new[] { server };
                    picker.SelectedItem = server;
                    tabs.SelectedItem = recommendations;
                    ClippedTextProbe.Settle(host);
                    typeof(MainWindow).GetField("_dataService", Instance)!.SetValue(w, new ViewerDataService(ClosedStore));
                    var refresh = (Task)typeof(MainWindow).GetMethod("RefreshVisibleAsync", Instance)!.Invoke(w, null)!;

                    /* The reader moves on before the read fails; the queued re-run then loads Alert History. */
                    tabs.SelectedItem = alerts;
                    await refresh;
                    ClippedTextProbe.Settle(host);

                    tabs.SelectedItem = recommendations;
                    if (!status.Text.StartsWith("refresh failed", StringComparison.Ordinal))
                    {
                        findings.Add($"with Recommendations back on screen the status bar reads \"{status.Text}\", not its own failed read");
                    }
                }
                finally
                {
                    host.Close();
                }
            });
        });

        Assert.True(findings.Count == 0, string.Join(Environment.NewLine, findings));
    }

    [Fact]
    public void AServerTabsFailedRefresh_ReadsFailed_WithTheTabSelected()
    {
        var findings = new List<string>();
        StaTestThread.Run(async () =>
        {
            await WithThemeAsync(async () =>
            {
                var w = Shell();
                var host = Host(w, 1000, 700);
                try
                {
                    var tabs = (TabControl)w.FindName("MainTabs");
                    var status = (TextBlock)w.FindName("StatusText");
                    var service = new ViewerDataService(ClosedStore);
                    var serverItem = AddServerTab(w, tabs, service, new DarlingServer(3, "srv-c", "srv-c", true, null));

                    /* The server tab is selected with no store, so nothing loads yet; then the store arrives and the tab's own load runs
                       and fails, raising its "refresh failed" line through the writer wired to its TabItem. */
                    tabs.SelectedItem = serverItem;
                    ClippedTextProbe.Settle(host);
                    typeof(MainWindow).GetField("_dataService", Instance)!.SetValue(w, service);
                    await (Task)typeof(MainWindow).GetMethod("RefreshVisibleAsync", Instance)!.Invoke(w, null)!;
                    ClippedTextProbe.Settle(host);

                    if (!status.Text.StartsWith("refresh failed: ", StringComparison.Ordinal))
                    {
                        findings.Add($"with a server tab whose read failed on screen the status bar reads \"{status.Text}\"");
                    }
                }
                finally
                {
                    host.Close();
                }
            });
        });

        Assert.True(findings.Count == 0, string.Join(Environment.NewLine, findings));
    }

    [Fact]
    public void AnEmptyBar_OpensNoTooltip_AndAFilledOneCarriesItsText()
    {
        var findings = StaTestThread.Run(() => WithTheme(() =>
        {
            var w = Shell();
            var host = Host(w, 1000, 700);
            try
            {
                var status = (TextBlock)w.FindName("StatusText");
                var found = new List<string>();
                status.Text = "";
                ClippedTextProbe.Settle(host);
                if (status.ToolTip is not null)
                {
                    found.Add($"an empty bar has the tooltip \"{status.ToolTip}\"");
                }

                status.Text = "overview — refreshed 18:33:05 UTC";
                ClippedTextProbe.Settle(host);
                if (!Equals(status.ToolTip, status.Text))
                {
                    found.Add("a filled bar's tooltip is " + (status.ToolTip is null ? "unset" : "\"" + status.ToolTip + "\""));
                }

                return found;
            }
            finally
            {
                host.Close();
            }
        }));

        Assert.True(findings.Count == 0, string.Join(Environment.NewLine, findings));
    }

    /// <summary>A server tab in the main window's tab strip, the way OpenServerTab places one, with its status writer wired the same way.</summary>
    private static TabItem AddServerTab(MainWindow w, TabControl tabs, ViewerDataService service, DarlingServer server)
    {
        var tab = new ViewerServerTab(service, server);
        ((UIElement)w.FindName("EmptyStatePanel")).Visibility = Visibility.Collapsed;
        tabs.Visibility = Visibility.Visible;
        var item = new TabItem { Header = server.DisplayName, Content = tab };
        tabs.Items.Add(item);
        var writer = (Delegate)typeof(MainWindow).GetMethod("StatusWriterFor", Instance)!.Invoke(w, new object[] { item })!;
        typeof(ViewerServerTab).GetEvent("StatusChanged")!.AddEventHandler(tab, writer);
        return item;
    }

    /// <summary>The main window, built and never shown; its constructor seeds the process-wide display mode from the app settings, which is put back.</summary>
    private static MainWindow Shell()
    {
        var mode = ViewerTimeHelper.CurrentDisplayMode;
        try
        {
            return new MainWindow();
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
        }
    }

    /// <summary>A window that was never shown has no layout, so the main window's content is moved into a shown host of the given size.</summary>
    private static Window Host(MainWindow w, double width, double height)
    {
        var root = (UIElement)w.Content;
        w.Content = null;
        var host = new Window { Content = root, Width = width, Height = height, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
        StaTestThread.ShowOffScreen(host);
        ClippedTextProbe.Settle(host);
        return host;
    }

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

    private static async Task WithThemeAsync(Func<Task> body)
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
            await body();
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
