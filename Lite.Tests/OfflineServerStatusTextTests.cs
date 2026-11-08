/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8304: the status bar of a server tab whose server is offline. It said "Connected to X - Data loaded" beside "Collectors: N
/// erroring", and with the sidebar open at 900x600 only "Connected to <serv..." of it survived. The tab's wording for an unreachable
/// server says that the server is offline, in the register of the other status texts, is short enough to show whole at 900x600
/// with the sidebar open beside the four counters the bar always carries, and follows the connection check while the tab stays
/// selected: a server that goes down reads Offline on the next check, and one that comes back reads Connected again.
/// </summary>
[Collection("credential-manager")]
public sealed class OfflineServerStatusTextTests
{
    [Fact]
    public void AnOfflineServersTab_SaysOffline_AndNotConnected()
    {
        var text = MainWindow.OfflineStatusText("SQL2022");

        Assert.Contains("SQL2022", text, StringComparison.Ordinal);
        Assert.Contains("offline", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Connected", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Data loaded", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AnOfflineServersStatus_ShowsWhole_At900x600_WithTheSidebarOpen()
    {
        var text = MainWindow.OfflineStatusText("SQL2022");
        var (natural, actual, trimming, lines, sidebar) = Run(w =>
        {
            var status = (TextBlock)w.FindName("StatusText");
            status.Text = text;
            ((TextBlock)w.FindName("CollectorHealthText")).Text = "Collectors: 40 erroring";
            ((TextBlock)w.FindName("DatabaseSizeText")).Text = "Database: 48.2 MB";
            ((TextBlock)w.FindName("CollectionStatusText")).Text = "Collection: 32s ago";
            ((TextBlock)w.FindName("ServerCountText")).Text = "Servers: 4";
            var host = HostOf(w);
            host.Width = 900;
            host.Height = 600;
            StaTestThread.WaitForIdle(host.Dispatcher, StaTestThread.IdleLimit);
            /* 900 wide opens on the rail; the person opens the sidebar. */
            var column = (ColumnDefinition)w.FindName("SidebarColumn");
            if (column.ActualWidth < 100)
            {
                ((Button)w.FindName("SidebarToggleButton")).RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
                host.UpdateLayout();
            }

            var one = new FormattedText("Xg", CultureInfo.CurrentCulture, FlowDirection.LeftToRight,
                new Typeface(status.FontFamily, status.FontStyle, status.FontWeight, status.FontStretch), status.FontSize, Brushes.Black,
                VisualTreeHelper.GetDpi(status).PixelsPerDip);
            var full = new FormattedText(status.Text, CultureInfo.CurrentCulture, FlowDirection.LeftToRight,
                new Typeface(status.FontFamily, status.FontStyle, status.FontWeight, status.FontStretch), status.FontSize, Brushes.Black,
                VisualTreeHelper.GetDpi(status).PixelsPerDip);
            return (full.WidthIncludingTrailingWhitespace, status.ActualWidth, status.TextTrimming, status.ActualHeight / one.Height, column.ActualWidth);
        });

        Assert.True(sidebar >= 100, $"the sidebar is {sidebar:F0} wide, not open");
        Assert.True(natural <= actual + 0.5,
            $"\"{text}\" needs {natural:F0} but the status bar gives it {actual:F0} at 900x600 with the sidebar open ({trimming}), so it is cut");
        Assert.True(lines < 1.5, $"the status text is {lines:F1} lines high");
    }

    /// <summary>
    /// The wiring: a server tab is selected, its cached connection status flips, and the status bar is re-worded the way the
    /// 30-second check re-words it. The bar follows the check in both directions while the tab stays selected.
    /// </summary>
    [Fact]
    public void TheStatusBar_FollowsTheConnectionCheck_WhileATabStaysSelected()
    {
        var dir = Path.Combine(Path.GetTempPath(), "OfflineStatusWiring_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var (selected, down, up) = Run(w =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "wiring-test", DisplayName = "wiring-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(tab, true);
                ((UIElement)w.FindName("EmptyStatePanel")).Visibility = Visibility.Collapsed;
                var tabs = (TabControl)w.FindName("ServerTabControl");
                tabs.Visibility = Visibility.Visible;
                var item = new TabItem { Header = "wiring-test", Content = tab };
                tabs.Items.Add(item);
                tabs.SelectedItem = item;
                HostOf(w).UpdateLayout();
                var status = (TextBlock)w.FindName("StatusText");
                var afterSelect = status.Text;

                var cached = ServerManagerOf(w).GetConnectionStatus(server.Id);
                cached.IsOnline = false;
                RefreshSelectedTabStatus(w);
                var afterDown = status.Text;

                cached.IsOnline = true;
                RefreshSelectedTabStatus(w);
                return (afterSelect, afterDown, status.Text);
            });

            Assert.Equal("Connected to wiring-test", selected);
            Assert.Equal(MainWindow.OfflineStatusText("wiring-test"), down);
            Assert.Equal("Connected to wiring-test", up);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>The window's connection-status cache and its selected-tab status refresh, by name so this file builds before the window has them.</summary>
    private static ServerManager ServerManagerOf(MainWindow w)
    {
        var property = typeof(MainWindow).GetProperty("ServerManager", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
        Assert.True(property is not null, "the window does not expose its connection-status cache yet");
        return (ServerManager)property!.GetValue(w)!;
    }

    private static void RefreshSelectedTabStatus(MainWindow w)
    {
        var method = typeof(MainWindow).GetMethod("WordSelectedServerTabStatus", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public, Type.EmptyTypes);
        Assert.True(method is not null, "the window has no selected-tab status refresh yet");
        method!.Invoke(w, null);
    }

    /* The window is built the way the chrome tests build it: the app's three paths point at a folder of the test's own, and the
       window's content moves into a plain host window whose size the test sets. */
    private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<MainWindow, Window> s_hosts = new();

    private static Window HostOf(MainWindow w) => s_hosts.TryGetValue(w, out var host) ? host : throw new InvalidOperationException("not hosted");

    private static T Run<T>(Func<MainWindow, T> body) =>
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var saved = app.Resources.MergedDictionaries.ToList();
            app.Resources.MergedDictionaries.Clear();
            app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") });
            var dir = Path.Combine(Path.GetTempPath(), "OfflineStatus_" + Guid.NewGuid().ToString("N")[..8]);
            Directory.CreateDirectory(dir);
            try
            {
                foreach (var (name, value) in new[] { ("DatabasePath", Path.Combine(dir, "monitor.duckdb")), ("ConfigDirectory", dir), ("SharedConfigDirectory", dir) })
                {
                    typeof(App).GetProperty(name, BindingFlags.Static | BindingFlags.Public)!.SetValue(null, value);
                }

                var w = new MainWindow();
                var root = (UIElement)w.Content;
                w.Content = null;
                var host = new Window { Content = root, Width = 900, Height = 600, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
                s_hosts.Add(w, host);
                StaTestThread.ShowOffScreen(host);
                try
                {
                    ClippedTextProbe.Settle(host);
                    return body(w);
                }
                finally
                {
                    host.Close();
                }
            }
            finally
            {
                app.Resources.MergedDictionaries.Clear();
                foreach (var d in saved)
                {
                    app.Resources.MergedDictionaries.Add(d);
                }

                try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
            }
        });
}
