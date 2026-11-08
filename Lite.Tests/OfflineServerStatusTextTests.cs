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
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8304: the status bar of a server tab whose server is offline. It said "Connected to X - Data loaded" beside "Collectors: N
/// erroring", and with the sidebar open at 900x600 only "Connected to <serv..." of it survived. The tab's wording for an unreachable
/// server says that the server is offline, in the register of the other status texts, and is short enough to show whole at 900x600
/// with the sidebar open, beside the four counters the bar always carries.
/// </summary>
[Collection("credential-manager")]
public sealed class OfflineServerStatusTextTests
{
    /// <summary>The status bar's wording for an unreachable server's tab, by name so this file builds whether or not the window has it yet.</summary>
    private static string OfflineText(string displayName)
    {
        var method = typeof(MainWindow).GetMethod("OfflineStatusText", BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public, [typeof(string)]);
        Assert.True(method is not null, "the status bar has no wording for an offline server's tab yet");
        return (string)method!.Invoke(null, [displayName])!;
    }

    [Fact]
    public void AnOfflineServersTab_SaysOffline_AndNotConnected()
    {
        var text = OfflineText("SQL2022");

        Assert.Contains("SQL2022", text, StringComparison.Ordinal);
        Assert.Contains("offline", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Connected", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Data loaded", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AnOfflineServersStatus_ShowsWhole_At900x600_WithTheSidebarOpen()
    {
        var text = OfflineText("SQL2022");
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
