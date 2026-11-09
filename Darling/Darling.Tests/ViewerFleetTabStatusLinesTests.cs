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
/// #8379: each fleet tab owns its status line. A fleet tab's refresh catches its own read failure and raises its failure line through
/// its status event, so the bar must still read that line once the refresh returns, not "&lt;tab&gt; — refreshed …" written over it by the
/// window. The real main window is built on the shared STA thread with the Dark theme and hosted at 1000x700, inside a 1024x768 screen;
/// a store at a closed local port makes every read fail without a server.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerFleetTabStatusLinesTests
{
    private const string ThemeUri = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml";

    /// <summary>A store nothing listens on: every read fails after its connection attempt, asynchronously.</summary>
    private const string ClosedStore = "Host=127.0.0.1;Port=1;Database=darling;Username=darling;Timeout=5";

    private static readonly BindingFlags Instance = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    /// <summary>The fleet tabs the window refreshes when they are selected, with the prefix of the failure line each raises when its
    /// read fails. Fleet Sweeps runs three reads and the last to fail writes the line, so its prefix is the stem the three share; Job
    /// History's best-effort choices reads can land after its grid read, so its prefix is the stem the two share.</summary>
    public static IEnumerable<object[]> FleetTabs()
    {
        yield return new object[] { "AlertsTab", "AlertsHistoryContent", "failed to load alert history: " };
        yield return new object[] { "JobHistoryTabItem", "JobHistoryContent", "failed to load " };
        yield return new object[] { "DatabaseProtectionTabItem", "DatabaseProtectionContent", "failed to load the server list for database protection: " };
        yield return new object[] { "AgentAlertsTabItem", "AgentAlertsContent", "failed to load the server list for agent alerts: " };
        yield return new object[] { "FleetSweepsTabItem", "FleetSweepsContent", "failed to load the fleet sweep" };
        yield return new object[] { "IdentityCapacityTabItem", "IdentityCapacityContent", "failed to load the server list for identity capacity: " };
        yield return new object[] { "AvailabilityGroupsTabItem", "AvailabilityGroupsContent", "Availability Groups failed to load: " };
        yield return new object[] { "FinOpsTab", "FinOpsContent", "refresh failed: " };
    }

    [Theory]
    [MemberData(nameof(FleetTabs))]
    public void AFleetTabsFailedRead_ReadsItsFailureLine_WithTheTabSelected(string tabName, string contentName, string failurePrefix)
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
                    var tab = (TabItem)w.FindName(tabName);
                    var status = (TextBlock)w.FindName("StatusText");
                    ((UIElement)w.FindName("EmptyStatePanel")).Visibility = Visibility.Collapsed;
                    tabs.Visibility = Visibility.Visible;
                    tab.Visibility = Visibility.Visible;

                    /* The tab is selected with no store, so nothing loads yet; then the store arrives, wired to the tab the way the
                       window wires it when it connects, and the window's refresh of the selected tab runs the tab's read, which fails. */
                    var service = new ViewerDataService(ClosedStore);
                    GiveStore(w.FindName(contentName), service);
                    tabs.SelectedItem = tab;
                    ClippedTextProbe.Settle(host);
                    typeof(MainWindow).GetField("_dataService", Instance)!.SetValue(w, service);
                    await (Task)typeof(MainWindow).GetMethod("RefreshVisibleAsync", Instance)!.Invoke(w, null)!;
                    ClippedTextProbe.Settle(host);

                    if (!status.Text.StartsWith(failurePrefix, StringComparison.Ordinal))
                    {
                        findings.Add($"with {tabName} on screen after its read failed the status bar reads \"{status.Text}\", not \"{failurePrefix}…\"");
                    }

                    if (status.Text.Contains("— refreshed", StringComparison.Ordinal))
                    {
                        findings.Add($"with {tabName} on screen after its read failed the status bar says the tab is refreshed: \"{status.Text}\"");
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

    /// <summary>Wires the store to a fleet tab's content the way the window does when it connects; FinOps also needs a picked server, since
    /// its refresh loads nothing without one.</summary>
    private static void GiveStore(object content, ViewerDataService service)
    {
        switch (content)
        {
            case AlertsHistoryTab t:
                t.Initialize(service);
                break;
            case JobHistoryTab t:
                t.Initialize(service);
                break;
            case DatabaseProtectionTab t:
                t.Initialize(service);
                break;
            case AgentAlertsTab t:
                t.Initialize(service);
                break;
            case FleetSweepsTab t:
                t.Initialize(service);
                break;
            case IdentityCapacityTab t:
                t.Initialize(service);
                break;
            case AvailabilityGroupsTab t:
                t.Initialize(service);
                break;
            case FinOpsTab t:
                var server = new DarlingServer(5, "srv-e", "srv-e", true, null);
                t.SetServers(new[] { server }, server.ServerId);
                t.Initialize(service);
                break;
            default:
                throw new InvalidOperationException($"no fleet tab content of type {content.GetType().Name}");
        }
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
