/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8367: the Viewer twin of Lite's server tab timer lifetime. A server tab's auto-refresh timer lives as long as the tab is in a
/// window: a <c>DispatcherTimer</c> is rooted by its dispatcher, so a tab whose window has closed would otherwise keep ticking on
/// that thread for the life of the process. A tab switch inside a <c>TabControl</c> unloads the tab too, and that must not stop the
/// timer: the tab is still in its window and shows again on the next click.
/// </summary>
public sealed class ViewerServerTabRefreshTimerLifetimeTests
{
    [Fact]
    public void AViewerServerTab_WhoseWindowHasClosed_LeavesNoRunningRefreshTimer()
    {
        var (runningWhileShown, runningAfterClose) = StaTestThread.Run(() => ViewerPageLayoutProbe.WithTheme("DarkTheme", () =>
        {
            var tab = NewTab();
            var timer = TimerOf(tab);
            var window = ViewerPageLayoutProbe.Open(tab, 1000, 700);
            var runningWhileShown = timer.IsEnabled;
            window.Close();
            return (runningWhileShown, timer.IsEnabled);
        }));

        Assert.True(runningWhileShown, "the auto-refresh timer is not running while the tab is shown with auto-refresh on");
        Assert.False(runningAfterClose, "the auto-refresh timer is still running after the tab's window closed");
    }

    [Fact]
    public void AViewerServerTab_SwitchedAwayFromInsideATabControl_KeepsItsRefreshTimer_UntilItsWindowCloses()
    {
        var seen = StaTestThread.Run(() => ViewerPageLayoutProbe.WithTheme("DarkTheme", () =>
        {
            var tab = NewTab();
            var timer = TimerOf(tab);
            var tabs = new TabControl();
            var own = new TabItem { Header = "server", Content = tab };
            var other = new TabItem { Header = "other", Content = new TextBlock { Text = "other" } };
            tabs.Items.Add(own);
            tabs.Items.Add(other);
            tabs.SelectedItem = own;
            var window = ViewerPageLayoutProbe.Open(tabs, 1000, 700);
            var loadedWhileSelected = tab.IsLoaded;

            tabs.SelectedItem = other;
            ViewerPageLayoutProbe.Settle(window);
            var unloadedBySwitch = !tab.IsLoaded;
            var runningAfterSwitch = timer.IsEnabled;

            tabs.SelectedItem = own;
            ViewerPageLayoutProbe.Settle(window);
            var runningAfterReturn = timer.IsEnabled;

            window.Close();
            return (loadedWhileSelected, unloadedBySwitch, runningAfterSwitch, runningAfterReturn, runningAfterClose: timer.IsEnabled);
        }));

        Assert.True(seen.loadedWhileSelected, "the selected tab is not loaded, so the switch away cannot unload it");
        Assert.True(seen.unloadedBySwitch, "switching to the other tab did not unload the server tab, so this does not exercise Unloaded");
        Assert.True(seen.runningAfterSwitch, "switching to another tab stopped the auto-refresh timer");
        Assert.True(seen.runningAfterReturn, "the auto-refresh timer is not running after switching back to the tab");
        Assert.False(seen.runningAfterClose, "the auto-refresh timer is still running after the window closed");
    }

    private static DispatcherTimer TimerOf(ViewerServerTab tab) =>
        typeof(ViewerServerTab).GetField("_autoRefreshTimer", ViewerPageLayoutProbe.Instance)?.GetValue(tab) as DispatcherTimer
        ?? throw new Xunit.Sdk.XunitException("ViewerServerTab has no _autoRefreshTimer field holding a DispatcherTimer");

    /// <summary>The real tab with the default preferences, which have auto-refresh on, against a store that is not there.</summary>
    private static ViewerServerTab NewTab()
    {
        var tab = new ViewerServerTab(
            new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
            new DarlingServer(1, "refresh-timer-test", "refresh-timer-test", true, null));
        /* Switching to a tab would start that tab's load against a store that is not there. */
        typeof(ViewerServerTab).GetField("_refreshInFlight", ViewerPageLayoutProbe.Instance)!.SetValue(tab, true);
        typeof(ViewerServerTab).GetField("_suppressDrillDownAutoRefresh", ViewerPageLayoutProbe.Instance)?.SetValue(tab, true);
        return tab;
    }
}
