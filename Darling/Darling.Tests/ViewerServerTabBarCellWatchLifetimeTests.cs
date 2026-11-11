/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8566: the Viewer twin of Lite's server tab bar-cell watch lifetime. A server tab recomputes its Top Queries bar maxima when the
/// grid's rows change, through a value-changed hook on the grid's ItemsSource. A <c>DependencyPropertyDescriptor</c> keeps every
/// element it watches, and the handler, in a table that lives as long as the process, so a hook that is never removed keeps a closed
/// tab, its grids and their rows reachable from that table. These tests show that a tab closed the way the main window closes one
/// is collectable, and that the maxima still follow the rows: rows set while the tab is switched away from inside a
/// <c>TabControl</c> are counted when it is selected again, and rows set after that are counted as they arrive.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class ViewerServerTabBarCellWatchLifetimeTests
{
    [Fact]
    public void AViewerServerTab_ClosedTheWayTheMainWindowClosesOne_IsCollectable()
    {
        var alive = StaTestThread.Run(() => ViewerPageLayoutProbe.WithTheme("DarkTheme", () =>
        {
            var tabs = new TabControl();
            tabs.Items.Add(new TabItem { Header = "other", Content = new TextBlock { Text = "other" } });
            var window = ViewerPageLayoutProbe.Open(tabs, 1000, 700);
            try
            {
                var closed = ShowThenClose(tabs, window);
                Dispatcher.CurrentDispatcher.Invoke(() => { }, DispatcherPriority.ContextIdle);
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                return closed.TryGetTarget(out _);
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.False(alive, "a server tab is still reachable after it was closed and dropped: something that outlives the tab holds it, and through it every grid, chart and row the tab had");
    }

    [Fact]
    public void TheBarMaxima_FollowTheRows_SetWhileTheTabIsSwitchedAwayFrom_AndAfterItIsSelectedAgain()
    {
        var seen = StaTestThread.Run(() => ViewerPageLayoutProbe.WithTheme("DarkTheme", () =>
        {
            var tab = NewTab();
            var grid = ViewerPageLayoutProbe.Find<DataGrid>(tab, "QueryStatsGrid");
            var tabs = new TabControl();
            var own = new TabItem { Header = "server", Content = tab };
            var other = new TabItem { Header = "other", Content = new TextBlock { Text = "other" } };
            tabs.Items.Add(own);
            tabs.Items.Add(other);
            tabs.SelectedItem = own;
            var window = ViewerPageLayoutProbe.Open(tabs, 1000, 700);
            try
            {
                var loadedWhileSelected = tab.IsLoaded;
                grid.ItemsSource = Rows(cpuMs: 100);
                var whileSelected = tab.BarMaxTotalCpu;

                tabs.SelectedItem = other;
                ViewerPageLayoutProbe.Settle(window);
                var unloadedBySwitch = !tab.IsLoaded;
                grid.ItemsSource = Rows(cpuMs: 250);

                tabs.SelectedItem = own;
                ViewerPageLayoutProbe.Settle(window);
                var afterReturn = tab.BarMaxTotalCpu;

                grid.ItemsSource = Rows(cpuMs: 75);
                var afterNextRows = tab.BarMaxTotalCpu;

                return (loadedWhileSelected, whileSelected, unloadedBySwitch, afterReturn, afterNextRows);
            }
            finally
            {
                tab.Dispose();
                window.Close();
            }
        }));

        Assert.True(seen.loadedWhileSelected, "the selected tab is not loaded, so the switch away cannot unload it");
        Assert.Equal(100, seen.whileSelected);
        Assert.True(seen.unloadedBySwitch, "switching to the other tab did not unload the server tab, so this does not exercise Unloaded");
        Assert.True(seen.afterReturn == 250, $"the CPU bar maximum is {seen.afterReturn} after the tab was selected again, not the 250 of the rows the grid was given while the tab was switched away from");
        Assert.True(seen.afterNextRows == 75, $"the CPU bar maximum is {seen.afterNextRows} after new rows arrived on the tab that was selected again, not their 75: the tab no longer follows its grid's rows");
    }

    /// <summary>
    /// Builds a tab, shows it in the window's tab control and closes it as the main window's close-tab path does, in its own frame so
    /// no local of the test keeps the tab alive through the collection.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference<ViewerServerTab> ShowThenClose(TabControl tabs, Window window)
    {
        var tab = NewTab();
        var item = new TabItem { Header = "server", Content = tab };
        tabs.Items.Add(item);
        tabs.SelectedItem = item;
        ViewerPageLayoutProbe.Settle(window);
        if (!tab.IsLoaded)
        {
            throw new Xunit.Sdk.XunitException("the selected server tab is not loaded, so this does not close a tab that was shown");
        }

        tab.Dispose();
        tabs.Items.Remove(item);
        ViewerPageLayoutProbe.Settle(window);
        return new WeakReference<ViewerServerTab>(tab);
    }

    private static List<ViewerQueryStatsRow> Rows(long cpuMs) => new()
    {
        new ViewerQueryStatsRow { DatabaseName = "db1", TotalExecutions = 3, TotalCpuUs = cpuMs * 1000, TotalElapsedUs = cpuMs * 2000, TotalLogicalReads = 40 },
        new ViewerQueryStatsRow { DatabaseName = "db2", TotalExecutions = 1, TotalCpuUs = cpuMs * 500, TotalElapsedUs = cpuMs * 1000, TotalLogicalReads = 10 },
    };

    /// <summary>The real tab against a store that is not there.</summary>
    private static ViewerServerTab NewTab()
    {
        var tab = new ViewerServerTab(
            new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
            new DarlingServer(1, "bar-cell-watch-test", "bar-cell-watch-test", true, null));
        /* Switching to a tab would start that tab's load against a store that is not there. */
        typeof(ViewerServerTab).GetField("_refreshInFlight", ViewerPageLayoutProbe.Instance)!.SetValue(tab, true);
        typeof(ViewerServerTab).GetField("_suppressDrillDownAutoRefresh", ViewerPageLayoutProbe.Instance)?.SetValue(tab, true);
        return tab;
    }
}
