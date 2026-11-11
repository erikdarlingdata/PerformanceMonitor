/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8535: picking a sub-tab puts "Loading..." over each grid in it that has no rows yet (#8647), and the text goes away when a read
/// assigns rows. The watch for those rows was a value-changed hook in a table that lives as long as the process, removed only when
/// rows arrived. A grid whose read never assigned any stayed in that table, and with it the whole closed server tab and every chart
/// bitmap it held: in the field each server tab opened and closed kept 500 to 700 MB. These tests show that a grid still waiting for
/// its rows, and a server tab closed with such grids in it, are collectable once dropped.
/// </summary>
[Collection("credential-manager")]
public sealed class ServerTabLoadingTextLifetimeTests
{
    [Fact]
    public void AGridStillWaitingForItsRows_IsCollectable_OnceDropped()
    {
        var alive = StaTestThread.Run(() =>
        {
            var grid = GridWaitingForRows();
            Dispatcher.CurrentDispatcher.Invoke(() => { }, DispatcherPriority.ContextIdle);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            return grid.TryGetTarget(out _);
        });

        Assert.False(alive, "a grid showing \"Loading...\" is still reachable after it was dropped: the watch for its rows holds it, and with it whatever the grid's templates reach");
    }

    [Fact]
    public void AServerTab_ClosedWhileAPickedSubTabsGridsStillWaitForRows_IsCollectable()
    {
        var (alive, waiting) = WithStore(duckDb =>
        {
            var tabs = new TabControl();
            tabs.Items.Add(new TabItem { Header = "other", Content = new TextBlock { Text = "other" } });
            var window = PageLayoutProbe.Open(tabs, 1000, 700);
            try
            {
                var (closed, waitingGrids) = PickSubTabThenClose(duckDb, tabs, window);
                /* The chart rows' layout pass runs on a 150 ms debounce timer, which holds the tab until it fires, as it does in the app. */
                PageLayoutProbe.Pump(System.Threading.Tasks.Task.Delay(500));
                Dispatcher.CurrentDispatcher.Invoke(() => { }, DispatcherPriority.ContextIdle);
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                return (closed.TryGetTarget(out _), waitingGrids);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.True(waiting > 0, "no grid on the picked sub-tab was given \"Loading...\", so this does not close a tab with grids still waiting for rows");
        Assert.False(alive, "a server tab closed while grids on its picked sub-tab still waited for rows is still reachable: the watch for those rows holds the tab, its charts and their bitmaps");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference<DataGrid> GridWaitingForRows()
    {
        var grid = new DataGrid();
        var cell = new Grid();
        cell.Children.Add(grid);
        if (!EmptyState.ShowLoadingUntilRead(grid))
        {
            throw new Xunit.Sdk.XunitException("the grid was not given \"Loading...\", so this does not test a grid waiting for rows");
        }

        return new WeakReference<DataGrid>(grid);
    }

    /// <summary>
    /// Builds a tab, shows it, picks its Queries sub-tab (whose grids get "Loading..." and never get rows, since no read runs here) and
    /// closes it as <c>MainWindow.CloseServerTab</c> does, in its own frame so no local of the test keeps the tab alive.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference<ServerTab> Tab, int WaitingGrids) PickSubTabThenClose(DuckDbInitializer duckDb, TabControl tabs, Window window)
    {
        var tab = NewTab(duckDb);
        var item = new TabItem { Header = "server", Content = tab };
        tabs.Items.Add(item);
        tabs.SelectedItem = item;
        PageLayoutProbe.Settle(window);
        if (!tab.IsLoaded)
        {
            throw new Xunit.Sdk.XunitException("the selected server tab is not loaded, so this does not close a tab that was shown");
        }

        var main = PageLayoutProbe.Find<TabControl>(tab, "MainTabControl");
        var queries = main.Items.OfType<TabItem>().Single(t => Equals(t.Header, "Queries"));
        main.SelectedItem = queries;
        PageLayoutProbe.Settle(window);
        var waiting = Grids((DependencyObject)queries.Content).Count(EmptyState.IsShowingLoading);

        tab.StopRefresh();
        tab.DisposeChartHelpers();
        tabs.Items.Remove(item);
        PageLayoutProbe.Settle(window);
        return (new WeakReference<ServerTab>(tab), waiting);
    }

    private static System.Collections.Generic.IEnumerable<DataGrid> Grids(DependencyObject node)
    {
        if (node is DataGrid grid)
        {
            yield return grid;
            yield break;
        }

        foreach (var child in LogicalTreeHelper.GetChildren(node).OfType<DependencyObject>())
            foreach (var found in Grids(child))
                yield return found;
    }

    private static ServerTab NewTab(DuckDbInitializer duckDb)
    {
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "loading-text-lifetime-test", DisplayName = "loading-text-lifetime-test", IsEnabled = true };
        var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
        /* Switching to a tab would start that tab's load against a server that is not there. */
        typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", PageLayoutProbe.Instance)!.SetValue(tab, true);
        return tab;
    }

    /// <summary>Runs <paramref name="body"/> on the shared STA thread under the Dark theme, with an empty store for the tab to read.</summary>
    private static T WithStore<T>(Func<DuckDbInitializer, T> body)
    {
        var dir = Path.Combine(Path.GetTempPath(), "LoadingTextLifetime_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            duckDb.InitializeAsync().GetAwaiter().GetResult();
            return StaTestThread.Run(() => PageLayoutProbe.WithTheme("DarkTheme", () => body(duckDb)));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
