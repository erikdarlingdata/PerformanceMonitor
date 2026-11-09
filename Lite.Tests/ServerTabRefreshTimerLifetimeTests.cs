/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8367: a server tab's auto-refresh timer lives as long as the tab is in a window. A <c>DispatcherTimer</c> is rooted by its
/// dispatcher, so a tab whose window has closed would otherwise keep polling on that thread for the life of the process. A tab
/// switch inside a <c>TabControl</c> unloads the tab too, and that must not stop the timer: the tab is still in its window and
/// shows again on the next click.
/// </summary>
[Collection("credential-manager")]
public sealed class ServerTabRefreshTimerLifetimeTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    [Fact]
    public void AServerTab_WhoseWindowHasClosed_LeavesNoRunningRefreshTimer()
    {
        var (runningWhileShown, runningAfterClose) = WithTab(tab =>
        {
            var timer = TimerOf(tab);
            var window = PageLayoutProbe.Open(tab, 1000, 700);
            var runningWhileShown = timer.IsEnabled;
            window.Close();
            return (runningWhileShown, timer.IsEnabled);
        });

        Assert.True(runningWhileShown, "the auto-refresh timer is not running while the tab is shown with auto-refresh on");
        Assert.False(runningAfterClose, "the auto-refresh timer is still running after the tab's window closed");
    }

    [Fact]
    public void AServerTab_SwitchedAwayFromInsideATabControl_KeepsItsRefreshTimer_UntilItsWindowCloses()
    {
        var seen = WithTab(tab =>
        {
            var timer = TimerOf(tab);
            var tabs = new TabControl();
            var own = new TabItem { Header = "server", Content = tab };
            var other = new TabItem { Header = "other", Content = new TextBlock { Text = "other" } };
            tabs.Items.Add(own);
            tabs.Items.Add(other);
            tabs.SelectedItem = own;
            var window = PageLayoutProbe.Open(tabs, 1000, 700);
            var loadedWhileSelected = tab.IsLoaded;

            tabs.SelectedItem = other;
            PageLayoutProbe.Settle(window);
            var unloadedBySwitch = !tab.IsLoaded;
            var runningAfterSwitch = timer.IsEnabled;

            tabs.SelectedItem = own;
            PageLayoutProbe.Settle(window);
            var runningAfterReturn = timer.IsEnabled;

            window.Close();
            return (loadedWhileSelected, unloadedBySwitch, runningAfterSwitch, runningAfterReturn, runningAfterClose: timer.IsEnabled);
        });

        Assert.True(seen.loadedWhileSelected, "the selected tab is not loaded, so the switch away cannot unload it");
        Assert.True(seen.unloadedBySwitch, "switching to the other tab did not unload the server tab, so this does not exercise Unloaded");
        Assert.True(seen.runningAfterSwitch, "switching to another tab stopped the auto-refresh timer");
        Assert.True(seen.runningAfterReturn, "the auto-refresh timer is not running after switching back to the tab");
        Assert.False(seen.runningAfterClose, "the auto-refresh timer is still running after the window closed");
    }

    [Fact]
    public void StopRefresh_LetsGoOfTheHostWindow()
    {
        var seen = WithTab(tab =>
        {
            var timer = TimerOf(tab);
            var window = PageLayoutProbe.Open(tab, 1000, 700);
            var hookedWhileShown = HostWindowOf(tab) is not null;
            tab.StopRefresh();
            var hookedAfterStop = HostWindowOf(tab) is not null;
            /* A timer started again after StopRefresh shows whether the window's Closed still reaches the tab. */
            timer.Start();
            window.Close();
            return (hookedWhileShown, hookedAfterStop, runningAfterClose: timer.IsEnabled);
        });

        Assert.True(seen.hookedWhileShown, "the shown tab did not hook its window");
        Assert.False(seen.hookedAfterStop, "StopRefresh left the tab hooked to its window's Closed event, which keeps a closed tab alive for the window's life");
        Assert.True(seen.runningAfterClose, "the window's Closed event still reached the tab after StopRefresh");
    }

    private static Window? HostWindowOf(ServerTab tab) =>
        (typeof(ServerTab).GetField("_hostWindow", Instance) ?? throw new Xunit.Sdk.XunitException("ServerTab has no _hostWindow field")).GetValue(tab) as Window;

    private static DispatcherTimer TimerOf(ServerTab tab) =>
        typeof(ServerTab).GetField("_refreshTimer", Instance)?.GetValue(tab) as DispatcherTimer
        ?? throw new Xunit.Sdk.XunitException("ServerTab has no _refreshTimer field holding a DispatcherTimer");

    /// <summary>Builds the real tab with auto-refresh on, as a fresh install has it, on the shared STA thread under the Dark theme.</summary>
    private static T WithTab<T>(Func<ServerTab, T> body)
    {
        var dir = Path.Combine(Path.GetTempPath(), "RefreshTimerLifetime_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            duckDb.InitializeAsync().GetAwaiter().GetResult();
            return StaTestThread.Run(() => PageLayoutProbe.WithTheme("DarkTheme", () =>
            {
                var autoRefreshWasOn = App.AutoRefreshEnabled;
                App.AutoRefreshEnabled = true;
                try
                {
                    var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "refresh-timer-test", DisplayName = "refresh-timer-test", IsEnabled = true };
                    var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                    /* Switching to a tab would start that tab's load against a server that is not there. */
                    typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                    return body(tab);
                }
                finally
                {
                    App.AutoRefreshEnabled = autoRefreshWasOn;
                }
            }));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
