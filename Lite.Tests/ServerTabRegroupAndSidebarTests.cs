/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Automation.Peers;
using System.Windows.Controls;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8169: Lite groups its server tabs as the Viewer does (CPU Scheduler under CPU, Plan Cache under Memory, 19 tabs), and its sidebar
/// footer shows four icon buttons with the other nine actions in a "More" menu. Everything is asserted from the real controls the
/// shared STA thread builds and renders: the tab headers and sub-tab content, the accessible names a screen reader or the UI walk
/// reads, and the sidebar's search filter.
/// </summary>
[Collection("credential-manager")]
public sealed class ServerTabRegroupAndSidebarTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    [Fact]
    public void TheServerTab_HasNineteenTabs_WithCpuSchedulerUnderCpuAndPlanCacheUnderMemory()
    {
        var dir = Path.Combine(Path.GetTempPath(), "ServerTabRegroup_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var (top, cpu, memory, schedulerReachable, planCacheReachable) = StaTestThread.Run(() => WithTheme(() =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "regroup-test", DisplayName = "regroup-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = 1280, Height = 800, Left = 0, Top = 0 };
                window.Show();
                try
                {
                    ClippedTextProbe.Settle(window);
                    var main = (TabControl)tab.FindName("MainTabControl");
                    var headers = main.Items.OfType<TabItem>().Select(t => (string)t.Header).ToList();
                    var cpuTab = main.Items.OfType<TabItem>().Single(t => (string)t.Header == "CPU");
                    var memoryTab = main.Items.OfType<TabItem>().Single(t => (string)t.Header == "Memory");
                    var cpuSubs = ((TabControl)cpuTab.Content).Items.OfType<TabItem>().Select(t => (string)t.Header).ToList();
                    var memorySubs = ((TabControl)memoryTab.Content).Items.OfType<TabItem>().Select(t => (string)t.Header).ToList();
                    return (headers, cpuSubs, memorySubs,
                        tab.FindName("CpuSchedulerGrid") is DataGrid && tab.FindName("CpuSchedulerChart") is not null,
                        tab.FindName("PlanCacheChart") is not null && tab.FindName("PlanCacheTotalPlansText") is not null);
                }
                finally
                {
                    window.Close();
                }
            }));

            Assert.Equal(19, top.Count);
            Assert.DoesNotContain("CPU Scheduler", top);
            Assert.DoesNotContain("Plan Cache", top);
            Assert.Contains("CPU Scheduler", cpu);
            Assert.Contains("Plan Cache", memory);
            Assert.True(schedulerReachable, "the CPU Scheduler grid and chart keep their names");
            Assert.True(planCacheReachable, "the Plan Cache chart and summary keep their names");
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public void TheSidebarFooter_ShowsFourIconButtons_AndMoreHoldsTheOtherNineUnderTheirOldNames()
    {
        var (buttons, icons, moreItems, height) = StaTestThread.Run(() => WithTheme(() =>
        {
            var footer = new SidebarFooter { VerticalAlignment = VerticalAlignment.Top };
            var window = new Window { Content = footer, Width = 280, Height = 400, Left = 0, Top = 0 };
            window.Show();
            try
            {
                ClippedTextProbe.Settle(window);
                var shown = ClippedTextProbe.Descendants(footer).OfType<Button>().ToList();
                var names = shown.Select(b => UIElementAutomationPeer.CreatePeerForElement(b).GetName()).ToList();
                var left = new List<bool>();
                foreach (var b in shown.Where(b => b.Content is Panel))
                {
                    var texts = ClippedTextProbe.Descendants(b).OfType<TextBlock>().ToList();
                    var iconX = texts[0].TransformToAncestor(b).Transform(new Point(0, 0)).X;
                    var labelX = texts[1].TransformToAncestor(b).Transform(new Point(0, 0)).X;
                    left.Add(iconX < labelX);
                }

                var more = shown.Single(b => UIElementAutomationPeer.CreatePeerForElement(b).GetName() == "More");
                var menu = more.ContextMenu ?? throw new Xunit.Sdk.XunitException("More has no menu");
                var moreItems = menu.Items.OfType<MenuItem>()
                    .Select(m => UIElementAutomationPeer.CreatePeerForElement(m).GetName()).ToList();
                return (names.Where(n => n != "More").ToList(), left, moreItems, footer.ActualHeight);
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.Equal(new[] { "Add Server", "Manage Servers", "Investigations", "Settings" }, buttons);
        Assert.All(icons, onLeft => Assert.True(onLeft, "each footer button has its icon to the left of its label"));
        Assert.Equal(
            new[] { "Add Multiple Servers…", "Manage Tags", "Manage Query Tags", "Open Plan Viewer", "Import Data", "Import Settings", "View Log", "Open Log Folder", "About" }.OrderBy(n => n),
            moreItems.OrderBy(n => n));
        Assert.True(height <= 120, $"the footer is {height:F0} DIPs tall; it must leave room for six server rows in a 900x600 window");
    }

    [Fact]
    public void TheSidebarSearch_FiltersTheServerListByName_AndClearingItRestoresAllFive()
    {
        var servers = new[] { "alpha-sql", "bravo-sql", "charlie-sql", "delta-db", "echo-db" }
            .Select((n, i) => new FleetServer(i + 1, n, false, null)).ToList();
        var fleet = new FleetView();
        fleet.SetAll(servers);

        fleet.SetSearch("sql");
        Assert.Equal(new[] { "alpha-sql", "bravo-sql", "charlie-sql" },
            fleet.Visible.OfType<FleetServerRow>().Select(r => r.Server.DisplayName));

        fleet.SetSearch("  ");
        Assert.Equal(5, fleet.Visible.OfType<FleetServerRow>().Count());
    }

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
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
