/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8133: the Viewer walkthrough items #8115 left out. The Job History fleet read takes a share of its cap from each server
/// and its count says which servers are at that share; a Fleet Sweeps watch row says its hits are the current run and its
/// first-seen column says it is the first ever sighting; the fleet Overview says it is loading during its first read. The
/// members the fix adds are reached by name, so this class builds against a tree without them and fails at the lookup.
/// </summary>
public sealed class ViewerWalkthroughLeftovers8133Tests
{
    private static readonly Assembly ViewerAssembly = typeof(FleetRollup).Assembly;

    internal static Type ViewerType(string name)
    {
        var type = ViewerAssembly.GetType("PerformanceMonitor.Darling.Viewer." + name);
        Assert.True(type is not null, $"The Viewer has no {name} type.");
        return type!;
    }

    internal static MethodInfo Method(Type type, string name, params Type[] args)
    {
        var method = type.GetMethod(name, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance, args);
        Assert.True(method is not null, $"{type.Name} has no {name}({string.Join(", ", args.Select(a => a.Name))}).");
        return method!;
    }

    private static string Constant(Type type, string name)
    {
        var field = type.GetField(name, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);
        Assert.True(field is not null, $"{type.Name} has no {name} constant.");
        return (string)field!.GetRawConstantValue()!;
    }

    // ── F5: the fleet read's cap is shared out per server ──

    [Theory]
    [InlineData(2000, 1, 2000)]
    [InlineData(2000, 10, 200)]
    [InlineData(2000, 3, 667)]
    [InlineData(2000, 0, 2000)]
    [InlineData(2000, 5000, 1)]
    public void JobHistoryPerServerShare_SplitsTheCapAcrossTheServersOffered(int cap, int servers, int expected)
    {
        var method = Method(typeof(JobHistoryCap), "PerServerShare", typeof(int), typeof(int));
        Assert.Equal(expected, (int)method.Invoke(null, new object[] { cap, servers })!);
    }

    // ── S2: a watch row's hits are the current run; the first-seen column is the first ever sighting ──

    [Theory]
    [InlineData(12, "12 consecutive hits in the current run")]
    [InlineData(1, "1 consecutive hit in the current run")]
    public void FleetSweepPosition_SaysTheHitsAreTheCurrentRun(int hits, string expected)
    {
        var node = new JsonObject
        {
            ["entry_bar_sweeps"] = 2,
            ["exit_bar_sweeps"] = 2,
            ["items"] = new JsonArray(new JsonObject
            {
                ["server_id"] = 7,
                ["server"] = "srv",
                ["item"] = "deadlocks",
                ["condition"] = "deadlocks above the bar",
                ["state"] = "carried",
                ["consecutive_hits"] = hits,
                ["consecutive_misses"] = 0,
                ["first_seen_at"] = "2026-01-01T00:00:00Z",
                ["last_seen_at"] = "2026-02-21T00:00:00Z",
            }),
        };

        var row = Assert.Single(FleetSweepsRows.WatchItems(node));
        Assert.Equal(expected, row.Position);
    }

    /// <summary>
    /// The real Fleet Sweeps tab, rendered on the shared STA thread with the app's theme, bound to one carried watch row: the
    /// first-seen column's header reads "First ever seen" and the row's Position cell reads the current-run sentence.
    /// </summary>
    [Fact]
    public void FleetSweepsGrid_RendersFirstEverSeenAndTheCurrentRun()
    {
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var theme = new ResourceDictionary { Source = new Uri(ThemeUri) };
            app.Resources.MergedDictionaries.Add(theme);
            var window = new Window { Width = 1200, Height = 700, ShowActivated = false, WindowStartupLocation = WindowStartupLocation.Manual, Left = -4000 };
            try
            {
                var tab = new FleetSweepsTab();
                window.Content = tab;
                var grid = (DataGrid)tab.FindName("WatchItemsDataGrid")!;
                grid.ItemsSource = FleetSweepsRows.WatchItems(new JsonObject
                {
                    ["entry_bar_sweeps"] = 2,
                    ["exit_bar_sweeps"] = 2,
                    ["items"] = new JsonArray(new JsonObject
                    {
                        ["server_id"] = 7, ["server"] = "srv", ["item"] = "deadlocks", ["condition"] = "deadlocks above the bar",
                        ["state"] = "carried", ["consecutive_hits"] = 12, ["consecutive_misses"] = 0,
                        ["first_seen_at"] = "2026-01-01T00:00:00Z", ["last_seen_at"] = "2026-02-21T00:00:00Z",
                    }),
                });
                StaTestThread.Settle(window);
                grid.UpdateLayout();

                var firstSeen = grid.Columns.Single(c => (c as DataGridBoundColumn)?.Binding is Binding { Path.Path: "FirstSeen" });
                Assert.Equal("First ever seen", firstSeen.Header as string);

                var position = grid.Columns.Single(c => (c as DataGridBoundColumn)?.Binding is Binding { Path.Path: "Position" });
                var row = (DataGridRow)grid.ItemContainerGenerator.ContainerFromIndex(0);
                Assert.True(row is not null, "The watch grid realized no row for its one item.");
                var cell = position.GetCellContent(row!) as TextBlock;
                Assert.True(cell is not null, "The Position cell realized no TextBlock.");
                Assert.Equal("12 consecutive hits in the current run", cell!.Text);
            }
            finally
            {
                window.Close();
                app.Resources.MergedDictionaries.Remove(theme);
            }
        });
    }

    private const string ThemeUri = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml";

    // ── F8(a): the fleet Overview says it is loading while its first read runs ──

    /// <summary>
    /// The text and the decision behind the Overview's loading state. The main window itself is not rendered here: it opens a
    /// store and the sidebar on construction, which no test fixture stands in for.
    /// </summary>
    [Fact]
    public void FleetOverview_HasALoadingTextForItsFirstRead()
    {
        var text = ViewerType("FleetOverviewLoadingText");
        Assert.Equal("Loading the fleet…", Constant(text, "Loading"));

        var shows = Method(text, "Shows", typeof(bool), typeof(int));
        Assert.True((bool)shows.Invoke(null, new object[] { true, 0 })!);
        Assert.False((bool)shows.Invoke(null, new object[] { false, 0 })!);
        Assert.False((bool)shows.Invoke(null, new object[] { true, 3 })!);
    }
}
