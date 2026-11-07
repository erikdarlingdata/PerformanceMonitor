/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8223 (part 3): the Viewer's twin of Lite's GridPolishRound2Tests. The real <see cref="ViewerServerTab"/> and <see cref="FinOpsTab"/> are built with
/// each Viewer theme loaded, a grid is shown at the width the tab has beside the 280 px sidebar of a window that wide (<see cref="GridWidth"/>), sample
/// rows are bound, and everything is read from the realized controls (<see cref="GridPolishProbe"/>): where each column starts, whether its header and
/// its values end on the same edge, whether a header carries the filter button, and whether long text is trimmed with an ellipsis and a tooltip.
/// </summary>
public sealed class ViewerGridPolishRound2Tests
{
    private static readonly string[] Themes = { "DarkTheme", "LightTheme", "CoolBreezeTheme" };
    private static readonly double[] WindowWidths = { 1280, 900 };
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    /// <summary>The tab's width in a window of <paramref name="window"/> px: the 280 px sidebar and the tab's own margin come off.</summary>
    private static double GridWidth(double window) => window - 300;

    private const string LongName = "Granted Workspace Memory (KB) of the very long counter name that no column of any width is wide enough to show in full";

    private sealed record View(string Label, string Grid, double Window, double Viewport, List<GridPolishProbe.Col> Cols);

    private sealed record Case(string Grid, Type Row, Dictionary<string, object?>? Values = null);

    private static readonly Dictionary<string, object?> NoTag = new() { ["QueryTag"] = null };

    /// <summary>Builds the tab named <paramref name="tabKind"/> in each theme and reads each case's grid at each window width.</summary>
    private static List<View> Views(string tabKind, params Case[] cases)
    {
        var views = new List<View>();
        foreach (var theme in Themes)
        {
            views.AddRange(StaTestThread.Run(() => WithTheme(theme, () =>
            {
                FrameworkElement tab = tabKind == "FinOps" ? new FinOpsTab() : NewServerTab();
                var found = new List<View>();
                foreach (var c in cases)
                {
                    var grid = GridColumnLayoutProbe.Detach(tab, c.Grid);
                    foreach (var window in WindowWidths)
                    {
                        var rows = GridPolishProbe.Rows(c.Row, 3, c.Values);
                        var cols = GridPolishProbe.Measure(grid, rows, GridWidth(window), GridPolishProbe.PngPath("Viewer" + tabKind, c.Grid, theme, window));
                        found.Add(new View($"{theme} {window:0}", c.Grid, window, GridWidth(window), cols));
                    }
                }

                return found;
            })));
        }

        return views;
    }

    /// <summary>The real server tab against a store that is not there; a refresh would start a load, so the refresh loop is told one is running.</summary>
    private static ViewerServerTab NewServerTab()
    {
        var tab = new ViewerServerTab(
            new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
            new DarlingServer(1, "grid-polish", "grid-polish", true, null));
        typeof(ViewerServerTab).GetField("_refreshInFlight", Instance)!.SetValue(tab, true);
        return tab;
    }

    private static T WithTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var saved = app.Resources.MergedDictionaries.ToList();
        app.Resources.MergedDictionaries.Clear();
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{theme}.xaml") });
        try
        {
            return body();
        }
        finally
        {
            app.Resources.MergedDictionaries.Clear();
            foreach (var dictionary in saved)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }
        }
    }

    private static GridPolishProbe.Col Col(View view, string header) =>
        view.Cols.FirstOrDefault(c => c.Header == header) ?? throw new InvalidOperationException($"{view.Grid} has no column '{header}' ({view.Label}): " + string.Join(", ", view.Cols.Select(c => c.Header)));

    private static void Empty(List<string> problems, string what) =>
        Assert.True(problems.Count == 0, what + Environment.NewLine + string.Join(Environment.NewLine, problems.Distinct().Take(40)));

    [Fact]
    public void TheFirstNumberColumn_StartsInView_WhenTheWindowIs900Wide()
    {
        var problems = new List<string>();
        var firstNumber = new Dictionary<string, string>
        {
            ["FinOpsApplicationConnectionsDataGrid"] = "Avg Connections",
            ["FinOpsServerInventoryDataGrid"] = "Logical CPUs",
            ["FinOpsHighImpactDataGrid"] = "Executions",
        };
        foreach (var view in Views("FinOps",
            new Case("FinOpsApplicationConnectionsDataGrid", typeof(ApplicationConnectionRow)),
            new Case("FinOpsServerInventoryDataGrid", typeof(ServerPropertyRow)),
            new Case("FinOpsHighImpactDataGrid", typeof(HighImpactQueryRow))))
        {
            var col = Col(view, firstNumber[view.Grid]);
            if (col.Left + col.Width / 2 > view.Viewport)
            {
                problems.Add($"{view.Grid} at {view.Label}: '{col.Header}' starts at {col.Left:0} in a {view.Viewport:0} px viewport");
            }
        }

        var firstNumberServer = new Dictionary<string, string>
        {
            ["ServerConfigChangesGrid"] = "Old Configured",
            ["DatabaseConfigChangesGrid"] = "Old Value",
        };
        foreach (var view in Views("Server",
            new Case("ServerConfigChangesGrid", typeof(ServerConfigChangeRow)),
            new Case("DatabaseConfigChangesGrid", typeof(DatabaseConfigChangeRow))))
        {
            var first = view.Cols[0];
            var col = Col(view, firstNumberServer[view.Grid]);
            if (first.Width > 200 || col.Left + col.Width / 2 > view.Viewport)
            {
                problems.Add($"{view.Grid} at {view.Label}: '{first.Header}' is {first.Width:0} wide and '{col.Header}' starts at {col.Left:0} in a {view.Viewport:0} px viewport");
            }
        }

        Empty(problems, "A first column keeps the number columns out of view:");
    }

    [Fact]
    public void TheTagColumns_StartNarrow_WhenNoQueryHasATag()
    {
        var problems = new List<string>();
        foreach (var view in Views("Server",
            new Case("QueryStatsGrid", typeof(ViewerQueryStatsRow), NoTag),
            new Case("ProcedureStatsGrid", typeof(ViewerProcedureStatsRow), NoTag),
            new Case("QueryStoreGrid", typeof(ViewerQueryStoreRow), NoTag),
            new Case("LongQueryCompletionsGrid", typeof(ViewerLongQueryRow), NoTag)))
        {
            var tag = Col(view, "Tag");
            if (tag.Width > 90)
            {
                problems.Add($"{view.Grid} at {view.Label}: Tag is {tag.Width:0} px wide with no tag on any row");
            }
        }

        Empty(problems, "A blank column takes room from the numbers:");
    }

    [Fact]
    public void NumberColumnsAndTheirHeaders_AreRightAligned()
    {
        var problems = new List<string>();
        var numbers = new Dictionary<string, string[]>
        {
            ["ServerConfigGrid"] = new[] { "Configured Value", "Value In Use" },
            ["DatabaseScopedConfigGrid"] = new[] { "Value", "Value for Secondary" },
            ["QueryStoreGrid"] = new[] { "Query ID", "Executions", "Total Duration (ms)" },
            ["DatabaseConfigGrid"] = new[] { "Compat Level" },
        };
        foreach (var view in Views("Server",
            new Case("ServerConfigGrid", typeof(ServerConfigRow), new() { ["ValueConfigured"] = 5L, ["ValueInUse"] = 5L }),
            new Case("DatabaseScopedConfigGrid", typeof(DatabaseScopedConfigRow), new() { ["Value"] = "1", ["ValueForSecondary"] = "0" }),
            new Case("QueryStoreGrid", typeof(ViewerQueryStoreRow), NoTag),
            new Case("DatabaseConfigGrid", typeof(DatabaseConfigRow))))
        {
            foreach (var name in numbers[view.Grid])
            {
                var col = Col(view, name);
                if (col.ValueGapRight is not { } value || value > 16)
                {
                    problems.Add($"{view.Grid} at {view.Label}: values of '{name}' end {col.ValueGapRight:0} px from the cell edge");
                }
                else if (Math.Abs(col.HeaderGapRight - value) > 24)
                {
                    problems.Add($"{view.Grid} at {view.Label}: header '{name}' ends {col.HeaderGapRight:0} px from its edge, its values {value:0}");
                }
            }
        }

        Empty(problems, "Numbers and headers on different edges:");
    }

    [Fact]
    public void EveryColumn_HasAFilterButton_InLockingLongQueriesAndTheQueryGrids()
    {
        var problems = new List<string>();
        foreach (var view in Views("FinOps", new Case("FinOpsIndexLockingDataGrid", typeof(IndexLockingRow)))
            .Concat(Views("Server",
                new Case("LongQueryCompletionsGrid", typeof(ViewerLongQueryRow)),
                new Case("QueryStatsGrid", typeof(ViewerQueryStatsRow)),
                new Case("ProcedureStatsGrid", typeof(ViewerProcedureStatsRow)),
                new Case("QueryStoreGrid", typeof(ViewerQueryStoreRow)))))
        {
            foreach (var col in view.Cols.Where(c => c.Header.Length > 0 && c.Header != "Query Plan" && !c.Filter))
            {
                problems.Add($"{view.Grid} at {view.Label}: '{col.Header}' has no filter button");
            }
        }

        Empty(problems, "Columns without a filter button:");
    }

    [Fact]
    public void LongNames_EndInAnEllipsis_AndShowInFullInATooltip()
    {
        var problems = new List<string>();
        var named = new Dictionary<string, string[]>
        {
            ["MemoryBrokerGrid"] = new[] { "Broker", "Notification" },
            ["DatabaseScopedConfigGrid"] = new[] { "Setting" },
        };
        foreach (var view in Views("Server",
            new Case("MemoryBrokerGrid", typeof(MemoryBrokerRow), new() { ["Broker"] = LongName, ["Notification"] = LongName }),
            new Case("DatabaseScopedConfigGrid", typeof(DatabaseScopedConfigRow), new() { ["ConfigurationName"] = LongName })))
        {
            foreach (var name in named[view.Grid])
            {
                CutText(problems, view, name);
            }
        }

        foreach (var view in Views("FinOps", new Case("FinOpsServerInventoryDataGrid", typeof(ServerPropertyRow),
            new() { ["Edition"] = LongName, ["HostOsVersion"] = LongName, ["ServerName"] = LongName, ["ProductVersion"] = LongName })))
        {
            foreach (var name in new[] { "Server", "Edition", "Version", "Host OS" })
            {
                CutText(problems, view, name);
            }
        }

        Empty(problems, "Text cut with no ellipsis or no tooltip:");
    }

    private static void CutText(List<string> problems, View view, string header)
    {
        var col = Col(view, header);
        if (col.ValueNaturalWidth <= col.CellWidth - 16)
        {
            return;
        }

        if (!col.Trimmed || col.Tooltip != LongName)
        {
            problems.Add($"{view.Grid} at {view.Label}: '{header}' is cut at {col.CellWidth - 16:0} px of {col.ValueNaturalWidth:0}, trimmed {col.Trimmed}, tooltip {(col.Tooltip is null ? "none" : "set")}");
        }
    }

    [Fact]
    public void ThePerfmonCounterList_TrimsALongCounterName_AndShowsItInFullInATooltip()
    {
        var problems = StaTestThread.Run(() => WithTheme("DarkTheme", () =>
        {
            var found = new List<string>();
            var tab = NewServerTab();
            var list = (ListBox)tab.FindName("PerfmonCountersList");
            var parent = (Panel)list.Parent;
            parent.Children.Remove(list);
            var window = new Window { Content = list, Width = 280, Height = 400, Left = 0, Top = 0 };
            list.ItemsSource = new[] { new SelectableItem { DisplayName = LongName, IsSelected = false } };
            window.Show();
            try
            {
                ClippedTextProbe.Settle(window);
                var block = ClippedTextProbe.Descendants(list).OfType<TextBlock>().First(b => b.Text == LongName);
                if (block.TextTrimming == TextTrimming.None || !block.IsVisible || (block.ToolTip ?? ToolTipOf(block)) as string != LongName)
                {
                    found.Add($"the counter name is {ClippedTextProbe.NaturalWidth(block):0} px of text in {block.ActualWidth:0} px, trimmed {block.TextTrimming}, tooltip {block.ToolTip ?? ToolTipOf(block) ?? "none"}");
                }

                var scroll = ClippedTextProbe.Descendants(list).OfType<ScrollViewer>().FirstOrDefault();
                if (scroll is not null && scroll.ComputedHorizontalScrollBarVisibility == Visibility.Visible)
                {
                    found.Add("the counter list shows a horizontal scroll bar");
                }
            }
            finally
            {
                window.Content = null;
                window.Close();
            }

            return found;
        }));

        Empty(problems, "The Perfmon counter list:");
    }

    private static object? ToolTipOf(DependencyObject block)
    {
        for (var p = System.Windows.Media.VisualTreeHelper.GetParent(block); p is not null; p = System.Windows.Media.VisualTreeHelper.GetParent(p))
        {
            if (p is FrameworkElement { ToolTip: not null } fe)
            {
                return fe.ToolTip;
            }
        }

        return null;
    }
}
