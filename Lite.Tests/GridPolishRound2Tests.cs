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
using System.Windows.Controls;
using System.Windows.Documents;
using Darling.Tests;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8223 (part 2): the grids a person reads at 1280x800 and 900x600, in all three themes. The real tabs are built with each theme loaded, a grid is shown
/// at the width the tab has beside the 280 px sidebar of a window that wide (<see cref="GridWidth"/>), sample rows are bound, and everything is read from
/// the realized controls (<see cref="GridPolishProbe"/>): where each column starts, whether its header and its values end on the same edge, whether a
/// header carries the filter button, and whether long text is trimmed with an ellipsis and a tooltip. The Viewer's twin is ViewerGridPolishRound2Tests.
/// </summary>
[Collection("credential-manager")]
public sealed class GridPolishRound2Tests
{
    private static readonly string[] Themes = { "DarkTheme", "LightTheme", "CoolBreezeTheme" };
    private static readonly double[] WindowWidths = { 1280, 900 };
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    /// <summary>The tab's width in a window of <paramref name="window"/> px: the 280 px sidebar and the tab's own margin come off.</summary>
    private static double GridWidth(double window) => window - 300;

    private const string LongName = "Granted Workspace Memory (KB) of the very long counter name that no column of any width is wide enough to show in full";

    private sealed record View(string Label, string Grid, double Window, double Viewport, List<GridPolishProbe.Col> Cols, GridPolishProbe.Filler? Filler = null);

    private sealed record Case(string Grid, Type Row, Dictionary<string, object?>? Values = null);

    private static readonly Dictionary<string, object?> NoTag = new() { ["QueryTag"] = null };

    /// <summary>Builds the tab named <paramref name="tabKind"/> in each theme and reads each case's grid at each window width.</summary>
    private static List<View> Views(string tabKind, params Case[] cases)
    {
        var views = new List<View>();
        var dir = Path.Combine(Path.GetTempPath(), "GridPolish_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            foreach (var theme in Themes)
            {
                views.AddRange(StaTestThread.Run(() => WithTheme(theme, () =>
                {
                    var tab = BuildTab(tabKind, duckDb);

                    var found = new List<View>();
                    foreach (var c in cases)
                    {
                        var grid = GridColumnLayoutProbe.Detach(tab, c.Grid);
                        foreach (var window in WindowWidths)
                        {
                            var rows = GridPolishProbe.Rows(c.Row, 3, c.Values);
                            var cols = GridPolishProbe.Measure(grid, rows, GridWidth(window), GridPolishProbe.PngPath(tabKind, c.Grid, theme, window));
                            var filler = GridPolishProbe.MeasureFiller(grid, rows, GridWidth(window));
                            found.Add(new View($"{theme} {window:0}", c.Grid, window, GridWidth(window), cols, filler));
                        }
                    }

                    return found;
                })));
            }
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }

        return views;
    }

    private static FrameworkElement BuildTab(string tabKind, DuckDbInitializer duckDb)
    {
        if (tabKind == "FinOps")
        {
            return new FinOpsTab();
        }

        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "grid-polish", DisplayName = "grid-polish", IsEnabled = true };
        var serverTab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
        typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(serverTab, true);
        return serverTab;
    }

    private static T WithTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var saved = app.Resources.MergedDictionaries.ToList();
        app.Resources.MergedDictionaries.Clear();
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}.xaml") });
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
            ["ApplicationConnectionsDataGrid"] = "Avg Connections",
            ["ServerInventoryDataGrid"] = "Logical CPUs",
            ["HighImpactDataGrid"] = "Executions",
        };
        foreach (var view in Views("FinOps",
            new Case("ApplicationConnectionsDataGrid", typeof(ApplicationConnectionRow)),
            new Case("ServerInventoryDataGrid", typeof(ServerPropertyRow)),
            new Case("HighImpactDataGrid", typeof(HighImpactQueryRow))))
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

        // #8223: the first number column of these two grids must end inside the viewport (not only start there),
        // so the first text columns start narrow instead of pushing the numbers to the edge.
        var wholeNumber = new Dictionary<string, string>
        {
            ["IndexLockingDataGrid"] = "Row Lock Waits",
        };
        foreach (var view in Views("FinOps", new Case("IndexLockingDataGrid", typeof(IndexLockingRow))))
        {
            var col = Col(view, wholeNumber[view.Grid]);
            if (col.Left + col.Width > view.Viewport)
            {
                problems.Add($"{view.Grid} at {view.Label}: '{col.Header}' runs from {col.Left:0} to {col.Left + col.Width:0} in a {view.Viewport:0} px viewport");
            }
        }

        foreach (var view in Views("Server", new Case("DatabaseScopedConfigGrid", typeof(DatabaseScopedConfigRow))))
        {
            var col = Col(view, "Value");
            if (col.Left + col.Width > view.Viewport)
            {
                problems.Add($"{view.Grid} at {view.Label}: '{col.Header}' runs from {col.Left:0} to {col.Left + col.Width:0} in a {view.Viewport:0} px viewport");
            }
        }

        Empty(problems, "A first column keeps the number columns out of view:");
    }

    [Fact]
    public void TheTagColumns_StartNarrow_WhenNoQueryHasATag()
    {
        var problems = new List<string>();
        foreach (var view in Views("Server",
            new Case("QueryStoreGrid", typeof(QueryStoreRow), NoTag),
            new Case("LongQueryCompletionsGrid", typeof(LongQueryCompletionRow), NoTag)))
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
            ["QueryStoreGrid"] = new[] { "Query ID", "Executions", "Total Duration (ms)", "Compat" },
            ["DatabaseConfigGrid"] = new[] { "Compat Level", "Target Recovery (s)" },
        };
        foreach (var view in Views("Server",
            new Case("ServerConfigGrid", typeof(ServerConfigRow), new() { ["ValueConfigured"] = 5L, ["ValueInUse"] = 5L }),
            new Case("DatabaseScopedConfigGrid", typeof(DatabaseScopedConfigRow), new() { ["Value"] = "1", ["ValueForSecondary"] = "0" }),
            new Case("QueryStoreGrid", typeof(QueryStoreRow), NoTag),
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

    /// <summary>
    /// #8223: a number header is the text header beside it with only its alignment changed, in every theme, including after a live switch in Settings
    /// (<c>ThemeManager.Apply</c>): the tab is built under Dark, then switched to Light and Cool Breeze, and each number header's rendered background, text
    /// colour and padding are compared with the "Setting" header of the same grid.
    /// </summary>
    [Fact]
    public void NumberHeaders_LookLikeTheTextHeaderBesideThem_AfterALiveThemeSwitch()
    {
        var dir = Path.Combine(Path.GetTempPath(), "GridPolish_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var passes = StaTestThread.Run(() => WithTheme("DarkTheme", () =>
            {
                var previous = ThemeManager.CurrentTheme;
                ThemeManager.ThemeUriProvider = name => new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{name}Theme.xaml");
                try
                {
                    /* As the app starts: the theme comes in through ThemeManager.Apply (with the shared styles), then the tab is built. */
                    ThemeManager.Apply("Dark");
                    var grid = GridColumnLayoutProbe.Detach(BuildTab("Server", duckDb), "ServerConfigGrid");
                    var rows = GridPolishProbe.Rows(typeof(ServerConfigRow), 3, new Dictionary<string, object?> { ["ValueConfigured"] = 5L, ["ValueInUse"] = 5L });
                    return GridPolishProbe.HeaderLooksAfterThemeSwitches(grid, rows, GridWidth(1280), "Dark", new[] { "Light", "CoolBreeze" },
                        theme => GridPolishProbe.PngPath("Server", "ServerConfigGrid-after-switch", theme, 1280));
                }
                finally
                {
                    ThemeManager.Apply(previous);
                    ThemeManager.ThemeUriProvider = ThemeManager.ThemeUri;
                }
            }));

            var problems = new List<string>();
            foreach (var (theme, looks) in passes)
            {
                var text = looks.First(l => l.Header == "Setting");
                foreach (var name in new[] { "Configured Value", "Value In Use" })
                {
                    var number = looks.First(l => l.Header == name);
                    if (number.Background != text.Background || number.Foreground != text.Foreground || number.Padding != text.Padding)
                    {
                        problems.Add($"{theme}: '{name}' paints {number.Background} / {number.Foreground} / {number.Padding}, 'Setting' paints {text.Background} / {text.Foreground} / {text.Padding}");
                    }
                }
            }

            Assert.NotEqual(passes[0].Looks.First(l => l.Header == "Setting").Background, passes[1].Looks.First(l => l.Header == "Setting").Background);
            Empty(problems, "Number headers that do not match the text header after a theme switch:");
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public void EveryColumn_HasAFilterButton_InLockingLongQueriesAndQueryStore()
    {
        var problems = new List<string>();
        foreach (var (tab, view) in Views("FinOps", new Case("IndexLockingDataGrid", typeof(IndexLockingRow))).Select(v => ("FinOps", v))
            .Concat(Views("Server", new Case("LongQueryCompletionsGrid", typeof(LongQueryCompletionRow)), new Case("QueryStoreGrid", typeof(QueryStoreRow))).Select(v => ("Server", v))))
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

        foreach (var view in Views("FinOps", new Case("ServerInventoryDataGrid", typeof(ServerPropertyRow),
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
            var dir = Path.Combine(Path.GetTempPath(), "GridPolish_" + Guid.NewGuid().ToString("N")[..8]);
            Directory.CreateDirectory(dir);
            try
            {
                using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "grid-polish", DisplayName = "grid-polish", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
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
                }
                finally
                {
                    window.Content = null;
                    window.Close();
                }
            }
            finally
            {
                try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
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

    [Fact]
    public void TheUtilizationSummary_ShowsEveryPercentageWithOneDecimal_AndNoDoubleSpace()
    {
        var shown = StaTestThread.Run(() => WithTheme("DarkTheme", () =>
        {
            var tab = new FinOpsTab();
            var window = new Window { Content = tab, Width = 1280, Height = 900 };
            window.Show();
            try
            {
                var row = (UtilizationEfficiencyRow)GridPolishProbe.Rows(typeof(UtilizationEfficiencyRow), 1, new Dictionary<string, object?>
                {
                    ["ProvisioningStatus"] = "RIGHT_SIZED",
                    ["AvgCpuPct"] = 0.49m,
                    ["P95CpuPct"] = 1.0m,
                    ["MaxCpuPct"] = 4,
                    ["PhysicalMemoryMb"] = 10000,
                    ["TotalMemoryMb"] = 6060,
                    ["BufferPoolMb"] = 3800,
                    ["CpuCount"] = 8,
                    ["MaxWorkersCount"] = 256,
                    ["CurrentWorkersCount"] = 27,
                    ["CpuSamples"] = 1000L,
                })[0];
                typeof(FinOpsTab).GetMethod("UpdateUtilizationSummary", Instance)!.Invoke(tab, new object?[] { row });
                ClippedTextProbe.Settle(window);
                var texts = new[] { "AvgCpuText", "P95CpuText", "MaxCpuText", "MemoryUtilText", "MemoryRatioText" }
                    .Select(n => ((TextBlock)tab.FindName(n)).Text).ToList();
                var summary = ((Run)tab.FindName("WorkerThreadsText")).Parent as TextBlock;
                texts.Add("line: " + new TextRange(summary!.ContentStart, summary.ContentEnd).Text);
                return texts;
            }
            finally
            {
                window.Content = null;
                window.Close();
            }
        }));

        var percentages = shown.Where(t => !t.StartsWith("line: ", StringComparison.Ordinal)).ToList();
        var decimals = percentages.Select(t => t.TrimEnd('%').Contains('.') ? t.TrimEnd('%').Split('.')[1].Length : 0).Distinct().ToList();
        Assert.True(decimals.Count == 1 && decimals[0] == 1, "One decimal in every percentage of the summary: " + string.Join(" | ", percentages));
        var line = shown.Last();
        Assert.False(line.Contains("  ", StringComparison.Ordinal), "A double space in the CPU line: " + line);
    }

    [Fact]
    public void TheFirstSeenAndHighImpactDatabaseCells_EndInAnEllipsis_WhenTheTextIsWiderThanTheColumn()
    {
        var problems = new List<string>();
        void Check(View view, string header)
        {
            var col = Col(view, header);
            if (col.ValueNaturalWidth > col.CellWidth - 16 && !col.Trimmed)
            {
                problems.Add($"{view.Grid} at {view.Label}: '{header}' shows '{col.Value}' ({col.ValueNaturalWidth:0} px) in a {col.CellWidth:0} px cell with no ellipsis");
            }
        }

        foreach (var view in Views("Server",
            new Case("ServerConfigChangesGrid", typeof(ServerConfigChangeRow)),
            new Case("DatabaseConfigChangesGrid", typeof(DatabaseConfigChangeRow))))
        {
            Check(view, "First Seen");
        }

        foreach (var view in Views("FinOps", new Case("HighImpactDataGrid", typeof(HighImpactQueryRow), new() { ["DatabaseName"] = LongName })))
        {
            Check(view, "Database");
        }

        foreach (var view in Views("FinOps", new Case("DatabaseResourcesDataGrid", typeof(DatabaseResourceUsageRow), new() { ["DatabaseName"] = LongName })))
        {
            Check(view, "Database");
        }

        Empty(problems, "Cut text without an ellipsis:");
    }

    [Fact]
    public void TheSchedulerIssuesGrid_HasNoBlankHeaderCell_RightOfItsLastColumn()
    {
        var problems = new List<string>();
        foreach (var view in Views("Server", new Case("SchedulerIssuesGrid", typeof(SchedulerIssueRow))))
        {
            var last = view.Cols[^1];
            var blank = view.Viewport - (last.Left + last.Width);
            if (blank > 1 && view.Filler is { BorderRight: > 0 } filler)
            {
                problems.Add($"{view.Label}: a {blank:0} px blank header cell with a {filler.BorderRight:0} px right border follows '{last.Header}'");
            }
        }

        Empty(problems, "A blank header cell shows after the last column:");
    }

    [Fact]
    public void TheProvisioningTrendGrid_ShowsEveryCpuPercentageWithOneDecimal()
    {
        var problems = new List<string>();
        var values = new Dictionary<string, object?> { ["AvgCpuPct"] = 37.3m, ["P95CpuPct"] = 41.2m, ["MaxCpuPct"] = 98 };
        foreach (var view in Views("FinOps", new Case("ProvisioningTrendGrid", typeof(ProvisioningTrendRow), values)))
        {
            foreach (var header in new[] { "Avg SQL Server CPU %", "P95 SQL Server CPU %", "Max SQL Server CPU %" })
            {
                var shown = Col(view, header).Value;
                if (shown is null || !System.Text.RegularExpressions.Regex.IsMatch(shown, @"^\d+\.\d$"))
                {
                    problems.Add($"{view.Label}: '{header}' shows '{shown}'");
                }
            }
        }

        Empty(problems, "Percentages in the provisioning trend do not share one format:");
    }
}
