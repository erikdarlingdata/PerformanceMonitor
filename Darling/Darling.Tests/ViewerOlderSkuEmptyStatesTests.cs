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
using System.Windows.Media;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A Viewer page on a SQL Server version that cannot have its data says so, a near-flat size series reads flat, and an empty FinOps card
/// says why. Each fact builds the real control on the shared STA thread with the theme and the shared styles loaded, shows it, and reads
/// what was realized: the empty-state sentence inside a grid, a chart's Y axis and its tick labels, the message in a card, an expander's
/// header and its distance from the grid below it. The server's version comes from its registry entry, the same fact the FinOps PVS grid
/// and get_plan_corrections read.
/// </summary>
public sealed class ViewerOlderSkuEmptyStatesTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    private const string ViewerPack = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/";

    private static readonly DateTime At = new(2026, 11, 7, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>The Plan Corrections grid's own sentence for a quiet window on a server whose version has automatic tuning.</summary>
    private const string QuietWindow = "No plan corrections in the selected time window.";

    /// <summary>The sentence the FinOps "Top Databases by Avg CPU / Execution" card shows when the Total card has rows and it has none.</summary>
    private const string NoExecutions = "No database recorded an execution in the last 24 hours, so there is no CPU per execution to rank.";

    /// <summary>
    /// On a server registered as SQL Server 2016, the Automatic Tuning grid and the Plan Corrections grid both say the version has no
    /// automatic tuning, in the collector's own sentence, instead of a bare grid and a "quiet window" sentence.
    /// </summary>
    [Fact]
    public void OnA2016Server_AutomaticTuningAndPlanCorrections_SayTheVersionHasNoAutomaticTuning()
    {
        var expected = PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 13 });
        var texts = EmptyTexts(sqlMajorVersion: 13);

        Assert.Equal(expected, texts.AutomaticTuning);
        Assert.Equal(expected, texts.PlanCorrections);
    }

    /// <summary>
    /// On a server whose version has automatic tuning, an empty Automatic Tuning grid says the collector has not stored a cycle, in the
    /// collector's own sentence, and the Plan Corrections grid keeps its quiet-window sentence.
    /// </summary>
    [Fact]
    public void OnA2019Server_AutomaticTuning_SaysNoCycleIsStored_AndPlanCorrectionsKeepsItsOwnSentence()
    {
        var expected = PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 15 });
        var texts = EmptyTexts(sqlMajorVersion: 15);

        Assert.Equal(expected, texts.AutomaticTuning);
        Assert.Equal(QuietWindow, texts.PlanCorrections);
    }

    /// <summary>
    /// A tempdb Size series that stays within 0.01 MB of 8,190 MB draws on an axis at least 1 MB tall, and its tick labels are not all the
    /// same number.
    /// </summary>
    [Fact]
    public void TempDbSize_ANearFlatSeries_GetsAnAxisWithAMinimumSpan_AndDistinctTickLabels()
    {
        var rows = Enumerable.Range(0, 24)
            .Select(i => new TempDbSample(At.AddMinutes(-5 * (24 - i)), 0, 0, 0, 8000.0, 190.25 + (i % 2) * 0.01, 0, 0, 0))
            .ToList();

        var (span, labels) = WithServerTab("DarkTheme", tab =>
        {
            typeof(ViewerServerTab).GetMethod("RenderTempDbSizeChart", Instance)!.Invoke(tab, new object?[] { rows, At.AddHours(-2), At });
            var chart = Find<ScottPlot.WPF.WpfPlot>(tab, "TempDbSizeChart");
            var limits = chart.Plot.Axes.GetLimits();
            using (chart.Plot.GetImage(900, 320)) { }
            var ticks = chart.Plot.Axes.Left.TickGenerator.Ticks.Where(t => t.IsMajor).Select(t => t.Label).ToList();
            return (limits.Top - limits.Bottom, ticks);
        });

        Assert.True(span >= 1.0, $"the Y axis spans {span:0.####} MB for a series that moves 0.01 MB");
        Assert.True(labels.Distinct(StringComparer.Ordinal).Count() >= 2, "the tick labels all read the same: " + string.Join(", ", labels));
    }

    /// <summary>
    /// A series that does move keeps the axis the autoscale gave it, so a real growth trend is not flattened by the floor.
    /// </summary>
    [Fact]
    public void TempDbSize_AGrowingSeries_KeepsTheAutoscaledAxis()
    {
        var rows = Enumerable.Range(0, 24)
            .Select(i => new TempDbSample(At.AddMinutes(-5 * (24 - i)), 0, 0, 0, 8000.0 + i * 50, 190.0, 0, 0, 0))
            .ToList();

        var (bottom, top) = WithServerTab("DarkTheme", tab =>
        {
            typeof(ViewerServerTab).GetMethod("RenderTempDbSizeChart", Instance)!.Invoke(tab, new object?[] { rows, At.AddHours(-2), At });
            var limits = Find<ScottPlot.WPF.WpfPlot>(tab, "TempDbSizeChart").Plot.Axes.GetLimits();
            return (limits.Bottom, limits.Top);
        });

        Assert.True(bottom <= 8190.0 && top >= 9340.0, $"the axis {bottom:0.#}..{top:0.#} does not hold the series 8,190..9,340");
        Assert.True(top - bottom < 1500, $"the axis {bottom:0.#}..{top:0.#} is far wider than the series");
    }

    /// <summary>
    /// When the "Top Databases by Total CPU" card has rows and the "Top Databases by Avg CPU / Execution" card has none, the Avg card says
    /// no execution was recorded in the last 24 hours, in a muted sentence inside the card.
    /// </summary>
    [Fact]
    public void FinOpsTopAvg_WithTotalRowsAndNoAvgRows_SaysNoExecutionWasRecorded()
    {
        var problems = StaTestThread.Run(() => WithTheme("DarkTheme", resources =>
        {
            var muted = ((SolidColorBrush)resources["ForegroundMutedBrush"]).Color;
            var found = new List<string>();
            var tab = new FinOpsTab();
            var window = Open(tab, 1000, 760);
            try
            {
                var total = Find<DataGrid>(tab, "FinOpsTopTotalGrid");
                var avg = Find<DataGrid>(tab, "FinOpsTopAvgGrid");
                total.ItemsSource = new List<TopResourceConsumerRow> { new() { DatabaseName = "Sales", CpuTimeMs = 0, ExecutionCount = 0, IoTotalMb = 12m } };
                avg.ItemsSource = new List<TopResourceConsumerRow>();
                var card = Find<FrameworkElement>(tab, "FinOpsTopAvgCard");
                StaTestThread.Reveal(card);
                StaTestThread.Settle(window);

                var message = Descendants(card).OfType<TextBlock>().FirstOrDefault(t => t.Text == NoExecutions);
                if (message is null)
                {
                    found.Add("the Avg card shows no sentence saying why it is empty; its visible text is: " + string.Join(" | ", Descendants(card).OfType<TextBlock>().Where(t => t.IsVisible && t.Text.Length > 0).Select(t => t.Text)));
                    return found;
                }

                if (!message.IsVisible) found.Add("the sentence is on the card but not visible");
                if (message.Foreground is not SolidColorBrush brush || brush.Color != muted) found.Add($"the sentence is painted {message.Foreground}, not the theme's muted colour");

                /* Rows in both cards: the sentence goes away. */
                avg.ItemsSource = new List<TopResourceConsumerRow> { new() { DatabaseName = "Sales", AvgCpuMs = 1.5m, ExecutionCount = 10 } };
                StaTestThread.Settle(window);
                if (message.IsVisible) found.Add("the sentence stays visible once the Avg card has rows");
                return found;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// The collapsed "CPU by database" expander above the Top Queries grid reads as a control, not as the grid's title: its header says what
    /// it does, and a gap separates it from the grid's column headers.
    /// </summary>
    [Fact]
    public void TheCpuByDatabaseExpander_ReadsAsAControl_NotAsTheGridTitle()
    {
        var problems = WithServerTab("DarkTheme", tab =>
        {
            var found = new List<string>();
            var cards = Find<FrameworkElement>(tab, "QueryStatsByDbCards");
            var expander = Ancestors(cards).OfType<Expander>().FirstOrDefault();
            if (expander is null)
            {
                found.Add("QueryStatsByDbCards is not inside an Expander");
                return found;
            }

            var grid = Find<DataGrid>(tab, "QueryStatsGrid");
            StaTestThread.Reveal(grid);
            StaTestThread.Settle(Window.GetWindow(tab));

            if (expander.Header is not string header || !header.StartsWith("Show ", StringComparison.Ordinal))
            {
                found.Add($"the expander header is \"{expander.Header}\", which reads as a title, not an action");
            }

            var expanderBottom = expander.TransformToAncestor(tab).Transform(new Point(0, expander.ActualHeight)).Y;
            var gridTop = grid.TransformToAncestor(tab).Transform(new Point(0, 0)).Y;
            if (gridTop - expanderBottom < 6)
            {
                found.Add($"the grid's headers start {gridTop - expanderBottom:0.#} px under the expander, so the two read as one block");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────────────

    /// <summary>Builds the tab for a server registered at <paramref name="sqlMajorVersion"/>, binds an empty list to both grids, and reads the sentence each shows.</summary>
    private static (string? AutomaticTuning, string? PlanCorrections) EmptyTexts(int sqlMajorVersion) =>
        WithServerTab("DarkTheme", tab =>
        {
            BindEmpty(tab, "_automaticTuningFilterMgr");
            BindEmpty(tab, "_planCorrectionFilterMgr");
            var window = Window.GetWindow(tab);
            var tuning = Find<DataGrid>(tab, "AutomaticTuningGrid");
            var corrections = Find<DataGrid>(tab, "PlanCorrectionGrid");
            StaTestThread.Reveal(tuning);
            StaTestThread.Settle(window);
            var tuningText = ShownEmptyText(tuning);
            StaTestThread.Reveal(corrections);
            StaTestThread.Settle(window);
            return (tuningText, ShownEmptyText(corrections));
        }, sqlMajorVersion);

    /// <summary>Binds an empty list through the grid's filter manager, the way a load with no rows does.</summary>
    private static void BindEmpty(ViewerServerTab tab, string managerField)
    {
        var manager = typeof(ViewerServerTab).GetField(managerField, Instance)!.GetValue(tab) ?? throw new Xunit.Sdk.XunitException(managerField + " is null");
        var update = manager.GetType().GetMethod("UpdateData") ?? throw new Xunit.Sdk.XunitException(managerField + " has no UpdateData");
        var rowType = update.GetParameters()[0].ParameterType.GetGenericArguments()[0];
        update.Invoke(manager, new[] { Activator.CreateInstance(typeof(List<>).MakeGenericType(rowType)) });
    }

    /// <summary>The visible empty-state sentence inside <paramref name="grid"/>'s cell, or null when the grid shows none.</summary>
    private static string? ShownEmptyText(DataGrid grid)
    {
        if (grid.Parent is not Grid parent) return null;
        var text = parent.Children.OfType<TextBlock>().FirstOrDefault(t => t.Name == EmptyState.ElementName + "_" + grid.Name);
        return text is { Visibility: Visibility.Visible } ? text.Text : null;
    }

    private static T WithServerTab<T>(string theme, Func<ViewerServerTab, T> body, int? sqlMajorVersion = null) =>
        StaTestThread.Run(() => WithTheme(theme, _ =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                new DarlingServer(1, "older-sku-test", "older-sku-test", true, sqlMajorVersion));
            /* Switching to a tab would start that tab's load against a store that is not there. */
            typeof(ViewerServerTab).GetField("_refreshInFlight", Instance)!.SetValue(tab, true);
            typeof(ViewerServerTab).GetField("_suppressDrillDownAutoRefresh", Instance)?.SetValue(tab, true);
            var window = Open(tab, 1000, 800);
            try
            {
                return body(tab);
            }
            finally
            {
                window.Close();
            }
        }));

    private static Window Open(FrameworkElement content, int width, int height)
    {
        var window = new Window { Content = content, Width = width, Height = height, Left = 0, Top = 0, WindowStyle = WindowStyle.None, ResizeMode = ResizeMode.NoResize };
        StaTestThread.ShowOffScreen(window);
        StaTestThread.Settle(window);
        return window;
    }

    private static T Find<T>(FrameworkElement root, string name) where T : FrameworkElement =>
        root.FindName(name) as T ?? throw new Xunit.Sdk.XunitException($"{name} is not on the control");

    private static IEnumerable<DependencyObject> Descendants(DependencyObject root)
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            yield return child;
            foreach (var grandChild in Descendants(child)) yield return grandChild;
        }
    }

    private static IEnumerable<DependencyObject> Ancestors(DependencyObject element)
    {
        for (var at = LogicalTreeHelper.GetParent(element); at is not null; at = LogicalTreeHelper.GetParent(at)) yield return at;
    }

    private static T WithTheme<T>(string theme, Func<ResourceDictionary, T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var added = new List<ResourceDictionary>
        {
            new() { Source = new Uri(ViewerPack + theme + ".xaml") },
            new() { Source = ThemeManager.SharedStylesUri },
        };
        foreach (var dictionary in added)
        {
            app.Resources.MergedDictionaries.Add(dictionary);
        }

        try
        {
            return body(app.Resources);
        }
        finally
        {
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Remove(dictionary);
            }
        }
    }
}
