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
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Threading;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8225: the Viewer's empty, count and note text reads the way the refreshed views do. A count label on a tab's toolbar is the
/// muted subtitle under a card title, not blue bold italic, and a count reads as a natural plural ("3 events", "1 recommendation");
/// an advice sentence or a footnote is muted and upright; an empty grid says one muted, upright sentence, centred in the grid, in the
/// words the other empty grids use, wrapping to the grid's width and never narrower; and the sentence sits inside its grid, not under
/// it. Every rendered fact builds the real tab on the shared STA thread, themed as the app themes it, shows it at the width a tab has
/// beside the sidebar of a 1280x800 and a 900x600 window, and reads the realized font style, weight, brush and bounds of each element.
/// With REFRESH_SHOTS_DIR set, each screen is saved as a PNG.
/// </summary>
public sealed class ViewerEmptyAndCountTextTests
{
    private const string ViewerPack = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/";

    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    private static readonly (int Width, int Height)[] Sizes = { (1280, 800), (900, 600) };

    /* The sidebar beside a tab in MainWindow.xaml: 280 px. */
    private static int PaneWidth(int windowWidth) => windowWidth - 280;

    /* The longest sentence an empty System Events grid says: wider than the 900x600 pane and narrower than the 1280x800 one. */
    private const string LongSentence = "No significant scheduler-monitor samples (SQL CPU, other-process CPU, or memory utilization outside the warning thresholds) in the selected time window.";

    private static readonly string[] ServerTabCounts =
    {
        "SchedulerIssuesCountIndicator", "SevereErrorsCountIndicator", "MemoryConditionsCountIndicator", "MemoryBrokerCountIndicator",
        "MemoryNodeOomCountIndicator", "SignificantWaitsCountIndicator", "CpuTasksCountIndicator", "IoIssuesCountIndicator", "DefaultTraceCountIndicator",
    };

    private static readonly string[] FinOpsCounts =
    {
        "FinOpsRecommendationsCountIndicator", "FinOpsDbResourcesCountIndicator", "FinOpsStorageGrowthCountIndicator", "FinOpsIndexLockingCountIndicator",
        "FinOpsDbSizeCountIndicator", "FinOpsPvsCountIndicator", "FinOpsIndexAnalysisCountIndicator", "FinOpsIdleDatabasesCountIndicator",
        "FinOpsExpensiveQueriesCountIndicator", "FinOpsHighImpactCountIndicator", "FinOpsAppConnectionsCountIndicator", "FinOpsServerInventoryCountIndicator",
    };

    /* The Optimization sub-tab's grids and the sentence each shows when it is empty. */
    private static readonly (string Grid, string Message)[] OptimizationGrids =
    {
        ("FinOpsIdleDatabasesDataGrid", "FinOpsIdleDatabasesNoDataMessage"), ("FinOpsTempdbPressureDataGrid", "FinOpsTempdbPressureNoDataMessage"),
        ("FinOpsWaitCategorySummaryDataGrid", "FinOpsWaitCategorySummaryNoDataMessage"), ("FinOpsExpensiveQueriesDataGrid", "FinOpsExpensiveQueriesNoDataMessage"),
        ("FinOpsMemoryGrantEfficiencyDataGrid", "FinOpsMemoryGrantEfficiencyNoDataMessage"),
    };

    [Theory]
    [InlineData(1, "1 recommendation")]
    [InlineData(3, "3 recommendations")]
    public void RecommendationsCount_ReadsAsANaturalPlural(int count, string expected)
    {
        Assert.Equal(expected, FinOpsRecommendationsCountText.For(count));
    }

    [Theory]
    [InlineData(0, "0 events")]
    [InlineData(1, "1 event")]
    [InlineData(2, "2 events")]
    [InlineData(1500, "1,500 events")]
    public void ACount_ReadsAsANaturalPlural(int count, string expected)
    {
        Assert.Equal(expected, PerformanceMonitor.Common.CountText.Plural(count, "event"));
    }

    [Theory]
    [InlineData(3, false, "3 servers")]
    [InlineData(1, false, "1 server")]
    [InlineData(3, true, "3 servers (removed servers included)")]
    public void TheServerInventoryCount_ReadsAsANaturalPlural(int count, bool removed, string expected) =>
        Assert.Equal(expected, FinOpsServerChoice.InventoryCountText(count, removed));

    public static IEnumerable<object[]> ThemesAndSizes()
    {
        foreach (var theme in new[] { "DarkTheme", "LightTheme", "CoolBreezeTheme" })
        {
            yield return new object[] { theme, 1280, 800 };
            yield return new object[] { theme, 900, 600 };
        }
    }

    public static IEnumerable<object[]> Themes()
    {
        yield return new object[] { "DarkTheme" };
        yield return new object[] { "LightTheme" };
        yield return new object[] { "CoolBreezeTheme" };
    }

    [Theory]
    [MemberData(nameof(ThemesAndSizes))]
    public void JobHistoryAndAlertHistoryCountLabels_AreMutedAndUpright_InsideTheTab(string theme, int width, int height)
    {
        var problems = StaTestThread.Run(() => WithTheme(theme, resources =>
        {
            var muted = ((SolidColorBrush)resources["ForegroundMutedBrush"]).Color;
            var found = new List<string>();
            var jobs = new JobHistoryTab();
            jobs.JobCountIndicator.Text = "12 runs";
            found.AddRange(Check("Job History", theme, jobs, jobs.JobCountIndicator, muted, width, height));
            var alerts = new AlertsHistoryTab();
            alerts.AlertCountIndicator.Text = "7 alerts";
            found.AddRange(Check("Alert History", theme, alerts, alerts.AlertCountIndicator, muted, width, height));
            return found;
        }));

        Assert.True(problems.Count == 0, $"{theme} at {width}x{height}:{Environment.NewLine}" + string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheServerTabsCountLabelsAdviceAndFootnote_AreMutedAndUpright_InsideTheTab(string theme)
    {
        var problems = WithServerTab(theme, (tab, window, muted) =>
        {
            var found = new List<string>();
            foreach (var (width, height) in Sizes)
            {
                Resize(window, PaneWidth(width), height);
                var at = $"{theme} {width}x{height}";

                foreach (var name in ServerTabCounts)
                {
                    var label = Find<TextBlock>(tab, name);
                    label.Text = "3 events";
                    Reveal(label);
                    Settle(window);
                    found.AddRange(Muted(label, tab, muted, $"{at}: {name}"));
                }

                Reveal(Find<TextBlock>(tab, "MemoryBrokerCountIndicator"));
                Settle(window);
                Shoot(window, $"MemoryBroker-{theme}-{width}x{height}.png");

                /* The Plan Cache sub-tab scrolls (the chart, the by-type grid, then the figures), so the strip is brought into view first. */
                var advice = Find<TextBlock>(tab, "PlanCacheRecommendationText");
                advice.Text = "Forced parameterization would let these single-use plans be reused.";
                Reveal(advice);
                Settle(window);
                advice.BringIntoView();
                Settle(window);
                found.AddRange(Muted(advice, tab, muted, $"{at}: PlanCacheRecommendationText"));
                Shoot(window, $"PlanCache-{theme}-{width}x{height}.png");

                var note = Find<TextBlock>(tab, "CurrentWaitsSparseNote");
                Reveal(note);
                Settle(window);
                found.AddRange(Muted(note, tab, muted, $"{at}: CurrentWaitsSparseNote"));
                Shoot(window, $"CurrentWaits-{theme}-{width}x{height}.png");

                var hint = tab.FindName("DailySummaryHintText") as TextBlock;
                if (hint is null)
                {
                    found.Add($"{at}: the Daily Summary hint (DailySummaryHintText) is not on the tab");
                }
                else
                {
                    Reveal(hint);
                    Settle(window);
                    found.AddRange(Muted(hint, tab, muted, $"{at}: DailySummaryHintText"));
                    Shoot(window, $"DailySummary-{theme}-{width}x{height}.png");
                }
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void AnEmptyGrid_SaysOneMutedUprightSentence_CentredInTheGrid_WrappingToItsWidth(string theme)
    {
        var problems = WithServerTab(theme, (tab, window, muted) =>
        {
            var found = new List<string>();

            /* A grid given no wording of its own says what the shared empty text says, in the theme's style too. */
            var styled = new TextBlock { Style = (Style)Application.Current.FindResource("NoDataMessage") };
            if (styled.Text != EmptyState.DefaultGridText)
            {
                found.Add($"{theme}: the theme's NoDataMessage style says \"{styled.Text}\", the shared empty text \"{EmptyState.DefaultGridText}\"");
            }

            var grid = Find<DataGrid>(tab, "PlanCorrectionGrid");
            Reveal(grid);
            foreach (var (width, height) in Sizes)
            {
                Resize(window, PaneWidth(width), height);
                var at = $"{theme} {width}x{height}: Plan Corrections";

                /* What the filter manager does when a read comes back with no rows. */
                EmptyState.Show(grid, true);
                Settle(window);
                var sentence = EmptyText(tab, grid);
                found.AddRange(Muted(sentence, tab, muted, at, weight: false));
                if (sentence.Text != "No plan corrections in the selected time window.")
                {
                    found.Add($"{at}: the sentence is \"{sentence.Text}\", not \"No plan corrections in the selected time window.\"");
                }

                if (Lines(sentence) != 1)
                {
                    found.Add($"{at}: the sentence wraps to {Lines(sentence)} lines");
                }

                var gridBounds = Bounds(grid, tab);
                var bounds = Bounds(sentence, tab);
                if (Math.Abs((bounds.Left + bounds.Right) / 2 - (gridBounds.Left + gridBounds.Right) / 2) > 1)
                {
                    found.Add($"{at}: the sentence is centred at {(bounds.Left + bounds.Right) / 2:0}, the grid at {(gridBounds.Left + gridBounds.Right) / 2:0}");
                }

                if (bounds.Top < gridBounds.Top - 0.5 || bounds.Bottom > gridBounds.Bottom + 0.5)
                {
                    found.Add($"{at}: the sentence spans {bounds.Top:0}..{bounds.Bottom:0}, outside the grid's {gridBounds.Top:0}..{gridBounds.Bottom:0}");
                }

                Shoot(window, $"PlanCorrections-{theme}-{width}x{height}.png");

                /* A long sentence wraps at its cell's width, never at a narrower one: its lines break where the same text breaks when
                   it is given the whole cell. */
                EmptyState.Show(grid, true, LongSentence);
                Settle(window);
                var natural = NaturalWidth(sentence);
                var room = CellWidth(sentence) - sentence.Margin.Left - sentence.Margin.Right;
                if (natural <= room && Lines(sentence) != 1)
                {
                    found.Add($"{at}: a {natural:0} px sentence wraps to {Lines(sentence)} lines in a {room:0} px cell");
                }

                if (natural > room && Math.Abs(sentence.ActualWidth - WrappedWidth(sentence, room)) > 1)
                {
                    found.Add($"{at}: a {natural:0} px sentence wraps at {sentence.ActualWidth:0} px in a {room:0} px cell, where its widest line is {WrappedWidth(sentence, room):0} px");
                }

                EmptyState.Show(grid, true);
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheFinOpsCountLabelsHintAndEmptySentences_ReadTheRefreshedWay(string theme)
    {
        var problems = StaTestThread.Run(() => WithTheme(theme, resources =>
        {
            var muted = ((SolidColorBrush)resources["ForegroundMutedBrush"]).Color;
            var tab = new FinOpsTab();
            var window = Open(tab, PaneWidth(1280), 800);
            try
            {
                var found = new List<string>();
                foreach (var (width, height) in Sizes)
                {
                    Resize(window, PaneWidth(width), height);
                    var at = $"{theme} {width}x{height}";

                    /* The Optimization sub-tab scrolls, so a count below the fold is brought into view first, as a reader scrolls to it. */
                    foreach (var name in FinOpsCounts)
                    {
                        var label = Find<TextBlock>(tab, name);
                        label.Text = "3 databases";
                        Reveal(label);
                        Settle(window);
                        label.BringIntoView();
                        Settle(window);
                        found.AddRange(Muted(label, tab, muted, $"{at}: {name}"));
                    }

                    Reveal(Find<TextBlock>(tab, "FinOpsRecommendationsCountIndicator"));
                    Settle(window);
                    Shoot(window, $"FinOpsRecommendations-{theme}-{width}x{height}.png");

                    /* Locking & Contention: the hint beside the database picker. */
                    Reveal(Find<TextBlock>(tab, "FinOpsIndexLockingCountIndicator"));
                    Settle(window);
                    var hint = ClippedTextProbe.Descendants(tab).OfType<TextBlock>().FirstOrDefault(t => t.Text.StartsWith("Cumulative since restart", StringComparison.Ordinal));
                    if (hint is null)
                    {
                        found.Add($"{at}: the Locking hint is not on the tab");
                    }
                    else
                    {
                        found.AddRange(Muted(hint, tab, muted, $"{at}: the Locking hint", weight: false));
                    }

                    Shoot(window, $"FinOpsLocking-{theme}-{width}x{height}.png");

                    /* Index Analysis: the lower grid shows the one empty sentence, centred in it. */
                    var lower = Find<DataGrid>(tab, "FinOpsIndexAnalysisDetailGrid");
                    var sentence = Find<TextBlock>(tab, "FinOpsIndexAnalysisNoDataMessage");
                    Reveal(lower);
                    sentence.Visibility = Visibility.Visible;
                    Settle(window);
                    found.AddRange(Muted(sentence, tab, muted, $"{at}: FinOpsIndexAnalysisNoDataMessage", weight: false));
                    found.AddRange(Inside(sentence, lower, tab, $"{at}: FinOpsIndexAnalysisNoDataMessage"));
                    found.AddRange(CentredOn(sentence, lower, tab, $"{at}: FinOpsIndexAnalysisNoDataMessage"));
                    Shoot(window, $"FinOpsIndexAnalysis-{theme}-{width}x{height}.png");

                    /* Optimization: each empty sentence sits inside its grid. */
                    foreach (var (gridName, messageName) in OptimizationGrids)
                    {
                        var grid = Find<DataGrid>(tab, gridName);
                        var message = Find<TextBlock>(tab, messageName);
                        Reveal(grid);
                        message.Visibility = Visibility.Visible;
                        Settle(window);
                        found.AddRange(Muted(message, tab, muted, $"{at}: {messageName}", weight: false, insideTab: false));
                        found.AddRange(Inside(message, grid, tab, $"{at}: {messageName}"));
                    }

                    Find<TextBlock>(tab, "FinOpsIdleDatabasesCountIndicator").Text = "";
                    Find<TextBlock>(tab, "FinOpsExpensiveQueriesCountIndicator").Text = "";
                    Settle(window);
                    Shoot(window, $"FinOpsOptimization-{theme}-{width}x{height}.png");
                }

                return found;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheSlicersEmptyText_IsMutedAndUpright_CentredInTheSlicer(string theme)
    {
        var problems = StaTestThread.Run(() => WithTheme(theme, resources =>
        {
            var muted = ((SolidColorBrush)resources["ForegroundMutedBrush"]).Color;
            var found = new List<string>();
            foreach (var (width, height) in Sizes)
            {
                var at = $"{theme} {width}x{height}: the slicer's empty text";
                /* A slicer with no message host draws the sentence itself, on its own canvas. */
                var slicer = new TimeRangeSlicerControl { VerticalAlignment = VerticalAlignment.Top };
                var host = new Grid();
                host.Children.Add(slicer);
                var window = Open(host, PaneWidth(width), height);
                try
                {
                    slicer.ShowEmpty("No active queries in the selected time window.");
                    Settle(window);
                    var text = Find<Canvas>(slicer, "SlicerCanvas").Children.OfType<TextBlock>().FirstOrDefault(t => t.Text == slicer.EmptyText);
                    if (text is null)
                    {
                        found.Add($"{at} is not drawn");
                    }
                    else
                    {
                        found.AddRange(Muted(text, slicer, muted, at, weight: false));
                        var band = Bounds(Find<FrameworkElement>(slicer, "SlicerBorder"), slicer);
                        var bounds = Bounds(text, slicer);
                        if (Math.Abs((bounds.Left + bounds.Right) / 2 - (band.Left + band.Right) / 2) > 1)
                        {
                            found.Add($"{at} is centred at {(bounds.Left + bounds.Right) / 2:0}, the slicer at {(band.Left + band.Right) / 2:0}");
                        }
                    }

                    Shoot(window, $"Slicer-{theme}-{width}x{height}.png");
                }
                finally
                {
                    window.Close();
                }
            }

            return found;
        }));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────────────

    /// <summary>Builds the real server tab under <paramref name="theme"/>, shows it, and runs <paramref name="body"/> with the theme's muted colour.</summary>
    private static List<string> WithServerTab(string theme, Func<ViewerServerTab, Window, Color, List<string>> body)
    {
        return StaTestThread.Run(() => WithTheme(theme, resources =>
        {
            var muted = ((SolidColorBrush)resources["ForegroundMutedBrush"]).Color;
            var tab = new ViewerServerTab(
                new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                new DarlingServer(1, "empty-text-test", "empty-text-test", true, null));
            /* Switching to a tab would start that tab's load against a store that is not there. */
            typeof(ViewerServerTab).GetField("_refreshInFlight", Instance)!.SetValue(tab, true);
            typeof(ViewerServerTab).GetField("_suppressDrillDownAutoRefresh", Instance)?.SetValue(tab, true);
            var window = Open(tab, PaneWidth(1280), 800);
            try
            {
                return body(tab, window, muted);
            }
            finally
            {
                window.Close();
            }
        }));
    }

    private static List<string> Check(string tabName, string theme, UserControl tab, TextBlock label, Color muted, int width, int height)
    {
        var problems = new List<string>();
        var window = new Window { Content = tab, Width = width, Height = height, Left = 0, Top = 0, WindowStyle = WindowStyle.None };
        StaTestThread.ShowOffScreen(window);
        try
        {
            Settle(window);
            Shoot(window, $"{tabName.Replace(" ", "", StringComparison.Ordinal)}-{theme}-{width}x{height}.png");

            if (!label.IsVisible)
            {
                problems.Add($"{tabName}: the count label is not visible");
                return problems;
            }

            if (label.FontStyle != FontStyles.Normal)
            {
                problems.Add($"{tabName}: the count label is {label.FontStyle}");
            }

            if (label.FontWeight != FontWeights.Normal)
            {
                problems.Add($"{tabName}: the count label is {label.FontWeight}");
            }

            if (label.Foreground is not SolidColorBrush brush || brush.Color != muted)
            {
                problems.Add($"{tabName}: the count label is {Describe(label.Foreground)}, not the muted foreground {muted}");
            }

            var bounds = label.TransformToAncestor(window).TransformBounds(new Rect(label.RenderSize));
            if (bounds.Right > window.ActualWidth + 0.5 || bounds.Bottom > window.ActualHeight + 0.5)
            {
                problems.Add($"{tabName}: the count label ends at {bounds.Right:0}x{bounds.Bottom:0} in a {window.ActualWidth:0}x{window.ActualHeight:0} window");
            }
        }
        finally
        {
            window.Close();
        }

        return problems;
    }

    private static Window Open(FrameworkElement content, int width, int height)
    {
        var window = new Window { Content = content, Width = width, Height = height, Left = 0, Top = 0, WindowStyle = WindowStyle.None, ResizeMode = ResizeMode.NoResize };
        StaTestThread.ShowOffScreen(window);
        Settle(window);
        return window;
    }

    private static void Resize(Window window, int width, int height)
    {
        window.Width = width;
        window.Height = height;
        Settle(window);
    }

    private static T Find<T>(FrameworkElement tab, string name) where T : FrameworkElement =>
        tab.FindName(name) as T ?? throw new Xunit.Sdk.XunitException($"{name} is not on the tab");

    /// <summary>Selects every tab on the way up to <paramref name="element"/>, so it is laid out and drawn.</summary>
    private static void Reveal(FrameworkElement element)
    {
        for (DependencyObject? at = element; at is not null; at = LogicalTreeHelper.GetParent(at))
        {
            if (at is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl owner)
            {
                owner.SelectedItem = item;
            }
        }
    }

    /// <summary>The text <see cref="EmptyState"/> drew for <paramref name="grid"/>.</summary>
    private static TextBlock EmptyText(FrameworkElement tab, DataGrid grid) =>
        ClippedTextProbe.Descendants(tab).OfType<TextBlock>().FirstOrDefault(t => t.Name == EmptyState.ElementName + "_" + grid.Name)
            ?? throw new Xunit.Sdk.XunitException($"{grid.Name} shows no empty-state text");

    private static Rect Bounds(FrameworkElement element, FrameworkElement ancestor) =>
        element.TransformToAncestor(ancestor).TransformBounds(new Rect(element.RenderSize));

    /// <summary>How many lines <paramref name="text"/> wrapped to, from its height against one line in the same font.</summary>
    private static int Lines(TextBlock text)
    {
        var one = new TextBlock { Text = "x", FontFamily = text.FontFamily, FontSize = text.FontSize, FontStyle = text.FontStyle, FontWeight = text.FontWeight };
        one.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
        return (int)Math.Round((text.ActualHeight - text.Padding.Top - text.Padding.Bottom) / one.DesiredSize.Height);
    }

    /// <summary>The width <paramref name="text"/> takes on one line.</summary>
    private static double NaturalWidth(TextBlock text)
    {
        var one = new TextBlock { Text = text.Text, FontFamily = text.FontFamily, FontSize = text.FontSize, FontStyle = text.FontStyle, FontWeight = text.FontWeight, TextWrapping = TextWrapping.NoWrap };
        one.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
        return one.DesiredSize.Width;
    }

    /// <summary>The width of the widest line <paramref name="text"/> makes when it wraps in <paramref name="room"/> pixels.</summary>
    private static double WrappedWidth(TextBlock text, double room)
    {
        var wrapped = new TextBlock { Text = text.Text, FontFamily = text.FontFamily, FontSize = text.FontSize, FontStyle = text.FontStyle, FontWeight = text.FontWeight, TextWrapping = TextWrapping.Wrap };
        wrapped.Measure(new Size(room, double.PositiveInfinity));
        return wrapped.DesiredSize.Width;
    }

    /// <summary>The width of the grid cell <paramref name="element"/> sits in: its column, or the whole grid when it has none.</summary>
    private static double CellWidth(FrameworkElement element)
    {
        var cell = LogicalTreeHelper.GetParent(element) as Grid ?? throw new Xunit.Sdk.XunitException($"{element.Name} is not in a grid cell");
        return cell.ColumnDefinitions.Count == 0 ? cell.ActualWidth : cell.ColumnDefinitions[Grid.GetColumn(element)].ActualWidth;
    }

    /// <summary>
    /// The realized look of <paramref name="label"/>: visible, upright, in the theme's muted foreground, normal weight when
    /// <paramref name="weight"/> is asked, and wholly inside <paramref name="tab"/> unless <paramref name="insideTab"/> is off (a
    /// sub-tab that scrolls keeps its lower content below the fold).
    /// </summary>
    private static List<string> Muted(TextBlock label, FrameworkElement tab, Color muted, string what, bool weight = true, bool insideTab = true)
    {
        var problems = new List<string>();
        if (!label.IsVisible)
        {
            problems.Add($"{what} is not visible");
            return problems;
        }

        if (label.FontStyle != FontStyles.Normal)
        {
            problems.Add($"{what} is {label.FontStyle}");
        }

        if (weight && label.FontWeight != FontWeights.Normal)
        {
            problems.Add($"{what} is {label.FontWeight}");
        }

        if (label.Foreground is not SolidColorBrush brush || brush.Color != muted)
        {
            problems.Add($"{what} is {Describe(label.Foreground)}, not the muted foreground {muted}");
        }

        var bounds = Bounds(label, tab);
        if (insideTab && (bounds.Left < -0.5 || bounds.Top < -0.5 || bounds.Right > tab.ActualWidth + 0.5 || bounds.Bottom > tab.ActualHeight + 0.5))
        {
            problems.Add($"{what} spans {bounds.Left:0},{bounds.Top:0}..{bounds.Right:0},{bounds.Bottom:0} in a {tab.ActualWidth:0}x{tab.ActualHeight:0} tab");
        }

        return problems;
    }

    /// <summary><paramref name="sentence"/> lies wholly inside <paramref name="grid"/>'s bounds.</summary>
    private static List<string> Inside(TextBlock sentence, DataGrid grid, FrameworkElement tab, string what)
    {
        var problems = new List<string>();
        if (!sentence.IsVisible)
        {
            return problems;
        }

        var g = Bounds(grid, tab);
        var s = Bounds(sentence, tab);
        if (s.Left < g.Left - 0.5 || s.Top < g.Top - 0.5 || s.Right > g.Right + 0.5 || s.Bottom > g.Bottom + 0.5)
        {
            problems.Add($"{what} spans {s.Top:0}..{s.Bottom:0}, outside {grid.Name}'s {g.Top:0}..{g.Bottom:0}");
        }

        return problems;
    }

    /// <summary><paramref name="sentence"/> is centred on <paramref name="grid"/>, not on the grids above it as well.</summary>
    private static List<string> CentredOn(TextBlock sentence, DataGrid grid, FrameworkElement tab, string what)
    {
        var problems = new List<string>();
        if (!sentence.IsVisible)
        {
            return problems;
        }

        var g = Bounds(grid, tab);
        var s = Bounds(sentence, tab);
        if (Math.Abs((s.Top + s.Bottom) / 2 - (g.Top + g.Bottom) / 2) > 1)
        {
            problems.Add($"{what} is centred at {(s.Top + s.Bottom) / 2:0}, {grid.Name} at {(g.Top + g.Bottom) / 2:0}");
        }

        return problems;
    }

    private static string Describe(Brush? brush) => brush is SolidColorBrush solid ? solid.Color.ToString() : brush?.ToString() ?? "null";

    private static void Settle(Window window)
    {
        window.UpdateLayout();
        Dispatcher.CurrentDispatcher.Invoke(DispatcherPriority.Loaded, () => { });
        window.UpdateLayout();
        ClippedTextProbe.Settle(window);
    }

    /// <summary>Saves the window as a PNG under REFRESH_SHOTS_DIR when that directory is given, so a run can be looked at; otherwise nothing.</summary>
    private static void Shoot(Window window, string fileName)
    {
        var dir = Environment.GetEnvironmentVariable("REFRESH_SHOTS_DIR");
        if (string.IsNullOrEmpty(dir) || window.Content is not FrameworkElement content || content.ActualWidth <= 0)
        {
            return;
        }

        var bitmap = new RenderTargetBitmap((int)content.ActualWidth, (int)content.ActualHeight, 96, 96, PixelFormats.Pbgra32);
        bitmap.Render(content);
        var encoder = new PngBitmapEncoder();
        encoder.Frames.Add(BitmapFrame.Create(bitmap));
        Directory.CreateDirectory(dir);
        using var stream = File.Create(Path.Combine(dir, fileName));
        encoder.Save(stream);
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
