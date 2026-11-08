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
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Threading;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8226: the Viewer panels that still used the old layout take the card look the chart panels have: a <see cref="ChartPanel"/> (or the
/// card border) with a semibold title and a muted subtitle, one scroll area per page, grids that size to their rows and a placeholder
/// in each search box; a recommendation section names its title once and reads a fact token as words. Each rendered fact builds the
/// real tab on the shared STA thread with the theme and the shared styles merged, shows it at the width a tab has beside the sidebar
/// of a 1280x800 and a 900x600 window, and reads the realized result. With REFRESH_SHOTS_DIR set, each screen is saved as a PNG.
/// </summary>
public sealed class ViewerPanelCardsTests
{
    private const string ViewerPack = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/";

    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    private static readonly (int Width, int Height)[] Sizes = { (1280, 800), (900, 600) };

    /* The sidebar beside a tab in MainWindow.xaml: 280 px. */
    private static int PaneWidth(int windowWidth) => windowWidth - 280;

    /* The title style's weight and size, and the subtitle style's size, from PerformanceMonitor.Ui/Themes/SharedStyles.xaml. */
    private const double TitleSize = 14;
    private const double SubtitleSize = 11;

    /* The server tab's lists and banners that sit in a card, each named by the element a reader looks at. */
    private static readonly (string Element, string Shot)[] ServerTabCards =
    {
        ("RunningJobsGrid", "running-jobs"),
        ("SkippedCyclesLine", "collection-health"),
        ("PerfmonCountersList", "perfmon"),
        ("MemoryClerksList", "memory-clerks"),
        ("MemoryBrokerGrid", "memory-broker"),
    };

    /* The Utilization and Optimization grids, each in a card, each given rows so a grid that scrolls on its own shows it. */
    private static readonly string[] UtilizationGrids = { "FinOpsTopTotalGrid", "FinOpsTopAvgGrid", "FinOpsProvisioningTrendGrid" };
    private static readonly string[] OptimizationGrids =
    {
        "FinOpsIdleDatabasesDataGrid", "FinOpsTempdbPressureDataGrid", "FinOpsWaitCategorySummaryDataGrid", "FinOpsExpensiveQueriesDataGrid", "FinOpsMemoryGrantEfficiencyDataGrid",
    };

    public static IEnumerable<object[]> Themes()
    {
        yield return new object[] { "DarkTheme" };
        yield return new object[] { "LightTheme" };
        yield return new object[] { "CoolBreezeTheme" };
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheServerTabsListsAndBanners_SitInCards_WithATitleAndASubtitle_AndTheSearchBoxesShowAPlaceholder(string theme)
    {
        var problems = WithServerTab(theme, (tab, window, muted) =>
        {
            var found = new List<string>();
            foreach (var (width, height) in Sizes)
            {
                Resize(window, PaneWidth(width), height);
                foreach (var (element, shot) in ServerTabCards)
                {
                    var target = Find<FrameworkElement>(tab, element);
                    Reveal(target);
                    Settle(window);
                    found.AddRange(Carded(target, tab, muted, $"{element} at {width}x{height}"));
                    Shoot(window, $"{theme}-{width}x{height}-{shot}.png");
                }

                found.AddRange(Placeholder(tab, window, "PerfmonSearchBox", "PerfmonSearchPlaceholder", $"{width}x{height}"));
                found.AddRange(Placeholder(tab, window, "MemoryClerkSearchBox", "MemoryClerkSearchPlaceholder", $"{width}x{height}"));
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheMemoryOverviewCard_HasATitleAndASubtitle_AndItsThirdRowIsSubtitleSized_AlignedWithItsColumns(string theme)
    {
        var problems = WithServerTab(theme, (tab, window, muted) =>
        {
            var found = new List<string>();
            foreach (var (width, height) in Sizes)
            {
                Resize(window, PaneWidth(width), height);
                var state = Find<TextBlock>(tab, "MemoryStateText");
                Reveal(state);
                Settle(window);
                var where = $"memory overview at {width}x{height}";

                if (tab.FindName("MemoryOverviewCard") is not Border card)
                {
                    found.Add($"{where}: the summary has no card border");
                }
                else if (card.BorderBrush is not SolidColorBrush brush || brush.Color != ((SolidColorBrush)Application.Current.FindResource("CardBorderBrush")).Color)
                {
                    found.Add($"{where}: the card border is {Describe(card.BorderBrush)}, not the theme's CardBorderBrush");
                }

                if (tab.FindName("MemoryOverviewTitle") is not TextBlock title)
                {
                    found.Add($"{where}: the card has no title");
                }
                else if (title.FontWeight != FontWeights.SemiBold || title.FontSize != TitleSize || string.IsNullOrWhiteSpace(title.Text))
                {
                    found.Add($"{where}: the title '{title.Text}' is {title.FontWeight} {title.FontSize}, not SemiBold {TitleSize}");
                }

                if (tab.FindName("MemoryOverviewSubtitle") is not TextBlock subtitle)
                {
                    found.Add($"{where}: the card has no subtitle");
                }
                else
                {
                    found.AddRange(MutedSubtitle(subtitle, muted, $"{where}: the subtitle"));
                }

                /* The third row: a sentence, a model name and a timestamp, at the subtitle size, each under the metric of its column. */
                var thirdRow = new[] { ("MemoryStateText", "PhysicalMemoryText"), ("SqlMemoryModelText", "TotalServerMemoryText"), ("MemoryCollectedText", "TargetServerMemoryText") };
                foreach (var (value, above) in thirdRow)
                {
                    var text = Find<TextBlock>(tab, value);
                    if (text.FontSize != SubtitleSize)
                    {
                        found.Add($"{where}: {value} is {text.FontSize} pt, not the {SubtitleSize} pt subtitle size");
                    }

                    var left = Bounds(text, tab).Left;
                    var columnLeft = Bounds(Find<TextBlock>(tab, above), tab).Left;
                    if (Math.Abs(left - columnLeft) > 0.5)
                    {
                        found.Add($"{where}: {value} starts at {left:F1}, its column ({above}) at {columnLeft:F1}");
                    }
                }

                Shoot(window, $"{theme}-{width}x{height}-memory-overview.png");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheUtilizationAndOptimizationPages_ScrollOnce_InCards_AndTheirGridsSizeToTheirRows(string theme)
    {
        var problems = StaTestThread.Run(() => WithTheme(theme, muted =>
        {
            var tab = new FinOpsTab();
            var found = new List<string>();
            FillGrids(tab);
            var window = Open(tab, PaneWidth(1280), 800);
            try
            {
                foreach (var (width, height) in Sizes)
                {
                    Resize(window, PaneWidth(width), height);
                    foreach (var (header, grids, shot) in new[] { ("Utilization", UtilizationGrids, "finops-utilization"), ("Optimization", OptimizationGrids, "finops-optimization") })
                    {
                        var page = SubTab(tab, header);
                        Reveal(page);
                        Settle(window);
                        var where = $"{header} at {width}x{height}";

                        /* The sub-tab's content is presented by the tab control, so the walk starts at the content, not the header. */
                        var content = (FrameworkElement)page.Content;
                        var scrollers = Scrollers(content).Count;
                        if (scrollers != 1)
                        {
                            found.Add($"{where}: {scrollers} scroll areas on the page, not one");
                        }

                        var boxes = ChartLayoutProbe.Logical<GroupBox>(page).Count() + ChartLayoutProbe.Logical<Expander>(page).Count();
                        if (boxes != 0)
                        {
                            found.Add($"{where}: {boxes} group boxes or expanders still frame the content");
                        }

                        foreach (var name in grids)
                        {
                            var grid = Find<DataGrid>(tab, name);
                            grid.BringIntoView();
                            Settle(window);
                            found.AddRange(Carded(grid, tab, muted, $"{where}: {name}", insideTab: false));
                            var inner = Visual<ScrollViewer>(grid).FirstOrDefault();
                            if (inner is not null && inner.ScrollableHeight > 0)
                            {
                                found.Add($"{where}: {name} scrolls on its own ({inner.ScrollableHeight:F0} px hidden) instead of sizing to its {grid.Items.Count} rows");
                            }
                        }

                        Shoot(window, $"{theme}-{width}x{height}-{shot}.png");

                        /* The page from its top as well, so the first cards are on record too. */
                        Scrollers(content).FirstOrDefault()?.ScrollToTop();
                        Settle(window);
                        Shoot(window, $"{theme}-{width}x{height}-{shot}-top.png");
                    }
                }

                /* The FinOps sub-tab of savings advice is not named like the Recommendations tab of the main window. */
                var headers = Headers(tab);
                if (headers.Contains("Recommendations"))
                {
                    found.Add("a FinOps sub-tab is named Recommendations, the same as the main window's tab");
                }

                if (!headers.Contains("Cost Savings"))
                {
                    found.Add("no FinOps sub-tab is named Cost Savings; the sub-tabs are " + string.Join(", ", headers));
                }
            }
            finally
            {
                window.Close();
            }

            return found;
        }));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void AnIncidentWithSeveralFindings_NamesItsTitleOnce_InTheSectionHeader()
    {
        var rows = new List<ViewerFindingRow>
        {
            Row(1.8, "Plan cache bloat", "inc-A"),
            Row(1.7, "Plan cache bloat", "inc-A"),
            Row(1.2, "Ad hoc queries", "inc-A"),
        };

        var section = Assert.Single(RecommendationsViewModel.FromFindings(rows, "SQL2022").Sections);

        Assert.StartsWith("Plan cache bloat", section.Header, StringComparison.Ordinal);
        /* The header already names the primary finding; a card repeats the title only when its own differs. */
        Assert.Equal(new[] { false, false, true }, section.Cards.Select(c => c.ShowTitle).ToArray());
    }

    [Theory]
    [InlineData("RESOURCE_MEMPHYSICAL_LOW", "Low physical memory")]
    [InlineData("RESOURCE_MEMPHYSICAL_HIGH", "Physical memory recovered")]
    [InlineData("RESOURCE_MEMVIRTUAL_LOW", "Low virtual memory")]
    [InlineData("SOS_SCHEDULER_YIELD", "SOS_SCHEDULER_YIELD waits")]
    [InlineData("Plan cache bloat", "Plan cache bloat")]
    public void AFindingNamedByItsToken_ReadsAsWords(string title, string expected)
    {
        var section = Assert.Single(RecommendationsViewModel.FromFindings(new List<ViewerFindingRow> { Row(1.8, title, "inc-A") }, "SQL2022").Sections);

        Assert.Equal(expected, Assert.Single(section.Cards).Title);
        Assert.StartsWith(expected, section.Header, StringComparison.Ordinal);
    }

    /// <summary>A finding row the way the read returns one, with the title the finding is named by.</summary>
    internal static ViewerFindingRow Row(double severity, string title, string incidentId)
    {
        var finding = new AnalysisFinding
        {
            FindingId = 1,
            AnalysisTime = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
            ServerId = 1,
            ServerName = "SQL2022",
            Severity = severity,
            Confidence = 0.9,
            Category = "memory",
            StoryPath = "a -> b",
            StoryPathHash = "hash-" + title,
            StoryText = "",
            RootFactKey = "FACT",
            IncidentId = incidentId,
            FactCount = 1,
        };
        return new ViewerFindingRow
        {
            SeverityBand = ViewerDataService.SeverityBand(severity),
            RawSeverity = severity,
            Title = title,
            Category = "memory",
            DatabaseName = "",
            Finding = finding,
        };
    }

    /// <summary>
    /// <paramref name="target"/> sits in a <see cref="ChartPanel"/> whose realized title is semibold at the title size, whose
    /// subtitle is set and muted at the subtitle size, and which lies wholly inside <paramref name="tab"/> (unless the page scrolls).
    /// </summary>
    private static List<string> Carded(FrameworkElement target, FrameworkElement tab, Color muted, string what, bool insideTab = true)
    {
        var found = new List<string>();
        var card = CardOf(target);
        if (card is null)
        {
            found.Add($"{what}: not inside a card");
            return found;
        }

        card.ApplyTemplate();
        if (string.IsNullOrWhiteSpace(card.Title))
        {
            found.Add($"{what}: the card has no title");
        }

        if (string.IsNullOrWhiteSpace(card.Subtitle))
        {
            found.Add($"{what}: the card has no subtitle");
        }

        if (card.Template?.FindName("PART_Title", card) is TextBlock title)
        {
            if (title.FontWeight != FontWeights.SemiBold || title.FontSize != TitleSize)
            {
                found.Add($"{what}: the title is {title.FontWeight} {title.FontSize} pt, not SemiBold {TitleSize} pt");
            }
        }
        else
        {
            found.Add($"{what}: the card drew no title");
        }

        if (card.Template?.FindName("PART_Subtitle", card) is TextBlock subtitle)
        {
            found.AddRange(MutedSubtitle(subtitle, muted, $"{what}: the subtitle"));
        }

        if (insideTab)
        {
            var bounds = Bounds(card, tab);
            if (bounds.Left < -0.5 || bounds.Top < -0.5 || bounds.Right > tab.ActualWidth + 0.5 || bounds.Bottom > tab.ActualHeight + 0.5)
            {
                found.Add($"{what}: the card ({bounds}) runs outside the {tab.ActualWidth}x{tab.ActualHeight} tab");
            }
        }

        return found;
    }

    private static List<string> MutedSubtitle(TextBlock subtitle, Color muted, string what)
    {
        var found = new List<string>();
        if (subtitle.Visibility != Visibility.Visible || string.IsNullOrWhiteSpace(subtitle.Text))
        {
            found.Add($"{what} is not shown");
        }

        if (subtitle.FontSize != SubtitleSize)
        {
            found.Add($"{what} is {subtitle.FontSize} pt, not {SubtitleSize} pt");
        }

        if (subtitle.Foreground is not SolidColorBrush brush || brush.Color != muted)
        {
            found.Add($"{what} is {Describe(subtitle.Foreground)}, not the theme's muted foreground {muted}");
        }

        return found;
    }

    /// <summary>The search box shows <paramref name="placeholder"/> while it is empty, and hides it once a reader types.</summary>
    private static List<string> Placeholder(FrameworkElement tab, Window window, string box, string placeholder, string where)
    {
        var found = new List<string>();
        var search = Find<TextBox>(tab, box);
        Reveal(search);
        Settle(window);
        if (tab.FindName(placeholder) is not TextBlock hint)
        {
            found.Add($"{box} at {where}: no placeholder");
            return found;
        }

        search.Text = string.Empty;
        Settle(window);
        if (hint.Visibility != Visibility.Visible || string.IsNullOrWhiteSpace(hint.Text))
        {
            found.Add($"{box} at {where}: the placeholder is not shown while the box is empty");
        }

        var bounds = Bounds(hint, tab);
        var boxBounds = Bounds(search, tab);
        if (bounds.Left < boxBounds.Left || bounds.Right > boxBounds.Right || bounds.Top < boxBounds.Top || bounds.Bottom > boxBounds.Bottom)
        {
            found.Add($"{box} at {where}: the placeholder ({bounds}) lies outside the box ({boxBounds})");
        }

        search.Text = "x";
        Settle(window);
        if (hint.Visibility == Visibility.Visible)
        {
            found.Add($"{box} at {where}: the placeholder stays over the text a reader typed");
        }

        search.Text = string.Empty;
        return found;
    }

    private static ChartPanel? CardOf(DependencyObject element)
    {
        for (DependencyObject? up = element; up is not null; up = LogicalTreeHelper.GetParent(up) ?? (up is Visual ? VisualTreeHelper.GetParent(up) : null))
        {
            if (up is ChartPanel card)
            {
                return card;
            }
        }

        return null;
    }

    /// <summary>The scroll areas of a page: every realized <see cref="ScrollViewer"/> that is not part of a grid, list, box or picker's own template.</summary>
    private static List<ScrollViewer> Scrollers(FrameworkElement page) =>
        Visual<ScrollViewer>(page).Where(s => !HasVisualAncestor(s, page, typeof(DataGrid), typeof(ListBox), typeof(ComboBox), typeof(TextBoxBase), typeof(TimeRangePicker))).ToList();

    /// <summary>Every realized <typeparamref name="T"/> under <paramref name="root"/>, by the visual tree.</summary>
    private static IEnumerable<T> Visual<T>(DependencyObject root) where T : DependencyObject
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            if (child is T match)
            {
                yield return match;
            }

            foreach (var below in Visual<T>(child))
            {
                yield return below;
            }
        }
    }

    private static bool HasVisualAncestor(DependencyObject element, DependencyObject stopAt, params Type[] types)
    {
        for (var up = VisualTreeHelper.GetParent(element); up is not null && up != stopAt; up = VisualTreeHelper.GetParent(up))
        {
            if (types.Any(t => t.IsInstanceOfType(up)))
            {
                return true;
            }
        }

        return false;
    }

    private static TabItem SubTab(FinOpsTab tab, string header) =>
        ChartLayoutProbe.Logical<TabItem>(tab).FirstOrDefault(t => t.Header as string == header) ?? throw new Xunit.Sdk.XunitException($"the FinOps tab has no '{header}' sub-tab");

    private static List<string> Headers(FinOpsTab tab) =>
        ChartLayoutProbe.Logical<TabControl>(tab).First().Items.OfType<TabItem>().Select(t => t.Header?.ToString() ?? "").ToList();

    /// <summary>Twelve rows in each Utilization and Optimization grid, so a grid capped in height scrolls on its own and a grid that sizes to its rows does not.</summary>
    private static void FillGrids(FinOpsTab tab)
    {
        Find<DataGrid>(tab, "FinOpsTopTotalGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { DatabaseDisplay = $"db{i}", DatabaseName = $"db{i}", PctCpu = 1.0 * i, PctIo = 1.0, CpuTimeMs = 100L * i, ExecutionCount = 10L * i }).ToList();
        Find<DataGrid>(tab, "FinOpsTopAvgGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { DatabaseDisplay = $"db{i}", DatabaseName = $"db{i}", AvgCpuMs = 1.0 * i, AvgIoMb = 1.0, ExecutionCount = 10L * i }).ToList();
        Find<DataGrid>(tab, "FinOpsProvisioningTrendGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { DayDisplay = $"day {i}", Status = "RIGHT_SIZED", StatusDisplay = "Right sized", AvgCpuPct = 1.0, P95CpuPct = 2.0, MaxCpuPct = 3.0, MemoryRatio = 1.0 }).ToList();
        Find<DataGrid>(tab, "FinOpsIdleDatabasesDataGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { DatabaseName = $"db{i}", TotalSizeMb = 1.0 * i }).ToList();
        Find<DataGrid>(tab, "FinOpsTempdbPressureDataGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { Metric = $"metric {i}", CurrentMb = 1.0, PeakMb = 2.0 }).ToList();
        Find<DataGrid>(tab, "FinOpsWaitCategorySummaryDataGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { Category = $"category {i}", TotalWaitTimeMs = 1000L * i, WaitingTasks = 10L * i }).ToList();
        Find<DataGrid>(tab, "FinOpsExpensiveQueriesDataGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { DatabaseName = $"db{i}", QueryText = $"select {i}", TotalCpuMs = 100L * i }).ToList();
        Find<DataGrid>(tab, "FinOpsMemoryGrantEfficiencyDataGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { DayDisplay = $"day {i}", Day = new DateTime(2026, 1, i), AvgGrantedMb = 1.0, AvgUsedMb = 0.5 }).ToList();
    }

    /// <summary>Builds the real server tab under <paramref name="theme"/>, shows it, and runs <paramref name="body"/> with the theme's muted colour.</summary>
    private static List<string> WithServerTab(string theme, Func<ViewerServerTab, Window, Color, List<string>> body)
    {
        return StaTestThread.Run(() => WithTheme(theme, muted =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                new DarlingServer(1, "panel-cards-test", "panel-cards-test", true, null));
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

    private static Window Open(FrameworkElement content, int width, int height)
    {
        var window = new Window { Content = content, Width = width, Height = height, Left = 0, Top = 0, WindowStyle = WindowStyle.None, ResizeMode = ResizeMode.NoResize };
        window.Show();
        Settle(window);
        return window;
    }

    private static void Resize(Window window, int width, int height)
    {
        window.Width = width;
        window.Height = height;
        Settle(window);
    }

    private static void Settle(Window window)
    {
        window.UpdateLayout();
        Dispatcher.CurrentDispatcher.Invoke(DispatcherPriority.Loaded, () => { });
        window.UpdateLayout();
        ClippedTextProbe.Settle(window);
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

    private static Rect Bounds(FrameworkElement element, FrameworkElement ancestor) =>
        element.TransformToAncestor(ancestor).TransformBounds(new Rect(element.RenderSize));

    private static string Describe(Brush? brush) => brush is SolidColorBrush solid ? solid.Color.ToString() : brush?.ToString() ?? "null";

    /// <summary>Saves the window's content as a PNG under REFRESH_SHOTS_DIR when that directory is given; otherwise nothing.</summary>
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

    /// <summary>Runs <paramref name="body"/> with <paramref name="theme"/> and the shared styles merged into the app's resources, and gives it the theme's muted colour.</summary>
    private static T WithTheme<T>(string theme, Func<Color, T> body)
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
            return body(((SolidColorBrush)app.FindResource("ForegroundMutedBrush")).Color);
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
