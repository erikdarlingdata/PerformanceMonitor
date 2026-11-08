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
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using Darling.Tests;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Analysis.Recommendations;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8226: the Lite panels that still used the old layout take the card look the chart panels have: a <see cref="ChartPanel"/> (or the
/// card border) with a semibold title and a muted subtitle, one scroll area per page, grids that size to their rows, a placeholder
/// in each search box, and a calendar that shows only its month's weeks. Each fact builds the real tab on the shared STA thread with
/// the theme and the shared styles merged, shows it at the width a tab has beside the sidebar of a 1280x800 and a 900x600 window,
/// and reads the realized result. With REFRESH_SHOTS_DIR set, each screen is saved as a PNG.
/// </summary>
[Collection("credential-manager")]
public sealed class LitePanelCardsTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    private static readonly (int Width, int Height)[] Sizes = { (1280, 800), (900, 600) };

    /* The sidebar beside a tab in MainWindow.xaml: 280 px, or its 40 px rail below about 1100 DIPs (#8219). */
    private static int PaneWidth(int windowWidth) => windowWidth - (windowWidth < 1100 ? 40 : 280);

    /* The title style's weight and size, and the subtitle style's size, from PerformanceMonitor.Ui/Themes/SharedStyles.xaml. */
    private const double TitleSize = 14;
    private const double SubtitleSize = 11;

    /* The server tab's lists and banners that sit in a card, each named by the element a reader looks at. */
    private static readonly (string Element, string Shot)[] ServerTabCards =
    {
        ("RunningJobsGrid", "running-jobs"),
        ("SkippedCyclesSummary", "collection-health"),
        ("PerfmonCountersList", "perfmon"),
        ("MemoryClerksList", "memory-clerks"),
        ("MemoryBrokerGrid", "memory-broker"),
    };

    /* The Utilization and Optimization grids, each in a card, each given rows so a grid that scrolls on its own shows it. */
    private static readonly string[] UtilizationGrids = { "TopTotalGrid", "TopAvgGrid", "ProvisioningTrendGrid" };
    private static readonly string[] OptimizationGrids =
    {
        "IdleDatabasesDataGrid", "TempdbPressureDataGrid", "WaitCategorySummaryDataGrid", "ExpensiveQueriesDataGrid", "MemoryGrantEfficiencyDataGrid",
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

                /* The fatal-database banner is not a collection cycle, so it stands above the Collection Cycles card, whose title and subtitle do not describe it. */
                var banner = Find<FrameworkElement>(tab, "LocalDatabaseBanner");
                var card = Find<FrameworkElement>(tab, "CollectionHealthCard");
                if (CardOf(banner) is not null)
                {
                    found.Add($"LocalDatabaseBanner at {width}x{height}: sits inside the Collection Cycles card");
                }
                else if (!(banner.Visibility == Visibility.Collapsed || Bounds(banner, tab).Bottom <= Bounds(card, tab).Top + 0.5))
                {
                    found.Add($"LocalDatabaseBanner at {width}x{height}: does not sit above the Collection Cycles card");
                }
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

    [Theory]
    [InlineData(2026, 2, 4)]
    [InlineData(2026, 11, 5)]
    [InlineData(2026, 8, 6)]
    public void TheCalendar_ShowsOnlyTheWeeksOfItsMonth(int year, int month, int weeks)
    {
        var problems = StaTestThread.Run(() =>
        {
            /* The grid starts on the culture's first day of the week; the invariant culture's is Sunday, so the week counts above hold. */
            var saved = CultureInfo.CurrentCulture;
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
            try
            {
                return WithTheme("DarkTheme", _ =>
                {
                    var found = new List<string>();
                    var calendar = new PerformanceCalendar { DisplayMonth = new DateTime(year, month, 1) };
                    var window = Open(calendar, 700, 600);
                    try
                    {
                        var lastDay = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                        if (calendar.Cells.Count != weeks * 7)
                        {
                            found.Add($"{year}-{month:00}: {calendar.Cells.Count} cells, not the {weeks * 7} of its {weeks} weeks");
                        }

                        var last = calendar.Cells[^1].Date;
                        if (last >= lastDay.AddDays(7))
                        {
                            found.Add($"{year}-{month:00}: the last cell is {last:yyyy-MM-dd}, a week past the month's last day {lastDay:yyyy-MM-dd}");
                        }

                        /* The realized day cells: one per cell, in rows of seven. */
                        var dayCells = Visual<UniformGrid>(calendar).FirstOrDefault(g => g.Columns == 7 && g.Children.Count > 7);
                        if (dayCells is null)
                        {
                            found.Add($"{year}-{month:00}: no seven-column grid of day cells was realized");
                        }
                        else if (dayCells.Children.Count != weeks * 7)
                        {
                            found.Add($"{year}-{month:00}: {dayCells.Children.Count} day cells drawn, not {weeks * 7}");
                        }

                        Shoot(window, $"DarkTheme-700x600-calendar-{year}-{month:00}.png");
                    }
                    finally
                    {
                        window.Close();
                    }

                    return found;
                });
            }
            finally
            {
                CultureInfo.CurrentCulture = saved;
            }
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void AnIncidentWithSeveralFindings_NamesItsTitleOnce_InTheSectionHeader()
    {
        var items = new[]
        {
            new LiteRecommendationItem { IncidentId = "i1", Severity = LiteRecommendationSeverity.Warning, RawSeverity = 2, Title = "Plan cache bloat", AdviceText = "Advice one.", ServerName = "s" },
            new LiteRecommendationItem { IncidentId = "i1", Severity = LiteRecommendationSeverity.Warning, RawSeverity = 2, Title = "Plan cache bloat", AdviceText = "Advice two.", ServerName = "s" },
            new LiteRecommendationItem { IncidentId = "i1", Severity = LiteRecommendationSeverity.Info, RawSeverity = 1, Title = "Ad hoc queries", AdviceText = "Advice three.", ServerName = "s" },
        };

        var section = Assert.Single(LiteRecommendationsViewModel.FromItems(items).Sections);

        Assert.StartsWith("Plan cache bloat", section.Header, StringComparison.Ordinal);
        /* The header already names the primary finding; a card repeats the title only when its own differs. */
        Assert.Equal(new[] { false, false, true }, section.Cards.Select(c => c.ShowTitle).ToArray());
    }

    /// <summary>The words themselves are the shared <see cref="RecommendationTitleText"/>'s, tested once in Darling.Tests; the card and its section header read through it.</summary>
    [Fact]
    public void AFindingNamedByItsToken_ReadsAsWords_OnTheCardAndInTheHeader()
    {
        var item = new LiteRecommendationItem { IncidentId = "i1", Severity = LiteRecommendationSeverity.Warning, RawSeverity = 2, Title = "RESOURCE_MEMPHYSICAL_LOW", ServerName = "s" };
        var section = Assert.Single(LiteRecommendationsViewModel.FromItems(new[] { item }).Sections);

        var expected = RecommendationTitleText.Readable("RESOURCE_MEMPHYSICAL_LOW");
        Assert.NotEqual("RESOURCE_MEMPHYSICAL_LOW", expected);
        Assert.Equal(expected, Assert.Single(section.Cards).Title);
        Assert.StartsWith(expected, section.Header, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void EveryChartCardTitle_OnTheServerTab_IsInTitleCase(string theme)
    {
        var problems = WithServerTab(theme, (tab, _, _) =>
            ChartLayoutProbe.Logical<ChartPanel>(tab).Select(card => card.Title).Where(title => !IsTitleCase(title)).Select(title => $"'{title}' is not in title case").ToList());

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// Title case as the chart cards use it: every word of four or more letters starts with a capital, "tempdb" excepted since
    /// that is the database's name. Short words (and, by, of, vs), acronyms and units are free.
    /// </summary>
    internal static bool IsTitleCase(string title) =>
        title.Split(new[] { ' ', '-', '/' }, StringSplitOptions.RemoveEmptyEntries)
            .All(word => word.Length < 4 || word == "tempdb" || !char.IsLetter(word[0]) || char.IsUpper(word[0]));

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
        Find<DataGrid>(tab, "TopTotalGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { DatabaseDisplay = $"db{i}", DatabaseName = $"db{i}", PctCpu = 1.0 * i, PctIo = 1.0, CpuTimeMs = 100L * i, ExecutionCount = 10L * i }).ToList();
        Find<DataGrid>(tab, "TopAvgGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { DatabaseDisplay = $"db{i}", DatabaseName = $"db{i}", AvgCpuMs = 1.0 * i, AvgIoMb = 1.0, ExecutionCount = 10L * i }).ToList();
        Find<DataGrid>(tab, "ProvisioningTrendGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { DayDisplay = $"day {i}", Status = "RIGHT_SIZED", StatusDisplay = "Right sized", AvgCpuPct = 1.0, P95CpuPct = 2.0, MaxCpuPct = 3.0, MemoryRatio = 1.0 }).ToList();
        Find<DataGrid>(tab, "IdleDatabasesDataGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { DatabaseName = $"db{i}", TotalSizeMb = 1.0 * i }).ToList();
        Find<DataGrid>(tab, "TempdbPressureDataGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { Metric = $"metric {i}", CurrentMb = 1.0, PeakMb = 2.0 }).ToList();
        Find<DataGrid>(tab, "WaitCategorySummaryDataGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { Category = $"category {i}", TotalWaitMs = 1000L * i }).ToList();
        Find<DataGrid>(tab, "ExpensiveQueriesDataGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { Database = $"db{i}", QueryText = $"select {i}", TotalCpuMs = 100L * i }).ToList();
        Find<DataGrid>(tab, "MemoryGrantEfficiencyDataGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { Day = $"day {i}", AvgGrantedMb = 1.0, AvgUsedMb = 0.5 }).ToList();
    }

    private static List<string> WithServerTab(string theme, Func<ServerTab, Window, Color, List<string>> body)
    {
        var dir = Path.Combine(Path.GetTempPath(), "PanelCards_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            return StaTestThread.Run(() => WithTheme(theme, muted =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "panel-cards-test", DisplayName = "panel-cards-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                /* Switching to a tab would start that tab's load against a server that is not there. */
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
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
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
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

    /// <summary>Runs <paramref name="body"/> with <paramref name="theme"/> and the shared styles as the app's resources, and gives it the theme's muted colour.</summary>
    private static T WithTheme<T>(string theme, Func<Color, T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var saved = app.Resources.MergedDictionaries.ToList();
        app.Resources.MergedDictionaries.Clear();
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}.xaml") });
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml") });
        try
        {
            return body(((SolidColorBrush)app.FindResource("ForegroundMutedBrush")).Color);
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
}
