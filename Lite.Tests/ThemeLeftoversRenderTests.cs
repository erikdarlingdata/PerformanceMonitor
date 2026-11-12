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
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8221: dark colors left over in Lite's Light and Cool Breeze themes. Every check builds the real control under the Dark theme, switches the
/// theme at run time the way Settings does (<see cref="ThemeManager.Apply"/>), and reads the color from the element that rendered.
/// </summary>
[Collection("credential-manager")]
public sealed class ThemeLeftoversRenderTests
{
    private static Uri ThemeUri(string name) => new($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{name}Theme.xaml");

    /// <summary>Runs <paramref name="body"/> on the STA thread with the app themed Dark; the body switches themes itself. Puts the app back after.</summary>
    private static T Themed<T>(Func<Application, T> body) =>
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var before = app.Resources.MergedDictionaries.ToList();
            var previous = ThemeManager.CurrentTheme;
            ThemeManager.ThemeUriProvider = name => ThemeUri(name);
            try
            {
                ThemeManager.Apply("Dark");
                return body(app);
            }
            finally
            {
                ThemeManager.Apply(previous);
                app.Resources.MergedDictionaries.Clear();
                foreach (var dictionary in before)
                {
                    app.Resources.MergedDictionaries.Add(dictionary);
                }

                ThemeManager.ThemeUriProvider = ThemeManager.ThemeUri;
            }
        });

    private static Color ColorOf(Brush? brush) => brush is SolidColorBrush solid ? solid.Color : Colors.Magenta;

    private static Color Resource(Application app, string key) => ColorOf((Brush)app.FindResource(key));

    private static IEnumerable<T> Visuals<T>(DependencyObject root) where T : DependencyObject
    {
        var count = VisualTreeHelper.GetChildrenCount(root);
        for (var i = 0; i < count; i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            if (child is T match)
            {
                yield return match;
            }

            foreach (var inner in Visuals<T>(child))
            {
                yield return inner;
            }
        }
    }

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

    private static void Settle(Window window)
    {
        window.UpdateLayout();
        ClippedTextProbe.Settle(window);
    }

    private static DataGridColumnHeader HeaderOf(DataGrid grid, int column) =>
        Visuals<DataGridColumnHeader>(grid).First(h => h.Column == grid.Columns[column]);

    // ── FinOps > Recommendations ───────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheRecommendationsDetailHeader_FollowsTheThemeAfterARunTimeSwitch(string theme)
    {
        var (detail, category, expected, dark) = Themed(app =>
        {
            var darkHeader = ColorOf(((Setter)((Style)app.FindResource(typeof(DataGridColumnHeader))).Setters
                .OfType<Setter>().First(s => s.Property == Control.BackgroundProperty)).Value as Brush);
            var tab = new FinOpsTab();
            var window = new Window { Content = tab, Width = 1280, Height = 800 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var grid = (DataGrid)tab.FindName("RecommendationsDataGrid");
                Reveal(grid);
                Settle(window);
                ThemeManager.Apply(theme);
                Settle(window);
                var want = ColorOf(((Setter)((Style)app.FindResource(typeof(DataGridColumnHeader))).Setters
                    .OfType<Setter>().First(s => s.Property == Control.BackgroundProperty)).Value as Brush);
                return (ColorOf(HeaderOf(grid, 4).Background), ColorOf(HeaderOf(grid, 0).Background), want, darkHeader);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.NotEqual(dark, expected);
        Assert.Equal(expected, category);
        Assert.Equal(expected, detail);
    }

    [Fact]
    public void TheRecommendationsGrid_HasAFilterButtonOnEveryColumn_AndWrapsItsDetail()
    {
        var (buttons, columns, lines, tips) = Themed(app =>
        {
            var tab = new FinOpsTab();
            var window = new Window { Content = tab, Width = 1280, Height = 800 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var grid = (DataGrid)tab.FindName("RecommendationsDataGrid");
                /* The Detail and Est. Savings headers carry a tooltip on the whole header (the filter button and the title), as the cells do. */
                var tipped = grid.Columns.Where(c => c.Header is StackPanel p && p.Children.OfType<TextBlock>().Any(t => t.Text is "Detail" or "Est. Savings ($/mo)"))
                    .Select(c => ((StackPanel)c.Header).ToolTip as string).ToList();
                Reveal(grid);
                grid.ItemsSource = new List<RecommendationRow>
                {
                    new()
                    {
                        Category = "Memory", Severity = "High", Confidence = "High", Finding = "Max server memory is too high",
                        Detail = string.Join(" ", Enumerable.Repeat("Current allocation leaves the operating system too little memory to work with.", 6)),
                    },
                };
                Settle(window);
                var filterButtons = Enumerable.Range(0, grid.Columns.Count)
                    .Count(i => Visuals<Button>(HeaderOf(grid, i)).Any(b => b.Tag is string));
                var block = Visuals<DataGridCell>(grid).Where(c => c.Column == grid.Columns[4])
                    .SelectMany(c => Visuals<TextBlock>(c)).First();
                var lineCount = (int)Math.Round(block.ActualHeight / (block.FontSize * block.FontFamily.LineSpacing));
                return (filterButtons, grid.Columns.Count, lineCount, tipped);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Equal(2, tips.Count);
        Assert.All(tips, tip => Assert.True(tip is { Length: >= 20 }, "a findings header has no tooltip"));
        Assert.Equal(columns, buttons);
        Assert.True(lines >= 2, $"the Detail text drew on {lines} line(s)");
    }

    /// <summary>
    /// The Recommendations grid is loaded through its filter manager (<c>UpdateData</c>), which the load path in the tab now uses. A filter set on
    /// one server's rows narrows the grid, and the switch to another server (which clears the filters and reloads, as the tab does) shows all of
    /// the new server's rows rather than the old filter's answer.
    /// </summary>
    [Fact]
    public void TheRecommendationsGrid_ShowsTheNewServersRows_AfterAFilterAndAServerSwitch()
    {
        var (first, filtered, second) = Themed(app =>
        {
            var tab = new FinOpsTab();
            var window = new Window { Content = tab, Width = 1280, Height = 800 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var grid = (DataGrid)tab.FindName("RecommendationsDataGrid");
                Reveal(grid);
                var manager = (DataGridFilterManager<RecommendationRow>)typeof(FinOpsTab)
                    .GetField("_recommendationsFilterMgr", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(tab)!;
                static RecommendationRow Row(string category, string finding) => new() { Category = category, Severity = "High", Confidence = "High", Finding = finding, Detail = "d" };

                manager.UpdateData(new List<RecommendationRow> { Row("Memory", "a1"), Row("Memory", "a2"), Row("Cpu", "a3") });
                Settle(window);
                var firstRows = grid.Items.Count;

                manager.SetFilter(new ColumnFilterState { ColumnName = "Category", Operator = FilterOperator.Equals, Value = "Memory" });
                var filteredRows = grid.Items.Count;

                manager.ClearFilters();
                manager.UpdateData(new List<RecommendationRow> { Row("Storage", "b1"), Row("Cpu", "b2"), Row("Cpu", "b3"), Row("Cpu", "b4") });
                Settle(window);
                return (firstRows, filteredRows, grid.Items.OfType<RecommendationRow>().Select(r => r.Finding).ToList());
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Equal(3, first);
        Assert.Equal(2, filtered);
        Assert.Equal(new[] { "b1", "b2", "b3", "b4" }, second);
    }

    /// <summary>
    /// The server picker's items are named by the server for a screen reader (moved here from a text pin on the XAML, which the theme-following
    /// item style changed). Checked on the rendered container, in the Light theme after a run-time switch.
    /// </summary>
    [Theory]
    [InlineData("FinOps")]
    [InlineData("Recommendations")]
    public void TheServerBoxItems_AreNamedByTheServer_AfterARunTimeThemeSwitch(string host)
    {
        var name = Themed(app =>
        {
            FrameworkElement tab = host == "FinOps" ? new FinOpsTab() : new RecommendationsTab();
            var window = new Window { Content = tab, Width = 1280, Height = 800 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                ThemeManager.Apply("Light");
                var box = (ComboBox)tab.FindName("ServerSelector");
                box.ItemsSource = new[] { new PerformanceMonitorLite.Models.ServerConnection { ServerName = "sql01", DisplayName = "SQL01", ReadOnlyIntent = true } };
                box.IsDropDownOpen = true;
                Settle(window);
                var item = (ComboBoxItem)box.ItemContainerGenerator.ContainerFromIndex(0);
                return System.Windows.Automation.AutomationProperties.GetName(item);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Equal("SQL01 (Read-Only)", name);
    }

    // ── Overview fleet card ────────────────────────────────────────────────────────────────────

    private static ServerSummaryItem FleetCard(int blocking, int deadlocks, ServerFreshness freshness = ServerFreshness.Fresh) => new()
    {
        DisplayName = "SQL01", ServerName = "SQL01", IsOnline = true, CpuPercent = 20, OtherProcessCpuPercent = 5, MemoryMb = 32768,
        BlockingCount = blocking, DeadlockCount = deadlocks,
        LastCollectionTime = DateTime.UtcNow, CollectionFreshness = freshness,
    };

    /// <summary>The border a fleet card renders under <paramref name="theme"/>, after the app was themed Dark and switched at run time.</summary>
    private static (Color Border, Color Neutral, Color Band, Color Background, List<(string Text, Color Fore)> Texts) RenderedCard(string theme, ServerSummaryItem item) =>
        Themed(app =>
        {
            ThemeManager.Apply(theme);
            var card = new ServerOverviewCard { DataContext = item };
            var window = new Window { Content = card, Width = 420, Height = 360 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                Settle(window);
                var box = Visuals<OverviewCardBorder>(card).First();
                var found = Visuals<TextBlock>(card)
                    .Where(t => t.Text == "Online" || t.Text == item.CpuDisplay || t.Text == item.LastCollectionDisplay
                        || (item.BlockingCount > 0 && t.Text == item.BlockingDisplay))
                    .Select(t => (t.Text, Fore: ColorOf(t.Foreground)))
                    .ToList();
                return (ColorOf(box.BorderBrush), Resource(app, "CardBorderBrush"), ColorOf(item.StatusBrush), ColorOf(box.Background), found);
            }
            finally
            {
                window.Close();
            }
        });

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void AHealthyFleetCard_HasTheThemesNeutralBorder_InEveryTheme(string theme)
    {
        var (border, neutral, band, _, _) = RenderedCard(theme, FleetCard(0, 0));

        Assert.Equal(neutral, border);
        Assert.NotEqual(band, border);
    }

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void AWarningAndACriticalFleetCard_KeepTheirBandBorder_InEveryTheme(string theme)
    {
        var (warning, neutralW, _, _, _) = RenderedCard(theme, FleetCard(3, 0));
        var (critical, neutralC, _, _, _) = RenderedCard(theme, FleetCard(0, 2));

        Assert.NotEqual(neutralW, warning);
        Assert.NotEqual(neutralC, critical);
        Assert.NotEqual(warning, critical);
    }

    private static double Distance(Color a, Color b) =>
        Math.Sqrt(Math.Pow(a.R - b.R, 2) + Math.Pow(a.G - b.G, 2) + Math.Pow(a.B - b.B, 2));

    /// <summary>The color's hue in degrees, 0 to 360 (0 is red, 30 orange, 60 yellow).</summary>
    private static double Hue(Color c)
    {
        double r = c.R / 255.0, g = c.G / 255.0, b = c.B / 255.0;
        var max = Math.Max(r, Math.Max(g, b));
        var delta = max - Math.Min(r, Math.Min(g, b));
        var hue = delta == 0 ? 0
            : max == r ? 60 * (((g - b) / delta) % 6)
            : max == g ? 60 * (((b - r) / delta) + 2)
            : 60 * (((r - g) / delta) + 4);
        return hue < 0 ? hue + 360 : hue;
    }

    /// <summary>
    /// A blocking card and a stale one stay apart in every theme, in Dark's order: blocking is the orange and stale the yellow, so
    /// blocking's hue is lower (toward orange or red) than stale's. Light and Cool Breeze once mapped both to the theme's Warning brush,
    /// then gave blocking a gold-brown and stale the Warning orange, which reversed the order and left the two borders near alike.
    /// Dark keeps them 30 apart in RGB, so the two light themes may not be closer than 25.
    /// </summary>
    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void ABlockingFleetCard_LooksDifferentFromAStaleOne_InEveryTheme(string theme)
    {
        var (blocking, _, _, _, _) = RenderedCard(theme, FleetCard(3, 0));
        var (stale, _, _, _, _) = RenderedCard(theme, FleetCard(0, 0, ServerFreshness.Stale));

        Assert.NotEqual(blocking, stale);
        Assert.True(Hue(blocking) < Hue(stale), $"blocking {blocking} (hue {Hue(blocking):0}) is not toward orange of stale {stale} (hue {Hue(stale):0}) in {theme}");
        Assert.True(Distance(blocking, stale) >= 25, $"blocking {blocking} and stale {stale} are {Distance(blocking, stale):0} apart in {theme}");
    }

    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheBandTextOnAFleetCard_IsReadable_InTheLightThemes(string theme)
    {
        foreach (var card in new[] { FleetCard(0, 0), FleetCard(3, 0), FleetCard(0, 0, ServerFreshness.Stale) })
        {
            var (_, _, _, background, texts) = RenderedCard(theme, card);

            Assert.True(texts.Count >= 3, $"found {texts.Count} band texts");
            foreach (var (text, fore) in texts)
            {
                var ratio = WcagContrast.Ratio(fore, background);
                Assert.True(ratio >= 4.5, $"'{text}' is {ratio:0.00}:1 on the card in {theme}");
            }
        }
    }

    // ── Buttons, text boxes, bar tracks, Cool Breeze grids ────────────────────────────────────

    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void AButtonAndATextBox_HaveAnOutlineOfThreeToOne_InTheLightThemes(string theme)
    {
        var (button, box, page) = Themed(app =>
        {
            var panel = new StackPanel();
            var b = new Button { Content = "Refresh" };
            var t = new TextBox { Text = "filter" };
            panel.Children.Add(b);
            panel.Children.Add(t);
            var window = new Window { Content = panel, Width = 300, Height = 200 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                ThemeManager.Apply(theme);
                Settle(window);
                var bt = Visuals<Border>(b).First();
                var tt = Visuals<Border>(t).First();
                var worst = new[] { "BackgroundBrush", "BackgroundDarkBrush", "BackgroundLightBrush" }
                    .Select(k => Resource(app, k)).ToList();
                return ((ColorOf(bt.BorderBrush), bt.BorderThickness.Left), (ColorOf(tt.BorderBrush), tt.BorderThickness.Left), worst);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.True(button.Item2 > 0, "the button draws no border");
        Assert.True(box.Item2 > 0, "the text box draws no border");
        foreach (var surface in page)
        {
            Assert.True(WcagContrast.Ratio(button.Item1, surface) >= 3, $"button outline on {surface} in {theme}");
            Assert.True(WcagContrast.Ratio(box.Item1, surface) >= 3, $"text box outline on {surface} in {theme}");
        }
    }

    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheUtilizationBarTracks_CanBeSeen_InTheLightThemes(string theme)
    {
        var (tracks, page) = Themed(app =>
        {
            var tab = new FinOpsTab();
            var window = new Window { Content = tab, Width = 1280, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var content = (FrameworkElement)tab.FindName("UtilizationContent");
                content.Visibility = Visibility.Visible;
                Reveal(content);
                ThemeManager.Apply(theme);
                Settle(window);
                var found = Visuals<Border>(content).Where(b => b.Height == 16 && string.IsNullOrEmpty(b.Name) && b.Background is SolidColorBrush)
                    .Select(b => (Fill: ColorOf(b.Background), Edge: ColorOf(b.BorderBrush), b.BorderThickness.Left)).ToList();
                return (found, Resource(app, "BackgroundBrush"));
            }
            finally
            {
                window.Close();
            }
        });

        Assert.True(tracks.Count >= 5, $"found {tracks.Count} bar tracks");
        foreach (var track in tracks)
        {
            Assert.True(track.Left > 0);
            Assert.True(WcagContrast.Ratio(track.Edge, page) >= 3, $"a track edge is {WcagContrast.Ratio(track.Edge, page):0.00}:1 in {theme}");
        }
    }

    [Fact]
    public void CoolBreezeGrids_HaveGentleBanding_AndAHeaderCloseToTheBody()
    {
        var (row, alt, header) = Themed(app =>
        {
            var grid = new DataGrid { AutoGenerateColumns = true, ItemsSource = new[] { new { A = 1 }, new { A = 2 } } };
            var window = new Window { Content = grid, Width = 400, Height = 300 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                ThemeManager.Apply("CoolBreeze");
                Settle(window);
                return (ColorOf(grid.RowBackground), ColorOf(grid.AlternatingRowBackground), ColorOf(Visuals<DataGridColumnHeader>(grid).First().Background));
            }
            finally
            {
                window.Close();
            }
        });

        Assert.True(WcagContrast.Ratio(row, alt) <= 1.05, $"row banding {WcagContrast.Ratio(row, alt):0.000}:1");
        Assert.True(WcagContrast.Ratio(row, header) <= 1.12, $"header against the body {WcagContrast.Ratio(row, header):0.000}:1");
    }

    // ── Daily Summary ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void TheCalendarWarningColor_IsTheSameInEveryTheme_AndTheLegendSwatchMatchesTheCells()
    {
        var results = Themed(app =>
        {
            var seen = new List<(Color Cell, double CellOpacity, Color Swatch, double SwatchOpacity)>();
            var calendar = new PerformanceCalendar
            {
                DisplayMonth = DateTime.UtcNow,
                Days = new[]
                {
                    new PerformanceCalendarDay { Date = new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, 2), Band = DailyHealthBand.Warning },
                },
            };
            var window = new Window { Content = calendar, Width = 1000, Height = 800 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                foreach (var theme in new[] { "Dark", "Light", "CoolBreeze" })
                {
                    ThemeManager.Apply(theme);
                    Settle(window);
                    var wash = Visuals<System.Windows.Shapes.Rectangle>(calendar)
                        .First(r => r.Opacity > 0.5 && r.RadiusX == 3 && ColorOf(r.Fill) != Resource(app, "SuccessBrush") && ColorOf(r.Fill) != Resource(app, "ForegroundMutedBrush"));
                    var swatch = Visuals<System.Windows.Shapes.Rectangle>(calendar)
                        .First(r => r.Width == 12 && ColorOf(r.Fill) == ColorOf(wash.Fill));
                    seen.Add((ColorOf(wash.Fill), wash.Opacity, ColorOf(swatch.Fill), swatch.Opacity));
                }

                return seen;
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Equal(3, results.Count);
        Assert.All(results, r => Assert.Equal(results[0].Cell, r.Cell));
        Assert.All(results, r => Assert.Equal(r.CellOpacity, r.SwatchOpacity, 2));
    }

    // ── The real server tab: selected main tabs after a run-time switch ────────────────────────

    [Fact]
    public void TheSelectedServerTabs_TakeTheCurrentThemesInkAndUnderline_AfterRunTimeSwitchesDarkLightCoolBreeze()
    {
        var dir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "ThemeLeftovers_" + Guid.NewGuid().ToString("N")[..8]);
        System.IO.Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new PerformanceMonitorLite.Database.DuckDbInitializer(System.IO.Path.Combine(dir, "test.duckdb"));
            var seen = Themed(app =>
            {
                var server = new PerformanceMonitorLite.Models.ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "theme-test", DisplayName = "theme-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = 1280, Height = 800, Left = 0, Top = 0 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    var main = (TabControl)tab.FindName("MainTabControl");
                    var found = new List<(string Theme, string Tab, Color Ink, Color WantInk, Color Underline, Color WantUnderline)>();
                    foreach (var theme in new[] { "Dark", "Light", "CoolBreeze" })
                    {
                        ThemeManager.Apply(theme);
                        foreach (var header in new[] { "System Events", "Configuration Changes" })
                        {
                            var item = main.Items.OfType<TabItem>().First(t => Equals(t.Header, header));
                            main.SelectedItem = item;
                            Settle(window);
                            var chrome = (Border)item.Template.FindName("border", item);
                            found.Add((theme, header, ColorOf(item.Foreground), Resource(app, "ForegroundBrush"),
                                ColorOf(chrome.BorderBrush), Resource(app, "AccentBrush")));
                        }

                        /* #8221 round 2: the sub-tab row under Queries is a second TabControl with its own ItemContainerStyle. */
                        main.SelectedItem = main.Items.OfType<TabItem>().First(t => Equals(t.Header, "Queries"));
                        Settle(window);
                        var sub = (TabControl)tab.FindName("QueriesSubTabControl");
                        var subItem = (TabItem)sub.SelectedItem;
                        var subChrome = (Border)subItem.Template.FindName("border", subItem);
                        found.Add((theme, "Queries > " + subItem.Header, ColorOf(subItem.Foreground), Resource(app, "ForegroundBrush"),
                            ColorOf(subChrome.BorderBrush), Resource(app, "AccentBrush")));
                    }

                    return found;
                }
                finally
                {
                    window.Close();
                }
            });

            // The fill is not read: the hover state paints it, so it follows where the mouse happens to be. The ink and underline are the selected state's own.
            Assert.Equal(9, seen.Count);
            var stale = seen
                .Where(r => r.Ink != r.WantInk || r.Underline != r.WantUnderline)
                .Select(r => $"{r.Theme} {r.Tab}: text {r.Ink} (theme {r.WantInk}), underline {r.Underline} (theme {r.WantUnderline})")
                .ToList();
            Assert.True(stale.Count == 0, string.Join("; ", stale));
        }
        finally
        {
            try { System.IO.Directory.Delete(dir, true); } catch (System.IO.IOException) { }
        }
    }

    // ── Availability Group cards: the severity text ─────────────────────────────────────────────

    /// <summary>The first opaque solid fill behind <paramref name="element"/>: a Border, panel or control (a grid cell) up its visual parents.</summary>
    private static Color BackgroundBehind(DependencyObject element)
    {
        for (var at = VisualTreeHelper.GetParent(element); at is not null; at = VisualTreeHelper.GetParent(at))
        {
            Brush? fill = at switch { Border b => b.Background, Panel p => p.Background, Control c => c.Background, _ => null };
            if (fill is SolidColorBrush { Color.A: > 0 } solid)
            {
                return solid.Color;
            }
        }

        return Colors.Magenta;
    }

    /// <summary>
    /// Every fill <paramref name="text"/> can be drawn on. A grid row's own background is a hover fill while the mouse is over it, so
    /// reading it off the tree gave a different answer depending on where the mouse was. A row's text is measured on each fill a
    /// row can show (its row, alternating row and hover fills, read from the grid and the row style); anything else has the one
    /// fill behind it.
    /// </summary>
    private static List<(string Surface, Color Fill)> SurfacesBehind(TextBlock text)
    {
        DataGridRow? row = null;
        for (var at = VisualTreeHelper.GetParent(text); at is not null && row is null; at = VisualTreeHelper.GetParent(at))
        {
            row = at as DataGridRow;
        }

        if (row is null)
        {
            return new() { ("card", BackgroundBehind(text)) };
        }

        var grid = (DataGrid)ItemsControl.ItemsControlFromItemContainer(row);
        var hover = row.Style.Triggers.OfType<Trigger>()
            .First(t => t.Property == UIElement.IsMouseOverProperty).Setters.OfType<Setter>()
            .First(s => s.Property == Control.BackgroundProperty).Value;
        return new()
        {
            ("row", ColorOf(grid.RowBackground)),
            ("alternating row", ColorOf(grid.AlternatingRowBackground)),
            ("hovered row", ColorOf(hover as Brush)),
        };
    }

    private static List<AgTopologyCard> OneGroup()
    {
        var at = new DateTime(2026, 9, 25, 12, 0, 0);
        var replicas = new List<AgTopologyReplicaRow>
        {
            new()
            {
                ServerId = 1, ServerName = "NODE0", CollectionTime = at, AgName = "AG0", ReplicaServerName = "NODE0_0",
                RoleDesc = "PRIMARY", OperationalStateDesc = "ONLINE", ConnectedStateDesc = "CONNECTED", RecoveryHealthDesc = "ONLINE",
                SynchronizationHealthDesc = "HEALTHY", AvailabilityModeDesc = "SYNCHRONOUS_COMMIT", FailoverModeDesc = "AUTOMATIC",
            },
        };
        var databases = new List<AgTopologyDatabaseRow>
        {
            new()
            {
                ServerId = 1, ServerName = "NODE0", CollectionTime = at, AgName = "AG0", DatabaseName = "DB0", ReplicaServerName = "NODE0_0",
                SynchronizationStateDesc = "SYNCHRONIZED", AvailabilityModeDesc = "SYNCHRONOUS_COMMIT", IsSuspended = false,
                SecondaryLagSeconds = 0, LogSendQueueKb = 0, RedoQueueKb = 0, LogSendRateKbPerSec = 0, RedoRateKbPerSec = 0,
            },
        };
        return AgTopology.BuildCards(replicas, databases);
    }

    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheSeverityTextOnAnAvailabilityGroupCard_IsReadable_AfterARunTimeSwitchToTheLightThemes(string theme)
    {
        var texts = Themed(app =>
        {
            var tab = new AvailabilityGroupsTab();
            var window = new Window { Content = tab, Width = 1280, Height = 800 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                tab.Render(OneGroup());
                Settle(window);
                ThemeManager.Apply(theme);
                Settle(window);
                return Visuals<TextBlock>(tab)
                    .Where(t => t.Text is "Healthy" or "SYNCHRONIZED")
                    .SelectMany(t => SurfacesBehind(t).Select(s => (t.Text, s.Surface, Ratio: WcagContrast.Ratio(ColorOf(t.Foreground), s.Fill))))
                    .ToList();
            }
            finally
            {
                window.Close();
            }
        });

        /* The group's "Healthy" badge and the sync state "SYNCHRONIZED" take a severity color. */
        Assert.True(texts.Select(t => t.Text).Distinct().Count() == 2, $"found {texts.Count} severity texts in {theme}: {string.Join(", ", texts.Select(t => t.Text))}");
        Assert.Contains(texts, t => t.Text == "SYNCHRONIZED" && t.Surface == "hovered row");
        foreach (var (text, surface, ratio) in texts)
        {
            Assert.True(ratio >= 4.5, $"'{text}' is {ratio:0.00}:1 on its {surface} in {theme}");
        }
    }

    // ── The other Lite tab rows: the FinOps tab's sub-tabs, and a top tab row written like the main window's ──

    [Fact]
    public void TheSelectedTabsOfTheFinOpsAndTopRows_TakeTheCurrentThemesInk_AfterRunTimeSwitchesDarkLightCoolBreeze()
    {
        var seen = Themed(app =>
        {
            var finOps = new FinOpsTab();
            /* The main window's row: TabItems under an app-level TabControl, no ItemContainerStyle of their own. */
            var top = new TabControl { Background = Brushes.Transparent, BorderThickness = new Thickness(0) };
            top.Items.Add(new TabItem { Header = "Overview", Content = new TextBlock { Text = "a" } });
            top.Items.Add(new TabItem { Header = "FinOps", Content = finOps });
            top.SelectedIndex = 1;
            var window = new Window { Content = top, Width = 1280, Height = 900, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var sub = Visuals<TabControl>(finOps).First();
                var found = new List<(string Theme, string Row, Color Ink, Color WantInk)>();
                foreach (var theme in new[] { "Dark", "Light", "CoolBreeze" })
                {
                    ThemeManager.Apply(theme);
                    foreach (var (row, tabs) in new[] { ("top", top), ("FinOps", sub) })
                    {
                        tabs.SelectedIndex = 1;
                        Settle(window);
                        var item = (TabItem)tabs.SelectedItem;
                        found.Add((theme, row + " > " + item.Header, ColorOf(item.Foreground), Resource(app, "ForegroundBrush")));
                    }
                }

                return found;
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Equal(6, seen.Count);
        foreach (var r in seen)
        {
            Assert.True(r.Ink == r.WantInk, $"{r.Theme} {r.Row}: the selected tab's text is {r.Ink}, the theme says {r.WantInk}");
        }
    }
}
