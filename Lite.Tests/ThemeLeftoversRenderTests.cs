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
            window.Show();
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
        var (buttons, columns, lines) = Themed(app =>
        {
            var tab = new FinOpsTab();
            var window = new Window { Content = tab, Width = 1280, Height = 800 };
            window.Show();
            try
            {
                var grid = (DataGrid)tab.FindName("RecommendationsDataGrid");
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
                return (filterButtons, grid.Columns.Count, lineCount);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Equal(columns, buttons);
        Assert.True(lines >= 2, $"the Detail text drew on {lines} line(s)");
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
            window.Show();
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

    private static ServerSummaryItem FleetCard(int blocking, int deadlocks) => new()
    {
        DisplayName = "SQL01", ServerName = "SQL01", IsOnline = true, CpuPercent = 20, OtherProcessCpuPercent = 5, MemoryMb = 32768,
        BlockingCount = blocking, DeadlockCount = deadlocks,
        LastCollectionTime = DateTime.UtcNow, CollectionFreshness = ServerFreshness.Fresh,
    };

    /// <summary>The border a fleet card renders under <paramref name="theme"/>, after the app was themed Dark and switched at run time.</summary>
    private static (Color Border, Color Neutral, Color Band, Color Background, List<(string Text, Color Fore)> Texts) RenderedCard(string theme, ServerSummaryItem item) =>
        Themed(app =>
        {
            ThemeManager.Apply(theme);
            var card = new ServerOverviewCard { DataContext = item };
            var window = new Window { Content = card, Width = 420, Height = 360 };
            window.Show();
            try
            {
                Settle(window);
                var box = Visuals<OverviewCardBorder>(card).First();
                var found = Visuals<TextBlock>(card)
                    .Where(t => t.Text == "Online" || t.Text == item.CpuDisplay || t.Text == item.LastCollectionDisplay)
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

    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheBandTextOnAFleetCard_IsReadable_InTheLightThemes(string theme)
    {
        var (_, _, _, background, texts) = RenderedCard(theme, FleetCard(0, 0));

        Assert.True(texts.Count >= 3, $"found {texts.Count} band texts");
        foreach (var (text, fore) in texts)
        {
            var ratio = WcagContrast.Ratio(fore, background);
            Assert.True(ratio >= 4.5, $"'{text}' is {ratio:0.00}:1 on the card in {theme}");
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
            window.Show();
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
            window.Show();
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
            window.Show();
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
            window.Show();
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
                window.Show();
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
                    }

                    return found;
                }
                finally
                {
                    window.Close();
                }
            });

            // The fill is not read: the hover state paints it, so it follows where the mouse happens to be. The ink and underline are the selected state's own.
            Assert.Equal(6, seen.Count);
            foreach (var r in seen)
            {
                Assert.True(r.Ink == r.WantInk, $"{r.Theme} {r.Tab}: the selected tab's text is {r.Ink}, the theme says {r.WantInk}");
                Assert.True(r.Underline == r.WantUnderline, $"{r.Theme} {r.Tab}: the selected tab's underline is {r.Underline}, the theme says {r.WantUnderline}");
            }
        }
        finally
        {
            try { System.IO.Directory.Delete(dir, true); } catch (System.IO.IOException) { }
        }
    }
}
