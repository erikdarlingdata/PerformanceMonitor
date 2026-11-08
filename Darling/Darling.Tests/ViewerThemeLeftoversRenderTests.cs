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
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8221: the Viewer's twin of Lite's ThemeLeftoversRenderTests. A header or grid style written with <c>BasedOn="{StaticResource {x:Type ...}}"</c>
/// is looked up once when the tab loads, so it keeps the theme the tab loaded under; and a calm fleet card's border was a Dark hex
/// shown in every theme. Every check builds the real control under the Dark theme, switches the theme at run time the way Settings does
/// (<see cref="ThemeManager.Apply"/>), and reads the color from the element that rendered.
/// </summary>
public sealed class ViewerThemeLeftoversRenderTests
{
    private static T Themed<T>(Func<Application, T> body) =>
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var before = app.Resources.MergedDictionaries.ToList();
            var previous = ThemeManager.CurrentTheme;
            ThemeManager.ThemeUriProvider = name => new Uri($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{name}Theme.xaml");
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

    /// <summary>The Background the current theme's implicit style for <paramref name="type"/> sets.</summary>
    private static Color StyleBackground(Application app, Type type) =>
        ColorOf(((Setter)((Style)app.FindResource(type)).Setters.OfType<Setter>().First(s => s.Property == Control.BackgroundProperty)).Value as Brush);

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

    // ── FinOps > Recommendations ───────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheRecommendationsHeaders_FollowTheThemeAfterARunTimeSwitch(string theme)
    {
        var (backgrounds, expected, dark) = Themed(app =>
        {
            var darkHeader = Resource(app, "ViewerGridHeaderBrush");   // the grid's own header style (DarkGridHeader) reads it
            var tab = new FinOpsTab();
            var window = new Window { Content = tab, Width = 1280, Height = 800 };
            window.Show();
            try
            {
                var grid = (DataGrid)tab.FindName("FinOpsRecommendationsDataGrid");
                StaTestThread.Reveal(grid);
                ClippedTextProbe.Settle(window);
                ThemeManager.Apply(theme);
                ClippedTextProbe.Settle(window);
                var headers = Visuals<DataGridColumnHeader>(grid).Where(h => h.Column is not null).OrderBy(h => h.Column.DisplayIndex).ToList();
                return (headers.Select(h => ColorOf(h.Background)).ToList(), Resource(app, "ViewerGridHeaderBrush"), darkHeader);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.NotEqual(dark, expected);
        Assert.True(backgrounds.Count >= 6, $"found {backgrounds.Count} headers");
        Assert.All(backgrounds, background => Assert.Equal(expected, background));
    }

    /// <summary>The Detail and Est. Savings headers carry a tooltip on the whole header (the filter button and the title), as the cells do. Replaces a text pin on the XAML.</summary>
    [Fact]
    public void TheRecommendationsDetailAndSavingsHeaders_HaveATooltipAndAFilterButton()
    {
        var (tips, buttons) = Themed(app =>
        {
            var tab = new FinOpsTab();
            var window = new Window { Content = tab, Width = 1280, Height = 800 };
            window.Show();
            try
            {
                var grid = (DataGrid)tab.FindName("FinOpsRecommendationsDataGrid");
                StaTestThread.Reveal(grid);
                ClippedTextProbe.Settle(window);
                var tipped = grid.Columns.Where(c => c.Header is StackPanel p && p.Children.OfType<TextBlock>().Any(t => t.Text is "Detail" or "Est. Savings ($/mo)"))
                    .Select(c => ((StackPanel)c.Header).ToolTip as string).ToList();
                var filterButtons = Enumerable.Range(0, grid.Columns.Count)
                    .Count(i => Visuals<Button>(Visuals<DataGridColumnHeader>(grid).First(h => h.Column == grid.Columns[i])).Any(b => b.Tag is string));
                return (tipped, filterButtons == grid.Columns.Count);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Equal(2, tips.Count);
        Assert.All(tips, tip => Assert.True(tip is { Length: >= 20 }, "a findings header has no tooltip"));
        Assert.True(buttons, "a Recommendations column has no filter button");
    }

    // ── Availability Groups ────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void ThePerDatabaseGrid_FollowsTheThemeAfterARunTimeSwitch_AndHidesWhenTheGroupHasNone(string theme)
    {
        var (visible, hidden, expected, dark) = Themed(app =>
        {
            var darkGrid = StyleBackground(app, typeof(DataGrid));
            var tab = new AvailabilityGroupsTab();
            var window = new Window { Content = tab, Width = 1280, Height = 800 };
            window.Show();
            try
            {
                tab.Render(Topology());
                ClippedTextProbe.Settle(window);
                ThemeManager.Apply(theme);
                ClippedTextProbe.Settle(window);
                var grids = Visuals<DataGrid>(tab).ToList();
                return (grids.Where(g => g.Visibility == Visibility.Visible).Select(g => ColorOf(g.Background)).ToList(),
                    grids.Count(g => g.Visibility == Visibility.Collapsed), StyleBackground(app, typeof(DataGrid)), darkGrid);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.NotEqual(dark, expected);
        Assert.Single(visible);
        Assert.Equal(expected, visible[0]);
        Assert.Equal(1, hidden);
    }

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

    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheSeverityTextOnAnAvailabilityGroupCard_IsReadable_AfterARunTimeSwitchToTheLightThemes(string theme)
    {
        var texts = Themed(app =>
        {
            var tab = new AvailabilityGroupsTab();
            var window = new Window { Content = tab, Width = 1280, Height = 800 };
            window.Show();
            try
            {
                tab.Render(Topology());
                ClippedTextProbe.Settle(window);
                ThemeManager.Apply(theme);
                ClippedTextProbe.Settle(window);
                var found = Visuals<TextBlock>(tab)
                    .Where(t => t.Text is "Healthy" or "SYNCHRONIZED")
                    .SelectMany(t => SurfacesBehind(t).Select(s => (t.Text, s.Surface, Ratio: WcagContrast.Ratio(ColorOf(t.Foreground), s.Fill))))
                    .ToList();
                return found;
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

    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheMetricValuesOnAFleetCard_AreReadable_AfterARunTimeSwitchToTheLightThemes(string theme)
    {
        var (ratios, card) = Themed(app =>
        {
            var item = Card();
            ThemeManager.Apply(theme);
            var cardFill = Resource(app, "BackgroundLightBrush");
            var brushes = new[] { item.CpuSeverityBrush, item.MemorySeverityBrush, item.BlockingSeverityBrush, item.DeadlockSeverityBrush, item.ThreadsSeverityBrush, item.CollectorSeverityBrush };
            return (brushes.Select(b => WcagContrast.Ratio(ColorOf(b), cardFill)).ToList(), cardFill);
        });

        Assert.Equal(6, ratios.Count);
        Assert.All(ratios, r => Assert.True(r >= 4.5, $"a metric value is {r:0.00}:1 on the {card} card in {theme}"));
    }

    /// <summary>One group with a database and one with none, so the grid shows for the first and is hidden for the second.</summary>
    private static List<AgTopologyCard> Topology()
    {
        var replicas = new List<AgTopologyReplicaRow>();
        var databases = new List<AgTopologyDatabaseRow>();
        for (var ag = 0; ag < 2; ag++)
        {
            var agName = $"AG{ag}";
            replicas.Add(new AgTopologyReplicaRow
            {
                ServerId = ag, ServerName = $"NODE{ag}", CollectionTime = new DateTime(2026, 9, 25, 12, 0, 0), AgName = agName, ReplicaServerName = $"NODE{ag}_0",
                RoleDesc = "PRIMARY", OperationalStateDesc = "ONLINE", ConnectedStateDesc = "CONNECTED", RecoveryHealthDesc = "ONLINE",
                SynchronizationHealthDesc = "HEALTHY", AvailabilityModeDesc = "SYNCHRONOUS_COMMIT", FailoverModeDesc = "AUTOMATIC",
            });
            if (ag == 0)
            {
                databases.Add(new AgTopologyDatabaseRow
                {
                    ServerId = ag, ServerName = $"NODE{ag}", CollectionTime = new DateTime(2026, 9, 25, 12, 0, 0), AgName = agName, DatabaseName = "DB0",
                    ReplicaServerName = $"NODE{ag}_0", SynchronizationStateDesc = "SYNCHRONIZED", AvailabilityModeDesc = "SYNCHRONOUS_COMMIT", IsSuspended = false,
                    SecondaryLagSeconds = 0, LogSendQueueKb = 0, RedoQueueKb = 0, LogSendRateKbPerSec = 0, RedoRateKbPerSec = 0,
                });
            }
        }

        return AgTopology.BuildCards(replicas, databases);
    }

    // ── Overview fleet card ────────────────────────────────────────────────────────────────────

    private static readonly DateTime Now = new(2026, 7, 3, 12, 0, 0, DateTimeKind.Utc);

    private static ServerSummaryItem Card(Action<ServerSummaryItem>? shape = null)
    {
        var item = new ServerSummaryItem { LastCollectionTime = Now };
        shape?.Invoke(item);
        item.ApplyFreshness(Now);
        return item;
    }

    /// <summary>The Border a fleet card draws, bound the way the Overview binds it, read after the theme was switched at run time.</summary>
    private static Color BorderUnder(string theme, ServerSummaryItem item) =>
        Themed(app =>
        {
            ThemeManager.Apply(theme);
            var border = new Border { BorderThickness = new Thickness(2), Width = 100, Height = 100, DataContext = item };
            border.SetBinding(Border.BorderBrushProperty, nameof(ServerSummaryItem.CardBorderBrush));
            var window = new Window { Content = border, Width = 200, Height = 200 };
            window.Show();
            try
            {
                ClippedTextProbe.Settle(window);
                return ColorOf(border.BorderBrush);
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
    public void ACalmFleetCard_HasTheThemesNeutralBorder_InEveryTheme(string theme)
    {
        var neutral = Themed(app => { ThemeManager.Apply(theme); return Resource(app, "CardBorderBrush"); });

        Assert.Equal(neutral, BorderUnder(theme, Card()));
    }

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void AWarningAndACriticalFleetCard_KeepTheirBandBorder_InEveryTheme(string theme)
    {
        var warning = BorderUnder(theme, Card(c => c.FailedCollectorCount = 1));
        var critical = BorderUnder(theme, Card(c => { c.DeadlockCount = 30; c.DeadlockWindow = TimeSpan.FromHours(1); }));

        /* Dark keeps its pale band colors. Light and Cool Breeze take the theme's own status tones (#8221), as Lite's cards do. */
        if (theme == "Dark")
        {
            Assert.Equal(Color.FromRgb(0xFF, 0xD5, 0x4F), warning);
            Assert.Equal(Color.FromRgb(0xE5, 0x73, 0x73), critical);
        }

        var neutral = BorderUnder(theme, Card());
        Assert.NotEqual(neutral, warning);
        Assert.NotEqual(neutral, critical);
        Assert.NotEqual(warning, critical);
    }

    // ── Buttons, text boxes and bar tracks (the Viewer twin of Lite's) ────────────────────────────

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
                ClippedTextProbe.Settle(window);
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
                var content = (FrameworkElement)tab.FindName("FinOpsUtilizationContent");
                content.Visibility = Visibility.Visible;
                StaTestThread.Reveal(content);
                ThemeManager.Apply(theme);
                ClippedTextProbe.Settle(window);
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

    // ── The server tab's tab rows after a run-time switch ──────────────────────────────────────

    [Fact]
    public void TheSelectedTabsOfTheServerTabRows_TakeTheCurrentThemesInkAndUnderline_AfterRunTimeSwitchesDarkLightCoolBreeze()
    {
        var seen = Themed(app =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                new DarlingServer(1, "theme-test", "theme-test", true, null));
            typeof(ViewerServerTab).GetField("_suppressDrillDownAutoRefresh", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.SetValue(tab, true);
            var window = new Window { Content = tab, Width = 1280, Height = 800, Left = 0, Top = 0 };
            window.Show();
            try
            {
                var inner = (TabControl)tab.FindName("InnerTabs");
                var queries = (TabControl)tab.FindName("QueriesSubTabControl");
                var found = new List<(string Theme, string Row, Color Ink, Color WantInk, Color Underline, Color WantUnderline)>();
                foreach (var theme in new[] { "Dark", "Light", "CoolBreeze" })
                {
                    ThemeManager.Apply(theme);
                    StaTestThread.WithDispatcherContext(() => inner.SelectedItem = inner.Items.OfType<TabItem>().First(t => Equals(t.Header, "Queries")));
                    ClippedTextProbe.Settle(window);
                    foreach (var (row, tabs) in new[] { ("inner", inner), ("queries", queries) })
                    {
                        var item = (TabItem)tabs.SelectedItem;
                        var chrome = (Border)item.Template.FindName("border", item);
                        found.Add((theme, row + " > " + item.Header, ColorOf(item.Foreground), Resource(app, "ForegroundBrush"),
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

        Assert.Equal(6, seen.Count);
        foreach (var r in seen)
        {
            Assert.True(r.Ink == r.WantInk, $"{r.Theme} {r.Row}: the selected tab's text is {r.Ink}, the theme says {r.WantInk}");
            Assert.True(r.Underline == r.WantUnderline, $"{r.Theme} {r.Row}: the selected tab's underline is {r.Underline}, the theme says {r.WantUnderline}");
        }
    }
}
