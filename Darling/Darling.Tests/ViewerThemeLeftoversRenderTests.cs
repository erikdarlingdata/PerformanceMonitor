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
                Reveal(grid);
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
                var band = new[] { "SuccessBrush", "WarningBrush", "ErrorBrush" }.Select(k => Resource(app, k))
                    .Concat(new[] { Color.FromRgb(0x81, 0xC7, 0x84), Color.FromRgb(0xFF, 0xD5, 0x4F), Color.FromRgb(0xE5, 0x73, 0x73) }).ToList();
                var found = Visuals<TextBlock>(tab)
                    .Where(t => t.Text.Length > 0 && band.Contains(ColorOf(t.Foreground)))
                    .Select(t => (t.Text, Ratio: WcagContrast.Ratio(ColorOf(t.Foreground), BackgroundBehind(t))))
                    .ToList();
                return found;
            }
            finally
            {
                window.Close();
            }
        });

        /* The group's "Healthy" badge, the sync state "SYNCHRONIZED" and the data movement text all take a severity color. */
        Assert.True(texts.Count >= 3, $"found {texts.Count} severity texts in {theme}");
        foreach (var (text, ratio) in texts)
        {
            Assert.True(ratio >= 4.5, $"'{text}' is {ratio:0.00}:1 on its card in {theme}");
        }
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

        Assert.Equal(Color.FromRgb(0xFF, 0xD5, 0x4F), warning);
        Assert.Equal(Color.FromRgb(0xE5, 0x73, 0x73), critical);
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
                    inner.SelectedItem = inner.Items.OfType<TabItem>().First(t => Equals(t.Header, "Queries"));
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
