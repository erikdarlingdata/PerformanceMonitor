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
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8222: round 2 of the tab refresh. The top tab row (the main window's server tab control, and the Settings window's tabs, which
/// use the same theme style) is flat with an underline on the selected tab and no fill, scrolls whole tabs behind a "»" menu,
/// every sub-tab row starts at one left offset, and the stock dotted focus box is replaced by a themed keyboard cue. Controls are
/// built on the shared STA thread, themed, shown at the given size and read from what rendered.
/// </summary>
[Collection("credential-manager")]
public sealed class FlatTabsFocusTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const string OverflowButtonName = "PART_TabOverflowButton";
    private const string ScrollName = "PART_TabStripScroll";

    private static readonly string[] Servers =
    {
        "SQL2016-PROD-EAST", "SQL2017-PROD-WEST", "SQL2019-REPORTING", "SQL2022-WAREHOUSE", "SQL2025-STAGING", "AZURE-MI-FINANCE",
    };

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void EverySelectedTab_ShowsTheUnderlineAndNoFill_OnTheTopRowTheSubRowsAndTheSubSubRows(string theme)
    {
        StaTestThread.Run(() => WithAppTheme(theme, () =>
        {
            var dict = LoadTheme(theme);
            var accent = ((SolidColorBrush)dict["AccentBrush"]).Color;
            var background = ((SolidColorBrush)dict["BackgroundBrush"]).Color;
            var rows = new (string What, TabControl Tabs)[]
            {
                ("top row", BuildTopRow(Servers.Take(2).ToArray())),
                ("sub-tab row", BuildRow("SubTabItemStyle", dict)),
                ("sub-sub-tab row", BuildRow("SubSubTabItemStyle", dict)),
            };

            foreach (var (what, tabs) in rows)
            {
                var window = Show(tabs, 600, 160, dict, background);
                try
                {
                    var selected = (TabItem)tabs.SelectedItem;
                    var other = tabs.Items.OfType<TabItem>().First(t => !t.IsSelected);
                    var chrome = (Border)selected.Template.FindName("border", selected);
                    Assert.True(IsClear(chrome.Background), $"[{theme} {what}] the selected tab '{Text(selected)}' has a fill ({chrome.Background})");
                    Assert.True(chrome.BorderThickness.Bottom >= 1, $"[{theme} {what}] the selected tab has no underline");
                    Assert.Equal(accent, ((SolidColorBrush)chrome.BorderBrush).Color);
                    var otherChrome = (Border)other.Template.FindName("border", other);
                    Assert.Equal(0, otherChrome.BorderThickness.Bottom);

                    /* Rendered pixels: the underline row of the selected tab is the accent colour, and its body is the window background. */
                    var bounds = selected.TransformToAncestor(window).TransformBounds(new Rect(selected.RenderSize));
                    var bitmap = Render(window, Path.Combine("tabs", $"{theme}-{what.Replace(' ', '-')}"));
                    var underline = Pixel(bitmap, bounds.Left + bounds.Width / 2, bounds.Bottom - 1);
                    /* A 1 px underline (the sub-sub row) lands between device pixels and blends; a 2 px one is the accent itself. */
                    Assert.True(chrome.BorderThickness.Bottom < 2 ? underline != background : underline == accent,
                        $"[{theme} {what}] the pixel at the bottom of the selected tab is {underline} (accent {accent}, background {background})");
                    var body = Pixel(bitmap, bounds.Left + 3, bounds.Top + bounds.Height / 2);
                    Assert.True(body == background, $"[{theme} {what}] the selected tab's body pixel is {body}, not the window background {background}");
                }
                finally
                {
                    window.Close();
                }
            }
        }));
    }

    [Theory]
    [InlineData("Dark", 900, 600)]
    [InlineData("Light", 900, 600)]
    [InlineData("CoolBreeze", 900, 600)]
    [InlineData("Dark", 1280, 800)]
    [InlineData("Light", 1280, 800)]
    [InlineData("CoolBreeze", 1280, 800)]
    public void TheTopTabRow_ScrollsWholeTabs_AndTheOverflowButtonListsTheTabsThatDoNotFit(string theme, int width, int height)
    {
        StaTestThread.Run(() => WithAppTheme(theme, () =>
        {
            var dict = LoadTheme(theme);
            var servers = width >= 1280 ? Servers.Concat(Servers.Select(s => s + "-2")).ToArray() : Servers;
            var tabs = BuildTopRow(servers);
            var window = Show(tabs, width, height, dict, null);
            try
            {
                StaTestThread.Settle(window);
                var scroll = (ScrollViewer)tabs.Template.FindName(ScrollName, tabs);
                var button = (Button)tabs.Template.FindName(OverflowButtonName, tabs);
                var visible = tabs.Items.OfType<TabItem>().Where(t => t.Visibility == Visibility.Visible).ToList();
                var problems = new List<string>();

                Assert.True(scroll.ExtentWidth > tabs.ActualWidth, $"the test row needs more tabs than fit in {tabs.ActualWidth:F0} px (they take {scroll.ExtentWidth:F0})");
                if (button.Visibility != Visibility.Visible)
                {
                    problems.Add("the top row overflows but has no » button");
                }

                foreach (var tab in visible)
                {
                    tabs.SelectedItem = tab;
                    StaTestThread.WaitForIdle(window.Dispatcher, TimeSpan.FromSeconds(10));
                    problems.AddRange(PartialTabs(tabs, scroll, $"{theme} {width}x{height} selecting '{Text(tab)}'"));
                    if (!Fits(tab, scroll))
                    {
                        problems.Add($"{theme} {width}x{height}: the selected tab '{Text(tab)}' is not fully inside the row");
                    }
                }

                tabs.SelectedItem = visible[0];
                StaTestThread.WaitForIdle(window.Dispatcher, TimeSpan.FromSeconds(10));
                var shown = visible.Where(t => Fits(t, scroll)).Select(Text).ToList();
                var expected = visible.Where(t => !Fits(t, scroll)).Select(Text).ToList();
                button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
                var menu = button.ContextMenu;
                var listed = menu is null ? new List<string>() : menu.Items.OfType<MenuItem>().Select(i => (string)i.Header).ToList();
                if (!listed.SequenceEqual(expected))
                {
                    problems.Add($"{theme} {width}x{height}: the menu lists [{string.Join(", ", listed)}] but [{string.Join(", ", expected)}] do not fit (shown: {string.Join(", ", shown)})");
                }

                if (menu is not null)
                {
                    menu.IsOpen = false;
                }

                /* The wheel moves the row by whole tabs, too. */
                for (var i = 0; i < 3; i++)
                {
                    scroll.RaiseEvent(new MouseWheelEventArgs(Mouse.PrimaryDevice, Environment.TickCount, -120) { RoutedEvent = UIElement.PreviewMouseWheelEvent });
                    StaTestThread.WaitForIdle(window.Dispatcher, TimeSpan.FromSeconds(10));
                    problems.AddRange(PartialTabs(tabs, scroll, $"{theme} {width}x{height} after wheel {i + 1}"));
                }

                Render(window, Path.Combine("toprow", $"{theme}-{width}x{height}"));
                Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems.Distinct()));
            }
            finally
            {
                window.Close();
            }
        }));
    }

    [Theory]
    [InlineData("Dark", 900, 600)]
    [InlineData("Light", 900, 600)]
    [InlineData("CoolBreeze", 900, 600)]
    [InlineData("Dark", 1280, 800)]
    [InlineData("Light", 1280, 800)]
    [InlineData("CoolBreeze", 1280, 800)]
    public void TheServerTabRows_NeverCutATabAtAnEdge_AndEverySubTabRowStartsAtOneLeftOffset(string theme, int width, int height)
    {
        var dir = Path.Combine(Path.GetTempPath(), "FlatTabs_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var problems = StaTestThread.Run(() => WithAppTheme(theme, () =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "flat-tabs-test", DisplayName = "flat-tabs-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = width, Height = height, Left = 0, Top = 0 };
                window.Resources.MergedDictionaries.Add(LoadTheme(theme));
                window.Show();
                try
                {
                    StaTestThread.Settle(window);
                    var found = new List<string>();
                    var main = (TabControl)tab.FindName("MainTabControl");
                    var scroll = (ScrollViewer)main.Template.FindName(ScrollName, main);
                    var subStyle = window.FindResource("SubTabItemStyle");
                    var offsets = new List<(string Tab, double X)>();
                    foreach (var item in main.Items.OfType<TabItem>().Where(t => t.Visibility == Visibility.Visible))
                    {
                        main.SelectedItem = item;
                        StaTestThread.WaitForIdle(window.Dispatcher, TimeSpan.FromSeconds(10));
                        found.AddRange(PartialTabs(main, scroll, $"server tabs, selecting '{item.Header}'"));
                        /* The selected tab's content is shown by the strip's own content host, not under the TabItem. */
                        var sub = Descendants<TabControl>(main)
                            .FirstOrDefault(t => ReferenceEquals(t.ItemContainerStyle, subStyle) && t.IsVisible && t.ActualWidth > 0);
                        if (sub is null)
                        {
                            continue;
                        }

                        var subScroll = (ScrollViewer)sub.Template.FindName(ScrollName, sub);
                        found.AddRange(PartialTabs(sub, subScroll, $"'{item.Header}' sub-tabs"));
                        var first = sub.Items.OfType<TabItem>().First(t => t.Visibility == Visibility.Visible);
                        offsets.Add((item.Header.ToString()!, first.TransformToAncestor(window).Transform(new Point(0, 0)).X));
                    }

                    if (offsets.Count < 6)
                    {
                        found.Add($"only {offsets.Count} server tabs have a sub-tab row to compare");
                    }
                    else if (offsets.Max(o => o.X) - offsets.Min(o => o.X) > 0.5)
                    {
                        found.Add("the sub-tab rows start at different offsets: " + string.Join(", ", offsets.Select(o => $"{o.Tab} {o.X:F1}")));
                    }

                    main.SelectedItem = main.Items.OfType<TabItem>().First(t => Equals(t.Header, "Queries"));
                    StaTestThread.Settle(window);
                    Render(window, Path.Combine("server", $"{theme}-{width}x{height}-queries"));
                    main.SelectedItem = main.Items.OfType<TabItem>().First(t => Equals(t.Header, "System Events"));
                    StaTestThread.Settle(window);
                    Render(window, Path.Combine("server", $"{theme}-{width}x{height}-systemevents"));
                    return found;
                }
                finally
                {
                    window.Close();
                }
            }));

            Assert.True(problems.Count == 0, $"{theme} at {width}x{height}:{Environment.NewLine}" + string.Join(Environment.NewLine, problems.Distinct().Take(20)));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheOverflowMenu_HasNoEmptyIconColumn_AndMarksTheCurrentTab(string theme)
    {
        StaTestThread.Run(() => WithAppTheme(theme, () =>
        {
            var dict = LoadTheme(theme);
            var tabs = BuildTopRow(Servers);
            var window = Show(tabs, 700, 300, dict, null);
            try
            {
                StaTestThread.Settle(window);
                var scroll = (ScrollViewer)tabs.Template.FindName(ScrollName, tabs);
                var button = (Button)tabs.Template.FindName(OverflowButtonName, tabs);
                Assert.Equal(Visibility.Visible, button.Visibility);

                /* Scroll the wheel until the selected tab (the first) is a hidden one, so the menu has to mark it. */
                for (var i = 0; i < 12 && Fits((TabItem)tabs.SelectedItem, scroll); i++)
                {
                    scroll.RaiseEvent(new MouseWheelEventArgs(Mouse.PrimaryDevice, Environment.TickCount, -120) { RoutedEvent = UIElement.PreviewMouseWheelEvent });
                    StaTestThread.WaitForIdle(window.Dispatcher, TimeSpan.FromSeconds(10));
                }

                button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
                var menu = button.ContextMenu!;
                StaTestThread.WaitForIdle(window.Dispatcher, TimeSpan.FromSeconds(10));
                var items = menu.Items.OfType<MenuItem>().ToList();
                Assert.NotEmpty(items);
                Render(menu, Path.Combine("menus", $"{theme}-overflow"));
                var current = items.Single(i => i.IsChecked);
                Assert.Equal(Text((TabItem)tabs.SelectedItem), (string)current.Header);

                var wide = items.Max(i => i.ActualWidth);
                var text = items.Select(i => TextWidth(i)).Max();
                Assert.True(wide - text < 40, $"[{theme}] the menu is {wide:F0} px wide for text {text:F0} px wide: an empty icon column is left");
                var mark = Descendants<FrameworkElement>(current).FirstOrDefault(e => e.Name == "CheckMark");
                Assert.True(mark is { IsVisible: true }, $"[{theme}] the current tab's menu item shows no check mark");
                var other = items.First(i => !i.IsChecked);
                var otherMark = Descendants<FrameworkElement>(other).FirstOrDefault(e => e.Name == "CheckMark");
                Assert.True(otherMark is null || !otherMark.IsVisible, $"[{theme}] another tab's menu item shows the check mark");
                menu.IsOpen = false;
            }
            finally
            {
                window.Close();
            }
        }));
    }

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheSidebarMoreMenu_OpensBesideItsButton_WithNoEmptyIconColumn(string theme)
    {
        StaTestThread.Run(() => WithAppTheme(theme, () =>
        {
            var footer = new SidebarFooter { Width = 260, VerticalAlignment = VerticalAlignment.Bottom, HorizontalAlignment = HorizontalAlignment.Left };
            var window = new Window { Content = footer, Width = 900, Height = 600, Left = 0, Top = 0 };
            window.Resources.MergedDictionaries.Add(LoadTheme(theme));
            window.Show();
            try
            {
                StaTestThread.Settle(window);
                var more = (Button)footer.FindName("MoreButton");
                var menu = (ContextMenu)footer.FindName("MoreMenu");
                more.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
                StaTestThread.WaitForIdle(window.Dispatcher, TimeSpan.FromSeconds(10));
                Assert.True(menu.IsOpen);
                Render(menu, Path.Combine("menus", $"{theme}-more"));

                var button = new Rect(more.PointToScreen(new Point(0, 0)), more.RenderSize);
                var shown = new Rect(menu.PointToScreen(new Point(0, 0)), menu.RenderSize);
                Assert.True(shown.Left >= button.Right - 2, $"[{theme}] the menu opens at x {shown.Left:F0}, not beside the button (its right edge is {button.Right:F0})");
                Assert.True(shown.Top < button.Bottom + 2 && shown.Bottom > button.Top - 2, $"[{theme}] the menu ({shown.Top:F0}..{shown.Bottom:F0}) is not level with the button ({button.Top:F0}..{button.Bottom:F0})");

                var items = menu.Items.OfType<MenuItem>().ToList();
                var wide = items.Max(i => i.ActualWidth);
                var text = items.Max(TextWidth);
                Assert.True(wide - text < 40, $"[{theme}] the More menu is {wide:F0} px wide for text {text:F0} px wide: an empty icon column is left");
                menu.IsOpen = false;
            }
            finally
            {
                window.Close();
            }
        }));
    }

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TabItems_UseTheThemedKeyboardFocusCue_NotTheStockDottedBox(string theme)
    {
        StaTestThread.Run(() => WithAppTheme(theme, () =>
        {
            var dict = LoadTheme(theme);
            var cue = (Style?)dict["KeyboardFocusVisualStyle"];
            Assert.NotNull(cue);
            var stock = FrameworkElement.FocusVisualStyleProperty.DefaultMetadata.DefaultValue;

            foreach (var styleKey in new string?[] { null, "SubTabItemStyle", "SubSubTabItemStyle" })
            {
                var tab = new TabItem { Header = "x", Content = new Grid() };
                if (styleKey is not null)
                {
                    tab.Style = (Style)dict[styleKey];
                }

                var tabs = new TabControl();
                tabs.Items.Add(tab);
                var window = Show(tabs, 300, 120, dict, null);
                try
                {
                    Assert.True(ReferenceEquals(tab.FocusVisualStyle, cue), $"[{theme} {styleKey ?? "TabItem"}] the tab's FocusVisualStyle is not the themed cue");
                    Assert.False(ReferenceEquals(tab.FocusVisualStyle, stock));
                }
                finally
                {
                    window.Close();
                }
            }

            /* The cue is a thin solid outline in the accent colour, not a dashed one. */
            var rectangle = (System.Windows.Shapes.Rectangle)((ControlTemplate)cue!.Setters.OfType<Setter>().Single(s => s.Property == Control.TemplateProperty).Value).LoadContent();
            Assert.Equal(1, rectangle.StrokeThickness);
            Assert.True(rectangle.StrokeDashArray is null || rectangle.StrokeDashArray.Count == 0, "the cue is dashed");
        }));
    }

    [Fact]
    public void TheAppLevelFocusDefault_IsTheThemedCue_ForControlsWithoutTheirOwnFocusStyle()
    {
        StaTestThread.Run(() => WithAppTheme("Dark", () =>
        {
            var install = typeof(PerformanceMonitorLite.App).GetMethod("InstallKeyboardFocusCue", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
            Assert.NotNull(install);
            install!.Invoke(null, null);
            var cue = Application.Current.FindResource("KeyboardFocusVisualStyle");
            foreach (var control in new Control[] { new Button(), new ComboBox(), new CheckBox(), new TabItem(), new UserControl() })
            {
                Assert.True(ReferenceEquals(control.FocusVisualStyle, cue), $"{control.GetType().Name} does not default to the themed focus cue");
            }
        }));
    }

    // ---------------------------------------------------------------------------------------------------------------- helpers

    /// <summary>The main window's server tab control: the default tab style, a transparent background, a string header for the fixed tabs and a StackPanel header per server.</summary>
    private static TabControl BuildTopRow(string[] servers)
    {
        var tabs = new TabControl { Background = Brushes.Transparent, BorderThickness = new Thickness(0) };
        foreach (var header in new[] { "Overview", "Alert History", "Job History", "FinOps", "Recommendations" })
        {
            tabs.Items.Add(new TabItem { Header = header, Content = new Grid() });
        }

        foreach (var name in servers)
        {
            var panel = new StackPanel { Orientation = Orientation.Horizontal };
            panel.Resources.Add(typeof(TextBlock), new Style(typeof(TextBlock)));
            panel.Children.Add(new TextBlock { Text = name, VerticalAlignment = VerticalAlignment.Center, Margin = new Thickness(0, 0, 4, 0) });
            tabs.Items.Add(new TabItem { Header = panel, Content = new Grid() });
        }

        tabs.SelectedIndex = 0;
        return tabs;
    }

    private static TabControl BuildRow(string itemStyleKey, ResourceDictionary dict)
    {
        var tabs = new TabControl { ItemContainerStyle = (Style)dict[itemStyleKey] };
        tabs.Items.Add(new TabItem { Header = "One", Content = new Grid() });
        tabs.Items.Add(new TabItem { Header = "Two", Content = new Grid() });
        tabs.Items.Add(new TabItem { Header = "Three", Content = new Grid() });
        return tabs;
    }

    private static Window Show(TabControl tabs, double width, double height, ResourceDictionary theme, Color? background)
    {
        var host = new Border { Child = tabs };
        if (background is { } color)
        {
            host.Background = new SolidColorBrush(color);
        }
        else
        {
            host.SetResourceReference(Border.BackgroundProperty, "BackgroundBrush");
        }

        var window = new Window { Content = host, Width = width, Height = height, Left = 0, Top = 0, WindowStyle = WindowStyle.None };
        window.Resources.MergedDictionaries.Add(theme);
        window.Show();
        StaTestThread.Settle(window);
        return window;
    }

    /// <summary>The tabs that are neither wholly inside nor wholly outside the row's viewport: a name cut at an edge.</summary>
    private static IEnumerable<string> PartialTabs(TabControl tabs, ScrollViewer scroll, string label)
    {
        foreach (var tab in tabs.Items.OfType<TabItem>().Where(t => t.Visibility == Visibility.Visible))
        {
            var b = tab.TransformToAncestor(scroll).TransformBounds(new Rect(tab.RenderSize));
            var cutLeft = b.Left < -0.5 && b.Right > 0.5;
            var cutRight = b.Left < scroll.ViewportWidth - 0.5 && b.Right > scroll.ViewportWidth + 0.5;
            if (cutLeft || cutRight)
            {
                yield return $"[{label}] '{Text(tab)}' is cut at the {(cutLeft ? "left" : "right")} edge ({b.Left:F1}..{b.Right:F1} of {scroll.ViewportWidth:F1})";
            }
        }
    }

    private static bool Fits(TabItem tab, ScrollViewer scroll)
    {
        var bounds = tab.TransformToAncestor(scroll).TransformBounds(new Rect(tab.RenderSize));
        return bounds.Left >= -0.5 && bounds.Right <= scroll.ViewportWidth + 0.5;
    }

    private static string Text(TabItem tab) => TabStripOverflow.HeaderText(tab);

    private static double TextWidth(MenuItem item) =>
        Descendants<TextBlock>(item).Where(t => !string.IsNullOrEmpty(t.Text)).Select(t => t.ActualWidth).DefaultIfEmpty(0).Max();

    private static bool IsClear(Brush? brush) => brush is null || (brush is SolidColorBrush { Color.A: 0 });

    private static IEnumerable<T> Descendants<T>(DependencyObject root) where T : DependencyObject
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            if (child is T match)
            {
                yield return match;
            }

            foreach (var inner in Descendants<T>(child))
            {
                yield return inner;
            }
        }
    }

    private static BitmapSource Render(Visual visual, string name)
    {
        var element = (FrameworkElement)visual;
        var bitmap = new RenderTargetBitmap((int)Math.Ceiling(element.ActualWidth), (int)Math.Ceiling(element.ActualHeight), 96, 96, PixelFormats.Pbgra32);
        bitmap.Render(visual);
        var dir = Environment.GetEnvironmentVariable("LITE_TEST_PNG_DIR");
        if (!string.IsNullOrEmpty(dir))
        {
            var path = Path.Combine(dir, name + ".png");
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            var encoder = new PngBitmapEncoder();
            encoder.Frames.Add(BitmapFrame.Create(bitmap));
            using var stream = File.Create(path);
            encoder.Save(stream);
        }

        return bitmap;
    }

    private static Color Pixel(BitmapSource bitmap, double x, double y)
    {
        var pixels = new byte[4];
        new CroppedBitmap(bitmap, new Int32Rect((int)Math.Clamp(x, 0, bitmap.PixelWidth - 1), (int)Math.Clamp(y, 0, bitmap.PixelHeight - 1), 1, 1)).CopyPixels(pixels, 4, 0);
        return Color.FromArgb(pixels[3], pixels[2], pixels[1], pixels[0]);
    }

    private static ResourceDictionary LoadTheme(string theme) =>
        new() { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}Theme.xaml") };

    /// <summary>Runs <paramref name="body"/> with the theme under test as the application's own, so a control resolves its brushes and the app-level text style from it as the running app does.</summary>
    private static T WithAppTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = LoadTheme(theme);
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            return body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }

    private static void WithAppTheme(string theme, Action body) => WithAppTheme(theme, () =>
    {
        body();
        return 0;
    });
}
