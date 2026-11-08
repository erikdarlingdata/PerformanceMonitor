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
using Shape = System.Windows.Shapes.Shape;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8305, the Viewer's side: the server tab's time-range picker at 900x600 opens its popup inside the window, its calendar draws
/// every week row and the weekday header, and its "Data starts" note names the server's data start, not the start of the range
/// on screen. A menu separator spans the menu in the theme's brush. Controls are built on the shared STA thread, themed as the
/// app themes them, shown at 900x600 on a monitor (a popup is placed against the screen) and read from what rendered.
/// </summary>
public sealed class ViewerRangePickerAndMenuAt900Tests
{
    /// <summary>A Sunday early in a month, so a calendar that hides the days after today shows only two week rows of six.</summary>
    private static readonly DateTime Today = new(2026, 11, 8, 15, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheRangePickerPopup_OpensInsideTheWindow_AndItsCalendarDrawsSixWeekRowsAndTheWeekdayHeader(string theme)
    {
        var problems = StaTestThread.Run(() => WithAppTheme(theme, () =>
        {
            var tab = BuildTab("range-picker-test");
            var window = new Window { Content = tab, Width = 900, Height = 600, Left = 0, Top = 0 };
            StaTestThread.ShowOnAMonitor(window);
            try
            {
                StaTestThread.Settle(window);
                var picker = (TimeRangePicker)tab.FindName("RangePicker");
                picker.NowProvider = () => Today;
                var popup = (Popup)picker.FindName("PickerPopup");
                popup.IsOpen = true;
                StaTestThread.WaitForIdle(window.Dispatcher, TimeSpan.FromSeconds(10));
                Assert.True(popup.IsOpen, $"[{theme}] the picker popup did not open");

                var found = new List<string>();
                var client = ClientRect(window);
                var shown = ScreenRect((FrameworkElement)popup.Child);
                if (shown.Left < client.Left - 1 || shown.Right > client.Right + 1 || shown.Top < client.Top - 1 || shown.Bottom > client.Bottom + 1)
                {
                    found.Add($"the popup spans x {shown.Left:F0}..{shown.Right:F0}, y {shown.Top:F0}..{shown.Bottom:F0}, outside the window's x {client.Left:F0}..{client.Right:F0}, y {client.Top:F0}..{client.Bottom:F0}");
                }

                var calendar = (Calendar)picker.FindName("PickCalendar");
                var month = ClippedTextProbe.Descendants(calendar).OfType<Grid>().FirstOrDefault(g => g.Name == "PART_MonthView")
                    ?? throw new Xunit.Sdk.XunitException($"[{theme}] the calendar has no month view");
                var days = month.Children.OfType<CalendarDayButton>().ToList();
                var drawnRows = days.Where(d => d.Visibility == Visibility.Visible && d.ActualHeight > 0 && d.ActualWidth > 0).Select(Grid.GetRow).Distinct().Count();
                if (drawnRows != 6)
                {
                    found.Add($"the calendar draws {drawnRows} week rows of 6 (days: {days.Count})");
                }

                var titles = month.Children.Cast<UIElement>().Where(c => Grid.GetRow(c) == 0 && c is not CalendarDayButton).ToList();
                var titleTexts = titles.Select(TextOf).ToList();
                if (titles.Count != 7 || titleTexts.Any(string.IsNullOrWhiteSpace))
                {
                    found.Add($"the weekday header has {titles.Count} {string.Join("/", titles.Select(t => t.GetType().Name).Distinct())} cells reading [{string.Join(", ", titleTexts.Select(t => t ?? "(empty)"))}]");
                }

                /* The days after today are drawn, and cannot be picked. */
                var tomorrow = days.FirstOrDefault(d => d.DataContext is DateTime date && date.Date == Today.Date.AddDays(1));
                if (tomorrow is null || !tomorrow.IsBlackedOut || tomorrow.Visibility != Visibility.Visible)
                {
                    found.Add(tomorrow is null ? "the day after today is not on the calendar" : $"the day after today is drawn (visible: {tomorrow.Visibility}) but can be picked (blacked out: {tomorrow.IsBlackedOut})");
                }

                popup.IsOpen = false;
                return found;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(problems.Count == 0, $"[{theme}] picker at 900x600:{Environment.NewLine}" + string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheDataStartNote_NamesTheServersDataStart_NotTheStartOfTheRangeOnScreen()
    {
        var now = DateTime.UtcNow;
        var dataStart = now.AddDays(-3);
        var (text, visibility, covered, expected) = StaTestThread.Run(() => WithAppTheme("Dark", () =>
        {
            var tab = BuildTab("data-start-test");
            var window = new Window { Content = tab, Width = 900, Height = 600, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                StaTestThread.Settle(window);
                /* What the tab's data-start read found for its charts: the server's oldest row. The picker's note is fed from it. */
                tab.ServerDataStartUtc = dataStart;

                var picker = (TimeRangePicker)tab.FindName("RangePicker");
                /* Past 4 hours is on screen, and the reader types a week: the note must name where the data starts, three days back. */
                picker.Value = TimeRangePresets.Find("4h")!;
                var coveredVisibility = picker.NoteText.Visibility;
                picker.Value = TimeRangePresets.Find("1w")!;
                var range = picker.Resolve()!;
                return (picker.NoteText.Text, picker.NoteText.Visibility, coveredVisibility, TimeRangeNotes.DataStartNote(range, dataStart));
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.NotNull(expected);
        Assert.Equal(Visibility.Collapsed, covered);
        Assert.True(visibility == Visibility.Visible, $"a week's range over data three days old shows no data-start note (the note reads '{text}')");
        Assert.Equal(expected, text);
    }

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void AMenuSeparator_SpansTheMenu_InTheThemesBorderBrush(string theme)
    {
        var problems = StaTestThread.Run(() => WithAppTheme(theme, () =>
        {
            var dict = LoadTheme(theme);
            var menu = new ContextMenu();
            menu.Items.Add(new MenuItem { Header = "Toggle Favorite" });
            menu.Items.Add(new Separator());
            menu.Items.Add(new MenuItem { Header = "Edit Server" });
            menu.Items.Add(new Separator());
            menu.Items.Add(new MenuItem { Header = "Remove Server" });
            var button = new Button { Content = "Server", ContextMenu = menu, VerticalAlignment = VerticalAlignment.Top, HorizontalAlignment = HorizontalAlignment.Left };
            var window = new Window { Content = button, Width = 900, Height = 600, Left = 0, Top = 0 };
            window.Resources.MergedDictionaries.Add(dict);
            StaTestThread.ShowOnAMonitor(window);
            try
            {
                StaTestThread.Settle(window);
                menu.PlacementTarget = button;
                menu.IsOpen = true;
                StaTestThread.WaitForIdle(window.Dispatcher, TimeSpan.FromSeconds(10));
                Assert.True(menu.IsOpen, $"[{theme}] the menu did not open");

                var found = new List<string>();
                var items = menu.Items.OfType<MenuItem>().ToList();
                var itemBounds = items[0].TransformToAncestor(menu).TransformBounds(new Rect(items[0].RenderSize));
                var brush = ((SolidColorBrush)dict["BorderBrush"]).Color;
                foreach (var (separator, index) in menu.Items.OfType<Separator>().Select((s, i) => (s, i)))
                {
                    var (line, color) = DrawnLine(separator);
                    if (line is null)
                    {
                        found.Add($"separator {index} draws no line");
                        continue;
                    }

                    var bounds = line.TransformToAncestor(menu).TransformBounds(new Rect(line.RenderSize));
                    if (Math.Abs(bounds.Left - itemBounds.Left) > 1 || Math.Abs(bounds.Width - itemBounds.Width) > 1)
                    {
                        found.Add($"separator {index} spans x {bounds.Left:F0}..{bounds.Right:F0}; the items span {itemBounds.Left:F0}..{itemBounds.Right:F0}");
                    }

                    if (color != brush)
                    {
                        found.Add($"separator {index} is {color}, not the theme's border brush {brush}");
                    }
                }

                menu.IsOpen = false;
                return found;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(problems.Count == 0, $"[{theme}] menu separators:{Environment.NewLine}" + string.Join(Environment.NewLine, problems));
    }

    private static ViewerServerTab BuildTab(string name) =>
        new(new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"), new DarlingServer(1, name, name, true, null));

    /// <summary>The first filled Border or Shape under <paramref name="separator"/>: the line a separator draws, and its colour.</summary>
    private static (FrameworkElement? Line, Color Color) DrawnLine(Separator separator)
    {
        foreach (var node in ClippedTextProbe.Descendants(separator))
        {
            switch (node)
            {
                case Border { Background: SolidColorBrush fill } border when fill.Color.A > 0 && border.ActualHeight > 0:
                    return (border, fill.Color);
                case Shape { Fill: SolidColorBrush fill } shape when fill.Color.A > 0 && shape.ActualHeight > 0:
                    return (shape, fill.Color);
            }
        }

        return (null, Colors.Transparent);
    }

    private static string? TextOf(UIElement cell) =>
        cell is TextBlock own
            ? own.Text
            : ClippedTextProbe.Descendants(cell).OfType<TextBlock>().Select(t => t.Text).FirstOrDefault(t => !string.IsNullOrWhiteSpace(t));

    /// <summary>The window's client area in screen pixels: the bounds of the visual that fills it, below any title bar.</summary>
    private static Rect ClientRect(Window window) =>
        ScreenRect((FrameworkElement)VisualTreeHelper.GetChild(window, 0));

    private static Rect ScreenRect(FrameworkElement element) =>
        new(element.PointToScreen(new Point(0, 0)), element.PointToScreen(new Point(element.ActualWidth, element.ActualHeight)));

    private static ResourceDictionary LoadTheme(string theme) =>
        new() { Source = new Uri($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{theme}Theme.xaml") };

    /// <summary>Runs <paramref name="body"/> with the theme under test as the application's own, so a control resolves its brushes and styles from it as the running app does.</summary>
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
}
