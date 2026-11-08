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
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using Shape = System.Windows.Shapes.Shape;
using DuckDB.NET.Data;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8305: the time-range picker and the sidebar's More menu at Lite's 900x600 minimum. The picker's popup opens inside the
/// window, its calendar draws every week row and the weekday header, and its "Data starts" note names the server's oldest data,
/// not the start of the range that was on screen. The More menu opens inside the window, beside its button, and its separators
/// span the menu in the theme's brush. Controls are built on the shared STA thread, themed as the app themes them, shown at
/// 900x600 on a monitor (a popup is placed against the screen, so an off-screen window cannot show where one opens) and read
/// from what rendered.
/// </summary>
[Collection("credential-manager")]
public sealed class RangePickerAndMoreMenuAt900Tests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    /// <summary>A Sunday early in a month, so a calendar that hides the days after today shows only two week rows of six.</summary>
    private static readonly DateTime Today = new(2026, 11, 8, 15, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheRangePickerPopup_OpensInsideTheWindow_AndItsCalendarDrawsSixWeekRowsAndTheWeekdayHeader(string theme)
    {
        var dir = Path.Combine(Path.GetTempPath(), "RangePicker900_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var problems = StaTestThread.Run(() => WithAppTheme(theme, () =>
            {
                var tab = BuildTab(duckDb, "range-picker-test");
                var window = new Window { Content = tab, Width = 900, Height = 600, Left = 0, Top = 0 };
                window.Resources.MergedDictionaries.Add(LoadTheme(theme));
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

                    Render((FrameworkElement)popup.Child, Path.Combine("picker", $"{theme}-popup"));
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
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public async Task TheDataStartNote_NamesTheServersOldestData_NotTheStartOfTheRangeOnScreen()
    {
        var dir = Path.Combine(Path.GetTempPath(), "RangePickerData_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            await duckDb.InitializeAsync();
            var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "data-start-test", DisplayName = "data-start-test", IsEnabled = true };
            var serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
            var now = DateTime.UtcNow;
            var oldest = now.AddDays(-3);
            SeedCollectionLog(duckDb, serverId, oldest, now.AddHours(-4), now.AddMinutes(-5));

            string? text = null, expected = null;
            Visibility visibility = Visibility.Collapsed, covered = Visibility.Visible;
            StaTestThread.Run(async () =>
            {
                var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
                var added = app.Resources.Contains("NumericCell") ? null : LoadTheme("Dark");
                if (added is not null)
                {
                    app.Resources.MergedDictionaries.Add(added);
                }

                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = 900, Height = 600, Left = 0, Top = 0 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    StaTestThread.Settle(window);
                    /* The read the tab makes for its charts with every refresh: the server's data start. The picker's note is fed from it. */
                    var read = (Task)typeof(ServerTab).GetMethod("RefreshServerDataStartAsync", Instance)!.Invoke(tab, new object?[] { 4, null })!;
                    await read;

                    var picker = (TimeRangePicker)tab.FindName("RangePicker");
                    /* Past 4 hours is on screen, and the reader types a week: the note must name where the data starts, three days back. */
                    picker.Value = TimeRangePresets.Find("4h")!;
                    covered = picker.NoteText.Visibility;
                    picker.Value = TimeRangePresets.Find("1w")!;
                    var range = picker.Resolve()!;
                    text = picker.NoteText.Text;
                    visibility = picker.NoteText.Visibility;
                    expected = TimeRangeNotes.DataStartNote(range, oldest);
                }
                finally
                {
                    window.Close();
                    if (added is not null)
                    {
                        app.Resources.MergedDictionaries.Remove(added);
                    }
                }
            });

            Assert.NotNull(expected);
            Assert.Equal(Visibility.Collapsed, covered);
            Assert.True(visibility == Visibility.Visible, $"a week's range over data three days old shows no data-start note (the note reads '{text}')");
            Assert.Equal(expected, text);
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
    public void TheMoreMenu_OpensInsideTheWindow_AndItsSeparatorsSpanTheMenuInTheThemeBrush(string theme)
    {
        var problems = StaTestThread.Run(() => WithAppTheme(theme, () =>
        {
            var dict = LoadTheme(theme);
            var footer = new SidebarFooter { Width = 260, VerticalAlignment = VerticalAlignment.Bottom, HorizontalAlignment = HorizontalAlignment.Left };
            var window = new Window { Content = footer, Width = 900, Height = 600, Left = 0, Top = 0 };
            window.Resources.MergedDictionaries.Add(dict);
            StaTestThread.ShowOnAMonitor(window);
            try
            {
                StaTestThread.Settle(window);
                var more = (Button)footer.FindName("MoreButton");
                var menu = (ContextMenu)footer.FindName("MoreMenu");
                more.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
                StaTestThread.WaitForIdle(window.Dispatcher, TimeSpan.FromSeconds(10));
                Assert.True(menu.IsOpen, $"[{theme}] the More menu did not open");

                var found = new List<string>();
                var client = ClientRect(window);
                var shown = ScreenRect(menu);
                if (shown.Top < client.Top - 1 || shown.Bottom > client.Bottom + 1 || shown.Left < client.Left - 1 || shown.Right > client.Right + 1)
                {
                    found.Add($"the menu spans x {shown.Left:F0}..{shown.Right:F0}, y {shown.Top:F0}..{shown.Bottom:F0}, outside the window's x {client.Left:F0}..{client.Right:F0}, y {client.Top:F0}..{client.Bottom:F0}");
                }

                var items = menu.Items.OfType<MenuItem>().ToList();
                var itemBounds = items[0].TransformToAncestor(menu).TransformBounds(new Rect(items[0].RenderSize));
                var brush = ((SolidColorBrush)dict["BorderBrush"]).Color;
                var separators = menu.Items.OfType<Separator>().ToList();
                if (separators.Count == 0)
                {
                    found.Add("the menu has no separators");
                }

                foreach (var (separator, index) in separators.Select((s, i) => (s, i)))
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

                Render(menu, Path.Combine("menus", $"{theme}-more-900"));
                menu.IsOpen = false;
                return found;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(problems.Count == 0, $"[{theme}] More menu at 900x600:{Environment.NewLine}" + string.Join(Environment.NewLine, problems));
    }

    private static ServerTab BuildTab(DuckDbInitializer duckDb, string name)
    {
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = name, DisplayName = name, IsEnabled = true };
        var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
        /* Switching to a tab would start that tab's load against a server that is not there. */
        typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
        return tab;
    }

    private static void SeedCollectionLog(DuckDbInitializer duckDb, int serverId, params DateTime[] collectionTimes)
    {
        using var connection = duckDb.CreateConnection();
        if (connection.State != System.Data.ConnectionState.Open)
        {
            connection.Open();
        }

        var logId = -8305000;
        foreach (var time in collectionTimes)
        {
            using var command = connection.CreateCommand();
            command.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status)
VALUES ($1, $2, 'data-start-test', 'wait_stats', $3, 5, 'SUCCESS')";
            command.Parameters.Add(new DuckDBParameter { Value = logId-- });
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(time, DateTimeKind.Unspecified) });
            command.ExecuteNonQuery();
        }
    }

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

    private static void Render(FrameworkElement element, string name)
    {
        var dir = Environment.GetEnvironmentVariable("LITE_TEST_PNG_DIR");
        if (string.IsNullOrEmpty(dir) || element.ActualWidth < 1 || element.ActualHeight < 1)
        {
            return;
        }

        var bitmap = new RenderTargetBitmap((int)Math.Ceiling(element.ActualWidth), (int)Math.Ceiling(element.ActualHeight), 96, 96, PixelFormats.Pbgra32);
        bitmap.Render(element);
        var path = Path.Combine(dir, name + ".png");
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var encoder = new PngBitmapEncoder();
        encoder.Frames.Add(BitmapFrame.Create(bitmap));
        using var stream = File.Create(path);
        encoder.Save(stream);
    }

    private static ResourceDictionary LoadTheme(string theme) =>
        new() { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}Theme.xaml") };

    /// <summary>Runs <paramref name="body"/> with the theme under test as the application's own, so a control resolves its brushes and the app-level styles from it as the running app does.</summary>
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
