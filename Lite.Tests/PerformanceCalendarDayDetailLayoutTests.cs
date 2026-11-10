/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8532: the month calendar keeps its day cells when a day is clicked. The day-detail panel opens under the
/// cells, inside the room the Daily Summary tab gives the calendar. In a small window the cells and the panel
/// cannot both fit, so the calendar scrolls: every cell keeps the height it asks for, with its day number and
/// band label inside it, and the calendar scrolls the panel it just opened into view. With nothing clicked, a
/// six-week month fits that window without scrolling. With room to spare, the cells share all the height that is
/// left, down to the host's bottom edge. Rendered: the real control with the app's theme, laid out in a host of a
/// stated size.
/// </summary>
public sealed class PerformanceCalendarDayDetailLayoutTests
{
    private static readonly Uri Theme = new("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml");

    /// <summary>August 2026 spans six week rows whether the week starts on Sunday or on Monday.</summary>
    private static readonly DateTime Month = new(2026, 8, 1);

    private static readonly DateTime Clicked = new(2026, 8, 12);

    private const double Tolerance = 0.5;

    /// <summary>The ring around today's cell makes every week row 2 px taller, 12 px over six rows.</summary>
    private const double TodayRingOverSixRows = 12;

    /// <summary>About the room the Daily Summary tab has for the calendar in a 900 x 600 window, and a shorter one.</summary>
    [Theory]
    [InlineData(900, 350)]
    [InlineData(900, 260)]
    public void ClickedDay_InASmallRoom_KeepsEveryCellAtItsHeight(double width, double height)
    {
        var violations = StaTestThread.Run(() =>
        {
            var (host, calendar) = Build(width, height, click: true);
            return CellViolations(calendar).Concat(ReachViolations(host, calendar)).ToList();
        });

        Assert.Empty(violations);
    }

    /// <summary>With no day clicked, a six-week month fits the room a 900 x 600 window gives without a scroll bar, with
    /// the 12 px to spare that a month holding today needs. Run with the week starting on Sunday and on Monday.</summary>
    [Theory]
    [InlineData("en-US")]
    [InlineData("en-GB")]
    public void NothingClicked_InASmallRoom_ASixWeekMonthFitsWithoutScrolling(string culture)
    {
        var violations = StaTestThread.Run(() =>
        {
            var saved = CultureInfo.CurrentCulture;
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
            try
            {
                var (host, calendar) = Build(900, 350, click: false);
                var found = CellViolations(calendar).Concat(ReachViolations(host, calendar)).ToList();
                if (calendar.Cells.Count != 42)
                    found.Add($"the month drew {calendar.Cells.Count} cells, not six weeks");

                foreach (var scroller in Descendants<ScrollViewer>(calendar))
                {
                    if (scroller.ComputedVerticalScrollBarVisibility != Visibility.Collapsed)
                        found.Add($"a scroll bar shows with nothing clicked: the calendar is {scroller.ExtentHeight:F1} px tall in a {scroller.ViewportHeight:F1} px view");
                    else if (scroller.ExtentHeight + TodayRingOverSixRows > scroller.ViewportHeight + Tolerance)
                        found.Add($"the calendar is {scroller.ExtentHeight:F1} px tall in a {scroller.ViewportHeight:F1} px view, too tall for a month that holds today");
                }

                return found;
            }
            finally
            {
                CultureInfo.CurrentCulture = saved;
            }
        });

        Assert.Empty(violations);
    }

    /// <summary>The panel is brought into view when a day is clicked, and at no other time: a host that hands the
    /// calendar a refreshed list of days leaves the scroll position where the user put it.</summary>
    [Fact]
    public void ARefreshedDaysList_LeavesTheScrollPositionAlone()
    {
        var moved = StaTestThread.Run(() =>
        {
            var (host, calendar) = Build(900, 350, click: true);
            var scroller = ScrollerAround(CellPresenters(calendar)[0], calendar);
            if (scroller is null)
                return null;

            scroller.ScrollToVerticalOffset(0);
            Layout(host);
            var before = scroller.VerticalOffset;

            calendar.Days = new[] { DayWithOneDeadlock() };
            Layout(host);
            return Math.Abs(scroller.VerticalOffset - before) > Tolerance
                ? $"the scroll position went from {before:F1} to {scroller.VerticalOffset:F1} px when the days were refreshed"
                : null;
        });

        Assert.Null(moved);
    }

    [Fact]
    public void ClickedDay_InALargeRoom_StillSharesTheSpareHeightAmongTheCells()
    {
        var violations = StaTestThread.Run(() =>
        {
            var (host, calendar) = Build(1600, 900, click: true);
            var found = CellViolations(calendar).Concat(ReachViolations(host, calendar)).ToList();

            var panel = DetailPanel(calendar);
            var panelBounds = BoundsIn(panel, host);
            var cells = CellPresenters(calendar);
            var lastCellBottom = cells.Max(cell => BoundsIn(cell, host).Bottom);

            /* The panel ends at the host's bottom edge and the last week row ends where the panel begins, so
               the week rows hold all the height the panel and the header rows leave. */
            if (host.Height - panelBounds.Bottom > 12)
                found.Add($"the day-detail panel ends at {panelBounds.Bottom:F1} px in a {host.Height:F0} px host");
            if (panelBounds.Top - lastCellBottom > 16)
                found.Add($"the last week row ends at {lastCellBottom:F1} px and the day-detail panel starts at {panelBounds.Top:F1} px");

            return found;
        });

        Assert.Empty(violations);
    }

    private static PerformanceCalendarDay DayWithOneDeadlock() => new()
    {
        Date = Clicked,
        Band = DailyHealthBand.Healthy,
        Signals = new DailyHealthSignals { HasData = true, Deadlocks = 1, Window = TimeSpan.FromHours(24) },
        Tooltip = "1 deadlock",
        TopWaitType = "LCK_M_X",
        TotalWaitSeconds = 12.5m,
        UniqueQueries = 40,
    };

    private static (Border Host, PerformanceCalendar Calendar) Build(double width, double height, bool click)
    {
        var calendar = new PerformanceCalendar { DisplayMonth = Month, Days = new[] { DayWithOneDeadlock() } };

        var host = new Border { Width = width, Height = height, Child = calendar };
        host.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = Theme });
        Layout(host);

        if (click)
        {
            calendar.SelectedDate = Clicked;
            Layout(host);
        }

        Assert.Equal(click ? Visibility.Visible : Visibility.Collapsed, DetailPanel(calendar).Visibility);
        Assert.Equal(calendar.Cells.Count, CellPresenters(calendar).Count);
        Assert.True(calendar.Cells.Count >= 28, $"the month drew {calendar.Cells.Count} cells");
        return (host, calendar);
    }

    private static void Layout(FrameworkElement host)
    {
        host.Measure(new Size(host.Width, host.Height));
        host.Arrange(new Rect(0, 0, host.Width, host.Height));
        host.UpdateLayout();
    }

    /// <summary>Every element of every day cell gets the room it asks for: nothing in a cell is cut by layout,
    /// nothing is handed less than its minimum height, and the cell's text lies inside the cell.</summary>
    private static IEnumerable<string> CellViolations(PerformanceCalendar calendar)
    {
        foreach (var presenter in CellPresenters(calendar))
        {
            var cell = (PerformanceCalendarCell)presenter.Content;
            var name = "cell " + cell.Date.ToString("MM-dd", CultureInfo.InvariantCulture);
            var slot = LayoutInformation.GetLayoutSlot(presenter);

            foreach (var element in Descendants<FrameworkElement>(presenter).Prepend(presenter))
            {
                var kind = element.GetType().Name;
                var elementSlot = LayoutInformation.GetLayoutSlot(element);
                if (LayoutInformation.GetLayoutClip(element) is not null)
                    yield return $"{name}: its {kind} is cut to a {elementSlot.Height:F1} px slot";
                if (elementSlot.Height + Tolerance < element.MinHeight)
                    yield return $"{name}: its {kind} asks for {element.MinHeight:F0} px and has {elementSlot.Height:F1} px";
            }

            foreach (var text in Descendants<TextBlock>(presenter).Where(t => !string.IsNullOrEmpty(t.Text)))
            {
                var bounds = BoundsIn(text, presenter);
                if (bounds.Top < -Tolerance || bounds.Bottom > slot.Height + Tolerance)
                    yield return $"{name}: \"{text.Text}\" spans {bounds.Top:F1} to {bounds.Bottom:F1} px in a {slot.Height:F1} px cell";
            }
        }
    }

    /// <summary>Everything the calendar draws can be reached, and an open day-detail panel is in view. With no
    /// scrolling region, everything lies inside the host. With one, it shows its scroll bar when its content is taller
    /// than its view, and it is scrolled so the panel lies inside the view, or fills the view from the panel's top edge
    /// when the panel is taller than the view.</summary>
    private static IEnumerable<string> ReachViolations(Border host, PerformanceCalendar calendar)
    {
        var panel = DetailPanel(calendar);
        var scroller = ScrollerAround(CellPresenters(calendar)[0], calendar);
        if (scroller is null)
        {
            var drawn = CellPresenters(calendar).Cast<FrameworkElement>().Append(panel);
            foreach (var element in drawn)
            {
                var bounds = BoundsIn(element, host);
                if (bounds.Bottom > host.Height + Tolerance)
                    yield return $"a {element.GetType().Name} ends at {bounds.Bottom:F1} px, below the {host.Height:F0} px host, and nothing scrolls";
            }

            yield break;
        }

        if (scroller.ExtentHeight > scroller.ViewportHeight + Tolerance
            && scroller.ComputedVerticalScrollBarVisibility != Visibility.Visible)
        {
            yield return $"the calendar is {scroller.ExtentHeight:F1} px tall in a {scroller.ViewportHeight:F1} px view and shows no scroll bar";
        }

        if (panel.Visibility != Visibility.Visible)
            yield break;

        var view = Descendants<ScrollContentPresenter>(scroller).First();
        var inView = BoundsIn(panel, view);
        if (inView.Height <= view.ActualHeight + Tolerance)
        {
            if (inView.Top < -Tolerance || inView.Bottom > view.ActualHeight + Tolerance)
                yield return $"the day-detail panel spans {inView.Top:F1} to {inView.Bottom:F1} px of a {view.ActualHeight:F1} px view";
        }
        else if (Math.Abs(inView.Top) > Tolerance)
        {
            yield return $"the day-detail panel is taller than the {view.ActualHeight:F1} px view and starts at {inView.Top:F1} px, not at the view's top";
        }
    }

    private static FrameworkElement DetailPanel(PerformanceCalendar calendar) =>
        Assert.IsAssignableFrom<FrameworkElement>(calendar.FindName("DayDetailPanel"));

    private static List<ContentPresenter> CellPresenters(PerformanceCalendar calendar) =>
        Descendants<ContentPresenter>(calendar).Where(presenter => presenter.Content is PerformanceCalendarCell).ToList();

    private static ScrollViewer? ScrollerAround(DependencyObject element, DependencyObject within)
    {
        for (var parent = VisualTreeHelper.GetParent(element); parent is not null && parent != within; parent = VisualTreeHelper.GetParent(parent))
        {
            if (parent is ScrollViewer scroller)
                return scroller;
        }

        return null;
    }

    private static Rect BoundsIn(FrameworkElement element, Visual ancestor) =>
        element.TransformToAncestor(ancestor).TransformBounds(new Rect(element.RenderSize));

    private static IEnumerable<T> Descendants<T>(DependencyObject root)
        where T : DependencyObject
    {
        var count = VisualTreeHelper.GetChildrenCount(root);
        for (var i = 0; i < count; i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            if (child is T match)
                yield return match;

            foreach (var deeper in Descendants<T>(child))
                yield return deeper;
        }
    }
}
