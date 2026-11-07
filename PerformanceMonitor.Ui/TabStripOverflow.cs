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
using System.Windows.Input;

namespace PerformanceMonitor.Ui;

/// <summary>
/// #8169: keeps a <see cref="TabControl"/>'s tab strip on one row. The theme's template puts the tabs in a horizontal
/// ScrollViewer (<see cref="ScrollPartName"/>) beside a "»" button (<see cref="OverflowButtonPartName"/>). This behavior shows
/// the button only while some tab does not fit, lists exactly those tabs in the button's menu, selects the one clicked and
/// scrolls it into view, and lets the mouse wheel scroll the strip.
/// </summary>
public static class TabStripOverflow
{
    public const string ScrollPartName = "PART_TabStripScroll";
    public const string OverflowButtonPartName = "PART_TabOverflowButton";

    /* A tab counts as fitting when it lies inside the viewport to within this many DIPs. */
    private const double FitSlack = 0.5;

    public static readonly DependencyProperty EnabledProperty =
        DependencyProperty.RegisterAttached(
            "Enabled",
            typeof(bool),
            typeof(TabStripOverflow),
            new PropertyMetadata(false, OnEnabledChanged));

    /// <summary>
    /// #8222: the space around the tab row inside the control (left, top, right, bottom), read by the theme's template. A row that sits
    /// inside a page with no margin of its own sets it so every sub-tab row starts at the same left offset.
    /// </summary>
    public static readonly DependencyProperty HeaderMarginProperty =
        DependencyProperty.RegisterAttached(
            "HeaderMargin",
            typeof(Thickness),
            typeof(TabStripOverflow),
            new PropertyMetadata(new Thickness(0)));

    public static Thickness GetHeaderMargin(DependencyObject obj) => (Thickness)obj.GetValue(HeaderMarginProperty);
    public static void SetHeaderMargin(DependencyObject obj, Thickness value) => obj.SetValue(HeaderMarginProperty, value);

    public static bool GetEnabled(DependencyObject obj) => (bool)obj.GetValue(EnabledProperty);
    public static void SetEnabled(DependencyObject obj, bool value) => obj.SetValue(EnabledProperty, value);

    private static void OnEnabledChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
    {
        if (d is not TabControl tabs)
        {
            return;
        }

        tabs.Loaded -= OnLoaded;
        tabs.SelectionChanged -= OnSelectionChanged;
        if (e.NewValue is true)
        {
            tabs.Loaded += OnLoaded;
            tabs.SelectionChanged += OnSelectionChanged;
            if (tabs.IsLoaded)
            {
                Wire(tabs);
            }
        }
    }

    private static void OnLoaded(object sender, RoutedEventArgs e)
    {
        if (sender is TabControl tabs)
        {
            Wire(tabs);
        }
    }

    private sealed class StripState
    {
        public bool Pending;
        public bool EnsureSelected;
        public bool Wired;
    }

    private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<TabControl, StripState> States = new();

    private static void Wire(TabControl tabs)
    {
        tabs.ApplyTemplate();
        var scroll = tabs.Template?.FindName(ScrollPartName, tabs) as ScrollViewer;
        var button = tabs.Template?.FindName(OverflowButtonPartName, tabs) as Button;
        if (scroll is null || button is null)
        {
            return;
        }

        scroll.PreviewMouseWheel -= OnPreviewMouseWheel;
        scroll.PreviewMouseWheel += OnPreviewMouseWheel;
        button.Click -= OnOverflowClick;
        button.Click += OnOverflowClick;

        var state = States.GetOrCreateValue(tabs);
        if (!state.Wired)
        {
            state.Wired = true;
            /* The row's width and the tabs' widths are what decide where it can start and end. */
            if (scroll.Parent is FrameworkElement host)
            {
                host.SizeChanged += (_, _) => Schedule(tabs, ensureSelected: false);
            }

            if (scroll.Content is FrameworkElement panel)
            {
                panel.SizeChanged += (_, _) => Schedule(tabs, ensureSelected: false);
                /* A focused or clicked tab asks to be brought into view, and the stock scroll would stop on any pixel, cutting a
                   name at the edge. Only the selected tab's request is answered, by the snap below. */
                panel.AddHandler(FrameworkElement.RequestBringIntoViewEvent, new RequestBringIntoViewEventHandler((_, args) =>
                {
                    args.Handled = true;
                    if (args.TargetObject is TabItem { IsSelected: true })
                    {
                        Schedule(tabs, ensureSelected: true);
                    }
                }));
            }
        }

        Schedule(tabs, ensureSelected: true);
    }

    /// <summary>Applies the snap once layout has settled; several triggers in one pass share one run.</summary>
    private static void Schedule(TabControl tabs, bool ensureSelected)
    {
        var state = States.GetOrCreateValue(tabs);
        state.EnsureSelected |= ensureSelected;
        if (state.Pending)
        {
            return;
        }

        state.Pending = true;
        tabs.Dispatcher.BeginInvoke(new Action(() =>
        {
            state.Pending = false;
            var ensure = state.EnsureSelected;
            state.EnsureSelected = false;
            Apply(tabs, ensure, 0);
        }), System.Windows.Threading.DispatcherPriority.Loaded);
    }

    /// <summary>
    /// Lays the row out in whole tabs. A row that holds every tab shows no button. A row that does not shows the button, starts at a
    /// tab's left edge (the nearest to where it was, moved <paramref name="step"/> tabs, and far enough to bring the selected tab
    /// in when <paramref name="ensureSelected"/>), and is exactly as wide as the whole tabs that fit, so no name is cut at either edge.
    /// </summary>
    private static void Apply(TabControl tabs, bool ensureSelected, int step)
    {
        if (!tabs.IsLoaded
            || tabs.Template?.FindName(ScrollPartName, tabs) is not ScrollViewer scroll
            || tabs.Template?.FindName(OverflowButtonPartName, tabs) is not Button button
            || scroll.Parent is not FrameworkElement host
            || scroll.Content is not FrameworkElement panel
            || host.ActualWidth <= 0)
        {
            return;
        }

        var visible = tabs.Items.OfType<TabItem>().Where(t => t.Visibility == Visibility.Visible && t.IsArrangeValid && t.RenderSize.Width > 0).ToList();
        if (visible.Count == 0)
        {
            return;
        }

        var lefts = new double[visible.Count];
        var rights = new double[visible.Count];
        for (var i = 0; i < visible.Count; i++)
        {
            var bounds = visible[i].TransformToAncestor(panel).TransformBounds(new Rect(visible[i].RenderSize));
            lefts[i] = bounds.Left;
            rights[i] = bounds.Right;
        }

        var total = rights.Max();
        if (total <= host.ActualWidth + FitSlack)
        {
            if (button.Visibility != Visibility.Collapsed)
            {
                button.Visibility = Visibility.Collapsed;
            }

            if (!double.IsNaN(scroll.Width))
            {
                scroll.ClearValue(FrameworkElement.WidthProperty);
                scroll.ClearValue(FrameworkElement.HorizontalAlignmentProperty);
                scroll.UpdateLayout();
            }

            scroll.ScrollToHorizontalOffset(0);
            return;
        }

        button.Visibility = Visibility.Visible;
        button.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
        var available = host.ActualWidth - button.DesiredSize.Width;

        var first = 0;
        for (var i = 1; i < visible.Count; i++)
        {
            if (Math.Abs(lefts[i] - scroll.HorizontalOffset) < Math.Abs(lefts[first] - scroll.HorizontalOffset))
            {
                first = i;
            }
        }

        first = Math.Clamp(first + step, 0, visible.Count - 1);
        var selected = visible.FindIndex(t => t.IsSelected);
        if (ensureSelected && selected >= 0)
        {
            if (selected < first)
            {
                first = selected;
            }

            while (first < selected && rights[selected] - lefts[first] > available + FitSlack)
            {
                first++;
            }
        }

        /* Never start so far along that the row ends short of the last tab with room to spare. */
        var lastStart = visible.Count - 1;
        while (lastStart > 0 && total - lefts[lastStart - 1] <= available + FitSlack)
        {
            lastStart--;
        }

        first = Math.Min(first, lastStart);

        var last = first;
        while (last + 1 < visible.Count && rights[last + 1] - lefts[first] <= available + FitSlack)
        {
            last++;
        }

        var width = Math.Min(rights[last] - lefts[first], available);
        if (scroll.HorizontalAlignment != HorizontalAlignment.Left)
        {
            scroll.HorizontalAlignment = HorizontalAlignment.Left;
        }

        if (double.IsNaN(scroll.Width) || Math.Abs(scroll.Width - width) > 0.01)
        {
            scroll.Width = width;
            scroll.UpdateLayout();
        }

        if (Math.Abs(scroll.HorizontalOffset - lefts[first]) > 0.01)
        {
            scroll.ScrollToHorizontalOffset(lefts[first]);
        }
    }

    private static void OnPreviewMouseWheel(object sender, MouseWheelEventArgs e)
    {
        if (sender is ScrollViewer { TemplatedParent: TabControl tabs }
            && tabs.Template?.FindName(OverflowButtonPartName, tabs) is Button { Visibility: Visibility.Visible })
        {
            Apply(tabs, ensureSelected: false, e.Delta < 0 ? 1 : -1);
            e.Handled = true;
        }
    }

    private static void OnSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        /* The event bubbles up from a nested TabControl in the tab's content; only this strip's own selection scrolls it. */
        if (sender is TabControl tabs && ReferenceEquals(e.OriginalSource, tabs))
        {
            Schedule(tabs, ensureSelected: true);
        }
    }

    private static void OnOverflowClick(object sender, RoutedEventArgs e)
    {
        if (sender is not Button button || button.TemplatedParent is not TabControl tabs)
        {
            return;
        }

        var menu = new ContextMenu { PlacementTarget = button, Placement = PlacementMode.Bottom };
        foreach (var tab in HiddenTabs(tabs))
        {
            var item = new MenuItem { Header = HeaderText(tab), IsChecked = tab.IsSelected };
            var target = tab;
            item.Click += (_, _) =>
            {
                tabs.SelectedItem = target;
                Schedule(tabs, ensureSelected: true);
            };
            menu.Items.Add(item);
        }

        button.ContextMenu = menu;
        menu.IsOpen = true;
    }

    /// <summary>The visible tabs that are cut off or scrolled out of the strip, in tab order.</summary>
    public static IReadOnlyList<TabItem> HiddenTabs(TabControl tabs)
    {
        var hidden = new List<TabItem>();
        if (tabs.Template?.FindName(ScrollPartName, tabs) is not ScrollViewer scroll)
        {
            return hidden;
        }

        foreach (var item in tabs.Items)
        {
            if (item is not TabItem tab || tab.Visibility != Visibility.Visible || !tab.IsArrangeValid)
            {
                continue;
            }

            var bounds = tab.TransformToAncestor(scroll).TransformBounds(new Rect(tab.RenderSize));
            if (bounds.Left < -FitSlack || bounds.Right > scroll.ViewportWidth + FitSlack)
            {
                hidden.Add(tab);
            }
        }

        return hidden;
    }

    /// <summary>A tab's caption: a string header as is, otherwise the first text found inside a custom header.</summary>
    public static string HeaderText(TabItem tab)
    {
        return tab.Header switch
        {
            string s => s,
            DependencyObject d => FirstText(d) ?? string.Empty,
            _ => tab.Header?.ToString() ?? string.Empty,
        };
    }

    private static string? FirstText(DependencyObject root)
    {
        if (root is TextBlock text && !string.IsNullOrEmpty(text.Text))
        {
            return text.Text;
        }

        foreach (var child in LogicalTreeHelper.GetChildren(root))
        {
            if (child is DependencyObject d && FirstText(d) is { } found)
            {
                return found;
            }
        }

        return null;
    }
}
