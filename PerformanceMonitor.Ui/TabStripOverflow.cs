/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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

    private static void Wire(TabControl tabs)
    {
        tabs.ApplyTemplate();
        var scroll = tabs.Template?.FindName(ScrollPartName, tabs) as ScrollViewer;
        var button = tabs.Template?.FindName(OverflowButtonPartName, tabs) as Button;
        if (scroll is null || button is null)
        {
            return;
        }

        scroll.ScrollChanged -= OnScrollChanged;
        scroll.ScrollChanged += OnScrollChanged;
        scroll.PreviewMouseWheel -= OnPreviewMouseWheel;
        scroll.PreviewMouseWheel += OnPreviewMouseWheel;
        button.Click -= OnOverflowClick;
        button.Click += OnOverflowClick;
        UpdateButton(scroll, button);
    }

    private static void OnScrollChanged(object sender, ScrollChangedEventArgs e)
    {
        if (sender is ScrollViewer scroll && scroll.TemplatedParent is TabControl tabs
            && tabs.Template?.FindName(OverflowButtonPartName, tabs) is Button button)
        {
            UpdateButton(scroll, button);
        }
    }

    private static void UpdateButton(ScrollViewer scroll, Button button)
    {
        button.Visibility = scroll.ScrollableWidth > FitSlack ? Visibility.Visible : Visibility.Collapsed;
    }

    private static void OnPreviewMouseWheel(object sender, MouseWheelEventArgs e)
    {
        if (sender is ScrollViewer scroll && scroll.ScrollableWidth > FitSlack)
        {
            scroll.ScrollToHorizontalOffset(scroll.HorizontalOffset - e.Delta);
            e.Handled = true;
        }
    }

    private static void OnSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        /* The event bubbles up from a nested TabControl in the tab's content; only this strip's own selection scrolls it. */
        if (sender is TabControl tabs && ReferenceEquals(e.OriginalSource, tabs) && tabs.SelectedItem is TabItem selected)
        {
            selected.Dispatcher.BeginInvoke(new Action(selected.BringIntoView), System.Windows.Threading.DispatcherPriority.Loaded);
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
                target.BringIntoView();
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
