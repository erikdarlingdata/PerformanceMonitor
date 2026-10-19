/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
using System.Windows.Threading;

/// <summary>
/// #6136: finds the text a laid-out window cuts off. Lives in Darling.Tests and is compiled into Lite.Tests by a link (one probe, two suites);
/// global namespace so both suites use it without a <c>using</c>.
/// </summary>
internal static class ClippedTextProbe
{
    internal static void Settle(Window window)
    {
        window.UpdateLayout();
        var frame = new DispatcherFrame();
        window.Dispatcher.BeginInvoke(DispatcherPriority.ApplicationIdle, new Action(() => frame.Continue = false));
        Dispatcher.PushFrame(frame);
        window.UpdateLayout();
    }

    internal static IEnumerable<DependencyObject> Descendants(DependencyObject root)
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            yield return child;
            foreach (var deeper in Descendants(child))
            {
                yield return deeper;
            }
        }
    }

    /// <summary>A grid scrolls sideways, so a column past the edge is reached by scrolling; its headers are checked by <see cref="ClippedHeaders"/>.</summary>
    private static bool InsideGrid(DependencyObject element)
    {
        for (var parent = VisualTreeHelper.GetParent(element); parent is not null; parent = VisualTreeHelper.GetParent(parent))
        {
            if (parent is DataGrid)
            {
                return true;
            }
        }

        return false;
    }

    private static ComboBox? ComboOf(DependencyObject element)
    {
        for (var parent = VisualTreeHelper.GetParent(element); parent is not null; parent = VisualTreeHelper.GetParent(parent))
        {
            if (parent is ComboBox combo)
            {
                return combo;
            }
        }

        return null;
    }

    internal static double NaturalWidth(TextBlock block)
    {
        var typeface = new Typeface(block.FontFamily, block.FontStyle, block.FontWeight, block.FontStretch);
        var formatted = new FormattedText(
            block.Text, CultureInfo.CurrentUICulture, FlowDirection.LeftToRight, typeface, block.FontSize, Brushes.Black,
            VisualTreeHelper.GetDpi(block).PixelsPerDip);
        return formatted.WidthIncludingTrailingWhitespace;
    }

    /// <summary>The text under <paramref name="root"/> that does not fit: a single-line block wider than its own box, or a block whose
    /// right edge is past the width <paramref name="limit"/> shows.</summary>
    internal static List<string> Clipped(DependencyObject root, FrameworkElement limit)
    {
        var found = new List<string>();
        foreach (var block in Descendants(root).OfType<TextBlock>())
        {
            if (!block.IsVisible || string.IsNullOrWhiteSpace(block.Text) || block.ActualWidth <= 0 || InsideGrid(block))
            {
                continue;
            }

            if (block.TextWrapping == TextWrapping.NoWrap && block.TextTrimming == TextTrimming.None && NaturalWidth(block) > block.ActualWidth + 1)
            {
                found.Add($"'{block.Text}' needs {NaturalWidth(block):0} but has {block.ActualWidth:0}");
                continue;
            }

            /* A drop-down's chosen text sits in a window that ends where the arrow starts (about 20 wide); text past it is cut. */
            var combo = ComboOf(block);
            if (combo is not null)
            {
                var inCombo = block.TransformToAncestor(combo).TransformBounds(new Rect(0, 0, block.ActualWidth, block.ActualHeight)).Right;
                if (inCombo > combo.ActualWidth - 20)
                {
                    found.Add($"'{block.Text}' ends at {inCombo:0} in a {combo.ActualWidth:0} drop-down");
                    continue;
                }
            }

            var right = block.TransformToAncestor(limit).TransformBounds(new Rect(0, 0, block.ActualWidth, block.ActualHeight)).Right;
            if (right > limit.ActualWidth + 1)
            {
                found.Add($"'{block.Text}' ends at {right:0}, past the {limit.ActualWidth:0} on screen");
            }
        }

        return found;
    }

    /// <summary>The column headers under <paramref name="root"/> whose text runs past the header's own width.</summary>
    internal static List<string> ClippedHeaders(DependencyObject root)
    {
        var found = new List<string>();
        foreach (var header in Descendants(root).OfType<DataGridColumnHeader>())
        {
            foreach (var block in Descendants(header).OfType<TextBlock>().Where(b => b.IsVisible && b.Text.Length > 0))
            {
                var right = block.TransformToAncestor(header).TransformBounds(new Rect(0, 0, block.ActualWidth, block.ActualHeight)).Right;
                if (right > header.ActualWidth + 1)
                {
                    found.Add($"header '{block.Text}' ends at {right:0} in a {header.ActualWidth:0} column");
                }
            }
        }

        return found;
    }

    internal static string Describe(List<string> clipped) =>
        string.Join(Environment.NewLine, clipped.Select(c => c.Length > 160 ? c[..160] + "..." : c));
}
