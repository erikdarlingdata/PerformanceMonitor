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
    /// <summary>How long <see cref="Settle(Window)"/> waits for the dispatcher to reach application idle: a healthy window takes tens of milliseconds, a loaded runner a few seconds.</summary>
    internal static readonly TimeSpan SettleLimit = TimeSpan.FromSeconds(15);

    internal static void Settle(Window window) => Settle(window, SettleLimit);

    /// <summary>
    /// Lays the window out, lets the dispatcher run to application idle, and lays it out again. #6216: the wait for idle is bounded. An
    /// idle that never comes (a layout or a handler that queues work ahead of it without end) used to leave the body, and with it the
    /// shared STA thread, in <c>PushFrame</c> until the hang guard gave up on the whole process; now it fails this test alone, naming how
    /// many operations the dispatcher ran meanwhile, and the thread stays usable.
    /// </summary>
    internal static void Settle(Window window, TimeSpan limit)
    {
        /* Each step is named, so a body that never returns says in the hang guard's message which of the three it was in. */
        StaTestThread.Step("Settle: the layout pass before the idle frame");
        window.UpdateLayout();
        var dispatcher = window.Dispatcher;
        var frame = new DispatcherFrame();
        var idle = false;
        var operations = 0;
        var lastPriority = DispatcherPriority.Invalid;
        DispatcherHookEventHandler counter = (_, e) =>
        {
            operations++;
            lastPriority = e.Operation.Priority;
        };
        dispatcher.Hooks.OperationStarted += counter;

        /* Send priority and promoted by the dispatcher on every pass, so it fires even when the queue is never empty. */
        var expiry = new DispatcherTimer(DispatcherPriority.Send, dispatcher) { Interval = limit };
        expiry.Tick += (_, _) => frame.Continue = false;
        try
        {
            dispatcher.BeginInvoke(DispatcherPriority.ApplicationIdle, new Action(() =>
            {
                idle = true;
                frame.Continue = false;
            }));
            expiry.Start();
            StaTestThread.Step("Settle: waiting in the frame for the dispatcher to reach application idle");
            Dispatcher.PushFrame(frame);
        }
        finally
        {
            expiry.Stop();
            dispatcher.Hooks.OperationStarted -= counter;
        }

        if (!idle)
        {
            throw new TimeoutException(
                $"The dispatcher did not reach application idle within {limit.TotalMilliseconds:0} ms: it ran {operations} operations meanwhile, the last at priority {lastPriority}. "
                + "A layout or a handler that queues work ahead of idle without end looks like this (#6216).");
        }

        StaTestThread.Step("Settle: the layout pass after the idle frame");
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
