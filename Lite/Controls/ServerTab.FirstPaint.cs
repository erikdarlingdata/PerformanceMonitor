/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    /// <summary>The Collection Cycles card's line until its read returns (#8647).</summary>
    internal const string SkippedCyclesUnreadText = EmptyState.LoadingText;

    /// <summary>
    /// #8647: the sub-tab just picked shows "Loading..." where its empty-state sentence goes, on every chart, grid and the Daily
    /// Summary calendar in it that no read has filled, and on the Collection Cycles card. The pass that reads the tab replaces each
    /// one; <see cref="ResolveFirstPaintLoading"/> ends whatever that pass left.
    /// </summary>
    private void ShowFirstPaintLoading(TabItem? picked)
    {
        if (picked?.Content is not DependencyObject content)
        {
            return;
        }

        FirstPaint.Show(content);

        /* The card's line is always shown above its grid, so it holds the same word instead of an empty band. */
        if (IsUnder(SkippedCyclesSummary, content) && string.IsNullOrEmpty(SkippedCyclesSummary.Text))
        {
            SkippedCyclesSummary.Text = SkippedCyclesUnreadText;
        }
    }

    /// <summary>
    /// #8647: the pass that read the picked tab has ended without being superseded. A surface still saying "Loading..." was never
    /// filled (its read failed, or its collector has nothing to say on this server), so it gets the empty look and the card line
    /// says it could not be read: "Loading..." never outlives the read it waited for.
    /// </summary>
    private void ResolveFirstPaintLoading()
    {
        if (MainTabControl.SelectedItem is not TabItem { Content: DependencyObject content })
        {
            return;
        }

        FirstPaint.Resolve(content);

        if (SkippedCyclesSummary.IsVisible && SkippedCyclesSummary.Text == SkippedCyclesUnreadText)
        {
            SkippedCyclesSummary.Text = "Skipped cycles in 24 h: could not be read";
        }
    }

    private static bool IsUnder(DependencyObject node, DependencyObject root)
    {
        for (DependencyObject? current = node; current is not null; current = LogicalTreeHelper.GetParent(current))
        {
            if (ReferenceEquals(current, root))
            {
                return true;
            }
        }

        return false;
    }
}
