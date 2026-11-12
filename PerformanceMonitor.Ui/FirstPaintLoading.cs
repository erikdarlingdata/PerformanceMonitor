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

namespace PerformanceMonitor.Ui;

/// <summary>
/// #8647: what a sub-tab shows between the moment it is picked and the moment its first read returns. Before this, its charts
/// were blank axes, its grids a bare header row and its calendar a month of "No data" grey, for one to four seconds on a tab opened
/// a moment ago: a surface that has not been read says nothing about its window, so it must not look like one that was read and
/// found nothing.
///
/// <para><see cref="Show"/> walks the logical tree under a tab's content and gives each surface the loading look in the place its
/// empty-state sentence goes: a chart gets <see cref="ChartRenderHelper.RefreshLoadingChart"/> (the empty look with a pinned
/// "Loading..." sentence), a grid gets <see cref="EmptyState.ShowLoadingUntilRead"/> (the shared muted text, which goes away when a
/// read assigns rows, or none), and a <see cref="PerformanceCalendar"/> dims behind "Loading...". The read that follows replaces
/// each with rows, or with the empty-state sentence if it found none.</para>
///
/// <para><see cref="Resolve"/> runs when the pass that read the tab ends. A surface that is on screen and still loading was never
/// drawn on (its read failed, or its collector has nothing to say on this server), so it becomes the empty look or loses the
/// text: "Loading..." never outlives the read it waited for. A surface that is not on screen is left for the pass that reads it.</para>
///
/// <para>A surface is given the look once: a tab picked again keeps the rows it holds. A composite control with a first-paint
/// look of its own (the Overview lanes, the time-range slicers) is not entered.</para>
/// </summary>
public sealed class FirstPaintLoading
{
    private readonly ChartRenderHelper _charts;
    private readonly Action<ScottPlot.WPF.WpfPlot> _prepare;
    private readonly HashSet<ScottPlot.WPF.WpfPlot> _primedCharts = new();

    /// <param name="charts">The helper that draws the chart looks, the host's own, so its loading marks and its empty looks agree.</param>
    /// <param name="prepare">Styles a chart for the host's theme before the look is drawn on it.</param>
    public FirstPaintLoading(ChartRenderHelper charts, Action<ScottPlot.WPF.WpfPlot> prepare)
    {
        ArgumentNullException.ThrowIfNull(charts);
        ArgumentNullException.ThrowIfNull(prepare);
        _charts = charts;
        _prepare = prepare;
    }

    /// <summary>Gives every chart, grid and calendar under <paramref name="root"/> that has not had it the loading look.</summary>
    public void Show(DependencyObject? root) => Walk(root, show: true);

    /// <summary>Ends the loading look on every surface under <paramref name="root"/> that is on screen and no read drew on.</summary>
    public void Resolve(DependencyObject? root) => Walk(root, show: false);

    private void Walk(DependencyObject? node, bool show)
    {
        if (node is null)
        {
            return;
        }

        switch (node)
        {
            case ScottPlot.WPF.WpfPlot chart:
                if (show)
                {
                    ShowChart(chart);
                }
                else if (chart.IsVisible)
                {
                    _charts.ResolveLoadingChart(chart);
                }

                return;

            case DataGrid grid:
                /* #8728: a grid its own Visibility hides (the Queries sub-tabs' comparison grids sit in the same cell as the main grid and
                   are collapsed until a comparison is on) gets no "Loading..." and has any it carries ended. It has no rows to wait for and
                   it is never on screen, so the resolve pass below skipped it and its text stayed up over the main grid's rows. This tests
                   the grid's own Visibility, not IsVisible: a grid in a sub-tab not yet picked is not IsVisible but still gets its text. */
                if (show)
                {
                    if (grid.Visibility == Visibility.Visible)
                    {
                        EmptyState.ShowLoadingUntilRead(grid);
                    }
                }
                else if ((grid.IsVisible || grid.Visibility != Visibility.Visible) && EmptyState.IsShowingLoading(grid))
                {
                    EmptyState.ShowLoading(grid, false);
                }

                return;

            case PerformanceCalendar calendar:
                if (show)
                {
                    if (_primedCalendars.Add(calendar))
                    {
                        calendar.IsLoading = true;
                    }
                }
                else if (calendar.IsVisible)
                {
                    calendar.IsLoading = false;
                }

                return;

            case UserControl:
                /* A composite control draws its own first paint. */
                return;
        }

        foreach (var child in LogicalTreeHelper.GetChildren(node))
        {
            if (child is DependencyObject next)
            {
                Walk(next, show);
            }
        }
    }

    private readonly HashSet<PerformanceCalendar> _primedCalendars = new();

    private void ShowChart(ScottPlot.WPF.WpfPlot chart)
    {
        /* A chart a read already drew on keeps its data. */
        if (!_primedCharts.Add(chart) || (chart.Plot.PlottableList.Count > 0 && !_charts.IsShowingLoading(chart)))
        {
            return;
        }

        _prepare(chart);
        _charts.RefreshLoadingChart(chart, TitleOf(chart), "");
    }

    /// <summary>The title of the card a chart sits in, the name its empty look will use; empty when it sits in none.</summary>
    private static string TitleOf(DependencyObject chart)
    {
        for (var parent = LogicalTreeHelper.GetParent(chart); parent is not null; parent = LogicalTreeHelper.GetParent(parent))
        {
            if (parent is ChartPanel panel)
            {
                return panel.Title;
            }
        }

        return string.Empty;
    }
}
