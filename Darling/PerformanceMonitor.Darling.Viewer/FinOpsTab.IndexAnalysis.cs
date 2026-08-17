/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// FinOps Index Analysis sub-tab (Stage 3) — the monitor-side reproduction of Erik's sp_IndexCleanup, run in
/// the viewer over the latest collected <c>v_index_object_stats</c> snapshot (parse-on-read, like the System
/// Events tab; NO live proc, NO "install the proc" prompt). <see cref="ViewerDataService.GetIndexAnalysisAsync"/>
/// reads + maps the snapshot, derives the edition/uptime options from <c>server_properties</c>, and runs the
/// pure Common <c>IndexCleanupAnalyzer</c> off the UI thread. This partial projects the result into the
/// reclaimable-space rollup grid (overall + per database) and the recommendations detail grid (DISABLE / MAKE
/// UNIQUE / MERGE / DROP CONSTRAINT / COMPRESS / REVIEW — REVIEW rows carry no destructive script), and surfaces
/// the analyzer's <c>UptimeWarning</c> / <c>DedupeOnlyApplied</c> flags + <c>Notes</c> (its stated limitations)
/// as banners. Load-on-view + a Refresh button, mirroring the other FinOps sub-tabs.
/// </summary>
public partial class FinOpsTab
{
    /// <summary>
    /// Runs the analysis over the latest collected snapshot and repaints the rollup + detail grids and the
    /// banners. When no index statistics have been collected yet (no rollups), shows the empty hint and clears
    /// the grids/banners.
    /// </summary>
    private async Task LoadFinOpsIndexAnalysisAsync()
    {
        var result = await _dataService.GetIndexAnalysisAsync(_server.ServerId);

        var nothingCollected = result.DatabaseRollups.Count == 0;

        if (nothingCollected)
        {
            _finopsIndexAnalysisRollupFilterMgr!.UpdateData(new List<IndexCleanupRollupRow>());
            _finopsIndexAnalysisFilterMgr!.UpdateData(new List<IndexCleanupRecommendationRow>());
            FinOpsIndexAnalysisBannerPanel.Children.Clear();
            FinOpsIndexAnalysisBannerScroller.Visibility = Visibility.Collapsed;
            FinOpsIndexAnalysisNoDataMessage.Visibility = Visibility.Visible;
            FinOpsIndexAnalysisCountIndicator.Text = "";
            return;
        }

        var recs = ViewerDataService.ProjectRecommendations(result);
        var rollups = ViewerDataService.ProjectRollups(result);

        _finopsIndexAnalysisRollupFilterMgr!.UpdateData(rollups);
        _finopsIndexAnalysisFilterMgr!.UpdateData(recs);

        BuildIndexAnalysisBanners(result);

        FinOpsIndexAnalysisNoDataMessage.Visibility = Visibility.Collapsed;
        FinOpsIndexAnalysisCountIndicator.Text = recs.Count > 0
            ? $"{recs.Count} recommendation(s)"
            : "no cleanup recommendations — indexes look clean";
    }

    /// <summary>Rebuilds the banner strip: the &lt;14-day uptime caveat, the dedupe-only notice, and every analyzer Note (its stated limitations).</summary>
    private void BuildIndexAnalysisBanners(IndexCleanupAnalysisResult result)
    {
        FinOpsIndexAnalysisBannerPanel.Children.Clear();

        if (result.UptimeWarning)
        {
            AddIndexAnalysisBanner(
                "Server uptime is under 14 days — index usage data may be incomplete, so some \"Unused Index\" findings could be premature.",
                "#F39C12", "#33290F");
        }

        if (result.DedupeOnlyApplied)
        {
            AddIndexAnalysisBanner(
                "Dedupe-only mode is in effect — unused indexes were NOT flagged (server uptime is 7 days or less). Only duplicate/consolidation and compression findings are shown.",
                "#3498DB", "#0F2433");
        }

        foreach (var note in result.Notes)
        {
            AddIndexAnalysisBanner(note, "#7F8C8D", "#202628");
        }

        /* #2300: visibility lives on the scroller — the element that occupies the layout row. The panel
           inside it stays visible; collapsing only the panel would leave an empty scroller holding the row. */
        FinOpsIndexAnalysisBannerScroller.Visibility =
            FinOpsIndexAnalysisBannerPanel.Children.Count > 0 ? Visibility.Visible : Visibility.Collapsed;
    }

    /// <summary>Adds one left-accented banner row to the Index Analysis banner strip.</summary>
    private void AddIndexAnalysisBanner(string text, string accentHex, string backgroundHex)
    {
        var border = new Border
        {
            Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString(backgroundHex)),
            BorderBrush = new SolidColorBrush((Color)ColorConverter.ConvertFromString(accentHex)),
            BorderThickness = new Thickness(3, 0, 0, 0),
            CornerRadius = new CornerRadius(2),
            Margin = new Thickness(0, 0, 0, 4),
            Padding = new Thickness(8, 5, 8, 5),
            Child = new TextBlock
            {
                Text = text,
                TextWrapping = TextWrapping.Wrap,
                FontSize = 12,
                Foreground = new SolidColorBrush(Color.FromRgb(0xE0, 0xE0, 0xE0)),
            },
        };

        FinOpsIndexAnalysisBannerPanel.Children.Add(border);
    }

    /// <summary>Refresh button — re-runs the analysis over the latest snapshot with the shell's status-bar error surfacing.</summary>
    private async void FinOpsRefreshIndexAnalysis_Click(object sender, RoutedEventArgs e)
        => await RunFinOpsLoad(LoadFinOpsIndexAnalysisAsync);
}
