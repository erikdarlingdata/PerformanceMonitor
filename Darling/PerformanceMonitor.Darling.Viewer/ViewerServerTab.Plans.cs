/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Microsoft.Win32;
using PerformanceMonitor.Ui;
using static PerformanceMonitor.Ui.FileSaveHelper;
using static PerformanceMonitor.Ui.DataGridHelpers;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Plan Viewer host (headless-plan wave), copied from Lite's <c>ServerTab.Plans.cs</c>: hosts the
/// shared <see cref="PlanViewerControl"/> as closable sub-tabs inside the "Plan Viewer" inner tab. The
/// single seam-difference from Lite is the plan SOURCE — Lite fetches plans live from the monitored
/// server's plan cache (or its DuckDB cache); the viewer never touches SQL Server, so it reads the plan
/// text the Darling service already stored in Postgres (<see cref="ViewerDataService.GetQueryStatsPlanXmlAsync"/>
/// / <see cref="ViewerDataService.GetQueryStorePlanTextAsync"/>). Consequently Lite's "Get Actual Plan"
/// (live execution) has no viewer equivalent and is omitted everywhere; only stored plans are shown.
///
/// <para><see cref="OpenPlanTab"/> is the shared entry point every "View Plan" surface calls: the keyed
/// Top Queries / Query Store / Top Procedures context menus here (fetched on click by
/// <see cref="ViewPlan_Click"/>), the FinOps High Impact / Expensive Queries menus, the Active Queries
/// snapshot grid, and the blocked-process / deadlock grids — the last three already carry their plan XML
/// in-row. It parses the plan off the UI thread, adds a closable sub-tab, and focuses the host.</para>
/// </summary>
public partial class ViewerServerTab
{
    /* Cancels a slow stored-plan fetch/parse (a large TOASTed plan over the wire) — the CancelPlanButton
       in the loading overlay drives it. There is no live-execution path to cancel, unlike Lite. */
    private CancellationTokenSource? _planLoadCts;

    /// <summary>
    /// Opens a plan as a closable sub-tab in the Plan Viewer host (Lite's <c>OpenPlanTab</c> verbatim, plus
    /// focusing the host tab so callers don't each have to). <see cref="PlanViewerControl.LoadPlan"/> parses
    /// + analyzes off the UI thread and throws <see cref="System.Xml.XmlException"/> for malformed plan XML.
    /// This is the hook the parallel Queries wave calls for its Active Queries snapshots
    /// (<c>OpenPlanTab(planXml, label, queryText)</c>).
    /// </summary>
    // Returns a Task (not async void) for signature parity with Lite's OpenPlanTab (#2870): the Ctrl+V paste
    // guard there awaits the load so it spans the off-thread parse, and this twin stays identical. This viewer
    // tab has no paste caller today; every "View Plan" caller here discards the Task (CS4014 is an error).
    private async Task OpenPlanTab(string planXml, string label, string? queryText = null)
    {
        HidePlanLoading();
        var viewer = new PlanViewerControl();
        try
        {
            await viewer.LoadPlan(planXml, label, queryText);
        }
        catch (System.Xml.XmlException ex)
        {
            viewer.Cleanup();
            MessageBox.Show(
                $"The plan XML is not valid:\n\n{ex.Message}",
                "Invalid Plan XML",
                MessageBoxButton.OK,
                MessageBoxImage.Warning);
            return;
        }
        catch (Exception ex)
        {
            viewer.Cleanup();
            MessageBox.Show(
                $"Failed to load the execution plan:\n\n{ex.Message}",
                "Plan Load Error",
                MessageBoxButton.OK,
                MessageBoxImage.Warning);
            return;
        }

        var header = new StackPanel { Orientation = System.Windows.Controls.Orientation.Horizontal };
        header.Children.Add(new TextBlock
        {
            Text = label.Length > 30 ? label[..30] + "…" : label,
            VerticalAlignment = System.Windows.VerticalAlignment.Center,
            ToolTip = label
        });
        var closeBtn = new Button
        {
            Style = (Style)FindResource("TabCloseButton")
        };
        header.Children.Add(closeBtn);

        var tab = new TabItem { Header = header, Content = viewer };
        closeBtn.Tag = tab;
        closeBtn.Click += ClosePlanTab_Click;

        PlanTabControl.Items.Add(tab);
        PlanTabControl.SelectedItem = tab;
        PlanEmptyState.Visibility = Visibility.Collapsed;
        PlanTabControl.Visibility = Visibility.Visible;
        PlanViewerTabItem.IsSelected = true;
    }

    private void ClosePlanTab_Click(object sender, RoutedEventArgs e)
    {
        if (sender is Button btn && btn.Tag is TabItem tab)
        {
            (tab.Content as PlanViewerControl)?.Cleanup();
            PlanTabControl.Items.Remove(tab);
            if (PlanTabControl.Items.Count == 0)
            {
                PlanTabControl.Visibility = Visibility.Collapsed;
                PlanEmptyState.Visibility = Visibility.Visible;
            }
        }
    }

    private void ShowPlanLoading(string label)
    {
        PlanLoadingLabel.Text = $"Loading plan: {label}";
        PlanEmptyState.Visibility = Visibility.Collapsed;
        PlanTabControl.Visibility = Visibility.Collapsed;
        PlanLoadingState.Visibility = Visibility.Visible;
        PlanViewerTabItem.IsSelected = true;
    }

    private void HidePlanLoading()
    {
        PlanLoadingState.Visibility = Visibility.Collapsed;
        if (PlanTabControl.Items.Count > 0)
            PlanTabControl.Visibility = Visibility.Visible;
        else
            PlanEmptyState.Visibility = Visibility.Visible;
    }

    private void CancelPlanButton_Click(object sender, RoutedEventArgs e) => _planLoadCts?.Cancel();

    /// <summary>
    /// The "View Plan" context-menu handler for the Top Queries + Query Store + Top Procedures grids:
    /// fetches the stored plan by the row's key columns (behind the loading overlay), then opens it via
    /// <see cref="OpenPlanTab"/>. Top Procedures now reaches here too — the procedure_stats collector DOES
    /// store the plan (query_plan_xml, cleanly captured from the cached plan since #1368 / migration V7,
    /// gated on the host's CapturePlanXml — Darling sets it, Lite writes NULL); it is fetched by object
    /// identity, exactly like the query-stats key. Comparison grids stay plan-less (a current-vs-baseline
    /// aggregate has no single stored plan keyed on the row).
    /// </summary>
    private async void ViewPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem == null) return;

        string label;
        string? queryText;
        Func<CancellationToken, Task<string?>> fetch;

        switch (grid.CurrentItem)
        {
            case ViewerQueryStatsRow stats:
                if (string.IsNullOrEmpty(stats.QueryHash)) return;
                label = $"Stored Plan - {stats.QueryHash}";
                queryText = stats.QueryText;
                fetch = ct => _dataService.GetQueryStatsPlanXmlAsync(_server.ServerId, stats.DatabaseName, stats.QueryHash, ct);
                break;
            case ViewerQueryStoreRow qs:
                label = $"Stored Plan - QS {qs.QueryId}";
                queryText = qs.QueryText;
                fetch = ct => _dataService.GetQueryStorePlanTextAsync(_server.ServerId, qs.DatabaseName, qs.QueryId, qs.PlanId, ct);
                break;
            case ViewerProcedureStatsRow proc:
                if (string.IsNullOrEmpty(proc.ObjectName)) return;
                label = $"Stored Plan - {proc.FullName}";
                queryText = null; /* the procedure grid carries no statement text; the plan XML holds its own */
                fetch = ct => _dataService.GetProcedureStatsPlanXmlAsync(_server.ServerId, proc.DatabaseName, proc.SchemaName, proc.ObjectName, ct);
                break;
            default:
                /* Comparison items / anything else: no stored plan keyed on the row — deferred. */
                return;
        }

        ShowPlanLoading(label);
        _planLoadCts?.Dispose();
        _planLoadCts = new CancellationTokenSource();

        try
        {
            var planXml = await fetch(_planLoadCts.Token);
            if (string.IsNullOrEmpty(planXml))
            {
                HidePlanLoading();
                MessageBox.Show(
                    "No execution plan was captured for this row. The plan may not have been collected yet, " +
                    "or it aged out of the store's retention window.",
                    "No Plan Available",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            _ = OpenPlanTab(planXml, label, queryText);
        }
        catch (OperationCanceledException)
        {
            HidePlanLoading();
        }
        catch (Exception ex)
        {
            HidePlanLoading();
            MessageBox.Show(
                $"Failed to load the execution plan:\n\n{ex.Message}",
                "Plan Load Error",
                MessageBoxButton.OK,
                MessageBoxImage.Error);
        }
    }

    /// <summary>
    /// The Top Queries grid's "Query Plan" Download button (mirrors Lite's <c>DownloadQueryStatsPlan_Click</c>):
    /// reads the stored query_plan_xml for the row and saves it as a .sqlplan file. The button is gated on
    /// <see cref="ViewerQueryStatsRow.HasQueryPlan"/> in XAML, so it only fires when a plan was captured.
    /// </summary>
    private async void DownloadQueryStatsPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || btn.DataContext is not ViewerQueryStatsRow row) return;
        if (string.IsNullOrEmpty(row.QueryHash)) return;

        btn.Content = "...";
        try
        {
            var plan = await _dataService.GetQueryStatsPlanXmlAsync(_server.ServerId, row.DatabaseName, row.QueryHash);
            if (string.IsNullOrEmpty(plan))
            {
                MessageBox.Show(
                    "No execution plan was captured for this query.",
                    "Plan Not Found",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            SavePlanFile(plan, $"QueryPlan_{row.QueryHash}");
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to retrieve plan: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            btn.Content = "Download";
        }
    }

    /// <summary>
    /// #1980: the Query Store grid's inline "View" button — the capability its two sibling grids
    /// always had. Wired to the SAME stored-plan path the history window's View Plan uses
    /// (<c>GetQueryStorePlanTextAsync</c> by the row's database/query_id/plan_id), opened in the
    /// Plan Viewer host via <see cref="OpenPlanTab"/>. No live-server hit; a row whose plan was
    /// never captured gets the same "Plan Not Found" message the sibling buttons show.
    /// </summary>
    private async void ViewQueryStorePlanInline_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || btn.DataContext is not ViewerQueryStoreRow row) return;
        if (row.QueryId <= 0) return;

        btn.Content = "...";
        try
        {
            var plan = await _dataService.GetQueryStorePlanTextAsync(_server.ServerId, row.DatabaseName, row.QueryId, row.PlanId);
            if (string.IsNullOrEmpty(plan))
            {
                MessageBox.Show(
                    "No execution plan was captured for this Query Store plan.",
                    "Plan Not Found",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            _ = OpenPlanTab(plan, $"QS Plan - Q{row.QueryId}/P{row.PlanId}", row.QueryText);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to retrieve plan: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            btn.Content = "View";
        }
    }

    /// <summary>
    /// The Top Procedures grid's "Query Plan" Download button (mirrors <see cref="DownloadQueryStatsPlan_Click"/>
    /// and Lite's <c>DownloadProcedurePlan_Click</c>): reads the stored procedure_stats.query_plan_xml for the
    /// row's object identity and saves it as a .sqlplan file. Gated on
    /// <see cref="ViewerProcedureStatsRow.HasQueryPlan"/> in XAML, so it only fires when a plan was captured.
    /// </summary>
    private async void DownloadProcedureStatsPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || btn.DataContext is not ViewerProcedureStatsRow row) return;
        if (string.IsNullOrEmpty(row.ObjectName)) return;

        btn.Content = "...";
        try
        {
            var plan = await _dataService.GetProcedureStatsPlanXmlAsync(_server.ServerId, row.DatabaseName, row.SchemaName, row.ObjectName);
            if (string.IsNullOrEmpty(plan))
            {
                MessageBox.Show(
                    "No execution plan was captured for this procedure.",
                    "Plan Not Found",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            SavePlanFile(plan, $"ProcedurePlan_{row.FullName}");
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to retrieve plan: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            btn.Content = "Download";
        }
    }

    /// <summary>
    /// Cleans up every open PlanViewerControl when the server tab closes (mirrors Lite's DisposeChartHelpers
    /// plan cleanup): each PlanViewerControl subscribes the static ThemeManager.ThemeChanged event, so a
    /// closed server tab with plan tabs still open would leak them — <see cref="ClosePlanTab_Click"/> only
    /// covers the per-tab close-button path. Forwarded from <c>Dispose()</c>.
    /// </summary>
    private void DisposePlanHelpers()
    {
        _planLoadCts?.Cancel();
        _planLoadCts?.Dispose();
        foreach (var item in PlanTabControl.Items)
        {
            if (item is TabItem { Content: PlanViewerControl pv })
            {
                pv.Cleanup();
            }
        }
    }
}
