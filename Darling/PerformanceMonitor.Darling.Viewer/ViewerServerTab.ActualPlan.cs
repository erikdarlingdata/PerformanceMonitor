/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using static PerformanceMonitor.Ui.DataGridHelpers;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Top Queries "Get Actual Plan" entry point — the side-effecting sibling of "Fetch Live Plan" (see
/// <see cref="ViewerServerTab.LivePlan.cs"/>). Where Fetch Live Plan READS a cached plan, this RE-EXECUTES the
/// stored query on the target so the plan comes back with runtime statistics. The viewer never touches SQL
/// Server: it enqueues an <c>execute_actual_plan</c> command (identifier only —
/// <see cref="ViewerDataService.BuildActualPlanArgs"/>) and the SERVICE resolves the text from its store and
/// re-executes. All the shared behavior — the read-only guard, the data-modification consent flag, and the
/// permission/timeout/no-plan messaging — lives in <see cref="ViewerActualPlanFlow"/>, reused by every surface
/// that hosts "Get Actual Plan"; this method only supplies the Top-Queries identifier + the tab's loading/host.
/// </summary>
public partial class ViewerServerTab
{
    /// <summary>
    /// "Get Actual Plan" on a Top Queries row — re-executes the stored query on the target (via the service) to
    /// capture its ACTUAL plan, opening it in the Plan Viewer tab. Gated in XAML on the row's
    /// <c>CanGetActualPlan</c>, so it only fires for a Top Queries (<see cref="ViewerQueryStatsRow"/>) row.
    /// </summary>
    private async void GetActualQueryPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem is not ViewerQueryStatsRow stats || string.IsNullOrEmpty(stats.QueryHash)) return;

        var label = $"Actual Plan - {stats.QueryHash}";

        /* #5639: an empty or all-zero hash names no particular query. The row's text can differ from the latest
           stored text the SERVICE runs, so the flow reads that text (same SQL and database as the service); the
           prompt, the modification check and the digest in the request all use it. */
        if (!ViewerActualPlanFlow.TryCheckQueryStatsHash(Window.GetWindow(this)!, stats.QueryHash))
        {
            return;
        }

        /* The Plan Viewer overlay's Cancel button cancels this token (shared with Fetch Live Plan). */
        _planLoadCts?.Dispose();
        _planLoadCts = new CancellationTokenSource();

        var capture = await ViewerActualPlanFlow.RequestActualPlanAsync(
            Window.GetWindow(this)!, _dataService, _server.ServerId, _server.DisplayName, stats.DatabaseName,
            queryText: null, fetchPlanXml: null, argsJson: "",
            onStarted: () =>
            {
                ShowPlanLoading(label);
                PlanLoadingLabel.Text = $"Capturing actual plan (re-executing): {label}";
            },
            onFinished: null,
            _planLoadCts.Token,
            queryStatsHash: stats.QueryHash);

        if (capture != null)
        {
            /* The plan tab gets the statement that ran, not the grid row's text. */
            _ = OpenPlanTab(capture.PlanXml, label, capture.QueryText); /* HidePlanLoading runs inside OpenPlanTab */
        }
        else
        {
            HidePlanLoading();
        }
    }

    /// <summary>
    /// "Get Actual Plan (re-run)" for the Query Store + Query Store Regressions grids. Both rows carry a
    /// QueryId; Regression rows aggregate across plans so they have no single PlanId or stored plan to view,
    /// so this RE-EXECUTES by QueryId via the same service path the Query Store history window uses
    /// (<see cref="ViewerDataService.BuildActualPlanArgsForQueryStore"/>). Gated per-row on CanGetActualPlan.
    /// </summary>
    private async void GetActualQueryStorePlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);

        long queryId;
        string databaseName;
        string queryText;
        long planId;
        switch (grid?.CurrentItem)
        {
            case ViewerQueryStoreRow qs when qs.QueryId != 0:
                queryId = qs.QueryId; databaseName = qs.DatabaseName; queryText = qs.QueryText; planId = qs.PlanId;
                break;
            case ViewerQueryStoreRegressionRow reg when reg.QueryId != 0:
                queryId = reg.QueryId; databaseName = reg.DatabaseName; queryText = reg.QueryTextSample; planId = 0;
                break;
            default:
                return;
        }

        var label = $"Actual Plan - QS {queryId}";

        /* Best-effort stored ESTIMATED plan for modification detection; null degrades to the fail-safe
           "treat as modifying" consent (Regression rows have no single PlanId, so they take that path). */
        var argsJson = ViewerDataService.BuildActualPlanArgsForQueryStore(queryId, databaseName);

        /* The Plan Viewer overlay's Cancel button cancels this token (shared with Fetch Live Plan). */
        _planLoadCts?.Dispose();
        _planLoadCts = new CancellationTokenSource();

        var capture = await ViewerActualPlanFlow.RequestActualPlanAsync(
            Window.GetWindow(this)!, _dataService, _server.ServerId, _server.DisplayName, databaseName,
            queryText,
            () => _dataService.GetQueryStorePlanTextAsync(_server.ServerId, databaseName, queryId, planId), /* #5639: read before the prompt; a failed read counts as no plan */
            argsJson,
            onStarted: () =>
            {
                ShowPlanLoading(label);
                PlanLoadingLabel.Text = $"Capturing actual plan (re-executing): {label}";
            },
            onFinished: null,
            _planLoadCts.Token);

        if (capture != null)
        {
            _ = OpenPlanTab(capture.PlanXml, label, capture.QueryText); /* HidePlanLoading runs inside OpenPlanTab */
        }
        else
        {
            HidePlanLoading();
        }
    }
}
