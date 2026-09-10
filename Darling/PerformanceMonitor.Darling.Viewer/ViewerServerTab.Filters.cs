/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

// Column-filter wiring copied from Lite/Controls/ServerTab.Filters.cs for the copy-parity program
// (copy-don't-promote: the Darling viewer owns its own copy; Lite/Dashboard are untouched). Byte-
// identical popup plumbing; the manager set covers the four Configuration sub-grids, the Running Jobs
// grid (W1h), and the Collection Health + Collection Log grids (W1i) — the filterable viewer grids
// ported so far — and grows as later waves add their own grids. Also holds LoadConfigurationAsync,
// the Configuration tab's parallel latest-snapshot load
// that feeds the config managers (the Running Jobs load lives in ViewerServerTab.RunningJobs.cs).

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using PerformanceMonitor.Ui;
using static PerformanceMonitor.Ui.DataGridHelpers;

namespace PerformanceMonitor.Darling.Viewer;

public partial class ViewerServerTab : UserControl
{
    /* Grid -> its filter manager, keyed for the FilterButton_Click visual-tree walk. */
    private readonly Dictionary<DataGrid, IDataGridFilterManager> _filterManagers = new();

    private DataGridFilterManager<ServerConfigRow>? _serverConfigFilterMgr;
    private DataGridFilterManager<DatabaseConfigRow>? _databaseConfigFilterMgr;
    private DataGridFilterManager<DatabaseScopedConfigRow>? _dbScopedConfigFilterMgr;
    private DataGridFilterManager<QueryStoreHealthRow>? _queryStoreHealthFilterMgr;
    private DataGridFilterManager<AutomaticTuningRow>? _automaticTuningFilterMgr;
    private DataGridFilterManager<TraceFlagRow>? _traceFlagsFilterMgr;
    private DataGridFilterManager<RunningJobRow>? _runningJobsFilterMgr;
    private DataGridFilterManager<ViewerLongQueryRow>? _longQueryFilterMgr;
    private DataGridFilterManager<ViewerBlockedProcessRow>? _blockedProcessFilterMgr;
    private DataGridFilterManager<DeadlockProcessDetail>? _deadlockFilterMgr;
    private DataGridFilterManager<ViewerQuerySnapshotRow>? _querySnapshotsFilterMgr;
    private DataGridFilterManager<ViewerQueryStatsRow>? _queryStatsFilterMgr;
    private DataGridFilterManager<ViewerProcedureStatsRow>? _procStatsFilterMgr;
    private DataGridFilterManager<ViewerQueryStoreRow>? _queryStoreFilterMgr;
    private DataGridFilterManager<ViewerQueryStoreRegressionRow>? _queryStoreRegressionsFilterMgr;
    private DataGridFilterManager<PlanCorrectionRow>? _planCorrectionFilterMgr;
    private DataGridFilterManager<CollectorHealthRow>? _collectionHealthFilterMgr;
    private DataGridFilterManager<CollectionLogRow>? _collectionLogFilterMgr;
    private DataGridFilterManager<ServerConfigChangeRow>? _serverConfigChangesFilterMgr;
    private DataGridFilterManager<DatabaseConfigChangeRow>? _databaseConfigChangesFilterMgr;
    private DataGridFilterManager<TraceFlagChangeRow>? _traceFlagChangesFilterMgr;

    /* ========== Column Filtering ========== */

    private void InitializeFilterManagers()
    {
        _serverConfigFilterMgr = new DataGridFilterManager<ServerConfigRow>(ServerConfigGrid);
        _databaseConfigFilterMgr = new DataGridFilterManager<DatabaseConfigRow>(DatabaseConfigGrid);
        _dbScopedConfigFilterMgr = new DataGridFilterManager<DatabaseScopedConfigRow>(DatabaseScopedConfigGrid);
        _queryStoreHealthFilterMgr = new DataGridFilterManager<QueryStoreHealthRow>(QueryStoreHealthGrid);
        _automaticTuningFilterMgr = new DataGridFilterManager<AutomaticTuningRow>(AutomaticTuningGrid);
        _traceFlagsFilterMgr = new DataGridFilterManager<TraceFlagRow>(TraceFlagsGrid);
        _runningJobsFilterMgr = new DataGridFilterManager<RunningJobRow>(RunningJobsGrid);
        _longQueryFilterMgr = new DataGridFilterManager<ViewerLongQueryRow>(LongQueryCompletionsGrid);
        _blockedProcessFilterMgr = new DataGridFilterManager<ViewerBlockedProcessRow>(BlockedProcessReportGrid);
        _deadlockFilterMgr = new DataGridFilterManager<DeadlockProcessDetail>(DeadlockGrid);
        _querySnapshotsFilterMgr = new DataGridFilterManager<ViewerQuerySnapshotRow>(QuerySnapshotsGrid);
        _queryStatsFilterMgr = new DataGridFilterManager<ViewerQueryStatsRow>(QueryStatsGrid);
        _procStatsFilterMgr = new DataGridFilterManager<ViewerProcedureStatsRow>(ProcedureStatsGrid);
        _queryStoreFilterMgr = new DataGridFilterManager<ViewerQueryStoreRow>(QueryStoreGrid);
        _queryStoreRegressionsFilterMgr = new DataGridFilterManager<ViewerQueryStoreRegressionRow>(QueryStoreRegressionsGrid);
        _planCorrectionFilterMgr = new DataGridFilterManager<PlanCorrectionRow>(PlanCorrectionGrid);
        _collectionHealthFilterMgr = new DataGridFilterManager<CollectorHealthRow>(CollectionHealthGrid);
        _collectionLogFilterMgr = new DataGridFilterManager<CollectionLogRow>(CollectionLogGrid);
        _serverConfigChangesFilterMgr = new DataGridFilterManager<ServerConfigChangeRow>(ServerConfigChangesGrid);
        _databaseConfigChangesFilterMgr = new DataGridFilterManager<DatabaseConfigChangeRow>(DatabaseConfigChangesGrid);
        _traceFlagChangesFilterMgr = new DataGridFilterManager<TraceFlagChangeRow>(TraceFlagChangesGrid);

        _filterManagers[ServerConfigGrid] = _serverConfigFilterMgr;
        _filterManagers[DatabaseConfigGrid] = _databaseConfigFilterMgr;
        _filterManagers[DatabaseScopedConfigGrid] = _dbScopedConfigFilterMgr;
        _filterManagers[QueryStoreHealthGrid] = _queryStoreHealthFilterMgr;
        _filterManagers[AutomaticTuningGrid] = _automaticTuningFilterMgr;
        _filterManagers[TraceFlagsGrid] = _traceFlagsFilterMgr;
        _filterManagers[RunningJobsGrid] = _runningJobsFilterMgr;
        _filterManagers[LongQueryCompletionsGrid] = _longQueryFilterMgr;
        _filterManagers[BlockedProcessReportGrid] = _blockedProcessFilterMgr;
        _filterManagers[DeadlockGrid] = _deadlockFilterMgr;
        _filterManagers[QuerySnapshotsGrid] = _querySnapshotsFilterMgr;
        _filterManagers[QueryStatsGrid] = _queryStatsFilterMgr;
        _filterManagers[ProcedureStatsGrid] = _procStatsFilterMgr;
        _filterManagers[QueryStoreGrid] = _queryStoreFilterMgr;
        _filterManagers[QueryStoreRegressionsGrid] = _queryStoreRegressionsFilterMgr;
        _filterManagers[PlanCorrectionGrid] = _planCorrectionFilterMgr;
        _filterManagers[CollectionHealthGrid] = _collectionHealthFilterMgr;
        _filterManagers[CollectionLogGrid] = _collectionLogFilterMgr;
        _filterManagers[ServerConfigChangesGrid] = _serverConfigChangesFilterMgr;
        _filterManagers[DatabaseConfigChangesGrid] = _databaseConfigChangesFilterMgr;
        _filterManagers[TraceFlagChangesGrid] = _traceFlagChangesFilterMgr;
    }

    /// <summary>
    /// Configuration tab load: fires all five latest-snapshot reads concurrently (NpgsqlDataSource
    /// pools a connection for each), then hands each result to its filter manager's UpdateData so
    /// active column filters survive the refresh. Mirrors Lite's RefreshConfigurationAsync
    /// (Task.WhenAll of the five) — but the reads are genuinely async, so there is no Task.Run wrap.
    /// LoadInnerTabAsync owns the try/catch that surfaces failures on the status bar.
    /// </summary>
    private async Task LoadConfigurationAsync()
    {
        using var readFanOut = ViewerReadFanOut.Of(6);

        var serverConfigTask = _dataService.GetLatestServerConfigAsync(_server.ServerId);
        var databaseConfigTask = _dataService.GetLatestDatabaseConfigAsync(_server.ServerId, databaseNames: SelectedDatabaseFilter);
        var databaseScopedConfigTask = _dataService.GetLatestDatabaseScopedConfigAsync(_server.ServerId, databaseNames: SelectedDatabaseFilter);
        var queryStoreHealthTask = _dataService.GetLatestQueryStoreHealthAsync(_server.ServerId, databaseNames: SelectedDatabaseFilter);
        var automaticTuningTask = _dataService.GetLatestAutomaticTuningAsync(_server.ServerId, databaseNames: SelectedDatabaseFilter);
        var traceFlagsTask = _dataService.GetLatestTraceFlagsAsync(_server.ServerId);

        await Task.WhenAll(serverConfigTask, databaseConfigTask, databaseScopedConfigTask, queryStoreHealthTask, automaticTuningTask, traceFlagsTask);

        _serverConfigFilterMgr!.UpdateData(serverConfigTask.Result);
        _databaseConfigFilterMgr!.UpdateData(databaseConfigTask.Result);
        _dbScopedConfigFilterMgr!.UpdateData(databaseScopedConfigTask.Result);
        _queryStoreHealthFilterMgr!.UpdateData(queryStoreHealthTask.Result);
        _automaticTuningFilterMgr!.UpdateData(automaticTuningTask.Result);
        _traceFlagsFilterMgr!.UpdateData(traceFlagsTask.Result);
    }

    /* Host/apply plumbing lives in the shared Ui controller. Lazy (a field initializer can't reference the
       instance field _filterManagers); the XAML-wired FilterButton_Click forwards to it. */
    private ColumnFilterPopupController? _filterPopupControllerField;
    private ColumnFilterPopupController FilterPopupController => _filterPopupControllerField ??= new ColumnFilterPopupController(_filterManagers);

    private void FilterButton_Click(object sender, RoutedEventArgs e) => FilterPopupController.HandleFilterButtonClick(sender);
}
