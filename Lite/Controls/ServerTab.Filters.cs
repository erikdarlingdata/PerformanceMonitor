/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using PerformanceMonitor.Ui;
using static PerformanceMonitor.Ui.DataGridHelpers;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    /* ========== Column Filtering ========== */

    private void InitializeFilterManagers()
    {
        _querySnapshotsFilterMgr = new DataGridFilterManager<QuerySnapshotRow>(QuerySnapshotsGrid);
        _queryStatsFilterMgr = new DataGridFilterManager<QueryStatsRow>(QueryStatsGrid);
        _procStatsFilterMgr = new DataGridFilterManager<ProcedureStatsRow>(ProcedureStatsGrid);
        _queryStoreFilterMgr = new DataGridFilterManager<QueryStoreRow>(QueryStoreGrid);
        _planCorrectionFilterMgr = new DataGridFilterManager<PlanCorrectionRow>(PlanCorrectionGrid);
        _blockedProcessFilterMgr = new DataGridFilterManager<BlockedProcessReportRow>(BlockedProcessReportGrid);
        _deadlockFilterMgr = new DataGridFilterManager<DeadlockProcessDetail>(DeadlockGrid);
        _runningJobsFilterMgr = new DataGridFilterManager<RunningJobRow>(RunningJobsGrid);
        _longQueryFilterMgr = new DataGridFilterManager<LongQueryCompletionRow>(LongQueryCompletionsGrid);
        _serverConfigFilterMgr = new DataGridFilterManager<ServerConfigRow>(ServerConfigGrid);
        _databaseConfigFilterMgr = new DataGridFilterManager<DatabaseConfigRow>(DatabaseConfigGrid);
        _dbScopedConfigFilterMgr = new DataGridFilterManager<DatabaseScopedConfigRow>(DatabaseScopedConfigGrid);
        _queryStoreHealthFilterMgr = new DataGridFilterManager<QueryStoreHealthRow>(QueryStoreHealthGrid);
        _automaticTuningFilterMgr = new DataGridFilterManager<AutomaticTuningRow>(AutomaticTuningGrid);
        _traceFlagsFilterMgr = new DataGridFilterManager<TraceFlagRow>(TraceFlagsGrid);
        _collectionHealthFilterMgr = new DataGridFilterManager<CollectorHealthRow>(CollectionHealthGrid);
        _collectionLogFilterMgr = new DataGridFilterManager<CollectionLogRow>(CollectionLogGrid);
        _latchStatsFilterMgr = new DataGridFilterManager<LatchStatsSnapshotRow>(LatchStatsGrid);
        _spinlockStatsFilterMgr = new DataGridFilterManager<SpinlockStatsSnapshotRow>(SpinlockStatsGrid);
        /* System Events grids (the two chart sub-tabs have no grid, so register no filter manager). */
        _seSchedulerFilterMgr = new DataGridFilterManager<SchedulerIssueRow>(SchedulerIssuesGrid);
        _seSevereErrorFilterMgr = new DataGridFilterManager<SevereErrorRow>(SevereErrorsGrid);
        _seMemoryConditionsFilterMgr = new DataGridFilterManager<MemoryConditionsRow>(MemoryConditionsGrid);
        _seMemoryBrokerFilterMgr = new DataGridFilterManager<MemoryBrokerRow>(MemoryBrokerGrid);
        _seMemoryNodeOomFilterMgr = new DataGridFilterManager<MemoryNodeOomRow>(MemoryNodeOomGrid);
        _seSignificantWaitsFilterMgr = new DataGridFilterManager<SignificantWaitRow>(SignificantWaitsGrid);
        _seCpuTasksFilterMgr = new DataGridFilterManager<CpuTasksRow>(CpuTasksGrid);
        _seIoIssuesFilterMgr = new DataGridFilterManager<IoIssuesRow>(IoIssuesGrid);
        _seDefaultTraceFilterMgr = new DataGridFilterManager<DefaultTraceEventRow>(DefaultTraceGrid);
        /* Configuration Changes grids (one per sub-tab). */
        _serverConfigChangesFilterMgr = new DataGridFilterManager<ServerConfigChangeRow>(ServerConfigChangesGrid);
        _databaseConfigChangesFilterMgr = new DataGridFilterManager<DatabaseConfigChangeRow>(DatabaseConfigChangesGrid);
        _traceFlagChangesFilterMgr = new DataGridFilterManager<TraceFlagChangeRow>(TraceFlagChangesGrid);

        _filterManagers[QuerySnapshotsGrid] = _querySnapshotsFilterMgr;
        _filterManagers[QueryStatsGrid] = _queryStatsFilterMgr;
        _filterManagers[ProcedureStatsGrid] = _procStatsFilterMgr;
        _filterManagers[QueryStoreGrid] = _queryStoreFilterMgr;
        _filterManagers[PlanCorrectionGrid] = _planCorrectionFilterMgr;
        _filterManagers[BlockedProcessReportGrid] = _blockedProcessFilterMgr;
        _filterManagers[DeadlockGrid] = _deadlockFilterMgr;
        _filterManagers[RunningJobsGrid] = _runningJobsFilterMgr;
        _filterManagers[LongQueryCompletionsGrid] = _longQueryFilterMgr;
        _filterManagers[ServerConfigGrid] = _serverConfigFilterMgr;
        _filterManagers[DatabaseConfigGrid] = _databaseConfigFilterMgr;
        _filterManagers[DatabaseScopedConfigGrid] = _dbScopedConfigFilterMgr;
        _filterManagers[QueryStoreHealthGrid] = _queryStoreHealthFilterMgr;
        _filterManagers[AutomaticTuningGrid] = _automaticTuningFilterMgr;
        _filterManagers[TraceFlagsGrid] = _traceFlagsFilterMgr;
        _filterManagers[CollectionHealthGrid] = _collectionHealthFilterMgr;
        _filterManagers[CollectionLogGrid] = _collectionLogFilterMgr;
        _filterManagers[LatchStatsGrid] = _latchStatsFilterMgr;
        _filterManagers[SpinlockStatsGrid] = _spinlockStatsFilterMgr;
        _filterManagers[SchedulerIssuesGrid] = _seSchedulerFilterMgr;
        _filterManagers[SevereErrorsGrid] = _seSevereErrorFilterMgr;
        _filterManagers[MemoryConditionsGrid] = _seMemoryConditionsFilterMgr;
        _filterManagers[MemoryBrokerGrid] = _seMemoryBrokerFilterMgr;
        _filterManagers[MemoryNodeOomGrid] = _seMemoryNodeOomFilterMgr;
        _filterManagers[SignificantWaitsGrid] = _seSignificantWaitsFilterMgr;
        _filterManagers[CpuTasksGrid] = _seCpuTasksFilterMgr;
        _filterManagers[IoIssuesGrid] = _seIoIssuesFilterMgr;
        _filterManagers[DefaultTraceGrid] = _seDefaultTraceFilterMgr;
        _filterManagers[ServerConfigChangesGrid] = _serverConfigChangesFilterMgr;
        _filterManagers[DatabaseConfigChangesGrid] = _databaseConfigChangesFilterMgr;
        _filterManagers[TraceFlagChangesGrid] = _traceFlagChangesFilterMgr;
    }

    /* Host/apply plumbing lives in the shared Ui controller. Lazy (a field initializer can't reference the
       instance field _filterManagers); the XAML-wired FilterButton_Click forwards to it. */
    private ColumnFilterPopupController? _filterPopupControllerField;
    private ColumnFilterPopupController FilterPopupController => _filterPopupControllerField ??= new ColumnFilterPopupController(_filterManagers);

    private void FilterButton_Click(object sender, RoutedEventArgs e) => FilterPopupController.HandleFilterButtonClick(sender);
}
