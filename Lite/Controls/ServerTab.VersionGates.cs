/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows.Controls;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    /// <summary>What the Plan Corrections grid says when its window holds no rows on a server whose version has automatic tuning.</summary>
    internal const string PlanCorrectionsQuietWindowText = "No plan corrections in the selected time window.";

    /// <summary>
    /// The server's SQL Server major version, from the newest stored server_properties row (13 is SQL Server 2016). 0 until it is read,
    /// which the collector gates take as unknown and assume newest, so the grids keep their ordinary sentences until then.
    /// </summary>
    private int _sqlMajorVersion;

    /// <summary>The engine facts the plan_correction collector's gate reads, from the edition and version this tab holds.</summary>
    private CollectorTargetInfo PlanCorrectionTarget() => new()
    {
        IsAzureSqlDb = _engineEdition == CollectorEngineCapability.AzureSqlDatabaseEngineEdition,
        IsAzureManagedInstance = _engineEdition == CollectorEngineCapability.AzureManagedInstanceEngineEdition,
        SqlMajorVersion = _sqlMajorVersion,
    };

    /// <summary>
    /// Reads the server's major version from the store once, then words the two plan-correction grids for it. A store with no row, or a
    /// read that fails, leaves the version unknown, which is assumed newest: the grids keep the sentences they have.
    /// </summary>
    private async System.Threading.Tasks.Task RefreshSqlMajorVersionAsync()
    {
        if (_sqlMajorVersion != 0)
        {
            return;
        }

        try
        {
            _sqlMajorVersion = await System.Threading.Tasks.Task.Run(() => _dataService.GetSqlMajorVersionAsync(_serverId));
        }
        catch (Exception)
        {
            return;
        }

        ApplyPlanCorrectionEmptyTexts(PlanCorrectionTarget());
    }

    /// <summary>
    /// Words the Automatic Tuning and Plan Corrections grids' empty states for <paramref name="target"/>. Both grids read the plan_correction
    /// collector's table, so both take its own sentence when the server's version has no automatic tuning (before SQL Server 2017); the
    /// sentence is the one the collector and get_plan_corrections give, not a copy. The Automatic Tuning grid is a latest-snapshot read with
    /// no window, so on any other version an empty grid means no cycle has been stored, which is the collector's other sentence. The Plan
    /// Corrections grid windows on time, so there an empty grid on a version that has automatic tuning is a quiet window.
    /// </summary>
    private void ApplyPlanCorrectionEmptyTexts(CollectorTargetInfo target)
    {
        var collectorSentence = PlanCorrectionCollector.NoRowsMessage(target);
        EmptyState.SetText(AutomaticTuningGrid, collectorSentence);
        EmptyState.SetText(PlanCorrectionGrid, PlanCorrectionCollector.Instance.AppliesTo(target) ? PlanCorrectionsQuietWindowText : collectorSentence);
    }
}
