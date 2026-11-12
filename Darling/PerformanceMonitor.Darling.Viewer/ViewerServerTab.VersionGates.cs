/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows.Controls;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

public partial class ViewerServerTab : UserControl
{
    /// <summary>What the Plan Corrections grid says when its window holds no rows on a server whose version has automatic tuning.</summary>
    internal const string PlanCorrectionsQuietWindowText = "No plan corrections in the selected time window.";

    /// <summary>
    /// The engine facts the plan_correction collector's gate reads, from the server's registry entry (the same facts the FinOps PVS grid
    /// and get_plan_corrections read). A registry entry with no version is unknown, which is assumed newest.
    /// </summary>
    private CollectorTargetInfo PlanCorrectionTarget() => new()
    {
        IsAzureSqlDb = _server.EngineEdition == CollectorEngineCapability.AzureSqlDatabaseEngineEdition,
        IsAzureManagedInstance = _server.EngineEdition == CollectorEngineCapability.AzureManagedInstanceEngineEdition,
        SqlMajorVersion = _server.SqlMajorVersion ?? 0,
    };

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
