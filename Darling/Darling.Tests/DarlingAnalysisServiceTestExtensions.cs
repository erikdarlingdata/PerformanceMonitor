/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Analysis;

namespace Darling.Tests;

/// <summary>
/// Test setup for <see cref="DarlingAnalysisService"/> (#9966).
/// </summary>
internal static class DarlingAnalysisServiceTestExtensions
{
    /// <summary>
    /// Runs the service's on-demand passes with no family, detector or drill-down time budget. A test that is not about
    /// the stage budgets uses this so a pass that reads the clock (the default budgets count from the start of the pass)
    /// finishes every stage whatever the runner's speed: a stage a budget stops drops its facts, anomalies or drill-downs,
    /// adds a left-out caveat and stores no findings, and a baseline a stopped detector was computing is stored by no one.
    /// A test of the budgets themselves sets them explicitly and does not call this.
    /// </summary>
    internal static DarlingAnalysisService WithNoStageBudgets(this DarlingAnalysisService service)
    {
        service.OnDemandFamilyBudget = null;
        service.OnDemandDetectorBudget = null;
        service.OnDemandDrillDownBudget = null;
        return service;
    }
}
