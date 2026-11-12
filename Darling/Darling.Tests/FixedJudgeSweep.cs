/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;

namespace Darling.Tests;

/// <summary>
/// The statement sweep's budget for a test that maps the web routes with <c>MapAll</c> and compares a route's body with its
/// tool's (#7855, the #6341 pattern). The route sweeps its body with the shared judge's wall-clock budget; the tool called
/// directly is not swept. On a CPU-starved run the real judge can run out of time, withhold values, and still answer 200, so
/// the two bodies differ for reasons that have nothing to do with the route. A judge that always answers clean, on a clock
/// that never advances, makes the sweep a pass-through. A test about the filter itself must not use this.
/// </summary>
internal static class FixedJudgeSweep
{
    /// <summary>A budget over a fixed judge that never runs out.</summary>
    public static SensitiveStatements.JudgeBudget Create() =>
        new(SensitiveStatements.ReadBudget, _ => SensitiveStatements.Verdict.Clean, () => TimeSpan.Zero);

    /// <summary>Call before <c>DarlingWebEndpoints.MapAll</c>: the next <c>MapAll</c> in this async flow sweeps its
    /// <c>/api/read/*</c> bodies with <see cref="Create"/>.</summary>
    public static void UseForNextMapAll() => DarlingWebEndpoints.TestOnlyReadSweepBudget.Value = Create();
}
