/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>Column selectivity (#8798), the <c>column_stats</c> run of one server. A stub until the capture body lands.</summary>
public static class DarlingColumnStatsCapture
{
    public static Task<CollectorRunResult> RunAsync(DarlingCollectorRunner runner, ServerRuntime server, CancellationToken ct) =>
        Task.FromResult(new CollectorRunResult(0, 0, 0, CollectorContext.NoMeasurements, "not implemented"));
}
