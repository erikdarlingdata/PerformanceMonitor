/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #8789: the collector's write of one cycle's over-cap sightings, with the trim's limits filled in. The write leaves out the new
/// rows the sweep's trim would drop in the same pass, so it needs the pending cap and the per-pass drop limit the trim uses
/// (<see cref="OversizedPlanBacklogSweep.PendingRowsPerServerCap"/>, <see cref="OversizedPlanBacklogSweep.MaxRowsDroppedPerServerPerTick"/>).
/// This is the only place that reads them for the collector, so the collector runner stays free of the sweep: it records sightings
/// and never fetches.
/// </summary>
internal static class OversizedPlanBacklogIntake
{
    /// <summary>See <see cref="OversizedPlanBacklog.RecordSightingsAsync"/>. Never throws, short of a shutdown.</summary>
    internal static Task<int> RecordAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string collectorName,
        IReadOnlyList<OversizedPlanObservation> observations,
        DateTime sightingUtc,
        ILogger? logger,
        CancellationToken cancellationToken) =>
        OversizedPlanBacklog.RecordSightingsAsync(
            postgres,
            serverId,
            collectorName,
            observations,
            sightingUtc,
            OversizedPlanBacklogSweep.PendingRowsPerServerCap,
            OversizedPlanBacklogSweep.MaxRowsDroppedPerServerPerTick,
            logger,
            cancellationToken);
}
