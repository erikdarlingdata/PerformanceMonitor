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
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// How often a collector runs on one server, for the sentences a tool writes about it ("this collector runs hourly, so a server
/// registered in the last hour has not reached its first collection"). The cadence is the effective one: the server's own
/// schedule override, else the fleet-wide one, else the shipped default, through the same
/// <see cref="CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes"/> the health reads use, so a tool never states the
/// shipped default as fact for a server whose schedule was changed.
/// </summary>
internal static class DarlingCollectorCadence
{
    /// <summary>The effective interval in minutes. <paramref name="fallbackMinutes"/> answers only for a collector the shipped
    /// schedule does not know or one with no recurring cadence. Pure, so a test applies a set of overrides without a store.</summary>
    internal static int Minutes(string collector, int serverId, IReadOnlyList<ScheduleOverride> overrides, int fallbackMinutes)
    {
        var minutes = CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes(collector, serverId, overrides);
        return minutes is > 0 ? minutes.Value : fallbackMinutes;
    }

    /// <summary>The effective interval read from the store's schedule rows. A failed read keeps the shipped cadence
    /// (<see cref="DarlingDataReader.ReadScheduleOverridesAsync"/> logs it and returns no overrides).</summary>
    internal static async Task<int> MinutesAsync(
        NpgsqlDataSource postgres, string collector, int serverId, int fallbackMinutes, CancellationToken cancellationToken)
    {
        var overrides = await DarlingDataReader.ReadScheduleOverridesAsync(postgres, serverId, cancellationToken);
        return Minutes(collector, serverId, overrides, fallbackMinutes);
    }
}
