/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The one place the shared instance event judges' answer becomes the engine's <see cref="InstanceEventInfo"/> (#8884). Both read adapters (Lite over
/// DuckDB, Darling over PostgreSQL) read the same <c>collector_state</c> rows and call this, so the mapping cannot drift between the two stores.
/// </summary>
public static class InstanceEventInfoMapper
{
    /// <summary>The shared judges' answer as the engine's types. Public so a test can plant rows without a store.</summary>
    public static InstanceEventInfo ToInstanceEventInfo(IReadOnlyList<BaselineDiscontinuities.StateRow> state)
    {
        ArgumentNullException.ThrowIfNull(state);

        var restart = InstanceEvents.JudgeRestart(state);
        var dumps = InstanceEvents.JudgeMemoryDumps(state);
        return new InstanceEventInfo(
            restart is { } r ? new InstanceRestartInfo(r.PreviousStartTime, r.NewStartTime, r.ObservedAtUtc) : null,
            dumps is { } d && d.Current.NewestUtc is { } newest
                ? new MemoryDumpInfo(
                    newest, d.Current.Count, d.Current.RecentCount, Math.Max(1, d.Current.Count - d.Previous.Count), d.ObservedAtUtc)
                : null);
    }
}
