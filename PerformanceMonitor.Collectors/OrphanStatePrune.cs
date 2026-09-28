/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Collectors;

/// <summary>#4660: how often the orphaned per-database state prune runs for one server. The prune is hygiene (nothing
/// reads an orphaned key; a late delete costs at most one plan-XML refetch) and its answer changes only when a
/// database leaves the database_states snapshot, so once an hour is enough. The first cycle after a start still
/// prunes, and a failed prune is retried on the next cycle because only a success is recorded.</summary>
public static class OrphanStatePrune
{
    public static readonly TimeSpan Interval = TimeSpan.FromHours(1);

    public static bool IsDue(DateTime? lastSucceededUtc, DateTime nowUtc) =>
        lastSucceededUtc is not DateTime last || nowUtc - last >= Interval || nowUtc < last;
}
