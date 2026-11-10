/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #8464: whether any collector has run against a server, read beside the newest successful collection by every surface that
/// bands freshness. A collector writes a <c>collection_log</c> row for every attempt, a failed one included, so a server whose
/// login connects but lacks VIEW SERVER STATE holds PERMISSIONS rows and no SUCCESS row: its newest success is null, as it is
/// for a server no collector has reached yet, and the two must read differently (Offline against awaiting its first
/// collection). This probe is the second fact: true when the log holds a collector row for the server at all. The cycle and
/// analysis-pass rows are not collector runs and do not count (<see cref="CollectionCycleRow.NotCycleRowSql"/>).
/// </summary>
public static class CollectionAttemptSql
{
    /// <summary>Whether the log holds any collector row for the <c>servers</c> row aliased <c>s</c>, as a select-list expression with
    /// no alias of its own. The EXISTS stops at the first row it finds on <c>idx_collection_log_time (server_id, collection_time)</c>,
    /// one index probe, and it reads every retained row's chunk, so it is not bounded to the window a surface reads its newest
    /// success over: a server tried and refused for longer than the window has still been tried.</summary>
    public const string Attempted =
        "EXISTS (SELECT 1 FROM v_collection_log cl WHERE cl.server_id = s.server_id AND cl." + CollectionCycleRow.NotCycleRowSql + ")";
}
