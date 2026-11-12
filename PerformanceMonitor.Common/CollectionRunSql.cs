/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// #8464: what counts as a collection in <c>collection_log</c>, written once for both stores. Every collector attempt writes a
/// row, a failed one included (ERROR, PERMISSIONS, SESSION_MISSING and the rest), so a server the collector cannot read from
/// writes one row per collector per cycle, and a "last collected" read over every row moves forward on each failed attempt: a
/// dark server reads fresh. Only a <c>SUCCESS</c> row is a collection that reached the server. The two rows that are not
/// collector runs at all, the loop's skipped-slots row and Darling's scheduled analysis pass, never carry SUCCESS, but the
/// predicate names them so the statement says what it leaves out. Darling's <c>CollectionCycleRow</c> and <c>AnalysisPassRow</c>
/// and Lite's <c>CollectionCycleSkip</c> read their collector names from here, so each name is spelled once.
/// </summary>
public static class CollectionRunSql
{
    /// <summary>The <c>collection_log.collector_name</c> of the row that records the collection slots a server's loop stepped
    /// over (Darling's <c>CollectionCycleRow</c>, Lite's <c>CollectionCycleSkip</c>). It is not a collector run.</summary>
    public const string CycleRowCollectorName = "collection_cycle";

    /// <summary>The <c>collection_log.collector_name</c> of the row Darling's scheduled analysis pass writes
    /// (<c>AnalysisPassRow</c>). It is not a collector run, and Lite writes no such row.</summary>
    public const string AnalysisPassCollectorName = "analysis_pass";

    /// <summary>The predicate that keeps only the collector runs that reached the server, for a read of <c>collection_log</c> or
    /// its view in either store: <c>status = 'SUCCESS' AND collector_name NOT IN ('collection_cycle', 'analysis_pass')</c>. The
    /// Overview card's Last Collect, the sidebar freshness dot, <c>list_servers</c> and <c>get_server_summary</c> read it in both
    /// apps, so a failed attempt moves none of them. A row's <c>status</c> and <c>collector_name</c> are NOT NULL.</summary>
    public const string SuccessfulCollectorRun =
        "status = 'SUCCESS' AND collector_name NOT IN ('" + CycleRowCollectorName + "', '" + AnalysisPassCollectorName + "')";
}
