/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The one rule for "current" in the PostgreSQL state reads (#7878, #7910): a stored row is current only if it came from
/// the collector's NEWEST successful run for the server. A row from an earlier run is the last thing seen before the
/// state cleared (a holder that finished, a table vacuumed clean) or the object was dropped (a replication slot, a
/// database), and must not be presented as present.
///
/// <para><b>Why the log, not the data table.</b> These collectors do not write a row for a state that is absent: the xmin
/// and autovacuum collectors store nothing when nothing is wrong, and a capture of a server with no slots stores no
/// slot rows. The newest data row therefore cannot say whether a later capture looked and found nothing. Only
/// <c>collection_log</c> has one row per run, including the runs that stored nothing.</para>
///
/// <para><b>Why the second-newest run, not the newest.</b> A row is stamped when its run STARTS and the run's
/// <c>collection_log</c> row when it ENDS (<c>DarlingObservability.LogCollectionAsync</c> stamps the clock at log time), so
/// comparing a row with the newest run's log time would call every row of the newest run stale. A row belongs to the
/// newest run exactly when it was written after the run before it ended, so the cut is the second-newest SUCCESS run's log
/// time. With fewer than two SUCCESS runs in the window there is no earlier run for a row to belong to, and every row is
/// current. A run whose log write was skipped only makes the rule keep a row, never drop one.</para>
///
/// <para>The window end is the caller's <c>$3</c>, so an as-of read judges "current" at that instant, not at the clock.
/// $1 server_id, $2/$3 window (naive UTC): the same parameters every reader here binds.</para>
/// </summary>
public static class PgCurrentCapture
{
    private const string Head = """
        capture_window AS (
            SELECT MAX(collection_time) AS newest_at,
                   CASE WHEN COUNT(*) = 2 THEN MIN(collection_time) END AS previous_at
            FROM (
                SELECT collection_time
                FROM collection_log
                WHERE server_id = $1
                AND   collector_name = '
        """;

    private const string Tail = """
        '
                AND   status = 'SUCCESS'
                AND   collection_time >= $2
                AND   collection_time <= $3
                ORDER BY collection_time DESC
                LIMIT 2
            ) AS runs
        )
        """;

    /// <summary>The CTE for the xmin horizon collector. Join it as <c>cw</c>.</summary>
    public const string XminCte = Head + "pg_xmin_horizon" + Tail;

    /// <summary>The CTE for the autovacuum collector. Join it as <c>cw</c>.</summary>
    public const string AutovacuumCte = Head + "pg_autovacuum_stats" + Tail;

    /// <summary>The CTE for the replication slot collector. Join it as <c>cw</c>.</summary>
    public const string SlotsCte = Head + "pg_replication_slots" + Tail;

    /// <summary>The CTE for the wraparound collector. Join it as <c>cw</c>.</summary>
    public const string WraparoundCte = Head + "pg_wraparound_stats" + Tail;

    /// <summary>
    /// The current test for a row aliased <c>l</c> beside the <c>cw</c> CTE. NULL <c>previous_at</c> (fewer than two runs)
    /// reads as current.
    /// </summary>
    public const string IsCurrentSql = "l.collection_time > COALESCE(cw.previous_at, '-infinity'::timestamp)";
}
