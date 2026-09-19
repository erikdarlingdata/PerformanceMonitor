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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads the per-database <c>pg_stat_database</c> counters from <c>pg_database_stats</c>, differenced across
/// the window — temp-file spills, cache hit ratio, deadlocks, and the commit/rollback split (#2539).
/// <para><b>Not yet read here: <c>numbackends</c></b> (V133, #3691) — the client backends connected to each
/// database at the instant of the row, a LEVEL beside the counters this reader differences. A reader that
/// takes it up must never difference it (the newest row's value is the reading, or a MAX/AVG over the window
/// is the shape), must treat a NULL as "not sampled" rather than zero connections (every pre-V133 row is
/// NULL), and must sum across databases for the cluster figure a <c>max_connections</c> fraction wants. The
/// saturation consumer is the follow-on lane's; this rung only stores the column.</para>
/// </summary>
public static class DarlingPgDatabaseReader
{
    public sealed record PgDatabaseRow(
        string? DatabaseName,
        long XactCommit,
        long XactRollback,
        long BlksRead,
        long BlksHit,
        long TempFiles,
        long TempBytes,
        long Deadlocks,
        int StatsResetCount,
        int CounterRewindCount,
        DateTime? StatsReset,
        int SampleCount,
        DateTime? FirstSampleAt,
        DateTime? LastSampleAt);

    /// <summary>
    /// One page of the read: the rows the cap admitted, beside the WINDOW's figures over every database that
    /// moved (#3653, the wwwroot-twins item; the #3541 A7 shape the five percent tools took in #3613).
    ///
    /// <para>Until this record existed the tool summed the rows it had fetched and published the sums as
    /// <c>total_temp_files</c> / <c>total_temp_bytes</c> / <c>total_deadlocks</c>, so at <c>limit = 2</c> on a
    /// cluster with five spilling databases the "total" was the top two's. That was carried honestly — a
    /// <c>limit_reached</c> flag, a note saying the totals covered only the databases returned, and a web tile
    /// labelled "Databases returned" — as the A7 census's one stated allowance. The window's figures are
    /// free on the statement the read already runs (a window aggregate over the grouped result, evaluated
    /// after <c>GROUP BY</c> / <c>HAVING</c> and before <c>ORDER BY</c> / <c>LIMIT</c>), so the allowance is
    /// retired rather than relabelled: <c>total_*</c> now means what it says.</para>
    ///
    /// <para><c>WindowDatabaseCount</c> is how many databases moved (or were reset) in the window —
    /// <c>COUNT(*) OVER ()</c> over the same grouped result — which is what a count called
    /// <c>database_count</c> has to mean beside window totals. <c>WindowTotalBlksHit</c> / <c>WindowTotalBlksRead</c>
    /// carry the cluster-wide hit ratio's two halves for the same reason: the old payload could only offer
    /// <c>cache_hit_pct_of_returned</c>, honest by name, because the true ratio would have cost a second
    /// unfiltered aggregate; on this statement it costs nothing. A page record rather than per-row copies,
    /// because these are facts about the window and the Viewer's projection census maps every ROW property
    /// to a display column.</para>
    ///
    /// <para><c>WindowResetCount</c> is the number of reset signals (explicit <c>stats_reset</c> moves plus
    /// counter rewinds) across every database in the window, so the payload's top-level "statistics were
    /// reset in this window" flag can be true of the WINDOW it now sits beside: a database reset off the page
    /// makes every window total a lower bound, and a flag computed over the page alone would say otherwise.</para>
    ///
    /// <para>Every window figure is 0 when the page is empty (<c>OVER ()</c> over zero rows returns no row to
    /// read them from), and the tool answers the empty case before it looks at them.</para>
    /// </summary>
    public sealed record PgDatabasePage(
        List<PgDatabaseRow> Rows,
        int WindowDatabaseCount,
        long WindowTotalTempFiles,
        long WindowTotalTempBytes,
        long WindowTotalDeadlocks,
        long WindowTotalBlksHit,
        long WindowTotalBlksRead,
        int WindowResetCount);

    /// <summary>
    /// Positive-difference-per-interval, summed over the window — the same rule the statement and I/O reads
    /// use, because these are cumulative-since-reset counters and a plain last-minus-first goes negative the
    /// moment <c>pg_stat_reset()</c> runs.
    ///
    /// <para><b>A reset is REPORTED, not just survived.</b> Clamping alone stops the negative rate and stops
    /// any spike, but it also silently drops the interval it clamped, so the window quietly under-counts and
    /// nothing says why. Two independent reset signals are carried out instead, because neither one sees
    /// every reset:</para>
    /// <list type="bullet">
    /// <item><c>stats_reset_count</c> — the EXPLICIT signal, from the server's own per-database
    /// <c>stats_reset</c> timestamp moving between two samples. This is the only thing that can see a reset
    /// followed by enough activity to climb back PAST the old value inside one collection interval: the
    /// difference is positive there, so the arithmetic sees a perfectly ordinary busy minute. The first
    /// sample of a series is excluded by its ROW_NUMBER, not by its <c>LAG</c> being NULL — see the
    /// comment on the expression, where that distinction is load-bearing.</item>
    /// <item><c>counter_rewind_count</c> — the IMPLICIT signal, a counter smaller than the previous sample's.
    /// Kept alongside rather than replaced by the explicit one because a crash restart discards the
    /// statistics, and a target that has never been reset at all reports <c>stats_reset</c> as NULL, so the
    /// timestamp can stay unchanged across a genuine rewind.</item>
    /// </list>
    /// <para>Both are counts rather than booleans: three resets in a window is a different conversation from
    /// one, and the sum of the differences means something different in each case.</para>
    ///
    /// <para><b>The series key is <c>database_name</c>, NULL included.</b> PostgreSQL emits one row with a
    /// NULL name for shared-relation activity; <c>PARTITION BY</c> and <c>GROUP BY</c> both treat NULLs as
    /// one group (grouping semantics, not <c>=</c>), so it differences correctly without a sentinel and the
    /// tool labels it.</para>
    ///
    /// <para><b>The window's totals ride on the same statement as the rows</b> (#3653, after #3541 A7).
    /// <c>SUM(coalesce(SUM(d_temp_files), 0)) OVER ()</c> is a window aggregate over the GROUPED result:
    /// PostgreSQL evaluates it after <c>GROUP BY</c> / <c>HAVING</c> and before <c>ORDER BY</c> / <c>LIMIT</c>,
    /// so it sums every database the window holds rather than the rows the cap admits, cannot drift from
    /// them (a collection landing between two statements is impossible on one), and costs one pass over a
    /// result the query has already grouped and is about to sort. The same <c>HAVING</c> bounds both: a
    /// database that neither moved nor was reset is in neither the rows nor the totals, which is the
    /// definition of "activity in the window" this read has always used. <c>COUNT(*) OVER ()</c> is the
    /// window's database count under the same rule. The six window columns are identical on every row
    /// (no partition) and the reader takes them off the last row it reads.</para>
    ///
    /// <para>$1 server_id, $2/$3 window (naive UTC), $4 row limit.</para>
    /// </summary>
    public const string PgDatabaseSql = """
        WITH sampled AS (
            SELECT
                database_name,
                collection_time,
                stats_reset,
                xact_commit   - LAG(xact_commit)   OVER series AS raw_xact_commit,
                xact_rollback - LAG(xact_rollback) OVER series AS raw_xact_rollback,
                blks_read     - LAG(blks_read)     OVER series AS raw_blks_read,
                blks_hit      - LAG(blks_hit)      OVER series AS raw_blks_hit,
                temp_files    - LAG(temp_files)    OVER series AS raw_temp_files,
                temp_bytes    - LAG(temp_bytes)    OVER series AS raw_temp_bytes,
                deadlocks     - LAG(deadlocks)     OVER series AS raw_deadlocks,
                /* The explicit reset. IS DISTINCT FROM rather than <> so a NULL on either side is a real
                   comparison, and ROW_NUMBER > 1 - not `LAG(stats_reset) IS NOT NULL` - is what excludes
                   the first sample of the series.

                   That distinction is the whole correctness of this line. LAG(stats_reset) is NULL in TWO
                   situations a guard on it cannot tell apart: there is no previous row, and there IS one
                   whose stats_reset was itself NULL. The second is the COMMON state - stats_reset is NULL
                   until the first reset ever - so a database's FIRST reset moves it NULL -> timestamp, the
                   LAG guard evaluates false, and the reset never fires. Worse, that is precisely the case
                   counter_rewind_count cannot cover either: if the database climbed back past its old
                   values before the next sample, every difference is positive and the reset is completely
                   invisible. ROW_NUMBER asks the question actually being asked - "is this the first sample
                   of the series" - and answers it from the row's position rather than from a value that
                   means something else. Proven both directions against live PostgreSQL 17. */
                (ROW_NUMBER() OVER series > 1
                 AND stats_reset IS DISTINCT FROM LAG(stats_reset) OVER series) AS reset_here
            FROM pg_database_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            WINDOW series AS (
                PARTITION BY database_name
                ORDER BY collection_time
            )
        ),
        differenced AS (
            SELECT
                database_name,
                collection_time,
                stats_reset,
                reset_here,
                GREATEST(raw_xact_commit, 0)   AS d_xact_commit,
                GREATEST(raw_xact_rollback, 0) AS d_xact_rollback,
                GREATEST(raw_blks_read, 0)     AS d_blks_read,
                GREATEST(raw_blks_hit, 0)      AS d_blks_hit,
                GREATEST(raw_temp_files, 0)    AS d_temp_files,
                GREATEST(raw_temp_bytes, 0)    AS d_temp_bytes,
                GREATEST(raw_deadlocks, 0)     AS d_deadlocks,
                /* The implicit reset: ANY counter below its predecessor. LEAST ignores NULLs the way
                   GREATEST does, so the first sample of a series - every raw NULL - yields NULL, and
                   NULL < 0 is NULL, which the FILTER below does not count. */
                (LEAST(raw_xact_commit, raw_xact_rollback, raw_blks_read, raw_blks_hit,
                       raw_temp_files, raw_temp_bytes, raw_deadlocks) < 0) AS rewound_here
            FROM sampled
        )
        SELECT
            database_name,
            CAST(coalesce(SUM(d_xact_commit), 0) AS bigint)   AS xact_commit,
            CAST(coalesce(SUM(d_xact_rollback), 0) AS bigint) AS xact_rollback,
            CAST(coalesce(SUM(d_blks_read), 0) AS bigint)     AS blks_read,
            CAST(coalesce(SUM(d_blks_hit), 0) AS bigint)      AS blks_hit,
            CAST(coalesce(SUM(d_temp_files), 0) AS bigint)    AS temp_files,
            CAST(coalesce(SUM(d_temp_bytes), 0) AS bigint)    AS temp_bytes,
            CAST(coalesce(SUM(d_deadlocks), 0) AS bigint)     AS deadlocks,
            CAST(count(*) FILTER (WHERE reset_here) AS integer)   AS stats_reset_count,
            CAST(count(*) FILTER (WHERE rewound_here) AS integer) AS counter_rewind_count,
            MAX(stats_reset)                                  AS stats_reset,
            CAST(count(*) AS integer)                         AS sample_count,
            MIN(collection_time)                              AS first_sample_at,
            MAX(collection_time)                              AS last_sample_at,
            /* The WINDOW's figures, on every row: over the grouped result, after the group filter below
               and before the cap - the same population the rows are drawn from (#3653). */
            CAST(COUNT(*) OVER () AS integer)                              AS window_database_count,
            CAST(SUM(coalesce(SUM(d_temp_files), 0)) OVER () AS bigint)   AS window_total_temp_files,
            CAST(SUM(coalesce(SUM(d_temp_bytes), 0)) OVER () AS bigint)   AS window_total_temp_bytes,
            CAST(SUM(coalesce(SUM(d_deadlocks), 0)) OVER () AS bigint)    AS window_total_deadlocks,
            CAST(SUM(coalesce(SUM(d_blks_hit), 0)) OVER () AS bigint)     AS window_total_blks_hit,
            CAST(SUM(coalesce(SUM(d_blks_read), 0)) OVER () AS bigint)    AS window_total_blks_read,
            CAST(SUM(count(*) FILTER (WHERE reset_here) + count(*) FILTER (WHERE rewound_here)) OVER () AS integer)
                                                              AS window_reset_count
        FROM differenced
        GROUP BY database_name
        /* Anything that moved, OR anything whose counters were reset. The reset clause is not decoration:
           a database that was reset and then sat idle has all-zero differences, and reporting nothing for it
           would hide the one fact that explains why the numbers look the way they do. */
        HAVING coalesce(SUM(d_xact_commit), 0) + coalesce(SUM(d_xact_rollback), 0)
             + coalesce(SUM(d_blks_read), 0) + coalesce(SUM(d_blks_hit), 0)
             + coalesce(SUM(d_temp_files), 0) + coalesce(SUM(d_deadlocks), 0) > 0
            OR count(*) FILTER (WHERE reset_here) > 0
            OR count(*) FILTER (WHERE rewound_here) > 0
        /* Spilled bytes first, because that is the question this read exists to answer and the one no other
           read here can. Blocks read breaks the tie, so a busy database still outranks an idle one when
           nothing spilled at all. */
        ORDER BY coalesce(SUM(d_temp_bytes), 0) DESC, coalesce(SUM(d_blks_read), 0) DESC
        LIMIT $4
        """;

    /// <summary>The rows alone — the Viewer's grid read, which has no total tiles and keeps its signature. The
    /// MCP tool goes through <see cref="GetPgDatabaseStatsPageAsync"/> for the window figures.</summary>
    public static async Task<List<PgDatabaseRow>> GetPgDatabaseStatsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default) =>
        (await GetPgDatabaseStatsPageAsync(postgres, serverId, startUtc, endUtc, limit, cancellationToken)).Rows;

    /// <summary>Runs <see cref="PgDatabaseSql"/>: the page's rows plus the window's totals off the same
    /// statement. <paramref name="limit"/> is bound as-is — the tool passes <c>limit + 1</c> and observes
    /// truncation from the extra row, the A3 dialect.</summary>
    public static async Task<PgDatabasePage> GetPgDatabaseStatsPageAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgDatabaseRow>();
        var windowDatabaseCount = 0;
        var windowResetCount = 0;
        long windowTempFiles = 0, windowTempBytes = 0, windowDeadlocks = 0, windowBlksHit = 0, windowBlksRead = 0;
        await using var command = postgres.CreateCommand(PgDatabaseSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* Kind-Unspecified at the BIND, per the store's naive-UTC discipline: a Kind=Utc DateTime makes
           Npgsql infer timestamptz, and PostgreSQL then resolves the comparison against these naive
           timestamp columns by converting THEM at the store session's TimeZone - east of UTC every fresh
           row falls out of the window and the read silently returns nothing. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgDatabaseRow(
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetInt64(3),
                reader.GetInt64(4),
                reader.GetInt64(5),
                reader.GetInt64(6),
                reader.GetInt64(7),
                reader.GetInt32(8),
                reader.GetInt32(9),
                reader.IsDBNull(10) ? null : reader.GetDateTime(10),
                reader.GetInt32(11),
                reader.IsDBNull(12) ? null : reader.GetDateTime(12),
                reader.IsDBNull(13) ? null : reader.GetDateTime(13)));

            /* Identical on every row (OVER () with no partition); the last write wins with the same number. */
            windowDatabaseCount = reader.GetInt32(14);
            windowTempFiles = reader.GetInt64(15);
            windowTempBytes = reader.GetInt64(16);
            windowDeadlocks = reader.GetInt64(17);
            windowBlksHit = reader.GetInt64(18);
            windowBlksRead = reader.GetInt64(19);
            windowResetCount = reader.GetInt32(20);
        }

        return new PgDatabasePage(rows, windowDatabaseCount, windowTempFiles, windowTempBytes, windowDeadlocks, windowBlksHit, windowBlksRead, windowResetCount);
    }

    /// <summary>
    /// The denominator for an empty answer, and the reason it is a DATA probe rather than a
    /// <c>collection_log</c> one.
    ///
    /// <para><c>pg_stat_database</c> is a PERIODIC surface, not an edge table: the collector writes a row per
    /// database every cycle whatever the server is doing, so the presence of any sample is proof somebody
    /// looked. That is the opposite of the blocking and deadlock reads, where a row exists only because
    /// something went wrong and a data probe would report a healthy server as uncollected (#2508). Probing
    /// <c>pg_database_stats</c> — the SAME relation the read walks, deliberately without the read's own
    /// <c>HAVING</c> filter — means it can never report "collected" for rows the read cannot see, and never
    /// report "uncollected" for a server that is simply quiet.</para>
    ///
    /// <para><b>The window count is capped at two, and two is the number that matters.</b> These are
    /// cumulative counters, so ONE sample produces no difference at all and the read returns nothing —
    /// indistinguishable, in the rows alone, from a genuinely idle window. A server whose first collection
    /// cycle has just run is in exactly that state, and telling its operator the database was quiet would be
    /// a confident wrong answer for the several minutes until the second cycle lands.</para>
    ///
    /// <para>$1 server_id, $2/$3 window (naive UTC).</para>
    /// </summary>
    public const string DatabaseStatsCoverageSql = """
        SELECT
            (SELECT count(*)
             FROM (SELECT DISTINCT collection_time
                   FROM pg_database_stats
                   WHERE server_id = $1
                   AND   collection_time >= $2
                   AND   collection_time <= $3
                   LIMIT 2) AS windowed)                                  AS samples_in_window,
            EXISTS (SELECT 1 FROM pg_database_stats WHERE server_id = $1) AS ever_collected
        """;

    /// <summary>Runs <see cref="DatabaseStatsCoverageSql"/>.</summary>
    public static async Task<(int SamplesInWindow, bool EverCollected)> GetCoverageAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(DatabaseStatsCoverageSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            return (0, false);
        }

        return ((int)reader.GetInt64(0), reader.GetBoolean(1));
    }
}
