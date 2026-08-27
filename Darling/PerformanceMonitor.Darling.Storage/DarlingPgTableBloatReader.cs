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
/// Reads the per-table bloat ESTIMATE (<c>pg_table_bloat_stats</c>), with the trust signals that decide
/// whether the estimate may be shown at all.
/// </summary>
public static class DarlingPgTableBloatReader
{
    public sealed record PgTableBloatRow(
        string? DatabaseName,
        string? SchemaName,
        string? TableName,
        DateTime MeasuredAt,
        long HeapBytes,
        long HeapPages,
        long ToastBytes,
        long IndexBytes,
        long LiveTuples,
        long DeadTuples,
        long ModsSinceAnalyze,
        DateTime? LastAnalyzed,
        double EstimatedTupleBytes,
        long EstimatedHeapPages,
        int FillFactor,
        long BloatBytesEstimate,
        decimal BloatPctEstimate,
        bool EstimateUnavailable,
        int AlignmentBytes,
        bool PgstattupleAvailable,
        long FirstHeapBytes,
        long FirstBloatBytesEstimate,
        DateTime FirstSeenAt,
        int SampleCount);

    /// <summary>
    /// Latest state per table, joined to that table's earliest reading in the window.
    ///
    /// <para><b>The estimate is not filtered or ranked on here beyond ordering</b> — the decision about
    /// whether it may be SHOWN belongs to the tool, which suppresses it when
    /// <c>estimate_unavailable</c> is set or the statistics behind it are stale. Doing that filtering in
    /// SQL would silently drop the rows a reader most needs to see, because a table whose estimate cannot
    /// be trusted is still a table somebody asked about.</para>
    ///
    /// <para>The earliest reading is the whole reason this is a monitoring read rather than a query anyone
    /// could run: a spot bloat percentage is what gets someone to run <c>VACUUM FULL</c> on a Tuesday, and
    /// the growth across the window is what says whether the waste is accumulating, holding steady, or
    /// already being reclaimed. <c>first_heap_bytes</c> is carried beside
    /// <c>first_bloat_bytes_estimate</c> so the trend can be read on the MEASURED size even when the
    /// estimate itself is suppressed.</para>
    ///
    /// <para>Ordered by estimated wasted bytes, largest first, with tables whose estimate is unavailable
    /// sorted to the BOTTOM rather than out: their <c>bloat_bytes_estimate</c> is arithmetic over statistics
    /// that do not exist, so letting it rank would put the least-known tables at the top of a list read as
    /// a work queue.</para>
    ///
    /// <para>$1 server_id, $2/$3 window (naive UTC), $4 row limit.</para>
    /// </summary>
    public const string PgTableBloatSql = """
        WITH latest AS (
            SELECT DISTINCT ON (database_name, schema_name, table_name)
                database_name, schema_name, table_name, collection_time,
                heap_bytes, heap_pages, toast_bytes, index_bytes,
                live_tuples, dead_tuples, mods_since_analyze, last_analyzed,
                estimated_tuple_bytes, estimated_heap_pages, fillfactor,
                bloat_bytes_estimate, bloat_pct_estimate, estimate_unavailable,
                alignment_bytes, pgstattuple_available
            FROM pg_table_bloat_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY database_name, schema_name, table_name, collection_time DESC
        ),
        earliest AS (
            SELECT DISTINCT ON (database_name, schema_name, table_name)
                database_name, schema_name, table_name,
                heap_bytes AS first_heap_bytes,
                bloat_bytes_estimate AS first_bloat_bytes_estimate,
                collection_time AS first_seen_at
            FROM pg_table_bloat_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY database_name, schema_name, table_name, collection_time ASC
        ),
        samples AS (
            SELECT database_name, schema_name, table_name, count(*) AS sample_count
            FROM pg_table_bloat_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY database_name, schema_name, table_name
        )
        SELECT
            l.database_name,
            l.schema_name,
            l.table_name,
            l.collection_time,
            l.heap_bytes,
            l.heap_pages,
            l.toast_bytes,
            l.index_bytes,
            l.live_tuples,
            l.dead_tuples,
            l.mods_since_analyze,
            l.last_analyzed,
            l.estimated_tuple_bytes,
            l.estimated_heap_pages,
            l.fillfactor,
            l.bloat_bytes_estimate,
            l.bloat_pct_estimate,
            l.estimate_unavailable,
            l.alignment_bytes,
            l.pgstattuple_available,
            e.first_heap_bytes,
            e.first_bloat_bytes_estimate,
            e.first_seen_at,
            s.sample_count
        FROM latest AS l
        JOIN earliest AS e
          ON  e.database_name IS NOT DISTINCT FROM l.database_name
          AND e.schema_name   IS NOT DISTINCT FROM l.schema_name
          AND e.table_name    IS NOT DISTINCT FROM l.table_name
        JOIN samples AS s
          ON  s.database_name IS NOT DISTINCT FROM l.database_name
          AND s.schema_name   IS NOT DISTINCT FROM l.schema_name
          AND s.table_name    IS NOT DISTINCT FROM l.table_name
        /* Unusable estimates sort last rather than being filtered out - see the remarks. Within each
           group, biggest estimated waste first.

           ALL THREE suppression reasons are in the sort key, not just estimate_unavailable. A row
           suppressed for stale statistics or for never having been analyzed carries an arbitrarily large
           and untrustworthy bloat_bytes_estimate - the 81-percentage-point case measured 94.28% - so
           ordering on the flag alone let exactly those rows take the top of a LIMIT that reads as a work
           queue, which is the outcome this ordering exists to prevent.

           This is the SQL twin of EstimateIsUnpublishable, and the duplication is forced: an ORDER BY
           cannot call into C#. TheSortKeyCoversEverySuppressionReason pins the two together by asserting
           each condition appears here, so dropping one goes red rather than quietly re-ranking. */
        ORDER BY
            (   l.estimate_unavailable
             OR l.live_tuples < 0
             OR (l.live_tuples >= 0 AND l.mods_since_analyze > l.live_tuples * 0.2)
            ) ASC,
            l.bloat_bytes_estimate DESC,
            l.heap_bytes DESC
        LIMIT $4
        """;

    /// <summary>
    /// The churn ratio as it is WRITTEN INTO the ORDER BY above. A raw string literal cannot be spliced,
    /// so the number is inlined there and this names what it must equal;
    /// <c>TheSortKeyUsesTheSharedChurnRatio</c> asserts the SQL actually contains it, which is what stops
    /// the two drifting after somebody tunes <see cref="StaleStatisticsChurnRatio"/> and misses the query.
    /// </summary>
    public const string StaleStatisticsChurnRatioSql = "0.2";

    public static async Task<List<PgTableBloatRow>> GetPgTableBloatAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgTableBloatRow>();
        await using var command = postgres.CreateCommand(PgTableBloatSql);
        command.Parameters.AddWithValue(serverId);
        /* Kind-Unspecified at the BIND, per the store's naive-UTC discipline - a Kind=Utc DateTime makes
           Npgsql infer timestamptz and PostgreSQL then converts the naive column at the session TimeZone,
           silently emptying the window east of UTC. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgTableBloatRow(
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetDateTime(3),
                /* -1 for an unmeasured size rather than 0: 0 is a claim that the table occupies nothing. */
                reader.IsDBNull(4) ? -1 : reader.GetInt64(4),
                reader.IsDBNull(5) ? -1 : reader.GetInt64(5),
                reader.IsDBNull(6) ? -1 : reader.GetInt64(6),
                reader.IsDBNull(7) ? -1 : reader.GetInt64(7),
                /* -1 preserves PG14+'s "never analyzed" reltuples marker rather than folding it to
                   "empty", which is what the estimate-unavailable flag keys on. */
                reader.IsDBNull(8) ? -1 : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                reader.IsDBNull(11) ? null : reader.GetDateTime(11),
                reader.IsDBNull(12) ? 0d : reader.GetDouble(12),
                reader.IsDBNull(13) ? -1 : reader.GetInt64(13),
                reader.IsDBNull(14) ? 100 : reader.GetInt32(14),
                reader.IsDBNull(15) ? 0 : reader.GetInt64(15),
                reader.IsDBNull(16) ? 0m : reader.GetDecimal(16),
                /* A NULL flag defaults to TRUE - "we cannot vouch for this estimate" is the only safe
                   reading of a missing trust signal, and a row stored before this column existed has
                   no basis to be trusted with. */
                reader.IsDBNull(17) || reader.GetBoolean(17),
                reader.IsDBNull(18) ? 0 : reader.GetInt32(18),
                !reader.IsDBNull(19) && reader.GetBoolean(19),
                reader.IsDBNull(20) ? -1 : reader.GetInt64(20),
                reader.IsDBNull(21) ? 0 : reader.GetInt64(21),
                reader.GetDateTime(22),
                reader.IsDBNull(23) ? 0 : (int)reader.GetInt64(23)));
        }

        return rows;
    }

    /// <summary>
    /// The honest-empty denominator, same shape and same reasoning as the index-usage probe:
    /// <c>pg_table_bloat_stats</c> is a PERIODIC surface, so any stored sample proves somebody looked, and
    /// zero rows for a collected server means every table is under the collector's size floor rather than
    /// that nothing was measured.
    /// <para>$1 server_id, $2/$3 window (naive UTC).</para>
    /// </summary>
    public const string PgTableBloatProbeSql = """
        SELECT
            (SELECT count(*) FROM pg_table_bloat_stats
             WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3) AS rows_in_window,
            (SELECT count(DISTINCT collection_time) FROM pg_table_bloat_stats
             WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3) AS snapshots_in_window,
            (SELECT count(*) FROM pg_table_bloat_stats WHERE server_id = $1) AS rows_ever
        """;

    public sealed record PgTableBloatProbe(long RowsInWindow, long SnapshotsInWindow, long RowsEver);

    /// <summary>
    /// Modifications since the last <c>ANALYZE</c>, as a fraction of the table's live row count, above
    /// which the estimate is not fit to publish.
    /// <para>0.2 - a fifth of the table rewritten since the column widths were measured. Taken from the
    /// measurement that motivated the guard rather than chosen: two byte-identical 8,998-page tables with a
    /// true bloat of 10.93% estimated 92.64% and 11.01%, differing only in whether <c>ANALYZE</c> had run
    /// after a widening UPDATE. The stale one had modified 200% of its rows since its last analyze and the
    /// fresh one 0%, so anywhere between the two separates them; a fifth is chosen low because the failure
    /// is asymmetric - a needlessly cautious estimate costs a second look, an over-trusted one costs a
    /// table rewrite.</para>
    /// </summary>
    public const double StaleStatisticsChurnRatio = 0.2;

    /// <summary>
    /// Whether this row's bloat estimate may be shown at all — the ONE copy of that rule.
    ///
    /// <para>It lives here, beside the reader, rather than in either consumer, because BOTH consume it: the
    /// MCP tool nulls the estimate fields and the WPF grid renders a dash, and those two surfaces must not
    /// be able to disagree about whether a number is publishable. Two matching literals in two projects is
    /// how that disagreement arrives — silently, and in the direction that publishes a figure one surface
    /// had already decided was untrustworthy.</para>
    ///
    /// <para>The three states, each measured rather than supposed:</para>
    /// <list type="number">
    /// <item><c>estimate_unavailable</c> — no column statistics. Usually PERMISSIONS rather than a missing
    /// <c>ANALYZE</c>: <c>pg_stats</c> is filtered by SELECT privilege and <c>pg_monitor</c> does not confer
    /// it, and in that state the estimator does not fail — measured on a live target, it reported 88.59% for
    /// a table whose true bloat is 0.50%.</item>
    /// <item><c>live_tuples &lt; 0</c> — PostgreSQL 14+'s explicit "never analyzed" marker, preserved by the
    /// reader rather than floored to zero precisely so this check has something to key on.</item>
    /// <item>Stale widths — see <see cref="StaleStatisticsChurnRatio"/>. The 81-percentage-point case, and
    /// the only one of the three the <c>estimate_unavailable</c> flag does NOT catch.</item>
    /// </list>
    ///
    /// <para><b>The churn comparison is <c>&gt;= 0</c>, not <c>&gt; 0</c>, and the difference is a real
    /// gap rather than a style choice.</b> <c>live_tuples = 0</c> is a genuine state distinct from the
    /// <c>-1</c> "never analyzed" sentinel: it means the last ANALYZE really did record zero rows, which is
    /// what a table analyzed just after a TRUNCATE or a bulk delete looks like. Guarding on <c>&gt; 0</c>
    /// short-circuits the whole clause there, so a table analyzed while EMPTY and then heavily inserted
    /// into — a shape the "autovacuum has fallen behind" scenarios this feature exists for produce
    /// routinely — would publish an estimate anchored to a stale zero row count with no suppression signal
    /// at all. That is the false-confidence failure this function exists to prevent, reached through the
    /// one input value the guard did not cover. At zero the comparison reduces to "any modification since
    /// the last analyze suppresses", which is correct: with no live rows recorded, ANY write makes the row
    /// count stale by definition.</para>
    /// </summary>
    public static bool EstimateIsUnpublishable(PgTableBloatRow row)
    {
        if (row is null)
        {
            throw new ArgumentNullException(nameof(row));
        }

        return row.EstimateUnavailable
            || row.LiveTuples < 0
            || (row.LiveTuples >= 0 && row.ModsSinceAnalyze > row.LiveTuples * StaleStatisticsChurnRatio);
    }

    public static async Task<PgTableBloatProbe> ProbePgTableBloatAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(PgTableBloatProbeSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new PgTableBloatProbe(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
        }

        return new PgTableBloatProbe(0, 0, 0);
    }
}
