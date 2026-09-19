/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetDrillDownCollector
{
    /// <summary>
    /// How much of the normalised statement text the drill-down carries. Longer than the SQL Server
    /// <c>BadActorDetailSql</c>'s 500 because the PostgreSQL text is already NORMALISED at capture (literals
    /// replaced by <c>$1</c>-style placeholders, whitespace collapsed) and carries no literal a privacy cut
    /// would want removed; the cap is the finding row's size, not the reader's eyes.
    /// </summary>
    internal const int StatementTextCap = 2000;

    /// <summary>How many temp-writing statements a spill-rooted finding carries — the offenders, not the
    /// catalogue; <c>get_pg_top_queries</c> is one call away for the rest.</summary>
    internal const int TempSpillStatementCap = 5;

    /// <summary>
    /// The statements that wrote the most temp blocks in the window, for a <c>PG_TEMP_SPILL</c> root (lane 6 of
    /// #3542): <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC), <c>$4</c> the row cap, <c>$5</c> the
    /// text cap. <c>temp_blks_written</c> is a lifetime counter on <c>pg_statement_stats</c>, so it is
    /// differenced the way <see cref="PgTargetBadActorDetailSql"/> differences the block counters —
    /// <c>GREATEST(x − LAG(x), 0)</c> over the full series identity — and summed per <c>queryid</c>; the calls
    /// and time beside it are the stored deltas, so the reader can see whether the spill is one heavy report or
    /// a hot statement spilling a little every call. <c>temp_blks_read</c> rides along: a statement that writes
    /// temp blocks and reads them back is a sort or hash that overflowed; one that only writes is a
    /// materialize. Blocks are 8 kB pages, and the C# states bytes beside them.
    ///
    /// <para>The text is <c>pg_statement_text</c>'s — normalised, captured hourly, never live SQL (the V86
    /// privacy design) — NULL when none has been captured yet. Only shapes that wrote at least one temp block
    /// in the window are returned (<c>HAVING</c>): a statement with no spill is not an offender.</para>
    /// </summary>
    public const string PgTargetTempSpillStatementsSql = @"
WITH stmt_series AS (
    SELECT
        queryid,
        database_id,
        delta_calls,
        delta_total_exec_time_ms,
        GREATEST(temp_blks_written - LAG(temp_blks_written) OVER identity, 0) AS d_temp_blks_written,
        GREATEST(temp_blks_read    - LAG(temp_blks_read)    OVER identity, 0) AS d_temp_blks_read
    FROM pg_statement_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    WINDOW identity AS (
        PARTITION BY queryid, database_id, user_id, toplevel
        ORDER BY collection_time
    )
)
SELECT
    s.queryid,
    CAST(COALESCE(SUM(s.d_temp_blks_written), 0) AS bigint)      AS temp_blks_written,
    CAST(COALESCE(SUM(s.d_temp_blks_read), 0) AS bigint)         AS temp_blks_read,
    CAST(COALESCE(SUM(s.delta_calls), 0) AS bigint)              AS calls,
    CAST(COALESCE(SUM(s.delta_total_exec_time_ms), 0) AS bigint) AS total_exec_ms,
    COUNT(DISTINCT s.database_id)                                AS database_count,
    LEFT(MAX(t.query_text), $5)                                  AS query_text,
    hashtext(MAX(t.query_text))                                  AS text_hash
FROM stmt_series AS s
LEFT JOIN pg_statement_text AS t
       ON  t.server_id = $1
       AND t.queryid = s.queryid
GROUP BY s.queryid
HAVING SUM(s.d_temp_blks_written) > 0
ORDER BY SUM(s.d_temp_blks_written) DESC
LIMIT $4";

    /// <summary>
    /// One statement's window, for a <c>PG_BAD_ACTOR_&lt;queryid&gt;</c> root or leaf: the deltas the fact was
    /// graded on, the block figures the fact does not carry, the per-database breakdown, and the normalised
    /// text with its hash. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC), <c>$4</c> queryid,
    /// <c>$5</c> the text cap.
    ///
    /// <para>The same differencing as the collector's read and <c>DarlingPgStatementReader.PgTopQueriesSql</c>:
    /// stored deltas for calls / time / rows, <c>GREATEST(x − LAG(x), 0)</c> over the full series identity
    /// for the block and WAL counters, high-water marks as <c>MAX</c>. The per-database rows are an ARRAY
    /// aggregate over the same grouped result so the detail is one row and one statement, and so the
    /// <c>database_id</c> OIDs (the collector's key; #3540 A11c — an OID is reused across DROP / CREATE and this
    /// read reproduces the key as written) are listed rather than summed away.</para>
    ///
    /// <para><b>The text is what <c>pg_statement_text</c> holds — normalised, captured hourly, keyed on
    /// <c>(server_id, queryid)</c>, never live SQL</b> (V73 / the V86 privacy design). NULL when none has been
    /// captured yet, and the JSON then says <c>null</c> rather than an empty string. <c>hashtext()</c> of it is
    /// the breadcrumb that survives the <c>queryid</c> re-key a major upgrade performs; <c>first_seen</c> is the
    /// one record of when the shape first appeared on this server that survives the same re-key.</para>
    /// </summary>
    public const string PgTargetBadActorDetailSql = @"
WITH stmt_series AS (
    SELECT
        queryid,
        database_id,
        collection_time,
        delta_calls,
        delta_total_exec_time_ms,
        delta_rows,
        max_exec_time_ms,
        GREATEST(shared_blks_hit   - LAG(shared_blks_hit)   OVER identity, 0) AS d_shared_blks_hit,
        GREATEST(shared_blks_read  - LAG(shared_blks_read)  OVER identity, 0) AS d_shared_blks_read,
        GREATEST(temp_blks_read    - LAG(temp_blks_read)    OVER identity, 0) AS d_temp_blks_read,
        GREATEST(temp_blks_written - LAG(temp_blks_written) OVER identity, 0) AS d_temp_blks_written,
        GREATEST(wal_bytes         - LAG(wal_bytes)         OVER identity, 0) AS d_wal_bytes
    FROM pg_statement_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   queryid = $4
    WINDOW identity AS (
        PARTITION BY queryid, database_id, user_id, toplevel
        ORDER BY collection_time
    )
),
per_database AS (
    SELECT
        database_id,
        CAST(COALESCE(SUM(delta_calls), 0) AS bigint)              AS calls,
        CAST(COALESCE(SUM(delta_total_exec_time_ms), 0) AS bigint) AS total_exec_ms
    FROM stmt_series
    GROUP BY database_id
)
SELECT
    CAST(COALESCE(SUM(s.delta_calls), 0) AS bigint)              AS calls,
    CAST(COALESCE(SUM(s.delta_total_exec_time_ms), 0) AS bigint) AS total_exec_ms,
    CAST(COALESCE(SUM(s.delta_rows), 0) AS bigint)               AS rows_returned,
    MAX(s.max_exec_time_ms)                                      AS max_exec_ms,
    CAST(COALESCE(SUM(s.d_shared_blks_hit), 0) AS bigint)        AS shared_blks_hit,
    CAST(COALESCE(SUM(s.d_shared_blks_read), 0) AS bigint)       AS shared_blks_read,
    CAST(COALESCE(SUM(s.d_temp_blks_read), 0) AS bigint)         AS temp_blks_read,
    CAST(COALESCE(SUM(s.d_temp_blks_written), 0) AS bigint)      AS temp_blks_written,
    CAST(COALESCE(SUM(s.d_wal_bytes), 0) AS bigint)              AS wal_bytes,
    COUNT(DISTINCT s.collection_time)                            AS snapshot_count,
    MIN(s.collection_time)                                       AS first_snapshot,
    MAX(s.collection_time)                                       AS last_snapshot,
    (SELECT array_agg(pd.database_id ORDER BY pd.total_exec_ms DESC) FROM per_database AS pd)   AS database_ids,
    (SELECT array_agg(pd.calls ORDER BY pd.total_exec_ms DESC) FROM per_database AS pd)         AS database_calls,
    (SELECT array_agg(pd.total_exec_ms ORDER BY pd.total_exec_ms DESC) FROM per_database AS pd) AS database_total_exec_ms,
    LEFT(MAX(t.query_text), $5)                                  AS query_text,
    hashtext(MAX(t.query_text))                                  AS text_hash,
    MAX(t.first_seen)                                            AS text_first_seen
FROM stmt_series AS s
LEFT JOIN pg_statement_text AS t
       ON  t.server_id = $1
       AND t.queryid = $4
HAVING COUNT(*) > 0";

    /// <summary>
    /// The statement leaf (filled by lane 7): for every <c>PG_BAD_ACTOR_*</c> key on the story path — the root
    /// on a standalone card, the leaf when lane 6's or lane 9's edge led here — one detail object under
    /// <c>pg_bad_actor_statements</c>, keyed by the fact key, from <see cref="PgTargetBadActorDetailSql"/>.
    /// The <c>queryid</c> is parsed back out of the key with the invariant culture, the inverse of
    /// <see cref="PgTargetFactKeys.BadActorKey"/>; a key that does not parse is skipped, never guessed.
    ///
    /// <para>Every command sets <c>CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds</c>
    /// and every store call passes <c>context.CancellationToken</c>; the window binds naive-UTC via
    /// <c>AsNaive</c>. No <c>DeSkew</c>: <c>pg_statement_stats</c> stamps <c>collection_time</c> host-UTC
    /// (<c>CreationTimeClockFrameDisciplineTests</c> lists the SQL Server twins by path and not these files,
    /// and that is correct).</para>
    ///
    /// <para><c>PG_TEMP_SPILL</c> on the path: the per-statement <c>temp_blks_written</c> offenders are lane 6's
    /// arm of this method (filled by lane 6 of #3542) — one list under <c>pg_temp_spill_statements</c>, heaviest
    /// first, from <see cref="PgTargetTempSpillStatementsSql"/>. It runs whenever the spill is on the path,
    /// beside any bad-actor detail rather than instead of it: the two answer different questions ("what is this
    /// statement's window" and "which statements spilled"), and a story that has both deserves both. "Offender"
    /// means wrote at least one temp block in the window; an empty list is not written.</para>
    /// </summary>
    private async partial Task CollectTopStatementsAsync(AnalysisFinding finding, AnalysisContext context, HashSet<string> pathKeys)
    {
        var badActorKeys = pathKeys
            .Where(k => k.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal))
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        if (badActorKeys.Count > 0)
        {
            var details = new Dictionary<string, object>(StringComparer.Ordinal);
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            foreach (var key in badActorKeys)
            {
                if (!long.TryParse(key.AsSpan(PgTargetFactKeys.BadActorKeyPrefix.Length), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var queryId))
                    continue;

                var detail = await ReadBadActorDetailAsync(connection, context, queryId);
                if (detail is not null)
                    details[key] = detail;
            }

            if (details.Count > 0)
                finding.DrillDown!["pg_bad_actor_statements"] = details;
        }

        if (pathKeys.Contains(PgTargetFactKeys.TempSpill))
        {
            var offenders = await ReadTempSpillStatementsAsync(context);
            if (offenders.Count > 0)
                finding.DrillDown!["pg_temp_spill_statements"] = offenders;
        }
    }

    private async Task<List<object>> ReadTempSpillStatementsAsync(AnalysisContext context)
    {
        var offenders = new List<object>(TempSpillStatementCap);
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(PgTargetTempSpillStatementsSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        cmd.Parameters.AddWithValue(TempSpillStatementCap);
        cmd.Parameters.AddWithValue(StatementTextCap);

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            var queryId = Convert.ToInt64(reader.GetValue(0));
            var tempBlksWritten = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
            var calls = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));
            offenders.Add(new
            {
                /* A string, as get_pg_top_queries returns it and as the bad-actor detail above does. */
                queryid = queryId.ToString(CultureInfo.InvariantCulture),
                temp_blks_written = tempBlksWritten,
                /* 8 kB pages, stated in bytes so the figure sits beside the spill fact's temp_bytes on one axis. */
                temp_bytes_written = tempBlksWritten * 8192L,
                temp_blks_read = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2)),
                calls,
                total_exec_ms = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                /* Null, not 0, for a mean over no calls. */
                temp_blks_written_per_call = calls > 0 ? Math.Round((double)tempBlksWritten / calls, 2) : (double?)null,
                database_count = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                /* Null when no text has been captured for this queryid yet — never "". */
                query_text = reader.IsDBNull(6) ? null : reader.GetString(6),
                text_hash = reader.IsDBNull(7) ? (int?)null : Convert.ToInt32(reader.GetValue(7)),
            });
        }

        return offenders;
    }

    private async Task<object?> ReadBadActorDetailAsync(NpgsqlConnection connection, AnalysisContext context, long queryId)
    {
        using var cmd = new NpgsqlCommand(PgTargetBadActorDetailSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        cmd.Parameters.AddWithValue(queryId);
        cmd.Parameters.AddWithValue(StatementTextCap);

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken))
            return null;

        var calls = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
        var totalExecMs = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
        var databaseIds = reader.IsDBNull(12) ? [] : reader.GetFieldValue<long[]>(12);
        var databaseCalls = reader.IsDBNull(13) ? [] : reader.GetFieldValue<long[]>(13);
        var databaseTotals = reader.IsDBNull(14) ? [] : reader.GetFieldValue<long[]>(14);

        return new
        {
            /* A string, as get_pg_top_queries returns it: a signed 64-bit id spread over the whole int8 range
               does not survive a JSON number decoded as a double. */
            queryid = queryId.ToString(CultureInfo.InvariantCulture),
            calls,
            total_exec_ms = totalExecMs,
            /* Null, not 0, for a mean over no calls. */
            mean_exec_ms = calls > 0 ? Math.Round((double)totalExecMs / calls, 2) : (double?)null,
            max_exec_ms = reader.IsDBNull(3) ? (double?)null : Math.Round(Convert.ToDouble(reader.GetValue(3)), 2),
            rows_returned = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2)),
            shared_blks_hit = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
            shared_blks_read = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
            temp_blks_read = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6)),
            temp_blks_written = reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7)),
            wal_bytes = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
            snapshot_count = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
            first_snapshot = reader.IsDBNull(10) ? null : reader.GetDateTime(10).ToString("o", CultureInfo.InvariantCulture),
            last_snapshot = reader.IsDBNull(11) ? null : reader.GetDateTime(11).ToString("o", CultureInfo.InvariantCulture),
            /* One entry per database OID the shape ran against, heaviest first — the collector's key, as written. */
            databases = databaseIds.Select((id, i) => new
            {
                database_id = id,
                calls = i < databaseCalls.Length ? databaseCalls[i] : 0L,
                total_exec_ms = i < databaseTotals.Length ? databaseTotals[i] : 0L,
            }).ToList(),
            /* Null when no text has been captured for this queryid yet — never "". */
            query_text = reader.IsDBNull(15) ? null : reader.GetString(15),
            text_hash = reader.IsDBNull(16) ? (int?)null : Convert.ToInt32(reader.GetValue(16)),
            text_first_seen = reader.IsDBNull(17) ? null : reader.GetDateTime(17).ToString("o", CultureInfo.InvariantCulture),
            queryid_note = "queryid is stable within a PostgreSQL major and re-keyed by a major upgrade or a compute_query_id change; text_hash is the hash of the normalised text and is how the same statement is recognised across that re-key.",
        };
    }
}
