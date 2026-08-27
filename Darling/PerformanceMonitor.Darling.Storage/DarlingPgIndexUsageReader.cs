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
/// Reads per-index usage (<c>pg_index_usage_stats</c>), ranked by the size of an index nothing is scanning.
/// </summary>
public static class DarlingPgIndexUsageReader
{
    public sealed record PgIndexUsageRow(
        string? DatabaseName,
        string? SchemaName,
        string? TableName,
        string? IndexName,
        DateTime MeasuredAt,
        long TotalScans,
        long ScansInWindow,
        long TuplesRead,
        long TuplesFetched,
        long BlocksRead,
        long BlocksHit,
        long IndexBytes,
        long TableBytes,
        bool IsUnique,
        bool IsPrimaryKey,
        bool IsValid,
        bool IsReady,
        bool IsReplicaIdentity,
        bool IsPartial,
        bool IsExpression,
        bool SupportsConstraint,
        string? IndexMethod,
        int ColumnCount,
        string? IndexDefinition,
        DateTime? LastScan,
        DateTime? StatsReset,
        DateTime FirstSeenAt,
        int SampleCount,
        bool StatsWereResetInWindow);

    /// <summary>
    /// Latest state per index, joined to that index's EARLIEST reading in the window.
    ///
    /// <para><b>Two different "unused" claims come out of this, and they are not the same claim.</b>
    /// <c>total_scans</c> is the server's own lifetime counter since its statistics were reset — the only
    /// number <c>pg_stat_user_indexes</c> can give anyone. <c>scans_in_window</c> is the difference across
    /// the stored samples, which is the more useful of the two and exists only because the store keeps
    /// history the server does not: an index with nine million lifetime scans and zero in ninety days is
    /// dead weight today, and no query against the live catalog can see that.</para>
    ///
    /// <para><c>scans_in_window</c> is <c>GREATEST(last - first, 0)</c>. The clamp is not decoration: a
    /// statistics reset inside the window makes the counter go backwards and an unclamped difference would
    /// report a NEGATIVE scan count, which would sort to the very top of a list whose whole purpose is to
    /// rank least-used first. The clamp alone would then hide the reset, so <c>stats_were_reset_in_window</c>
    /// travels beside it, taken from the server's own <c>stats_reset</c> timestamp moving between the two
    /// samples rather than inferred from the counters — a reset followed by enough scans to climb back past
    /// the old value leaves every difference positive and is invisible to the arithmetic.</para>
    ///
    /// <para><b><c>first_seen_at</c> and <c>sample_count</c> are what make the answer honest rather than
    /// confident.</b> An index created twenty minutes ago has zero scans for the same reason a genuinely
    /// dead one does, and PostgreSQL records no creation time anywhere — so the only available evidence is
    /// how long WE have been watching, which is exactly what these two columns are. The tool refuses to call
    /// anything unused on the strength of a window it has only one sample of.</para>
    ///
    /// <para>Ordered by the bytes of an index nothing scanned in the window, largest first, because the
    /// finding is the space and write cost being paid for nothing — and an index nobody has scanned is
    /// interesting in proportion to its size, not its name. Indexes that WERE scanned sort below all of
    /// them regardless of size.</para>
    ///
    /// <para>$1 server_id, $2/$3 window (naive UTC), $4 row limit.</para>
    /// </summary>
    public const string PgIndexUsageSql = """
        WITH latest AS (
            SELECT DISTINCT ON (database_name, schema_name, table_name, index_name)
                database_name, schema_name, table_name, index_name, collection_time,
                index_scans, tuples_read, tuples_fetched, blocks_read, blocks_hit,
                index_bytes, table_bytes, is_unique, is_primary_key, is_valid, is_ready,
                is_replica_identity, is_partial, is_expression, supports_constraint,
                index_method, column_count, index_definition, last_scan, stats_reset
            FROM pg_index_usage_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY database_name, schema_name, table_name, index_name, collection_time DESC
        ),
        earliest AS (
            SELECT DISTINCT ON (database_name, schema_name, table_name, index_name)
                database_name, schema_name, table_name, index_name,
                index_scans AS first_scans,
                stats_reset AS first_stats_reset,
                collection_time AS first_seen_at
            FROM pg_index_usage_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY database_name, schema_name, table_name, index_name, collection_time ASC
        ),
        samples AS (
            SELECT database_name, schema_name, table_name, index_name, count(*) AS sample_count
            FROM pg_index_usage_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY database_name, schema_name, table_name, index_name
        )
        SELECT
            l.database_name,
            l.schema_name,
            l.table_name,
            l.index_name,
            l.collection_time,
            l.index_scans                                        AS total_scans,
            /* Clamped per the reset argument above; the flag beside it says when the clamp fired for a
               reason rather than because nothing happened. */
            GREATEST(l.index_scans - e.first_scans, 0)           AS scans_in_window,
            l.tuples_read,
            l.tuples_fetched,
            l.blocks_read,
            l.blocks_hit,
            l.index_bytes,
            l.table_bytes,
            l.is_unique,
            l.is_primary_key,
            l.is_valid,
            l.is_ready,
            l.is_replica_identity,
            l.is_partial,
            l.is_expression,
            l.supports_constraint,
            l.index_method,
            l.column_count,
            l.index_definition,
            l.last_scan,
            l.stats_reset,
            e.first_seen_at,
            s.sample_count,
            /* IS DISTINCT FROM, not <>, because stats_reset is NULL until a database's statistics are
               reset for the first time — so the first reset a database ever has moves the column
               NULL -> timestamp, and <> would evaluate to NULL and miss precisely the case the counters
               also cannot see. */
            (l.stats_reset IS DISTINCT FROM e.first_stats_reset) AS stats_were_reset_in_window
        FROM latest AS l
        JOIN earliest AS e
          ON  e.database_name IS NOT DISTINCT FROM l.database_name
          AND e.schema_name   IS NOT DISTINCT FROM l.schema_name
          AND e.table_name    IS NOT DISTINCT FROM l.table_name
          AND e.index_name    IS NOT DISTINCT FROM l.index_name
        JOIN samples AS s
          ON  s.database_name IS NOT DISTINCT FROM l.database_name
          AND s.schema_name   IS NOT DISTINCT FROM l.schema_name
          AND s.table_name    IS NOT DISTINCT FROM l.table_name
          AND s.index_name    IS NOT DISTINCT FROM l.index_name
        /* Biggest unscanned index first. An INVALID index sorts above everything: it is the one case where
           "unused" and "safe to drop" genuinely coincide (the planner will not use it, writes still
           maintain it), so it should never be pushed under the LIMIT by a larger index that is merely
           idle. */
        ORDER BY
            l.is_valid ASC,
            CASE WHEN GREATEST(l.index_scans - e.first_scans, 0) = 0 THEN l.index_bytes ELSE -1 END DESC,
            l.index_bytes DESC
        LIMIT $4
        """;

    public static async Task<List<PgIndexUsageRow>> GetPgIndexUsageAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgIndexUsageRow>();
        await using var command = postgres.CreateCommand(PgIndexUsageSql);
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
            rows.Add(new PgIndexUsageRow(
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.GetDateTime(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                /* -1, not 0, for an unmeasured size: 0 would read as "this index occupies nothing", which
                   is a claim, where -1 is visibly not a byte count. */
                reader.IsDBNull(11) ? -1 : reader.GetInt64(11),
                reader.IsDBNull(12) ? -1 : reader.GetInt64(12),
                !reader.IsDBNull(13) && reader.GetBoolean(13),
                !reader.IsDBNull(14) && reader.GetBoolean(14),
                /* A NULL is_valid defaults to TRUE, deliberately the opposite direction from its
                   siblings: is_valid = false is what the tool reports as "safe to drop", so an absent
                   value must never manufacture that finding. */
                reader.IsDBNull(15) || reader.GetBoolean(15),
                reader.IsDBNull(16) || reader.GetBoolean(16),
                !reader.IsDBNull(17) && reader.GetBoolean(17),
                !reader.IsDBNull(18) && reader.GetBoolean(18),
                !reader.IsDBNull(19) && reader.GetBoolean(19),
                /* And here the safe direction is TRUE: supports_constraint = true is what STOPS the tool
                   recommending a drop, so an absent value must take the cautious side. */
                reader.IsDBNull(20) || reader.GetBoolean(20),
                reader.IsDBNull(21) ? null : reader.GetString(21),
                reader.IsDBNull(22) ? 0 : reader.GetInt32(22),
                reader.IsDBNull(23) ? null : reader.GetString(23),
                reader.IsDBNull(24) ? null : reader.GetDateTime(24),
                reader.IsDBNull(25) ? null : reader.GetDateTime(25),
                reader.GetDateTime(26),
                reader.IsDBNull(27) ? 0 : (int)reader.GetInt64(27),
                !reader.IsDBNull(28) && reader.GetBoolean(28)));
        }

        return rows;
    }

    /// <summary>
    /// How many index snapshots exist for this server in the window, and whether any exist at all — the
    /// honest-empty denominator.
    /// <para><c>pg_index_usage_stats</c> is a PERIODIC surface: the collector writes a row per index every
    /// cycle whatever the server is doing, so the presence of any stored sample is proof somebody looked.
    /// That is the opposite of the blocking and deadlock edge tables, where a row exists only because
    /// something went wrong and a data probe would report a healthy server as uncollected (#2508).</para>
    /// <para>It reads the same relation the main read walks and deliberately WITHOUT that read's ordering
    /// or limit, so it can neither report "collected" for rows the read cannot see nor report
    /// "uncollected" for a server whose indexes are simply all in use.</para>
    /// <para>$1 server_id, $2/$3 window (naive UTC).</para>
    /// </summary>
    public const string PgIndexUsageProbeSql = """
        SELECT
            (SELECT count(*) FROM pg_index_usage_stats
             WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3) AS rows_in_window,
            (SELECT count(DISTINCT collection_time) FROM pg_index_usage_stats
             WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3) AS snapshots_in_window,
            (SELECT count(*) FROM pg_index_usage_stats WHERE server_id = $1) AS rows_ever
        """;

    public sealed record PgIndexUsageProbe(long RowsInWindow, long SnapshotsInWindow, long RowsEver);

    public static async Task<PgIndexUsageProbe> ProbePgIndexUsageAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(PgIndexUsageProbeSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new PgIndexUsageProbe(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
        }

        return new PgIndexUsageProbe(0, 0, 0);
    }
}
