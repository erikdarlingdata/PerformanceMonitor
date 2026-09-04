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

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side reads for the index / object diagnostic-depth MCP tools
/// (<see cref="DarlingMcpObjectStatsTools"/>) — the SAME collected data Lite's <c>McpObjectStatsTools</c> /
/// <c>McpServerInfoTools</c> and the viewer read, adapted here so the MCP host never references the WPF
/// viewer project. All four read the LATEST daily snapshot (the collectors run <c>index_object_stats</c>
/// daily and <c>database_size_stats</c> hourly) via a correlated <c>MAX(collection_time)</c> — a STORED
/// read, no live monitored-server hit.
///
/// <para>
/// get_table_index_sizes / get_index_usage / get_object_locking all read <c>v_index_object_stats</c> (the
/// one collector table carries size + usage + locking columns together); get_database_sizes reads
/// <c>v_database_size_stats</c>. get_object_locking and get_database_sizes port the viewer's existing reads
/// (<c>GetIndexLockingAsync</c>, <c>GetDatabaseSizeLatestAsync</c>); get_table_index_sizes and
/// get_index_usage port Lite's server-wide reads (<c>GetObjectSizeGrowthAsync</c>, <c>GetIndexUsageAsync</c>)
/// which the viewer has only per-object variants of. Numeric MB columns are <c>numeric(19,2)</c> and CAST to
/// double precision for the typed reader. Every SQL string is a public const so Darling.Tests can pin the
/// dialect + columns without a live Postgres.
/// </para>
/// </summary>
internal static class DarlingObjectStatsReader
{
    /* ─────────────────────────── result rows ─────────────────────────── */

    /// <summary>One per-table size + growth row (indexes rolled up per table).</summary>
    public sealed record ObjectSizeGrowthRow(
        string DatabaseName, string SchemaName, string TableName, double CurrentReservedMb, double CurrentUsedMb,
        long TotalRows, int IndexCount, double Growth7dMb, double Growth30dMb, double DailyGrowthRateMb, double GrowthPct30d);

    /// <summary>One per-index usage row with its Unused / Write-only / Active classification.</summary>
    public sealed record IndexUsageRow(
        string DatabaseName, string SchemaName, string TableName, string? IndexName, string? IndexTypeDesc,
        double ReservedMb, long TotalRows, long UserSeeks, long UserScans, long UserLookups, long TotalReads,
        long UserUpdates, DateTime? LastUserAccess, string Classification);

    /// <summary>One per-index locking / latch-contention row.</summary>
    public sealed record IndexLockingRow(
        string DatabaseName, string SchemaName, string TableName, string? IndexName, string? IndexTypeDesc,
        double ReservedMb, long TotalRows, long RowLockWaitCount, long RowLockWaitInMs, long PageLockWaitCount,
        long PageLockWaitInMs, long IndexLockPromotionCount, long PageLatchWaitInMs, long PageIoLatchWaitInMs);

    /// <summary>One database file's latest size snapshot.</summary>
    public sealed record DatabaseSizeRow(
        DateTime CollectionTime, string DatabaseName, string? FileName, string? FileTypeDesc, double TotalSizeMb,
        double? UsedSizeMb, double? AutoGrowthMb, double? MaxSizeMb, string? VolumeMountPoint, double? VolumeTotalMb, double? VolumeFreeMb);

    /* ─────────────────────────── table / index sizes + growth ─────────────────────────── */

    /// <summary>
    /// Per-table size + growth over the daily snapshots — Lite's <c>GetObjectSizeGrowthAsync</c> ported to
    /// Postgres: roll indexes up per (database, schema, table) at the latest snapshot, compare against the
    /// newest snapshot at/older-than the 7-day ($2) and 30-day ($3) cutoffs (and the earliest snapshot as a
    /// fallback), and derive the daily rate from the span of collected data. Ranks by current reserved size
    /// descending, cap $4. $1 server_id.
    /// </summary>
    public const string ObjectSizeGrowthSql = """
        WITH boundaries AS (
            SELECT
                MAX(collection_time) AS latest_time,
                MIN(collection_time) AS earliest_time,
                CAST(MAX(collection_time) AS date) - CAST(MIN(collection_time) AS date) AS days_of_data
            FROM v_index_object_stats
            WHERE server_id = $1
        ),
        latest AS (
            SELECT database_name, schema_name, table_name,
                SUM(reserved_mb) AS current_reserved_mb,
                SUM(used_mb) AS current_used_mb,
                MAX(total_rows) AS total_rows,
                COUNT(*) AS index_count
            FROM v_index_object_stats
            WHERE server_id = $1 AND collection_time = (SELECT latest_time FROM boundaries)
            GROUP BY database_name, schema_name, table_name
        ),
        past_7d AS (
            SELECT database_name, schema_name, table_name, SUM(reserved_mb) AS reserved_mb
            FROM v_index_object_stats
            WHERE server_id = $1 AND collection_time = (
                SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1 AND collection_time <= $2)
            GROUP BY database_name, schema_name, table_name
        ),
        past_30d AS (
            SELECT database_name, schema_name, table_name, SUM(reserved_mb) AS reserved_mb
            FROM v_index_object_stats
            WHERE server_id = $1 AND collection_time = (
                SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1 AND collection_time <= $3)
            GROUP BY database_name, schema_name, table_name
        ),
        oldest AS (
            SELECT database_name, schema_name, table_name, SUM(reserved_mb) AS reserved_mb
            FROM v_index_object_stats
            WHERE server_id = $1 AND collection_time = (SELECT earliest_time FROM boundaries)
            GROUP BY database_name, schema_name, table_name
        )
        SELECT
            l.database_name,
            l.schema_name,
            l.table_name,
            CAST(l.current_reserved_mb AS double precision) AS current_reserved_mb,
            CAST(l.current_used_mb AS double precision) AS current_used_mb,
            l.total_rows,
            l.index_count,
            CAST(l.current_reserved_mb - COALESCE(p7.reserved_mb, o.reserved_mb, l.current_reserved_mb) AS double precision) AS growth_7d_mb,
            CAST(l.current_reserved_mb - COALESCE(p30.reserved_mb, p7.reserved_mb, o.reserved_mb, l.current_reserved_mb) AS double precision) AS growth_30d_mb,
            CASE WHEN b.days_of_data >= 1
                 THEN CAST(l.current_reserved_mb - COALESCE(o.reserved_mb, l.current_reserved_mb) AS double precision) / CAST(b.days_of_data AS double precision)
                 ELSE 0 END AS daily_growth_rate_mb,
            CASE WHEN COALESCE(p30.reserved_mb, p7.reserved_mb, o.reserved_mb) > 0
                 THEN CAST(l.current_reserved_mb - COALESCE(p30.reserved_mb, p7.reserved_mb, o.reserved_mb) AS double precision) * 100.0
                      / CAST(COALESCE(p30.reserved_mb, p7.reserved_mb, o.reserved_mb) AS double precision)
                 ELSE 0 END AS growth_pct_30d
        FROM latest l
        CROSS JOIN boundaries b
        LEFT JOIN past_7d p7 ON p7.database_name = l.database_name AND p7.schema_name = l.schema_name AND p7.table_name = l.table_name
        LEFT JOIN past_30d p30 ON p30.database_name = l.database_name AND p30.schema_name = l.schema_name AND p30.table_name = l.table_name
        LEFT JOIN oldest o ON o.database_name = l.database_name AND o.schema_name = l.schema_name AND o.table_name = l.table_name
        ORDER BY l.current_reserved_mb DESC
        LIMIT $4
        """;

    public static async Task<List<ObjectSizeGrowthRow>> GetObjectSizeGrowthAsync(
        NpgsqlDataSource postgres, int serverId, DateTime cutoff7dUtc, DateTime cutoff30dUtc, int top, CancellationToken cancellationToken = default)
    {
        var rows = new List<ObjectSizeGrowthRow>();
        await using var command = postgres.CreateCommand(ObjectSizeGrowthSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddTimestamp(command, cutoff7dUtc);
        DarlingMcpReadParameters.AddTimestamp(command, cutoff30dUtc);
        DarlingMcpReadParameters.AddInt(command, top);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ObjectSizeGrowthRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
                reader.IsDBNull(7) ? 0 : reader.GetDouble(7),
                reader.IsDBNull(8) ? 0 : reader.GetDouble(8),
                reader.IsDBNull(9) ? 0 : reader.GetDouble(9),
                reader.IsDBNull(10) ? 0 : reader.GetDouble(10)));
        }

        return rows;
    }

    /* ─────────────────────────── index usage ─────────────────────────── */

    /// <summary>
    /// Per-index usage at the latest snapshot — Lite's <c>GetIndexUsageAsync</c> ported to Postgres:
    /// seeks/scans/lookups/updates with the Unused / Write-only / Active classification, unused-first then
    /// largest reserved. Counters are cumulative since the last restart.
    /// <para>$1 server_id, $2 database filter (NULL = every database), $3 cap.</para>
    /// <para>#2636: the ordering and the cap interact badly and a field report found the sharp edge. Unused
    /// sorts ahead of everything SERVER-WIDE, so on an instance with 200+ unused indexes concentrated in one
    /// legacy database, the entire capped result is consumed by that database and every Active index in every
    /// other database is invisible — with nothing in the answer to say so. The reporter's database had
    /// healthy collection, full retention and zero returned rows, which reads exactly like a collection
    /// failure. The database filter is what makes the question answerable; the count below is what stops the
    /// answer being read as complete.</para>
    /// </summary>
    public const string IndexUsageSql = """
        SELECT
            database_name,
            schema_name,
            table_name,
            index_name,
            index_type_desc,
            CAST(reserved_mb AS double precision) AS reserved_mb,
            total_rows,
            COALESCE(user_seeks, 0) AS user_seeks,
            COALESCE(user_scans, 0) AS user_scans,
            COALESCE(user_lookups, 0) AS user_lookups,
            COALESCE(user_seeks, 0) + COALESCE(user_scans, 0) + COALESCE(user_lookups, 0) AS total_reads,
            COALESCE(user_updates, 0) AS user_updates,
            GREATEST(last_user_seek, last_user_scan, last_user_lookup, last_user_update) AS last_user_access,
            CASE
                WHEN COALESCE(user_seeks, 0) + COALESCE(user_scans, 0) + COALESCE(user_lookups, 0) = 0
                     AND COALESCE(user_updates, 0) = 0 THEN 'Unused'
                WHEN COALESCE(user_seeks, 0) + COALESCE(user_scans, 0) + COALESCE(user_lookups, 0) = 0
                     AND COALESCE(user_updates, 0) > 0 THEN 'Write-only'
                ELSE 'Active'
            END AS classification
        FROM v_index_object_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1)
        AND   ($2::text IS NULL OR database_name = $2::text)
        ORDER BY
            CASE WHEN COALESCE(user_seeks, 0) + COALESCE(user_scans, 0) + COALESCE(user_lookups, 0) = 0 THEN 0 ELSE 1 END,
            reserved_mb DESC
        LIMIT $3
        """;

    /// <summary>
    /// How many rows the same filter MATCHES, before the cap (#2636). Read alongside the rows so the answer
    /// can say it was truncated and by how much — the reporter's complaint was not that a cap exists, it was
    /// that nothing distinguished "not returned" from "not collected".
    /// <para>A second query rather than a window function over the first: the count has to be of the whole
    /// match, and a COUNT(*) OVER () inside a LIMITed statement returns the count of what survived the
    /// LIMIT — which is the exact mistake this is here to report.</para>
    /// </summary>
    public const string IndexUsageMatchCountSql = """
        SELECT count(*)
        FROM v_index_object_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1)
        AND   ($2::text IS NULL OR database_name = $2::text)
        """;

    /// <summary>
    /// The rows the cap allowed. <paramref name="databaseName"/> null means every database — the shape the
    /// tool had before #2636, kept so callers that genuinely want a server-wide sweep still get one.
    /// </summary>
    public static async Task<List<IndexUsageRow>> GetIndexUsageAsync(
        NpgsqlDataSource postgres, int serverId, int top, string? databaseName = null, CancellationToken cancellationToken = default)
    {
        var rows = new List<IndexUsageRow>();
        await using var command = postgres.CreateCommand(IndexUsageSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        command.Parameters.AddWithValue((object?)databaseName ?? DBNull.Value);
        DarlingMcpReadParameters.AddInt(command, top);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new IndexUsageRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? 0 : reader.GetDouble(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                reader.IsDBNull(12) ? null : reader.GetDateTime(12),
                reader.IsDBNull(13) ? "" : reader.GetString(13)));
        }

        return rows;
    }

    /// <summary>
    /// How many index rows the same server and database filter match at the latest snapshot, ignoring the
    /// cap (#2636).
    /// </summary>
    public static async Task<long> GetIndexUsageMatchCountAsync(
        NpgsqlDataSource postgres, int serverId, string? databaseName = null, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(IndexUsageMatchCountSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        command.Parameters.AddWithValue((object?)databaseName ?? DBNull.Value);

        return await command.ExecuteScalarAsync(cancellationToken) is long count ? count : 0;
    }

    /* ─────────────────────────── object locking ─────────────────────────── */

    /// <summary>
    /// Per-index locking / latch contention at each database's latest snapshot — the viewer's
    /// <c>IndexLockingAllSql</c> projected to the columns get_object_locking surfaces: only rows with a
    /// nonzero lock/latch wait or promotion, most contended first. Counters are cumulative since the last
    /// restart. $1 server_id, $2 cap.
    /// </summary>
    public const string IndexLockingSql = """
        WITH latest AS (
            SELECT database_name, MAX(collection_time) AS latest_time
            FROM v_index_object_stats
            WHERE server_id = $1
            GROUP BY database_name
        )
        SELECT
            ios.database_name,
            ios.schema_name,
            ios.table_name,
            ios.index_name,
            ios.index_type_desc,
            CAST(ios.reserved_mb AS double precision) AS reserved_mb,
            ios.total_rows,
            COALESCE(ios.row_lock_wait_count, 0) AS row_lock_wait_count,
            COALESCE(ios.row_lock_wait_in_ms, 0) AS row_lock_wait_in_ms,
            COALESCE(ios.page_lock_wait_count, 0) AS page_lock_wait_count,
            COALESCE(ios.page_lock_wait_in_ms, 0) AS page_lock_wait_in_ms,
            COALESCE(ios.index_lock_promotion_count, 0) AS index_lock_promotion_count,
            COALESCE(ios.page_latch_wait_in_ms, 0) AS page_latch_wait_in_ms,
            COALESCE(ios.page_io_latch_wait_in_ms, 0) AS page_io_latch_wait_in_ms
        FROM v_index_object_stats ios
        JOIN latest l ON l.database_name = ios.database_name AND l.latest_time = ios.collection_time
        WHERE ios.server_id = $1
        AND (
            COALESCE(ios.row_lock_wait_in_ms, 0) > 0
            OR COALESCE(ios.page_lock_wait_in_ms, 0) > 0
            OR COALESCE(ios.page_latch_wait_in_ms, 0) > 0
            OR COALESCE(ios.page_io_latch_wait_in_ms, 0) > 0
            OR COALESCE(ios.index_lock_promotion_count, 0) > 0
        )
        ORDER BY
            COALESCE(ios.row_lock_wait_in_ms, 0) + COALESCE(ios.page_lock_wait_in_ms, 0)
            + COALESCE(ios.page_latch_wait_in_ms, 0) + COALESCE(ios.page_io_latch_wait_in_ms, 0) DESC
        LIMIT $2
        """;

    public static async Task<List<IndexLockingRow>> GetIndexLockingAsync(
        NpgsqlDataSource postgres, int serverId, int top, CancellationToken cancellationToken = default)
    {
        var rows = new List<IndexLockingRow>();
        await using var command = postgres.CreateCommand(IndexLockingSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddInt(command, top);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new IndexLockingRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? 0 : reader.GetDouble(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                reader.IsDBNull(13) ? 0 : reader.GetInt64(13)));
        }

        return rows;
    }

    /* ─────────────────────────── database sizes ─────────────────────────── */

    /// <summary>
    /// The latest per-file database-size snapshot — the viewer's <c>DatabaseSizeLatestSql</c> projected to
    /// the columns Lite's get_database_sizes surfaces (plus <c>collection_time</c> for the envelope and
    /// <c>max_size_mb</c>). MB columns are <c>numeric(19,2)</c> → double precision. $1 server_id.
    /// </summary>
    public const string DatabaseSizeLatestSql = """
        SELECT
            collection_time,
            database_name,
            file_name,
            file_type_desc,
            CAST(total_size_mb AS double precision) AS total_size_mb,
            CAST(used_size_mb AS double precision) AS used_size_mb,
            CAST(auto_growth_mb AS double precision) AS auto_growth_mb,
            CAST(max_size_mb AS double precision) AS max_size_mb,
            volume_mount_point,
            CAST(volume_total_mb AS double precision) AS volume_total_mb,
            CAST(volume_free_mb AS double precision) AS volume_free_mb
        FROM v_database_size_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT MAX(collection_time) FROM v_database_size_stats WHERE server_id = $1)
        ORDER BY database_name, file_type_desc, file_name
        """;

    public static async Task<List<DatabaseSizeRow>> GetLatestDatabaseSizesAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<DatabaseSizeRow>();
        await using var command = postgres.CreateCommand(DatabaseSizeLatestSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new DatabaseSizeRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                reader.IsDBNull(5) ? null : reader.GetDouble(5),
                reader.IsDBNull(6) ? null : reader.GetDouble(6),
                reader.IsDBNull(7) ? null : reader.GetDouble(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.IsDBNull(9) ? null : reader.GetDouble(9),
                reader.IsDBNull(10) ? null : reader.GetDouble(10)));
        }

        return rows;
    }
}
