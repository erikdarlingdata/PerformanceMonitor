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
/// One per-table size + growth row (indexes rolled up per table), carrying the raw baselines the growth
/// figures are derived from rather than the derived figures alone (#3541 A12, contract rule 5).
/// <para>The SQL this replaced computed <c>growth_7d</c> / <c>growth_30d</c> / <c>growth_pct_30d</c> through a
/// <c>COALESCE(p30, p7, oldest, current)</c> chain, which is three lies in one expression: with ten days
/// of history "30-day growth" was growth since the SEVEN-day snapshot; with two days it was growth since
/// the oldest snapshot, still labelled 30d; and a table absent from every baseline (created this week)
/// fell through to <c>current - current = 0</c>, "not growing", for the one table that is nothing BUT
/// growth. A nominal window the store cannot reach is not a smaller window — it is no measurement, and
/// the payload has to say so. So the row carries each baseline as the store holds it (null where the
/// snapshot exists but the table was not in it, or where no snapshot old enough exists) plus the
/// store's span, and the derivations live in the properties below where each can refuse.</para>
/// </summary>
/// <param name="ReservedMb7dAgo">The table's reserved MB at the newest snapshot at or before the 7-day cutoff; null when
/// no such snapshot exists or the table was not in it.</param>
/// <param name="ReservedMb30dAgo">Same for the 30-day cutoff.</param>
/// <param name="ReservedMbOldest">The table's reserved MB at the store's EARLIEST snapshot; null when the table was not
/// in it (created since).</param>
/// <param name="Snapshot7dTime">The snapshot the 7-day baseline was read from; null when the store holds nothing that old.</param>
/// <param name="Snapshot30dTime">Same for 30 days.</param>
/// <param name="EarliestSnapshotTime">The store's oldest index_object_stats capture for this server.</param>
/// <param name="LatestSnapshotTime">The store's newest — the snapshot every current_* figure is read from.</param>
/// <param name="DaysOfData">Whole calendar days between the earliest and latest snapshots — how much history the
/// growth figures can honestly span. 0 means one day of snapshots: no growth is knowable.</param>
public sealed record TableSizeGrowthRow(
    string DatabaseName, string SchemaName, string TableName, double? CurrentReservedMb, double? CurrentUsedMb,
    long TotalRows, int IndexCount,
    double? ReservedMb7dAgo, double? ReservedMb30dAgo, double? ReservedMbOldest,
    DateTime? Snapshot7dTime, DateTime? Snapshot30dTime, DateTime EarliestSnapshotTime, DateTime LatestSnapshotTime, int DaysOfData)
{
    /// <summary>Growth since the 7-day baseline; null when there is no such baseline for this table.</summary>
    public double? Growth7dMb => ReservedMb7dAgo is { } b ? CurrentReservedMb - b : null;

    /// <summary>Growth since the 30-day baseline; null when there is no such baseline for this table.</summary>
    public double? Growth30dMb => ReservedMb30dAgo is { } b ? CurrentReservedMb - b : null;

    /// <summary>Percent growth over the 30-day baseline; null without a baseline, and null when the baseline
    /// is 0 (no denominator — a table that was empty 30 days ago has no ratio, not an infinite one).</summary>
    public double? GrowthPct30d => ReservedMb30dAgo is > 0 ? (CurrentReservedMb - ReservedMb30dAgo.Value) * 100.0 / ReservedMb30dAgo.Value : null;

    /// <summary>Growth since the store's earliest snapshot — the honest figure when the nominal windows
    /// are out of reach. Null when the store holds a single day (no span) or the table was not in the
    /// earliest snapshot.</summary>
    public double? GrowthOverAvailableHistoryMb => DaysOfData >= 1 && ReservedMbOldest is { } o ? CurrentReservedMb - o : null;

    /// <summary>Percent form of <see cref="GrowthOverAvailableHistoryMb"/>; null on a 0 baseline.</summary>
    public double? GrowthOverAvailableHistoryPct =>
        DaysOfData >= 1 && ReservedMbOldest is > 0 ? (CurrentReservedMb - ReservedMbOldest.Value) * 100.0 / ReservedMbOldest.Value : null;

    /// <summary>MB per day over the available span; null when there is no span to divide by.</summary>
    public double? DailyGrowthRateMb => GrowthOverAvailableHistoryMb is { } g ? g / DaysOfData : null;
}

public static class DarlingTableSizeGrowthReader
{
    /// <summary>The page the web tool and the Viewer grid show: the largest tables by reserved size. Callers ask for one more row to observe truncation.</summary>
    public const int TopTables = 100;

    /// <summary>
    /// Per-table size + growth over the daily snapshots — Lite's <c>GetObjectSizeGrowthAsync</c> ported to
    /// Postgres: roll indexes up per (database, schema, table) at the latest snapshot, and read the same
    /// table's reserved size at the newest snapshot at/older-than the 7-day ($2) and 30-day ($3) cutoffs and
    /// at the store's earliest snapshot. Ranks by current reserved size descending, cap $4. $1 server_id.
    /// <para>#5244: $5 is the database filter (a text[], NULL = every database; the shape of <see cref="DatabaseFilter.Clause"/>).
    /// It sits on the four snapshot CTEs and NOT on <c>boundaries</c>, so the snapshot anchors, the store's span and
    /// the 7-day and 30-day baselines stay the server's: a filtered and an unfiltered call read the same captures,
    /// and the cap $4 applies after the filter, so the page is the largest N tables of the chosen databases.</para>
    /// <para><b>Baselines are projected RAW, not folded (#3541 A12).</b> The previous shape derived the growth
    /// columns in SQL through <c>COALESCE(p30, p7, oldest, current)</c>, so a baseline the store did not hold
    /// was silently replaced by a nearer one and labelled with the farther window's name — and a table in no
    /// baseline at all read as growth 0. Each baseline now comes back as its own nullable column, beside the
    /// snapshot time it was read from and the store's span, and <see cref="TableSizeGrowthRow"/> derives
    /// each growth figure from exactly the baseline it names or declines to. The two cutoff snapshots are
    /// resolved once in <c>boundaries</c> with <c>FILTER</c> so the baseline CTEs and the projected snapshot
    /// times cannot disagree about which capture was used.</para>
    /// </summary>
    public const string Sql = """
        WITH boundaries AS (
            SELECT
                MAX(collection_time) AS latest_time,
                MIN(collection_time) AS earliest_time,
                CAST(MAX(collection_time) AS date) - CAST(MIN(collection_time) AS date) AS days_of_data,
                MAX(collection_time) FILTER (WHERE collection_time <= $2) AS snapshot_7d_time,
                MAX(collection_time) FILTER (WHERE collection_time <= $3) AS snapshot_30d_time
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
            AND ($5::text[] IS NULL OR database_name = ANY($5))
            AND LEFT(table_name, 1) IS DISTINCT FROM '#'
            GROUP BY database_name, schema_name, table_name
        ),
        past_7d AS (
            SELECT database_name, schema_name, table_name, SUM(reserved_mb) AS reserved_mb
            FROM v_index_object_stats
            WHERE server_id = $1 AND collection_time = (SELECT snapshot_7d_time FROM boundaries)
            AND ($5::text[] IS NULL OR database_name = ANY($5))
            GROUP BY database_name, schema_name, table_name
        ),
        past_30d AS (
            SELECT database_name, schema_name, table_name, SUM(reserved_mb) AS reserved_mb
            FROM v_index_object_stats
            WHERE server_id = $1 AND collection_time = (SELECT snapshot_30d_time FROM boundaries)
            AND ($5::text[] IS NULL OR database_name = ANY($5))
            GROUP BY database_name, schema_name, table_name
        ),
        oldest AS (
            SELECT database_name, schema_name, table_name, SUM(reserved_mb) AS reserved_mb
            FROM v_index_object_stats
            WHERE server_id = $1 AND collection_time = (SELECT earliest_time FROM boundaries)
            AND ($5::text[] IS NULL OR database_name = ANY($5))
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
            CAST(p7.reserved_mb AS double precision) AS reserved_mb_7d_ago,
            CAST(p30.reserved_mb AS double precision) AS reserved_mb_30d_ago,
            CAST(o.reserved_mb AS double precision) AS reserved_mb_oldest,
            b.snapshot_7d_time,
            b.snapshot_30d_time,
            b.earliest_time,
            b.latest_time,
            b.days_of_data
        FROM latest l
        CROSS JOIN boundaries b
        LEFT JOIN past_7d p7 ON p7.database_name = l.database_name AND p7.schema_name = l.schema_name AND p7.table_name = l.table_name
        LEFT JOIN past_30d p30 ON p30.database_name = l.database_name AND p30.schema_name = l.schema_name AND p30.table_name = l.table_name
        LEFT JOIN oldest o ON o.database_name = l.database_name AND o.schema_name = l.schema_name AND o.table_name = l.table_name
        ORDER BY l.current_reserved_mb DESC NULLS LAST, l.database_name, l.schema_name, l.table_name
        LIMIT $4
        """;

    /// <summary>
    /// The read shared by <c>get_table_index_sizes</c> (the web endpoint's MCP tool) and the Viewer's Table &amp; Index Sizes
    /// grid (#6388), moved here from the Service project's <c>DarlingObjectStatsReader</c> unchanged except that the command
    /// deadline is passed in. Rows of the chosen databases only, capped after the filter; the snapshot anchors stay the server's.
    /// </summary>
    public static async Task<List<TableSizeGrowthRow>> GetAsync(
        NpgsqlDataSource postgres, int serverId, DateTime cutoff7dUtc, DateTime cutoff30dUtc, int top, DatabaseFilter databases,
        int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var rows = new List<TableSizeGrowthRow>();
        await using var command = postgres.CreateCommand(Sql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff7dUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff30dUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = top });
        command.Parameters.Add(databases.Parameter());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new TableSizeGrowthRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                /* #5633 item 15: a table with no measured size stays null, never 0 (the sort puts it last, below). */
                reader.IsDBNull(3) ? null : reader.GetDouble(3),
                reader.IsDBNull(4) ? null : reader.GetDouble(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
                /* The baselines stay NULL when the store has none — a missing baseline is not a 0 baseline. */
                reader.IsDBNull(7) ? null : reader.GetDouble(7),
                reader.IsDBNull(8) ? null : reader.GetDouble(8),
                reader.IsDBNull(9) ? null : reader.GetDouble(9),
                reader.IsDBNull(10) ? null : reader.GetDateTime(10),
                reader.IsDBNull(11) ? null : reader.GetDateTime(11),
                reader.GetDateTime(12),
                reader.GetDateTime(13),
                Convert.ToInt32(reader.GetValue(14))));
        }

        return rows;
    }
}
