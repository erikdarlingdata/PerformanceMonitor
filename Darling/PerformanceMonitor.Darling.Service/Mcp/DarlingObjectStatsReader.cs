/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

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
    public sealed record ObjectSizeGrowthRow(
        string DatabaseName, string SchemaName, string TableName, double CurrentReservedMb, double CurrentUsedMb,
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

    /// <summary>One per-index usage row with its Unused / Write-only / Active classification.</summary>
    public sealed record IndexUsageRow(
        string DatabaseName, string SchemaName, string TableName, string? IndexName, string? IndexTypeDesc,
        double ReservedMb, long TotalRows, long UserSeeks, long UserScans, long UserLookups, long TotalReads,
        long UserUpdates, DateTime? LastUserAccessUtc, string Classification);

    /// <summary>One index's locking / latch contention at the server's latest capture.
    /// <para><b>#3880: <c>CollectionTime</c> is the snapshot's own stamp</b>, projected on the row statement
    /// (never re-read with a second <c>MAX()</c>, which could stamp the NEXT capture) so
    /// <c>get_object_locking</c> can publish <c>captured_at</c> the way every other stamped latest read does.
    /// It is first in the positional list for the same reason <see cref="DatabaseSizeRow"/> carries it first:
    /// the stamp is a property of the capture, not of the index.</para></summary>
    public sealed record IndexLockingRow(
        DateTime CollectionTime,
        string DatabaseName, string SchemaName, string TableName, string? IndexName, string? IndexTypeDesc,
        double ReservedMb, long TotalRows, long RowLockWaitCount, long RowLockWaitInMs, long PageLockWaitCount,
        long PageLockWaitInMs, long IndexLockPromotionCount, long PageLatchWaitInMs, long PageIoLatchWaitInMs);

    /// <summary>
    /// #5311: the four counters of one index's locking detail pane, the ones the Locking list leaves out (row lock count,
    /// page lock count, page latch wait count, page I/O latch wait count). <c>CollectionTime</c> is the snapshot's own
    /// stamp, first for the reason <see cref="IndexLockingRow"/> carries it first.
    /// </summary>
    public sealed record IndexLockingDetailRow(
        DateTime CollectionTime,
        string DatabaseName, string SchemaName, string TableName, string? IndexName,
        long RowLockCount, long PageLockCount, long PageLatchWaitCount, long PageIoLatchWaitCount);

    /// <summary>One database file's latest size snapshot. <c>TotalSizeMb</c> is null for the LOG file of an Azure SQL
    /// Database Hyperscale database (the log service): see <see cref="PerformanceMonitor.Common.HyperscaleLogSize"/>.
    /// <c>FileId</c> is null for the one row another database on an Azure SQL Database server gets, which holds
    /// that database's data size only: see <see cref="PerformanceMonitor.Common.AzureSiblingDatabaseSize"/>.</summary>
    public sealed record DatabaseSizeRow(
        DateTime CollectionTime, string DatabaseName, string? FileName, string? FileTypeDesc, double? TotalSizeMb,
        double? UsedSizeMb, double? AutoGrowthMb, double? MaxSizeMb, string? VolumeMountPoint, double? VolumeTotalMb, double? VolumeFreeMb,
        int? FileId = null)
    {
        /// <summary>True for the one row another database on an Azure SQL Database server gets: it holds the
        /// database's data size, and its log size is not reported.</summary>
        public bool IsAzureSiblingRow => PerformanceMonitor.Common.AzureSiblingDatabaseSize.IsSiblingRow(FileId, FileName);
    }

    /* ─────────────────────────── table / index sizes + growth ─────────────────────────── */

    /// <summary>
    /// Per-table size + growth over the daily snapshots — Lite's <c>GetObjectSizeGrowthAsync</c> ported to
    /// Postgres: roll indexes up per (database, schema, table) at the latest snapshot, and read the same
    /// table's reserved size at the newest snapshot at/older-than the 7-day ($2) and 30-day ($3) cutoffs and
    /// at the store's earliest snapshot. Ranks by current reserved size descending, cap $4. $1 server_id.
    /// <para><b>Baselines are projected RAW, not folded (#3541 A12).</b> The previous shape derived the growth
    /// columns in SQL through <c>COALESCE(p30, p7, oldest, current)</c>, so a baseline the store did not hold
    /// was silently replaced by a nearer one and labelled with the farther window's name — and a table in no
    /// baseline at all read as growth 0. Each baseline now comes back as its own nullable column, beside the
    /// snapshot time it was read from and the store's span, and <see cref="ObjectSizeGrowthRow"/> derives
    /// each growth figure from exactly the baseline it names or declines to. The two cutoff snapshots are
    /// resolved once in <c>boundaries</c> with <c>FILTER</c> so the baseline CTEs and the projected snapshot
    /// times cannot disagree about which capture was used.</para>
    /// </summary>
    public const string ObjectSizeGrowthSql = """
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
            GROUP BY database_name, schema_name, table_name
        ),
        past_7d AS (
            SELECT database_name, schema_name, table_name, SUM(reserved_mb) AS reserved_mb
            FROM v_index_object_stats
            WHERE server_id = $1 AND collection_time = (SELECT snapshot_7d_time FROM boundaries)
            GROUP BY database_name, schema_name, table_name
        ),
        past_30d AS (
            SELECT database_name, schema_name, table_name, SUM(reserved_mb) AS reserved_mb
            FROM v_index_object_stats
            WHERE server_id = $1 AND collection_time = (SELECT snapshot_30d_time FROM boundaries)
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

    /* ─────────────────────────── index usage ─────────────────────────── */

    /// <summary>
    /// Per-index usage at the latest snapshot — Lite's <c>GetIndexUsageAsync</c> ported to Postgres:
    /// seeks/scans/lookups/updates with the Unused / Write-only / Active classification, unused-first then
    /// largest reserved. Counters are cumulative since the last restart.
    /// <para>$1 server_id, $2 database filter (a text[], NULL = every database; #5245, the shape of <see cref="DatabaseFilter.Clause"/>), $3 cap.</para>
    /// <para>#2636: the ordering and the cap interact badly and a field report found the sharp edge. Unused
    /// sorts ahead of everything SERVER-WIDE, so on an instance with 200+ unused indexes concentrated in one
    /// legacy database, the entire capped result is consumed by that database and every Active index in every
    /// other database is invisible — with nothing in the answer to say so. The reporter's database had
    /// healthy collection, full retention and zero returned rows, which reads exactly like a collection
    /// failure. The database filter is what makes the question answerable; the count below is what stops the
    /// answer being read as complete.</para>
    /// <para><b><c>last_user_access</c> comes back as the server's local clock and is converted to naive UTC in
    /// C#.</b> The four columns it is the <c>GREATEST</c> of come straight off <c>sys.dm_db_index_usage_stats</c>
    /// (<c>IndexObjectStatsCollector</c> ships <c>us.last_user_seek</c> and its three siblings verbatim), so
    /// the stored values are the monitored server's LOCAL wall clock. <c>GREATEST</c> ignoring NULLs is what is
    /// wanted here — an index used in only one of the four ways still reports that one access — and it stays
    /// NULL when all four are. <see cref="MapIndexUsageRow"/> then converts the result through the server's
    /// <see cref="ServerClock"/> (<see cref="DarlingServerClockReader"/>): its time zone where SQL Server reports
    /// one, else the newest collected offset. This read returns NO other timestamp, which is why converting
    /// rather than labelling matters more here than elsewhere: there is nothing else in the payload for a
    /// reader to notice a disagreement against.</para>
    /// <para><b>The conversion follows the server's time zone, and this is the read where that matters most
    /// (#4793).</b> <c>sys.dm_db_index_usage_stats</c> persists since the instance restarted, which on a stable
    /// production box is routinely months, so a large share of values predate the most recent daylight saving
    /// change. Subtracting the ONE newest offset put those values 60 minutes early — silently, and in the
    /// plausible direction. Any target in a DST-observing zone had this, a target configured to UTC did not, and
    /// on AWS RDS the instance takes its time zone from a creation-time parameter, so a non-UTC zone is an
    /// ordinary configuration. #2932 records the measured offset behind the four-hour figure quoted above.
    /// Known edge: the <c>GREATEST</c> of the four stored local times is taken before converting, which equals
    /// converting each first except inside the repeated hour of a fall back, where a local time cannot say which
    /// occurrence it was and takes the first.</para>
    /// <para>The alias deliberately does NOT carry a <c>_utc</c> suffix, unlike the other fifteen. This one
    /// is a projection alias rather than a column, and <c>ConsumedTimestampFrameDisciplineTests</c> reaches
    /// the payload field through the alias — a suffix here would make the field name and the alias diverge
    /// and drop the site out of that census. The conversion is pinned directly by
    /// <c>EveryDeSkewedAtReadSite_CarriesItsConversionInTheReaderItDependsOn</c>, which is a stronger claim
    /// than a suffix nothing checks.</para>
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
        AND   ($2::text[] IS NULL OR database_name = ANY($2))
        ORDER BY
            CASE WHEN COALESCE(user_seeks, 0) + COALESCE(user_scans, 0) + COALESCE(user_lookups, 0) = 0 THEN 0 ELSE 1 END,
            reserved_mb DESC NULLS LAST,
            database_name,
            schema_name,
            table_name,
            index_name NULLS LAST
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
        AND   ($2::text[] IS NULL OR database_name = ANY($2))
        """;

    /// <summary>
    /// The rows the cap allowed. <paramref name="databaseName"/> null means every database — the shape the
    /// tool had before #2636, kept so callers that genuinely want a server-wide sweep still get one. One name:
    /// <see cref="GetIndexUsageAsync(NpgsqlDataSource,int,int,DatabaseFilter,CancellationToken)"/> with
    /// <see cref="DatabaseFilter.One"/>.
    /// </summary>
    public static Task<List<IndexUsageRow>> GetIndexUsageAsync(
        NpgsqlDataSource postgres, int serverId, int top, string? databaseName = null, CancellationToken cancellationToken = default) =>
        GetIndexUsageAsync(postgres, serverId, top, DatabaseFilter.One(databaseName), cancellationToken);

    /// <summary>
    /// #5245: <see cref="GetIndexUsageAsync(NpgsqlDataSource,int,int,string,CancellationToken)"/> over a SET of databases.
    /// <paramref name="databases"/> is <see cref="DatabaseFilter.All"/> for every database; otherwise the rows are
    /// the unused-first, largest-first top <paramref name="top"/> of the CHOSEN databases only (the cap applies after
    /// the filter, in SQL). The snapshot anchor stays the server's newest capture, so a filtered and an unfiltered
    /// call read the same capture.
    /// </summary>
    public static async Task<List<IndexUsageRow>> GetIndexUsageAsync(
        NpgsqlDataSource postgres, int serverId, int top, DatabaseFilter databases, CancellationToken cancellationToken = default)
    {
        var rows = new List<IndexUsageRow>();
        var clock = await DarlingServerClockReader.ReadAsync(postgres, serverId, cancellationToken);
        await using var command = postgres.CreateCommand(IndexUsageSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        command.Parameters.Add(databases.Parameter());
        DarlingMcpReadParameters.AddInt(command, top);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(MapIndexUsageRow(reader, clock));
        }

        return rows;
    }

    /// <summary>Maps one row of <see cref="IndexUsageSql"/> (14 columns, in the SELECT's order).</summary>
    internal static IndexUsageRow MapIndexUsageRow(DbDataReader reader, ServerClock clock) =>
        new(
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
            DarlingServerClockReader.ToUtc(clock, reader, 12),
            reader.IsDBNull(13) ? "" : reader.GetString(13));

    /// <summary>
    /// How many index rows the same server and database filter match at the latest snapshot, ignoring the
    /// cap (#2636). One name, or null for every database.
    /// </summary>
    public static Task<long> GetIndexUsageMatchCountAsync(
        NpgsqlDataSource postgres, int serverId, string? databaseName = null, CancellationToken cancellationToken = default) =>
        GetIndexUsageMatchCountAsync(postgres, serverId, DatabaseFilter.One(databaseName), cancellationToken);

    /// <summary>
    /// #5245: <see cref="GetIndexUsageMatchCountAsync(NpgsqlDataSource,int,string,CancellationToken)"/> over a SET of
    /// databases: the count of the CHOSEN databases' rows, the population
    /// <see cref="GetIndexUsageAsync(NpgsqlDataSource,int,int,DatabaseFilter,CancellationToken)"/> pages over.
    /// <see cref="DatabaseFilter.All"/> is the server-wide count.
    /// </summary>
    public static async Task<long> GetIndexUsageMatchCountAsync(
        NpgsqlDataSource postgres, int serverId, DatabaseFilter databases, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(IndexUsageMatchCountSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        command.Parameters.Add(databases.Parameter());

        return await command.ExecuteScalarAsync(cancellationToken) is long count ? count : 0;
    }

    /* ─────────────────────────── object locking ─────────────────────────── */

    /// <summary>
    /// Per-index locking / latch contention at the SERVER's latest capture — the viewer's
    /// <c>IndexLockingAllSql</c> projected to the columns get_object_locking surfaces: only rows with a
    /// nonzero lock/latch wait or promotion, most contended first. Counters are cumulative since the last
    /// restart. $1 server_id, $2 database filter (a text[], NULL = every database; #5245), $3 cap.
    ///
    /// <para><b>#3878: this read used to resolve "latest" PER <c>database_name</c></b> —
    /// <c>MAX(collection_time)</c> GROUPed BY the name string, joined back to the rows — which made every
    /// name the store has ever seen its own immortal group. A database renamed away keeps a group whose
    /// newest row is the last capture before the rename, so the read returned it forever, and an agent
    /// calling <c>get_object_locking</c> was handed month-dead database names as live peers (#3876 is the
    /// field report against Lite's identical port; #3877 fixed that half). The grouping was meant to keep a
    /// database visible when it missed the newest pass, and it cannot: one collector run stamps every
    /// database it collects with a single <c>collection_time</c>, so for a database present in the newest
    /// pass the per-name MAX IS the server-wide MAX — identical rows — while for one absent from it the
    /// grouping adds nothing but a name that is gone. The anchor is now the server's newest capture, which
    /// is how <see cref="IndexUsageSql"/> one screen up and <see cref="DatabaseSizeLatestSql"/> below have
    /// always resolved it, and how <see cref="ObjectSizeGrowthSql"/>'s <c>boundaries</c> resolves its own:
    /// one instant, one answer about which databases exist. Capture-time names stay in the store as the
    /// history they honestly are — nothing is rewritten, it is only no longer read as the present.</para>
    ///
    /// <para><b>#3880 — the anchor column is PROJECTED, which is what lets the tool above stamp itself.</b>
    /// Making this read a one-instant snapshot (above) is precisely what made it visible to the
    /// latest-anchored-read census in <c>McpPayloadContractCensusTests</c>: a per-name MAX group inside a CTE
    /// picks a SET, and that census deliberately ignores such an anchor, so the defect had been hiding the
    /// read from the very rule it broke. #3879's lane answered the census's standing question by rostering
    /// this constant on <c>LatestAnchoredReadsWithoutTheirStamp</c> beside the two <c>IndexUsage*</c> reads,
    /// on the pre-existing-debt argument. <b>Erik ruled the other way:</b> stamp it, do not roster it — the
    /// census is shrink-only by design, and a read that now resolves ONE instant can say which instant.
    /// <c>ios.collection_time</c> therefore comes back on the ROW statement, the same shape
    /// <see cref="DatabaseSizeLatestSql"/> below has always had; the roster entry is gone and
    /// <c>get_object_locking</c> publishes <c>captured_at</c>. A second <c>MAX(collection_time)</c> read to
    /// fetch the stamp would have been the dishonest alternative: it can resolve to the NEXT capture landing
    /// between the two queries, which is the rule <c>McpLatestSnapshotStampTests</c> pins per read.</para>
    /// <para>#5372 L1: the order ends in a total one (lock promotions, then database, schema, table and index name, as
    /// <c>get_index_usage</c> does), so two runs over a set that ties at the cap return the same rows. The rows listed
    /// only for a lock promotion all sum to 0 and tie.</para>
    /// </summary>
    public const string IndexLockingSql = """
        SELECT
            ios.collection_time,
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
        WHERE ios.server_id = $1
        AND   ios.collection_time = (SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1)
        AND (
            COALESCE(ios.row_lock_wait_in_ms, 0) > 0
            OR COALESCE(ios.page_lock_wait_in_ms, 0) > 0
            OR COALESCE(ios.page_latch_wait_in_ms, 0) > 0
            OR COALESCE(ios.page_io_latch_wait_in_ms, 0) > 0
            OR COALESCE(ios.index_lock_promotion_count, 0) > 0
        )
        AND   ($2::text[] IS NULL OR ios.database_name = ANY($2))
        ORDER BY
            COALESCE(ios.row_lock_wait_in_ms, 0) + COALESCE(ios.page_lock_wait_in_ms, 0)
            + COALESCE(ios.page_latch_wait_in_ms, 0) + COALESCE(ios.page_io_latch_wait_in_ms, 0) DESC,
            COALESCE(ios.index_lock_promotion_count, 0) DESC,
            ios.database_name, ios.schema_name, ios.table_name, ios.index_name NULLS LAST
        LIMIT $3
        """;

    /// <summary>
    /// The newest stored <c>is_optimized_locking_on</c> flag per database on the server. The anchor is the newest
    /// <c>capture_time</c> of the whole server, as <see cref="DarlingCurrentConfigReader.DatabaseConfigSql"/> reads
    /// it: one collection run writes every database with one capture time, so that capture is the server's whole
    /// snapshot, and a dropped database's old true flag does not outlive it. <c>capture_time</c> is projected so the
    /// latest-anchor census sees the anchor. A NULL flag means unknown. $1 server_id, $2 database filter (a text[], NULL = every database; #5245: the
    /// anchor stays the server's capture, only WHICH databases' flags count changes).
    /// </summary>
    public const string OptimizedLockingFlagsSql = """
        SELECT is_optimized_locking_on, capture_time
        FROM database_config
        WHERE server_id = $1
        AND   capture_time = (SELECT MAX(capture_time) FROM database_config WHERE server_id = $1)
        AND   ($2::text[] IS NULL OR database_name = ANY($2))
        """;

    /// <summary>The shared optimized-locking note when any database's newest flag is true; null otherwise. #5245: with a
    /// <paramref name="databases"/> selection only the CHOSEN databases' flags count, so the note never warns about a
    /// database the page does not show.</summary>
    public static Task<string?> GetOptimizedLockingNoteAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default) =>
        GetOptimizedLockingNoteAsync(postgres, serverId, DatabaseFilter.All, cancellationToken);

    /// <summary>The note over a selection: see <see cref="GetOptimizedLockingNoteAsync(NpgsqlDataSource,int,CancellationToken)"/>;
    /// only the <paramref name="databases"/> chosen count (#5245).</summary>
    public static async Task<string?> GetOptimizedLockingNoteAsync(
        NpgsqlDataSource postgres, int serverId, DatabaseFilter databases, CancellationToken cancellationToken = default)
    {
        var flags = new List<bool?>();
        await using var command = postgres.CreateCommand(OptimizedLockingFlagsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        command.Parameters.Add(databases.Parameter());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            flags.Add(reader.IsDBNull(0) ? null : reader.GetBoolean(0));
        return OptimizedLockingNote.For(flags);
    }

    /// <summary>The server's latest-capture locking rows over every database, most contended first, at most
    /// <paramref name="top"/>: <see cref="GetIndexLockingAsync(NpgsqlDataSource,int,int,DatabaseFilter,CancellationToken)"/>
    /// with <see cref="DatabaseFilter.All"/>.</summary>
    public static Task<List<IndexLockingRow>> GetIndexLockingAsync(
        NpgsqlDataSource postgres, int serverId, int top, CancellationToken cancellationToken = default) =>
        GetIndexLockingAsync(postgres, serverId, top, DatabaseFilter.All, cancellationToken);

    /// <summary>
    /// #5245: the server's latest-capture locking rows, most contended first, at most <paramref name="top"/>.
    /// <paramref name="databases"/> is <see cref="DatabaseFilter.All"/> for every database; otherwise only the CHOSEN
    /// databases' rows are ranked and capped, so the page is the top N of those databases. The snapshot anchor stays
    /// the server's newest capture (a filter picks rows, it never picks a different capture), so a filtered and an
    /// unfiltered call read the same instant. The names bind as one text[] and are never spliced into the statement.
    /// </summary>
    public static async Task<List<IndexLockingRow>> GetIndexLockingAsync(
        NpgsqlDataSource postgres, int serverId, int top, DatabaseFilter databases, CancellationToken cancellationToken = default)
    {
        var rows = new List<IndexLockingRow>();
        await using var command = postgres.CreateCommand(IndexLockingSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        command.Parameters.Add(databases.Parameter());
        DarlingMcpReadParameters.AddInt(command, top);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new IndexLockingRow(
                /* #3880: ordinal 0 is the snapshot's stamp, the read's own anchor column. */
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? "" : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? 0 : reader.GetDouble(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                reader.IsDBNull(14) ? 0 : reader.GetInt64(14)));
        }

        return rows;
    }

    /// <summary>
    /// #5311: ONE index's four detail counters at the server's latest capture, for the web Locking page's detail pane
    /// (the desktop's <c>ShowLockingDetail</c>). The index is named exactly by database ($2, the database filter's one
    /// <c>text[]</c> parameter as the list read binds it), schema ($3), table ($4) and index name ($5; NULL names a heap,
    /// which is why it compares with <c>IS NOT DISTINCT FROM</c>). Every name is a bound parameter, never part of the
    /// statement text. The snapshot anchor is the list read's: the server's newest capture, so a detail row is the same
    /// instant the list row it came from showed.
    /// </summary>
    public const string IndexLockingDetailSql = """
        SELECT
            ios.collection_time,
            ios.database_name,
            ios.schema_name,
            ios.table_name,
            ios.index_name,
            COALESCE(ios.row_lock_count, 0) AS row_lock_count,
            COALESCE(ios.page_lock_count, 0) AS page_lock_count,
            COALESCE(ios.page_latch_wait_count, 0) AS page_latch_wait_count,
            COALESCE(ios.page_io_latch_wait_count, 0) AS page_io_latch_wait_count
        FROM v_index_object_stats ios
        WHERE ios.server_id = $1
        AND   ios.collection_time = (SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1)
        AND   ($2::text[] IS NULL OR ios.database_name = ANY($2))
        AND   ios.schema_name = $3
        AND   ios.table_name = $4
        AND   ios.index_name IS NOT DISTINCT FROM $5
        LIMIT 1
        """;

    /// <summary>
    /// #5311: the one index <see cref="IndexLockingDetailSql"/> names, or null when the latest capture holds no such
    /// index. <paramref name="database"/> must name a database (a blank name would be the "every database" filter, and
    /// a selector never widens): the caller refuses a blank one.
    /// </summary>
    public static async Task<IndexLockingDetailRow?> GetIndexLockingDetailAsync(
        NpgsqlDataSource postgres, int serverId, DatabaseFilter database, string schema, string table, string? index,
        CancellationToken cancellationToken = default)
    {
        if (database.IsAll) throw new ArgumentException("A detail read names one database.", nameof(database));
        await using var command = postgres.CreateCommand(IndexLockingDetailSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        command.Parameters.Add(database.Parameter());
        DarlingMcpReadParameters.AddText(command, schema);
        DarlingMcpReadParameters.AddText(command, table);
        DarlingMcpReadParameters.AddNullableText(command, index);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        return new IndexLockingDetailRow(
            reader.GetDateTime(0),
            reader.IsDBNull(1) ? "" : reader.GetString(1),
            reader.IsDBNull(2) ? "" : reader.GetString(2),
            reader.IsDBNull(3) ? "" : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
            reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
            reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
            reader.IsDBNull(8) ? 0 : reader.GetInt64(8));
    }

    /* ─────────────────────────── database sizes ─────────────────────────── */

    /// <summary>
    /// The latest per-file database-size snapshot — the viewer's <c>DatabaseSizeLatestSql</c> projected to
    /// the columns Lite's get_database_sizes surfaces (plus <c>collection_time</c> for the envelope and
    /// <c>max_size_mb</c>). MB columns are <c>numeric(19,2)</c> → double precision. $1 server_id,
    /// $2 collection_time (the snapshot <see cref="GetLatestSnapshotTimeAsync"/> resolved).
    ///
    /// <para>#4245: this used to bind <c>collection_time</c> to a correlated <c>MAX(collection_time)</c>
    /// subquery with no bound of its own, so TimescaleDB had to build a subplan for every retained
    /// <c>database_size_stats</c> chunk before it could even start deciding which one held the answer — 148 ms
    /// of planning against 2.6 ms of execution in the field, over 71 chunks. <see cref="GetLatestSnapshotTimeAsync"/>
    /// resolves the snapshot as its own round trip first, so this statement only ever binds a literal
    /// <c>collection_time</c> the planner can exclude every other chunk against at plan time.</para>
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
            CAST(volume_free_mb AS double precision) AS volume_free_mb,
            file_id
        FROM v_database_size_stats
        WHERE server_id = $1
        AND   collection_time = $2
        ORDER BY database_name, file_type_desc, file_name
        """;

    /// <summary>The windowed half of <see cref="GetLatestSnapshotTimeAsync"/>: bounded below so the planner can
    /// exclude every chunk outside the window at plan time. $1 server_id, $2 window start. No upper bound: this
    /// is a *latest* read, and bounding above by "now" would hide a snapshot the collector host stamped a few
    /// minutes ahead of this reader's own clock (#4245 follow-up).</summary>
    public const string DatabaseSizeLatestSnapshotWindowedProbeSql = """
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        """;

    /// <summary>The fully unbounded fallback half of <see cref="GetLatestSnapshotTimeAsync"/>, reached only
    /// when the windowed probe finds nothing. $1 server_id. No bound at all — the same shape the pre-#4245
    /// correlated subquery had, reached only for the rare stale-or-empty-server case.</summary>
    public const string DatabaseSizeLatestSnapshotFallbackProbeSql = """
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
        """;

    public static async Task<List<DatabaseSizeRow>> GetLatestDatabaseSizesAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<DatabaseSizeRow>();
        var snapshotTime = await GetLatestSnapshotTimeAsync(postgres, serverId, cancellationToken);
        if (snapshotTime is null)
        {
            return rows;
        }

        await using var command = postgres.CreateCommand(DatabaseSizeLatestSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddTimestamp(command, snapshotTime.Value);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new DatabaseSizeRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                /* NULL is the Hyperscale log file: it stays null, never 0. */
                reader.IsDBNull(4) ? null : reader.GetDouble(4),
                reader.IsDBNull(5) ? null : reader.GetDouble(5),
                reader.IsDBNull(6) ? null : reader.GetDouble(6),
                reader.IsDBNull(7) ? null : reader.GetDouble(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.IsDBNull(9) ? null : reader.GetDouble(9),
                reader.IsDBNull(10) ? null : reader.GetDouble(10),
                /* NULL is the one row another database on an Azure SQL Database server gets: it has no file id. */
                reader.IsDBNull(11) ? null : reader.GetInt32(11)));
        }

        return rows;
    }

    /// <summary>The newest <c>database_size_stats</c> snapshot for one server (#4245): a windowed probe first
    /// — <c>collection_time</c> bound to the last two days, letting the planner exclude every other chunk at
    /// plan time — falling back to an unbounded probe only when the window is empty (a server whose
    /// collection stopped more than two days ago, which the windowed probe alone cannot tell apart from
    /// "never collected"). Two days is generous headroom over the roughly hourly collection cadence
    /// (<c>CollectorScheduleDefaults["database_size_stats"]</c>) while still excluding nearly all of a
    /// retention that runs to 70+ daily chunks in the field. Correctness: the windowed probe's MAX, when it
    /// finds any row, IS the true unbounded MAX — no row older than the window can be newer than a row inside
    /// it — so the fallback only ever fires when the window is genuinely empty. Null when the server has no
    /// database-size history at all. <b>Neither probe bounds above by "now"</b> — this is a latest read, and
    /// the collector host's clock is not this reader's clock; a snapshot stamped a few minutes into this
    /// reader's future is still the latest snapshot that exists (#4245 follow-up).</summary>
    private static async Task<DateTime?> GetLatestSnapshotTimeAsync(NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
    {
        var windowStart = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified);

        await using (var probe = postgres.CreateCommand(DatabaseSizeLatestSnapshotWindowedProbeSql))
        {
            probe.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(probe, serverId);
            DarlingMcpReadParameters.AddTimestamp(probe, windowStart);
            var windowed = await probe.ExecuteScalarAsync(cancellationToken);
            if (windowed is DateTime windowedStamp)
            {
                return windowedStamp;
            }
        }

        await using var fallback = postgres.CreateCommand(DatabaseSizeLatestSnapshotFallbackProbeSql);
        fallback.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(fallback, serverId);
        var unbounded = await fallback.ExecuteScalarAsync(cancellationToken);
        return unbounded is DateTime unboundedStamp ? unboundedStamp : null;
    }
}
