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
using NpgsqlTypes;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage.FinOps;

/// <summary>One database's size now against 7 and 30 days ago. The past sizes, growth, daily rate and percent are
/// null when the database has no past snapshot to compare with, never 0.</summary>
public sealed record StorageGrowthDto(
    string DatabaseName, decimal CurrentSizeMb, decimal? Size7dAgoMb, decimal? Size30dAgoMb, decimal? Growth7dMb,
    decimal? Growth30dMb, decimal? DailyGrowthRateMb, decimal? GrowthPct30d, bool HasSiblingRow, bool HasLogServiceFile);

/// <summary>One table in the ranked object-growth summary. The growth figures are null when the table has no earlier
/// sample to compare with.</summary>
public sealed record ObjectSizeGrowthDto(
    string SchemaName, string TableName, decimal CurrentReservedMb, decimal CurrentUsedMb, long TotalRows, int IndexCount,
    decimal? Growth30dMb, decimal? DailyGrowthRateMb, decimal? GrowthPct30d);

/// <summary>One index of one table at its database's latest snapshot. <see cref="LastUserAccess"/> is the monitored
/// server's own wall clock, read verbatim.</summary>
public sealed record IndexUsageDto(
    string DatabaseName, string SchemaName, string TableName, string IndexName, string IndexTypeDesc, int IndexId,
    decimal ReservedMb, long TotalRows, long UserSeeks, long UserScans, long UserLookups, long TotalReads, long UserUpdates,
    DateTime? LastUserAccess, string Classification);

/// <summary>
/// The FinOps Storage Growth reads and the database-size snapshot probes they share with the viewer's database-size
/// reads. The viewer and the Darling service read through the same copy. Every method takes the clock reading the
/// caller computed, so the clock is read where the caller reads it.
/// </summary>
public static class DarlingFinOpsStorageGrowthReader
{
    /// <summary>The newest <c>database_size_stats</c> snapshot at or before <paramref name="asOf"/> for one
    /// server (#4245). A windowed probe first, with <c>collection_time</c> bound between <paramref name="asOf"/>
    /// minus two days and <paramref name="asOf"/>, letting the planner exclude every chunk outside that
    /// span before it builds a subplan for it. It falls back to an unbounded "at or before" probe only when
    /// the window is empty, which a windowed probe cannot itself tell from "never collected": a server whose
    /// collection stopped more than two days before <paramref name="asOf"/>. The two-day width is generous
    /// headroom over the roughly hourly cadence of <c>database_size_stats</c>, enough to absorb a missed cycle
    /// or two, while still excluding nearly all of a retention that runs to 70+ daily chunks in the field.
    ///
    /// <para>Correctness: the windowed probe's MAX, when it finds any row, IS the true unbounded MAX, because no row
    /// outside a "collection_time &gt;= cutoff" window can be newer than a row inside it. So the fallback only
    /// ever fires when the window is genuinely empty, never as an approximation. Returns null when there is no
    /// row at or before <paramref name="asOf"/> at all.</para>
    /// </summary>
    public static async Task<DateTime?> GetDatabaseSizeSnapshotAtOrBeforeAsync(
        NpgsqlDataSource dataSource, int serverId, DateTime asOf, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var asOfBound = DateTime.SpecifyKind(asOf, DateTimeKind.Unspecified);
        var windowStart = DateTime.SpecifyKind(asOf.AddDays(-2), DateTimeKind.Unspecified);

        await using (var probe = dataSource.CreateCommand(DatabaseSizeSnapshotWindowedProbeSql))
        {
            probe.CommandTimeout = commandTimeoutSeconds;
            probe.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            probe.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
            probe.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = asOfBound });
            var windowed = await probe.ExecuteScalarAsync(cancellationToken);
            if (windowed is DateTime windowedStamp)
            {
                return windowedStamp;
            }
        }

        await using var fallback = dataSource.CreateCommand(DatabaseSizeSnapshotFallbackProbeSql);
        fallback.CommandTimeout = commandTimeoutSeconds;
        fallback.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        fallback.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = asOfBound });
        var unbounded = await fallback.ExecuteScalarAsync(cancellationToken);
        return unbounded is DateTime unboundedStamp ? unboundedStamp : null;
    }

    /// <summary>The windowed half of <see cref="GetDatabaseSizeSnapshotAtOrBeforeAsync"/>. $1 server_id,
    /// $2 window start, $3 asOf.</summary>
    public const string DatabaseSizeSnapshotWindowedProbeSql = @"
SELECT MAX(collection_time)
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

    /// <summary>The unbounded fallback half of <see cref="GetDatabaseSizeSnapshotAtOrBeforeAsync"/>, reached
    /// only when the windowed probe finds nothing. $1 server_id, $2 asOf.</summary>
    public const string DatabaseSizeSnapshotFallbackProbeSql = @"
SELECT MAX(collection_time)
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time <= $2";


    /// <summary>The newest <c>database_size_stats</c> snapshot for one server, full stop, with no upper bound
    /// (#4245 follow-up). A windowed probe first (<c>collection_time &gt;= now minus two days</c>), letting
    /// the planner exclude every chunk outside that span before it builds a subplan for it. It falls back to
    /// a fully unbounded probe only when the window is empty (a server whose collection stopped more than two
    /// days ago). <b>Unlike <see cref="GetDatabaseSizeSnapshotAtOrBeforeAsync"/>, neither probe here bounds
    /// above by <c>DateTime.UtcNow</c>.</b> The reader's clock and the collector host's clock are not
    /// the same clock; a snapshot the collector stamped a few minutes ahead of the reader's "now" is still the
    /// latest snapshot that exists, and an upper bound at "now" would silently skip it. The windowed
    /// replacement must not introduce that failure mode. Correctness: the windowed probe's MAX, when it finds any
    /// row, IS the true unbounded MAX, because no row older than the window can be newer than a row inside it, so
    /// the fallback only ever fires when the window is genuinely empty. Null when the server has no
    /// database-size history at all.</summary>
    public static async Task<DateTime?> GetLatestDatabaseSizeSnapshotAsync(
        NpgsqlDataSource dataSource, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var windowStart = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified);

        await using (var probe = dataSource.CreateCommand(DatabaseSizeLatestSnapshotWindowedProbeSql))
        {
            probe.CommandTimeout = commandTimeoutSeconds;
            probe.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            probe.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
            var windowed = await probe.ExecuteScalarAsync(cancellationToken);
            if (windowed is DateTime windowedStamp)
            {
                return windowedStamp;
            }
        }

        await using var fallback = dataSource.CreateCommand(DatabaseSizeLatestSnapshotFallbackProbeSql);
        fallback.CommandTimeout = commandTimeoutSeconds;
        fallback.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        var unbounded = await fallback.ExecuteScalarAsync(cancellationToken);
        return unbounded is DateTime unboundedStamp ? unboundedStamp : null;
    }

    /// <summary>Binds a nullable snapshot time: SQL NULL when there is none, which every caller here uses to
    /// mean "no snapshot in range" (an equality against NULL matches no row, same as the old correlated
    /// subquery returning no row).</summary>
    private static NpgsqlParameter TimestampOrNull(DateTime? value) =>
        new() { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = value.HasValue ? DateTime.SpecifyKind(value.Value, DateTimeKind.Unspecified) : DBNull.Value };

    /// <summary>The windowed half of <see cref="GetLatestDatabaseSizeSnapshotAsync"/>. $1 server_id, $2 window
    /// start. No upper bound — see the method doc for why a latest read must not have one.</summary>
    public const string DatabaseSizeLatestSnapshotWindowedProbeSql = @"
SELECT MAX(collection_time)
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time >= $2";

    /// <summary>The fully unbounded fallback half of <see cref="GetLatestDatabaseSizeSnapshotAsync"/>, reached
    /// only when the windowed probe finds nothing. $1 server_id. No <c>asOf</c> bound at all: this IS the old
    /// pre-#4245 statement's semantics (an unqualified <c>MAX(collection_time)</c>), reached only for the rare
    /// stale-or-empty-server case.</summary>
    public const string DatabaseSizeLatestSnapshotFallbackProbeSql = @"
SELECT MAX(collection_time)
FROM v_database_size_stats
WHERE server_id = $1";


    /// <summary>Per-database storage growth vs 7d/30d ago (Storage Growth parent grid). #4245: all three
    /// snapshot times are resolved before this runs — the <c>latest</c> CTE's from the unbounded
    /// <see cref="GetLatestDatabaseSizeSnapshotAsync"/>, <c>past_7d</c>/<c>past_30d</c>'s from the upper-bounded
    /// <see cref="GetDatabaseSizeSnapshotAtOrBeforeAsync"/> (their cutoff IS a past "at or before" point, so
    /// they keep the upper bound the latest read must not have) — so this statement only ever binds three
    /// literal <c>collection_time</c> values (never a bound-free chunk scan). A cutoff with no
    /// qualifying snapshot (a database younger than 7 or 30 days) binds SQL NULL, which the CTE's equality
    /// turns into zero rows — the LEFT JOIN below already treats that as "no prior snapshot", unchanged from
    /// before this fix. $1 server_id, $2 latest collection_time, $3 collection_time at or before 7d ago,
    /// $4 collection_time at or before 30d ago (either of the last two may be null).
    ///
    /// <para>A file whose row in the latest snapshot has no size is left out of all three sums, by one predicate
    /// (the <c>NOT EXISTS</c> against <c>log_service_files</c>) repeated in each. That file is the log of an Azure
    /// SQL Database Hyperscale database (<see cref="HyperscaleLogSize"/>). Its older rows can still hold the ~1 TB
    /// that sys.database_files reported before the collector stored NULL for it, and summing them on the past side
    /// alone read as a -99% drop. The rule is applied at read time, so it covers history collected before the
    /// change without rewriting it. On the latest side it drops only the rows SUM already skips. A file that is
    /// gone from the latest snapshot has no row there, so it still counts on the past side, as shrinkage.
    /// <c>log_service_files</c> binds the same literal <c>$2</c> as the latest CTE, so the #4245 plan shape
    /// holds. Lite's <c>LocalDataService.StorageGrowthSql</c> is the twin.</para>
    ///
    /// <para>A row stored before the allocated/used fix for another database on an Azure SQL Database server holds that
    /// database's USED space as its total, where every later row holds the ALLOCATED size
    /// (<see cref="AzureSiblingDatabaseSize"/>). The same predicate leaves those rows out of all three sums, so the
    /// one-time change reads as no history and not as growth: the database shows a blank past size and growth n/a (null) until a
    /// newer sample is old enough to compare against, as a database added inside the window does. Until the first
    /// collection after the upgrade the latest snapshot holds only old-shape rows, so the database is not listed here at
    /// all.</para>
    ///
    /// <para>The <c>latest</c> CTE also flags each database whose size leaves its log out: <c>has_sibling_row</c> is true
    /// when the database has the one row another database on an Azure SQL Database server gets
    /// (<see cref="AzureSiblingDatabaseSize.RowPredicate"/>, so its size is data space only), and
    /// <c>has_log_service_file</c> is true when it has a row in <c>log_service_files</c> (the Hyperscale log, which the
    /// sums skip). The second reads the same <c>$2</c> snapshot as the CTE it joins, so the #4245 plan shape holds. The
    /// row's <c>Note</c> says which. Lite's <c>LocalDataService.StorageGrowthSql</c> is the twin.</para></summary>
    public const string StorageGrowthSql = @"
WITH log_service_files AS (
    SELECT
        database_name,
        file_id
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = $2
    AND   total_size_mb IS NULL
),
latest AS (
    SELECT
        s.database_name,
        SUM(s.total_size_mb) AS current_size_mb,
        bool_or(" + AzureSiblingDatabaseSize.RowPredicate + @") AS has_sibling_row,
        EXISTS (
            SELECT 1
            FROM log_service_files AS ls
            WHERE ls.database_name = s.database_name
        ) AS has_log_service_file
    FROM v_database_size_stats AS s
    WHERE s.server_id = $1
    AND   s.collection_time = $2
    AND   NOT EXISTS (
        SELECT 1
        FROM log_service_files AS ls
        WHERE ls.database_name = s.database_name
        AND   ls.file_id = s.file_id
    )
    AND   " + AzureSiblingDatabaseSize.ExcludePreFixRows + @"
    GROUP BY s.database_name
),
past_7d AS (
    SELECT
        s.database_name,
        SUM(s.total_size_mb) AS size_mb
    FROM v_database_size_stats AS s
    WHERE s.server_id = $1
    AND   s.collection_time = $3
    AND   NOT EXISTS (
        SELECT 1
        FROM log_service_files AS ls
        WHERE ls.database_name = s.database_name
        AND   ls.file_id = s.file_id
    )
    AND   " + AzureSiblingDatabaseSize.ExcludePreFixRows + @"
    GROUP BY s.database_name
),
past_30d AS (
    SELECT
        s.database_name,
        SUM(s.total_size_mb) AS size_mb
    FROM v_database_size_stats AS s
    WHERE s.server_id = $1
    AND   s.collection_time = $4
    AND   NOT EXISTS (
        SELECT 1
        FROM log_service_files AS ls
        WHERE ls.database_name = s.database_name
        AND   ls.file_id = s.file_id
    )
    AND   " + AzureSiblingDatabaseSize.ExcludePreFixRows + @"
    GROUP BY s.database_name
)
SELECT
    l.database_name,
    l.current_size_mb,
    p7.size_mb,
    p30.size_mb,
    l.current_size_mb - p7.size_mb AS growth_7d_mb,
    l.current_size_mb - p30.size_mb AS growth_30d_mb,
    CASE
        WHEN p30.size_mb IS NOT NULL
        THEN (l.current_size_mb - p30.size_mb) / NULLIF(EXTRACT(EPOCH FROM ($2::timestamp - $4::timestamp)) / 86400.0, 0)
        WHEN p7.size_mb IS NOT NULL
        THEN (l.current_size_mb - p7.size_mb) / NULLIF(EXTRACT(EPOCH FROM ($2::timestamp - $3::timestamp)) / 86400.0, 0)
        ELSE NULL
    END AS daily_growth_rate_mb,
    CASE
        WHEN p30.size_mb IS NOT NULL AND p30.size_mb > 0
        THEN (l.current_size_mb - p30.size_mb) * 100.0 / p30.size_mb
        ELSE NULL
    END AS growth_pct_30d,
    l.has_sibling_row,
    l.has_log_service_file
FROM latest l
LEFT JOIN past_7d p7 ON p7.database_name = l.database_name
LEFT JOIN past_30d p30 ON p30.database_name = l.database_name
ORDER BY growth_30d_mb DESC NULLS LAST, growth_7d_mb DESC NULLS LAST, l.database_name";

    public static async Task<List<StorageGrowthDto>> GetStorageGrowthAsync(
        NpgsqlDataSource dataSource, int serverId, DateTime nowUtc, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var items = new List<StorageGrowthDto>();
        var now = nowUtc;

        var latestSnapshot = await GetLatestDatabaseSizeSnapshotAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken);
        if (latestSnapshot is null)
        {
            return items;
        }

        var past7Snapshot = await GetDatabaseSizeSnapshotAtOrBeforeAsync(dataSource, serverId, now.AddDays(-7), commandTimeoutSeconds, cancellationToken);
        var past30Snapshot = await GetDatabaseSizeSnapshotAtOrBeforeAsync(dataSource, serverId, now.AddDays(-30), commandTimeoutSeconds, cancellationToken);

        /* A past snapshot must be strictly older than the latest one. When collection stopped more than a
           window ago, "at or before now - window" IS the latest snapshot, and comparing it with itself would read
           as growth 0 over zero days; that is no comparison, so it is null (n/a). */
        if (past7Snapshot is DateTime p7 && p7 >= latestSnapshot.Value)
        {
            past7Snapshot = null;
        }
        if (past30Snapshot is DateTime p30 && p30 >= latestSnapshot.Value)
        {
            past30Snapshot = null;
        }

        await using var command = dataSource.CreateCommand(StorageGrowthSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = latestSnapshot.Value });
        command.Parameters.Add(TimestampOrNull(past7Snapshot));
        command.Parameters.Add(TimestampOrNull(past30Snapshot));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new StorageGrowthDto(
                DatabaseName: reader.IsDBNull(0) ? "" : reader.GetString(0),
                CurrentSizeMb: reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                Size7dAgoMb: reader.IsDBNull(2) ? null : Convert.ToDecimal(reader.GetValue(2)),
                Size30dAgoMb: reader.IsDBNull(3) ? null : Convert.ToDecimal(reader.GetValue(3)),
                Growth7dMb: reader.IsDBNull(4) ? null : Convert.ToDecimal(reader.GetValue(4)),
                Growth30dMb: reader.IsDBNull(5) ? null : Convert.ToDecimal(reader.GetValue(5)),
                DailyGrowthRateMb: reader.IsDBNull(6) ? null : Convert.ToDecimal(reader.GetValue(6)),
                GrowthPct30d: reader.IsDBNull(7) ? null : Convert.ToDecimal(reader.GetValue(7)),
                HasSiblingRow: !reader.IsDBNull(8) && reader.GetBoolean(8),
                HasLogServiceFile: !reader.IsDBNull(9) && reader.GetBoolean(9)));
        }
        return items;
    }

    /// <summary>
    /// The latest/earliest collection_time in the window, for a server+database — computed ONCE (#4227) and
    /// passed as exact instants to both <see cref="ObjectGrowthSummarySql"/> and <see cref="ObjectGrowthSeriesSql"/>,
    /// which used to each recompute this same MAX/MIN(collection_time) as their own <c>bounds</c> CTE (89ms and
    /// 21.3k buffers apiece on a seeded store, with no index leading (server_id, collection_time)). $1 server_id,
    /// $2 database, $3 window start. NULL/NULL when nothing in v_index_object_stats matches (collection hasn't
    /// started, or stopped before the window) — the caller short-circuits to empty rather than passing NULL
    /// instants down.
    /// </summary>
    public const string ObjectGrowthBoundsSql = @"
SELECT MAX(collection_time) AS latest_time, MIN(collection_time) AS earliest_time
FROM v_index_object_stats
WHERE server_id = $1 AND database_name = $2 AND collection_time >= $3";

    /// <summary>Ranked top-N object growth summary for the object drill (#1138 §3A). $1 server_id, $2 database, $3 latest instant, $4 earliest instant, $5 topN — the two instants come from <see cref="ObjectGrowthBoundsSql"/>.</summary>
    public const string ObjectGrowthSummarySql = @"
WITH latest AS (
    SELECT schema_name, table_name,
        SUM(reserved_mb) AS cur_reserved_mb,
        SUM(used_mb) AS cur_used_mb,
        MAX(total_rows) AS cur_rows,
        COUNT(*) AS index_count
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time = $3
    GROUP BY schema_name, table_name
),
earliest AS (
    SELECT schema_name, table_name, SUM(reserved_mb) AS e_reserved_mb
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time = $4
    GROUP BY schema_name, table_name
)
SELECT
    l.schema_name,
    l.table_name,
    l.cur_reserved_mb,
    l.cur_used_mb,
    l.cur_rows,
    l.index_count,
    l.cur_reserved_mb - e.e_reserved_mb AS growth_mb
FROM latest l
LEFT JOIN earliest e ON e.schema_name = l.schema_name AND e.table_name = l.table_name
ORDER BY growth_mb DESC NULLS LAST, l.schema_name, l.table_name
LIMIT $5";

    /// <summary>Daily reserved-MB series for the ranked top-N objects (heatmap). $1 server_id, $2 database, $3 window start, $4 latest instant, $5 earliest instant, $6 topN — the two instants come from <see cref="ObjectGrowthBoundsSql"/>.</summary>
    public const string ObjectGrowthSeriesSql = @"
WITH latest AS (
    SELECT schema_name, table_name, SUM(reserved_mb) AS cur_reserved_mb
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time = $4
    GROUP BY schema_name, table_name
),
earliest AS (
    SELECT schema_name, table_name, SUM(reserved_mb) AS e_reserved_mb
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time = $5
    GROUP BY schema_name, table_name
),
ranked AS (
    SELECT l.schema_name, l.table_name,
        l.cur_reserved_mb - COALESCE(e.e_reserved_mb, l.cur_reserved_mb) AS growth_mb
    FROM latest l
    LEFT JOIN earliest e ON e.schema_name = l.schema_name AND e.table_name = l.table_name
    ORDER BY growth_mb DESC, l.schema_name, l.table_name
    LIMIT $6
)
SELECT
    ios.schema_name,
    ios.table_name,
    date_trunc('day', ios.collection_time) AS the_day,
    SUM(ios.reserved_mb) AS reserved_mb
FROM v_index_object_stats ios
JOIN ranked r ON r.schema_name = ios.schema_name AND r.table_name = ios.table_name
WHERE ios.server_id = $1 AND ios.database_name = $2 AND ios.collection_time >= $3
GROUP BY ios.schema_name, ios.table_name, date_trunc('day', ios.collection_time)
ORDER BY ios.schema_name, ios.table_name, the_day";

    /// <summary>
    /// Object-growth heatmap data for a single database (#1138 §3A): the ranked top-N summary rows and the
    /// long-form daily reserved-MB samples (pivoted to a matrix by FinOpsHeatmapBuilder). Two pooled
    /// commands (mirrors Lite's two-command split for DuckDB's one-result-set-per-command limit).
    /// <paramref name="windowStart"/> is the naive-UTC window start the caller computed from <paramref name="daysBack"/>.
    /// </summary>
    public static async Task<(List<ObjectSizeGrowthDto> Objects, List<FinOpsObjectDaySample> Samples)> GetObjectGrowthHeatmapDataAsync(
        NpgsqlDataSource dataSource, int serverId, string databaseName, DateTime windowStart, int daysBack, int topN,
        int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var objects = new List<ObjectSizeGrowthDto>();
        var samples = new List<FinOpsObjectDaySample>();

        /* #4227: bounds computed once, then handed to both statements below as exact instants instead of
           each re-deriving them via its own `bounds` CTE. */
        DateTime? latestTime;
        DateTime? earliestTime;

        await using (var boundsCommand = dataSource.CreateCommand(ObjectGrowthBoundsSql))
        {
            boundsCommand.CommandTimeout = commandTimeoutSeconds;
            boundsCommand.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            boundsCommand.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
            boundsCommand.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });

            await using var reader = await boundsCommand.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken); /* MAX/MIN with no GROUP BY always returns one row. */
            latestTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0);
            earliestTime = reader.IsDBNull(1) ? null : reader.GetDateTime(1);
        }

        /* No v_index_object_stats row in the window (collection hasn't started, or stopped before the window
           started) — both old statements' `collection_time = (SELECT ... FROM bounds)` matched nothing in
           that case, so this is the same empty result without issuing either statement. */
        if (latestTime is not DateTime latest || earliestTime is not DateTime earliest)
        {
            return (objects, samples);
        }

        await using (var command = dataSource.CreateCommand(ObjectGrowthSummarySql))
        {
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = latest });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = earliest });
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = topN });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var current = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2));
                /* No earlier sample for this table (new table, or one snapshot only): null, not 0. */
                decimal? growth = reader.IsDBNull(6) || latest == earliest ? null : Convert.ToDecimal(reader.GetValue(6));
                objects.Add(new ObjectSizeGrowthDto(
                    SchemaName: reader.IsDBNull(0) ? "" : reader.GetString(0),
                    TableName: reader.IsDBNull(1) ? "" : reader.GetString(1),
                    CurrentReservedMb: current,
                    CurrentUsedMb: reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                    TotalRows: reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                    IndexCount: reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
                    Growth30dMb: growth,
                    DailyGrowthRateMb: growth is decimal g && daysBack > 0 ? g / daysBack : null,
                    GrowthPct30d: growth is decimal g2 && current - g2 > 0 ? g2 * 100m / (current - g2) : null));
            }
        }

        await using (var command = dataSource.CreateCommand(ObjectGrowthSeriesSql))
        {
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = latest });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = earliest });
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = topN });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var schema = reader.IsDBNull(0) ? "" : reader.GetString(0);
                var table = reader.IsDBNull(1) ? "" : reader.GetString(1);
                var day = reader.IsDBNull(2) ? DateTime.MinValue : reader.GetDateTime(2);
                var reservedMb = reader.IsDBNull(3) ? 0d : Convert.ToDouble(reader.GetValue(3));
                samples.Add(new FinOpsObjectDaySample($"{schema}.{table}", day, reservedMb));
            }
        }

        return (objects, samples);
    }

    /// <summary>Per-index size + usage for one object at its database's latest snapshot (Storage Growth index drill).</summary>
    public const string ObjectIndexDetailSql = @"
SELECT
    database_name,
    schema_name,
    table_name,
    index_name,
    index_type_desc,
    index_id,
    reserved_mb,
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
AND   database_name = $2
AND   schema_name = $3
AND   table_name = $4
AND   collection_time = (
    SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1 AND database_name = $2)
ORDER BY index_id";

    public static async Task<List<IndexUsageDto>> GetObjectIndexDetailAsync(
        NpgsqlDataSource dataSource, int serverId, string databaseName, string schemaName, string tableName,
        int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(ObjectIndexDetailSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = schemaName });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = tableName });

        var items = new List<IndexUsageDto>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new IndexUsageDto(
                DatabaseName: reader.IsDBNull(0) ? "" : reader.GetString(0),
                SchemaName: reader.IsDBNull(1) ? "" : reader.GetString(1),
                TableName: reader.IsDBNull(2) ? "" : reader.GetString(2),
                IndexName: reader.IsDBNull(3) ? "(heap)" : reader.GetString(3),
                IndexTypeDesc: reader.IsDBNull(4) ? "" : reader.GetString(4),
                IndexId: reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
                ReservedMb: reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6)),
                TotalRows: reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7)),
                UserSeeks: reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                UserScans: reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
                UserLookups: reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10)),
                TotalReads: reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11)),
                UserUpdates: reader.IsDBNull(12) ? 0L : Convert.ToInt64(reader.GetValue(12)),
                /* last_user_seek/scan/lookup/update (sys.dm_db_index_usage_stats) are SERVER-LOCAL in the
                   store, not naive UTC, so this GREATEST is read verbatim like Lite — NOT through
                   ViewerTimeHelper.ForDisplay (which assumes naive UTC and, in Local/Server mode, would shift it). */
                LastUserAccess: reader.IsDBNull(13) ? null : reader.GetDateTime(13),
                Classification: reader.IsDBNull(14) ? "" : reader.GetString(14)));
        }
        return items;
    }
}
