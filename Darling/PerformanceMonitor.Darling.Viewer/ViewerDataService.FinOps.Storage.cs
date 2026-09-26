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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// FinOps Database Sizes / Storage Growth (parent + object-growth heatmap drill + index detail) /
/// Optimization (idle databases + tempdb) reads — Lite's <c>LocalDataService.FinOps.StorageGrowth.cs</c>,
/// <c>.Inventory.cs</c>, and <c>.IndexObjects.cs</c> storage halves ported to Postgres. The DuckDB SQL
/// ports near-verbatim: the correlated <c>collection_time = (SELECT MAX(...))</c> latest-snapshot pattern,
/// <c>EXCEPT</c>, <c>GREATEST</c>, <c>date_trunc('day', ...)</c>, and date subtraction all run identically
/// on PG. The object-growth heatmap's two DuckDB commands (DuckDB returns one result set per command)
/// become two pooled Npgsql commands. SQL kept in <c>public const</c> so tests pin it.
///
/// <para><b>#4245: the one place the DuckDB port stops being verbatim.</b> DuckDB has no chunks, so a
/// correlated <c>collection_time = (SELECT MAX(collection_time) ...)</c> costs DuckDB nothing extra to plan.
/// On a <c>database_size_stats</c> hypertable it costs the PostgreSQL planner one subplan per retained
/// chunk, unconditionally, because nothing in the query lets it exclude any chunk before walking it — the
/// field measurement was 148 ms of planning against 2.6 ms of execution, over 71 chunks. The single-server
/// "latest snapshot" reads below (<see cref="GetDatabaseSizeLatestAsync"/>, <see cref="GetDatabaseSizeSummaryAsync"/>,
/// and <see cref="GetStorageGrowthAsync"/>'s current-size CTE) go through <see cref="GetLatestDatabaseSizeSnapshotAsync"/>
/// instead: a windowed probe the planner CAN bound, read as its own round trip so the snapshot's
/// <c>collection_time</c> comes back as a literal value the main read can bind an equality to, rather than a
/// subquery the planner must re-derive per chunk. <see cref="GetStorageGrowthAsync"/>'s 7-day-ago and
/// 30-day-ago comparison points go through <see cref="GetDatabaseSizeSnapshotAtOrBeforeAsync"/> instead — same
/// two-step shape, but genuinely bounded above, since "at or before a past cutoff" is what they mean. A
/// *latest* read must not carry that upper bound: nothing should hide a snapshot merely because the collector
/// host's clock ran ahead of the reader's own <c>DateTime.UtcNow</c> (#4245 follow-up).</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The newest <c>database_size_stats</c> snapshot at or before <paramref name="asOf"/> for one
    /// server (#4245). A windowed probe first — <c>collection_time</c> bound between <paramref name="asOf"/>
    /// minus two days and <paramref name="asOf"/>, letting the planner exclude every chunk outside that
    /// span before it builds a subplan for it — falling back to an unbounded "at or before" probe only when
    /// the window is empty, which is exactly the case a windowed probe cannot itself distinguish from "never
    /// collected": a server whose collection stopped more than two days before <paramref name="asOf"/>. The
    /// two-day width is generous headroom over <c>database_size_stats</c>' roughly hourly cadence
    /// (<c>CollectorScheduleDefaults["database_size_stats"]</c>) — enough to absorb a missed cycle or two —
    /// while still excluding nearly all of a retention that runs to 70+ daily chunks in the field.
    ///
    /// <para>Correctness: the windowed probe's MAX, when it finds any row, IS the true unbounded MAX — no row
    /// outside a "collection_time &gt;= cutoff" window can be newer than a row inside it. So the fallback only
    /// ever fires when the window is genuinely empty, never as an approximation. Returns null when there is no
    /// row at or before <paramref name="asOf"/> at all.</para>
    /// </summary>
    private async Task<DateTime?> GetDatabaseSizeSnapshotAtOrBeforeAsync(int serverId, DateTime asOf, CancellationToken cancellationToken)
    {
        var asOfBound = DateTime.SpecifyKind(asOf, DateTimeKind.Unspecified);
        var windowStart = DateTime.SpecifyKind(asOf.AddDays(-2), DateTimeKind.Unspecified);

        await using (var probe = _dataSource.CreateCommand(DatabaseSizeSnapshotWindowedProbeSql))
        {
            probe.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            probe.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            probe.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
            probe.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = asOfBound });
            var windowed = await probe.ExecuteScalarAsync(cancellationToken);
            if (windowed is DateTime windowedStamp)
            {
                return windowedStamp;
            }
        }

        await using var fallback = _dataSource.CreateCommand(DatabaseSizeSnapshotFallbackProbeSql);
        fallback.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        fallback.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        fallback.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = asOfBound });
        var unbounded = await fallback.ExecuteScalarAsync(cancellationToken);
        return unbounded is DateTime unboundedStamp ? unboundedStamp : null;
    }

    /// <summary>Binds a nullable snapshot time — SQL NULL when there is none, which every caller here uses to
    /// mean "no snapshot in range" (an equality against NULL matches no row, same as the old correlated
    /// subquery returning no row).</summary>
    private static NpgsqlParameter TimestampOrNull(DateTime? value) =>
        new() { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = value.HasValue ? DateTime.SpecifyKind(value.Value, DateTimeKind.Unspecified) : DBNull.Value };

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

    /// <summary>The newest <c>database_size_stats</c> snapshot for one server, full stop — no upper bound
    /// (#4245 follow-up). A windowed probe first — <c>collection_time &gt;= asOf minus two days</c>, letting
    /// the planner exclude every chunk outside that span before it builds a subplan for it — falling back to
    /// a fully unbounded probe only when the window is empty (a server whose collection stopped more than two
    /// days ago). <b>Unlike <see cref="GetDatabaseSizeSnapshotAtOrBeforeAsync"/>, neither probe here bounds
    /// above by <c>DateTime.UtcNow</c>.</b> The viewer machine's clock and the collector host's clock are not
    /// the same clock; a snapshot the collector stamped a few minutes ahead of the viewer's "now" is still the
    /// latest snapshot that exists, and an upper bound at "now" would silently skip it — the original bug this
    /// PR fixes (an unbounded MAX with no bound at all) never had that failure mode, so the windowed
    /// replacement must not introduce one. Correctness: the windowed probe's MAX, when it finds any row, IS
    /// the true unbounded MAX — no row older than the window can be newer than a row inside it — so the
    /// fallback only ever fires when the window is genuinely empty. Null when the server has no
    /// database-size history at all.</summary>
    private async Task<DateTime?> GetLatestDatabaseSizeSnapshotAsync(int serverId, CancellationToken cancellationToken)
    {
        var windowStart = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified);

        await using (var probe = _dataSource.CreateCommand(DatabaseSizeLatestSnapshotWindowedProbeSql))
        {
            probe.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            probe.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            probe.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
            var windowed = await probe.ExecuteScalarAsync(cancellationToken);
            if (windowed is DateTime windowedStamp)
            {
                return windowedStamp;
            }
        }

        await using var fallback = _dataSource.CreateCommand(DatabaseSizeLatestSnapshotFallbackProbeSql);
        fallback.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        fallback.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        var unbounded = await fallback.ExecuteScalarAsync(cancellationToken);
        return unbounded is DateTime unboundedStamp ? unboundedStamp : null;
    }

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

    /// <summary>Latest database size snapshot per file for one server, biggest files first (a capacity view —
    /// you hunt the largest files, not browse alphabetically; files can interleave across databases by design).
    /// The <c>total_size_mb DESC</c> lead is display-only and safe to change: the grid is the sole order-sensitive
    /// consumer — the other two callers (<c>GetUtilizationEfficiencyAsync</c>'s free-space math and the dormant-DB
    /// recommendation's cost share) only <c>Sum</c> the rows, and the "Allocated vs Used" chart is a separate read
    /// (<c>GetDatabaseSizeSummaryAsync</c>). $1 server_id, $2 collection_time (resolved by
    /// <see cref="GetLatestDatabaseSizeSnapshotAsync"/> — see #4245 on the type doc).</summary>
    public const string DatabaseSizeLatestSql = @"
SELECT
    database_name,
    file_type_desc,
    file_name,
    total_size_mb,
    used_size_mb,
    volume_mount_point,
    volume_total_mb,
    volume_free_mb,
    recovery_model_desc,
    auto_growth_mb,
    is_percent_growth,
    growth_pct,
    vlf_count
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time = $2
ORDER BY total_size_mb DESC, database_name, file_type_desc, file_name";

    public async Task<List<DatabaseSizeRow>> GetDatabaseSizeLatestAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var items = new List<DatabaseSizeRow>();
        var snapshotTime = await GetLatestDatabaseSizeSnapshotAsync(serverId, cancellationToken);
        if (snapshotTime is null)
        {
            return items;
        }

        await using var command = _dataSource.CreateCommand(DatabaseSizeLatestSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = snapshotTime.Value });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new DatabaseSizeRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                FileTypeDesc = reader.IsDBNull(1) ? "" : reader.GetString(1),
                FileName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                TotalSizeMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                UsedSizeMb = reader.IsDBNull(4) ? null : Convert.ToDecimal(reader.GetValue(4)),
                VolumeMountPoint = reader.IsDBNull(5) ? null : reader.GetString(5),
                VolumeTotalMb = reader.IsDBNull(6) ? null : Convert.ToDecimal(reader.GetValue(6)),
                VolumeFreeMb = reader.IsDBNull(7) ? null : Convert.ToDecimal(reader.GetValue(7)),
                RecoveryModel = reader.IsDBNull(8) ? null : reader.GetString(8),
                AutoGrowthMb = reader.IsDBNull(9) ? null : Convert.ToDecimal(reader.GetValue(9)),
                IsPercentGrowth = reader.IsDBNull(10) ? null : reader.GetBoolean(10),
                GrowthPct = reader.IsDBNull(11) ? null : Convert.ToInt32(reader.GetValue(11)),
                VlfCount = reader.IsDBNull(12) ? null : Convert.ToInt32(reader.GetValue(12))
            });
        }
        return items;
    }

    /// <summary>Per-database allocated + used space for the Utilization size chart. $1 server_id,
    /// $2 collection_time (resolved by <see cref="GetLatestDatabaseSizeSnapshotAsync"/> — see #4245 on
    /// the type doc), $3 topN.</summary>
    public const string DatabaseSizeSummarySql = @"
SELECT
    database_name,
    SUM(total_size_mb) AS total_mb,
    SUM(used_size_mb) AS used_mb
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time = $2
GROUP BY database_name
ORDER BY total_mb DESC
LIMIT $3";

    public async Task<List<DatabaseSizeSummaryRow>> GetDatabaseSizeSummaryAsync(int serverId, int topN = 10, CancellationToken cancellationToken = default)
    {
        var items = new List<DatabaseSizeSummaryRow>();
        var snapshotTime = await GetLatestDatabaseSizeSnapshotAsync(serverId, cancellationToken);
        if (snapshotTime is null)
        {
            return items;
        }

        await using var command = _dataSource.CreateCommand(DatabaseSizeSummarySql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = snapshotTime.Value });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = topN });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new DatabaseSizeSummaryRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                TotalMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                UsedMb = reader.IsDBNull(2) ? null : Convert.ToDecimal(reader.GetValue(2))
            });
        }
        return items;
    }

    /// <summary>Databases with zero query executions over the last N days. $1 server_id, $2 cutoff.</summary>
    public const string IdleDatabasesSql = $@"
WITH db_sizes AS (
    SELECT
        database_name,
        SUM(total_size_mb) AS total_size_mb,
        COUNT(*) AS file_count
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = (
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
    )
    GROUP BY database_name
),
db_activity AS (
    SELECT
        database_name,
        SUM(delta_execution_count) FILTER (WHERE {TimescaleSupport.IntervalHonestSourceFilter}) AS total_executions,
        MAX(last_execution_time) AS last_execution
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_execution_count IS NOT NULL
    GROUP BY database_name
)
SELECT
    ds.database_name,
    ds.total_size_mb,
    ds.file_count,
    a.last_execution
FROM db_sizes ds
LEFT JOIN db_activity a ON a.database_name = ds.database_name
WHERE COALESCE(a.total_executions, 0) = 0
AND   (a.last_execution IS NULL OR a.last_execution < $2)
AND   ds.database_name NOT IN ('master', 'model', 'msdb', 'tempdb', 'PerformanceMonitor')
ORDER BY ds.total_size_mb DESC";

    public async Task<List<IdleDatabaseRow>> GetIdleDatabasesAsync(int serverId, int daysBack = 7, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddDays(-daysBack);

        await using var command = _dataSource.CreateCommand(IdleDatabasesSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });

        var items = new List<IdleDatabaseRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new IdleDatabaseRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                TotalSizeMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                FileCount = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                /* last_execution_time (sys.dm_exec_query_stats) is SERVER-LOCAL in the store, not naive
                   UTC, so it is read verbatim like Lite — NOT through ViewerTimeHelper.ForDisplay (which
                   assumes naive UTC and, in Local/Server mode, would shift it). Contrast the
                   collection_time-derived timestamps in this port, which ARE naive UTC and correctly use ForDisplay. */
                LastExecutionTime = reader.IsDBNull(3) ? null : reader.GetDateTime(3)
            });
        }
        return items;
    }

    /// <summary>tempdb pressure summary: latest and 24h peak values. $1 server_id, $2 cutoff.</summary>
    public const string TempdbSummarySql = @"
WITH latest AS (
    SELECT
        user_object_reserved_mb,
        internal_object_reserved_mb,
        version_store_reserved_mb,
        total_reserved_mb
    FROM v_tempdb_stats
    WHERE server_id = $1
    ORDER BY collection_time DESC
    LIMIT 1
),
peak AS (
    SELECT
        MAX(user_object_reserved_mb) AS max_user_mb,
        MAX(internal_object_reserved_mb) AS max_internal_mb,
        MAX(version_store_reserved_mb) AS max_version_store_mb,
        MAX(total_reserved_mb) AS max_total_mb
    FROM v_tempdb_stats
    WHERE server_id = $1
    AND   collection_time >= $2
)
SELECT 'User Objects', l.user_object_reserved_mb, p.max_user_mb,
    CASE WHEN p.max_user_mb > 1024 THEN 'High user object usage' ELSE '' END
FROM latest l CROSS JOIN peak p
UNION ALL
SELECT 'Internal Objects', l.internal_object_reserved_mb, p.max_internal_mb,
    CASE WHEN p.max_internal_mb > 1024 THEN 'High internal object usage (sorts/hashes)' ELSE '' END
FROM latest l CROSS JOIN peak p
UNION ALL
SELECT 'Version Store', l.version_store_reserved_mb, p.max_version_store_mb,
    CASE WHEN p.max_version_store_mb > 2048 THEN 'Version store pressure — check long-running transactions' ELSE '' END
FROM latest l CROSS JOIN peak p
UNION ALL
SELECT 'Total Reserved', l.total_reserved_mb, p.max_total_mb, ''
FROM latest l CROSS JOIN peak p";

    public async Task<List<TempdbSummaryRow>> GetTempdbSummaryAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-24);

        await using var command = _dataSource.CreateCommand(TempdbSummarySql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });

        var items = new List<TempdbSummaryRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new TempdbSummaryRow
            {
                Metric = reader.IsDBNull(0) ? "" : reader.GetString(0),
                CurrentMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                Peak24hMb = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                Warning = reader.IsDBNull(3) ? "" : reader.GetString(3)
            });
        }
        return items;
    }

    /// <summary>Per-database storage growth vs 7d/30d ago (Storage Growth parent grid). #4245: all three
    /// snapshot times are resolved before this runs — the <c>latest</c> CTE's from the unbounded
    /// <see cref="GetLatestDatabaseSizeSnapshotAsync"/>, <c>past_7d</c>/<c>past_30d</c>'s from the upper-bounded
    /// <see cref="GetDatabaseSizeSnapshotAtOrBeforeAsync"/> (their cutoff IS a past "at or before" point, so
    /// they keep the upper bound the latest read must not have) — so this statement only ever binds three
    /// literal <c>collection_time</c> values (never a bound-free chunk scan). A cutoff with no
    /// qualifying snapshot (a database younger than 7 or 30 days) binds SQL NULL, which the CTE's equality
    /// turns into zero rows — the LEFT JOIN below already treats that as "no prior snapshot", unchanged from
    /// before this fix. $1 server_id, $2 latest collection_time, $3 collection_time at or before 7d ago,
    /// $4 collection_time at or before 30d ago (either of the last two may be null).</summary>
    public const string StorageGrowthSql = @"
WITH latest AS (
    SELECT
        database_name,
        SUM(total_size_mb) AS current_size_mb
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = $2
    GROUP BY database_name
),
past_7d AS (
    SELECT
        database_name,
        SUM(total_size_mb) AS size_mb
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = $3
    GROUP BY database_name
),
past_30d AS (
    SELECT
        database_name,
        SUM(total_size_mb) AS size_mb
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = $4
    GROUP BY database_name
)
SELECT
    l.database_name,
    l.current_size_mb,
    p7.size_mb,
    p30.size_mb,
    l.current_size_mb - COALESCE(p7.size_mb, l.current_size_mb) AS growth_7d_mb,
    l.current_size_mb - COALESCE(p30.size_mb, l.current_size_mb) AS growth_30d_mb,
    CASE
        WHEN p30.size_mb IS NOT NULL
        THEN (l.current_size_mb - p30.size_mb) / 30.0
        WHEN p7.size_mb IS NOT NULL
        THEN (l.current_size_mb - p7.size_mb) / 7.0
        ELSE 0
    END AS daily_growth_rate_mb,
    CASE
        WHEN p30.size_mb IS NOT NULL AND p30.size_mb > 0
        THEN (l.current_size_mb - p30.size_mb) * 100.0 / p30.size_mb
        ELSE 0
    END AS growth_pct_30d
FROM latest l
LEFT JOIN past_7d p7 ON p7.database_name = l.database_name
LEFT JOIN past_30d p30 ON p30.database_name = l.database_name
ORDER BY growth_30d_mb DESC";

    public async Task<List<StorageGrowthRow>> GetStorageGrowthAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var items = new List<StorageGrowthRow>();
        var now = DateTime.UtcNow;

        var latestSnapshot = await GetLatestDatabaseSizeSnapshotAsync(serverId, cancellationToken);
        if (latestSnapshot is null)
        {
            return items;
        }

        var past7Snapshot = await GetDatabaseSizeSnapshotAtOrBeforeAsync(serverId, now.AddDays(-7), cancellationToken);
        var past30Snapshot = await GetDatabaseSizeSnapshotAtOrBeforeAsync(serverId, now.AddDays(-30), cancellationToken);

        await using var command = _dataSource.CreateCommand(StorageGrowthSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = latestSnapshot.Value });
        command.Parameters.Add(TimestampOrNull(past7Snapshot));
        command.Parameters.Add(TimestampOrNull(past30Snapshot));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new StorageGrowthRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                CurrentSizeMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                Size7dAgoMb = reader.IsDBNull(2) ? null : Convert.ToDecimal(reader.GetValue(2)),
                Size30dAgoMb = reader.IsDBNull(3) ? null : Convert.ToDecimal(reader.GetValue(3)),
                Growth7dMb = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                Growth30dMb = reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5)),
                DailyGrowthRateMb = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6)),
                GrowthPct30d = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7))
            });
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
    l.cur_reserved_mb - COALESCE(e.e_reserved_mb, l.cur_reserved_mb) AS growth_mb
FROM latest l
LEFT JOIN earliest e ON e.schema_name = l.schema_name AND e.table_name = l.table_name
ORDER BY growth_mb DESC, l.schema_name, l.table_name
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
    /// </summary>
    public async Task<(List<ObjectSizeGrowthRow> Objects, List<FinOpsObjectDaySample> Samples)> GetObjectGrowthHeatmapDataAsync(
        int serverId, string databaseName, int daysBack = 30, int topN = 20, CancellationToken cancellationToken = default)
    {
        var windowStart = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-daysBack), DateTimeKind.Unspecified);

        var objects = new List<ObjectSizeGrowthRow>();
        var samples = new List<FinOpsObjectDaySample>();

        /* #4227: bounds computed once, then handed to both statements below as exact instants instead of
           each re-deriving them via its own `bounds` CTE. */
        DateTime? latestTime;
        DateTime? earliestTime;

        await using (var boundsCommand = _dataSource.CreateCommand(ObjectGrowthBoundsSql))
        {
            boundsCommand.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
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

        await using (var command = _dataSource.CreateCommand(ObjectGrowthSummarySql))
        {
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = latest });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = earliest });
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = topN });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var current = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2));
                var growth = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6));
                var earlier = current - growth;
                objects.Add(new ObjectSizeGrowthRow
                {
                    DatabaseName = databaseName,
                    SchemaName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                    TableName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    CurrentReservedMb = current,
                    CurrentUsedMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                    TotalRows = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                    IndexCount = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
                    Growth30dMb = growth,
                    DailyGrowthRateMb = daysBack > 0 ? growth / daysBack : 0m,
                    GrowthPct30d = earlier > 0 ? growth * 100m / earlier : 0m
                });
            }
        }

        await using (var command = _dataSource.CreateCommand(ObjectGrowthSeriesSql))
        {
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
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

    public async Task<List<IndexUsageRow>> GetObjectIndexDetailAsync(int serverId, string databaseName, string schemaName, string tableName, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ObjectIndexDetailSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = schemaName });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = tableName });

        var items = new List<IndexUsageRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new IndexUsageRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                SchemaName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                TableName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                IndexName = reader.IsDBNull(3) ? "(heap)" : reader.GetString(3),
                IndexTypeDesc = reader.IsDBNull(4) ? "" : reader.GetString(4),
                IndexId = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
                ReservedMb = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6)),
                TotalRows = reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7)),
                UserSeeks = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                UserScans = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
                UserLookups = reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10)),
                TotalReads = reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11)),
                UserUpdates = reader.IsDBNull(12) ? 0L : Convert.ToInt64(reader.GetValue(12)),
                /* last_user_seek/scan/lookup/update (sys.dm_db_index_usage_stats) are SERVER-LOCAL in the
                   store, not naive UTC, so this GREATEST is read verbatim like Lite — NOT through
                   ViewerTimeHelper.ForDisplay (which assumes naive UTC and, in Local/Server mode, would shift it). */
                LastUserAccess = reader.IsDBNull(13) ? null : reader.GetDateTime(13),
                Classification = reader.IsDBNull(14) ? "" : reader.GetString(14)
            });
        }
        return items;
    }
}
