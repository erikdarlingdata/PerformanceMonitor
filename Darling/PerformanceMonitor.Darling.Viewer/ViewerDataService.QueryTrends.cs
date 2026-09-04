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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One point on a Performance-Trends chart — the viewer copy of Lite's <c>QueryTrendPoint</c>
/// (LocalDataService.QueryStats.cs). <see cref="Value"/> is the per-second rate the chart plots
/// (elapsed ms/sec for the duration trends, executions/sec for the execution-count trend);
/// <see cref="ExecutionCount"/> carries the executions/sec rate the duration trends also compute
/// (unused by the execution-count trend). CollectionTime is naive UTC — the chart converts it
/// through <see cref="ViewerTimeHelper.ForDisplay"/>.
/// </summary>
public sealed class QueryTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public double Value { get; set; }
    public long ExecutionCount { get; set; }
}

public sealed partial class ViewerDataService
{
    /* The four Performance-Trends reads — Lite's Get{Query,Procedure,ExecutionCount}DurationTrendAsync
       (LocalDataService.QueryStats.cs:1029-1168) + GetQueryStoreDurationTrendAsync
       (LocalDataService.QueryStore.cs:582) ported to Postgres. The SQL is byte-identical to Lite's
       apart from the table name (Lite reads the v_* views; the viewer reads the base tables, matching
       W1f-1's Top-Queries read) — every operator Lite uses (date_trunc('second', …), the
       LAG() OVER (ORDER BY collection_time) per-interval rate, extract(epoch FROM interval),
       CAST(… AS double precision), the positional $1/$2/$3 placeholders) is native Postgres, so no
       dialect rewrite is needed here (unlike the heatmap's time_bucket/ARG_MAX). The per-snapshot
       rate = summed delta over the seconds since the previous snapshot; the first row's LAG is NULL,
       so interval_seconds is NULL and the CASE yields 0 (Lite's behaviour). Summed bigint deltas come
       back as Postgres numeric, so the reads go through Convert tolerantly.
       $1 server_id, $2 window start, $3 window end (naive UTC). */

    /// <summary>Query-stats duration trend: elapsed ms/sec + executions/sec per collection snapshot.</summary>
    public const string QueryDurationTrendSql = """
        WITH raw AS
        (
            SELECT
                collection_time,
                SUM(delta_elapsed_time) / 1000.0 AS total_elapsed_ms,
                SUM(delta_execution_count) AS total_executions,
                extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_seconds
            FROM query_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            GROUP BY collection_time
        )
        SELECT
            collection_time,
            CASE WHEN interval_seconds > 0 THEN total_elapsed_ms / interval_seconds ELSE 0 END AS elapsed_ms_per_second,
            CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS executions_per_second
        FROM raw
        ORDER BY collection_time
        """;

    /// <summary>Procedure-stats duration trend: elapsed ms/sec + executions/sec per collection snapshot.</summary>
    public const string ProcedureDurationTrendSql = """
        WITH raw AS
        (
            SELECT
                collection_time,
                SUM(delta_elapsed_time) / 1000.0 AS total_elapsed_ms,
                SUM(delta_execution_count) AS total_executions,
                extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_seconds
            FROM procedure_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            GROUP BY collection_time
        )
        SELECT
            collection_time,
            CASE WHEN interval_seconds > 0 THEN total_elapsed_ms / interval_seconds ELSE 0 END AS elapsed_ms_per_second,
            CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS executions_per_second
        FROM raw
        ORDER BY collection_time
        """;

    /// <summary>
    /// Query Store duration trend: each interval's work, placed at the time that work RAN.
    ///
    /// <para>#1841 tier 2 corrected this, and the fix is a different shape from the other four Query
    /// Store reads. The three trends above sum per-cycle DELTA columns; Query Store has none, so its
    /// cumulative per-interval snapshots — re-fetched every cycle while the interval stays open — used to
    /// make an interval that reached 40 executions charge 10, then 25, then 40 to three successive
    /// points. Tier 1 deliberately left that in, because dedup ALONE makes this chart worse: it keeps ONE
    /// row per interval at the collection where the interval closed, and Query Store's default
    /// INTERVAL_LENGTH_MINUTES is 60 against a 5-minute cadence, so twelve snapshots collapse onto one
    /// collection_time and a 1-hour window renders a SINGLE point valued 0.</para>
    ///
    /// <para>What unlocks it is the x-axis, not the dedup: <c>interval_start_time_utc</c> is the
    /// interval's own start boundary, converted to UTC AT COLLECTION, so it shares the clock with
    /// collection_time. (The premise this was blocked on — that the interval clock is server-LOCAL — was
    /// wrong on both halves: Query Store's interval start_time and first_execution_time are both
    /// <c>datetimeoffset</c>, and the collector already normalized them through
    /// <c>DateTimeOffset.UtcDateTime</c> before storing.) Deduped and placed at its start, each interval
    /// contributes its true total exactly once at the hour it ran, and the series resolution becomes
    /// Query Store's OWN interval length — the honest resolution of this source.</para>
    ///
    /// <para><b>The legacy boundary.</b> Rows collected before tier 2 carry no interval start and none can
    /// be reconstructed, so they keep the pre-tier-2 treatment exactly: un-deduped, at collection_time,
    /// still overstating. The arms split on <c>interval_start_time_utc IS NULL</c>, which partitions the
    /// rows with no overlap and no gap. Mirrors Lite's GetQueryStoreDurationTrendAsync.</para>
    /// <para><b>#2736: this is now the FALLBACK, not the read.</b> The rank-over-raw below costs the whole
    /// slab regardless of the window; on stores with a materialized
    /// <c>query_store_stats_corrected_hourly</c> the read routes through
    /// <see cref="QueryStoreDurationTrendRollupSql"/> and this shape runs only where it is affordable
    /// (no rollup: plain PostgreSQL, or nothing materialized yet). Its ±slab stays untouched on purpose —
    /// see <see cref="QueryStoreTrendRouting"/>.</para>
    /// </summary>
    public const string QueryStoreDurationTrendSql = """
        WITH placed AS
        (
            /* Arm 1 (#1841 tier 2) — rows carrying the interval identity. Dedup to the interval's FINAL
               cumulative snapshot, then place it at interval_start_time_utc: the hour the work ran, not
               the cycle that last fetched it. Both halves are needed; see the remarks for why dedup alone
               collapses this series and placement alone leaves it inflated. */
            SELECT
                interval_start_time_utc AS point_time,
                execution_count,
                avg_duration_us
            FROM
            (
                SELECT
                    interval_start_time_utc,
                    execution_count,
                    avg_duration_us,
                    ROW_NUMBER() OVER
                    (
                        PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                        ORDER BY collection_time DESC, execution_count DESC
                    ) AS rn
                FROM query_store_stats
                WHERE server_id = $1
                /* Windowed on the column this arm PLACES its points at (#1892), which inside the IS NOT NULL
                   guard below is what COALESCE(interval_start_time_utc, collection_time) resolves to anyway.
                   Filtering on collection_time here put a point outside the range the caller asked for, and
                   dropped the range's final interval because its closing fetch had not happened yet. */
                AND   interval_start_time_utc >= $2
                AND   interval_start_time_utc <= $3
                /* Chunk-exclusion bounds only; see the slicer for why the floor is free and why the ceiling
                   is a month rather than tight. */
                AND   collection_time >= $2 - interval '1 day'
                AND   collection_time <= $3 + interval '30 days'
                AND   interval_start_time_utc IS NOT NULL
                AND   ($4::text[] IS NULL OR database_name = ANY($4))
            ) AS identified
            WHERE rn = 1

            UNION ALL

            /* Arm 2 — rows collected before tier 2. No interval start exists and none can be
               reconstructed, so these keep the pre-tier-2 treatment byte for byte: un-deduped, placed at
               collection_time, still overstating. The split is on IS NULL / IS NOT NULL, so the two arms
               partition the rows exactly — nothing counted twice, nothing dropped. */
            SELECT
                collection_time AS point_time,
                execution_count,
                avg_duration_us
            FROM query_store_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   interval_start_time_utc IS NULL
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        raw AS
        (
            SELECT
                point_time,
                SUM(execution_count * avg_duration_us / 1000.0) AS total_duration_ms,
                SUM(execution_count) AS total_executions,
                extract(epoch FROM (date_trunc('second', point_time) - date_trunc('second', LAG(point_time) OVER (ORDER BY point_time)))) AS interval_seconds
            FROM placed
            GROUP BY point_time
        )
        SELECT
            point_time AS collection_time,
            CASE WHEN interval_seconds > 0 THEN total_duration_ms / interval_seconds ELSE 0 END AS duration_ms_per_second,
            CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS executions_per_second
        FROM raw
        ORDER BY point_time
        """;

    /// <summary>
    /// The rollup-routed Query Store duration trend (#2736): the materialized window portion served from
    /// <c>query_store_stats_corrected_hourly</c> as a rollup scan, the unmaterialized tail from the raw arms
    /// with tail-tight bounds instead of the fixed ±slab. Built by
    /// <see cref="QueryStoreTrendRouting.BuildRollupTrendSql"/> — the SAME builder the MCP reader's twin
    /// uses, so the browser and the desktop viewer cannot disagree about the same hour; this copy carries
    /// the #1319 database filter as $5 (the corrected hourly groups by database_name, so the filter
    /// survives the routing). Chosen per read by <see cref="QueryStoreTrendRouting.ResolveAsync"/>; a store
    /// without the rollup (plain PostgreSQL, or nothing materialized yet) keeps
    /// <see cref="QueryStoreDurationTrendSql"/> byte for byte.
    /// $1 server_id, $2/$3 window, $4 the raw boundary (naive UTC), $5 database filter.
    /// </summary>
    public static readonly string QueryStoreDurationTrendRollupSql =
        QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter: true);

    /// <summary>Execution-count trend: executions/sec per collection snapshot from query_stats.</summary>
    public const string ExecutionCountTrendSql = """
        WITH raw AS
        (
            SELECT
                collection_time,
                SUM(delta_execution_count) AS total_executions,
                extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_seconds
            FROM query_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            GROUP BY collection_time
        )
        SELECT
            collection_time,
            CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS executions_per_second
        FROM raw
        ORDER BY collection_time
        """;

    /// <summary>Query-stats duration trend over [<paramref name="startUtc"/>, <paramref name="endUtc"/>].</summary>
    public Task<List<QueryTrendPoint>> GetQueryDurationTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
        => ReadDurationTrendAsync(QueryDurationTrendSql, serverId, startUtc, endUtc, databaseNames, cancellationToken);

    /// <summary>Procedure-stats duration trend over the window.</summary>
    public Task<List<QueryTrendPoint>> GetProcedureDurationTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
        => ReadDurationTrendAsync(ProcedureDurationTrendSql, serverId, startUtc, endUtc, databaseNames, cancellationToken);

    /// <summary>
    /// Query Store duration trend over the window — routed through the corrected rollup where the store has
    /// one (#2736; see <see cref="QueryStoreDurationTrendRollupSql"/>). The raw-only read is kept unchanged
    /// as the fallback for stores without a materialized rollup, where it is affordable.
    /// </summary>
    public async Task<List<QueryTrendPoint>> GetQueryStoreDurationTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var route = await QueryStoreTrendRouting.ResolveAsync(_dataSource, cancellationToken);
        if (!route.UseRollup)
        {
            return await ReadDurationTrendAsync(QueryStoreDurationTrendSql, serverId, startUtc, endUtc, databaseNames, cancellationToken);
        }

        var items = new List<QueryTrendPoint>();

        await using var command = _dataSource.CreateCommand(QueryStoreDurationTrendRollupSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(new Npgsql.NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(route.RawStartUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new QueryTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                Value = reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1)),
                ExecutionCount = reader.IsDBNull(2) ? 0 : (long)Convert.ToDouble(reader.GetValue(2)),
            });
        }

        return items;
    }

    /// <summary>
    /// Shared reader for the three duration trends (same column shape: collection_time,
    /// value/sec, executions/sec). Value = column 1; ExecutionCount = column 2 truncated to long
    /// (Lite's <c>(long)ToDouble(…)</c>).
    /// </summary>
    private async Task<List<QueryTrendPoint>> ReadDurationTrendAsync(
        string sql, int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames, CancellationToken cancellationToken)
    {
        var items = new List<QueryTrendPoint>();

        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new QueryTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                Value = reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1)),
                ExecutionCount = reader.IsDBNull(2) ? 0 : (long)Convert.ToDouble(reader.GetValue(2)),
            });
        }

        return items;
    }

    /// <summary>Execution-count trend over the window (single value column; no ExecutionCount).</summary>
    public async Task<List<QueryTrendPoint>> GetExecutionCountTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var items = new List<QueryTrendPoint>();

        await using var command = _dataSource.CreateCommand(ExecutionCountTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new QueryTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                Value = reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1)),
            });
        }

        return items;
    }
}
