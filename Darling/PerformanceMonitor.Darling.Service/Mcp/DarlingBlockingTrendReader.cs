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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side reads for the three Blocking-Trends MCP tools (<see cref="DarlingMcpBlockingTools"/>
/// get_blocking_trend / get_deadlock_trend / get_lock_wait_trend). The SQL is reproduced verbatim from the
/// viewer's Blocking-Trends charts (<c>ViewerDataService.BlockingTrends.cs</c>), which are Lite's
/// <c>GetBlockingTrendAsync</c> / <c>GetDeadlockTrendAsync</c> / <c>GetLockWaitTrendAsync</c> ported to
/// Postgres. All are STORED reads (no live monitored-server hit). The first two share a
/// <c>(bucket timestamp, COUNT(*))</c> shape, so one reader maps both; COUNT(*) is <c>bigint</c> in Postgres,
/// read via GetInt64 and narrowed to the point's int. The lock-wait lane has its own two reads — the family's
/// bucketed fractional RATE and the per-type legend (#3897) — rather than a count.
/// Public-const SQL so Darling.Tests pin the dialect (the XE-preferred + DMV-fallback union, the deadlock
/// bucket-on-deadlock_time, the lock-wait LAG interval) without a live Postgres.
/// </summary>
internal static class DarlingBlockingTrendReader
{
    /// <summary>One incident-count-per-minute bucket (mirror of the viewer's <c>BlockingTrendPoint</c>).</summary>
    public sealed record BlockingTrendReadPoint(DateTime Time, int Count);

    /// <summary>
    /// Blocking-incident count per minute — the viewer's <c>BlockingTrendSql</c>. XE blocked-process reports
    /// (<c>v_blocked_process_reports</c>) are the primary source, bucketed on <c>event_time</c>; the always-on
    /// DMV snapshot (<c>v_dmv_blocking_snapshots</c>) is appended only when the XE source has no rows in the
    /// window (<c>WHERE NOT EXISTS</c>), so a server with both sources never double-counts. $1 server_id,
    /// $2 window start, $3 window end (naive UTC). $4 is the <see cref="EventWindowFloor"/> for $2 — both
    /// tables are hypertables partitioned on <c>collection_time</c>, which this event-time window alone gives
    /// the planner nothing to exclude a chunk on (#4229); the floor lets it skip every chunk older than the
    /// window, without being able to drop a row (an event is collected after it happens).
    /// </summary>
    public const string BlockingTrendSql = """
        WITH bpr AS (
            SELECT DATE_TRUNC('minute', event_time) AS bucket, COUNT(*) AS incident_count
            FROM v_blocked_process_reports
            WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
            AND   collection_time >= $4
            GROUP BY DATE_TRUNC('minute', event_time)
        ),
        dmv AS (
            SELECT DATE_TRUNC('minute', event_time) AS bucket, COUNT(*) AS incident_count
            FROM v_dmv_blocking_snapshots
            WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
            AND   collection_time >= $4
            GROUP BY DATE_TRUNC('minute', event_time)
        )
        SELECT bucket, incident_count FROM bpr
        UNION ALL
        SELECT bucket, incident_count FROM dmv WHERE NOT EXISTS (SELECT 1 FROM bpr)
        ORDER BY bucket
        """;

    /// <summary>
    /// Deadlock count per minute — the viewer's <c>DeadlockTrendSql</c>. Buckets on the deadlock's own
    /// <c>deadlock_time</c> while windowing on the collection prefix. Reads <c>v_deadlocks</c>. $1 server_id,
    /// $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string DeadlockTrendSql = """
        SELECT
            bucket,
            deadlock_count
        FROM (
            SELECT
                DATE_TRUNC('minute', deadlock_time) AS bucket,
                COUNT(*) AS deadlock_count
            FROM v_deadlocks
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY DATE_TRUNC('minute', deadlock_time)
        ) sub
        ORDER BY bucket
        """;

    /// <summary>
    /// The LCK% rows both lock-wait statements read (#3897), reduced to the ones whose rate is knowable — the rows
    /// the pre-#3897 per-collection read kept. <c>raw</c> is the viewer's <c>LockWaitTrendSql</c> CTE verbatim:
    /// <c>v_wait_stats</c> filtered to lock waits, each row's interval the STORED one (#3540; 0, no delta knowable,
    /// becomes NULL through NULLIF) or, on a pre-V127 row, the LAG of the prior collection per wait type.
    /// <c>rated</c> keeps a row only where that interval is positive and the delta is not negative — a negative
    /// delta is the counter reset across a SQL Server restart, not a negative wait — and CASTs the delta to double
    /// precision so a sub-one-millisecond-per-second rate does not truncate to zero (#2507).
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    private const string LockWaitRatedCtes = """
        raw AS
        (
            SELECT
                collection_time,
                wait_type,
                delta_wait_time_ms,
                /* #3540: the STORED interval where the row has one; 0 (no delta knowable) becomes NULL through NULLIF
                   and the row is left out rather than read as 0.00. NULL (a pre-V127 row) falls back to the LAG. */
                CASE WHEN sample_interval_seconds IS NULL
                     THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY wait_type ORDER BY collection_time))))
                     ELSE NULLIF(sample_interval_seconds, 0)
                END AS interval_seconds
            FROM v_wait_stats
            WHERE server_id = $1
            AND   wait_type LIKE 'LCK%'
            AND   collection_time >= $2
            AND   collection_time <= $3
        ),
        rated AS
        (
            SELECT
                collection_time,
                wait_type,
                CAST(delta_wait_time_ms AS double precision) AS wait_ms,
                interval_seconds
            FROM raw
            WHERE interval_seconds > 0
            AND   delta_wait_time_ms >= 0
        )
        """;

    /// <summary>
    /// Every LCK% type the window rated, with its window totals (#3897): the legend that says which types make up
    /// the family series, and the one read that still names them one by one — the pre-#3897 payload carried a row
    /// per type per collection, 96% of them zero, and grew with the number of lock types a server had ever seen.
    /// A type that never waited comes back with a zero total, so the tool can count it without listing it.
    /// The rate is the total over the seconds the rated rows covered; the peak is the worst single collection.
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string LockWaitTypesSql = $"""
        WITH {LockWaitRatedCtes}
        SELECT
            wait_type,
            SUM(wait_ms) AS total_wait_ms,
            SUM(interval_seconds) AS rated_seconds,
            MAX(CASE WHEN interval_seconds > 0 THEN wait_ms / interval_seconds END) AS peak_wait_time_ms_per_second
        FROM rated
        GROUP BY wait_type
        ORDER BY wait_type
        """;

    /// <summary>
    /// The lock-wait FAMILY, bucketed (#3897) — what the tool's description has always called it: every LCK% type's
    /// wait summed per collection and rated over that collection's interval (<c>per_collection</c>: one interval
    /// per collection, not one per type, so summing the types cannot multiply the denominator), then
    /// time-weighted across the bucket — the bucket's summed wait over its summed seconds, never an average of
    /// per-collection rates. The peak is the worst single collection's family rate, so a one-minute pile-up
    /// survives a sixty-minute bucket. A collection none of whose rows is rated contributes nothing, and a
    /// bucket made only of those does not appear — the old read dropped such rows the same way.
    /// Each point is stamped at its bucket's start, the first at the window's start. $1 server_id, $2 window
    /// start, $3 window end (naive UTC), $4 the bucket width in minutes.
    /// </summary>
    public const string LockWaitTrendSql = $"""
        WITH {LockWaitRatedCtes},
        per_collection AS
        (
            SELECT
                collection_time,
                SUM(wait_ms) AS wait_ms,
                MAX(interval_seconds) AS interval_seconds
            FROM rated
            GROUP BY collection_time
        )
        SELECT
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
            SUM(wait_ms) / SUM(interval_seconds) AS wait_time_ms_per_second,
            MAX(CASE WHEN interval_seconds > 0 THEN wait_ms / interval_seconds END) AS peak_wait_time_ms_per_second
        FROM per_collection
        GROUP BY 1
        ORDER BY 1
        """;

    /// <summary>Blocking-incident-per-minute buckets for one server over the window.</summary>
    public static Task<List<BlockingTrendReadPoint>> GetBlockingTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
        => ReadCountTrendAsync(postgres, BlockingTrendSql, serverId, startUtc, endUtc, boundEventWindow: true, cancellationToken);

    /// <summary>Deadlock-per-minute buckets for one server over the window.</summary>
    public static Task<List<BlockingTrendReadPoint>> GetDeadlockTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
        => ReadCountTrendAsync(postgres, DeadlockTrendSql, serverId, startUtc, endUtc, boundEventWindow: false, cancellationToken);

    /// <summary>
    /// The lock-wait family for one server over the window, one point per bucket of <paramref name="bucketMinutes"/>
    /// (#3897). Its own mapper rather than the count-trend one above: the rate is a double the whole point of
    /// which is that it is fractional. The SQL rates only knowable rows, so every point it returns has a rate.
    /// </summary>
    public static async Task<List<LockWaitPoint>> GetLockWaitTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int bucketMinutes,
        CancellationToken cancellationToken = default)
    {
        var items = new List<LockWaitPoint>();
        await using var command = postgres.CreateCommand(LockWaitTrendSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddInt(command, bucketMinutes);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new LockWaitPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture),
                reader.IsDBNull(2) ? null : Convert.ToDouble(reader.GetValue(2), CultureInfo.InvariantCulture)));
        }

        return items;
    }

    /// <summary>Runs <see cref="LockWaitTypesSql"/>: every LCK% type the window rated, with its totals.</summary>
    public static async Task<List<LockWaitType>> GetLockWaitTypesAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<LockWaitType>();
        await using var command = postgres.CreateCommand(LockWaitTypesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new LockWaitType(
                reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture),
                reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2), CultureInfo.InvariantCulture),
                reader.IsDBNull(3) ? null : Convert.ToDouble(reader.GetValue(3), CultureInfo.InvariantCulture)));
        }

        return items;
    }

    /// <summary>The blocking and deadlock trends share a (bucket timestamp, COUNT(*)) shape, so one reader
    /// maps both. COUNT(*) is bigint in Postgres, read via GetInt64 and narrowed to the point's int.</summary>
    private static async Task<List<BlockingTrendReadPoint>> ReadCountTrendAsync(
        NpgsqlDataSource postgres, string sql, int serverId, DateTime startUtc, DateTime endUtc,
        bool boundEventWindow, CancellationToken cancellationToken)
    {
        var items = new List<BlockingTrendReadPoint>();
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        if (boundEventWindow)
            DarlingMcpReadParameters.AddTimestamp(command, EventWindowFloor.For(startUtc));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new BlockingTrendReadPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : (int)reader.GetInt64(1)));
        }

        return items;
    }

    /* ───────────────── the denominator an empty trend needs ───────────────── */

    /// <summary>
    /// One collector's SUCCESSFUL run count inside a window, with the first and last of those runs.
    /// <para>Both trends read EDGE tables — rows exist only where an event happened — so "no rows" is a
    /// capture that found nothing and a capture that never ran, wearing the same face. Neither table can
    /// tell them apart; <c>collection_log</c> can, because a collector that ran and stored nothing still
    /// records a SUCCESS with zero rows. Same reasoning as
    /// <c>DarlingPgBlockingReader.PgBlockingCaptureCounts</c>, applied to the SQL Server side.</para>
    /// </summary>
    public sealed record CaptureCount(string CollectorName, long Runs, DateTime? FirstRunAt, DateTime? LastRunAt);

    /// <summary>
    /// Successful runs per blocking collector inside the window. BOTH capture paths, deliberately: the trend
    /// above unions <c>v_blocked_process_reports</c> with <c>v_dmv_blocking_snapshots</c>, so counting one of
    /// them would report "never captured" for a server capturing perfectly well through the other — the wrong
    /// branch in exactly the case this exists to get right. Only SUCCESS counts as having looked; a PERMISSIONS
    /// or ERROR row is a collector that did not see the window either. $1 server_id, $2/$3 window (naive UTC).
    /// </summary>
    public const string BlockingCaptureCountsSql = """
        SELECT
            collector_name,
            COUNT(*),
            MIN(collection_time),
            MAX(collection_time)
        FROM v_collection_log
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   status = 'SUCCESS'
        AND   collector_name IN ('blocked_process_report', 'dmv_blocking_snapshot')
        GROUP BY collector_name
        ORDER BY collector_name
        """;

    /// <summary>
    /// Successful runs of the deadlock collector inside the window. One capture path here, not two — deadlocks
    /// come only from the <c>deadlocks</c> collector's system_health read, and there is no DMV fallback to
    /// count. $1 server_id, $2/$3 window (naive UTC).
    /// </summary>
    public const string DeadlockCaptureCountsSql = """
        SELECT
            collector_name,
            COUNT(*),
            MIN(collection_time),
            MAX(collection_time)
        FROM v_collection_log
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   status = 'SUCCESS'
        AND   collector_name = 'deadlocks'
        GROUP BY collector_name
        ORDER BY collector_name
        """;

    /// <summary>
    /// Whether either blocking collector has EVER run successfully for this server, ignoring any window.
    /// <para>Asked ONLY when the window count came back zero, and only to pick which sentence is true: a
    /// server whose collectors have run before has a GAP in this window (widen it, or go look at collection
    /// health), while one that has never run them is not collecting blocking at all. Both are "not an
    /// all-clear" and they want different next moves. LIMIT 1, so it stops at the first row.</para>
    /// <para>NOT the same question as asking whether an EVENT was ever captured. A server collected
    /// perfectly for months that simply never blocked has no event rows, and an event-existence probe
    /// reports it as never captured — the reassuring-answer failure inverted, sending someone to fix
    /// collection that is working. Hence "collector run", not "capture": the denominator is whether we
    /// LOOKED, not whether we found something.</para>
    /// <para>This applies to get_blocking_stats too, which originally used an event-existence probe on the
    /// grounds that its verdict is about severity. That reasoning does not survive contact with a healthy
    /// server: zero severity is the NORMAL state, so the same false alarm fires. It uses these.</para>
    /// </summary>
    public const string HasAnyBlockingCollectorRunSql = """
        SELECT 1
        FROM v_collection_log
        WHERE server_id = $1
        AND   status = 'SUCCESS'
        AND   collector_name IN ('blocked_process_report', 'dmv_blocking_snapshot')
        LIMIT 1
        """;

    /// <summary>Whether the deadlock collector has EVER run successfully for this server. See
    /// <see cref="HasAnyBlockingCollectorRunSql"/> for why the question is asked at all.</summary>
    public const string HasAnyDeadlockCollectorRunSql = """
        SELECT 1
        FROM v_collection_log
        WHERE server_id = $1
        AND   status = 'SUCCESS'
        AND   collector_name = 'deadlocks'
        LIMIT 1
        """;

    /// <summary>Runs <see cref="BlockingCaptureCountsSql"/>.</summary>
    public static Task<List<CaptureCount>> GetBlockingCaptureCountsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
        => ReadCaptureCountsAsync(postgres, BlockingCaptureCountsSql, serverId, startUtc, endUtc, cancellationToken);

    /// <summary>Runs <see cref="DeadlockCaptureCountsSql"/>.</summary>
    public static Task<List<CaptureCount>> GetDeadlockCaptureCountsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
        => ReadCaptureCountsAsync(postgres, DeadlockCaptureCountsSql, serverId, startUtc, endUtc, cancellationToken);

    /// <summary>Runs <see cref="HasAnyBlockingCollectorRunSql"/>.</summary>
    public static Task<bool> HasAnyBlockingCollectorRunAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
        => HasAnyCaptureAsync(postgres, HasAnyBlockingCollectorRunSql, serverId, cancellationToken);

    /// <summary>Runs <see cref="HasAnyDeadlockCollectorRunSql"/>.</summary>
    public static Task<bool> HasAnyDeadlockCollectorRunAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
        => HasAnyCaptureAsync(postgres, HasAnyDeadlockCollectorRunSql, serverId, cancellationToken);

    /// <summary>Both capture-count reads share a (collector_name, COUNT(*), MIN, MAX) shape, so one mapper
    /// serves them. COUNT(*) is bigint in Postgres.</summary>
    private static async Task<List<CaptureCount>> ReadCaptureCountsAsync(
        NpgsqlDataSource postgres, string sql, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        var items = new List<CaptureCount>();
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new CaptureCount(
                reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                reader.IsDBNull(3) ? null : reader.GetDateTime(3)));
        }

        return items;
    }

    /// <summary>Both existence probes share one shape: a scalar that is null when no row qualifies.</summary>
    private static async Task<bool> HasAnyCaptureAsync(
        NpgsqlDataSource postgres, string sql, int serverId, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }
}
