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
using Microsoft.Extensions.Logging;
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
    public sealed record BlockingTrendReadPoint(DateTime Time, int Count, string? Source = null);

    /// <summary>
    /// Blocking-incident count per minute — the viewer's <c>BlockingTrendSql</c>. XE blocked-process reports
    /// (<c>v_blocked_process_reports</c>) are the primary source, bucketed on <c>event_time</c>; the always-on
    /// DMV snapshot (<c>v_dmv_blocking_snapshots</c>) is appended only when the XE source has no rows in the
    /// window for the chosen databases (<c>WHERE NOT EXISTS</c> over the filtered <c>bpr</c>, the rule <c>get_blocking_stats</c> and the desktop use too), so a server with both sources never double-counts. $1 server_id,
    /// $2 window start, $3 window end (naive UTC). Every row carries a <c>source</c> column (#5244, "blocked-process-report" or
    /// "DMV snapshot", the tags <c>get_blocking</c> rows carry) so the answer says which arm it came from; the arms are never mixed.
    /// $4 is the <see cref="EventWindowFloor"/> for $2, and $5 the
    /// database filter (<c>text[]</c>, NULL = all; #5244) — both
    /// tables are hypertables partitioned on <c>collection_time</c>, which this event-time window alone gives
    /// the planner nothing to exclude a chunk on (#4229); the floor lets it skip every chunk older than the
    /// window, without being able to drop a row (an event is collected after it happens).
    ///
    /// <para><b>The database predicate is the list form (#5244), on BOTH arms</b>, as the desktop's <c>BlockingTrendSql</c> has it.
    /// The DMV arm's <c>NOT EXISTS (SELECT 1 FROM bpr)</c> therefore asks whether the XE source has rows for the CHOSEN databases:
    /// a database with only DMV-captured blocking still charts, whatever other databases' XE reports hold.</para>
    ///
    /// <para><b>A parallel query waiting on itself is not blocking (#6645, #6705)</b>: both arms carry
    /// <see cref="BlockingWaits.NotSelfWaitSql"/>, as <c>PgAnomalyDetector.BlockingWindowSql</c> does, so the bars (and the web
    /// Overview strip drawn from this read) agree with the baseline band. The DMV arm is therefore drawn when the FILTERED XE
    /// arm is empty, a window whose only XE reports are self-waits included.</para>
    /// </summary>
    public static readonly string BlockingTrendSql = $$"""
        WITH bpr AS (
            SELECT DATE_TRUNC('minute', event_time) AS bucket, COUNT(*) AS incident_count, 'blocked-process-report' AS source
            FROM v_blocked_process_reports
            WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
            AND   collection_time >= $4
            {{DatabaseFilter.All.Clause("database_name", 5)}}
            AND   {{BlockingWaits.NotSelfWaitSql}}
            GROUP BY DATE_TRUNC('minute', event_time)
        ),
        dmv AS (
            SELECT DATE_TRUNC('minute', event_time) AS bucket, COUNT(*) AS incident_count, 'DMV snapshot' AS source
            FROM v_dmv_blocking_snapshots
            WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
            AND   collection_time >= $4
            {{DatabaseFilter.All.Clause("database_name", 5)}}
            AND   {{BlockingWaits.NotSelfWaitSql}}
            GROUP BY DATE_TRUNC('minute', event_time)
        )
        SELECT bucket, incident_count, source FROM bpr
        UNION ALL
        SELECT bucket, incident_count, source FROM dmv WHERE NOT EXISTS (SELECT 1 FROM bpr)
        ORDER BY bucket
        """;

    /// <summary>
    /// Deadlock count per minute — the viewer's <c>DeadlockTrendSql</c>. Buckets on the deadlock's own
    /// <c>deadlock_time</c> and windows on it too, so a count and its bucket agree. Reads <c>v_deadlocks</c>.
    /// $1 server_id, $2 window start, $3 window end (naive UTC). $4 is the <see cref="EventWindowFloor"/> for
    /// $2 (no upper bound, so a late-collected deadlock still counts).
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
            AND   deadlock_time >= $2
            AND   deadlock_time <= $3
            AND   collection_time >= $4
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
        => GetBlockingTrendAsync(postgres, serverId, startUtc, endUtc, DatabaseFilter.All, cancellationToken);

    /// <summary>The same trend over a SET of databases (#5244): <see cref="DatabaseFilter.All"/> is every database, otherwise both
    /// arms count only the named databases' blocking.</summary>
    public static Task<List<BlockingTrendReadPoint>> GetBlockingTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, DatabaseFilter databases, CancellationToken cancellationToken = default)
        => ReadCountTrendAsync(postgres, BlockingTrendSql, serverId, startUtc, endUtc, boundEventWindow: true, databases, cancellationToken);

    /// <summary>Deadlock-per-minute buckets for one server over the window.</summary>
    public static Task<List<BlockingTrendReadPoint>> GetDeadlockTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
        => ReadCountTrendAsync(postgres, DeadlockTrendSql, serverId, startUtc, endUtc, boundEventWindow: true, null, cancellationToken);

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
        bool boundEventWindow, DatabaseFilter? databases, CancellationToken cancellationToken)
    {
        var items = new List<BlockingTrendReadPoint>();
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        if (boundEventWindow)
            DarlingMcpReadParameters.AddTimestamp(command, EventWindowFloor.For(startUtc));
        if (databases is { } filter)
            command.Parameters.Add(filter.Parameter());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new BlockingTrendReadPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : (int)reader.GetInt64(1),
                reader.FieldCount > 2 && !reader.IsDBNull(2) ? reader.GetString(2) : null));
        }

        return items;
    }

    /* ───────────────── the time a trend's runs covered (#7850) ───────────────── */

    /// <summary>One stretch of time the collector's successful runs read, naive UTC, clipped to the window.</summary>
    public sealed record CoveredRange(DateTime Start, DateTime End);

    /// <summary>
    /// The longest spacing between two successful runs that still counts as read time, in minutes (#7850).
    /// <para>Both trends read a dedicated Extended Events RING BUFFER (<c>PerformanceMonitor_BlockedProcess</c>,
    /// <c>PerformanceMonitor_Deadlock</c>), not a file target. The session keeps capturing on the monitored server while the
    /// service is stopped, so an event in a stop is usually read at the next run, but the buffer is 4 MB and drops its oldest
    /// events when it fills, a SQL Server restart or a re-created session empties it, and a run that finds no stored event reads
    /// back only <c>CollectorContext.EventFallbackWindow</c> (10 minutes). The store keeps no record of what the buffer still
    /// held, so a long stretch between two runs is not time anyone can call event-free. A SUCCESS run proves the session existed
    /// and was read (a missing one logs SESSION_MISSING, not SUCCESS). The hour is the floor the quiet bar's observed time uses
    /// for the same question (<c>ObservedQuietGaps.GapFloorMinutes</c>); a collector scheduled less often than that is never
    /// covered, which leaves its trend unfilled, the pre-#7839 look, never a false 0.</para>
    /// </summary>
    public const int CoveredGapCapMinutes = (int)ObservedQuietGaps.GapFloorMinutes;

    /// <summary>
    /// Each consecutive pair of one collector's SUCCESS runs no further apart than <see cref="CoveredGapCapMinutes"/>, for the
    /// pairs that overlap the window. The lookback of one cap before the window start brings in the run that began the first
    /// pair, and the look-ahead of one cap past the window end brings in the run that ended the last one, so a pair that crosses
    /// either edge is kept and <see cref="MergeCovered"/> clips it (an <c>as_of</c> window ending between two runs is covered up
    /// to its end). A window that ends now has no run past it, so the time after the newest run stays uncovered. $1 server_id, $2/$3 window (naive UTC), $4 collector name, $5 the cap in minutes. Only the SUCCESS rows of
    /// the one collector: a run that failed read nothing, and the other blocking collector (the DMV snapshot) is a sample, not
    /// the report source this trend's zero fill depends on.
    /// </summary>
    public const string CoveredRunPairsSql = """
        SELECT prev_time, run_time
        FROM (
            SELECT collection_time AS run_time,
                   LAG(collection_time) OVER (ORDER BY collection_time) AS prev_time
            FROM v_collection_log
            WHERE server_id = $1
            AND   collector_name = $4
            AND   status = 'SUCCESS'
            AND   collection_time >= $2 - ($5 * INTERVAL '1 minute')
            AND   collection_time <= $3 + ($5 * INTERVAL '1 minute')
        ) runs
        WHERE prev_time IS NOT NULL
        AND   run_time >= $2
        AND   prev_time <= $3
        AND   run_time - prev_time <= $5 * INTERVAL '1 minute'
        ORDER BY run_time
        """;

    /// <summary>
    /// Merges the run pairs into ranges and clips them to the window. Pairs that touch or overlap become one range, so a collector
    /// that ran without a break gives one. Input must be ordered by its end time, as <see cref="CoveredRunPairsSql"/> returns it.
    /// </summary>
    public static List<CoveredRange> MergeCovered(IEnumerable<(DateTime From, DateTime To)> pairs, DateTime startUtc, DateTime endUtc)
    {
        var merged = new List<CoveredRange>();
        foreach (var (from, to) in pairs)
        {
            var s = from < startUtc ? startUtc : from;
            var e = to > endUtc ? endUtc : to;
            if (e <= s)
            {
                continue;
            }

            if (merged.Count > 0 && s <= merged[^1].End)
            {
                if (e > merged[^1].End)
                {
                    merged[^1] = merged[^1] with { End = e };
                }
            }
            else
            {
                merged.Add(new CoveredRange(s, e));
            }
        }

        return merged;
    }

    /// <summary>
    /// The time <paramref name="collectorName"/>'s successful runs read inside the window (#7850), as merged ranges: empty when no
    /// two runs within <see cref="CoveredGapCapMinutes"/> of each other fall in it.
    /// </summary>
    public static async Task<List<CoveredRange>> GetCoveredRangesAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, string collectorName, CancellationToken cancellationToken = default)
    {
        var pairs = new List<(DateTime, DateTime)>();
        await using var command = postgres.CreateCommand(CoveredRunPairsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddText(command, collectorName);
        DarlingMcpReadParameters.AddInt(command, CoveredGapCapMinutes);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            pairs.Add((reader.GetDateTime(0), reader.GetDateTime(1)));
        }

        return MergeCovered(pairs, startUtc, endUtc);
    }

    /// <summary>A test's stand-in for the covered read, per async flow (null: the store is asked).</summary>
    private static readonly System.Threading.AsyncLocal<Func<Task<List<CoveredRange>>>?> s_testOnlyCoveredRead = new();

    /// <summary>Test seam: while set, <see cref="TryGetCoveredRangesAsync"/> runs this instead of the store read.</summary>
    internal static Func<Task<List<CoveredRange>>>? TestOnlyCoveredRead
    {
        get => s_testOnlyCoveredRead.Value;
        set => s_testOnlyCoveredRead.Value = value;
    }

    /// <summary>
    /// <see cref="GetCoveredRangesAsync"/> for a trend reply (#7850): a covered read that fails (a statement timeout, a read budget
    /// that ran out) returns null and logs once, and the caller leaves the <c>covered</c> field out, so the page keeps its
    /// whole-window fill and the trend still answers. The caller's own cancellation is not swallowed.
    /// </summary>
    internal static async Task<List<CoveredRange>?> TryGetCoveredRangesAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, DateTime startUtc, DateTime endUtc, string collectorName,
        ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            var testOnly = TestOnlyCoveredRead;
            return testOnly != null
                ? await testOnly()
                : await GetCoveredRangesAsync(postgres, serverId, startUtc, endUtc, collectorName, cancellationToken);
        }
        catch (Exception ex) when (!(ex is OperationCanceledException && cancellationToken.IsCancellationRequested))
        {
            logger?.LogWarning(ex, "The covered-time read of {Collector} for {Server} failed; the trend goes without its covered list.", collectorName, serverName);
            return null;
        }
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
