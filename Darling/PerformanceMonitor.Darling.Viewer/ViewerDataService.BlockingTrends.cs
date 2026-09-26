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

using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One incident-count-per-minute bucket for the Blocking Trends charts (mirror of Lite's
/// <c>TrendPoint</c>); shared by the blocking-incident and deadlock trend charts, which spike-plot it.</summary>
public sealed record BlockingTrendPoint(DateTime Time, int Count);

/// <summary>One LCK% wait's per-second rate at a collection (mirror of Lite's <c>LockWaitTrendPoint</c>),
/// grouped by wait type for the Blocking Trends lock-wait chart.
/// <para>#4349: <c>CollectionTime</c> is a <c>date_bin</c> bucket start, EXCEPT when every bucket the call
/// returned holds exactly one physical collection, in which case every point is stamped at its own raw
/// collection time instead — see <see cref="ViewerDataService.GetLockWaitTrendAsync"/>.</para>
/// </summary>
public sealed record LockWaitTrendPoint(DateTime CollectionTime, string WaitType, double WaitTimeMsPerSecond);

/// <summary>One wait type's total waiting-task duration at a collection (mirror of Lite's
/// <c>WaitingTaskTrendPoint</c>), for the Current Waits duration chart.
/// <para>#4349: bucketed the same way as <see cref="LockWaitTrendPoint"/> — see
/// <see cref="ViewerDataService.GetWaitingTaskTrendAsync"/>.</para>
/// </summary>
public sealed record WaitingTaskTrendPoint(DateTime CollectionTime, string WaitType, long TotalWaitMs);

/// <summary>One database's blocked-session count at a collection (mirror of Lite's
/// <c>BlockedSessionTrendPoint</c>), for the Current Waits blocked-sessions chart.
/// <para>#4349: bucketed the same way as <see cref="LockWaitTrendPoint"/> — see
/// <see cref="ViewerDataService.GetBlockedSessionTrendAsync"/>.</para>
/// </summary>
public sealed record BlockedSessionTrendPoint(DateTime CollectionTime, string DatabaseName, int BlockedCount);

/// <summary>
/// The Blocking tab's trend reads (W1c viewer copy-parity): the three Blocking-Trends charts
/// (lock-wait rate, blocking incidents, deadlocks) and the two Current-Waits charts (waiting-task
/// duration by wait type, blocked sessions by database). Each is Lite's <c>LocalDataService</c> query
/// (<c>.Blocking.cs</c> / <c>.WaitingTasks.cs</c>) ported to Postgres. Two dialect notes: (1) COUNT(*)
/// is <c>bigint</c> in Postgres, read via GetInt64 then narrowed to the record's int; (2) SUM of a
/// bigint column is <c>numeric</c> in Postgres, so the waiting-task total is CAST back to bigint for the
/// typed GetInt64 reader — the same adaptation the Queries read makes. The waiting-task reads hit the
/// base <c>waiting_tasks</c> table directly: unlike Lite's DuckDB, the Darling store has no
/// <c>v_waiting_tasks</c> passthrough view (it was left out of the V4/V5 migration view lists); the base
/// table has existed since V1, so no migration is added in this wave.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// Blocking-incident count per minute — Lite's <c>GetBlockingTrendAsync</c> ported to Postgres.
    /// XE blocked-process reports (<c>v_blocked_process_reports</c>) are the primary source, bucketed on
    /// <c>event_time</c>; the always-on DMV snapshot (<c>v_dmv_blocking_snapshots</c>) is appended only
    /// when the XE source has no rows in the window (<c>WHERE NOT EXISTS</c>), so a server with both
    /// sources never double-counts. $1 server_id, $2 window start, $3 window end (naive UTC), $4 database
    /// filter. $5 is the <see cref="EventWindowFloor"/> for $2 — both tables are hypertables partitioned on
    /// <c>collection_time</c>, which this event-time window alone gives the planner nothing to exclude a chunk
    /// on (#4229); the floor lets it skip every chunk older than the window, without being able to drop a row
    /// (an event is collected after it happens).
    /// </summary>
    public const string BlockingTrendSql = """
        WITH bpr AS (
            SELECT DATE_TRUNC('minute', event_time) AS bucket, COUNT(*) AS incident_count
            FROM v_blocked_process_reports
            WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
            AND   collection_time >= $5
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            GROUP BY DATE_TRUNC('minute', event_time)
        ),
        dmv AS (
            SELECT DATE_TRUNC('minute', event_time) AS bucket, COUNT(*) AS incident_count
            FROM v_dmv_blocking_snapshots
            WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
            AND   collection_time >= $5
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            GROUP BY DATE_TRUNC('minute', event_time)
        )
        SELECT bucket, incident_count FROM bpr
        UNION ALL
        SELECT bucket, incident_count FROM dmv WHERE NOT EXISTS (SELECT 1 FROM bpr)
        ORDER BY bucket
        """;

    /// <summary>
    /// Deadlock count per minute — Lite's <c>GetDeadlockTrendAsync</c> ported to Postgres. Buckets on
    /// the deadlock's own <c>deadlock_time</c> while windowing on the collection prefix. Reads
    /// <c>v_deadlocks</c>. $1 server_id, $2 window start, $3 window end (naive UTC).
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
    /// LCK% wait per-second rates — Lite's <c>GetLockWaitTrendAsync</c> ported to Postgres, then bucketed
    /// (#4349, matching #4234's other trend reads). Reads <c>v_wait_stats</c> filtered to lock waits, takes
    /// each row's stored <c>sample_interval_seconds</c> (falling back to the LAG of the prior
    /// collection_time, partitioned by wait type, for pre-V127 rows that never recorded one — #3540), and
    /// nulls (not filters) a row whose interval is unknowable into a <c>rated</c> CTE so the row still
    /// counts toward <c>collection_count</c> below. A bucket's rate is its summed rated wait time over its
    /// summed rated interval-seconds — time-weighted, never an average of per-collection rates, matching
    /// <c>WaitTrendsSql</c>. <c>HAVING COUNT(rated_seconds) &gt; 0</c> drops a bucket with no rated
    /// collection, same as the pre-bucket read's "absent, not 0.00 ms/sec" answer.
    /// $1 server_id, $2 window start, $3 window end (naive UTC), $4 bucket width minutes.
    /// </summary>
    public const string LockWaitTrendSql = $$"""
        WITH raw AS
        (
            SELECT
                collection_time,
                wait_type,
                delta_wait_time_ms,
                /* #3540: the STORED interval where the row has one; 0 (no delta knowable) becomes NULL through NULLIF
                   and the reader drops the row rather than reading 0.00. NULL (a pre-V127 row) falls back to the LAG. */
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
                CASE WHEN interval_seconds > 0 AND delta_wait_time_ms >= 0 THEN delta_wait_time_ms END AS rated_wait_ms,
                CASE WHEN interval_seconds > 0 AND delta_wait_time_ms >= 0 THEN interval_seconds END AS rated_seconds
            FROM raw
        )
        SELECT
            wait_type,
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}), $2) AS bucket_start,
            CAST(SUM(rated_wait_ms) AS double precision) / SUM(rated_seconds) AS wait_time_ms_per_second,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM rated
        GROUP BY wait_type, 2
        HAVING COUNT(rated_seconds) > 0
        ORDER BY wait_type, 2
        """;

    /// <summary>
    /// Waiting-task total duration per (collection, wait type) — Lite's <c>GetWaitingTaskTrendAsync</c>
    /// ported to Postgres, then bucketed (#4349). Reads the base <c>waiting_tasks</c> table (no v_ view
    /// exists); a physical waiting-task snapshot carries no delta or sample interval, so unlike the
    /// #3540 trend family every row is unconditionally "rated" — a bucket's total is simply the SUM of
    /// its rows' durations, and <c>collection_count</c> can never be 0 for a bucket the GROUP BY produced.
    /// SUM of the bigint duration is <c>numeric</c> in Postgres, CAST back to bigint for the typed reader.
    /// $1 server_id, $2 window start, $3 window end (naive UTC), $4 bucket width minutes.
    /// </summary>
    public const string WaitingTaskTrendSql = $$"""
        SELECT
            wait_type,
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}), $2) AS bucket_start,
            CAST(SUM(wait_duration_ms) AS bigint) AS total_wait_ms,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM waiting_tasks
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   wait_type IS NOT NULL
        GROUP BY
            wait_type, 2
        ORDER BY
            wait_type, 2
        """;

    /// <summary>
    /// Blocked-session count per (collection, database) — Lite's <c>GetBlockedSessionTrendAsync</c>
    /// ported to Postgres, then bucketed (#4349). Reads the base <c>waiting_tasks</c> table (no v_ view
    /// exists), counting rows whose <c>blocking_session_id</c> is set. A blocked-session count is a
    /// PER-SNAPSHOT gauge, not a delta — a bucket's value is the AVERAGE of the per-collection counts it
    /// covers (rounded, matching the CPU tab's gauge-averaging idiom, #4234), never their SUM: summing
    /// would double (or N-tuple) the count purely because a wide bucket merged N snapshots, with no more
    /// blocking having happened. The inner <c>per_collection</c> CTE keeps the pre-bucket per-collection
    /// count exactly as the un-bucketed read computed it; <c>collection_count</c> is the number of distinct
    /// physical collections the bucket merged. $1 server_id, $2 window start, $3 window end (naive UTC),
    /// $4 database filter, $5 bucket width minutes.
    /// </summary>
    public const string BlockedSessionTrendSql = $$"""
        WITH per_collection AS
        (
            SELECT
                collection_time,
                database_name,
                COUNT(*) AS blocked_count
            FROM waiting_tasks
            WHERE server_id = $1
            AND   blocking_session_id > 0
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            AND   database_name IS NOT NULL
            GROUP BY
                collection_time,
                database_name
        )
        SELECT
            database_name,
            GREATEST(date_bin(CAST($5 AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}), $2) AS bucket_start,
            CAST(ROUND(AVG(blocked_count)) AS bigint) AS blocked_count,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM per_collection
        GROUP BY
            database_name, 2
        ORDER BY
            database_name, 2
        """;

    /// <summary>Blocking-incident-per-minute buckets for one server over the window (Blocking Trends).</summary>
    public async Task<List<BlockingTrendPoint>> GetBlockingTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
        => await ReadCountTrendAsync(BlockingTrendSql, serverId, startUtc, endUtc, applyDatabaseFilter: true, databaseNames: databaseNames, boundEventWindow: true, cancellationToken: cancellationToken);

    /// <summary>Deadlock-per-minute buckets for one server over the window (Blocking Trends).</summary>
    public async Task<List<BlockingTrendPoint>> GetDeadlockTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
        => await ReadCountTrendAsync(DeadlockTrendSql, serverId, startUtc, endUtc, cancellationToken: cancellationToken);

    /// <summary>
    /// The blocking and deadlock trends share a (bucket timestamp, COUNT(*)) shape, so one reader maps
    /// both. COUNT(*) is bigint in Postgres, read via GetInt64 and narrowed to the record's int.
    /// The blocking trend applies the #1319 database filter (both CTEs carry database_name); the deadlock
    /// trend is server-global (v_deadlocks has no database_name column), so it leaves the filter off.
    /// </summary>
    private async Task<List<BlockingTrendPoint>> ReadCountTrendAsync(
        string sql, int serverId, DateTime startUtc, DateTime endUtc,
        bool applyDatabaseFilter = false, IReadOnlyList<string>? databaseNames = null,
        bool boundEventWindow = false, CancellationToken cancellationToken = default)
    {
        var items = new List<BlockingTrendPoint>();

        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        if (applyDatabaseFilter)
            command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        if (boundEventWindow)
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(startUtc) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new BlockingTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : (int)reader.GetInt64(1)));
        }

        return items;
    }

    /// <summary>
    /// LCK% wait per-second rates for one server over the window (Blocking Trends).
    /// <para>#4349: buckets to <see cref="TrendBudget.Chart"/>'s point budget PER SERIES (wait type), like
    /// <c>GetWaitStatsTrendsByTypesAsync</c> (<c>seriesCount</c> is always 1 into
    /// <see cref="TrendBuckets.AutoMinutes"/>). When every bucket the call returns holds exactly one
    /// physical collection (any wait type, any bucket), every point is stamped at its own raw
    /// <c>first_collection_time</c> instead of the <c>bucket_start</c> grid line.</para>
    /// </summary>
    public async Task<List<LockWaitTrendPoint>> GetLockWaitTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(LockWaitTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });

        var rows = new List<(string WaitType, DateTime BucketStart, DateTime FirstCollectionTime, double Rate)>();
        var everyBucketSingleton = true;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt64(4) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(3),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2)));
        }

        var items = new List<LockWaitTrendPoint>();
        foreach (var row in rows)
        {
            items.Add(new LockWaitTrendPoint(
                everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                row.WaitType,
                row.Rate));
        }

        return items;
    }

    /// <summary>
    /// Waiting-task total duration by wait type for one server over the window (Current Waits).
    /// <para>#4349: bucketed the same way as <see cref="GetLockWaitTrendAsync"/> — see its remarks for
    /// the width and singleton-stamping rules, which this read shares verbatim.</para>
    /// </summary>
    public async Task<List<WaitingTaskTrendPoint>> GetWaitingTaskTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(WaitingTaskTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });

        var rows = new List<(string WaitType, DateTime BucketStart, DateTime FirstCollectionTime, long TotalWaitMs)>();
        var everyBucketSingleton = true;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt64(4) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(3),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2)));
        }

        var items = new List<WaitingTaskTrendPoint>();
        foreach (var row in rows)
        {
            items.Add(new WaitingTaskTrendPoint(
                everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                row.WaitType,
                row.TotalWaitMs));
        }

        return items;
    }

    /// <summary>
    /// Blocked-session count by database for one server over the window (Current Waits).
    /// <para>#4349: bucketed the same way as <see cref="GetLockWaitTrendAsync"/> — see its remarks for
    /// the width and singleton-stamping rules, which this read shares verbatim (per database series).</para>
    /// </summary>
    public async Task<List<BlockedSessionTrendPoint>> GetBlockedSessionTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(BlockedSessionTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });

        var rows = new List<(string DatabaseName, DateTime BucketStart, DateTime FirstCollectionTime, int BlockedCount)>();
        var everyBucketSingleton = true;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt64(4) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(3),
                reader.IsDBNull(2) ? 0 : (int)reader.GetInt64(2)));
        }

        var items = new List<BlockedSessionTrendPoint>();
        foreach (var row in rows)
        {
            items.Add(new BlockedSessionTrendPoint(
                everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                row.DatabaseName,
                row.BlockedCount));
        }

        return items;
    }
}
