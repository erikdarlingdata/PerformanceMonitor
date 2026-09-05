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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One incident-count-per-minute bucket for the Blocking Trends charts (mirror of Lite's
/// <c>TrendPoint</c>); shared by the blocking-incident and deadlock trend charts, which spike-plot it.</summary>
public sealed record BlockingTrendPoint(DateTime Time, int Count);

/// <summary>One LCK% wait's per-second rate at a collection (mirror of Lite's <c>LockWaitTrendPoint</c>),
/// grouped by wait type for the Blocking Trends lock-wait chart.</summary>
public sealed record LockWaitTrendPoint(DateTime CollectionTime, string WaitType, double WaitTimeMsPerSecond);

/// <summary>One wait type's total waiting-task duration at a collection (mirror of Lite's
/// <c>WaitingTaskTrendPoint</c>), for the Current Waits duration chart.</summary>
public sealed record WaitingTaskTrendPoint(DateTime CollectionTime, string WaitType, long TotalWaitMs);

/// <summary>One database's blocked-session count at a collection (mirror of Lite's
/// <c>BlockedSessionTrendPoint</c>), for the Current Waits blocked-sessions chart.</summary>
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
    /// sources never double-counts. $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string BlockingTrendSql = """
        WITH bpr AS (
            SELECT DATE_TRUNC('minute', event_time) AS bucket, COUNT(*) AS incident_count
            FROM v_blocked_process_reports
            WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            GROUP BY DATE_TRUNC('minute', event_time)
        ),
        dmv AS (
            SELECT DATE_TRUNC('minute', event_time) AS bucket, COUNT(*) AS incident_count
            FROM v_dmv_blocking_snapshots
            WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
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
    /// LCK% wait per-second rates — Lite's <c>GetLockWaitTrendAsync</c> ported to Postgres. Reads
    /// <c>v_wait_stats</c> filtered to lock waits, derives each row's collection interval from the LAG of
    /// the prior collection_time (partitioned by wait type), and divides the delta wait time by that
    /// interval for a per-second rate (delta cast to double precision). $1 server_id, $2 window start,
    /// $3 window end (naive UTC).
    /// </summary>
    public const string LockWaitTrendSql = """
        WITH raw AS
        (
            SELECT
                collection_time,
                wait_type,
                delta_wait_time_ms,
                extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY wait_type ORDER BY collection_time)))) AS interval_seconds
            FROM v_wait_stats
            WHERE server_id = $1
            AND   wait_type LIKE 'LCK%'
            AND   collection_time >= $2
            AND   collection_time <= $3
        )
        SELECT
            collection_time,
            wait_type,
            CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS double precision) / interval_seconds ELSE 0 END AS wait_time_ms_per_second
        FROM raw
        WHERE delta_wait_time_ms >= 0
        ORDER BY collection_time, wait_type
        """;

    /// <summary>
    /// Waiting-task total duration per (collection, wait type) — Lite's <c>GetWaitingTaskTrendAsync</c>
    /// ported to Postgres. Reads the base <c>waiting_tasks</c> table (no v_ view exists). SUM of the
    /// bigint duration is <c>numeric</c> in Postgres, so it is CAST back to bigint for the typed reader.
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string WaitingTaskTrendSql = """
        SELECT
            collection_time,
            wait_type,
            CAST(SUM(wait_duration_ms) AS bigint) AS total_wait_ms
        FROM waiting_tasks
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   wait_type IS NOT NULL
        GROUP BY
            collection_time,
            wait_type
        ORDER BY
            collection_time,
            wait_type
        """;

    /// <summary>
    /// Blocked-session count per (collection, database) — Lite's <c>GetBlockedSessionTrendAsync</c>
    /// ported to Postgres. Reads the base <c>waiting_tasks</c> table (no v_ view exists), counting rows
    /// whose <c>blocking_session_id</c> is set. $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string BlockedSessionTrendSql = """
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
        ORDER BY
            collection_time,
            database_name
        """;

    /// <summary>Blocking-incident-per-minute buckets for one server over the window (Blocking Trends).</summary>
    public async Task<List<BlockingTrendPoint>> GetBlockingTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
        => await ReadCountTrendAsync(BlockingTrendSql, serverId, startUtc, endUtc, applyDatabaseFilter: true, databaseNames: databaseNames, cancellationToken: cancellationToken);

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
        bool applyDatabaseFilter = false, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
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
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new BlockingTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : (int)reader.GetInt64(1)));
        }

        return items;
    }

    /// <summary>LCK% wait per-second rates for one server over the window (Blocking Trends).</summary>
    public async Task<List<LockWaitTrendPoint>> GetLockWaitTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<LockWaitTrendPoint>();

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
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new LockWaitTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2)));
        }

        return items;
    }

    /// <summary>Waiting-task total duration by wait type for one server over the window (Current Waits).</summary>
    public async Task<List<WaitingTaskTrendPoint>> GetWaitingTaskTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<WaitingTaskTrendPoint>();

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
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new WaitingTaskTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2)));
        }

        return items;
    }

    /// <summary>Blocked-session count by database for one server over the window (Current Waits).</summary>
    public async Task<List<BlockedSessionTrendPoint>> GetBlockedSessionTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var items = new List<BlockedSessionTrendPoint>();

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
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new BlockedSessionTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : (int)reader.GetInt64(2)));
        }

        return items;
    }
}
