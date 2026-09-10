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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Blocking-tab time-range slicer bucket reads (W1e) — Lite's <c>GetBlockingSlicerDataAsync</c> and
/// <c>GetDeadlockSlicerDataAsync</c> (LocalDataService.Blocking.cs) ported to Postgres. Both bucket by
/// <c>date_trunc('hour', collection_time)</c> into the shared <see cref="TimeSliceBucket"/> the copied
/// slicer control consumes. Two dialect notes: Postgres <c>COUNT(*)</c>/<c>COUNT(DISTINCT …)</c> are
/// <c>bigint</c> and <c>SUM(…)/1000.0</c> is <c>numeric</c>, so the reads go through
/// <c>Convert.ToInt64</c>/<c>Convert.ToDouble</c> (the same tolerant idiom the pair-row + aggregate reads
/// use). The blocking read prefers the XE source and appends the always-on DMV snapshot only when the XE
/// source has no buckets in the window (<c>WHERE NOT EXISTS</c>), so a server with both never
/// double-counts.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// Hourly blocking-event buckets for the slicer — Lite's <c>GetBlockingSlicerDataAsync</c> ported.
    /// The XE blocked-process reports are the primary source; the DMV snapshot contributes only when the
    /// XE source has no buckets in the window. Columns feed the slicer's sort-driven metric overlay
    /// (events / total-wait-sec / distinct blockers / blocked / databases).
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string BlockingSlicerSql = """
        WITH bpr AS (
            SELECT
                date_trunc('hour', collection_time) AS bucket,
                COUNT(*) AS event_count,
                COALESCE(SUM(wait_time_ms), 0) / 1000.0 AS total_wait_sec,
                COUNT(DISTINCT blocking_spid) AS distinct_blockers,
                COUNT(DISTINCT blocked_spid) AS distinct_blocked,
                COUNT(DISTINCT database_name) AS distinct_databases
            FROM v_blocked_process_reports
            WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            GROUP BY date_trunc('hour', collection_time)
        ),
        dmv AS (
            SELECT
                date_trunc('hour', collection_time) AS bucket,
                COUNT(*) AS event_count,
                COALESCE(SUM(wait_time_ms), 0) / 1000.0 AS total_wait_sec,
                COUNT(DISTINCT blocking_spid) AS distinct_blockers,
                COUNT(DISTINCT blocked_spid) AS distinct_blocked,
                COUNT(DISTINCT database_name) AS distinct_databases
            FROM v_dmv_blocking_snapshots
            WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            GROUP BY date_trunc('hour', collection_time)
        )
        SELECT bucket, event_count, total_wait_sec, distinct_blockers, distinct_blocked, distinct_databases FROM bpr
        UNION ALL
        SELECT bucket, event_count, total_wait_sec, distinct_blockers, distinct_blocked, distinct_databases FROM dmv
        WHERE NOT EXISTS (SELECT 1 FROM bpr)
        ORDER BY bucket
        """;

    public async Task<List<TimeSliceBucket>> GetBlockingSlicerDataAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var items = new List<TimeSliceBucket>();

        await using var command = _dataSource.CreateCommand(BlockingSlicerSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddBlockingParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var eventCount = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1));
            items.Add(new TimeSliceBucket
            {
                BucketTime = reader.GetDateTime(0),
                SessionCount = eventCount,
                TotalCpu = reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2)),
                TotalElapsed = reader.IsDBNull(3) ? 0 : Convert.ToDouble(reader.GetValue(3)),
                TotalReads = reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4)),
                TotalLogicalReads = reader.IsDBNull(5) ? 0 : Convert.ToDouble(reader.GetValue(5)),
                Value = eventCount,
            });
        }

        return items;
    }

    /// <summary>
    /// Hourly deadlock-count buckets for the slicer — Lite's <c>GetDeadlockSlicerDataAsync</c> ported.
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string DeadlockSlicerSql = """
        SELECT
            date_trunc('hour', collection_time) AS bucket,
            COUNT(*) AS deadlock_count
        FROM v_deadlocks
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY date_trunc('hour', collection_time)
        ORDER BY bucket
        """;

    public async Task<List<TimeSliceBucket>> GetDeadlockSlicerDataAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<TimeSliceBucket>();

        await using var command = _dataSource.CreateCommand(DeadlockSlicerSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddBlockingParameters(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var count = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1));
            items.Add(new TimeSliceBucket
            {
                BucketTime = reader.GetDateTime(0),
                SessionCount = count,
                Value = count,
            });
        }

        return items;
    }
}
