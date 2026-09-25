/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;

using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets recent waiting task snapshots for a server: the waiting tasks captured over the window, newest capture first then longest wait, capped at
    /// <paramref name="limit"/> when one is given.
    ///
    /// <para>The cap is a PARAMETER (#3541 A3), null for the grid callers that read the whole window as they
    /// always did. <c>get_waiting_tasks</c> read this UNBOUNDED and then took <c>limit</c> rows in C#, so a busy
    /// window materialised every waiting-task row to return thirty, and the envelope stated no bound at all.
    /// The tool now passes <c>limit + 1</c> and reads the extra row as truncation.</para>
    /// </summary>
    public async Task<List<WaitingTaskRow>> GetWaitingTasksAsync(int serverId, int hoursBack = 1, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null, int? limit = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        /* #1240 parity: exclude the user's ignored (benign) wait types at DISPLAY time too, so waiting
           tasks already in the DuckDB (collected before a type was ignored) don't surface here — the same
           BuildExclusionClause the wait-stats reads use. */
        var exclude = IgnoredWaitTypes.BuildExclusionClause(_ignoredWaitTypes.Value);

        /* The window's upper edge is $3, so the optional database list starts at $4. Bounding both edges
           (rather than only the lower one) is what lets an as_of anchor mean anything here. The row cap, when
           one is given, binds LAST so the database list keeps its ordinals whether or not a cap is present. */
        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc, utcOffsetMinutes: 0);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);
        var limitClause = limit.HasValue ? $"\nLIMIT ${4 + dbValues.Count}" : string.Empty;
        command.CommandText = $@"
SELECT
    collection_time,
    session_id,
    wait_type,
    wait_duration_ms,
    blocking_session_id,
    resource_description,
    database_name
FROM v_waiting_tasks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3{dbClause}
{exclude}
ORDER BY collection_time DESC, wait_duration_ms DESC{limitClause}";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });
        if (limit.HasValue)
            command.Parameters.Add(new DuckDBParameter { Value = limit.Value });

        var items = new List<WaitingTaskRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new WaitingTaskRow
            {
                CollectionTime = reader.GetDateTime(0),
                SessionId = reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                WaitType = reader.IsDBNull(2) ? "" : reader.GetString(2),
                WaitDurationMs = reader.IsDBNull(3) ? 0 : ToInt64(reader.GetValue(3)),
                BlockingSessionId = reader.IsDBNull(4) ? (int?)null : reader.GetInt32(4),
                ResourceDescription = reader.IsDBNull(5) ? "" : reader.GetString(5),
                DatabaseName = reader.IsDBNull(6) ? "" : reader.GetString(6)
            });
        }

        return items;
    }

    /// <summary>
    /// Whether the waiting-task collector has EVER sampled this server, ignoring any window.
    /// <para>Separates an all-clear from missing data. The wrong answer here is the REASSURING one:
    /// "nothing was waiting" stops a caller looking, where "never collected" sends them to check the
    /// collector. Darling's twin is <c>DarlingDataReader.HasAnyWaitingTaskSampleAsync</c>.</para>
    /// <para>Reads <c>v_waiting_tasks</c>, the same source every other reader in this file uses. A probe
    /// on the base table could report that a server has been sampled for rows the trend itself cannot
    /// see, which would pick the wrong branch in exactly the case this exists to get right.</para>
    /// </summary>
    public async Task<bool> HasAnyWaitingTaskSampleAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT 1
FROM v_waiting_tasks
WHERE server_id = $1
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        return await command.ExecuteScalarAsync() is not null and not DBNull;
    }

    /// <summary>
    /// Gets waiting task duration trend grouped by wait type for charting.
    /// <para>#4349: bucketed (matching #4234/#4340's shape). A waiting-task snapshot carries no delta or
    /// sample interval, so unlike the #3540 trend family every row is unconditionally "rated" — a bucket's
    /// total is the SUM of its rows' durations, and <c>collection_count</c> is a plain <c>COUNT(*)</c>.
    /// Buckets to <see cref="TrendBudget.Chart"/>'s point budget per wait type; when every bucket the call
    /// returns holds exactly one physical collection, every point is stamped at its own raw collection time
    /// instead of the <c>time_bucket</c> grid line.</para>
    /// </summary>
    public async Task<List<WaitingTaskTrendPoint>> GetWaitingTaskTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        /* #1240 parity: exclude the user's ignored (benign) wait types at DISPLAY time (mirrors the
           wait-stats reads) so the Current Waits duration chart matches the Wait Stats tab. */
        var exclude = IgnoredWaitTypes.BuildExclusionClause(_ignoredWaitTypes.Value);
        command.CommandText = $@"
SELECT
    wait_type,
    GREATEST(time_bucket(to_minutes(CAST($4 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    SUM(wait_duration_ms) AS total_wait_ms,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) AS collection_count
FROM v_waiting_tasks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   wait_type IS NOT NULL
{exclude}
GROUP BY
    wait_type, 2
ORDER BY
    wait_type, 2";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(string WaitType, DateTime BucketStart, DateTime FirstCollectionTime, long TotalWaitMs)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (Convert.ToInt64(reader.GetValue(4)) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(3),
                reader.IsDBNull(2) ? 0 : ToInt64(reader.GetValue(2))));
        }

        var items = new List<WaitingTaskTrendPoint>();
        foreach (var row in rows)
        {
            items.Add(new WaitingTaskTrendPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                WaitType = row.WaitType,
                TotalWaitMs = row.TotalWaitMs
            });
        }
        return items;
    }

    /// <summary>
    /// Gets blocked session count trend grouped by database for charting.
    /// <para>#4349: bucketed (matching #4234/#4340's shape). Same "every row is rated" note as
    /// <see cref="GetWaitingTaskTrendAsync"/> — a snapshot row carries no delta, so <c>collection_count</c>
    /// is a plain <c>COUNT(*)</c> over the bucket's rows.</para>
    /// </summary>
    public async Task<List<BlockedSessionTrendPoint>> GetBlockedSessionTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);
        var widthParam = 4 + dbValues.Count;

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        command.CommandText = $@"
SELECT
    database_name,
    GREATEST(time_bucket(to_minutes(CAST(${widthParam} AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    COUNT(*) AS blocked_count,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) AS collection_count
FROM v_waiting_tasks
WHERE server_id = $1
AND   blocking_session_id > 0
AND   collection_time >= $2
AND   collection_time <= $3" + dbClause + $@"
AND   database_name IS NOT NULL
GROUP BY
    database_name, 2
ORDER BY
    database_name, 2";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(string DatabaseName, DateTime BucketStart, DateTime FirstCollectionTime, int BlockedCount)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (Convert.ToInt64(reader.GetValue(4)) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(3),
                reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2))));
        }

        var items = new List<BlockedSessionTrendPoint>();
        foreach (var row in rows)
        {
            items.Add(new BlockedSessionTrendPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                DatabaseName = row.DatabaseName,
                BlockedCount = row.BlockedCount
            });
        }
        return items;
    }
}

public class WaitingTaskRow
{
    public DateTime CollectionTime { get; set; }
    public int SessionId { get; set; }
    public string WaitType { get; set; } = "";
    public long WaitDurationMs { get; set; }
    public int? BlockingSessionId { get; set; }
    public string ResourceDescription { get; set; } = "";
    public string DatabaseName { get; set; } = "";

    public string WaitDurationFormatted => WaitDurationMs < 1000
        ? $"{WaitDurationMs} ms"
        : WaitDurationMs < 60000
            ? $"{WaitDurationMs / 1000.0:F1} s"
            : $"{WaitDurationMs / 60000.0:F1} min";
}

public class WaitingTaskTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public string WaitType { get; set; } = "";
    public long TotalWaitMs { get; set; }
}

public class BlockedSessionTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public string DatabaseName { get; set; } = "";
    public int BlockedCount { get; set; }
}
