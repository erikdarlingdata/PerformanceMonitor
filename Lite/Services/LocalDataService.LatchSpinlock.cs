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

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /* The Latches & Spinlocks readers — the Lite (DuckDB) port of the Darling viewer's
       ViewerDataService.LatchSpinlock reads. Both are cumulative-counter DMV tables (like wait_stats),
       so the per-second rates are computed in SQL from the per-contender LAG interval — the exact
       date_trunc/extract(epoch)/LAG idiom GetWaitStatsTrendAsync uses — because the delta tables carry
       no stored sample_interval_seconds. Reads run against the v_latch_stats / v_spinlock_stats archive
       views (base table UNION the archived parquet), matching every other Lite trend reader. */

    /// <summary>
    /// The Latch Stats trend: the per-second wait rate for the TOP 5 latch classes (by total delta wait
    /// time over the window), normalized to ms/sec via the per-class LAG interval.
    /// </summary>
    public async Task<List<LatchStatsTrendPoint>> GetLatchStatsTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetLatchStatsTrendAsync", "v_latch_stats top-5 per-second trend");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
WITH top_latches AS
(
    SELECT latch_class
    FROM v_latch_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY latch_class
    ORDER BY SUM(delta_wait_time_ms) DESC
    LIMIT 5
),
raw AS
(
    SELECT
        latch_class,
        collection_time,
        delta_wait_time_ms,
        extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY latch_class ORDER BY collection_time)))) AS interval_seconds
    FROM v_latch_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   latch_class IN (SELECT latch_class FROM top_latches)
)
SELECT
    latch_class,
    collection_time,
    CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS wait_time_ms_per_second
FROM raw
ORDER BY latch_class, collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<LatchStatsTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new LatchStatsTrendPoint
            {
                LatchClass = reader.GetString(0),
                CollectionTime = reader.GetDateTime(1),
                WaitTimeMsPerSecond = reader.IsDBNull(2) ? 0 : reader.GetDouble(2)
            });
        }

        return items;
    }

    /// <summary>
    /// The Latch Stats latest-snapshot grid: every latch class captured at the most recent collection in
    /// the window, ordered by the last interval's delta wait time then cumulative wait time, capped at 20.
    /// </summary>
    public async Task<List<LatchStatsSnapshotRow>> GetLatchStatsSnapshotAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetLatchStatsSnapshotAsync", "v_latch_stats latest snapshot");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        command.CommandText = @"
WITH latest AS
(
    SELECT MAX(collection_time) AS mx
    FROM v_latch_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
)
SELECT
    latch_class,
    waiting_requests_count,
    wait_time_ms,
    max_wait_time_ms,
    delta_waiting_requests_count,
    delta_wait_time_ms
FROM v_latch_stats
WHERE server_id = $1
AND   collection_time = (SELECT mx FROM latest)
ORDER BY delta_wait_time_ms DESC, wait_time_ms DESC
LIMIT 20";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<LatchStatsSnapshotRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new LatchStatsSnapshotRow
            {
                LatchClass = reader.GetString(0),
                WaitingRequestsCount = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                WaitTimeMs = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                MaxWaitTimeMs = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                DeltaWaitingRequestsCount = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                DeltaWaitTimeMs = reader.IsDBNull(5) ? 0 : reader.GetInt64(5)
            });
        }

        return items;
    }

    /// <summary>
    /// The Spinlock Stats trend: the per-second collision rate for the TOP 5 spinlocks (by total delta
    /// collisions over the window), the collision analog of the latch trend — collisions/sec via the
    /// per-name LAG interval.
    /// </summary>
    public async Task<List<SpinlockStatsTrendPoint>> GetSpinlockStatsTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetSpinlockStatsTrendAsync", "v_spinlock_stats top-5 per-second trend");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
WITH top_spinlocks AS
(
    SELECT spinlock_name
    FROM v_spinlock_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY spinlock_name
    ORDER BY SUM(delta_collisions) DESC
    LIMIT 5
),
raw AS
(
    SELECT
        spinlock_name,
        collection_time,
        delta_collisions,
        extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY spinlock_name ORDER BY collection_time)))) AS interval_seconds
    FROM v_spinlock_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   spinlock_name IN (SELECT spinlock_name FROM top_spinlocks)
)
SELECT
    spinlock_name,
    collection_time,
    CASE WHEN interval_seconds > 0 THEN CAST(delta_collisions AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS collisions_per_second
FROM raw
ORDER BY spinlock_name, collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<SpinlockStatsTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new SpinlockStatsTrendPoint
            {
                SpinlockName = reader.GetString(0),
                CollectionTime = reader.GetDateTime(1),
                CollisionsPerSecond = reader.IsDBNull(2) ? 0 : reader.GetDouble(2)
            });
        }

        return items;
    }

    /// <summary>
    /// The Spinlock Stats latest-snapshot grid: every spinlock captured at the most recent collection in
    /// the window, ordered by the last interval's delta collisions then cumulative collisions, capped at 20.
    /// </summary>
    public async Task<List<SpinlockStatsSnapshotRow>> GetSpinlockStatsSnapshotAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetSpinlockStatsSnapshotAsync", "v_spinlock_stats latest snapshot");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        command.CommandText = @"
WITH latest AS
(
    SELECT MAX(collection_time) AS mx
    FROM v_spinlock_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
)
SELECT
    spinlock_name,
    collisions,
    spins,
    spins_per_collision,
    sleep_time,
    backoffs,
    delta_collisions,
    delta_spins
FROM v_spinlock_stats
WHERE server_id = $1
AND   collection_time = (SELECT mx FROM latest)
ORDER BY delta_collisions DESC, collisions DESC
LIMIT 20";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<SpinlockStatsSnapshotRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new SpinlockStatsSnapshotRow
            {
                SpinlockName = reader.GetString(0),
                Collisions = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                Spins = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                SpinsPerCollision = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                SleepTime = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                Backoffs = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                DeltaCollisions = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                DeltaSpins = reader.IsDBNull(7) ? 0 : reader.GetInt64(7)
            });
        }

        return items;
    }
}

/// <summary>One point on a latch class's per-second wait trend (this interval's delta_wait_time_ms over
/// the seconds since the previous collection of the SAME class, via a per-class LAG window).</summary>
public class LatchStatsTrendPoint
{
    public string LatchClass { get; set; } = "";
    public DateTime CollectionTime { get; set; }
    public double WaitTimeMsPerSecond { get; set; }
}

/// <summary>One row of the Latch Stats latest-snapshot grid: cumulative counters plus the last interval's
/// deltas for one latch class at the most recent collection in the window.</summary>
public class LatchStatsSnapshotRow
{
    public string LatchClass { get; set; } = "";
    public long WaitingRequestsCount { get; set; }
    public long WaitTimeMs { get; set; }
    public long MaxWaitTimeMs { get; set; }
    public long DeltaWaitingRequestsCount { get; set; }
    public long DeltaWaitTimeMs { get; set; }
}

/// <summary>One point on a spinlock's per-second collision trend (this interval's delta_collisions over
/// the seconds since the previous collection of the SAME spinlock, via a per-name LAG window).</summary>
public class SpinlockStatsTrendPoint
{
    public string SpinlockName { get; set; } = "";
    public DateTime CollectionTime { get; set; }
    public double CollisionsPerSecond { get; set; }
}

/// <summary>One row of the Spinlock Stats latest-snapshot grid: cumulative counters plus the last
/// interval's deltas for one spinlock at the most recent collection in the window.</summary>
public class SpinlockStatsSnapshotRow
{
    public string SpinlockName { get; set; } = "";
    public long Collisions { get; set; }
    public long Spins { get; set; }
    public double SpinsPerCollision { get; set; }
    public long SleepTime { get; set; }
    public long Backoffs { get; set; }
    public long DeltaCollisions { get; set; }
    public long DeltaSpins { get; set; }
}
