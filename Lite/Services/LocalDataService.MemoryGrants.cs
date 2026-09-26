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
    /// The bucketed memory-grant overlay statement text (#4349), pulled out of
    /// <see cref="GetMemoryGrantTrendAsync"/> so its shape is checkable without a live DuckDB, the same
    /// idiom <see cref="MemoryTrendSql"/> uses. A gauge (not an accumulating counter), so a bucket's value
    /// is the plain AVERAGE of its collections' summed-per-collection totals — no #3540 rated split
    /// applies. $1 server_id, $2/$3 the UTC window (also the GREATEST clamp), $4 the bucket width minutes.
    /// </summary>
    internal static string MemoryGrantTrendSql => $@"
WITH per_collection AS
(
    SELECT
        collection_time,
        SUM(granted_memory_mb) AS total_granted_mb
    FROM v_memory_grant_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY collection_time
)
SELECT
    GREATEST(time_bucket(to_minutes(CAST($4 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    AVG(total_granted_mb) AS total_granted_mb,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) AS collection_count
FROM per_collection
GROUP BY 1
ORDER BY 1";

    /// <summary>
    /// Gets memory grant trend — total granted MB per bucket for the Memory Overview overlay. Bucketed to
    /// <see cref="TrendBudget.Chart"/>'s point budget (#4349); a bucket holding exactly one physical
    /// collection is stamped at that collection's own raw time rather than the bucket grid when EVERY
    /// bucket this call returned is such a singleton (ruling item 3).
    /// </summary>
    public async Task<List<MemoryTrendPoint>> GetMemoryGrantTrendAsync(int serverId, int hoursBack = 4, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        command.CommandText = MemoryGrantTrendSql;

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(DateTime BucketStart, DateTime FirstCollectionTime, double TotalGrantedMb, long CollectionCount)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var collectionCount = reader.GetInt64(3);
            if (collectionCount != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetDateTime(0),
                reader.GetDateTime(2),
                reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                collectionCount));
        }

        var items = new List<MemoryTrendPoint>(rows.Count);
        foreach (var row in rows)
        {
            items.Add(new MemoryTrendPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                TotalGrantedMb = row.TotalGrantedMb
            });
        }
        return items;
    }

    /// <summary>
    /// The bucketed Memory Grants chart statement text (#4349), pulled out of
    /// <see cref="GetMemoryGrantChartDataAsync"/> so its shape is checkable without a live DuckDB. The
    /// inner <c>per_collection</c> CTE is the UNCHANGED pre-bucket aggregation (multiple resource-semaphore
    /// rows per pool summed to one row per collection + pool); the outer bucket then AVERAGES every gauge
    /// (sizing MB, grantee/waiter counts — point-in-time readings) and SUMS the two true deltas
    /// (<c>timeout_error_count_delta</c>/<c>forced_grant_count_delta</c>) over a <c>rated</c> CTE that
    /// nulls them out (not filters the row) when <c>sample_interval_seconds</c> is 0 (#3540), matching
    /// Darling's <c>MemoryGrantChartDataSql</c>. $1 server_id, $2/$3 the UTC window, $4 bucket width minutes.
    /// </summary>
    internal static string MemoryGrantChartDataSql => $@"
WITH per_collection AS
(
    SELECT
        collection_time,
        pool_id,
        SUM(available_memory_mb) AS available_memory_mb,
        SUM(granted_memory_mb) AS granted_memory_mb,
        SUM(used_memory_mb) AS used_memory_mb,
        SUM(grantee_count) AS grantee_count,
        SUM(waiter_count) AS waiter_count,
        /* #3540/#4349/#4364: MAX over the collection's own resource-semaphore rows. interval_seconds is the
           sanctioned NULLIF form the measurement-contract census requires of any alias named
           interval_sec(onds) — 0 (a restart, the ONLY known-unrateable case) becomes NULL through it. But
           NULLIF alone cannot tell that known-zero NULL apart from a pre-#3540 row's true NULL (interval
           never recorded) once collapsed into one alias, so the rated CTE below tests the RAW
           max_interval_seconds_raw instead — IS DISTINCT FROM 0 — to keep an unknown interval's delta
           while still nulling a genuine restart's, matching Darling's MemoryGrantChartDataSql. */
        NULLIF(MAX(sample_interval_seconds), 0) AS interval_seconds,
        MAX(sample_interval_seconds) AS max_interval_seconds_raw,
        SUM(timeout_error_count_delta) AS timeout_error_count_delta,
        SUM(forced_grant_count_delta) AS forced_grant_count_delta
    FROM v_memory_grant_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY collection_time, pool_id
),
rated AS
(
    SELECT
        collection_time,
        pool_id,
        available_memory_mb,
        granted_memory_mb,
        used_memory_mb,
        grantee_count,
        waiter_count,
        CASE WHEN max_interval_seconds_raw IS DISTINCT FROM 0 THEN timeout_error_count_delta END AS rated_timeout_error_count_delta,
        CASE WHEN max_interval_seconds_raw IS DISTINCT FROM 0 THEN forced_grant_count_delta END AS rated_forced_grant_count_delta
    FROM per_collection
)
SELECT
    pool_id,
    GREATEST(time_bucket(to_minutes(CAST($4 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    AVG(available_memory_mb) AS available_memory_mb,
    AVG(granted_memory_mb) AS granted_memory_mb,
    AVG(used_memory_mb) AS used_memory_mb,
    AVG(grantee_count) AS grantee_count,
    AVG(waiter_count) AS waiter_count,
    SUM(rated_timeout_error_count_delta) AS timeout_error_count_delta,
    SUM(rated_forced_grant_count_delta) AS forced_grant_count_delta,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) AS collection_count
FROM rated
GROUP BY pool_id, 2
ORDER BY pool_id, 2";

    /// <summary>
    /// Gets memory grant chart data aggregated by bucket and pool_id for the Memory Grants sub-tab charts.
    /// Bucketed to <see cref="TrendBudget.Chart"/>'s point budget PER POOL (#4349); a bucket holding
    /// exactly one physical collection is stamped at that collection's own raw time rather than the
    /// bucket grid when EVERY bucket this call returned (across every pool) is such a singleton (ruling
    /// item 3).
    /// </summary>
    public async Task<List<MemoryGrantChartPoint>> GetMemoryGrantChartDataAsync(int serverId, int hoursBack = 4, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        command.CommandText = MemoryGrantChartDataSql;

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(int PoolId, DateTime BucketStart, DateTime FirstCollectionTime, double Available, double Granted, double Used, int Grantee, int Waiter, long TimeoutDelta, long ForcedDelta, long CollectionCount)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var collectionCount = reader.GetInt64(10);
            if (collectionCount != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                reader.GetDateTime(1),
                reader.GetDateTime(9),
                reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                reader.IsDBNull(5) ? 0 : (int)ToInt64(reader.GetValue(5)),
                reader.IsDBNull(6) ? 0 : (int)ToInt64(reader.GetValue(6)),
                reader.IsDBNull(7) ? 0 : ToInt64(reader.GetValue(7)),
                reader.IsDBNull(8) ? 0 : ToInt64(reader.GetValue(8)),
                collectionCount));
        }

        var items = new List<MemoryGrantChartPoint>(rows.Count);
        foreach (var row in rows)
        {
            items.Add(new MemoryGrantChartPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                PoolId = row.PoolId,
                AvailableMemoryMb = row.Available,
                GrantedMemoryMb = row.Granted,
                UsedMemoryMb = row.Used,
                GranteeCount = row.Grantee,
                WaiterCount = row.Waiter,
                TimeoutErrorCountDelta = row.TimeoutDelta,
                ForcedGrantCountDelta = row.ForcedDelta
            });
        }
        return items;
    }

    /// <summary>
    /// The resource-semaphore latest-snapshot read (the get_resource_semaphore MCP lens): every resource
    /// semaphore captured at the most recent collection in the window, with the full ceiling column set
    /// (target / max-target / total workspace memory, cumulative timeout/forced grant counts, and the
    /// per-interval deltas) — the same shape the Dashboard's get_resource_semaphore serves. Distinct from
    /// <see cref="GetMemoryGrantChartDataAsync"/>, which aggregates a per-pool grant subset for the charts;
    /// this returns one row per (pool_id, resource_semaphore_id) with the ceiling metrics intact.
    /// </summary>
    public async Task<List<ResourceSemaphoreRow>> GetResourceSemaphoreSnapshotAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
WITH latest AS
(
    SELECT MAX(collection_time) AS mx
    FROM v_memory_grant_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
)
SELECT
    collection_time,
    resource_semaphore_id,
    pool_id,
    target_memory_mb,
    max_target_memory_mb,
    total_memory_mb,
    available_memory_mb,
    granted_memory_mb,
    used_memory_mb,
    grantee_count,
    waiter_count,
    timeout_error_count,
    forced_grant_count,
    timeout_error_count_delta,
    forced_grant_count_delta,
    sample_interval_seconds
FROM v_memory_grant_stats
WHERE server_id = $1
AND   collection_time = (SELECT mx FROM latest)
ORDER BY pool_id, resource_semaphore_id";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<ResourceSemaphoreRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new ResourceSemaphoreRow
            {
                CollectionTime = reader.GetDateTime(0),
                ResourceSemaphoreId = reader.IsDBNull(1) ? 0 : (int)ToInt64(reader.GetValue(1)),
                PoolId = reader.IsDBNull(2) ? 0 : (int)ToInt64(reader.GetValue(2)),
                TargetMemoryMb = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                MaxTargetMemoryMb = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                TotalMemoryMb = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                AvailableMemoryMb = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                GrantedMemoryMb = reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7)),
                UsedMemoryMb = reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8)),
                GranteeCount = reader.IsDBNull(9) ? 0 : (int)ToInt64(reader.GetValue(9)),
                WaiterCount = reader.IsDBNull(10) ? 0 : (int)ToInt64(reader.GetValue(10)),
                TimeoutErrorCount = reader.IsDBNull(11) ? 0 : ToInt64(reader.GetValue(11)),
                ForcedGrantCount = reader.IsDBNull(12) ? 0 : ToInt64(reader.GetValue(12)),
                TimeoutErrorCountDelta = reader.IsDBNull(13) ? 0 : ToInt64(reader.GetValue(13)),
                ForcedGrantCountDelta = reader.IsDBNull(14) ? 0 : ToInt64(reader.GetValue(14)),
                /* NULL stays NULL: a pre-v61 row never recorded its interval, and that is a different
                   statement from the 0 the calculator writes when no delta was knowable. */
                SampleIntervalSeconds = reader.IsDBNull(15) ? null : (int)ToInt64(reader.GetValue(15))
            });
        }
        return items;
    }

    /* ─────────────────────────── the window, per semaphore / per pool (#3541 A10) ─────────────────────────── */

    /// <summary>
    /// The SQL behind <see cref="GetResourceSemaphoreWindowAsync"/>: every memory_grant_stats snapshot in the
    /// window aggregated per (resource_semaphore_id, pool_id) — the storm detector get_resource_semaphore
    /// serves BESIDE its latest snapshot. Until #3541 A10 the tool accepted <c>hours_back</c> and read only the
    /// newest snapshot in it, so a grant storm three hours ago was invisible behind a calm latest row while
    /// the parameter read as a window. The peak's instant comes from a <c>DISTINCT ON</c> over the same
    /// windowed rows (highest waiter_count first, newest first on a tie, so a storm that plateaued reports its
    /// latest snapshot); the two <c>*_in_window</c> figures SUM the stored per-interval deltas — the reason the
    /// collector stores them, and no interval arithmetic (a restart's fabricated 0 adds 0). Public so
    /// Lite.Tests can pin the dialect without a store. $1 server_id, $2 window start, $3 window end.
    /// </summary>
    public const string ResourceSemaphoreWindowSql = @"
WITH windowed AS
(
    SELECT
        collection_time,
        resource_semaphore_id,
        pool_id,
        waiter_count,
        granted_memory_mb,
        available_memory_mb,
        timeout_error_count_delta,
        forced_grant_count_delta
    FROM v_memory_grant_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
),
agg AS
(
    SELECT
        resource_semaphore_id,
        pool_id,
        COUNT(*) AS snapshots_in_window,
        MIN(collection_time) AS first_snapshot_at,
        MAX(collection_time) AS last_snapshot_at,
        MAX(waiter_count) AS peak_waiter_count,
        MAX(granted_memory_mb) AS peak_granted_memory_mb,
        MIN(available_memory_mb) AS min_available_memory_mb,
        SUM(timeout_error_count_delta) AS timeout_errors_in_window,
        SUM(forced_grant_count_delta) AS forced_grants_in_window
    FROM windowed
    GROUP BY resource_semaphore_id, pool_id
),
peak AS
(
    SELECT DISTINCT ON (resource_semaphore_id, pool_id)
        resource_semaphore_id,
        pool_id,
        collection_time AS peak_waiters_at
    FROM windowed
    ORDER BY resource_semaphore_id, pool_id, waiter_count DESC, collection_time DESC
)
SELECT
    a.resource_semaphore_id,
    a.pool_id,
    a.snapshots_in_window,
    a.first_snapshot_at,
    a.last_snapshot_at,
    a.peak_waiter_count,
    p.peak_waiters_at,
    a.peak_granted_memory_mb,
    a.min_available_memory_mb,
    a.timeout_errors_in_window,
    a.forced_grants_in_window
FROM agg AS a
JOIN peak AS p
  ON p.resource_semaphore_id = a.resource_semaphore_id
 AND p.pool_id = a.pool_id
ORDER BY a.pool_id, a.resource_semaphore_id";

    /// <summary>
    /// The SQL behind <see cref="GetMemoryGrantsWindowAsync"/>: the pool lens of <see cref="ResourceSemaphoreWindowSql"/>.
    /// A pool's semaphores are SUMMED at each snapshot first (the same per-snapshot SUM
    /// <see cref="GetMemoryGrantChartDataAsync"/> serves), THEN the window's peak / floor / total is taken over
    /// those per-snapshot pool figures, so peak waiters is the most sessions waiting on the pool at one instant,
    /// not the largest single semaphore's count. <c>resource_semaphore_id</c> is a typed NULL so both reads
    /// project the same eleven columns. $1 server_id, $2 window start, $3 window end.
    /// </summary>
    public const string MemoryGrantsWindowSql = @"
WITH per_snapshot AS
(
    SELECT
        collection_time,
        pool_id,
        SUM(waiter_count) AS waiter_count,
        SUM(granted_memory_mb) AS granted_memory_mb,
        SUM(available_memory_mb) AS available_memory_mb,
        SUM(timeout_error_count_delta) AS timeout_error_count_delta,
        SUM(forced_grant_count_delta) AS forced_grant_count_delta
    FROM v_memory_grant_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY collection_time, pool_id
),
agg AS
(
    SELECT
        pool_id,
        COUNT(*) AS snapshots_in_window,
        MIN(collection_time) AS first_snapshot_at,
        MAX(collection_time) AS last_snapshot_at,
        MAX(waiter_count) AS peak_waiter_count,
        MAX(granted_memory_mb) AS peak_granted_memory_mb,
        MIN(available_memory_mb) AS min_available_memory_mb,
        SUM(timeout_error_count_delta) AS timeout_errors_in_window,
        SUM(forced_grant_count_delta) AS forced_grants_in_window
    FROM per_snapshot
    GROUP BY pool_id
),
peak AS
(
    SELECT DISTINCT ON (pool_id)
        pool_id,
        collection_time AS peak_waiters_at
    FROM per_snapshot
    ORDER BY pool_id, waiter_count DESC, collection_time DESC
)
SELECT
    CAST(NULL AS INTEGER) AS resource_semaphore_id,
    a.pool_id,
    a.snapshots_in_window,
    a.first_snapshot_at,
    a.last_snapshot_at,
    a.peak_waiter_count,
    p.peak_waiters_at,
    a.peak_granted_memory_mb,
    a.min_available_memory_mb,
    a.timeout_errors_in_window,
    a.forced_grants_in_window
FROM agg AS a
JOIN peak AS p ON p.pool_id = a.pool_id
ORDER BY a.pool_id";

    /// <summary>Every snapshot in the window aggregated per (resource_semaphore_id, pool_id) — the window half
    /// of get_resource_semaphore (#3541 A10). Same window arguments as
    /// <see cref="GetResourceSemaphoreSnapshotAsync"/>, so the window's <c>last_snapshot_at</c> IS the
    /// snapshot that read serves.</summary>
    public Task<List<MemoryGrantWindowRow>> GetResourceSemaphoreWindowAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null) =>
        ReadMemoryGrantWindowAsync(ResourceSemaphoreWindowSql, "GetResourceSemaphoreWindowAsync", serverId, hoursBack, fromDate, toDate, asOfUtc);

    /// <summary>Every snapshot in the window aggregated per pool — the window half of get_memory_grants
    /// (#3541 A10).</summary>
    public Task<List<MemoryGrantWindowRow>> GetMemoryGrantsWindowAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null) =>
        ReadMemoryGrantWindowAsync(MemoryGrantsWindowSql, "GetMemoryGrantsWindowAsync", serverId, hoursBack, fromDate, toDate, asOfUtc);

    /// <summary>The two window reads project the SAME eleven columns in the same order, so one materialiser
    /// serves both. DuckDB widens SUM(INTEGER) to HUGEINT and COUNT(*) to BIGINT, so every count goes through
    /// <c>ToInt64</c> and every MB figure through <c>ToDouble</c>, like the readers above.</summary>
    private async Task<List<MemoryGrantWindowRow>> ReadMemoryGrantWindowAsync(
        string sql, string queryName, int serverId, int hoursBack, DateTime? fromDate, DateTime? toDate, DateTime? asOfUtc)
    {
        using var _q = TimeQuery(queryName, "v_memory_grant_stats window aggregate");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = sql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<MemoryGrantWindowRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new MemoryGrantWindowRow
            {
                ResourceSemaphoreId = reader.IsDBNull(0) ? null : (int)ToInt64(reader.GetValue(0)),
                PoolId = reader.IsDBNull(1) ? 0 : (int)ToInt64(reader.GetValue(1)),
                SnapshotsInWindow = ToInt64(reader.GetValue(2)),
                FirstSnapshotAt = reader.GetDateTime(3),
                LastSnapshotAt = reader.GetDateTime(4),
                PeakWaiterCount = reader.IsDBNull(5) ? 0 : ToInt64(reader.GetValue(5)),
                PeakWaitersAt = reader.GetDateTime(6),
                PeakGrantedMemoryMb = reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7)),
                MinAvailableMemoryMb = reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8)),
                TimeoutErrorsInWindow = reader.IsDBNull(9) ? 0 : ToInt64(reader.GetValue(9)),
                ForcedGrantsInWindow = reader.IsDBNull(10) ? 0 : ToInt64(reader.GetValue(10))
            });
        }
        return items;
    }
}

/// <summary>
/// One (resource_semaphore_id, pool_id) — or, for the pool lens, one pool with <see cref="ResourceSemaphoreId"/>
/// null — aggregated over EVERY memory_grant_stats snapshot in the window (#3541 A10). <see cref="PeakWaiterCount"/>
/// / <see cref="PeakWaitersAt"/> is the storm detector: the most sessions ever seen waiting for a grant in the
/// window and the snapshot it happened at. The two <c>*InWindow</c> figures are SUMs of the stored per-interval
/// deltas — how many grants timed out / were forced across the whole window, not just the last interval.
/// Twin of Darling's <c>DarlingMemoryGrantReader.MemoryGrantWindowRow</c>.
/// </summary>
public class MemoryGrantWindowRow
{
    public int? ResourceSemaphoreId { get; set; }
    public int PoolId { get; set; }
    public long SnapshotsInWindow { get; set; }
    public DateTime FirstSnapshotAt { get; set; }
    public DateTime LastSnapshotAt { get; set; }
    public long PeakWaiterCount { get; set; }
    public DateTime PeakWaitersAt { get; set; }
    public double PeakGrantedMemoryMb { get; set; }
    public double MinAvailableMemoryMb { get; set; }
    public long TimeoutErrorsInWindow { get; set; }
    public long ForcedGrantsInWindow { get; set; }
}

public class MemoryGrantChartPoint
{
    public DateTime CollectionTime { get; set; }
    public int PoolId { get; set; }
    public double AvailableMemoryMb { get; set; }
    public double GrantedMemoryMb { get; set; }
    public double UsedMemoryMb { get; set; }
    public int GranteeCount { get; set; }
    public int WaiterCount { get; set; }
    public long TimeoutErrorCountDelta { get; set; }
    public long ForcedGrantCountDelta { get; set; }
}

/// <summary>One resource-semaphore latest-snapshot row (the get_resource_semaphore MCP lens): one
/// (pool_id, resource_semaphore_id) semaphore's full ceiling metrics at the most recent collection in the
/// window — target / max-target / total workspace memory, granted vs available/used, grantee/waiter counts,
/// the cumulative + per-interval-delta timeout/forced-grant pressure counters, and (since v61, #3540) the
/// measured seconds those deltas accrued over: <c>0</c> is the calculator's "no delta knowable" marker (a
/// restart, not a quiet semaphore), <c>null</c> a pre-v61 row that never recorded one.</summary>
public class ResourceSemaphoreRow
{
    public DateTime CollectionTime { get; set; }
    public int ResourceSemaphoreId { get; set; }
    public int PoolId { get; set; }
    public double TargetMemoryMb { get; set; }
    public double MaxTargetMemoryMb { get; set; }
    public double TotalMemoryMb { get; set; }
    public double AvailableMemoryMb { get; set; }
    public double GrantedMemoryMb { get; set; }
    public double UsedMemoryMb { get; set; }
    public int GranteeCount { get; set; }
    public int WaiterCount { get; set; }
    public long TimeoutErrorCount { get; set; }
    public long ForcedGrantCount { get; set; }
    public long TimeoutErrorCountDelta { get; set; }
    public long ForcedGrantCountDelta { get; set; }
    public int? SampleIntervalSeconds { get; set; }

    /// <summary>True when the row's deltas are the calculator's (0, 0) marker: no delta was knowable, so
    /// the two <c>*Delta</c> zeros beside it are not "no timeouts this interval".</summary>
    public bool IsUnknowable => SampleIntervalSeconds == 0;
}
