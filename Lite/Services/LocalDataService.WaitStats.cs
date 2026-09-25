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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>#4234: TTL memoization for <see cref="GetDistinctWaitTypesAsync"/> — see <see cref="LiteNameListCache"/>.</summary>
    private readonly LiteNameListCache _distinctWaitTypesCache = new();

    /* #1240: the wait-stats tab shares the collector's ignored-wait list and excludes those types at
       DISPLAY time, so benign waits already in the DuckDB (collected before the filter was active) don't
       surface in the tab/picker — copying the JSON only stops new collection, not existing rows. */
    private readonly Lazy<HashSet<string>> _ignoredWaitTypes = new(IgnoredWaitTypes.Load);

    /// <summary>The Wait Stats grid's row cap — the default <paramref name="limit"/> of
    /// <see cref="GetWaitStatsAsync"/>, so every grid caller reads exactly what it always read.</summary>
    public const int WaitStatsGridCap = 50;

    /// <summary>
    /// Gets aggregated wait stats for a server over a time period, sorted by delta wait time, capped at
    /// <paramref name="limit"/> wait types.
    ///
    /// <para>The cap is a PARAMETER with the grid's value as its default (#3541 A3). It was <c>LIMIT 50</c>
    /// while <c>get_wait_stats</c> advertised a <c>limit</c> up to 1,000 and applied it with <c>Take(limit)</c>,
    /// so a caller asking for every wait type on a server that had observed 80 silently got 50. The MCP tool
    /// passes <c>limit + 1</c> and reads the extra row as truncation; the grids pass nothing.</para>
    /// </summary>
    public async Task<List<WaitStatsRow>> GetWaitStatsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null, int limit = WaitStatsGridCap)
    {
        using var _q = TimeQuery("GetWaitStatsAsync", "v_wait_stats top by delta");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        var exclude = IgnoredWaitTypes.BuildExclusionClause(_ignoredWaitTypes.Value);
        command.CommandText = $@"
SELECT
    wait_type,
    SUM(delta_waiting_tasks) AS total_waiting_tasks,
    SUM(delta_wait_time_ms) AS total_wait_time_ms,
    SUM(delta_signal_wait_time_ms) AS total_signal_wait_time_ms,
    COUNT(*) AS sample_count
FROM v_wait_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
{exclude}
GROUP BY wait_type
ORDER BY SUM(delta_wait_time_ms) DESC
LIMIT $4";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = limit });

        var items = new List<WaitStatsRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new WaitStatsRow
            {
                WaitType = reader.GetString(0),
                TotalWaitingTasks = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                TotalWaitTimeMs = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                TotalSignalWaitTimeMs = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                SampleCount = reader.IsDBNull(4) ? 0 : reader.GetInt64(4)
            });
        }

        return items;
    }

    /// <summary>
    /// Whether this server has EVER recorded a wait sample, ignoring any window.
    /// <para>Lets an empty get_wait_types say WHICH kind of nothing it found. "No wait types in the last N
    /// hours" is true both of a quiet window and of a server nothing has been stored for, and those want
    /// opposite responses — widen the window, versus go find out why collection is not running. Reads
    /// <c>v_wait_stats</c>, the same source <see cref="GetDistinctWaitTypesAsync"/> reads. Deliberately
    /// does NOT apply the ignored-wait-type exclusion the read applies: the question here is whether the
    /// COLLECTOR has stored anything, not whether anything survives the display filter. Darling's twin is
    /// <c>DarlingDataReader.HasAnyWaitStatAsync</c>; the two must stay in step so a user moving between the
    /// SKUs is not told a different story about the same state.</para>
    /// </summary>
    public async Task<bool> HasAnyWaitStatAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT 1
FROM v_wait_stats
WHERE server_id = $1
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        return await command.ExecuteScalarAsync() is not null and not DBNull;
    }

    /// <summary>
    /// Gets the distinct wait types that have been collected for a server.
    /// <para>#4234: memoized through <see cref="_distinctWaitTypesCache"/> — keyed on (server, window length)
    /// for <see cref="LiteNameListCache.Ttl"/>, so the full-window read behind this runs at most once per 15
    /// minutes rather than on every 1-minute auto-refresh. <paramref name="nowUtc"/> is the cache's clock
    /// seam — null uses the wall clock; a test passes an explicit time to fast-forward past the TTL without
    /// sleeping.</para>
    /// </summary>
    public async Task<List<string>> GetDistinctWaitTypesAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null, DateTime? nowUtc = null)
    {
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        var effectiveNow = nowUtc ?? DateTime.UtcNow;
        var windowLength = endTime - startTime;
        if (_distinctWaitTypesCache.TryGet(serverId, windowLength, endTime, effectiveNow, out var cached))
        {
            return cached;
        }

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var exclude = IgnoredWaitTypes.BuildExclusionClause(_ignoredWaitTypes.Value);
        command.CommandText = $@"
SELECT
    wait_type,
    SUM(delta_wait_time_ms) AS total_delta
FROM v_wait_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
{exclude}
GROUP BY wait_type
ORDER BY SUM(delta_wait_time_ms) DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<string>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(reader.GetString(0));
        }

        _distinctWaitTypesCache.Set(serverId, windowLength, endTime, items, effectiveNow);
        return items;
    }

    /// <summary>
    /// Gets wait stats trend data for charting.
    /// </summary>
    public async Task<List<WaitStatsTrendPoint>> GetWaitStatsTrendAsync(int serverId, string waitType, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
WITH raw AS
(
    SELECT
        collection_time,
        delta_wait_time_ms,
        delta_signal_wait_time_ms,
        delta_waiting_tasks,
        /* #3540: the STORED interval where the row has one. 0 is the calculator's no-delta-knowable
           marker (first sighting, counter reset, a gap past the policy) and becomes NULL through NULLIF,
           so the rates below are NULL and the reader drops the row — a missing sample, never the confident
           0.00 ms/sec a restart used to render. NULL (a pre-v60 row that never recorded one) falls back to
           the LAG over collection_time this read always used, so history renders exactly as it did. */
        CASE WHEN sample_interval_seconds IS NULL
             THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))
             ELSE NULLIF(sample_interval_seconds, 0)
        END AS interval_seconds
    FROM v_wait_stats
    WHERE server_id = $1
    AND   wait_type = $2
    AND   collection_time >= $3
    AND   collection_time <= $4
)
SELECT
    collection_time,
    CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / interval_seconds END AS wait_time_ms_per_second,
    CASE WHEN interval_seconds > 0 THEN CAST(delta_signal_wait_time_ms AS DOUBLE PRECISION) / interval_seconds END AS signal_wait_time_ms_per_second,
    CASE WHEN interval_seconds > 0 AND delta_waiting_tasks > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / delta_waiting_tasks WHEN interval_seconds > 0 THEN 0 END AS avg_ms_per_wait
FROM raw
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = waitType });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<WaitStatsTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            /* A NULL rate is an unknowable interval (#3540): the row is dropped, not read as 0. */
            if (reader.IsDBNull(1))
            {
                continue;
            }

            items.Add(new WaitStatsTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                WaitTimeMsPerSecond = reader.GetDouble(1),
                SignalWaitTimeMsPerSecond = reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                AvgMsPerWait = reader.IsDBNull(3) ? 0 : reader.GetDouble(3)
            });
        }

        return items;
    }

    /// <summary>
    /// Batched sibling of <see cref="GetWaitStatsTrendAsync"/>: fetches the per-second trend for
    /// ALL selected wait types in ONE query (replacing an N+1 query-per-type loop), grouped by type.
    /// The LAG window is partitioned by wait_type so each type's per-second rate is computed independently.
    /// <para>#4234: buckets to <see cref="TrendBudget.Chart"/>'s point budget PER SERIES (the ruling's own
    /// wording, not the MCP convention of dividing one shared budget across every line a call draws), so the
    /// pin is rows &lt;= budget * series count; <c>seriesCount</c> is therefore always 1 into
    /// <see cref="TrendBuckets.AutoMinutes"/>. A bucket's rate is its summed wait over its summed rated
    /// seconds (time-weighted, never an average of per-collection rates), and a bucket with no rated
    /// collection is dropped (<c>HAVING</c>), same as the per-collection read always dropped that collection.
    /// When EVERY bucket the whole call returned holds exactly one physical collection, each point is stamped
    /// at its bucket's <c>first_collection_time</c> (that one collection's own raw time) instead of
    /// <c>bucket_start</c>, a <c>time_bucket</c> grid line; a single merged bucket anywhere (any wait type)
    /// keeps <c>bucket_start</c> throughout. Darling's twin is <c>ViewerDataService.WaitTrendsSql</c> /
    /// <c>GetWaitStatsTrendsByTypesAsync</c> (#4234, PR #4304).</para>
    /// </summary>
    public async Task<Dictionary<string, List<WaitStatsTrendPoint>>> GetWaitStatsTrendsByTypesAsync(int serverId, List<string> waitTypes, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetWaitStatsTrendsByTypesAsync", "v_wait_stats trends batched by type, bucketed");
        var result = new Dictionary<string, List<WaitStatsTrendPoint>>();
        if (waitTypes.Count == 0) return result;

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);
        var typeParams = string.Join(", ", waitTypes.Select((_, i) => "$" + (i + 4)));
        var widthParam = "$" + (waitTypes.Count + 4);

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        command.CommandText = $@"
WITH raw AS
(
    SELECT
        wait_type,
        collection_time,
        delta_wait_time_ms,
        delta_signal_wait_time_ms,
        delta_waiting_tasks,
        /* #3540: stored interval first, LAG only for pre-v60 rows — see GetWaitStatsTrendAsync. */
        CASE WHEN sample_interval_seconds IS NULL
             THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY wait_type ORDER BY collection_time))))
             ELSE NULLIF(sample_interval_seconds, 0)
        END AS interval_seconds
    FROM v_wait_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   wait_type IN ({typeParams})
),
rated AS
(
    SELECT
        wait_type,
        collection_time,
        CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) END AS rated_wait_ms,
        CASE WHEN interval_seconds > 0 THEN CAST(delta_signal_wait_time_ms AS DOUBLE PRECISION) END AS rated_signal_ms,
        CASE WHEN interval_seconds > 0 THEN interval_seconds END AS rated_seconds,
        CASE WHEN interval_seconds > 0 THEN delta_waiting_tasks END AS rated_tasks
    FROM raw
)
SELECT
    wait_type,
    GREATEST(time_bucket(to_minutes(CAST({widthParam} AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    SUM(rated_wait_ms) / SUM(rated_seconds) AS wait_time_ms_per_second,
    SUM(rated_signal_ms) / SUM(rated_seconds) AS signal_wait_time_ms_per_second,
    CASE WHEN SUM(rated_tasks) > 0 THEN SUM(rated_wait_ms) / SUM(rated_tasks) ELSE 0 END AS avg_ms_per_wait,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) AS collection_count
FROM rated
GROUP BY wait_type, 2
HAVING COUNT(rated_seconds) > 0
ORDER BY wait_type, 2";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var wt in waitTypes)
            command.Parameters.Add(new DuckDBParameter { Value = wt });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(string WaitType, DateTime BucketStart, DateTime FirstCollectionTime, double Rate, double Signal, double Avg)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            /* A NULL rate is a bucket with no rated collection; HAVING already excludes it. */
            if (reader.IsDBNull(2))
            {
                continue;
            }

            if (reader.GetInt64(6) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(5),
                reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4)));
        }

        foreach (var row in rows)
        {
            if (!result.TryGetValue(row.WaitType, out var list))
            {
                list = new List<WaitStatsTrendPoint>();
                result[row.WaitType] = list;
            }

            list.Add(new WaitStatsTrendPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                WaitTimeMsPerSecond = row.Rate,
                SignalWaitTimeMsPerSecond = row.Signal,
                AvgMsPerWait = row.Avg
            });
        }

        return result;
    }

    /// <summary>
    /// Gets total wait time trend across all wait types as a single aggregated time-series.
    /// Used by the correlated timeline lanes for a single-line wait stats overview.
    /// </summary>
    public async Task<List<WaitStatsTrendPoint>> GetTotalWaitTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetTotalWaitTrendAsync", "wait_stats total trend");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);

        var exclude = IgnoredWaitTypes.BuildExclusionClause(_ignoredWaitTypes.Value);
        command.CommandText = $@"
WITH per_collection AS
(
    SELECT
        collection_time,
        SUM(delta_wait_time_ms) AS total_delta_ms,
        /* #3540: the collection's STORED interval — MAX over its rows, because a wait type first seen in
           an otherwise steady pass carries 0 beside its siblings' real interval and contributes 0 to the
           sum; MAX is 0 only when EVERY row was unknowable (a restart), and that 0 becomes NULL through
           NULLIF so the point is dropped rather than rendered as 0.00. NULL (pre-v60 rows) falls back to
           the LAG this read always used. */
        CASE WHEN MAX(sample_interval_seconds) IS NULL
             THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))
             ELSE NULLIF(MAX(sample_interval_seconds), 0)
        END AS interval_seconds
    FROM v_wait_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    {exclude}
    GROUP BY collection_time
)
SELECT
    collection_time,
    CASE WHEN interval_seconds > 0 THEN CAST(total_delta_ms AS DOUBLE PRECISION) / interval_seconds END AS wait_time_ms_per_second
FROM per_collection
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<WaitStatsTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            /* A NULL rate is an unknowable interval (#3540): the row is dropped, not read as 0. */
            if (reader.IsDBNull(1))
            {
                continue;
            }

            items.Add(new WaitStatsTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                WaitTimeMsPerSecond = reader.GetDouble(1)
            });
        }

        return items;
    }

    /// <summary>
    /// Accumulated poison wait per wait type over the alert's window (#3539 A4) — the DuckDB twin of
    /// <c>DarlingAlertReadAdapter.PoisonWaitsSql</c>, over <c>v_wait_stats</c>, for
    /// <see cref="LiteAlertReadAdapter.GetPoisonWaitAccumulationAsync"/>. The Darling text carries the
    /// full rationale (no LIMIT, no <c>delta_waiting_tasks &gt; 0</c> filter, every observed type returned,
    /// how (0, 0) rows are treated); this is that text in DuckDB's dialect and nothing else.
    /// <para>Dialect: DuckDB's <c>SUM(BIGINT)</c> is a HUGEINT, so both sums are CAST back to BIGINT for
    /// <c>GetInt64</c>; <c>$2</c> is a naive-UTC <see cref="DateTime"/> parameter exactly as the retired
    /// read bound it, computed from the CALLER's window so the read's cutoff and the evaluator's denominator
    /// are the same number by construction.</para>
    /// </summary>
    public async Task<List<PoisonWaitAccumulation>> GetPoisonWaitAccumulationAsync(int serverId, int windowMinutes)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = PoisonWaitAccumulationSql;

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddMinutes(-windowMinutes) });

        var items = new List<PoisonWaitAccumulation>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new PoisonWaitAccumulation(
                reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                reader.IsDBNull(4) ? DateTime.MinValue : reader.GetDateTime(4)));
        }

        return items;
    }

    /// <summary>The SQL text of <see cref="GetPoisonWaitAccumulationAsync"/>, exposed for the parity pins.</summary>
    public const string PoisonWaitAccumulationSql = @"
SELECT
    wait_type,
    CAST(SUM(delta_wait_time_ms) AS BIGINT) AS accumulated_wait_ms,
    CAST(SUM(delta_waiting_tasks) AS BIGINT) AS accumulated_waits,
    CAST(COUNT(*) AS BIGINT) AS observed_intervals,
    MAX(collection_time) AS newest_collection_time
FROM v_wait_stats
WHERE server_id = $1
AND wait_type IN ('THREADPOOL', 'RESOURCE_SEMAPHORE', 'RESOURCE_SEMAPHORE_QUERY_COMPILE')
AND collection_time >= $2
GROUP BY wait_type
ORDER BY accumulated_wait_ms DESC";

    /// <summary>
    /// Gets query snapshots filtered by wait type, for the wait drill-down feature.
    /// Returns sessions that were experiencing the specified wait type during the time range.
    /// </summary>
    public async Task<List<QuerySnapshotRow>> GetQuerySnapshotsByWaitTypeAsync(
        int serverId, string waitType, int hoursBack = 24,
        DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
WITH blocked_counts AS (
    SELECT collection_time, blocking_session_id AS blocker_session_id, COUNT(*) AS blocked_session_count
    FROM v_query_snapshots
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   blocking_session_id > 0
    GROUP BY collection_time, blocking_session_id
)
SELECT
    q.session_id,
    q.database_name,
    q.elapsed_time_formatted,
    q.query_text,
    q.status,
    q.blocking_session_id,
    q.wait_type,
    q.wait_time_ms,
    q.wait_resource,
    q.cpu_time_ms,
    q.total_elapsed_time_ms,
    q.reads,
    q.writes,
    q.logical_reads,
    q.granted_query_memory_gb,
    q.transaction_isolation_level,
    q.dop,
    q.parallel_worker_count,
    q.query_plan,
    q.live_query_plan,
    q.collection_time,
    q.login_name,
    q.host_name,
    q.program_name,
    q.open_transaction_count,
    q.percent_complete,
    q.query_hash,
    COALESCE(bc.blocked_session_count, 0) AS blocked_session_count,
    q.requested_memory_mb,
    q.used_memory_mb,
    q.max_used_memory_mb,
    q.tempdb_current_mb,
    q.tempdb_allocations_mb,
    q.tran_log_used_mb,
    q.tran_start_time,
    q.request_id
FROM v_query_snapshots q
LEFT JOIN blocked_counts bc
  ON  bc.collection_time = q.collection_time
  AND bc.blocker_session_id = q.session_id
WHERE q.server_id = $1
AND   q.collection_time >= $2
AND   q.collection_time <= $3
AND   q.wait_type = $4
ORDER BY q.wait_time_ms DESC
LIMIT 500";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = waitType });

        var items = new List<QuerySnapshotRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QuerySnapshotRow
            {
                SessionId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ElapsedTimeFormatted = reader.IsDBNull(2) ? "" : reader.GetString(2),
                QueryText = reader.IsDBNull(3) ? "" : reader.GetString(3),
                Status = reader.IsDBNull(4) ? "" : reader.GetString(4),
                BlockingSessionId = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                WaitType = reader.IsDBNull(6) ? "" : reader.GetString(6),
                WaitTimeMs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                WaitResource = reader.IsDBNull(8) ? "" : reader.GetString(8),
                CpuTimeMs = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                TotalElapsedTimeMs = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                Reads = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                Writes = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                LogicalReads = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                GrantedQueryMemoryGb = reader.IsDBNull(14) ? 0 : ToDouble(reader.GetValue(14)),
                TransactionIsolationLevel = reader.IsDBNull(15) ? "" : reader.GetString(15),
                Dop = reader.IsDBNull(16) ? 0 : reader.GetInt32(16),
                ParallelWorkerCount = reader.IsDBNull(17) ? 0 : reader.GetInt32(17),
                QueryPlan = reader.IsDBNull(18) ? null : reader.GetString(18),
                LiveQueryPlan = reader.IsDBNull(19) ? null : reader.GetString(19),
                CollectionTime = reader.IsDBNull(20) ? DateTime.MinValue : reader.GetDateTime(20),
                LoginName = reader.IsDBNull(21) ? "" : reader.GetString(21),
                HostName = reader.IsDBNull(22) ? "" : reader.GetString(22),
                ProgramName = reader.IsDBNull(23) ? "" : reader.GetString(23),
                OpenTransactionCount = reader.IsDBNull(24) ? 0 : reader.GetInt32(24),
                PercentComplete = reader.IsDBNull(25) ? 0m : Convert.ToDecimal(reader.GetValue(25)),
                QueryHash = reader.IsDBNull(26) ? "" : reader.GetString(26),
                BlockedSessionCount = reader.IsDBNull(27) ? 0 : Convert.ToInt32(reader.GetValue(27)),
                RequestedMemoryMb = reader.IsDBNull(28) ? 0 : ToDouble(reader.GetValue(28)),
                UsedMemoryMb = reader.IsDBNull(29) ? 0 : ToDouble(reader.GetValue(29)),
                MaxUsedMemoryMb = reader.IsDBNull(30) ? 0 : ToDouble(reader.GetValue(30)),
                TempdbCurrentMb = reader.IsDBNull(31) ? 0 : ToDouble(reader.GetValue(31)),
                TempdbAllocationsMb = reader.IsDBNull(32) ? 0 : ToDouble(reader.GetValue(32)),
                TranLogUsedMb = reader.IsDBNull(33) ? 0 : ToDouble(reader.GetValue(33)),
                TranStartTime = reader.IsDBNull(34) ? (DateTime?)null : reader.GetDateTime(34),
                RequestId = reader.IsDBNull(35) ? 0 : reader.GetInt32(35)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets ALL query snapshots in a time range (for chain walking).
    /// Used when a chain wait type (LCK_M_*, LATCH_EX/UP) needs blocking chain traversal.
    /// </summary>
    public async Task<List<QuerySnapshotRow>> GetAllQuerySnapshotsInRangeAsync(
        int serverId, int hoursBack = 24,
        DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
WITH blocked_counts AS (
    SELECT collection_time, blocking_session_id AS blocker_session_id, COUNT(*) AS blocked_session_count
    FROM v_query_snapshots
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   blocking_session_id > 0
    GROUP BY collection_time, blocking_session_id
)
SELECT
    q.session_id,
    q.database_name,
    q.elapsed_time_formatted,
    q.query_text,
    q.status,
    q.blocking_session_id,
    q.wait_type,
    q.wait_time_ms,
    q.wait_resource,
    q.cpu_time_ms,
    q.total_elapsed_time_ms,
    q.reads,
    q.writes,
    q.logical_reads,
    q.granted_query_memory_gb,
    q.transaction_isolation_level,
    q.dop,
    q.parallel_worker_count,
    q.query_plan,
    q.live_query_plan,
    q.collection_time,
    q.login_name,
    q.host_name,
    q.program_name,
    q.open_transaction_count,
    q.percent_complete,
    q.query_hash,
    COALESCE(bc.blocked_session_count, 0) AS blocked_session_count,
    q.requested_memory_mb,
    q.used_memory_mb,
    q.max_used_memory_mb,
    q.tempdb_current_mb,
    q.tempdb_allocations_mb,
    q.tran_log_used_mb,
    q.tran_start_time,
    q.request_id
FROM v_query_snapshots q
LEFT JOIN blocked_counts bc
  ON  bc.collection_time = q.collection_time
  AND bc.blocker_session_id = q.session_id
WHERE q.server_id = $1
AND   q.collection_time >= $2
AND   q.collection_time <= $3
ORDER BY q.collection_time DESC
LIMIT 2000";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<QuerySnapshotRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QuerySnapshotRow
            {
                SessionId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ElapsedTimeFormatted = reader.IsDBNull(2) ? "" : reader.GetString(2),
                QueryText = reader.IsDBNull(3) ? "" : reader.GetString(3),
                Status = reader.IsDBNull(4) ? "" : reader.GetString(4),
                BlockingSessionId = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                WaitType = reader.IsDBNull(6) ? "" : reader.GetString(6),
                WaitTimeMs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                WaitResource = reader.IsDBNull(8) ? "" : reader.GetString(8),
                CpuTimeMs = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                TotalElapsedTimeMs = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                Reads = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                Writes = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                LogicalReads = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                GrantedQueryMemoryGb = reader.IsDBNull(14) ? 0 : ToDouble(reader.GetValue(14)),
                TransactionIsolationLevel = reader.IsDBNull(15) ? "" : reader.GetString(15),
                Dop = reader.IsDBNull(16) ? 0 : reader.GetInt32(16),
                ParallelWorkerCount = reader.IsDBNull(17) ? 0 : reader.GetInt32(17),
                QueryPlan = reader.IsDBNull(18) ? null : reader.GetString(18),
                LiveQueryPlan = reader.IsDBNull(19) ? null : reader.GetString(19),
                CollectionTime = reader.IsDBNull(20) ? DateTime.MinValue : reader.GetDateTime(20),
                LoginName = reader.IsDBNull(21) ? "" : reader.GetString(21),
                HostName = reader.IsDBNull(22) ? "" : reader.GetString(22),
                ProgramName = reader.IsDBNull(23) ? "" : reader.GetString(23),
                OpenTransactionCount = reader.IsDBNull(24) ? 0 : reader.GetInt32(24),
                PercentComplete = reader.IsDBNull(25) ? 0m : Convert.ToDecimal(reader.GetValue(25)),
                QueryHash = reader.IsDBNull(26) ? "" : reader.GetString(26),
                BlockedSessionCount = reader.IsDBNull(27) ? 0 : Convert.ToInt32(reader.GetValue(27)),
                RequestedMemoryMb = reader.IsDBNull(28) ? 0 : ToDouble(reader.GetValue(28)),
                UsedMemoryMb = reader.IsDBNull(29) ? 0 : ToDouble(reader.GetValue(29)),
                MaxUsedMemoryMb = reader.IsDBNull(30) ? 0 : ToDouble(reader.GetValue(30)),
                TempdbCurrentMb = reader.IsDBNull(31) ? 0 : ToDouble(reader.GetValue(31)),
                TempdbAllocationsMb = reader.IsDBNull(32) ? 0 : ToDouble(reader.GetValue(32)),
                TranLogUsedMb = reader.IsDBNull(33) ? 0 : ToDouble(reader.GetValue(33)),
                TranStartTime = reader.IsDBNull(34) ? (DateTime?)null : reader.GetDateTime(34),
                RequestId = reader.IsDBNull(35) ? 0 : reader.GetInt32(35)
            });
        }

        return items;
    }

    /// <summary>How fresh the latest snapshot must be for its sessions to count as still running. Bound as
    /// <c>$4</c> rather than written into the SQL — see the bind site.</summary>
    private static readonly TimeSpan LatestSnapshotFreshness = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Gets long-running queries from the latest collection snapshot.
    /// Returns sessions whose total elapsed time exceeds the given threshold, and — since #3653 (A5, Q5) — how
    /// many over-threshold sessions the <paramref name="exclusions"/> opt-out knob removed from the same snapshot,
    /// split by the arm that removed them (program-name prefix / exact login).
    ///
    /// <para><b>The knob is applied IN the read, ahead of <c>LIMIT</c>.</b> The read is longest-elapsed first and
    /// capped at <paramref name="maxResults"/> (default 5); the sessions an operator excludes here are permanent
    /// background requests, i.e. the longest-running on the server by construction, so a post-read filter would
    /// let them fill the cap on every sweep and hide the real long-running query behind them. The <c>candidates</c>
    /// CTE flags each over-threshold row once per arm, the outer query keeps the unflagged ones under the cap, and
    /// the two excluded counts are uncorrelated scalars over the same CTE — one statement, one snapshot, so the
    /// counts and the rows describe the same instant. The counts are DISTINCT <c>session_id</c>s (one collection
    /// of one server, where the production read's (server_id, session_id, tran_start_time) identity collapses to
    /// <c>session_id</c>; a MARS session with two request rows is one session), and the login count is taken
    /// <c>AND NOT</c> the program flag so a session matching both arms counts once, under the prefix. An arm with
    /// no entries is the literal <c>FALSE</c>, and with both empty the rows are exactly what the pre-knob read
    /// returned.</para>
    ///
    /// <para><b>#3742: <paramref name="excludedDatabases"/> is the CTE's third flag, for the same reason.</b>
    /// Until #3742 the adapter above this read applied the database list in C#, AFTER <c>LIMIT</c> — exactly the
    /// shape the paragraph above refuses for the knob — so an excluded reporting database whose ETL held the five
    /// longest sessions consumed the page and the alert came back short or empty while un-excluded long-running
    /// sessions existed. The list now rides this statement as <c>excluded_by_database</c> (exact, case-insensitive,
    /// one <c>ILIKE</c> term per entry through the same builder, operands appended after the knob's so no ordinal
    /// the knob bound moves), the outer <c>WHERE</c> drops it with the knob's two, and its count is taken <c>AND
    /// NOT</c> both knob flags — the database arm is last, so a session the knob would have removed anyway is the
    /// knob's, and the three counts sum to the sessions removed. Null or empty spells the arm <c>FALSE</c>.</para>
    /// </summary>
    public async Task<LongRunningQueryReadResult> GetLongRunningQueriesAsync(
        int serverId,
        int thresholdMinutes,
        int maxResults = 5,
        bool excludeSpServerDiagnostics = true,
        bool excludeWaitFor = true,
        bool excludeBackups = true,
        bool excludeMiscWaits = true,
        bool excludeCdc = true,
        LongRunningQueryExclusions? exclusions = null,
        IReadOnlyList<string>? excludedDatabases = null)
    {
        exclusions ??= LongRunningQueryExclusions.None;

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var thresholdMs = (long)thresholdMinutes * 60 * 1000;

        // sp_server_diagnostics usually sits in SP_SERVER_DIAGNOSTICS_SLEEP, but it also does Extended Events work,
        // so it can be captured in a different wait (e.g. PREEMPTIVE_XE_GETTARGETSTATE) where the wait-type match
        // alone misses it and the alert fires; the query-text match (case-insensitive, NULL-safe) catches it
        // regardless of the wait it happens to be in at capture time.
        string spServerDiagnosticsFilter = excludeSpServerDiagnostics
            ? "AND r.wait_type NOT LIKE N'%SP_SERVER_DIAGNOSTICS%' AND (r.query_text IS NULL OR r.query_text NOT ILIKE N'%sp_server_diagnostics%')" : "";
        string waitForFilter = excludeWaitFor
            ? "AND r.wait_type NOT IN (N'WAITFOR', N'BROKER_RECEIVE_WAITFOR')" : "";
        string backupsFilter = excludeBackups
            ? "AND r.wait_type NOT IN (N'BACKUPTHREAD', N'BACKUPIO')" : "";
        string miscWaitsFilter = excludeMiscWaits
            ? "AND r.wait_type NOT IN (N'XE_LIVE_TARGET_TVF')" : "";
        // CDC capture sessions are flagged server-side by the collector (is_cdc_capture). COALESCE
        // guards pre-migration / archived rows where the column is NULL.
        string cdcFilter = excludeCdc
            ? "AND COALESCE(r.is_cdc_capture, FALSE) = FALSE" : "";
        maxResults = Math.Clamp(maxResults, 1, 1000);

        /* #3653 (A5, Q5): the opt-out knob's two arms, operands binding as $5 onward after the four fixed
           parameters below (program prefixes first, then logins); the shared builder spells the ILIKE … ESCAPE
           predicates so this read and Darling's cannot drift. #3742: the shared excludedDatabases list is the
           third arm, its operands appended after the logins. */
        var exclusionSql = exclusions.BuildSqlPredicates("r.program_name", "r.login_name", "r.database_name", excludedDatabases, firstParameterOrdinal: 5);

        command.CommandText = @$"
                WITH candidates AS (
                    SELECT
                        r.session_id,
                        r.database_name,
                        SUBSTRING(r.query_text, 1, 300) AS query_text,
                        r.total_elapsed_time_ms / 1000 AS elapsed_seconds,
                        r.cpu_time_ms,
                        r.reads,
                        r.writes,
                        r.wait_type,
                        r.blocking_session_id,
                        r.query_hash,
                        r.program_name,
                        r.login_name,
                        r.total_elapsed_time_ms,
                        {exclusionSql.ProgramPrefixPredicate} AS excluded_by_program_prefix,
                        {exclusionSql.LoginPredicate} AS excluded_by_login,
                        {exclusionSql.DatabasePredicate} AS excluded_by_database
                    FROM v_query_snapshots AS r
                    WHERE r.server_id = $1
                        AND r.collection_time = (SELECT MAX(vqs.collection_time) FROM v_query_snapshots AS vqs WHERE vqs.server_id = $1)
                        AND r.collection_time >= $4
                        AND r.session_id > 50
                        {spServerDiagnosticsFilter}
                        {waitForFilter}
                        {backupsFilter}
                        {miscWaitsFilter}
                        {cdcFilter}
                        AND r.total_elapsed_time_ms >= $2
                )
                SELECT
                    r.session_id,
                    r.database_name,
                    r.query_text,
                    r.elapsed_seconds,
                    r.cpu_time_ms,
                    r.reads,
                    r.writes,
                    r.wait_type,
                    r.blocking_session_id,
                    r.query_hash,
                    r.program_name,
                    r.login_name,
                    CAST((SELECT COUNT(DISTINCT x.session_id) FROM candidates AS x WHERE x.excluded_by_program_prefix) AS INTEGER) AS excluded_by_program_prefix_count,
                    CAST((SELECT COUNT(DISTINCT x.session_id) FROM candidates AS x WHERE x.excluded_by_login AND NOT x.excluded_by_program_prefix) AS INTEGER) AS excluded_by_login_count,
                    CAST((SELECT COUNT(DISTINCT x.session_id) FROM candidates AS x WHERE x.excluded_by_database AND NOT (x.excluded_by_program_prefix OR x.excluded_by_login)) AS INTEGER) AS excluded_by_database_count
                FROM candidates AS r
                WHERE NOT (r.excluded_by_program_prefix OR r.excluded_by_login OR r.excluded_by_database)
                ORDER BY r.total_elapsed_time_ms DESC
                LIMIT $3;";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = thresholdMs });
        command.Parameters.Add(new DuckDBParameter { Value = maxResults });
        /* $4 is the snapshot-freshness floor, bound rather than spelled `NOW() - INTERVAL '10 MINUTES'`:
           collection_time is a naive-UTC TIMESTAMP, NOW() is TIMESTAMP WITH TIME ZONE, and the mixed
           comparison resolves the naive side in the host's TimeZone. East of UTC the floor lands in the
           future and this read returns nothing, so the long-running-query alert never fires at all. */
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow - LatestSnapshotFreshness });
        foreach (var operand in exclusionSql.Operands)
        {
            command.Parameters.Add(new DuckDBParameter { Value = operand });
        }

        var items = new List<LongRunningQueryInfo>();
        int excludedByProgramPrefix = 0;
        int excludedByLogin = 0;
        int excludedByDatabase = 0;
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new LongRunningQueryInfo
            {
                SessionId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                QueryText = reader.IsDBNull(2) ? "" : reader.GetString(2),
                ElapsedSeconds = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                CpuTimeMs = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                Reads = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                Writes = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                WaitType = reader.IsDBNull(7) ? null : reader.GetString(7),
                BlockingSessionId = reader.IsDBNull(8) ? null : (int?)reader.GetInt32(8),
                QueryHash = reader.IsDBNull(9) ? null : reader.GetString(9),
                /* Phase-5 A reconciliation: populate ProgramName so the shared
                   BuildLongRunningQueryContext renders the ("Program", ...) detail item the
                   Dashboard's alert already had. */
                ProgramName = reader.IsDBNull(10) ? "" : reader.GetString(10),
                LoginName = reader.IsDBNull(11) ? "" : reader.GetString(11)
            });
            /* The same three scalars on every row — read once is enough, and a read with no rows has nothing to
               fire and therefore nothing to render the counts beside (LongRunningQueryReadResult says why). */
            excludedByProgramPrefix = reader.IsDBNull(12) ? 0 : reader.GetInt32(12);
            excludedByLogin = reader.IsDBNull(13) ? 0 : reader.GetInt32(13);
            excludedByDatabase = reader.IsDBNull(14) ? 0 : reader.GetInt32(14);
        }

        return new LongRunningQueryReadResult(items, excludedByProgramPrefix, excludedByLogin, excludedByDatabase);
    }
}

/* LongRunningQueryInfo and PoisonWaitDelta moved to PerformanceMonitor.Alerting (Phase-5 A0);
   the bare names resolve through the global using aliases in GlobalUsings.cs. */

public class WaitStatsRow
{
    public string WaitType { get; set; } = "";
    public long TotalWaitingTasks { get; set; }
    public long TotalWaitTimeMs { get; set; }
    public long TotalSignalWaitTimeMs { get; set; }
    public long ResourceWaitTimeMs => TotalWaitTimeMs - TotalSignalWaitTimeMs;
    public long SampleCount { get; set; }
    public double AvgWaitMsPerTask => TotalWaitingTasks > 0 ? (double)TotalWaitTimeMs / TotalWaitingTasks : 0;
    public string AvgWaitMsFormatted => AvgWaitMsPerTask < 0.1 ? "< 0.1 ms" : $"{AvgWaitMsPerTask:F1} ms";
    public string TotalWaitTimeFormatted => FormatMs(TotalWaitTimeMs);
    public string SignalWaitTimeFormatted => FormatMs(TotalSignalWaitTimeMs);
    public string ResourceWaitTimeFormatted => FormatMs(ResourceWaitTimeMs);
    public double SignalWaitPercent => TotalWaitTimeMs > 0 ? (double)TotalSignalWaitTimeMs / TotalWaitTimeMs * 100 : 0;

    private static string FormatMs(long ms)
    {
        if (ms < 1000) return $"{ms} ms";
        if (ms < 60000) return $"{ms / 1000.0:F1} sec";
        if (ms < 3600000) return $"{ms / 60000.0:F1} min";
        return $"{ms / 3600000.0:F1} hr";
    }
}

public class WaitStatsTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public double WaitTimeMsPerSecond { get; set; }
    public double SignalWaitTimeMsPerSecond { get; set; }
    public double AvgMsPerWait { get; set; }
}
