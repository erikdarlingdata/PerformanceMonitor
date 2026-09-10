/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets the latest Query Store snapshot for a server, aggregated across all databases.
    /// Shows top queries by total duration (execution_count * avg_duration).
    /// </summary>
    public async Task<List<TimeSliceBucket>> GetQueryStoreSlicerDataAsync(
        int serverId, int hoursBack, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = @"
WITH deduped AS
(
    -- LOAD-BEARING (correctness, not just perf) — #1841. query_store_stats rows are CUMULATIVE
    -- per-Query-Store-interval snapshots. The QueryStoreCollector is incremental and re-fetches the
    -- OPEN interval every cycle as its last_execution_time advances, so the SAME interval is stored
    -- repeatedly with a growing execution_count. Summing the raw rows counts one interval's work once per
    -- collection: a live store showed 496 collections of a single interval inside ONE hour bucket. Keep
    -- only the LATEST snapshot per interval, then aggregate.
    --
    -- runtime_stats_interval_id is the REAL interval identity (tier 2); first_execution_time stays beside
    -- it as the tier-1 PROXY. Both are in the key rather than a COALESCE of the two: the id is NULL on
    -- every row collected before tier 2 and non-NULL on every row after, so the two generations can never
    -- collide, and on a legacy-only window the id is a constant and the key degrades to exactly tier 1's.
    -- Where the id IS present it also fixes what the proxy could not — a row whose first_execution_time is
    -- NULL had no identity at all under tier 1 and collapsed with every other such interval of the same
    -- plan, which UNDER-counts. The id is per-DATABASE (each database has its own Query Store and its own
    -- interval sequence), which is why database_name must stay in the key.
    --
    -- The execution_count tie-break is the #1907 residual, and it is NOT decoration. collection_time alone
    -- was not a total order on rows collected before that fix: Query Store hands back the flushed and the
    -- still-in-memory slice of ONE interval as two ADDITIVE rows, the collector stored both, and they then
    -- shared every partition column above AND collection_time. ROW_NUMBER kept whichever the engine emitted
    -- first, so a grid could show an in-memory sliver of 8 where the interval's total was 94, and show a
    -- different one on the next run. The collector now combines the slices before storing, so rows collected
    -- after #1907 cannot tie at all and this clause never fires on them. It is here for the rows already in
    -- the store, which cannot be rewritten: it deterministically picks the FLUSHED slice, the one holding the
    -- bulk of the interval's work, instead of flapping. Closest-available, not correct — the correct value is
    -- the SUM of the slices, which no read-side rule can express (#1912). This applies to EVERY dedup site in
    -- both apps: Lite.Tests/QueryStoreSliceTieBreakSourceTests pins all 7 of Lite's across the 3 files that
    -- hold them, and Darling.Tests/QueryStoreSliceTieBreakSourceTests pins its 12, so dropping one fails
    -- loudly and names the file. Both enumerate their files rather than globbing, so MOVING a read has to be
    -- re-declared instead of silently shrinking the guard.
    SELECT
        collection_time,
        interval_start_time_utc,
        query_id,
        execution_count,
        avg_cpu_time_us,
        avg_duration_us,
        avg_logical_io_reads,
        avg_logical_io_writes,
        avg_physical_io_reads,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    -- The window is filtered on the SAME expression the bars are keyed on (#1892). It used to filter on
    -- collection_time while bucketing on the interval start, and once those stopped being the same instant
    -- (#1841 tier 2) the two disagreed at both edges: an interval that started before the window but closed
    -- inside it drew an extra bar to the LEFT of the requested range, and the window's own last interval --
    -- whose closing fetch lands after the range ends -- was dropped entirely, which is the collection lag
    -- #1841 set out to remove showing up one layer down.
    AND   COALESCE(interval_start_time_utc, collection_time) >= $2
    AND   COALESCE(interval_start_time_utc, collection_time) <= $3
    -- Pruning bounds, NOT filters: neither can exclude a row the predicate above keeps in any realistic
    -- store. They earn their place because v_query_store_stats unions the live table with the parquet
    -- archive, and bounds on collection_time are what let the reader skip archive row groups instead of
    -- scanning the whole glob -- without them an old fixed date range reads every file from the window
    -- through the present.
    --
    -- The FLOOR is free: an interval is always collected after it starts, so
    -- interval_start_time_utc <= collection_time, and therefore COALESCE(...) >= $2 already implies
    -- collection_time >= $2. The extra day is slack against clock skew between the monitored server's
    -- interval clock and ours.
    --
    -- The CEILING is deliberately enormous rather than tight, because tight is unsafe here. A row's
    -- collection_time exceeds its interval start by the interval's own length -- at most 1 day, since
    -- Query Store's INTERVAL_LENGTH_MINUTES accepts only 1/5/10/15/30/60/1440 -- plus however long the
    -- collector was behind, which nothing bounds. So 30 days is 1 day of engine maximum and 29 of
    -- collector-outage allowance. A month-long outage that then back-collects an interval straddling an
    -- old window's edge could still omit that one bar; the data stays in the store, and the alternative
    -- (no ceiling) makes every historical window scan to the present.
    AND   collection_time >= $2 - INTERVAL '1 day'
    AND   collection_time <= $3 + INTERVAL '30 days'" + dbClause + @"
)
SELECT
    -- The bar sits in the hour the work RAN, not the hour it was last COLLECTED (#1841 tier 2). Query
    -- Store's default interval is 60 minutes and the closing fetch lands in the cycle AFTER the interval
    -- ends, so a collection_time bucket was reliably one bar late. interval_start_time_utc is the
    -- interval's own start boundary, converted to UTC at collection (the same clock as collection_time),
    -- so this is a placement fix and not a timezone trade. Legacy rows have no interval start and keep
    -- collection_time placement, so a window spanning the upgrade renders correctly on both sides and the
    -- lag simply disappears as the pre-upgrade rows age out.
    date_trunc('hour', COALESCE(interval_start_time_utc, collection_time)) AS bucket,
    COUNT(DISTINCT query_id) AS query_count,
    COALESCE(SUM(CAST(avg_cpu_time_us AS DOUBLE PRECISION) * execution_count), 0) / 1000.0 AS total_cpu_ms,
    COALESCE(SUM(CAST(avg_duration_us AS DOUBLE PRECISION) * execution_count), 0) / 1000.0 AS total_duration_ms,
    COALESCE(SUM(CAST(avg_logical_io_reads AS DOUBLE PRECISION) * execution_count), 0) AS total_reads,
    COALESCE(SUM(CAST(avg_logical_io_writes AS DOUBLE PRECISION) * execution_count), 0) AS total_writes,
    COALESCE(SUM(CAST(avg_physical_io_reads AS DOUBLE PRECISION) * execution_count), 0) AS total_physical_reads
FROM deduped
WHERE rn = 1
GROUP BY date_trunc('hour', COALESCE(interval_start_time_utc, collection_time))
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<TimeSliceBucket>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TimeSliceBucket
            {
                BucketTime = reader.GetDateTime(0),
                SessionCount = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                TotalCpu = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                TotalElapsed = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                TotalReads = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                TotalWrites = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                TotalLogicalReads = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                Value = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
            });
        }
        return items;
    }

    public async Task<List<QueryStoreRow>> GetQueryStoreTopQueriesAsync(int serverId, int hoursBack = 24, int top = 50, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetQueryStoreTopQueriesAsync", "v_query_store_stats top N");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 5, out var dbValues);

        command.CommandText = @"
WITH deduped AS (
    /* LOAD-BEARING (correctness, not just perf) — #1841. query_store_stats rows are CUMULATIVE
       per-Query-Store-interval snapshots, and the collector re-fetches the OPEN interval every cycle
       as its last_execution_time advances, so the SAME interval is stored repeatedly with a growing
       execution_count. SUM(execution_count) over the raw rows reports 10 + 25 + 40 for an interval that
       reached 40, and AVG(avg_*) becomes an avg-of-avgs weighted by how many times each interval happened
       to be re-collected. Keep the LATEST snapshot per interval. SELECT * because the aggregate below
       projects nearly every payload column.

       runtime_stats_interval_id is the REAL interval identity (tier 2), with first_execution_time kept
       beside it as the tier-1 proxy for rows collected before it existed — see the slicer above for why
       both are in the key rather than a COALESCE of the two.

       replica_role and execution_type_desc are in the partition because the aggregate below is grouped
       (or MAXed) on them: the dedup key must be at least as fine as the read's own row identity, or
       dedup would silently drop a row the grid is supposed to show rather than de-duplicate one. */
    SELECT
        *,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3" + dbClause + @"
),
ranked AS (
    SELECT
        database_name,
        query_id,
        plan_id,
        query_hash,
        /* A GROUP BY key, not MAX(): with Query Store for secondary replicas (2022+) the primary holds
           ONE shared Query Store carrying every replica's rows, so grouping without it would average
           primary and secondary workload into a single blended row — the conflation this column exists
           to expose. Grouping splits them into one row per replica role instead. On a standalone/non-AG
           server every row shares one value (NULL, or 'Primary' on 2025), so the grouping is a no-op
           and the grid is unchanged. */
        replica_role,
        MAX(module_name) AS module_name,
        SUM(execution_count) AS total_executions,
        AVG(CAST(avg_duration_us AS DOUBLE PRECISION)) / 1000.0 AS avg_duration_ms,
        AVG(CAST(avg_cpu_time_us AS DOUBLE PRECISION)) / 1000.0 AS avg_cpu_time_ms,
        AVG(CAST(avg_logical_io_reads AS DOUBLE PRECISION)) AS avg_logical_reads,
        AVG(CAST(avg_logical_io_writes AS DOUBLE PRECISION)) AS avg_logical_writes,
        AVG(CAST(avg_physical_io_reads AS DOUBLE PRECISION)) AS avg_physical_reads,
        AVG(CAST(avg_rowcount AS DOUBLE PRECISION)) AS avg_rowcount,
        MIN(min_dop) AS min_dop,
        MAX(max_dop) AS max_dop,
        MAX(last_execution_time) AS last_execution_time,
        MAX(query_plan_hash) AS query_plan_hash,
        MAX(CASE WHEN is_forced_plan THEN TRUE ELSE FALSE END) AS is_forced_plan,
        MAX(plan_forcing_type) AS plan_forcing_type,
        MAX(execution_type_desc) AS execution_type_desc,
        MIN(first_execution_time) AS first_execution_time,
        AVG(CAST(avg_clr_time_us AS DOUBLE PRECISION)) / 1000.0 AS avg_clr_time_ms,
        AVG(CAST(avg_tempdb_space_used AS DOUBLE PRECISION)) AS avg_tempdb_space_used,
        AVG(CAST(avg_log_bytes_used AS DOUBLE PRECISION)) AS avg_log_bytes_used,
        MAX(plan_type) AS plan_type,
        MAX(force_failure_count) AS force_failure_count,
        MAX(last_force_failure_reason) AS last_force_failure_reason,
        MAX(compatibility_level) AS compatibility_level,
        MIN(CAST(min_duration_us AS DOUBLE PRECISION)) / 1000.0 AS min_duration_ms,
        MAX(CAST(max_duration_us AS DOUBLE PRECISION)) / 1000.0 AS max_duration_ms,
        MIN(CAST(min_cpu_time_us AS DOUBLE PRECISION)) / 1000.0 AS min_cpu_time_ms,
        MAX(CAST(max_cpu_time_us AS DOUBLE PRECISION)) / 1000.0 AS max_cpu_time_ms,
        MIN(CAST(min_logical_io_reads AS DOUBLE PRECISION)) AS min_logical_reads,
        MAX(CAST(max_logical_io_reads AS DOUBLE PRECISION)) AS max_logical_reads,
        MIN(CAST(min_logical_io_writes AS DOUBLE PRECISION)) AS min_logical_writes,
        MAX(CAST(max_logical_io_writes AS DOUBLE PRECISION)) AS max_logical_writes,
        MIN(CAST(min_physical_io_reads AS DOUBLE PRECISION)) AS min_physical_reads,
        MAX(CAST(max_physical_io_reads AS DOUBLE PRECISION)) AS max_physical_reads,
        MIN(CAST(min_clr_time_us AS DOUBLE PRECISION)) / 1000.0 AS min_clr_time_ms,
        MAX(CAST(max_clr_time_us AS DOUBLE PRECISION)) / 1000.0 AS max_clr_time_ms,
        MIN(CAST(min_rowcount AS DOUBLE PRECISION)) AS min_rowcount,
        MAX(CAST(max_rowcount AS DOUBLE PRECISION)) AS max_rowcount,
        MIN(CAST(min_log_bytes_used AS DOUBLE PRECISION)) AS min_log_bytes_used,
        MAX(CAST(max_log_bytes_used AS DOUBLE PRECISION)) AS max_log_bytes_used,
        MIN(CAST(min_tempdb_space_used AS DOUBLE PRECISION)) AS min_tempdb_space_used,
        MAX(CAST(max_tempdb_space_used AS DOUBLE PRECISION)) AS max_tempdb_space_used,
        AVG(CAST(avg_query_max_used_memory AS DOUBLE PRECISION)) * 8.0 / 1024.0 AS avg_memory_mb,
        MIN(CAST(min_query_max_used_memory AS DOUBLE PRECISION)) * 8.0 / 1024.0 AS min_memory_mb,
        MAX(CAST(max_query_max_used_memory AS DOUBLE PRECISION)) * 8.0 / 1024.0 AS max_memory_mb,
        AVG(CAST(avg_num_physical_io_reads AS DOUBLE PRECISION)) AS avg_num_physical_io_reads,
        MIN(CAST(min_num_physical_io_reads AS DOUBLE PRECISION)) AS min_num_physical_io_reads,
        MAX(CAST(max_num_physical_io_reads AS DOUBLE PRECISION)) AS max_num_physical_io_reads
    FROM deduped
    WHERE rn = 1
    GROUP BY database_name, query_id, plan_id, query_hash, replica_role
    ORDER BY SUM(execution_count) * AVG(CAST(avg_duration_us AS DOUBLE PRECISION)) DESC
    LIMIT $4 + 5
)
SELECT
    r.database_name,
    r.query_id,
    r.plan_id,
    r.query_hash,
    t.query_text,
    r.module_name,
    r.total_executions,
    r.avg_duration_ms,
    r.avg_cpu_time_ms,
    r.avg_logical_reads,
    r.avg_logical_writes,
    r.avg_physical_reads,
    r.avg_rowcount,
    r.min_dop,
    r.max_dop,
    r.last_execution_time,
    r.query_plan_hash,
    r.is_forced_plan,
    r.plan_forcing_type,
    NULL AS query_plan_text,
    r.execution_type_desc,
    r.first_execution_time,
    r.avg_clr_time_ms,
    r.avg_tempdb_space_used,
    r.avg_log_bytes_used,
    r.plan_type,
    r.force_failure_count,
    r.last_force_failure_reason,
    r.compatibility_level,
    r.min_duration_ms,
    r.max_duration_ms,
    r.min_cpu_time_ms,
    r.max_cpu_time_ms,
    r.min_logical_reads,
    r.max_logical_reads,
    r.min_logical_writes,
    r.max_logical_writes,
    r.min_physical_reads,
    r.max_physical_reads,
    r.min_clr_time_ms,
    r.max_clr_time_ms,
    r.min_rowcount,
    r.max_rowcount,
    r.min_log_bytes_used,
    r.max_log_bytes_used,
    r.min_tempdb_space_used,
    r.max_tempdb_space_used,
    r.avg_memory_mb,
    r.min_memory_mb,
    r.max_memory_mb,
    r.avg_num_physical_io_reads,
    r.min_num_physical_io_reads,
    r.max_num_physical_io_reads,
    r.replica_role
FROM ranked r
LEFT JOIN LATERAL (
    SELECT query_text
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   query_id = r.query_id
    AND   database_name = r.database_name
    AND   query_text IS NOT NULL
    ORDER BY collection_time DESC
    LIMIT 1
) t ON TRUE
WHERE t.query_text IS NULL OR t.query_text NOT LIKE 'WAITFOR%'
ORDER BY r.total_executions * r.avg_duration_ms DESC
LIMIT $4";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = top });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<QueryStoreRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QueryStoreRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                QueryId = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                PlanId = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                QueryHash = reader.IsDBNull(3) ? "" : reader.GetString(3),
                QueryText = reader.IsDBNull(4) ? "" : reader.GetString(4),
                ModuleName = reader.IsDBNull(5) ? "" : reader.GetString(5),
                TotalExecutions = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                AvgDurationMs = reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7)),
                AvgCpuTimeMs = reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8)),
                AvgLogicalReads = reader.IsDBNull(9) ? 0 : ToDouble(reader.GetValue(9)),
                AvgLogicalWrites = reader.IsDBNull(10) ? 0 : ToDouble(reader.GetValue(10)),
                AvgPhysicalReads = reader.IsDBNull(11) ? 0 : ToDouble(reader.GetValue(11)),
                AvgRowcount = reader.IsDBNull(12) ? 0 : ToDouble(reader.GetValue(12)),
                MinDop = reader.IsDBNull(13) ? 0 : Convert.ToInt64(reader.GetValue(13)),
                MaxDop = reader.IsDBNull(14) ? 0 : Convert.ToInt64(reader.GetValue(14)),
                LastExecutionTime = reader.IsDBNull(15) ? (DateTime?)null : reader.GetDateTime(15),
                QueryPlanHash = reader.IsDBNull(16) ? "" : reader.GetString(16),
                IsForcedPlan = !reader.IsDBNull(17) && reader.GetBoolean(17),
                PlanForcingType = reader.IsDBNull(18) ? "" : reader.GetString(18),
                QueryPlanText = reader.IsDBNull(19) ? null : reader.GetString(19),
                ExecutionTypeDesc = reader.IsDBNull(20) ? "" : reader.GetString(20),
                FirstExecutionTime = reader.IsDBNull(21) ? (DateTime?)null : reader.GetDateTime(21),
                AvgClrTimeMs = reader.IsDBNull(22) ? 0 : ToDouble(reader.GetValue(22)),
                AvgTempdbSpaceUsed = reader.IsDBNull(23) ? 0 : ToDouble(reader.GetValue(23)),
                AvgLogBytesUsed = reader.IsDBNull(24) ? 0 : ToDouble(reader.GetValue(24)),
                PlanType = reader.IsDBNull(25) ? "" : reader.GetString(25),
                ForceFailureCount = reader.IsDBNull(26) ? 0 : Convert.ToInt64(reader.GetValue(26)),
                LastForceFailureReason = reader.IsDBNull(27) ? "" : reader.GetString(27),
                CompatibilityLevel = reader.IsDBNull(28) ? 0 : Convert.ToInt32(reader.GetValue(28)),
                MinDurationMs = reader.IsDBNull(29) ? 0 : ToDouble(reader.GetValue(29)),
                MaxDurationMs = reader.IsDBNull(30) ? 0 : ToDouble(reader.GetValue(30)),
                MinCpuTimeMs = reader.IsDBNull(31) ? 0 : ToDouble(reader.GetValue(31)),
                MaxCpuTimeMs = reader.IsDBNull(32) ? 0 : ToDouble(reader.GetValue(32)),
                MinLogicalReads = reader.IsDBNull(33) ? 0 : ToDouble(reader.GetValue(33)),
                MaxLogicalReads = reader.IsDBNull(34) ? 0 : ToDouble(reader.GetValue(34)),
                MinLogicalWrites = reader.IsDBNull(35) ? 0 : ToDouble(reader.GetValue(35)),
                MaxLogicalWrites = reader.IsDBNull(36) ? 0 : ToDouble(reader.GetValue(36)),
                MinPhysicalReads = reader.IsDBNull(37) ? 0 : ToDouble(reader.GetValue(37)),
                MaxPhysicalReads = reader.IsDBNull(38) ? 0 : ToDouble(reader.GetValue(38)),
                MinClrTimeMs = reader.IsDBNull(39) ? 0 : ToDouble(reader.GetValue(39)),
                MaxClrTimeMs = reader.IsDBNull(40) ? 0 : ToDouble(reader.GetValue(40)),
                MinRowcount = reader.IsDBNull(41) ? 0 : ToDouble(reader.GetValue(41)),
                MaxRowcount = reader.IsDBNull(42) ? 0 : ToDouble(reader.GetValue(42)),
                MinLogBytesUsed = reader.IsDBNull(43) ? 0 : ToDouble(reader.GetValue(43)),
                MaxLogBytesUsed = reader.IsDBNull(44) ? 0 : ToDouble(reader.GetValue(44)),
                MinTempdbSpaceUsed = reader.IsDBNull(45) ? 0 : ToDouble(reader.GetValue(45)),
                MaxTempdbSpaceUsed = reader.IsDBNull(46) ? 0 : ToDouble(reader.GetValue(46)),
                AvgMemoryMb = reader.IsDBNull(47) ? 0 : ToDouble(reader.GetValue(47)),
                MinMemoryMb = reader.IsDBNull(48) ? 0 : ToDouble(reader.GetValue(48)),
                MaxMemoryMb = reader.IsDBNull(49) ? 0 : ToDouble(reader.GetValue(49)),
                AvgNumPhysicalIoReads = reader.IsDBNull(50) ? 0 : ToDouble(reader.GetValue(50)),
                MinNumPhysicalIoReads = reader.IsDBNull(51) ? 0 : ToDouble(reader.GetValue(51)),
                MaxNumPhysicalIoReads = reader.IsDBNull(52) ? 0 : ToDouble(reader.GetValue(52)),
                ReplicaRole = reader.IsDBNull(53) ? null : reader.GetString(53)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets query store comparison between a current time range and a baseline range.
    /// Uses weighted averages (execution_count * avg_metric) for accurate aggregation.
    /// </summary>
    public async Task<List<QueryStatsComparisonItem>> GetQueryStoreComparisonAsync(
        int serverId,
        DateTime currentStart, DateTime currentEnd,
        DateTime baselineStart, DateTime baselineEnd,
        IReadOnlyList<string>? databaseNames = null)
    {
        using var _q = TimeQuery("GetQueryStoreComparisonAsync", "v_query_store_stats comparison");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var dbClause = BuildDbInClause(databaseNames, "database_name", 6, out var dbValues);

        command.CommandText = @"
WITH deduped_current AS (
    /* LOAD-BEARING (correctness, not just perf) — #1841. query_store_stats rows are CUMULATIVE
       per-Query-Store-interval snapshots and the collector re-fetches the OPEN interval every cycle,
       so the SAME interval (same first_execution_time) is stored repeatedly with a growing
       execution_count. Keep the LATEST snapshot per interval before aggregating.

       The execution-count weighting below does NOT rescue this on its own: the repeated snapshots of
       one interval carry DIFFERENT (growing) weights AND different avg_* values, so an open interval
       is weighted by the triangular sum of its own growth. That bias is stronger in the recent window
       than in the baseline window (recent windows hold more still-open intervals), which skews the
       delta this comparison exists to compute. One deduped CTE per window, so both arms are treated
       identically. */
    SELECT
        database_name,
        query_hash,
        query_text,
        execution_count,
        avg_duration_us,
        avg_cpu_time_us,
        avg_logical_io_reads,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   collection_time >= $2 AND collection_time <= $3" + dbClause + @"
),
deduped_baseline AS (
    SELECT
        database_name,
        query_hash,
        query_text,
        execution_count,
        avg_duration_us,
        avg_cpu_time_us,
        avg_logical_io_reads,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   collection_time >= $4 AND collection_time <= $5" + dbClause + @"
),
top_current AS (
    SELECT database_name, query_hash
    FROM deduped_current
    WHERE rn = 1
    AND   execution_count > 0
    GROUP BY database_name, query_hash
    ORDER BY SUM(execution_count) DESC
    LIMIT 100
),
top_baseline AS (
    SELECT database_name, query_hash
    FROM deduped_baseline
    WHERE rn = 1
    AND   execution_count > 0
    GROUP BY database_name, query_hash
    ORDER BY SUM(execution_count) DESC
    LIMIT 100
),
top_hashes AS (
    SELECT DISTINCT database_name, query_hash
    FROM (
        SELECT * FROM top_current
        UNION ALL
        SELECT * FROM top_baseline
    ) combined
),
current_period AS (
    SELECT th.database_name, th.query_hash,
           SUM(qs.execution_count) AS exec_count,
           SUM(qs.execution_count * qs.avg_duration_us::DOUBLE PRECISION) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_duration_ms,
           SUM(qs.execution_count * qs.avg_cpu_time_us::DOUBLE PRECISION) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_cpu_ms,
           SUM(qs.execution_count * qs.avg_logical_io_reads::DOUBLE PRECISION) / NULLIF(SUM(qs.execution_count), 0) AS avg_reads,
           MAX(qs.query_text) AS query_text
    FROM top_hashes th
    INNER JOIN deduped_current qs
      ON  qs.query_hash IS NOT DISTINCT FROM th.query_hash
      AND qs.database_name IS NOT DISTINCT FROM th.database_name
    WHERE qs.rn = 1
    AND   qs.execution_count > 0
    GROUP BY th.database_name, th.query_hash
),
baseline_period AS (
    SELECT th.database_name, th.query_hash,
           SUM(qs.execution_count) AS exec_count,
           SUM(qs.execution_count * qs.avg_duration_us::DOUBLE PRECISION) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_duration_ms,
           SUM(qs.execution_count * qs.avg_cpu_time_us::DOUBLE PRECISION) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_cpu_ms,
           SUM(qs.execution_count * qs.avg_logical_io_reads::DOUBLE PRECISION) / NULLIF(SUM(qs.execution_count), 0) AS avg_reads,
           MAX(qs.query_text) AS query_text
    FROM top_hashes th
    INNER JOIN deduped_baseline qs
      ON  qs.query_hash IS NOT DISTINCT FROM th.query_hash
      AND qs.database_name IS NOT DISTINCT FROM th.database_name
    WHERE qs.rn = 1
    AND   qs.execution_count > 0
    GROUP BY th.database_name, th.query_hash
)
SELECT COALESCE(c.database_name, b.database_name) AS database_name,
       COALESCE(c.query_hash, b.query_hash) AS query_hash,
       COALESCE(c.query_text, b.query_text) AS query_text,
       c.exec_count, c.avg_duration_ms, c.avg_cpu_ms, c.avg_reads,
       b.exec_count AS baseline_exec_count,
       b.avg_duration_ms AS baseline_avg_duration_ms,
       b.avg_cpu_ms AS baseline_avg_cpu_ms,
       b.avg_reads AS baseline_avg_reads
FROM current_period c
FULL OUTER JOIN baseline_period b
  ON  c.database_name IS NOT DISTINCT FROM b.database_name
  AND c.query_hash IS NOT DISTINCT FROM b.query_hash;";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = currentStart });
        command.Parameters.Add(new DuckDBParameter { Value = currentEnd });
        command.Parameters.Add(new DuckDBParameter { Value = baselineStart });
        command.Parameters.Add(new DuckDBParameter { Value = baselineEnd });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<QueryStatsComparisonItem>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QueryStatsComparisonItem
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                QueryHash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                QueryText = reader.IsDBNull(2) ? "" : reader.GetString(2),
                ExecutionCount = reader.IsDBNull(3) ? 0 : ToInt64(reader.GetValue(3)),
                AvgDurationMs = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                AvgCpuMs = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                AvgReads = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                BaselineExecutionCount = reader.IsDBNull(7) ? 0 : ToInt64(reader.GetValue(7)),
                BaselineAvgDurationMs = reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8)),
                BaselineAvgCpuMs = reader.IsDBNull(9) ? 0 : ToDouble(reader.GetValue(9)),
                BaselineAvgReads = reader.IsDBNull(10) ? 0 : ToDouble(reader.GetValue(10)),
            });
        }

        return items;
    }

    /// <summary>
    /// One point on the Query Store slicer overlay: the interval's per-interval totals, placed at the hour
    /// the work RAN.
    /// </summary>
    public sealed record QueryStoreItemTimelinePoint(DateTime PointTime, double CpuMs, double ElapsedMs, double Reads);

    /// <summary>
    /// The selected Query Store row's execution timeline, for the grid→slicer overlay (#683).
    ///
    /// <para><b>Why this exists rather than the overlay reusing <see cref="GetQueryStoreHistoryAsync"/>, which
    /// is what it did before #1921.</b> That read feeds the history GRID, a deliberately raw per-collection
    /// projection (#1845) whose window is keyed on <c>collection_time</c> — which is correct for it, because a
    /// list of collection events is exactly what it shows. The overlay needs the opposite: points placed and
    /// windowed on the interval start, matching the bars it is drawn over. Moving the shared read would have
    /// silently changed the history grid's window as well, so the two surfaces get the two reads their
    /// different questions need. Darling has had this split since #683 (<c>ItemTimeline</c>); this converges
    /// Lite onto the same structure rather than inventing a third shape.</para>
    ///
    /// <para>It also picks up the DEDUP the overlay never had. Darling's equivalent dedups per interval
    /// because the rows are cumulative snapshots and an un-deduped overlay draws one interval as a rising
    /// staircase of restatements; Lite's overlay was plotting every history row. Both halves of the invariant
    /// — dedup and placement — are what make the overlay agree with the bars, which is the whole point of the
    /// series.</para>
    /// </summary>
    public async Task<List<QueryStoreItemTimelinePoint>> GetQueryStoreItemTimelineAsync(
        int serverId, string databaseName, long queryId, long planId,
        int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
WITH deduped AS
(
    -- LOAD-BEARING (correctness, not just perf) — #1841. query_store_stats rows are CUMULATIVE per-interval
    -- snapshots and the collector re-fetches the OPEN interval every cycle, so an un-deduped projection draws
    -- one interval as a rising staircase of restatements rather than new work. The overlay is drawn OVER the
    -- deduped slicer bars, so leaving it raw makes it disagree with the bars it annotates.
    --
    -- The execution_count tie-break is the #1907 residual, same as every other dedup site: rows collected
    -- before that fix can share this whole partition AND collection_time, and it resolves them to the flushed
    -- slice deterministically instead of flapping.
    SELECT
        -- Placed where the work RAN (#1921). #1841 moved the bars to the interval start and left this series
        -- on collection_time, so a point sat up to one Query Store interval — 60 minutes by default, 1440 at
        -- most — to the RIGHT of the bar describing the very same work. COALESCE, not a bare column: rows
        -- collected before tier 2 have no interval start and keep collection_time placement.
        COALESCE(interval_start_time_utc, collection_time) AS point_time,
        execution_count,
        avg_cpu_time_us,
        avg_duration_us,
        avg_logical_io_reads,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   database_name = $2
    AND   query_id = $3
    AND   plan_id = $4
    -- Windowed on the SAME expression the points are placed on, and the same one the bars use. Filtering on
    -- collection_time while placing on the interval start disagrees at both edges, and disagrees with the
    -- bars about whether an interval is in the window at all.
    AND   COALESCE(interval_start_time_utc, collection_time) >= $5
    AND   COALESCE(interval_start_time_utc, collection_time) <= $6
    -- Pruning bounds, NOT filters: v_query_store_stats unions the live table with the parquet archive, and
    -- bounds on collection_time are what let the reader skip archive row groups instead of scanning the whole
    -- glob. Same shipped shape and reasoning as the slicer's (#1892/#1923) — the floor is free because an
    -- interval is always collected after it starts, and the ceiling is deliberately enormous because a row's
    -- collection_time exceeds its interval start by the interval's length (at most 1 day) plus however far
    -- behind the collector was, which nothing bounds.
    AND   collection_time >= $5 - INTERVAL '1 day'
    AND   collection_time <= $6 + INTERVAL '30 days'
)
SELECT
    point_time,
    COALESCE(CAST(avg_cpu_time_us AS DOUBLE PRECISION) * execution_count, 0) / 1000.0 AS cpu_ms,
    COALESCE(CAST(avg_duration_us AS DOUBLE PRECISION) * execution_count, 0) / 1000.0 AS elapsed_ms,
    COALESCE(CAST(avg_logical_io_reads AS DOUBLE PRECISION) * execution_count, 0) AS reads
FROM deduped
WHERE rn = 1
-- Ordered on the axis the points are PLOTTED on: a series whose x-values are not monotonic draws as a
-- zig-zag rather than a curve.
ORDER BY point_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        command.Parameters.Add(new DuckDBParameter { Value = queryId });
        command.Parameters.Add(new DuckDBParameter { Value = planId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var points = new List<QueryStoreItemTimelinePoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            points.Add(new QueryStoreItemTimelinePoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3))));
        }
        return points;
    }

    /// <summary>
    /// Gets collection-level history for ALL plans of a Query Store query (query-scoped, matching
    /// the Dashboard drilldown) so plan switches and regressions are visible over time.
    ///
    /// <para>Deliberately NOT deduped per interval (#1841), unlike every aggregate read in this file:
    /// this is the "show me every snapshot" surface — a raw per-collection projection with no SUM, AVG
    /// or COUNT, so nothing is multiply-counted. What it does show is the cumulative restatement itself
    /// (one interval re-collected N times appears as N rows with a growing execution_count), and
    /// first_execution_time is already projected so a reader can tell those rows apart. Tier 2 stored the
    /// real interval identity, which would now make collapsing them trivial — and the exclusion STANDS
    /// anyway, because whether this drilldown should show every snapshot or one row per interval is a
    /// product call, not an arithmetic one, and the arithmetic was never wrong here.</para>
    /// </summary>
    public async Task<List<QueryStoreHistoryRow>> GetQueryStoreHistoryAsync(int serverId, string databaseName, long queryId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);
        command.CommandText = @"
SELECT
    collection_time,
    execution_count,
    avg_duration_us / 1000.0 AS avg_duration_ms,
    avg_cpu_time_us / 1000.0 AS avg_cpu_time_ms,
    CAST(avg_logical_io_reads AS DOUBLE PRECISION) AS avg_logical_reads,
    CAST(avg_logical_io_writes AS DOUBLE PRECISION) AS avg_logical_writes,
    CAST(avg_physical_io_reads AS DOUBLE PRECISION) AS avg_physical_reads,
    CAST(avg_rowcount AS DOUBLE PRECISION) AS avg_rowcount,
    last_execution_time,
    min_dop,
    max_dop,
    execution_type_desc,
    first_execution_time,
    module_name,
    CAST(avg_num_physical_io_reads AS DOUBLE PRECISION) AS avg_num_physical_reads,
    CAST(min_num_physical_io_reads AS DOUBLE PRECISION) AS min_num_physical_reads,
    CAST(max_num_physical_io_reads AS DOUBLE PRECISION) AS max_num_physical_reads,
    avg_clr_time_us / 1000.0 AS avg_clr_time_ms,
    min_clr_time_us / 1000.0 AS min_clr_time_ms,
    max_clr_time_us / 1000.0 AS max_clr_time_ms,
    CAST(avg_log_bytes_used AS DOUBLE PRECISION) / 1048576.0 AS avg_log_mb,
    CAST(min_log_bytes_used AS DOUBLE PRECISION) / 1048576.0 AS min_log_mb,
    CAST(max_log_bytes_used AS DOUBLE PRECISION) / 1048576.0 AS max_log_mb,
    plan_id,
    min_duration_us / 1000.0 AS min_duration_ms,
    max_duration_us / 1000.0 AS max_duration_ms,
    min_cpu_time_us / 1000.0 AS min_cpu_time_ms,
    max_cpu_time_us / 1000.0 AS max_cpu_time_ms,
    CAST(min_logical_io_reads AS DOUBLE PRECISION) AS min_logical_reads,
    CAST(max_logical_io_reads AS DOUBLE PRECISION) AS max_logical_reads,
    CAST(min_logical_io_writes AS DOUBLE PRECISION) AS min_logical_writes,
    CAST(max_logical_io_writes AS DOUBLE PRECISION) AS max_logical_writes,
    CAST(min_physical_io_reads AS DOUBLE PRECISION) AS min_physical_reads,
    CAST(max_physical_io_reads AS DOUBLE PRECISION) AS max_physical_reads,
    CAST(min_rowcount AS DOUBLE PRECISION) AS min_rowcount,
    CAST(max_rowcount AS DOUBLE PRECISION) AS max_rowcount,
    CAST(avg_query_max_used_memory AS DOUBLE PRECISION) * 8.0 / 1024.0 AS avg_memory_mb,
    CAST(min_query_max_used_memory AS DOUBLE PRECISION) * 8.0 / 1024.0 AS min_memory_mb,
    CAST(max_query_max_used_memory AS DOUBLE PRECISION) * 8.0 / 1024.0 AS max_memory_mb,
    CAST(avg_tempdb_space_used AS DOUBLE PRECISION) * 8.0 / 1024.0 AS avg_tempdb_mb,
    CAST(min_tempdb_space_used AS DOUBLE PRECISION) * 8.0 / 1024.0 AS min_tempdb_mb,
    CAST(max_tempdb_space_used AS DOUBLE PRECISION) * 8.0 / 1024.0 AS max_tempdb_mb,
    plan_type,
    is_forced_plan,
    force_failure_count,
    last_force_failure_reason,
    plan_forcing_type,
    compatibility_level,
    query_hash,
    query_plan_hash
FROM v_query_store_stats
WHERE server_id = $1
AND   database_name = $2
AND   query_id = $3
AND   collection_time >= $4
AND   collection_time <= $5
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        command.Parameters.Add(new DuckDBParameter { Value = queryId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<QueryStoreHistoryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QueryStoreHistoryRow
            {
                CollectionTime = reader.GetDateTime(0),
                ExecutionCount = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                AvgDurationMs = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                AvgCpuTimeMs = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                AvgLogicalReads = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                AvgLogicalWrites = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                AvgPhysicalReads = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                AvgRowcount = reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7)),
                LastExecutionTime = reader.IsDBNull(8) ? (DateTime?)null : reader.GetDateTime(8),
                MinDop = reader.IsDBNull(9) ? 0 : Convert.ToInt64(reader.GetValue(9)),
                MaxDop = reader.IsDBNull(10) ? 0 : Convert.ToInt64(reader.GetValue(10)),
                ExecutionTypeDesc = reader.IsDBNull(11) ? "" : reader.GetString(11),
                FirstExecutionTime = reader.IsDBNull(12) ? (DateTime?)null : reader.GetDateTime(12),
                ModuleName = reader.IsDBNull(13) ? "" : reader.GetString(13),
                AvgNumPhysicalReads = reader.IsDBNull(14) ? 0 : ToDouble(reader.GetValue(14)),
                MinNumPhysicalReads = reader.IsDBNull(15) ? 0 : ToDouble(reader.GetValue(15)),
                MaxNumPhysicalReads = reader.IsDBNull(16) ? 0 : ToDouble(reader.GetValue(16)),
                AvgClrTimeMs = reader.IsDBNull(17) ? 0 : ToDouble(reader.GetValue(17)),
                MinClrTimeMs = reader.IsDBNull(18) ? 0 : ToDouble(reader.GetValue(18)),
                MaxClrTimeMs = reader.IsDBNull(19) ? 0 : ToDouble(reader.GetValue(19)),
                AvgLogMb = reader.IsDBNull(20) ? 0 : ToDouble(reader.GetValue(20)),
                MinLogMb = reader.IsDBNull(21) ? 0 : ToDouble(reader.GetValue(21)),
                MaxLogMb = reader.IsDBNull(22) ? 0 : ToDouble(reader.GetValue(22)),
                PlanId = reader.IsDBNull(23) ? 0 : reader.GetInt64(23),
                MinDurationMs = reader.IsDBNull(24) ? 0 : ToDouble(reader.GetValue(24)),
                MaxDurationMs = reader.IsDBNull(25) ? 0 : ToDouble(reader.GetValue(25)),
                MinCpuTimeMs = reader.IsDBNull(26) ? 0 : ToDouble(reader.GetValue(26)),
                MaxCpuTimeMs = reader.IsDBNull(27) ? 0 : ToDouble(reader.GetValue(27)),
                MinLogicalReads = reader.IsDBNull(28) ? 0 : ToDouble(reader.GetValue(28)),
                MaxLogicalReads = reader.IsDBNull(29) ? 0 : ToDouble(reader.GetValue(29)),
                MinLogicalWrites = reader.IsDBNull(30) ? 0 : ToDouble(reader.GetValue(30)),
                MaxLogicalWrites = reader.IsDBNull(31) ? 0 : ToDouble(reader.GetValue(31)),
                MinPhysicalReads = reader.IsDBNull(32) ? 0 : ToDouble(reader.GetValue(32)),
                MaxPhysicalReads = reader.IsDBNull(33) ? 0 : ToDouble(reader.GetValue(33)),
                MinRowcount = reader.IsDBNull(34) ? 0 : ToDouble(reader.GetValue(34)),
                MaxRowcount = reader.IsDBNull(35) ? 0 : ToDouble(reader.GetValue(35)),
                AvgMemoryMb = reader.IsDBNull(36) ? 0 : ToDouble(reader.GetValue(36)),
                MinMemoryMb = reader.IsDBNull(37) ? 0 : ToDouble(reader.GetValue(37)),
                MaxMemoryMb = reader.IsDBNull(38) ? 0 : ToDouble(reader.GetValue(38)),
                AvgTempdbMb = reader.IsDBNull(39) ? 0 : ToDouble(reader.GetValue(39)),
                MinTempdbMb = reader.IsDBNull(40) ? 0 : ToDouble(reader.GetValue(40)),
                MaxTempdbMb = reader.IsDBNull(41) ? 0 : ToDouble(reader.GetValue(41)),
                PlanType = reader.IsDBNull(42) ? "" : reader.GetString(42),
                IsForcedPlan = !reader.IsDBNull(43) && reader.GetBoolean(43),
                ForceFailureCount = reader.IsDBNull(44) ? 0 : reader.GetInt64(44),
                LastForceFailureReason = reader.IsDBNull(45) ? "" : reader.GetString(45),
                PlanForcingType = reader.IsDBNull(46) ? "" : reader.GetString(46),
                CompatibilityLevel = reader.IsDBNull(47) ? 0 : reader.GetInt32(47),
                QueryHash = reader.IsDBNull(48) ? "" : reader.GetString(48),
                QueryPlanHash = reader.IsDBNull(49) ? "" : reader.GetString(49)
            });
        }

        return items;
    }

    /// <summary>
    /// Fetches a query plan on-demand from Query Store by plan_id.
    /// Uses three-part naming with sp_executesql for Azure SQL DB compatibility.
    /// </summary>
    public static async Task<string?> FetchQueryStorePlanAsync(string connectionString, string databaseName, long planId)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        /*
        Validate the database name exists and get the QUOTENAME'd version
        to prevent SQL injection via malicious database names
        */
        var quotedDbName = await GetValidatedDatabaseNameAsync(connection, databaseName);
        if (quotedDbName == null)
        {
            return null;
        }

        /*
        Use three-part naming (database.sys.sp_executesql) instead of USE statement
        for Azure SQL DB compatibility
        */
        var query = $@"
EXECUTE {quotedDbName}.sys.sp_executesql
    N'
SELECT TOP (1)
    query_plan = CONVERT(nvarchar(max), qsp.query_plan)
FROM sys.query_store_plan AS qsp
WHERE qsp.plan_id = @plan_id
AND   qsp.query_plan IS NOT NULL
OPTION(RECOMPILE);',
    N'@plan_id bigint',
    @plan_id;";

        using var command = new SqlCommand(query, connection) { CommandTimeout = 30 };
        command.Parameters.Add(new SqlParameter("@plan_id", SqlDbType.BigInt) { Value = planId });
        var result = await command.ExecuteScalarAsync();
        return result as string;
    }
    /// <summary>
    /// Gets Query Store duration trend — each interval's work, placed at the time that work RAN.
    ///
    /// <para>#1841 tier 2 corrected this, and it is the one Query Store read whose FIX is a different
    /// shape from the other four. Query Store rows are cumulative per-interval snapshots that the
    /// collector re-fetches every cycle, so an interval that reached 40 executions used to charge 10,
    /// then 25, then 40 to three successive points. Tier 1 deliberately left the overstatement in,
    /// because dedup ALONE makes this chart worse rather than better: it keeps one row per interval at
    /// the collection where the interval CLOSED, and Query Store's default INTERVAL_LENGTH_MINUTES is
    /// 60 against a 5-minute cadence, so twelve snapshots collapse onto one collection_time and a
    /// 1-hour window renders a SINGLE point valued 0.</para>
    ///
    /// <para>What unlocks it is the x-axis, not the dedup: <c>interval_start_time_utc</c> is the
    /// interval's own start boundary, converted to UTC AT COLLECTION, so it is the same clock as
    /// collection_time. (The premise this was blocked on — that the interval clock is the monitored
    /// server's LOCAL wall time — was wrong on both halves: Query Store's interval start_time and
    /// first_execution_time are both <c>datetimeoffset</c>, and the collector already normalized them
    /// through <c>DateTimeOffset.UtcDateTime</c> before storing.) Deduped and placed at its start, each
    /// interval contributes its true final total exactly once, at the hour it ran, and the series
    /// resolution becomes Query Store's OWN interval length — which is the honest resolution of this
    /// source. A window shorter than one interval showing one or two points is the data's resolution,
    /// not a defect.</para>
    ///
    /// <para><b>The legacy boundary.</b> Rows collected before tier 2 carry no interval start, and
    /// nothing can reconstruct one — so they keep the pre-tier-2 treatment exactly: un-deduped, placed
    /// at collection_time, still overstating. The two arms are split on
    /// <c>interval_start_time_utc IS NULL</c>, which partitions the rows with no overlap and no gap, so
    /// no row is counted twice and none is dropped. A window spanning the upgrade therefore renders a
    /// corrected recent section and an un-corrected older one, each behaving as its own generation
    /// always did, and the mixture resolves itself as the pre-upgrade rows age out of retention.</para>
    /// </summary>
    public async Task<List<QueryTrendPoint>> GetQueryStoreDurationTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = @"
WITH placed AS
(
    -- Arm 1 (#1841 tier 2) — rows carrying the interval identity. Dedup to the interval's FINAL
    -- cumulative snapshot, then place it at interval_start_time_utc: the hour the work ran, not the
    -- cycle that last fetched it. Both halves are needed; see the method remarks for why dedup alone
    -- collapses this series and placement alone leaves it inflated.
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
        FROM v_query_store_stats
        WHERE server_id = $1
        -- Windowed on the column this arm PLACES its points at (#1892), which inside the IS NOT NULL guard
        -- below is what COALESCE(interval_start_time_utc, collection_time) resolves to anyway. Filtering on
        -- collection_time here put a point outside the range the caller asked for, and dropped the range's
        -- final interval because its closing fetch had not happened yet.
        AND   interval_start_time_utc >= $2
        AND   interval_start_time_utc <= $3
        -- Pruning bounds only; see the slicer above for why the floor is free and why the ceiling is a
        -- month rather than tight.
        AND   collection_time >= $2 - INTERVAL '1 day'
        AND   collection_time <= $3 + INTERVAL '30 days'
        AND   interval_start_time_utc IS NOT NULL" + dbClause + @"
    ) identified
    WHERE rn = 1

    UNION ALL

    -- Arm 2 — rows collected before tier 2. No interval start exists and none can be reconstructed, so
    -- these keep the pre-tier-2 treatment byte for byte: un-deduped, placed at collection_time, still
    -- overstating. The split is on IS NULL / IS NOT NULL, so the two arms partition the rows exactly —
    -- nothing is counted twice and nothing is dropped.
    SELECT
        collection_time AS point_time,
        execution_count,
        avg_duration_us
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   interval_start_time_utc IS NULL" + dbClause + @"
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
ORDER BY point_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<QueryTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QueryTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                Value = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                ExecutionCount = reader.IsDBNull(2) ? 0 : (long)ToDouble(reader.GetValue(2)),
                ExecutionsPerSecond = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2))
            });
        }
        return items;
    }
}

public class QueryStoreRow
{
    /// <summary>Gates "Get Actual Plan (re-run)" — see <see cref="QuerySnapshotRow.CanGetActualPlan"/>.</summary>
    public bool CanGetActualPlan => !string.IsNullOrEmpty(QueryText);

    public string DatabaseName { get; set; } = "";
    public long QueryId { get; set; }
    public long PlanId { get; set; }

    /// <summary>
    /// The replica role SQL Server 2022+ attributed these runtime stats to ('Primary', 'Secondary',
    /// 'Geo Secondary', 'Geo HA Secondary'), a GROUP BY key so an AG's shared Query Store does not
    /// blend replicas into one row. NULL/blank = the server did not attribute it (pre-2022, or a 2022
    /// standalone) — expected, and deliberately not coalesced.
    /// </summary>
    public string? ReplicaRole { get; set; }
    public string QueryHash { get; set; } = "";
    public string QueryText { get; set; } = "";
    public string ModuleName { get; set; } = "";
    public long TotalExecutions { get; set; }
    public double AvgDurationMs { get; set; }
    public double AvgCpuTimeMs { get; set; }
    public double AvgLogicalReads { get; set; }
    public double AvgLogicalWrites { get; set; }
    public double AvgPhysicalReads { get; set; }
    public double AvgRowcount { get; set; }
    public long MinDop { get; set; }
    public long MaxDop { get; set; }
    public DateTime? LastExecutionTime { get; set; }
    public string QueryPlanHash { get; set; } = "";
    public bool IsForcedPlan { get; set; }
    public string PlanForcingType { get; set; } = "";
    public string? QueryPlanText { get; set; }
    public string ExecutionTypeDesc { get; set; } = "";
    public DateTime? FirstExecutionTime { get; set; }
    public double AvgClrTimeMs { get; set; }
    public double AvgTempdbSpaceUsed { get; set; }
    public double AvgLogBytesUsed { get; set; }
    public string PlanType { get; set; } = "";
    public long ForceFailureCount { get; set; }
    public string LastForceFailureReason { get; set; } = "";
    public int CompatibilityLevel { get; set; }
    // Min/max variants + memory-grant + num-physical-io-reads families, for parity with the Dashboard QS grid.
    // Duration/CPU/CLR are in ms (converted from us in SQL); reads/writes/rows/tempdb pages are raw;
    // log bytes are raw bytes; memory grant is converted pages->MB in SQL.
    public double MinDurationMs { get; set; }
    public double MaxDurationMs { get; set; }
    public double MinCpuTimeMs { get; set; }
    public double MaxCpuTimeMs { get; set; }
    public double MinLogicalReads { get; set; }
    public double MaxLogicalReads { get; set; }
    public double MinLogicalWrites { get; set; }
    public double MaxLogicalWrites { get; set; }
    public double MinPhysicalReads { get; set; }
    public double MaxPhysicalReads { get; set; }
    public double MinClrTimeMs { get; set; }
    public double MaxClrTimeMs { get; set; }
    public double MinRowcount { get; set; }
    public double MaxRowcount { get; set; }
    public double MinLogBytesUsed { get; set; }
    public double MaxLogBytesUsed { get; set; }
    public double MinTempdbSpaceUsed { get; set; }
    public double MaxTempdbSpaceUsed { get; set; }
    public double AvgMemoryMb { get; set; }
    public double MinMemoryMb { get; set; }
    public double MaxMemoryMb { get; set; }
    public double AvgNumPhysicalIoReads { get; set; }
    public double MinNumPhysicalIoReads { get; set; }
    public double MaxNumPhysicalIoReads { get; set; }
    public bool HasQueryPlan => !string.IsNullOrEmpty(QueryPlanText);
    public string FirstExecutionTimeLocal => ServerTimeHelper.FormatServerTime(FirstExecutionTime);
    public string LastExecutionTimeLocal => ServerTimeHelper.FormatServerTime(LastExecutionTime);
    public double TotalCpuMs => TotalExecutions * AvgCpuTimeMs;
    public double TotalDurationMs => TotalExecutions * AvgDurationMs;
}

public class QueryStoreHistoryRow
{
    public DateTime CollectionTime { get; set; }
    public long ExecutionCount { get; set; }
    public double AvgDurationMs { get; set; }
    public double AvgCpuTimeMs { get; set; }
    public double AvgLogicalReads { get; set; }
    public double AvgLogicalWrites { get; set; }
    public double AvgPhysicalReads { get; set; }
    public double AvgRowcount { get; set; }
    public DateTime? LastExecutionTime { get; set; }
    public long MinDop { get; set; }
    public long MaxDop { get; set; }

    // Execution type (Regular / Aborted / Exception)
    public string ExecutionTypeDesc { get; set; } = "";

    // First execution time in the interval
    public DateTime? FirstExecutionTime { get; set; }

    // Module (proc/function) the query came from
    public string ModuleName { get; set; } = "";

    // Number of physical IO reads (avg/min/max)
    public double AvgNumPhysicalReads { get; set; }
    public double MinNumPhysicalReads { get; set; }
    public double MaxNumPhysicalReads { get; set; }

    // CLR time, already converted to ms in SQL (avg/min/max)
    public double AvgClrTimeMs { get; set; }
    public double MinClrTimeMs { get; set; }
    public double MaxClrTimeMs { get; set; }

    // Log bytes used, already converted to MB in SQL (avg/min/max)
    public double AvgLogMb { get; set; }
    public double MinLogMb { get; set; }
    public double MaxLogMb { get; set; }

    // Per-plan identity + min/max spreads + memory/tempdb + plan-forcing metadata,
    // for parity with the Dashboard Query Store drilldown (QueryExecutionHistoryWindow).
    public long PlanId { get; set; }
    public double MinDurationMs { get; set; }
    public double MaxDurationMs { get; set; }
    public double MinCpuTimeMs { get; set; }
    public double MaxCpuTimeMs { get; set; }
    public double MinLogicalReads { get; set; }
    public double MaxLogicalReads { get; set; }
    public double MinLogicalWrites { get; set; }
    public double MaxLogicalWrites { get; set; }
    public double MinPhysicalReads { get; set; }
    public double MaxPhysicalReads { get; set; }
    public double MinRowcount { get; set; }
    public double MaxRowcount { get; set; }
    public double AvgMemoryMb { get; set; }
    public double MinMemoryMb { get; set; }
    public double MaxMemoryMb { get; set; }
    public double AvgTempdbMb { get; set; }
    public double MinTempdbMb { get; set; }
    public double MaxTempdbMb { get; set; }
    public string PlanType { get; set; } = "";
    public bool IsForcedPlan { get; set; }
    public long ForceFailureCount { get; set; }
    public string LastForceFailureReason { get; set; } = "";
    public string PlanForcingType { get; set; } = "";
    public int CompatibilityLevel { get; set; }
    public string QueryHash { get; set; } = "";
    public string QueryPlanHash { get; set; } = "";

    public double TotalDurationMs => ExecutionCount * AvgDurationMs;
    public double TotalCpuMs => ExecutionCount * AvgCpuTimeMs;
    public string CollectionTimeLocal => ServerTimeHelper.FormatServerTime(CollectionTime);
    public string FirstExecutionTimeLocal => ServerTimeHelper.FormatServerTime(FirstExecutionTime);
    public string LastExecutionTimeLocal => ServerTimeHelper.FormatServerTime(LastExecutionTime);
}
