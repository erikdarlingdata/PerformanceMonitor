/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One Query-Store-by-Duration grid row — the viewer copy of Lite's <c>QueryStoreRow</c>
/// (LocalDataService.QueryStore.cs). A (database, query_id, plan_id, query_hash, execution outcome, replica
/// role) group's interval averages + min/max spreads over the window, with the latest captured query text.
/// Duration / CPU / CLR are converted to ms (from us) in SQL; memory-grant pages are converted to MB
/// in SQL; reads/writes/rows/tempdb pages / log bytes stay raw. The View-Plan surface is deferred, so
/// unlike Lite there is no QueryPlanText/HasQueryPlan (the collected query_plan_text is not read).
/// </summary>
public sealed class ViewerQueryStoreRow : QueryTagRowBase
{
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

    /// <summary>Gates "View Stored Plan" in the QueryStorePlanContextMenu — Query Store rows carry a stored plan.</summary>
    public bool HasStoredPlan => true;

    /// <summary>Gates "Get Actual Plan (re-run)" — the service re-executes by QueryId (BuildActualPlanArgsForQueryStore).</summary>
    public bool CanGetActualPlan => QueryId != 0;
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
    public string ExecutionTypeDesc { get; set; } = "";
    public DateTime? FirstExecutionTime { get; set; }
    public double AvgClrTimeMs { get; set; }
    public double AvgTempdbSpaceUsed { get; set; }
    public double AvgLogBytesUsed { get; set; }
    public string PlanType { get; set; } = "";
    public long ForceFailureCount { get; set; }
    public string LastForceFailureReason { get; set; } = "";
    public int CompatibilityLevel { get; set; }
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
    /// <summary>Query Store returns <c>datetimeoffset</c> and <c>QueryStoreCollector</c> normalises both
    /// execution stamps through <c>((DateTimeOffset)…).UtcDateTime</c> before storing, so
    /// <c>query_store_stats.first_execution_time</c> and <c>last_execution_time</c> are naive UTC —
    /// unlike the <c>sys.dm_exec_*</c> stamps, which are the server's own clock. They therefore convert
    /// through <see cref="ViewerDataService.FormatStoredUtc"/> rather than through the server-clock
    /// renderer.</summary>
    public string FirstExecutionTimeLocal => ViewerDataService.FormatStoredUtc(FirstExecutionTime);

    /// <inheritdoc cref="FirstExecutionTimeLocal"/>
    public string LastExecutionTimeLocal => ViewerDataService.FormatStoredUtc(LastExecutionTime);
    public double TotalCpuMs => TotalExecutions * AvgCpuTimeMs;
    public double TotalDurationMs => TotalExecutions * AvgDurationMs;
}

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The Query-Store-by-Duration read — Lite's <c>GetQueryStoreTopQueriesAsync</c> ported to Postgres
    /// against the base <c>query_store_stats</c> table (the store name; Lite reads its
    /// <c>v_query_store_stats</c> view). Group by (database, query_id, plan_id, query_hash), average the
    /// per-interval metrics + carry min/max spreads, rank by the plan's total duration,
    /// <c>SUM(avg_duration_us * execution_count)</c>, descending (every avg column is averaged weighted by
    /// execution_count), over-fetch by 5 for the WAITFOR trim, and cap
    /// at top. Viewer deviations: <c>SUM(execution_count)</c> CAST to bigint; the query_plan_text column
    /// is not selected (plan surfacing deferred); the forced-plan flag folds through Postgres
    /// <c>bool_or(is_forced_plan)</c> (Postgres has no <c>MAX(boolean)</c> aggregate, unlike DuckDB).
    /// Every avg/min/max metric column is stored as bigint, so each is CAST to double precision before
    /// the AVG/scale arithmetic (matching Lite's DuckDB casts).
    /// $1 server_id, $2 window start, $3 window end (naive UTC), $4 top.
    /// </summary>
    public const string QueryStoreTopSql = QueryStoreTopRawPrefix + QueryStoreTopSuffix;

    /// <summary>#7776: the collection_time bounds beside the interval-start window on the raw top read and the slicer. The floor is the
    /// clock-skew slack below the window and the ceiling is the longest a row's collection trails its interval's start
    /// (<see cref="QueryStoreIntervalWide.CollectionCeiling"/>) above it (see <see cref="QueryStoreSlicerSql"/>). The top read's inline-text fallback uses the ceiling too, so it reaches the fact rows.</summary>
    private const string CollectionFloorSlackSql = QueryStoreIntervalWide.CollectionFloorSlackSql;

    /// <summary>The ceiling half of <see cref="CollectionFloorSlackSql"/>.</summary>
    private const string CollectionCeilingSql = QueryStoreIntervalWide.CollectionCeilingSql;

    /// <summary>
    /// The raw read's head (#3953 split it off <see cref="QueryStoreTopSql"/>'s prior single-string form,
    /// byte-identical — the split itself changes nothing about the text a raw call sends): the interval dedupe
    /// over the server's raw Query Store slice. <see cref="QueryStoreTopSuffix"/> is shared with
    /// <see cref="QueryStoreTopTableSql"/>, so the two reads cannot drift above <c>ranked</c>.
    /// $1 server_id, $2 window start, $3 window end (naive UTC), $4 top, $5 database filter.
    /// </summary>
    private const string QueryStoreTopRawPrefix = $$"""
        WITH deduped AS (
            /* LOAD-BEARING (correctness, not just perf) — #1841. query_store_stats rows are CUMULATIVE
               per-Query-Store-interval snapshots, and the collector re-fetches the OPEN interval every
               cycle as its last_execution_time advances, so the SAME interval (same first_execution_time)
               is stored repeatedly with a growing execution_count. SUM(execution_count) over the raw rows
               reports 10 + 25 + 40 for an interval that reached 40, and the execution-weighted
               averages would count a re-collected interval again at its own execution count. Keep the LATEST
               snapshot per interval. Matches the dedup convention in PgFactCollector.QueryPerf and
               PgDrillDownCollector.Queries, and Lite's GetQueryStoreTopQueriesAsync.

               replica_role and execution_type_desc are in the partition because the aggregate below is
               grouped (or MAXed) on them: the dedup key must be at least as fine as the read's own row
               identity, or dedup would silently drop a row the grid must show rather than de-duplicate one.

               The execution_count tie-break is the #1907 residual, and it is NOT decoration. collection_time
               alone was not a total order on rows collected before that fix: Query Store hands back the
               flushed and the still-in-memory slice of ONE interval as two ADDITIVE rows, the collector
               stored both, and they then shared every partition column above AND collection_time. ROW_NUMBER
               kept whichever the engine emitted first, so a grid could show an in-memory sliver of 8 where
               the interval's total was 94, and show a different one on the next run. The collector now
               combines the slices before storing, so rows collected after #1907 cannot tie at all and this
               clause never fires on them. It is here for the rows already in the store, which cannot be
               rewritten: it deterministically picks the FLUSHED slice, the one holding the bulk of the
               interval's work, instead of flapping. Closest-available, not correct — the correct value is the
               SUM of the slices, which no read-side rule can express (#1912). This applies to EVERY dedup
               site in both apps: Darling.Tests/QueryStoreSliceTieBreakSourceTests pins all 12 of Darling's
               across the 8 files that hold them, and Lite.Tests/QueryStoreSliceTieBreakSourceTests pins its
               7, so dropping one fails loudly and names the file. Both enumerate their files rather than
               globbing, so MOVING a read has to be re-declared instead of silently shrinking the guard. */
            SELECT
                database_name,
                query_id,
                plan_id,
                query_hash,
                execution_type_desc,
                replica_role,
                execution_count,
                avg_duration_us,
                ROW_NUMBER() OVER
                (
                    PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                    ORDER BY collection_time DESC, execution_count DESC
                ) AS rn
            FROM query_store_stats
            WHERE server_id = $1
            {{QueryStoreTopRawWindow}}
        ),

        """;

    /// <summary>The raw read's window predicates, the one place both of its passes over <c>query_store_stats</c> take them from (#8549):
    /// <c>deduped</c> (the narrow pass that ranks) and <c>wide</c> (the pass that fetches the kept keys' rows) read the same rows, so a
    /// predicate changed in one cannot miss the other. The rows' source is <c>FROM query_store_stats WHERE server_id = $1</c> before it.
    /// $2 window start, $3 window end (naive UTC), $5 database filter.</summary>
    private const string QueryStoreTopRawWindow = $$"""
        /* #7776: the window is the interval's own start, the expression the slicer's bars are keyed on (QueryStoreSlicerSql, #1892),
           so a slice's grid rows are the intervals its bars draw. It was collection_time, which trails the interval by up to the
           interval's length: an interval collected inside the slice but started before it showed here with no bar, and the slice's
           last interval, collected after its end, was missing. Legacy rows have no interval start and keep collection_time.
           The collection_time bounds below are chunk exclusion, not filters, with the slicer read's reasoning: the floor is implied
           by the predicate (an interval is collected after it starts; the hour is clock-skew slack) and the ceiling is the
           longest a row's collection trails its interval's start (QueryStoreIntervalWide.CollectionCeiling). They keep the
           (server_id, collection_time) index and the chunk pruning. */
        AND   COALESCE(interval_start_time_utc, collection_time) >= $2
        AND   COALESCE(interval_start_time_utc, collection_time) < $3
        AND   collection_time >= $2 - {{CollectionFloorSlackSql}}
        AND   collection_time <= $3 + {{CollectionCeilingSql}}
        AND   ($5::text[] IS NULL OR database_name = ANY($5))
        """;

    /// <summary>
    /// #9373: where the by-keys statements' prefilter goes. Every interval-table arm and the <c>query_store_grid_daily</c> arm of a table or
    /// rollup prefix carries this token after its database list ($5). <see cref="WithByKeysFilter"/> fills it for a by-keys statement and
    /// <see cref="WithoutByKeysFilter"/> empties it for the keys statements and the one-statement read, so one prefix serves all of them.
    /// </summary>
    private const string ByKeysSlot = "/*BYKEYS*/";

    /// <summary>
    /// #9373: the by-keys statements' arms bounded to the kept keys' database names and query ids, as the NULL-able arrays
    /// <paramref name="databaseParameter"/> and <paramref name="queryIdParameter"/> (<see cref="TableReadKeys.AddFilterParameters"/>). Both are
    /// superset filters on columns of the dedupe: these prefixes read rows with a literal <c>rn</c> of 1 and no window, so a row is kept or
    /// dropped on its own columns. Every row <see cref="KeyMatch"/> and the two <c>EXISTS</c> branches above can match has its database name
    /// among the kept keys' and its query id among theirs, so the rows kept, their <c>rn</c> and the page do not change. A kept key with a NULL
    /// database name or query id cannot match a plain equality, so the caller binds both arrays NULL for it, the filter is true, and the
    /// arms read as they did before (the second <c>EXISTS</c> branch finds those keys).
    /// </summary>
    private static string WithByKeysFilter(string prefix, int databaseParameter, int queryIdParameter) =>
        prefix.Replace(ByKeysSlot,
            $"AND   (${databaseParameter}::text[] IS NULL OR database_name = ANY(${databaseParameter}))\n"
            + $"            AND   (${queryIdParameter}::bigint[] IS NULL OR query_id = ANY(${queryIdParameter}))", StringComparison.Ordinal);

    /// <summary>#9373: <paramref name="prefix"/> with <see cref="ByKeysSlot"/> left empty, for the statements that rank (the keys statements) and the one-statement read.</summary>
    private static string WithoutByKeysFilter(string prefix) => prefix.Replace(ByKeysSlot, "", StringComparison.Ordinal);

    /// <summary>The first free parameter of a table by-keys statement after its keys ($1 to $5, $6 to $11).</summary>
    private const int TableByKeysFilterParameter = 12;

    /// <summary>The first free parameter of a rollup by-keys statement: after its keys and the built run's two midnights ($12, $13).</summary>
    private const int RollupByKeysFilterParameter = 14;

    /// <summary>
    /// The table read's head (#3953): <c>query_store_interval_wide</c> already holds the latest snapshot per
    /// interval, every outcome — the raw prefix's ROW_NUMBER dedupe above, maintained as the table is written
    /// (<see cref="QueryStoreIntervalWide.UpsertSql"/> keeps only the running maximum under
    /// <c>(collection_time DESC, execution_count DESC)</c> per identity) — so this reads it directly and sets
    /// <c>rn</c> to a literal 1 rather than computing a rank. $2 is the gate's own clamp,
    /// <c>max(window start, raw's chunk floor)</c> (review D4R H3): raw chunks drop whole, so bounding the table
    /// read there reaches further back only where raw has already dropped the chunk. The clamp is a
    /// <c>collection_time</c> and the window is placed by the interval's start (#7776), so at the clamp itself the
    /// table read leaves out the intervals that started before the floor and were collected after it, which raw still
    /// holds: the one interval straddling the floor, at the edge of what raw retains. $3 is nullable: NULL is an open end (a WPF
    /// preset), which reads through whatever the table currently holds; a literal end (a custom range, MCP
    /// <c>as_of</c>) bounds it exactly as raw's own $3 does.
    /// <para><b>The <c>first_execution_time</c> floor (#4605).</b> Neither the unique key (it leads with
    /// <c>server_id</c>) nor <c>idx_query_store_interval_wide_first_exec</c> serves <c>collection_time</c>, so
    /// this read walked all of the server's rows. <c>first_execution_time</c> is a key column of that unique key, so
    /// <c>first_execution_time &gt;= $2 - </c><see cref="QueryStoreIntervalWide.PurgeEdgeMarginSql"/> filters its
    /// entries before the heap, and it drops no row: every stored row has
    /// <c>first_execution_time &gt; collection_time - (IntervalSpanMargin + MaxCatchup)</c>, and
    /// <see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/> is that bound plus an hour (the argument is in
    /// <see cref="QueryStoreIntervalWide.PurgeEdgeMarginSql"/>'s summary). A static readonly rather than a const
    /// because the interval literal is derived from that TimeSpan; <c>$$"""</c> keeps <c>$1</c> literal.</para>
    /// <para><b>The upper bound (#5523, #7776).</b> A window with an end ($3 not NULL) also carries
    /// <c>first_execution_time &lt;= $3 + </c><see cref="QueryStoreIntervalWide.IntervalStartFirstExecMarginSql"/>, so the
    /// <c>(server_id, first_execution_time)</c> btree scans a range finite at both ends instead of everything newer than
    /// the window start. An open end (NULL) reads to now, as before. The window is placed by the interval's start
    /// (<c>COALESCE(interval_start_time_utc, collection_time)</c>, #7776), so the margin is the one the duration trend's start-placed
    /// arm uses: an interval that starts before $3 can have a first execution up to its own length (a day) after it. It was
    /// <see cref="QueryStoreIntervalWide.FirstExecUpperSlackSql"/> while the window was placed by <c>collection_time</c>.</para>
    /// </summary>
    private static readonly string QueryStoreTopTablePrefix = $$"""
        WITH deduped AS NOT MATERIALIZED (
            SELECT
                *,
                1 AS rn
            FROM query_store_interval_wide
            WHERE server_id = $1
            /* #7776: placed by the interval's start, as the raw read and the slicer's bars are (QueryStoreTopRawPrefix). The
               first_execution_time range below is what the btree serves; this is a residual filter, and no collection_time
               bound is added because none could drop a row here (an interval is collected after it starts). */
            AND   COALESCE(interval_start_time_utc, collection_time) >= $2
            AND   ($3::timestamp IS NULL OR COALESCE(interval_start_time_utc, collection_time) < $3)
            AND   first_execution_time >= $2 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   ($3::timestamp IS NULL OR first_execution_time <= $3 + {{QueryStoreIntervalWide.IntervalStartFirstExecMarginSql}})
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
            {{ByKeysSlot}}
        ),

        """;

    /// <summary>The select list both arms of <see cref="QueryStoreTopTableSplitPrefix"/> project: every column, and the literal <c>rn</c> of <see cref="QueryStoreTopTablePrefix"/>.</summary>
    private const string QueryStoreTopTableSplitSelect = "*, 1 AS rn";

    /// <summary>The predicates both arms repeat: the database list ($5) and the by-keys slot (<see cref="ByKeysSlot"/>). The grid has no execution outcome filter.</summary>
    private const string QueryStoreTopTableSplitTrailing = "            AND   ($5::text[] IS NULL OR database_name = ANY($5))\n            " + ByKeysSlot;

    /// <summary>
    /// <see cref="QueryStoreTopTablePrefix"/> with the window as two arms (#9283), for a store whose
    /// <see cref="QueryStoreBackgroundIndexes.WideNullStartFirstExec"/> is built: rows with an interval start take the one hour
    /// <c>first_execution_time</c> floor, rows without one keep the <see cref="QueryStoreIntervalWide.PurgeEdgeMarginSql"/> floor and read
    /// through the partial index (<see cref="QueryStoreIntervalWide.IntervalStartSplitSql"/>), as the MCP top read does since #9276. The
    /// Viewer's window can have no end, so both arms take the helper's open end form ($3 NULL reads to now). The rows, columns and
    /// parameters are <see cref="QueryStoreTopTablePrefix"/>'s, and a statement built on it keeps the <c>deduped</c> name the keys, tag and
    /// self-statement slots read. The closed-day rollup prefix has the same split in <see cref="QueryStoreTopRollupSplitPrefix"/> (#9287).
    /// </summary>
    private static readonly string QueryStoreTopTableSplitPrefix = $$"""
        WITH deduped AS NOT MATERIALIZED (
        {{QueryStoreIntervalWide.IntervalStartSplitSql(QueryStoreTopTableSplitSelect, QueryStoreTopTableSplitTrailing, openEnd: true)}}
        ),

        """;

    /// <summary>The select list of <c>ranked</c>: the group keys and every aggregate, one row per identity. Shared by the raw
    /// read's <see cref="QueryStoreTopSuffix"/> and the interval-table read's <see cref="QueryStoreTopTableRankedHead"/>, so the two
    /// cannot drift in what they return.</summary>
    private const string QueryStoreTopRankedColumns = $$"""
                database_name,
                query_id,
                plan_id,
                query_hash,
                /* A GROUP BY key, not MAX(): with Query Store for secondary replicas (2022+) the primary
                   holds ONE shared Query Store carrying every replica's rows, so grouping without it would
                   average primary and secondary workload into a single blended row — the conflation this
                   column exists to expose. Grouping splits them into one row per replica role instead. On a
                   standalone/non-AG server every row shares one value (NULL, or 'Primary' on 2025), so the
                   grouping is a no-op and the grid is unchanged. Twins Lite's QueryStore reader. */
                replica_role,
                /* A GROUP BY key too, for the same reason: Query Store keeps Regular, Aborted and Exception
                   executions of one plan in separate runtime-stats rows. MAX() showed "Regular" for any mixed
                   group (it sorts last) while the averages blended a timeout's duration into the plan's normal
                   cost. One row per outcome, matching Lite's grid and both SKUs' get_query_store_top. */
                execution_type_desc,
                MAX(module_name) AS module_name,
                CAST(SUM(execution_count) AS bigint) AS total_executions,
                {{QueryStoreWeightedAverage.DurationUs}} / 1000.0 AS avg_duration_ms,
                {{QueryStoreWeightedAverage.CpuTimeUs}} / 1000.0 AS avg_cpu_time_ms,
                {{QueryStoreWeightedAverage.LogicalReads}} AS avg_logical_reads,
                {{QueryStoreWeightedAverage.LogicalWrites}} AS avg_logical_writes,
                {{QueryStoreWeightedAverage.PhysicalReads}} AS avg_physical_reads,
                {{QueryStoreWeightedAverage.Rowcount}} AS avg_rowcount,
                MIN(min_dop) AS min_dop,
                MAX(max_dop) AS max_dop,
                MAX(last_execution_time) AS last_execution_time,
                MAX(query_plan_hash) AS query_plan_hash,
                bool_or(is_forced_plan) AS is_forced_plan,
                MAX(plan_forcing_type) AS plan_forcing_type,
                MIN(first_execution_time) AS first_execution_time,
                {{QueryStoreWeightedAverage.ClrTimeUs}} / 1000.0 AS avg_clr_time_ms,
                {{QueryStoreWeightedAverage.TempdbSpaceUsed}} AS avg_tempdb_space_used,
                {{QueryStoreWeightedAverage.LogBytesUsed}} AS avg_log_bytes_used,
                MAX(plan_type) AS plan_type,
                MAX(force_failure_count) AS force_failure_count,
                MAX(last_force_failure_reason) AS last_force_failure_reason,
                MAX(compatibility_level) AS compatibility_level,
                MIN(CAST(min_duration_us AS double precision)) / 1000.0 AS min_duration_ms,
                MAX(CAST(max_duration_us AS double precision)) / 1000.0 AS max_duration_ms,
                MIN(CAST(min_cpu_time_us AS double precision)) / 1000.0 AS min_cpu_time_ms,
                MAX(CAST(max_cpu_time_us AS double precision)) / 1000.0 AS max_cpu_time_ms,
                MIN(CAST(min_logical_io_reads AS double precision)) AS min_logical_reads,
                MAX(CAST(max_logical_io_reads AS double precision)) AS max_logical_reads,
                MIN(CAST(min_logical_io_writes AS double precision)) AS min_logical_writes,
                MAX(CAST(max_logical_io_writes AS double precision)) AS max_logical_writes,
                MIN(CAST(min_physical_io_reads AS double precision)) AS min_physical_reads,
                MAX(CAST(max_physical_io_reads AS double precision)) AS max_physical_reads,
                MIN(CAST(min_clr_time_us AS double precision)) / 1000.0 AS min_clr_time_ms,
                MAX(CAST(max_clr_time_us AS double precision)) / 1000.0 AS max_clr_time_ms,
                MIN(CAST(min_rowcount AS double precision)) AS min_rowcount,
                MAX(CAST(max_rowcount AS double precision)) AS max_rowcount,
                MIN(CAST(min_log_bytes_used AS double precision)) AS min_log_bytes_used,
                MAX(CAST(max_log_bytes_used AS double precision)) AS max_log_bytes_used,
                MIN(CAST(min_tempdb_space_used AS double precision)) AS min_tempdb_space_used,
                MAX(CAST(max_tempdb_space_used AS double precision)) AS max_tempdb_space_used,
                {{QueryStoreWeightedAverage.QueryMaxUsedMemory}} * 8.0 / 1024.0 AS avg_memory_mb,
                MIN(CAST(min_query_max_used_memory AS double precision)) * 8.0 / 1024.0 AS min_memory_mb,
                MAX(CAST(max_query_max_used_memory AS double precision)) * 8.0 / 1024.0 AS max_memory_mb,
                {{QueryStoreWeightedAverage.NumPhysicalIoReads}} AS avg_num_physical_io_reads,
                MIN(CAST(min_num_physical_io_reads AS double precision)) AS min_num_physical_io_reads,
                MAX(CAST(max_num_physical_io_reads AS double precision)) AS max_num_physical_io_reads
        """;

    /// <summary>The raw read's <c>keys</c>, <c>wide</c>, <c>ranked</c> and everything below them: <see cref="QueryStoreTopRawRankedHead"/>, then
    /// <see cref="QueryStoreTopTail"/>. References $1, $4 and $6, plus $2/$3 (#5420: the inline-text fallback's window bound, the same $2/$3
    /// the raw prefix binds). The interval-table read has its own head (<see cref="QueryStoreTopTableRankedHead"/>, #8549).</summary>
    private const string QueryStoreTopSuffix = QueryStoreTopRawRankedHead + "\n" + QueryStoreTopTail;

    /// <summary>
    /// #8549: the raw read's <c>ranked</c>, in two steps so the window function sorts narrow rows, as <see cref="QueryStoreTopTableRankedHead"/>
    /// does for the interval table. One step ranked the groups over a dedupe of <c>SELECT *</c>: the window sorted every row of the
    /// window at its full width, and on a field store that sort wrote 136,690 temp blocks (about 1 GB) and took 10.1 s. <c>deduped</c>
    /// (in <see cref="QueryStoreTopRawPrefix"/>) now carries the group key and the two columns the rank needs, <c>keys</c> ranks and keeps the
    /// candidate limit ($6) with the self-statement slot and the tag filter before it (#7736), and <c>wide</c> dedupes the full rows of
    /// the kept groups only. <c>wide</c> keeps a row's whole dedupe partition or none of it: its filter is on the partition's own columns
    /// (database_name, query_id, plan_id, execution_type_desc, replica_role, matched NULL-safe), all in PARTITION BY, so every partition it
    /// ranks has all the rows it has in <c>deduped</c> and <c>rn</c> picks the same row. <c>ranked</c> then matches each kept row to its group
    /// on all six columns (<see cref="KeyMatch"/>, query_hash included). The rows, their groups and the candidate count are the one-step read's.
    /// </summary>
    private const string QueryStoreTopRawRankedHead = $$"""
        keys AS (
            SELECT
                database_name,
                query_id,
                plan_id,
                query_hash,
                execution_type_desc,
                replica_role
            FROM deduped
            WHERE rn = 1
            /*SELFKEY:deduped*/
            /*TAG:hash:query_hash*/
            GROUP BY database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role
            ORDER BY {{QueryStoreWeightedAverage.RankByDuration}} DESC, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role
            LIMIT $6
        ),
        wide AS (
            /* The same dedupe as deduped above (QueryStoreTopRawPrefix explains it: #1841, #1907), over the rows of the kept groups. */
            SELECT
                *,
                ROW_NUMBER() OVER
                (
                    PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                    ORDER BY collection_time DESC, execution_count DESC
                ) AS rn
            FROM query_store_stats
            WHERE server_id = $1
            {{QueryStoreTopRawWindow}}
            AND   EXISTS
                  (
                      SELECT 1
                      FROM keys AS k
                      WHERE COALESCE(k.database_name, '') = COALESCE(query_store_stats.database_name, '') AND (k.database_name IS NULL) = (query_store_stats.database_name IS NULL)
                      AND   COALESCE(k.query_id, 0) = COALESCE(query_store_stats.query_id, 0) AND (k.query_id IS NULL) = (query_store_stats.query_id IS NULL)
                      AND   COALESCE(k.plan_id, 0) = COALESCE(query_store_stats.plan_id, 0) AND (k.plan_id IS NULL) = (query_store_stats.plan_id IS NULL)
                      AND   COALESCE(k.execution_type_desc, '') = COALESCE(query_store_stats.execution_type_desc, '') AND (k.execution_type_desc IS NULL) = (query_store_stats.execution_type_desc IS NULL)
                      AND   COALESCE(k.replica_role, '') = COALESCE(query_store_stats.replica_role, '') AND (k.replica_role IS NULL) = (query_store_stats.replica_role IS NULL)
                  )
        ),
        ranked AS (
            SELECT
        {{QueryStoreTopRankedColumns}}
            FROM
            (
                SELECT
                    deduped.*
                FROM wide AS deduped
                WHERE deduped.rn = 1
                AND   EXISTS (SELECT 1 FROM keys AS k WHERE {{KeyMatch}})
            ) AS deduped
            GROUP BY database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role
        ),
        """;

    /// <summary>The NULL-safe match of a <c>deduped</c> row to a <c>keys</c> row on the six group columns, written so PostgreSQL can
    /// hash it: <c>IS NOT DISTINCT FROM</c> cannot be a hash condition, so each column is matched as its value under COALESCE plus
    /// whether it is NULL. Equal to <c>IS NOT DISTINCT FROM</c> for every value, a stored empty string and 0 included.</summary>
    private const string KeyMatch =
        "COALESCE(k.database_name, '') = COALESCE(deduped.database_name, '') AND (k.database_name IS NULL) = (deduped.database_name IS NULL)" +
        "\n                  AND   COALESCE(k.query_id, 0) = COALESCE(deduped.query_id, 0) AND (k.query_id IS NULL) = (deduped.query_id IS NULL)" +
        "\n                  AND   COALESCE(k.plan_id, 0) = COALESCE(deduped.plan_id, 0) AND (k.plan_id IS NULL) = (deduped.plan_id IS NULL)" +
        "\n                  AND   COALESCE(k.query_hash, '') = COALESCE(deduped.query_hash, '') AND (k.query_hash IS NULL) = (deduped.query_hash IS NULL)" +
        "\n                  AND   COALESCE(k.execution_type_desc, '') = COALESCE(deduped.execution_type_desc, '') AND (k.execution_type_desc IS NULL) = (deduped.execution_type_desc IS NULL)" +
        "\n                  AND   COALESCE(k.replica_role, '') = COALESCE(deduped.replica_role, '') AND (k.replica_role IS NULL) = (deduped.replica_role IS NULL)";

    /// <summary>
    /// #8549: the interval-table read's <c>ranked</c>, in two steps so the sort carries narrow rows. One step (the raw read's
    /// shape) grouped the table's wide rows: on a field store the Query Store grid over 30 days sorted about 1.29 million
    /// rows per worker, 453 bytes wide, into external merges that wrote 1.8 GB of temp files, for 115,093 groups of which 250
    /// were kept, and took 32.7 s cold. <c>keys</c> groups and ranks on the group key and the columns the rank needs, so its
    /// sort or hash aggregate is narrow, and keeps the candidate limit ($6); the self-statement slot and the tag filter stay
    /// there, before that limit (#7736). <c>ranked</c> then reads the same rows again and aggregates only the groups <c>keys</c>
    /// kept. The rows, their keys and the candidate count are the one-step read's: <c>keys</c> ranks by the same expression and
    /// tie-breaks, and the match to a kept key is NULL-safe (<see cref="KeyMatch"/>), because replica_role is NULL off an
    /// availability group. <c>deduped</c> is <c>NOT MATERIALIZED</c> in <see cref="QueryStoreTopTablePrefix"/>: this reads it
    /// twice, and a materialized copy of every wide row is the temp file this exists to avoid.
    /// </summary>
    private const string QueryStoreTopTableKeysCte = $$"""
        keys AS (
            SELECT
                database_name,
                query_id,
                plan_id,
                query_hash,
                execution_type_desc,
                replica_role,
                {{QueryStoreWeightedAverage.RankByDuration}} AS rank_cost
            FROM deduped
            WHERE rn = 1
            /*SELFKEY:deduped*/
            /*TAG:hash:query_hash*/
            GROUP BY database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role
            ORDER BY {{QueryStoreWeightedAverage.RankByDuration}} DESC, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role
            LIMIT $6
        )
        """;

    /// <summary>
    /// The <c>ranked</c> CTE of the interval-table read, with no trailing comma. Both the one-statement read (<see cref="QueryStoreTopTableSql"/>)
    /// and the two-statement read (<see cref="QueryStoreTopTableByKeysSql"/>) use it as it is.
    /// </summary>
    private const string QueryStoreTopTableRankedCte = $$"""
        ranked AS (
            SELECT
        {{QueryStoreTopRankedColumns}}
            FROM
            (
                /* #8549: the kept keys' rows are fetched by key, not found by reading the window again. A semi-join on KeyMatch alone
                   could only hash, so this pass read every row of the window a second time: on a seeded store of 1.73 million intervals it
                   was 5.7 s of a 10.3 s read, and on a cold field store it doubled the wide heap pages the first read loads. The two plain
                   equalities in front of KeyMatch give the planner a second way to run the same semi-join: de-duplicate the kept keys (250
                   narrow rows) and probe the (server_id, database_name, query_id, first_execution_time) index once per key. KeyMatch stays
                   the exact NULL-safe test. It is a semi-join and not a join on purpose: with no such index (it is built in the background)
                   the planner keeps hashing the narrow keys against one scan of the window, as it did before. An inner join on the same
                   equalities let it hash the WIDE rows instead on a store without the index, which wrote the temp files the narrow ranking
                   exists to avoid (QueryStoreGridNarrowRankLiveTests). A key with a NULL database_name or query_id cannot match a plain
                   equality, so the second branch reads the window for those keys only, and only when such a key was kept (the uncorrelated
                   EXISTS runs once, before the scan). A row matches at most one branch, so none is read twice. */
                SELECT
                    deduped.*
                FROM deduped
                WHERE EXISTS (SELECT 1 FROM keys AS k WHERE k.database_name = deduped.database_name AND k.query_id = deduped.query_id AND {{KeyMatch}})
                UNION ALL
                SELECT
                    deduped.*
                FROM deduped
                WHERE EXISTS (SELECT 1 FROM keys AS n WHERE n.database_name IS NULL OR n.query_id IS NULL)
                AND   EXISTS (SELECT 1 FROM keys AS k WHERE (k.database_name IS NULL OR k.query_id IS NULL) AND {{KeyMatch}})
            ) AS deduped
            WHERE rn = 1
            GROUP BY database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role
        )
        """;

    /// <summary>The one-statement <c>keys</c> and <c>ranked</c>, the pair <see cref="QueryStoreTopTableSql"/> reads.</summary>
    private const string QueryStoreTopTableRankedHead = QueryStoreTopTableKeysCte + ",\n" + QueryStoreTopTableRankedCte + ",\n";

    /// <summary>The page and its candidate count, from <c>page AS (</c> down, shared by the raw and the interval-table reads.</summary>
    private const string QueryStoreTopTail = $$"""
        page AS (
        SELECT
            p.database_name,
            p.query_id,
            p.plan_id,
            p.query_hash,
            p.query_text,
            p.module_name,
            p.total_executions,
            p.avg_duration_ms,
            p.avg_cpu_time_ms,
            p.avg_logical_reads,
            p.avg_logical_writes,
            p.avg_physical_reads,
            p.avg_rowcount,
            p.min_dop,
            p.max_dop,
            p.last_execution_time,
            p.query_plan_hash,
            p.is_forced_plan,
            p.plan_forcing_type,
            p.execution_type_desc,
            p.first_execution_time,
            p.avg_clr_time_ms,
            p.avg_tempdb_space_used,
            p.avg_log_bytes_used,
            p.plan_type,
            p.force_failure_count,
            p.last_force_failure_reason,
            p.compatibility_level,
            p.min_duration_ms,
            p.max_duration_ms,
            p.min_cpu_time_ms,
            p.max_cpu_time_ms,
            p.min_logical_reads,
            p.max_logical_reads,
            p.min_logical_writes,
            p.max_logical_writes,
            p.min_physical_reads,
            p.max_physical_reads,
            p.min_clr_time_ms,
            p.max_clr_time_ms,
            p.min_rowcount,
            p.max_rowcount,
            p.min_log_bytes_used,
            p.max_log_bytes_used,
            p.min_tempdb_space_used,
            p.max_tempdb_space_used,
            p.avg_memory_mb,
            p.min_memory_mb,
            p.max_memory_mb,
            p.avg_num_physical_io_reads,
            p.min_num_physical_io_reads,
            p.max_num_physical_io_reads,
            p.replica_role,
            ROW_NUMBER() OVER (ORDER BY p.sort_cost DESC, p.database_name, p.query_id, p.plan_id, p.query_hash, p.execution_type_desc, p.replica_role) AS page_ord
        FROM (
            /* #5815: the candidates are sorted BEFORE the text lateral and the page keeps that order, so the
               LIMIT $4 below stops the lateral at the row that fills the page, and one run at the fill's final
               candidate limit (TopFill.FinalCandidates) looks up no more text than the first round did. The
               order is page_ord's own, so the rows, their order and page_ord are unchanged. sort_cost is the
               page_ord sort expression, projected so the outer ORDER BY matches the inner sort and the planner
               adds no second sort above the lateral. Twins the MCP reader's QueryStoreTopTail; keep them matching. */
            SELECT r.*, t.query_text
            FROM (
                SELECT k.*, k.total_executions * k.avg_duration_ms AS sort_cost
                FROM ranked AS k
                ORDER BY k.total_executions * k.avg_duration_ms DESC, k.database_name, k.query_id, k.plan_id, k.query_hash, k.execution_type_desc, k.replica_role
            ) AS r
            /* #2150: resolve the text inside the lateral, so the projection above and the WAITFOR and self-statement
               tests below all read one t.query_text without knowing where it came from.
               First arm: collect.query_store_text, one row per (server, database, query_id), which is where the
               collector lands text once the separate fetch is on. Second arm: the newest inline query_text on
               the fact row, which is where text lived BEFORE the cutover. It stays because dropping it would
               blank the text on all existing history. OFFSET 0 keeps the lookup a subquery of its own, run once
               for each candidate the LIMIT reaches. Without it the planner copies the lookup into the projection
               and into each text test, and runs every copy. */
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                           (
                               SELECT x.query_sql_text
                               FROM query_store_text AS x
                               WHERE x.server_id = $1
                               AND   x.database_name = r.database_name
                               AND   x.query_id = r.query_id
                           ),
                           (
                               SELECT s.query_text
                               FROM query_store_stats AS s
                               WHERE s.server_id = $1
                               AND   s.query_id = r.query_id
                               AND   s.database_name = r.database_name
                               /* #5420: bounded to the read's own window ($2 through $3). Without it a query with no inline text anywhere walked every
                                  retained chunk of query_store_stats on the time index looking for one. A query whose
                                  only inline text is older than the window now shows none, as a query with no text
                                  at all already did. #7776: the ceiling is the one the fact rows above are read with (CollectionCeilingSql), not the
                                  bare $3, because the fact rows are windowed by interval start and an interval started in the window
                                  can be collected after $3; a bare $3 left its inline-only text blank. The floor stays $2: a fact row's
                                  collection is never before its interval's start, and text only older than the window shows none. The Darling MCP copy of this tail (DarlingDataReader) keeps the bare window with its own
                                  collection_time window (#7814 is the twin). */
                               AND   s.collection_time > $2
                               AND   ($3::timestamp IS NULL OR s.collection_time <= $3 + {{CollectionCeilingSql}})
                               AND   s.query_text IS NOT NULL
                               ORDER BY s.collection_time DESC, s.collection_id DESC
                               LIMIT 1
                           )
                       ) AS query_text
                OFFSET 0
            ) AS t ON TRUE
            /* #6665: the monitor's own statement is left out here too, before the LIMIT, by the shared read-side predicate
               (QueryStoreSelfStatement): on Darling its row is stored, because the collector's drop tests an inline text column
               that the separate text fetch leaves empty, and the marker is only in the text resolved above. */
            WHERE (t.query_text IS NULL OR t.query_text NOT LIKE 'WAITFOR%')
            AND   {{QueryStoreSelfStatement.NotSelfOfLateralText}}
            ORDER BY r.sort_cost DESC, r.database_name, r.query_id, r.plan_id, r.query_hash, r.execution_type_desc, r.replica_role
            LIMIT $4
        ) AS p
        ORDER BY page_ord
        )
        /* #5313: the candidate count rides on its own row, joined to the page, so a round whose candidates were
           ALL trimmed (every one a WAITFOR statement) still reports it; an empty page used to read as
           exhausted. page_ord is NULL on that row, which is how the caller tells it from a page row. */
        SELECT p.*, c.candidate_count
        FROM (SELECT COUNT(*) AS candidate_count FROM ranked) AS c
        LEFT JOIN page AS p ON TRUE
        ORDER BY p.page_ord
        """;

    /// <summary>
    /// The table twin of <see cref="QueryStoreTopSql"/> (#3953): <see cref="QueryStoreTopTablePrefix"/> reading
    /// <c>query_store_interval_wide</c> instead of the raw dedupe, with the two-step <see cref="QueryStoreTopTableRankedHead"/> (#8549)
    /// and the <see cref="QueryStoreTopTail"/> the raw read shares, so the two reads cannot drift below <c>ranked</c>.
    /// Chosen per call by <see cref="QueryStoreIntervalWide.ReadsTableAsync"/>; never used for <see cref="QueryStoreComparisonSql"/>.
    /// $1 server_id, $2 the gate's clamp (<c>max(window start, raw's chunk floor)</c>), $3 window end (naive
    /// UTC, NULL for an open/preset end), $4 top, $5 database filter.
    /// <para>A static readonly (#4605), declared AFTER <see cref="QueryStoreTopTablePrefix"/> on purpose: static
    /// initializers run in textual order, so declared above it this would concatenate a null prefix.</para>
    /// </summary>
    public static readonly string QueryStoreTopTableSql = WithoutByKeysFilter(QueryStoreTopTablePrefix) + QueryStoreTopTableRankedHead + "\n" + QueryStoreTopTail;

    /// <summary>
    /// #8549: the first statement of the grid's table read: the kept keys, one row per key, ranked and limited exactly as the one-statement
    /// read's <c>keys</c> is (<see cref="QueryStoreTopTableKeysCte"/>, with the self-statement slot and the tag filter before the limit).
    /// $1 server_id, $2 the read start, $3 window end (NULL for an open end), $4 unused, $5 database filter, $6 the candidate limit.
    /// </summary>
    public static readonly string QueryStoreTopTableKeysSql = TableKeysSql(QueryStoreTopTablePrefix);

    /// <summary>
    /// <see cref="QueryStoreTopTableKeysSql"/> over <see cref="QueryStoreTopTableSplitPrefix"/> (#9283), chosen per call when
    /// <see cref="QueryStoreBackgroundIndexes.WideNullStartFirstExec"/> is built and no closed-day run is served; same parameters, rows and order.
    /// </summary>
    public static readonly string QueryStoreTopTableSplitKeysSql = TableKeysSql(QueryStoreTopTableSplitPrefix);

    private static string TableKeysSql(string prefix) => WithoutByKeysFilter(prefix) + QueryStoreTopTableKeysCte + "\n"
        + "SELECT database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role FROM keys\n"
        + "ORDER BY rank_cost DESC, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role";

    /// <summary>
    /// #8549: the grid's table read with the kept keys bound as arrays (<c>unnest</c>), run in the same transaction as
    /// <see cref="QueryStoreTopTableKeysSql"/> so both see one snapshot. The per-key join is the one-statement read's (the <c>EXISTS</c> on
    /// <c>keys</c> with <see cref="KeyMatch"/>), because bounding the first branch with <c>ANY</c> arrays sent the planner to the unique
    /// identity index with a 1 to 3 row estimate per leaf (about 3,500 to 4,000 rows actual): on a field store 61,611 reads, 18.2 s. The
    /// caller runs this statement with index scans off, so each leaf is a Bitmap Heap Scan over the db_query_first_exec index per key.
    /// $1 server_id, $2 the read start, $3 window end, $4 top, $5 database filter, $6 to $11 the keys' six columns as parallel arrays
    /// (database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role), $12 the kept keys' distinct database names and $13 their
    /// distinct query ids (#9373, <see cref="WithByKeysFilter"/>), each NULL when a kept key has a NULL in either of the two columns.
    /// </summary>
    public static readonly string QueryStoreTopTableByKeysSql = TableByKeysSql(QueryStoreTopTablePrefix);

    /// <summary>
    /// <see cref="QueryStoreTopTableByKeysSql"/> over <see cref="QueryStoreTopTableSplitPrefix"/> (#9283), chosen per call with
    /// <see cref="QueryStoreTopTableSplitKeysSql"/>; same parameters, rows and order.
    /// </summary>
    public static readonly string QueryStoreTopTableSplitByKeysSql = TableByKeysSql(QueryStoreTopTableSplitPrefix);

    private static string TableByKeysSql(string prefix) => WithByKeysFilter(prefix, TableByKeysFilterParameter, TableByKeysFilterParameter + 1)
        + "keys AS (\n"
        + "    SELECT *\n"
        + "    FROM unnest($6::text[], $7::bigint[], $8::bigint[], $9::text[], $10::text[], $11::text[])\n"
        + "         AS k(database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role)\n"
        + "),\n"
        + QueryStoreTopTableRankedCte
        + ",\n" + QueryStoreTopTail;

    /// <summary>
    /// #9111: the grid's <c>deduped</c> when a run of whole built UTC days lies inside the window. Three arms stacked: the interval table
    /// from the read start up to the run, the interval table from the end of the run to the window's end, and
    /// <c>query_store_grid_daily</c> for the run itself. Every arm projects the grid's group columns and the same recombinable parts
    /// (<see cref="QueryStoreClosedDayRollup.GridValueColumns"/>): the execution count, each weighted average as its product
    /// <c>avg_x * execution_count</c>, each minimum and maximum, and the plan-state columns. The aggregates above it are the
    /// interval-table read's own with each weighted product read from its column (<see cref="OverRollupParts"/>), so a day comes out the
    /// same from either source. The interval arms carry the interval-table read's window and <c>first_execution_time</c> bounds with the
    /// run's edge in place of the window's, so the partitions of the run's days are not read. <paramref name="runStart"/> and
    /// <paramref name="runEnd"/> are the parameters holding the run's first midnight and the midnight after its last day.
    /// </summary>
    private static string QueryStoreTopRollupPrefix(string runStart, string runEnd) => $$"""
        WITH deduped AS NOT MATERIALIZED (
            SELECT
                {{QueryStoreClosedDayRollup.GridKeyColumns}},
                {{QueryStoreClosedDayRollup.GridValueColumnsFromInterval}},
                1 AS rn
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   COALESCE(interval_start_time_utc, collection_time) >= $2
            AND   COALESCE(interval_start_time_utc, collection_time) < {{runStart}}::timestamp
            AND   first_execution_time >= $2 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time <= {{runStart}}::timestamp + {{QueryStoreIntervalWide.IntervalStartFirstExecMarginSql}}
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
            {{ByKeysSlot}}
            UNION ALL
            SELECT
                {{QueryStoreClosedDayRollup.GridKeyColumns}},
                {{QueryStoreClosedDayRollup.GridValueColumnsFromInterval}},
                1 AS rn
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   COALESCE(interval_start_time_utc, collection_time) >= {{runEnd}}::timestamp
            AND   ($3::timestamp IS NULL OR COALESCE(interval_start_time_utc, collection_time) < $3)
            AND   first_execution_time >= {{runEnd}}::timestamp - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   ($3::timestamp IS NULL OR first_execution_time <= $3 + {{QueryStoreIntervalWide.IntervalStartFirstExecMarginSql}})
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
            {{ByKeysSlot}}
            UNION ALL
            SELECT
                {{QueryStoreClosedDayRollup.GridKeyColumns}},
                {{QueryStoreClosedDayRollup.GridValueColumns}},
                1 AS rn
            FROM query_store_grid_daily
            WHERE server_id = $1
            AND   day_utc >= CAST({{runStart}}::timestamp AS date)
            AND   day_utc < CAST({{runEnd}}::timestamp AS date)
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
            {{ByKeysSlot}}
        ),

        """;

    /// <summary>#9111: <paramref name="sql"/> with every weighted product <c>SUM(CAST(avg_x AS double precision) * execution_count)</c> read
    /// from the column <see cref="QueryStoreTopRollupPrefix"/> projects it as, <c>SUM(avg_x_wsum)</c>. Nothing else changes, so the
    /// rollup read ranks and aggregates by the interval-table read's own text.</summary>
    private static string OverRollupParts(string sql) =>
        System.Text.RegularExpressions.Regex.Replace(
            sql, @"SUM\(CAST\((avg_\w+) AS double precision\) \* execution_count\)", "SUM(${1}_wsum)", System.Text.RegularExpressions.RegexOptions.CultureInvariant);

    /// <summary>
    /// #9111: <see cref="QueryStoreTopTableKeysSql"/> over <see cref="QueryStoreTopRollupPrefix"/>. $1 to $6 as that statement's, $7 the
    /// first midnight of the built run, $8 the midnight after its last day.
    /// </summary>
    public static readonly string QueryStoreTopRollupKeysSql = WithoutByKeysFilter(QueryStoreTopRollupPrefix("$7", "$8")) + OverRollupParts(QueryStoreTopTableKeysCte) + "\n"
        + "SELECT database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role FROM keys\n"
        + "ORDER BY rank_cost DESC, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role";

    /// <summary>
    /// #9111: <see cref="QueryStoreTopTableByKeysSql"/> over <see cref="QueryStoreTopRollupPrefix"/>. $1 to $11 as that statement's, $12 the
    /// first midnight of the built run, $13 the midnight after its last day, $14 and $15 the kept keys' distinct database names and query ids (#9373).
    /// </summary>
    public static readonly string QueryStoreTopRollupByKeysSql = WithByKeysFilter(QueryStoreTopRollupPrefix("$12", "$13"), RollupByKeysFilterParameter, RollupByKeysFilterParameter + 1)
        + "keys AS (\n"
        + "    SELECT *\n"
        + "    FROM unnest($6::text[], $7::bigint[], $8::bigint[], $9::text[], $10::text[], $11::text[])\n"
        + "         AS k(database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role)\n"
        + "),\n"
        + OverRollupParts(QueryStoreTopTableRankedCte)
        + ",\n" + QueryStoreTopTail;

    /// <summary>
    /// <see cref="QueryStoreTopRollupPrefix"/> with each interval-table arm as two (#9287), for a store whose
    /// <see cref="QueryStoreBackgroundIndexes.WideNullStartFirstExec"/> is built, the way <see cref="QueryStoreTopTableSplitPrefix"/> does it for
    /// the one-arm window (#9283). Rows with an interval start take the one hour <c>first_execution_time</c> floor
    /// (<see cref="QueryStoreIntervalWide.IntervalStartSlackSql"/>, QueryStoreIntervalWide.cs:885-894 gives the argument), rows without one keep
    /// <see cref="QueryStoreIntervalWide.PurgeEdgeMarginSql"/> and read through the partial index, and their upper bound stays
    /// <see cref="QueryStoreIntervalWide.IntervalStartFirstExecMarginSql"/>. Measured cold, three day partitions returned no rows and cost 37 to 38
    /// percent of each statement: the floor day before the window (the 26 hour floor), the first rollup day's own partition (reached by the margin
    /// arm's 25 hour upper slack) and the last rollup day on the open arm (its 26 hour floor).
    /// <para><paramref name="startUpperSlackSql"/> is the upper slack of the two arms with an interval start, an interval literal. In the margin arm
    /// such a row has <c>interval_start_time_utc &lt; runStart</c>, and its <c>first_execution_time</c> is an end time inside its interval
    /// (QueryStoreIntervalWide.cs:885-894), so never after <c>interval_start_time_utc + span</c>: <c>first_execution_time &lt; runStart + longest span</c>.
    /// The bound is that span plus the hour of slack the floor uses. The open arm's row has <c>interval_start_time_utc &lt; $3</c> when $3 is set, so
    /// <c>first_execution_time &lt; $3 + longest span</c> by the same argument, and a NULL $3 has no bound. The span is
    /// <see cref="QueryStoreIntervalWide.ResolveLongestIntervalSpanAsync"/>'s (QueryStoreIntervalWide.cs:810-828), a day where it cannot be trusted,
    /// which makes the slack the 25 hours the arms had.</para>
    /// </summary>
    private static string QueryStoreTopRollupSplitPrefix(string runStart, string runEnd, string startUpperSlackSql) => $$"""
        WITH deduped AS NOT MATERIALIZED (
            SELECT
                {{QueryStoreClosedDayRollup.GridKeyColumns}},
                {{QueryStoreClosedDayRollup.GridValueColumnsFromInterval}},
                1 AS rn
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   interval_start_time_utc IS NOT NULL
            AND   interval_start_time_utc >= $2
            AND   interval_start_time_utc < {{runStart}}::timestamp
            AND   first_execution_time >= $2 - {{QueryStoreIntervalWide.IntervalStartSlackSql}}
            AND   first_execution_time <= {{runStart}}::timestamp + {{startUpperSlackSql}}
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
            {{ByKeysSlot}}
            UNION ALL
            SELECT
                {{QueryStoreClosedDayRollup.GridKeyColumns}},
                {{QueryStoreClosedDayRollup.GridValueColumnsFromInterval}},
                1 AS rn
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   interval_start_time_utc IS NULL
            AND   collection_time >= $2
            AND   collection_time < {{runStart}}::timestamp
            AND   first_execution_time >= $2 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time <= {{runStart}}::timestamp + {{QueryStoreIntervalWide.IntervalStartFirstExecMarginSql}}
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
            {{ByKeysSlot}}
            UNION ALL
            SELECT
                {{QueryStoreClosedDayRollup.GridKeyColumns}},
                {{QueryStoreClosedDayRollup.GridValueColumnsFromInterval}},
                1 AS rn
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   interval_start_time_utc IS NOT NULL
            AND   interval_start_time_utc >= {{runEnd}}::timestamp
            AND   ($3::timestamp IS NULL OR interval_start_time_utc < $3)
            AND   first_execution_time >= {{runEnd}}::timestamp - {{QueryStoreIntervalWide.IntervalStartSlackSql}}
            AND   ($3::timestamp IS NULL OR first_execution_time <= $3 + {{startUpperSlackSql}})
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
            {{ByKeysSlot}}
            UNION ALL
            SELECT
                {{QueryStoreClosedDayRollup.GridKeyColumns}},
                {{QueryStoreClosedDayRollup.GridValueColumnsFromInterval}},
                1 AS rn
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   interval_start_time_utc IS NULL
            AND   collection_time >= {{runEnd}}::timestamp
            AND   ($3::timestamp IS NULL OR collection_time < $3)
            AND   first_execution_time >= {{runEnd}}::timestamp - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   ($3::timestamp IS NULL OR first_execution_time <= $3 + {{QueryStoreIntervalWide.IntervalStartFirstExecMarginSql}})
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
            {{ByKeysSlot}}
            UNION ALL
            SELECT
                {{QueryStoreClosedDayRollup.GridKeyColumns}},
                {{QueryStoreClosedDayRollup.GridValueColumns}},
                1 AS rn
            FROM query_store_grid_daily
            WHERE server_id = $1
            AND   day_utc >= CAST({{runStart}}::timestamp AS date)
            AND   day_utc < CAST({{runEnd}}::timestamp AS date)
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
            {{ByKeysSlot}}
        ),

        """;

    /// <summary>#9287: <see cref="QueryStoreTopRollupKeysSql"/> over <see cref="QueryStoreTopRollupSplitPrefix"/>; same parameters, rows and order.
    /// <paramref name="startUpperSlackSql"/> is the upper slack of the arms with an interval start.</summary>
    internal static string QueryStoreTopRollupSplitKeysSqlFor(string startUpperSlackSql) =>
        WithoutByKeysFilter(QueryStoreTopRollupSplitPrefix("$7", "$8", startUpperSlackSql)) + OverRollupParts(QueryStoreTopTableKeysCte) + "\n"
        + "SELECT database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role FROM keys\n"
        + "ORDER BY rank_cost DESC, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role";

    /// <summary>#9287: <see cref="QueryStoreTopRollupByKeysSql"/> over <see cref="QueryStoreTopRollupSplitPrefix"/>; same parameters, rows and order.</summary>
    internal static string QueryStoreTopRollupSplitByKeysSqlFor(string startUpperSlackSql) =>
        WithByKeysFilter(QueryStoreTopRollupSplitPrefix("$12", "$13", startUpperSlackSql), RollupByKeysFilterParameter, RollupByKeysFilterParameter + 1)
        + "keys AS (\n"
        + "    SELECT *\n"
        + "    FROM unnest($6::text[], $7::bigint[], $8::bigint[], $9::text[], $10::text[], $11::text[])\n"
        + "         AS k(database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role)\n"
        + "),\n"
        + OverRollupParts(QueryStoreTopTableRankedCte)
        + ",\n" + QueryStoreTopTail;

    /// <summary>
    /// #9287: the two statements of a read served from the closed-day rollup. With <paramref name="splitBuilt"/> (the partial index of
    /// <see cref="QueryStoreBackgroundIndexes.WideNullStartFirstExec"/> is built) the split pair, whose arms with an interval start are bounded above
    /// by <paramref name="longestSpan"/> plus an hour (<see cref="QueryStoreIntervalWide.ResolveEdgeMarginAndLongestSpanAsync"/>, a day where it cannot be
    /// trusted). Without the index, <see cref="QueryStoreTopRollupKeysSql"/> and <see cref="QueryStoreTopRollupByKeysSql"/>, as before.
    /// </summary>
    internal static (string KeysSql, string TopSql) QueryStoreTopRollupStatements(bool splitBuilt, TimeSpan longestSpan)
    {
        if (!splitBuilt)
        {
            return (QueryStoreTopRollupKeysSql, QueryStoreTopRollupByKeysSql);
        }

        var slackSql = QueryStoreIntervalWide.EdgeMarginSqlFor(longestSpan + QueryStoreIntervalWide.IntervalStartSlack);
        return (QueryStoreTopRollupSplitKeysSqlFor(slackSql), QueryStoreTopRollupSplitByKeysSqlFor(slackSql));
    }

    /// <summary>The two parameters a rollup read adds after its statement's own: the built run's first midnight and the midnight after
    /// its last day.</summary>
    private static void AddServedDaysParameters(Npgsql.NpgsqlCommand command, QueryStoreClosedDayRollup.ServedDays served)
    {
        command.Parameters.Add(new Npgsql.NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(served.Start, DateTimeKind.Unspecified) });
        command.Parameters.Add(new Npgsql.NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(served.End, DateTimeKind.Unspecified) });
    }

    /// <summary>#9111: the built run of whole UTC days this read takes from the closed-day rollups, or null to read the interval table
    /// alone: a store below the rollups' version, a server with no built day in the window, or one whose builds have stopped. #9287: the
    /// run is clipped to the days wholly inside retention on the store's clock, and is not cut to the days the interval table still holds
    /// whole. A day near the retention edge comes from its rollup as collected, and the same day read from the interval table (the fault
    /// fallback, or a day with no rollup) can lack the rows the purge dropped early.</summary>
    private async Task<(QueryStoreClosedDayRollup.ServedDays? Served, QueryStoreClosedDayRollup.SourceReason Reason, IReadOnlyList<DateOnly> BuiltDays)> ResolveServedDaysAsync(
        Npgsql.NpgsqlConnection connection, Npgsql.NpgsqlTransaction transaction, int serverId, DateTime readStart, DateTime end, CancellationToken cancellationToken)
    {
        if (_cachedStoreSchemaVersion is not int version || version < QueryStoreClosedDayRollup.Version)
        {
            return (null, QueryStoreClosedDayRollup.SourceReason.Unavailable, Array.Empty<DateOnly>());
        }

        return await QueryStoreClosedDayRollup.ResolveServedDaysWithReasonAsync(
            connection, transaction, serverId, readStart, end, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
    }

    /// <summary>
    /// The store schema version <see cref="GetQueryStoreTopQueriesAsync"/> (the grid) requires before it will
    /// even attempt <c>query_store_interval_wide</c> (#3953 B4): must equal the version of the
    /// <c>PgMigrations</c> migration that creates the table. The slicer (#8549) reads the same constant before it routes to the
    /// table. B3a's separate <see cref="PerformanceMonitor.Darling.Service.Mcp.DarlingDataReader"/>
    /// constant for the MCP top read is pinned the same way but stays its own symbol: that surface compares the
    /// compiled <see cref="PerformanceMonitor.Darling.Storage.StorageVersion.SchemaVersion"/> instead of probing
    /// the store live, for the reason documented on that constant.
    /// </summary>
    private const int QueryStoreIntervalWideMinSchemaVersion = 145;

    /// <summary>
    /// The top Query Store queries for one server over [<paramref name="startUtc"/>, <paramref name="endUtc"/>],
    /// pre-sorted by total duration descending (the grid's default sort). #3953: reads
    /// <c>query_store_interval_wide</c> when <see cref="QueryStoreIntervalWide.ReadsTableAsync"/> says its
    /// coverage holds the window; any fault or a "no" reads <see cref="QueryStoreTopSql"/> unchanged, exactly as
    /// before this table existed. <paramref name="literalEndUtc"/> is the gate's clause-4 input: NULL for an open
    /// end (a WPF preset — the caller means "through now", not through this exact instant), or
    /// <paramref name="endUtc"/> itself for a custom range or an MCP <c>as_of</c>.
    /// <paramref name="endUtc"/> alone still binds the raw read's own $3 and the gate's minimum-window clause.
    /// </summary>
    public async Task<List<ViewerQueryStoreRow>> GetQueryStoreTopQueriesAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int top = TopQueriesPageSize, IReadOnlyList<string>? databaseNames = null,
        DateTime? literalEndUtc = null, QueryTagFilter? tags = null, CancellationToken cancellationToken = default)
        => (await GetQueryStoreTopQueriesWithReachAsync(serverId, startUtc, endUtc, top, databaseNames, literalEndUtc, tags, cancellationToken)).Rows;

    /// <summary>
    /// <see cref="GetQueryStoreTopQueriesAsync"/> plus the read plan when the interval table served
    /// (#4689): the plan's <see cref="QueryStoreIntervalWide.WideReadPlan.ReadStart"/> is the lower bound the
    /// table read bound, which reaches below raw's chunk floor, so the grid banner can name where the rows start
    /// and why. The plan is null when the read was raw.
    /// </summary>
    public async Task<(List<ViewerQueryStoreRow> Rows, QueryStoreIntervalWide.WideReadPlan? Plan)> GetQueryStoreTopQueriesWithReachAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int top = TopQueriesPageSize, IReadOnlyList<string>? databaseNames = null,
        DateTime? literalEndUtc = null, QueryTagFilter? tags = null, CancellationToken cancellationToken = default)
    {
        var read = await GetQueryStoreTopQueriesUntaggedAsync(serverId, startUtc, endUtc, top, databaseNames, literalEndUtc, tags, cancellationToken);
        await LabelTagsAsync(serverId, QueryTagReadJoin.HashKind, read.Rows, r => r.QueryHash, cancellationToken);
        return read;
    }

    private async Task<(List<ViewerQueryStoreRow> Rows, QueryStoreIntervalWide.WideReadPlan? Plan)> GetQueryStoreTopQueriesUntaggedAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int top, IReadOnlyList<string>? databaseNames,
        DateTime? literalEndUtc, QueryTagFilter? tags, CancellationToken cancellationToken)
    {
        /* Review D4R H1: the window check comes FIRST, before the schema probe (a 121-column
           EXISTS catalog query) and before TryGetQueryStoreTopQueriesFromTableAsync (a second
           connection, a transaction, and the gate's own round trips including the unindexed table
           floor scan). A window under GridWideMinWindow can only ever read raw (clause 5), so it must
           reach raw with ZERO extra store round trips versus before this table existed. */
        if (endUtc - startUtc >= QueryStoreIntervalWide.GridWideMinWindow)
        {
            var schemaVersion = _cachedStoreSchemaVersion ??= await GetStoreSchemaVersionAsync(cancellationToken);
            if (schemaVersion is int version && version >= QueryStoreIntervalWideMinSchemaVersion)
            {
                var tableRead = await TryGetQueryStoreTopQueriesFromTableAsync(
                    serverId, startUtc, endUtc, literalEndUtc, top, databaseNames, tags, cancellationToken);
                if (tableRead is { } served)
                {
                    return (served.Rows, served.Plan);
                }
            }
        }

        /* #5313: the WAITFOR trim can leave the page short, so the candidate limit ($6) runs past top. #5815: one run
           at TopFill's final limit, not up to three rounds that each re-read the window; the tail looks up text only
           until the page is full, so the answer is the rounds' own (TopFill.FinalCandidates). */
        List<ViewerQueryStoreRow> rows;
        /* #7736: the monitor's own statements leave ranked before its candidate limit once the key backfill has run, so the gate and
           the read are one snapshot: one read-only REPEATABLE READ transaction, as the interval-table read below runs in. */
        await using (var connection = await _dataSource.OpenConnectionAsync(ViewerReadScope.Effective(cancellationToken)))
        await using (var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken))
        {
            await using (var readOnly = new Npgsql.NpgsqlCommand("SET TRANSACTION READ ONLY", connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds })
            {
                await readOnly.ExecuteNonQueryAsync(cancellationToken);
            }

            var rawSql = await WithSelfKeysBeforeCandidateLimitAsync(connection, transaction, QueryTagReadJoin.Apply(QueryStoreTopSql, serverId, tags), cancellationToken);
            await using var command = new Npgsql.NpgsqlCommand(rawSql, connection, transaction);
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            AddServerWindowParameters(command, serverId, startUtc, endUtc);
            command.Parameters.Add(new Npgsql.NpgsqlParameter<int> { TypedValue = top });
            command.Parameters.Add(DatabaseFilterParameter(databaseNames));
            command.Parameters.Add(new Npgsql.NpgsqlParameter<int> { TypedValue = TopFill.FinalCandidates(top) });
            await using var reader = await command.ExecuteReaderAsync(ViewerReadScope.Effective(cancellationToken));
            (rows, _) = await TopFill.ReadPageAsync(reader, QueryStorePageOrdinal, ReadQueryStoreTopRow, cancellationToken);
        }

        return (rows, null);
    }

    /// <summary>#7736: <paramref name="sql"/> with the self-statement test on the key table in <c>ranked</c>, ahead of its candidate limit,
    /// once its backfill has run (<see cref="QueryStoreSelfKeys.ReadyAsync"/>, read in this read's snapshot, closed on a store without
    /// the state table), else as it is. The tail's text test still removes the monitor's own statements from a store the backfill has
    /// not reached. The MCP read's <c>DarlingDataReader.WithSelfKeysBeforeCandidateLimitAsync</c> is the same shape.</summary>
    private static async Task<string> WithSelfKeysBeforeCandidateLimitAsync(Npgsql.NpgsqlConnection connection, Npgsql.NpgsqlTransaction transaction, string sql, CancellationToken cancellationToken) =>
        await QueryStoreSelfKeys.ReadyAsync(connection, transaction, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken)
            ? QueryStoreSelfStatement.WithRankedKeyTest(sql)
            : sql;

    /// <summary>The first five parameters both statements of the grid's table read share ($1 to $5).</summary>
    private static void AddTableReadParameters(
        Npgsql.NpgsqlCommand command, int serverId, DateTime readStart, DateTime? literalEndUtc, int top, IReadOnlyList<string>? databaseNames)
    {
        command.Parameters.Add(new Npgsql.NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new Npgsql.NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(readStart, DateTimeKind.Unspecified) });
        command.Parameters.Add(new Npgsql.NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp,
            Value = literalEndUtc.HasValue ? DateTime.SpecifyKind(literalEndUtc.Value, DateTimeKind.Unspecified) : DBNull.Value,
        });
        command.Parameters.Add(new Npgsql.NpgsqlParameter<int> { TypedValue = top });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
    }

    /// <summary>The kept keys of <see cref="QueryStoreTopTableKeysSql"/>, held to bind as the arrays of <see cref="QueryStoreTopTableByKeysSql"/>.</summary>
    internal sealed class TableReadKeys
    {
        private readonly List<string?> _databaseNames = new();
        private readonly List<long?> _queryIds = new();
        private readonly List<long?> _planIds = new();
        private readonly List<string?> _queryHashes = new();
        private readonly List<string?> _executionTypes = new();
        private readonly List<string?> _replicaRoles = new();

        internal void Add(System.Data.Common.DbDataReader reader)
        {
            _databaseNames.Add(reader.IsDBNull(0) ? null : reader.GetString(0));
            _queryIds.Add(reader.IsDBNull(1) ? null : reader.GetInt64(1));
            _planIds.Add(reader.IsDBNull(2) ? null : reader.GetInt64(2));
            _queryHashes.Add(reader.IsDBNull(3) ? null : reader.GetString(3));
            _executionTypes.Add(reader.IsDBNull(4) ? null : reader.GetString(4));
            _replicaRoles.Add(reader.IsDBNull(5) ? null : reader.GetString(5));
        }

        /// <summary>How many keys are held.</summary>
        internal int Count => _databaseNames.Count;

        /// <summary>The first <paramref name="count"/> keys in the order they were added (rank order, from <see cref="QueryStoreTopTableKeysSql"/>).</summary>
        internal TableReadKeys First(int count)
        {
            var first = new TableReadKeys();
            var take = Math.Min(count, Count);
            first._databaseNames.AddRange(_databaseNames.Take(take));
            first._queryIds.AddRange(_queryIds.Take(take));
            first._planIds.AddRange(_planIds.Take(take));
            first._queryHashes.AddRange(_queryHashes.Take(take));
            first._executionTypes.AddRange(_executionTypes.Take(take));
            first._replicaRoles.AddRange(_replicaRoles.Take(take));
            return first;
        }

        /// <summary>
        /// #9373: the kept keys' distinct database names and distinct query ids, the arrays of <see cref="WithByKeysFilter"/>, or NULL for both
        /// when any kept key has a NULL database name or a NULL query id (a plain equality cannot match it, and the filter would drop its rows).
        /// </summary>
        internal (string[]? DatabaseNames, long[]? QueryIds) FilterArrays()
        {
            if (_databaseNames.Any(name => name is null) || _queryIds.Any(id => id is null))
            {
                return (null, null);
            }

            return (_databaseNames.Select(name => name!).Distinct().ToArray(), _queryIds.Select(id => id!.Value).Distinct().ToArray());
        }

        /// <summary>
        /// #9373: adds the two filter parameters of a by-keys statement, which follow $11 for the table statements and the built run's two
        /// midnights for the rollup ones (<see cref="TableByKeysFilterParameter"/>, <see cref="RollupByKeysFilterParameter"/>). Call it last.
        /// </summary>
        internal void AddFilterParameters(Npgsql.NpgsqlCommand command)
        {
            var (databaseNames, queryIds) = FilterArrays();
            command.Parameters.Add(new Npgsql.NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text,
                Value = databaseNames is null ? DBNull.Value : databaseNames,
            });
            command.Parameters.Add(new Npgsql.NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Bigint,
                Value = queryIds is null ? DBNull.Value : queryIds,
            });
        }

        /// <summary>Adds $6 to $11 of <see cref="QueryStoreTopTableByKeysSql"/>.</summary>
        internal void AddParameters(Npgsql.NpgsqlCommand command)
        {
            static Npgsql.NpgsqlParameter Array(NpgsqlTypes.NpgsqlDbType type, object values) =>
                new() { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | type, Value = values };

            command.Parameters.Add(Array(NpgsqlTypes.NpgsqlDbType.Text, _databaseNames.ToArray()));
            command.Parameters.Add(Array(NpgsqlTypes.NpgsqlDbType.Bigint, _queryIds.ToArray()));
            command.Parameters.Add(Array(NpgsqlTypes.NpgsqlDbType.Bigint, _planIds.ToArray()));
            command.Parameters.Add(Array(NpgsqlTypes.NpgsqlDbType.Text, _queryHashes.ToArray()));
            command.Parameters.Add(Array(NpgsqlTypes.NpgsqlDbType.Text, _executionTypes.ToArray()));
            command.Parameters.Add(Array(NpgsqlTypes.NpgsqlDbType.Text, _replicaRoles.ToArray()));
        }
    }

    /// <summary>
    /// #3953's gate and table read, on ONE connection in ONE read-only REPEATABLE READ transaction (M1, ruling
    /// issuecomment-5836972848), so <see cref="QueryStoreIntervalWide.ReadsTableAsync"/>'s decision and the read
    /// it authorizes see the same snapshot. Returns null (never an empty list) when the gate says raw, so the
    /// caller can tell "read raw instead" from "the table legitimately has nothing" — an empty list from the
    /// table IS a valid answer and must not fall back to raw. Any fault opening the connection, starting the
    /// transaction, running the gate, or reading the table also returns null (except cancellation, which
    /// propagates): the gate already does this for its own statements (<see cref="QueryStoreIntervalWide.ReadsTableAsync"/>'s
    /// catch), and the table read must fail the same way rather than surface to the caller as an error.
    /// </summary>
    private async Task<(List<ViewerQueryStoreRow> Rows, QueryStoreIntervalWide.WideReadPlan Plan)?> TryGetQueryStoreTopQueriesFromTableAsync(
        int serverId, DateTime startUtc, DateTime endUtc, DateTime? literalEndUtc, int top, IReadOnlyList<string>? databaseNames, QueryTagFilter? tags, CancellationToken cancellationToken,
        bool useRollups = true)
    {
        var elapsed = System.Diagnostics.Stopwatch.StartNew();
        var rollupsTried = false;
        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(ViewerReadScope.Effective(cancellationToken));
            await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);

            await using (var readOnly = new Npgsql.NpgsqlCommand("SET TRANSACTION READ ONLY", connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds })
            {
                await readOnly.ExecuteNonQueryAsync(cancellationToken);
            }

            /* #4689: bind the plan's ReadStart, which reaches below raw's chunk floor down to the earliest
               instant the table provably equals what raw held; the banner names it. */
            var plan = await QueryStoreIntervalWide.ResolveReadAsync(
                connection, serverId, startUtc, endUtc, literalEndUtc, QueryStoreIntervalWide.GridWideMinWindow,
                ViewerCommandDeadlines.CurrentInteractiveReadSeconds, logger: null, legacyRows: QueryStoreIntervalWide.LegacyRows.Allow, cancellationToken);
            if (!plan.UseTable)
            {
                if (plan.DecisionFailed)
                {
                    LogTableReadFallback("Query Store grid", elapsed, "the source decision failed", null);
                }

                return null;
            }

            /* #5313: the same candidate limit as the raw read, on this transaction's snapshot ($6). #5815: one run at
               TopFill's final limit; each fill round used to re-read the server's window from the wide table. */
            List<ViewerQueryStoreRow> rows;

            /* #5882: the same shorter first_execution_time floor as the MCP read (DarlingDataReader.TryGetQueryStoreTopFromTableAsync
               explains it), one extra round trip per table-served read. An open end reads to now. */
            /* #9111: a run of whole built days inside the window comes from the closed-day rollup, in this read's snapshot; the window's
               edges, the open days and any day that is not built stay on the interval table. No run reads the interval table alone. */
            /* The built-day lookup itself reads the rollup bookkeeping tables, so a fault in it counts. Once it says no day is served, the
               rest of the read is the interval table alone and a fault there is not the rollups' to retry. */
            rollupsTried = useRollups && _cachedStoreSchemaVersion is int rollupVersion && rollupVersion >= QueryStoreClosedDayRollup.Version;
            var (served, servedReason, builtDays) = rollupsTried
                ? await ResolveServedDaysAsync(connection, transaction, serverId, plan.ReadStart, literalEndUtc ?? endUtc, cancellationToken)
                : (null, useRollups ? QueryStoreClosedDayRollup.SourceReason.Unavailable : QueryStoreClosedDayRollup.SourceReason.FellBack, Array.Empty<DateOnly>());
            rollupsTried = served is not null;
            var source = QueryStoreClosedDayRollup.DescribeSourceFor(connection, plan.ReadStart, literalEndUtc ?? endUtc, served, servedReason, builtDays);
            /* #9283: the window as two arms, each with the floor that holds for its rows (the MCP top read's split, #9276), once the partial
               index the NULL-start arm reads through is built. #9287: a served run's two interval arms take the same split, and the arms with an
               interval start are bounded above by the server's longest interval span plus an hour (QueryStoreTopRollupSplitPrefix). Until the
               index is built the read keeps the one 26 hour floor and the statements it had. */
            var splitBuilt = await QueryStoreBackgroundIndexes.IsBuiltAsync(
                connection, transaction, QueryStoreBackgroundIndexes.WideNullStartFirstExec, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
            var (edgeMargin, longestSpan) = await QueryStoreIntervalWide.ResolveEdgeMarginAndLongestSpanAsync(
                connection, serverId, plan.ReadStart, literalEndUtc ?? DateTime.UtcNow, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
            var (keysSql, topSql) = served is not null
                ? QueryStoreTopRollupStatements(splitBuilt, longestSpan)
                : splitBuilt ? (QueryStoreTopTableSplitKeysSql, QueryStoreTopTableSplitByKeysSql) : (QueryStoreTopTableKeysSql, QueryStoreTopTableByKeysSql);
            /* #9287: the slow-load line says which shape this read got, so a cold read can be told apart without a store read. */
            source = source with { Shape = QueryStoreClosedDayRollup.DescribeShape(edgeMargin, longestSpan, splitBuilt) };
            if (edgeMargin < QueryStoreIntervalWide.PurgeEdgeMargin)
            {
                var marginSql = QueryStoreIntervalWide.EdgeMarginSqlFor(edgeMargin);
                keysSql = QueryStoreIntervalWide.WithEdgeMargin(keysSql, marginSql);
                topSql = QueryStoreIntervalWide.WithEdgeMargin(topSql, marginSql);
            }

            /* The tag filter and the self-statement slot belong to the keys statement alone: they decide which keys are kept. */
            keysSql = QueryTagReadJoin.Apply(keysSql, serverId, tags);

            /* #7736: the monitor's own statements leave ranked before its candidate limit, once the key backfill has run. */
            keysSql = await WithSelfKeysBeforeCandidateLimitAsync(connection, transaction, keysSql, cancellationToken);
            /* #8549: the table read alone gets the longer deadline (ViewerCommandDeadlines.QueryStoreTableReadSeconds): a cold first read can run past 15 s and still beat the raw read it would otherwise fall back to. */
            var keys = new TableReadKeys();
            /* #8549: the keys statement reads and aggregates millions of interval rows, and the planner's default parallel costs keep it
               serial. On a 30-day field read, zero setup and tuple costs gave a Gather with 2 workers and took the cold read from 22.8 s
               to 19.5 s. Both go back to their defaults below, so the per-key statement keeps the plan it has always had (a role or database setting still holds). */
            await SetKeysParallelCostsAsync(connection, transaction, cancellationToken);
            await using (var keysCommand = new Npgsql.NpgsqlCommand(keysSql, connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentQueryStoreTableReadSeconds })
            {
                AddTableReadParameters(keysCommand, serverId, plan.ReadStart, literalEndUtc, top, databaseNames);
                keysCommand.Parameters.Add(new Npgsql.NpgsqlParameter<int> { TypedValue = TopFill.FinalCandidates(top) });
                if (served is { } run)
                {
                    AddServedDaysParameters(keysCommand, run);
                }

                await using var keyReader = await keysCommand.ExecuteReaderAsync(ViewerReadScope.Effective(cancellationToken));
                while (await keyReader.ReadAsync(cancellationToken))
                {
                    keys.Add(keyReader);
                }
            }

            /* #8549: statement 2 reads each kept key's rows by key. With index scans on, each leaf runs an Index Scan per key on
               db_query_first_exec, which visits heap pages in index order, one random read at a time. With them off it is a Bitmap Heap
               Scan per leaf (pages in physical order, prefetched). Cold 30-day field reads on two servers of the same store with matched
               heap blocks per key: 0.66 against 0.33 ms per block, about half the per-key read time. enable_indexscan = off also turns off
               index-only scans, which is why the keys statement ran first. This is the transaction's last statement, so SET LOCAL
               ends with it. */
            await SetPerKeyFetchPlanAsync(connection, transaction, cancellationToken);

            (rows, _) = await ReadTablePageByKeysAsync(connection, transaction, topSql, serverId, plan.ReadStart, literalEndUtc, top, databaseNames, keys, cancellationToken, served);

            return (rows, plan with { Source = source });
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Review D4R M2: a permission failure here (a viewer role that can't SELECT the new table on an
               upgraded store) must not fall back to raw forever with no trace anywhere. #8549: it goes to the Viewer's own log, with the
               time spent, because a read cancelled at the deadline never reaches the store's statement history and a Trace line has no listener. */
            /* #9111: a fault with the closed-day rollups in the read (a table the viewer role cannot read, a missing relation) aborts this
               snapshot, so the read starts again on the interval table alone and gives that read's answer. A deadline is not retried: the
               second read would only wait another one. */
            if (rollupsTried && !IsCommandTimeout(ex))
            {
                LogRollupReadFallback("Query Store grid", elapsed, ex);
                return await TryGetQueryStoreTopQueriesFromTableAsync(
                    serverId, startUtc, endUtc, literalEndUtc, top, databaseNames, tags, cancellationToken, useRollups: false);
            }

            LogTableReadFallback("Query Store grid", elapsed, "the interval-table read failed", ex);
            return null;
        }
    }

    /// <summary>
    /// #8549: the grid's page, read from the rows of as few kept keys as it needs. The keys statement kept up to
    /// <see cref="TopFill.FinalCandidates"/> keys, in rank order, and the page is the first <paramref name="top"/> of them that the text
    /// trim keeps (the tail drops WAITFOR and the monitor's own statement). Fetching every kept key's rows read 250 keys' heap pages for a
    /// page of 50: on a field store, 250 loops of about 13 rows, 25.3 K buffers, 8.6 s cold. The first round fetches the first
    /// <see cref="TopFill.FirstCandidates"/> keys, and a round whose page is short and whose keys were all used fetches the next
    /// <see cref="TopFill.NextCandidates"/> count, up to every kept key, so the page, its order and its rows are those of one fetch over
    /// all the kept keys. The keys statement orders by the page's own sort (total duration), so a prefix of the keys holds the page's
    /// first rows. The candidate count of the old fetch was never read by the grid, so none is returned.
    /// Run inside the transaction that read <paramref name="keys"/>, after <see cref="SetPerKeyFetchPlanAsync"/>. <paramref name="served"/> is the
    /// built run a rollup statement (<see cref="QueryStoreTopRollupByKeysSql"/>) reads, bound after the keys (#9111).
    /// </summary>
    /// <returns>The page's rows, and how many keys each round fetched.</returns>
    internal static async Task<(List<ViewerQueryStoreRow> Rows, List<int> KeysPerRound)> ReadTablePageByKeysAsync(
        Npgsql.NpgsqlConnection connection, Npgsql.NpgsqlTransaction? transaction, string topSql, int serverId, DateTime readStart, DateTime? literalEndUtc,
        int top, IReadOnlyList<string>? databaseNames, TableReadKeys keys, CancellationToken cancellationToken, QueryStoreClosedDayRollup.ServedDays? served = null)
    {
        var keysPerRound = new List<int>();
        var limit = TopFill.FirstCandidates(top);
        for (var round = 1; ; round++)
        {
            var fetched = keys.First(limit);
            keysPerRound.Add(fetched.Count);
            List<ViewerQueryStoreRow> rows;
            await using (var command = new Npgsql.NpgsqlCommand(topSql, connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentQueryStoreTableReadSeconds })
            {
                AddTableReadParameters(command, serverId, readStart, literalEndUtc, top, databaseNames);
                fetched.AddParameters(command);
                if (served is { } run)
                {
                    AddServedDaysParameters(command, run);
                }

                /* #9373: after the served run's two parameters, where a rollup statement's filter parameters start. */
                fetched.AddFilterParameters(command);

                await using var reader = await command.ExecuteReaderAsync(ViewerReadScope.Effective(cancellationToken));
                (rows, _) = await TopFill.ReadPageAsync(reader, QueryStorePageOrdinal, ReadQueryStoreTopRow, cancellationToken);
            }

            var next = TopFill.NextCandidates(top, round, limit, rows.Count, fetched.Count);
            if (next is null)
            {
                return (rows, keysPerRound);
            }

            limit = next.Value;
        }
    }

    /// <summary>#8549: the parallel costs back to the session defaults and plain index scans off, for the per-key statement alone.</summary>
    private static async Task SetPerKeyFetchPlanAsync(Npgsql.NpgsqlConnection connection, Npgsql.NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        await using var set = new Npgsql.NpgsqlCommand(
            "SET LOCAL parallel_setup_cost TO DEFAULT; SET LOCAL parallel_tuple_cost TO DEFAULT; SET LOCAL enable_indexscan TO off", connection, transaction)
        { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds };
        await set.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>#8549: sets the two parallel-plan costs to zero for the rest of the transaction, for the keys statement alone.
    /// <see cref="SetPerKeyFetchPlanAsync"/> puts them back to the session defaults.</summary>
    private static async Task SetKeysParallelCostsAsync(Npgsql.NpgsqlConnection connection, Npgsql.NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        await using var set = new Npgsql.NpgsqlCommand(
            "SET LOCAL parallel_setup_cost TO 0; SET LOCAL parallel_tuple_cost TO 0", connection, transaction)
        { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds };
        await set.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// #8549: one Warning line in the Viewer's log each time an interval-table read gives up and the read starts again on raw history: which
    /// read, how long the failed attempt took, why, and that raw is next. On a field store the first 30-day open spent about 15 s on a
    /// table read that ran into the read deadline, then read raw; the cancelled statement is not in the store's statement history and
    /// the fallback was a Trace line nobody listens to, so no log showed where the first 15 s went.
    /// </summary>
    private static void LogTableReadFallback(string read, System.Diagnostics.Stopwatch elapsed, string why, Exception? ex)
    {
        var detail = ex is null
            ? ""
            : $" ({ex.GetType().Name}: {ex.Message}{(ex.InnerException is { } inner ? $" / {inner.GetType().Name}" : "")})";
        ViewerLogger.Warn("QueryStore", $"{read}: {why} after {elapsed.ElapsedMilliseconds} ms{detail}; retrying on raw history (#8549)");
    }

    /// <summary>#9111: a read that took whole days from the closed-day rollups faulted and starts again on the interval table.</summary>
    private static void LogRollupReadFallback(string read, System.Diagnostics.Stopwatch elapsed, Exception ex) =>
        ViewerLogger.Warn("QueryStore", $"{read}: the closed-day rollup read failed after {elapsed.ElapsedMilliseconds} ms ({ex.GetType().Name}: {ex.Message}); reading the interval table instead (#9111)");

    /// <summary>The ordinal of <c>page_ord</c>, the column after <c>replica_role</c> (#5313): <c>candidate_count</c>
    /// follows it. <see cref="TopFill.ReadPageAsync{T}"/> reads both.</summary>
    internal const int QueryStorePageOrdinal = 53;

    /// <summary>Shared by <see cref="GetQueryStoreTopQueriesAsync"/>'s raw and table paths: both
    /// <see cref="QueryStoreTopSql"/> and <see cref="QueryStoreTopTableSql"/> project the same
    /// <see cref="QueryStoreTopSuffix"/> column list, in the same order.</summary>
    internal static ViewerQueryStoreRow ReadQueryStoreTopRow(System.Data.Common.DbDataReader reader) => new()
    {
        DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
        QueryId = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
        PlanId = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
        QueryHash = reader.IsDBNull(3) ? "" : reader.GetString(3),
        QueryText = reader.IsDBNull(4) ? "" : reader.GetString(4),
        ModuleName = reader.IsDBNull(5) ? "" : reader.GetString(5),
        TotalExecutions = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
        AvgDurationMs = reader.IsDBNull(7) ? 0 : Convert.ToDouble(reader.GetValue(7)),
        AvgCpuTimeMs = reader.IsDBNull(8) ? 0 : Convert.ToDouble(reader.GetValue(8)),
        AvgLogicalReads = reader.IsDBNull(9) ? 0 : Convert.ToDouble(reader.GetValue(9)),
        AvgLogicalWrites = reader.IsDBNull(10) ? 0 : Convert.ToDouble(reader.GetValue(10)),
        AvgPhysicalReads = reader.IsDBNull(11) ? 0 : Convert.ToDouble(reader.GetValue(11)),
        AvgRowcount = reader.IsDBNull(12) ? 0 : Convert.ToDouble(reader.GetValue(12)),
        MinDop = reader.IsDBNull(13) ? 0 : Convert.ToInt64(reader.GetValue(13)),
        MaxDop = reader.IsDBNull(14) ? 0 : Convert.ToInt64(reader.GetValue(14)),
        LastExecutionTime = reader.IsDBNull(15) ? null : reader.GetDateTime(15),
        QueryPlanHash = reader.IsDBNull(16) ? "" : reader.GetString(16),
        IsForcedPlan = !reader.IsDBNull(17) && reader.GetBoolean(17),
        PlanForcingType = reader.IsDBNull(18) ? "" : reader.GetString(18),
        ExecutionTypeDesc = reader.IsDBNull(19) ? "" : reader.GetString(19),
        FirstExecutionTime = reader.IsDBNull(20) ? null : reader.GetDateTime(20),
        AvgClrTimeMs = reader.IsDBNull(21) ? 0 : Convert.ToDouble(reader.GetValue(21)),
        AvgTempdbSpaceUsed = reader.IsDBNull(22) ? 0 : Convert.ToDouble(reader.GetValue(22)),
        AvgLogBytesUsed = reader.IsDBNull(23) ? 0 : Convert.ToDouble(reader.GetValue(23)),
        PlanType = reader.IsDBNull(24) ? "" : reader.GetString(24),
        ForceFailureCount = reader.IsDBNull(25) ? 0 : Convert.ToInt64(reader.GetValue(25)),
        LastForceFailureReason = reader.IsDBNull(26) ? "" : reader.GetString(26),
        CompatibilityLevel = reader.IsDBNull(27) ? 0 : Convert.ToInt32(reader.GetValue(27)),
        MinDurationMs = reader.IsDBNull(28) ? 0 : Convert.ToDouble(reader.GetValue(28)),
        MaxDurationMs = reader.IsDBNull(29) ? 0 : Convert.ToDouble(reader.GetValue(29)),
        MinCpuTimeMs = reader.IsDBNull(30) ? 0 : Convert.ToDouble(reader.GetValue(30)),
        MaxCpuTimeMs = reader.IsDBNull(31) ? 0 : Convert.ToDouble(reader.GetValue(31)),
        MinLogicalReads = reader.IsDBNull(32) ? 0 : Convert.ToDouble(reader.GetValue(32)),
        MaxLogicalReads = reader.IsDBNull(33) ? 0 : Convert.ToDouble(reader.GetValue(33)),
        MinLogicalWrites = reader.IsDBNull(34) ? 0 : Convert.ToDouble(reader.GetValue(34)),
        MaxLogicalWrites = reader.IsDBNull(35) ? 0 : Convert.ToDouble(reader.GetValue(35)),
        MinPhysicalReads = reader.IsDBNull(36) ? 0 : Convert.ToDouble(reader.GetValue(36)),
        MaxPhysicalReads = reader.IsDBNull(37) ? 0 : Convert.ToDouble(reader.GetValue(37)),
        MinClrTimeMs = reader.IsDBNull(38) ? 0 : Convert.ToDouble(reader.GetValue(38)),
        MaxClrTimeMs = reader.IsDBNull(39) ? 0 : Convert.ToDouble(reader.GetValue(39)),
        MinRowcount = reader.IsDBNull(40) ? 0 : Convert.ToDouble(reader.GetValue(40)),
        MaxRowcount = reader.IsDBNull(41) ? 0 : Convert.ToDouble(reader.GetValue(41)),
        MinLogBytesUsed = reader.IsDBNull(42) ? 0 : Convert.ToDouble(reader.GetValue(42)),
        MaxLogBytesUsed = reader.IsDBNull(43) ? 0 : Convert.ToDouble(reader.GetValue(43)),
        MinTempdbSpaceUsed = reader.IsDBNull(44) ? 0 : Convert.ToDouble(reader.GetValue(44)),
        MaxTempdbSpaceUsed = reader.IsDBNull(45) ? 0 : Convert.ToDouble(reader.GetValue(45)),
        AvgMemoryMb = reader.IsDBNull(46) ? 0 : Convert.ToDouble(reader.GetValue(46)),
        MinMemoryMb = reader.IsDBNull(47) ? 0 : Convert.ToDouble(reader.GetValue(47)),
        MaxMemoryMb = reader.IsDBNull(48) ? 0 : Convert.ToDouble(reader.GetValue(48)),
        AvgNumPhysicalIoReads = reader.IsDBNull(49) ? 0 : Convert.ToDouble(reader.GetValue(49)),
        MinNumPhysicalIoReads = reader.IsDBNull(50) ? 0 : Convert.ToDouble(reader.GetValue(50)),
        MaxNumPhysicalIoReads = reader.IsDBNull(51) ? 0 : Convert.ToDouble(reader.GetValue(51)),
        ReplicaRole = reader.IsDBNull(52) ? null : reader.GetString(52),
    };

    /// <summary>
    /// #4231: the raw floor for <c>query_store_stats</c> over [<paramref name="startUtc"/>,
    /// <paramref name="endUtc"/>] — the shared probe (<see cref="RawWindowFloor"/>), never a second, hand-copied
    /// floor query. The Queries tab's <c>LoadQueryStoreAsync</c> reads this
    /// beside <see cref="GetQueryStoreTopQueriesAsync"/> so the grid header can disclose a window the raw tier
    /// no longer fully holds, the same fact <c>get_query_store_top</c> reports over MCP (#2364). The same
    /// window backs <see cref="GetQueryStoreSlicerDataAsync"/>'s initial (unsliced) read, so one floor read
    /// covers both rather than a second round trip for the identical [start, end]. A window no longer than
    /// <see cref="DurationTrendRouting.TruncationSlack"/> answers null without a store read (#4966): no banner can show for one.
    /// </summary>
    public Task<DateTime?> GetQueryStoreWindowFloorAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        /* #4966: a window no longer than the truncation slack can never get a coverage note, so the probe starts no query for it, as
           DataWindowFloor.GetForServerAsync's does not (GetQueryStatsWindowFloorAsync says why). The skip is here, in the viewer's
           methods, and not in RawWindowFloor.GetAsync: Darling's MCP tools share that probe and read its null as "nothing was read". */
        if (endUtc - startUtc <= DurationTrendRouting.TruncationSlack)
        {
            return Task.FromResult<DateTime?>(null);
        }

        return RawWindowFloor.GetAsync(_dataSource, RawWindowFloor.Table.QueryStoreStats, serverId, startUtc, endUtc,
            ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
    }

    /// <summary>
    /// Query-Store comparison — Lite's <c>GetQueryStoreComparisonAsync</c> ported. Uses execution-count
    /// weighted averages (<c>SUM(execution_count * avg_metric) / SUM(execution_count)</c>) so periods with
    /// uneven interval counts aggregate correctly, top-100-union + FULL OUTER JOIN keyed on
    /// (database, query_hash), each key joined as <c>COALESCE(key,'') = COALESCE(key,'')</c> plus an <c>IS NULL</c> pair so
    /// a NULL key and an empty-string key stay apart (#5420). Returns the shared <see cref="QueryStatsComparisonItem"/>.
    /// $1 server_id, $2/$3 current window, $4/$5 baseline window (naive UTC).
    /// </summary>
    public static readonly string QueryStoreComparisonSql = $$"""
        WITH deduped_current AS (
            /* LOAD-BEARING (correctness, not just perf) — #1841. The rows are CUMULATIVE per-interval
               snapshots and the collector re-fetches the OPEN interval every cycle, so the SAME interval
               (same first_execution_time) is stored repeatedly with a growing execution_count. Keep the
               LATEST snapshot per interval before aggregating.

               The execution-count weighting below does NOT rescue this on its own: the repeated snapshots
               of one interval carry DIFFERENT (growing) weights AND different avg_* values, so an open
               interval is weighted by the triangular sum of its own growth. That bias is stronger in the
               recent window than in the baseline window (recent windows hold more still-open intervals),
               which skews the very delta this comparison exists to compute. One deduped CTE per window,
               so both arms are treated identically. */
            SELECT
                database_name,
                /* #2150: query_id is projected (it was already a partition key) so the period CTEs can
                   resolve text from collect.query_store_text, which is keyed on it. */
                query_id,
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
            FROM query_store_stats
            WHERE server_id = $1
            AND   collection_time > $2 AND collection_time <= $3
            AND   ($6::text[] IS NULL OR database_name = ANY($6))
            /*SELFKEY:stats*/
        ),
        deduped_baseline AS (
            SELECT
                database_name,
                /* #2150: query_id is projected (it was already a partition key) so the period CTEs can
                   resolve text from collect.query_store_text, which is keyed on it. */
                query_id,
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
            FROM query_store_stats
            WHERE server_id = $1
            AND   collection_time > $4 AND collection_time <= $5
            AND   ($6::text[] IS NULL OR database_name = ANY($6))
            /*SELFKEY:stats*/
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
            ) AS combined
        ),
        /* #5420: each period aggregates its deduped rows ONCE (the derived table w) and the top list joins to that, on the
           null-safe pair COALESCE(x,'') = COALESCE(y,'') AND (x IS NULL) = (y IS NULL). The old form joined the top list
           (at most 200 hashes) to the window's rows with IS NOT DISTINCT FROM, which PostgreSQL can only run as a
           nested loop that filters every window row against every hash: seconds on a 24 hour window, 18 to 24 s on 7 days. */
        current_period AS (
            SELECT th.database_name, th.query_hash,
                   w.exec_count, w.avg_duration_ms, w.avg_cpu_ms, w.avg_reads, w.query_text
            FROM top_hashes th
            INNER JOIN (
                SELECT qs.database_name, qs.query_hash,
                       SUM(qs.execution_count) AS exec_count,
                       SUM(qs.execution_count * qs.avg_duration_us::double precision) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_duration_ms,
                       SUM(qs.execution_count * qs.avg_cpu_time_us::double precision) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_cpu_ms,
                       SUM(qs.execution_count * qs.avg_logical_io_reads::double precision) / NULLIF(SUM(qs.execution_count), 0) AS avg_reads,
                       /* #2150: this comparison groups by query_hash, but text is stored per query_id, so the
                          side table is joined on the finer key and MAX still picks one member's text for the
                          group — the same arbitrary-but-deterministic choice MAX(qs.query_text) made before.
                          The join cannot fan out (query_store_text is one row per server/database/query_id, by
                          primary key), so the execution-count SUMs above are unaffected. The COALESCE keeps
                          pre-cutover rows, whose text is still inline, reading exactly as they used to. */
                       MAX(COALESCE(x.query_sql_text, qs.query_text)) AS query_text
                FROM deduped_current qs
                LEFT JOIN query_store_text AS x
                  ON  x.server_id = $1
                  AND x.database_name = qs.database_name
                  AND x.query_id = qs.query_id
                WHERE qs.rn = 1
                AND   qs.execution_count > 0
                GROUP BY qs.database_name, qs.query_hash
            ) w
              ON  COALESCE(w.query_hash, '') = COALESCE(th.query_hash, '')
              AND (w.query_hash IS NULL) = (th.query_hash IS NULL)
              AND COALESCE(w.database_name, '') = COALESCE(th.database_name, '')
              AND (w.database_name IS NULL) = (th.database_name IS NULL)
        ),
        baseline_period AS (
            SELECT th.database_name, th.query_hash,
                   w.exec_count, w.avg_duration_ms, w.avg_cpu_ms, w.avg_reads, w.query_text
            FROM top_hashes th
            INNER JOIN (
                SELECT qs.database_name, qs.query_hash,
                       SUM(qs.execution_count) AS exec_count,
                       SUM(qs.execution_count * qs.avg_duration_us::double precision) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_duration_ms,
                       SUM(qs.execution_count * qs.avg_cpu_time_us::double precision) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_cpu_ms,
                       SUM(qs.execution_count * qs.avg_logical_io_reads::double precision) / NULLIF(SUM(qs.execution_count), 0) AS avg_reads,
                       /* #2150 — same resolution as current_period above. Both arms need it because the final
                          projection takes COALESCE(c.query_text, b.query_text): converting only one arm would
                          leave a GONE row (present in baseline only) with no text to fall back to. */
                       MAX(COALESCE(x.query_sql_text, qs.query_text)) AS query_text
                FROM deduped_baseline qs
                LEFT JOIN query_store_text AS x
                  ON  x.server_id = $1
                  AND x.database_name = qs.database_name
                  AND x.query_id = qs.query_id
                WHERE qs.rn = 1
                AND   qs.execution_count > 0
                GROUP BY qs.database_name, qs.query_hash
            ) w
              ON  COALESCE(w.query_hash, '') = COALESCE(th.query_hash, '')
              AND (w.query_hash IS NULL) = (th.query_hash IS NULL)
              AND COALESCE(w.database_name, '') = COALESCE(th.database_name, '')
              AND (w.database_name IS NULL) = (th.database_name IS NULL)
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
          ON  COALESCE(c.database_name, '') = COALESCE(b.database_name, '')
          AND COALESCE(c.query_hash, '') = COALESCE(b.query_hash, '')
          AND (c.database_name IS NULL) = (b.database_name IS NULL)
          AND (c.query_hash IS NULL) = (b.query_hash IS NULL)
        /* #6665: the monitor's own statement is left out of the comparison by the shared read-side predicate. It is applied to the
           text the comparison resolved, after the top-100 cut, so a statement that ranks in the top 100 costs the grid one row. */
        WHERE {{QueryStoreSelfStatement.NotSelfOfText("COALESCE(c.query_text, b.query_text)")}}
        """;

    /// <summary>Query-Store current-vs-baseline comparison rows (shared .Ui item; delta % + NEW/GONE badges).</summary>
    public async Task<List<QueryStatsComparisonItem>> GetQueryStoreComparisonAsync(
        int serverId,
        DateTime currentStart, DateTime currentEnd,
        DateTime baselineStart, DateTime baselineEnd,
        IReadOnlyList<string>? databaseNames = null,
        CancellationToken cancellationToken = default)
    {
        var items = new List<QueryStatsComparisonItem>();

        await using var connection = await _dataSource.OpenConnectionAsync(ViewerReadScope.Effective(cancellationToken));
        /* #7730: the key table's test ahead of each window's top-100 cut once its backfill has run; the tail's text test stays for the
           marker texts of a store the backfill has not reached. */
        var comparisonSql = await QueryStoreSelfKeys.ReadyAsync(connection, null, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken)
            ? QueryStoreSelfStatement.WithComparisonKeyTest(QueryStoreComparisonSql)
            : QueryStoreComparisonSql;
        await using var command = new Npgsql.NpgsqlCommand(comparisonSql, connection);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddComparisonParameters(command, serverId, currentStart, currentEnd, baselineStart, baselineEnd);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(ViewerReadScope.Effective(cancellationToken));
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new QueryStatsComparisonItem
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                QueryHash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                QueryText = reader.IsDBNull(2) ? "" : reader.GetString(2),
                ExecutionCount = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
                AvgDurationMs = reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4)),
                AvgCpuMs = reader.IsDBNull(5) ? 0 : Convert.ToDouble(reader.GetValue(5)),
                AvgReads = reader.IsDBNull(6) ? 0 : Convert.ToDouble(reader.GetValue(6)),
                BaselineExecutionCount = reader.IsDBNull(7) ? 0 : Convert.ToInt64(reader.GetValue(7)),
                BaselineAvgDurationMs = reader.IsDBNull(8) ? 0 : Convert.ToDouble(reader.GetValue(8)),
                BaselineAvgCpuMs = reader.IsDBNull(9) ? 0 : Convert.ToDouble(reader.GetValue(9)),
                BaselineAvgReads = reader.IsDBNull(10) ? 0 : Convert.ToDouble(reader.GetValue(10)),
            });
        }

        return items;
    }

    /// <summary>
    /// Hourly Query Store buckets for the slicer — Lite's <c>GetQueryStoreSlicerDataAsync</c> ported.
    /// Query Store rows are already per-interval averages, so the bucket totals weight each by
    /// <c>execution_count</c> (<c>SUM(avg_metric * execution_count)</c>). Same 7-column shape as the
    /// query/procedure slicers, so it shares <see cref="ReadQueryStatsSlicerAsync"/>.
    ///
    /// The slicer reads this raw statement below <see cref="QueryStoreIntervalWide.SlicerWideMinWindow"/>, and whenever the interval
    /// table cannot serve the window (ruling issuecomment-5840737421, amending issuecomment-5836972848 item 3: at 6 h, 12 h and
    /// 24 h raw beat the table with non-overlapping spreads, and at 7 d the two tied). From that window up it reads
    /// <see cref="QueryStoreSlicerTableSql"/> instead (#8549: 30 days measured faster than this statement on a field-sized store).
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string QueryStoreSlicerSql = $$"""
        WITH deduped AS (
            /* LOAD-BEARING (correctness, not just perf) — #1841. The rows are CUMULATIVE per-interval
               snapshots and the collector re-fetches the OPEN interval every cycle, so summing the raw
               rows counts one interval's work once per collection. The bucket is an HOUR and the cadence
               is ~5 minutes, so an open interval is summed up to ~12 times per bar — and a live store
               showed 496 collections of a single interval inside one hour bucket. Keep the LATEST
               snapshot per interval, then bucket, so the bars sum to the true total across the window.

               runtime_stats_interval_id is the REAL interval identity (tier 2), with first_execution_time
               kept beside it as the tier-1 proxy for rows collected before it existed — both in the key
               rather than a COALESCE, because the id is NULL on exactly the pre-tier-2 generation and
               non-NULL on exactly the post-tier-2 one, so the two can never collide and a legacy-only
               window degrades to precisely tier 1's key. Twins Lite's GetQueryStoreSlicerDataAsync.

               #8549: the same rule, written so a compressed store reads it cheaply. On a field store of
               3.5 million raw rows (12 compressed chunks, 5 raw, 30-day window) the window-function form took
               14.5 s at work_mem 64MB. 7.9 s of that was the per-chunk sorts, on a key whose FIRST column was
               database_name, a text column with a handful of values; with the integer columns first they took 2.9 s.
               The key now leads with query_id, plan_id, runtime_stats_interval_id and first_execution_time, and
               the text columns follow. A key is the same set of columns in any order, so the same rows are
               grouped. DISTINCT ON keeps the FIRST row of each key
               in the ORDER BY below, which is exactly what ROW_NUMBER() = 1 kept: the newest collection_time, then the
               highest execution_count, a NULL count first because a descending sort puts NULLs first. It is also
               a node the planner sizes from column statistics, where the window's rn = 1 filter was sized by a
               default 0.5 percent guess (12,533 rows estimated, 1,767,500 returned) that made the bucket step a
               1.77-million-row external sort. */
            SELECT DISTINCT ON (query_id, plan_id, runtime_stats_interval_id, first_execution_time, database_name, execution_type_desc, replica_role)
                collection_time,
                interval_start_time_utc,
                query_id,
                execution_count,
                avg_cpu_time_us,
                avg_duration_us,
                avg_logical_io_reads,
                avg_logical_io_writes,
                avg_physical_io_reads
            FROM query_store_stats
            WHERE server_id = $1
            /* The window is filtered on the SAME expression the bars are keyed on (#1892). It used to filter
               on collection_time while bucketing on the interval start, and once those stopped being the
               same instant (#1841 tier 2) the two disagreed at both edges: an interval that started before
               the window but closed inside it drew an extra bar to the LEFT of the requested range, and the
               window's own last interval -- whose closing fetch lands after the range ends -- was dropped
               entirely, which is the collection lag #1841 set out to remove showing up one layer down. */
            AND   COALESCE(interval_start_time_utc, collection_time) >= $2
            AND   COALESCE(interval_start_time_utc, collection_time) < $3
            /* Chunk-exclusion bounds, NOT filters: neither can exclude a row the predicate above keeps in
               any realistic store. They matter because query_store_stats is a hypertable partitioned on
               collection_time -- without bounds on that column TimescaleDB must open and decompress every
               chunk from the window through the present rather than the ones the window overlaps. Same
               shape and reasoning as the drill-down collector's last_execution_time bound.

               The FLOOR is free: an interval is always collected after it starts, so
               interval_start_time_utc <= collection_time, and therefore COALESCE(...) >= $2 already implies
               collection_time >= $2. The extra hour (#3953: was a day) is slack against clock skew between
               the monitored server's interval clock and ours — an hour covers it, and the wider margin was
               only ever scanning extra chunks the floor already made redundant.

               The CEILING is the longest a row's collection_time can trail its interval's start
               (QueryStoreIntervalWide.CollectionCeiling): the interval's own length -- at most 1 day, since
               Query Store's INTERVAL_LENGTH_MINUTES accepts only 1/5/10/15/30/60/1440 -- plus the collector's
               catch-up cap, which the live cutoff clamp and the backdated backfill slices both hold, plus an
               hour of slack. So it drops no row the predicate keeps, and a historical window reads the chunks
               collected within a day of it rather than every chunk through the present. */
            AND   collection_time >= $2 - {{CollectionFloorSlackSql}}
            AND   collection_time <= $3 + {{CollectionCeilingSql}}
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            ORDER BY query_id, plan_id, runtime_stats_interval_id, first_execution_time, database_name, execution_type_desc, replica_role, collection_time DESC, execution_count DESC
        ),
        binned AS (
            /* The bar sits in the hour the work RAN, not the hour it was last COLLECTED (#1841 tier 2).
               Query Store's default interval is 60 minutes and the closing fetch lands in the cycle AFTER
               the interval ends, so a collection_time bucket was reliably one bar late.
               interval_start_time_utc is the interval's own start boundary, converted to UTC at collection
               (the same clock as collection_time), so this is a placement fix and not a timezone trade.
               Legacy rows have no interval start and keep collection_time placement, so a window spanning
               the upgrade renders correctly on both sides and the lag disappears as those rows age out. */
            SELECT
                query_id,
                date_bin(CAST($5 AS integer) * INTERVAL '1 minute', COALESCE(interval_start_time_utc, collection_time), {{TrendBucketSql.OriginSql}}) AS bucket,
                CAST(avg_cpu_time_us AS double precision) * execution_count AS cpu,
                CAST(avg_duration_us AS double precision) * execution_count AS dur,
                CAST(avg_logical_io_reads AS double precision) * execution_count AS reads,
                CAST(avg_logical_io_writes AS double precision) * execution_count AS writes,
                CAST(avg_physical_io_reads AS double precision) * execution_count AS preads
            FROM deduped
        ),
        flagged AS (
            /* #8549: the bucket step used to be COUNT(DISTINCT query_id), an aggregate PostgreSQL can only run
               over input sorted by (bucket, query_id) -- a 1.77-million-row external sort (128 MB of temp files)
               on the field store, under a GROUP BY it could not hash. The deduped rows arrive sorted by query_id,
               so numbering each query's rows within a bucket is a sort of about 100 rows at a time (an Incremental
               Sort), and the bucket totals are then a plain hash aggregate over the bucket. qrn = 1 marks the
               first row of each query_id inside a bucket, so counting those rows is COUNT(DISTINCT query_id):
               a NULL query_id is never counted, as before. */
            SELECT *, ROW_NUMBER() OVER (PARTITION BY query_id, bucket) AS qrn
            FROM binned
        )
        SELECT
            bucket,
            COUNT(query_id) FILTER (WHERE qrn = 1) AS query_count,
            COALESCE(SUM(cpu), 0) / 1000.0 AS total_cpu_ms,
            COALESCE(SUM(dur), 0) / 1000.0 AS total_duration_ms,
            COALESCE(SUM(reads), 0) AS total_reads,
            COALESCE(SUM(writes), 0) AS total_writes,
            COALESCE(SUM(preads), 0) AS total_physical_reads
        FROM flagged
        GROUP BY bucket
        ORDER BY bucket
        """;

    /// <summary>
    /// The interval-table twin of <see cref="QueryStoreSlicerSql"/> (#8549). <c>query_store_interval_wide</c> already holds one row per
    /// interval identity, the newest snapshot (the rule the raw statement's DISTINCT ON applies), so there is no dedupe step: the rows
    /// are binned on the same expression, and the same columns weighted the same way. The bucket totals are one aggregate over the binned rows, and the distinct-query
    /// count is a separate narrow step over (bucket, query_id) pairs alone, where the raw statement counts first rows of a query in a bucket
    /// over rows that arrive sorted by query_id. The window and the <c>first_execution_time</c> bounds are the grid's own
    /// (<see cref="QueryStoreTopTablePrefix"/>), so the btree and the day partitions serve it.
    /// $1 server_id, $2 the read plan's start (<see cref="QueryStoreIntervalWide.WideReadPlan.ReadStart"/>), $3 window end (naive UTC),
    /// $4 database filter, $5 bucket minutes.
    /// </summary>
    public static readonly string QueryStoreSlicerTableSql = $$"""
        WITH binned AS NOT MATERIALIZED (
            /* Read twice below, once per aggregate. NOT MATERIALIZED scans the table twice, the second time from cache, instead of writing every binned row to a temp store (#8549). */
            SELECT
                query_id,
                date_bin(CAST($5 AS integer) * INTERVAL '1 minute', COALESCE(interval_start_time_utc, collection_time), {{TrendBucketSql.OriginSql}}) AS bucket,
                CAST(avg_cpu_time_us AS double precision) * execution_count AS cpu,
                CAST(avg_duration_us AS double precision) * execution_count AS dur,
                CAST(avg_logical_io_reads AS double precision) * execution_count AS reads,
                CAST(avg_logical_io_writes AS double precision) * execution_count AS writes,
                CAST(avg_physical_io_reads AS double precision) * execution_count AS preads
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   COALESCE(interval_start_time_utc, collection_time) >= $2
            AND   COALESCE(interval_start_time_utc, collection_time) < $3
            AND   first_execution_time >= $2 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time <= $3 + {{QueryStoreIntervalWide.IntervalStartFirstExecMarginSql}}
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        sums AS (
            /* #8549: the bucket totals are added straight off the binned rows. They used to be summed per (bucket, query_id) first, which made
               the sort or hash carry all five sums on a 2.5-million-group step: on a field store that was a sort of 2.77 million wide rows
               (about 196 MB of temp files) and then two more aggregate steps. Only the distinct-query count needs the pairs. */
            SELECT
                bucket,
                SUM(cpu) AS cpu,
                SUM(dur) AS dur,
                SUM(reads) AS reads,
                SUM(writes) AS writes,
                SUM(preads) AS preads
            FROM binned
            GROUP BY bucket
        ),
        counts AS (
            /* One narrow row per (bucket, query_id): the grouping carries two columns and no sums. A NULL query_id is never counted, as
               COUNT(DISTINCT) skips it, but its work is still in sums above. */
            SELECT
                bucket,
                COUNT(*) AS query_count
            FROM
            (
                SELECT bucket, query_id
                FROM binned
                WHERE query_id IS NOT NULL
                GROUP BY bucket, query_id
            ) AS pairs
            GROUP BY bucket
        ),
        parts AS (
            /* One row per bucket from each side, stacked and added up below. Joining the two aggregates instead made the planner sort the
               2.7 million wide rows by bucket to feed a merge join (#8549). */
            SELECT bucket, CAST(NULL AS bigint) AS query_count, cpu, dur, reads, writes, preads
            FROM sums
            UNION ALL
            SELECT bucket, query_count, NULL, NULL, NULL, NULL, NULL
            FROM counts
        )
        SELECT
            bucket,
            CAST(COALESCE(SUM(query_count), 0) AS bigint) AS query_count,
            COALESCE(SUM(cpu), 0) / 1000.0 AS total_cpu_ms,
            COALESCE(SUM(dur), 0) / 1000.0 AS total_duration_ms,
            COALESCE(SUM(reads), 0) AS total_reads,
            COALESCE(SUM(writes), 0) AS total_writes,
            COALESCE(SUM(preads), 0) AS total_physical_reads
        FROM parts
        GROUP BY bucket
        ORDER BY bucket
        """;

    /// <summary>
    /// #9111: <see cref="QueryStoreSlicerTableSql"/> when a run of whole built UTC days lies inside the window and the bin is a whole number
    /// of hours. <c>binned</c> stacks three arms: the interval table from the read start up to the run, the interval table from the end of
    /// the run to the window's end, and <c>query_store_slicer_hourly</c> for the run, whose rows are already the five products summed per
    /// (hour, database, query). An hour lies inside one bin (the bin origin is a midnight and the bin a multiple of 60 minutes), so the
    /// rollup's hour bins where its rows did. Everything from <c>sums</c> down is the interval-table statement's own text, and the query
    /// stays in the rollup's key, so the totals and the distinct query count are the same from either source.
    /// $1 to $5 as that statement's, $6 the first midnight of the built run, $7 the midnight after its last day.
    /// </summary>
    public static readonly string QueryStoreSlicerRollupSql = $$"""
        WITH binned AS NOT MATERIALIZED (
            SELECT
                query_id,
                date_bin(CAST($5 AS integer) * INTERVAL '1 minute', COALESCE(interval_start_time_utc, collection_time), {{TrendBucketSql.OriginSql}}) AS bucket,
                CAST(avg_cpu_time_us AS double precision) * execution_count AS cpu,
                CAST(avg_duration_us AS double precision) * execution_count AS dur,
                CAST(avg_logical_io_reads AS double precision) * execution_count AS reads,
                CAST(avg_logical_io_writes AS double precision) * execution_count AS writes,
                CAST(avg_physical_io_reads AS double precision) * execution_count AS preads
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   COALESCE(interval_start_time_utc, collection_time) >= $2
            AND   COALESCE(interval_start_time_utc, collection_time) < $6::timestamp
            AND   first_execution_time >= $2 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time <= $6::timestamp + {{QueryStoreIntervalWide.IntervalStartFirstExecMarginSql}}
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            UNION ALL
            SELECT
                query_id,
                date_bin(CAST($5 AS integer) * INTERVAL '1 minute', COALESCE(interval_start_time_utc, collection_time), {{TrendBucketSql.OriginSql}}) AS bucket,
                CAST(avg_cpu_time_us AS double precision) * execution_count AS cpu,
                CAST(avg_duration_us AS double precision) * execution_count AS dur,
                CAST(avg_logical_io_reads AS double precision) * execution_count AS reads,
                CAST(avg_logical_io_writes AS double precision) * execution_count AS writes,
                CAST(avg_physical_io_reads AS double precision) * execution_count AS preads
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   COALESCE(interval_start_time_utc, collection_time) >= $7::timestamp
            AND   COALESCE(interval_start_time_utc, collection_time) < $3
            AND   first_execution_time >= $7::timestamp - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time <= $3 + {{QueryStoreIntervalWide.IntervalStartFirstExecMarginSql}}
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            UNION ALL
            SELECT
                query_id,
                date_bin(CAST($5 AS integer) * INTERVAL '1 minute', hour_utc, {{TrendBucketSql.OriginSql}}) AS bucket,
                cpu_wsum AS cpu,
                duration_wsum AS dur,
                logical_reads_wsum AS reads,
                logical_writes_wsum AS writes,
                physical_reads_wsum AS preads
            FROM query_store_slicer_hourly
            WHERE server_id = $1
            AND   hour_utc >= $6::timestamp
            AND   hour_utc < $7::timestamp
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),

        """ + QueryStoreSlicerTableSql[QueryStoreSlicerTableSql.IndexOf("sums AS (", StringComparison.Ordinal)..];

    /// <summary>Query Store slicer buckets over [<paramref name="startUtc"/>, <paramref name="endUtc"/>], at the width <see cref="SlicerBuckets"/> picks for it ($5 minutes).
    /// #8549: a window of <see cref="QueryStoreIntervalWide.SlicerWideMinWindow"/> or more reads the interval table when the grid's gate
    /// (<see cref="QueryStoreIntervalWide.ResolveReadAsync"/>, schema version, coverage, the raw chunk floor) lets it; any "no" or fault reads
    /// <see cref="QueryStoreSlicerSql"/>, as before.</summary>
    public async Task<List<TimeSliceBucket>> GetQueryStoreSlicerDataAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null,
        CancellationToken cancellationToken = default)
        => (await GetQueryStoreSlicerWithReachAsync(serverId, startUtc, endUtc, databaseNames, cancellationToken)).Buckets;

    /// <summary>
    /// <see cref="GetQueryStoreSlicerDataAsync"/> plus the read plan when the interval table served the buckets: the plan's
    /// <see cref="QueryStoreIntervalWide.WideReadPlan.ReadStart"/> is where the slicer's points start, which reaches below raw's chunk
    /// floor, so the grid banner names the slicer's start from the source that drew it. The plan is null when the read was raw.
    /// </summary>
    public async Task<(List<TimeSliceBucket> Buckets, QueryStoreIntervalWide.WideReadPlan? Plan)> GetQueryStoreSlicerWithReachAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null,
        CancellationToken cancellationToken = default)
    {
        if (endUtc - startUtc >= QueryStoreIntervalWide.SlicerWideMinWindow)
        {
            var schemaVersion = _cachedStoreSchemaVersion ??= await GetStoreSchemaVersionAsync(cancellationToken);
            if (schemaVersion is int version && version >= QueryStoreIntervalWideMinSchemaVersion)
            {
                var served = await TryGetQueryStoreSlicerFromTableAsync(serverId, startUtc, endUtc, databaseNames, cancellationToken);
                if (served is not null)
                {
                    return served.Value;
                }
            }
        }

        return (await ReadQueryStatsSlicerAsync(QueryStoreSlicerSql, serverId, startUtc, endUtc, databaseNames, cancellationToken), null);
    }

    /// <summary>The slicer's interval-table read, or null for "read raw instead" (the gate said no, or any fault except cancellation), as
    /// <see cref="TryGetQueryStoreTopQueriesFromTableAsync"/> does. No literal window end is passed: the raw statement keeps the newest
    /// snapshot of every interval that starts in the window, whatever the end, so the table's newer snapshots are the same answer.
    /// The legacy-row probe is skipped (<see cref="QueryStoreIntervalWide.LegacyRows.Allow"/>) because one snapshot per identity is
    /// what both statements keep for a legacy row too. The plan that served goes back with the buckets.</summary>
    private async Task<(List<TimeSliceBucket> Buckets, QueryStoreIntervalWide.WideReadPlan? Plan)?> TryGetQueryStoreSlicerFromTableAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames, CancellationToken cancellationToken, bool useRollups = true)
    {
        var elapsed = System.Diagnostics.Stopwatch.StartNew();
        var rollupsTried = false;
        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(ViewerReadScope.Effective(cancellationToken));
            await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);
            await using (var readOnly = new Npgsql.NpgsqlCommand("SET TRANSACTION READ ONLY", connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds })
            {
                await readOnly.ExecuteNonQueryAsync(cancellationToken);
            }

            var plan = await QueryStoreIntervalWide.ResolveReadAsync(
                connection, serverId, startUtc, endUtc, literalWindowEnd: null, QueryStoreIntervalWide.SlicerWideMinWindow,
                ViewerCommandDeadlines.CurrentInteractiveReadSeconds, logger: null, legacyRows: QueryStoreIntervalWide.LegacyRows.Allow, cancellationToken);
            if (!plan.UseTable)
            {
                if (plan.DecisionFailed)
                {
                    LogTableReadFallback("Query Store slicer", elapsed, "the source decision failed", null);
                }

                return null;
            }

            /* #8549: the table read alone gets the longer deadline, as the grid's does. */
            /* #9111: whole built days come from the hourly rollup when the bin is a whole number of hours, in this read's snapshot. */
            var bucketMinutes = SlicerBuckets.MinutesFor(startUtc, endUtc);
            rollupsTried = useRollups && bucketMinutes % 60 == 0 && _cachedStoreSchemaVersion is int rollupVersion && rollupVersion >= QueryStoreClosedDayRollup.Version;
            var (served, servedReason, builtDays) = rollupsTried
                ? await ResolveServedDaysAsync(connection, transaction, serverId, plan.ReadStart, endUtc, cancellationToken)
                : (null, useRollups ? QueryStoreClosedDayRollup.SourceReason.Unavailable : QueryStoreClosedDayRollup.SourceReason.FellBack, Array.Empty<DateOnly>());
            rollupsTried = served is not null;
            var source = QueryStoreClosedDayRollup.DescribeSourceFor(connection, plan.ReadStart, endUtc, served, servedReason, builtDays);
            await using var command = new Npgsql.NpgsqlCommand(served is null ? QueryStoreSlicerTableSql : QueryStoreSlicerRollupSql, connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentQueryStoreTableReadSeconds };
            command.Parameters.Add(new Npgsql.NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new Npgsql.NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.ReadStart, DateTimeKind.Unspecified) });
            command.Parameters.Add(new Npgsql.NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
            command.Parameters.Add(DatabaseFilterParameter(databaseNames));
            command.Parameters.Add(new Npgsql.NpgsqlParameter<int> { TypedValue = bucketMinutes });
            if (served is { } run)
            {
                AddServedDaysParameters(command, run);
            }

            await using var reader = await command.ExecuteReaderAsync(ViewerReadScope.Effective(cancellationToken));
            return (await ReadSlicerBucketsAsync(reader, cancellationToken), plan with { Source = source });
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #9111: as the grid's: a fault with the hourly rollup in the read starts it again on the interval table alone. */
            if (rollupsTried && !IsCommandTimeout(ex))
            {
                LogRollupReadFallback("Query Store slicer", elapsed, ex);
                return await TryGetQueryStoreSlicerFromTableAsync(serverId, startUtc, endUtc, databaseNames, cancellationToken, useRollups: false);
            }

            LogTableReadFallback("Query Store slicer", elapsed, "the interval-table read failed", ex);
            return null;
        }
    }
}
