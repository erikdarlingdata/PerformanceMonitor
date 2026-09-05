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

/// <summary>
/// Per-item execution-timeline reads backing the slicer overlay-on-select (#683): selecting a Top Queries /
/// Top Procedures / Query Store row overlays THAT item's activity curve on its sub-tab's slicer. Ports Lite's
/// <c>ServerTab.Grids.cs</c> overlay feed to Postgres. Lite fed the Query Store overlay from its history-grid
/// read until #1921, when it gained a dedicated <c>GetQueryStoreItemTimelineAsync</c> for the same reason this
/// file exists — a series drawn over the bars needs the bars' axis and dedup, which a raw per-collection grid
/// read does not have. The two apps now carry the same structure for it. One deviation from Lite: Darling's
/// <c>delta_*</c> columns are already per-collection-cycle deltas (Lite's history rows carry cumulative
/// values it diffs row-over-row), so the per-interval magnitude is read directly — no C# differencing. Query
/// Store keeps its per-execution averages, scaled by <c>execution_count</c> to a per-interval total the way
/// the Query Store slicer aggregate does.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>One point on an item's execution timeline: the collection cycle's per-interval magnitudes
    /// in the same units the slicer's sort-driven metric overlay uses (ms for CPU/elapsed, raw counts for
    /// reads/writes).</summary>
    public sealed record ItemTimelinePoint(
        DateTime PointTime, double CpuMs, double ElapsedMs, double Reads, double Writes, double PhysicalReads);

    public const string QueryStatsItemTimelineSql = """
        SELECT
            collection_time,
            COALESCE(delta_worker_time, 0) / 1000.0 AS cpu_ms,
            COALESCE(delta_elapsed_time, 0) / 1000.0 AS elapsed_ms,
            COALESCE(delta_logical_reads, 0) AS reads,
            COALESCE(delta_logical_writes, 0) AS writes,
            COALESCE(delta_physical_reads, 0) AS physical_reads
        FROM query_stats
        WHERE server_id = $1
        AND   database_name = $2
        AND   query_hash = $3
        AND   collection_time >= $4
        AND   collection_time <= $5
        ORDER BY collection_time
        """;

    /// <summary>The selected Top-Queries row's per-interval execution timeline over the window.</summary>
    public async Task<List<ItemTimelinePoint>> GetQueryStatsItemTimelineAsync(
        int serverId, string databaseName, string queryHash, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(QueryStatsItemTimelineSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddItemWindowParameters(command, serverId, databaseName, queryHash, startUtc, endUtc);
        return await ReadItemTimelineAsync(command, cancellationToken);
    }

    public const string ProcStatsItemTimelineSql = """
        SELECT
            collection_time,
            COALESCE(delta_worker_time, 0) / 1000.0 AS cpu_ms,
            COALESCE(delta_elapsed_time, 0) / 1000.0 AS elapsed_ms,
            COALESCE(delta_logical_reads, 0) AS reads,
            COALESCE(delta_logical_writes, 0) AS writes,
            COALESCE(delta_physical_reads, 0) AS physical_reads
        FROM procedure_stats
        WHERE server_id = $1
        AND   database_name = $2
        AND   schema_name = $3
        AND   object_name = $4
        AND   collection_time >= $5
        AND   collection_time <= $6
        ORDER BY collection_time
        """;

    /// <summary>The selected Top-Procedures row's per-interval execution timeline over the window.</summary>
    public async Task<List<ItemTimelinePoint>> GetProcStatsItemTimelineAsync(
        int serverId, string databaseName, string schemaName, string objectName, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ProcStatsItemTimelineSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName ?? "" });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = schemaName ?? "" });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = objectName ?? "" });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
        return await ReadItemTimelineAsync(command, cancellationToken);
    }

    public const string QueryStoreItemTimelineSql = """
        WITH deduped AS (
            /* LOAD-BEARING (correctness, not just perf) — #1841. The rows are CUMULATIVE per-interval
               snapshots and the collector re-fetches the OPEN interval every cycle, so an un-deduped
               projection draws one interval as a rising staircase of avg_* x execution_count products
               that are restatements of the same work, not new work.

               Two reasons this read needs it even though it has no SUM. (1) This series is drawn OVER the
               Query Store slicer bars, which ARE deduped (QueryStoreSlicerSql) — leaving the overlay raw
               would make the overlay disagree with the bars it annotates. (2) The WHERE narrows to
               query_id + plan_id but NOT to an interval, so one collection cycle can return several
               intervals for the same plan; the reader appends those as separate points. Deduping per
               interval collapses the restatements while keeping genuinely distinct intervals as their own
               points. */
            SELECT
                /* The point is placed where the work RAN, not where it was observed (#1921, Erik's call
                   between the two defensible readings). #1841 moved the slicer bars to the interval start
                   and left this overlay on collection_time, so a point sat up to one Query Store interval
                   — 60 minutes by default, 1440 at most — to the RIGHT of the bar describing the very same
                   work. The invariant three lines up is what that broke: the dedup half kept holding, the
                   PLACEMENT half silently stopped, and an overlay that disagrees with the bars it annotates
                   is worse than no overlay.

                   The accepted cost, stated so nobody re-opens it as a bug: the overlay can no longer show
                   WHEN a value was observed, and several intervals returned by one collection cycle now
                   spread across the x-axis instead of stacking at that cycle's instant. Spreading is the
                   correct picture — those intervals really did run at different times.

                   COALESCE, not a bare column: rows collected before #1841 tier 2 have no interval start,
                   and they keep collection_time placement, which is the same two-generation handling every
                   other post-#1841 read uses. */
                COALESCE(interval_start_time_utc, collection_time) AS point_time,
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
            FROM query_store_stats
            WHERE server_id = $1
            AND   database_name = $2
            AND   query_id = $3
            AND   plan_id = $4
            /* The window is filtered on the SAME expression the points are placed on (#1892's shape, applied
               here by #1921). Filtering on collection_time while placing on the interval start disagrees at
               both edges — an interval that started before the window but closed inside it would draw a point
               to the LEFT of the requested range, and the window's own last interval, whose closing fetch
               lands after the range ends, would vanish. It also has to match the BARS' window, or the two
               surfaces on one chart disagree about whether an interval is in the window at all. */
            AND   COALESCE(interval_start_time_utc, collection_time) >= $5
            AND   COALESCE(interval_start_time_utc, collection_time) <= $6
            /* Chunk-exclusion bounds, NOT filters: neither can exclude a row the predicate above keeps in any
               realistic store. query_store_stats is a hypertable partitioned on collection_time, so without
               bounds on that column TimescaleDB opens every chunk from the window through the present.

               The FLOOR is free — an interval is always collected after it starts, so COALESCE(...) >= $5
               already implies collection_time >= $5; the extra day is slack for clock skew. The CEILING is
               deliberately enormous: a row's collection_time exceeds its interval start by the interval's own
               length (at most 1 day, since INTERVAL_LENGTH_MINUTES accepts only 1/5/10/15/30/60/1440) plus
               however far behind the collector was, which nothing bounds — so 30 days is 1 of engine maximum
               and 29 of outage allowance. Same shipped shape as the slicer's (#1923); the issue's own scope
               note said "no ceiling", which was written before that round. */
            AND   collection_time >= $5 - interval '1 day'
            AND   collection_time <= $6 + interval '30 days'
        )
        SELECT
            point_time,
            COALESCE(CAST(avg_cpu_time_us AS double precision) * execution_count, 0) / 1000.0 AS cpu_ms,
            COALESCE(CAST(avg_duration_us AS double precision) * execution_count, 0) / 1000.0 AS elapsed_ms,
            COALESCE(CAST(avg_logical_io_reads AS double precision) * execution_count, 0) AS reads,
            COALESCE(CAST(avg_logical_io_writes AS double precision) * execution_count, 0) AS writes,
            COALESCE(CAST(avg_physical_io_reads AS double precision) * execution_count, 0) AS physical_reads
        FROM deduped
        WHERE rn = 1
        /* Ordered on the axis the points are PLOTTED on, which is now the interval start. Ordering by
           collection_time while placing on the interval start would hand the chart a series whose x-values
           are not monotonic — a line control draws that as a zig-zag rather than a curve. (It would also no
           longer compile: collection_time is not a column of `deduped` any more.) */
        ORDER BY point_time
        """;

    /// <summary>The selected Query Store row's per-interval execution timeline (avg × exec count) over the window.</summary>
    public async Task<List<ItemTimelinePoint>> GetQueryStoreItemTimelineAsync(
        int serverId, string databaseName, long queryId, long planId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(QueryStoreItemTimelineSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName ?? "" });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = queryId });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = planId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
        return await ReadItemTimelineAsync(command, cancellationToken);
    }

    /// <summary>$1 server_id, $2 database_name, $3 query_hash, $4 start, $5 end (window naive UTC).</summary>
    private static void AddItemWindowParameters(
        NpgsqlCommand command, int serverId, string databaseName, string queryHash, DateTime startUtc, DateTime endUtc)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName ?? "" });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queryHash ?? "" });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
    }

    private static async Task<List<ItemTimelinePoint>> ReadItemTimelineAsync(
        NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var points = new List<ItemTimelinePoint>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            points.Add(new ItemTimelinePoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : Convert.ToDouble(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4)),
                reader.IsDBNull(5) ? 0 : Convert.ToDouble(reader.GetValue(5))));
        }
        return points;
    }
}
