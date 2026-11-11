/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #7609: <c>get_query_fleet_summary</c>, one read that answers "where does this statement run, and what does it cost on
/// each server". An agent used to call <c>get_query_trend</c> or <c>get_top_queries_by_cpu</c> once per server (40 calls on a
/// 42-target store). The store answers it with one statement: the sums per server and database for one <c>query_hash</c>.
///
/// <para><b>Source.</b> The plan-cache hourly tier, through the SAME relation <c>get_query_trend</c> reads
/// (<see cref="RollupCoverage.StitchedRelationSql"/>: the legacy rollup stitched to its interval-honest successor) and the same
/// tier decision (<see cref="DurationTrendRouting.ResolveTier"/>). The rollup lags the clock, so the hours at or after its
/// measured materialization ceiling (<see cref="RollupCoverage.HourlyEndCeiling"/>) are summed from the raw
/// <c>query_stats</c> rows instead, and an hour is never counted from both. A window the raw tier can hold on its own, or a
/// store with no rollup, reads raw only.</para>
///
/// <para><b>Measured</b> on a restored copy of a 42-target store, cold cache, the statement alone: 7 days 4.3 s, 28 days 5.8 s,
/// 90 days 9.0 s with planning (0.12 s, 0.29 s and 0.31 s warm). All three meet the 10-second bar, so the window reaches 90 days.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpQueryFleetSummaryTools
{
    /// <summary>The default window: seven days.</summary>
    public const int DefaultHoursBack = 168;

    /// <summary>The default and most rows listed.</summary>
    public const int DefaultLimit = 50;

    /// <summary>The most rows a caller may ask for.</summary>
    public const int MaxLimit = 200;

    /// <summary>The label every row of the plan-cache read carries.</summary>
    public const string PlanCacheSource = "plan_cache";

    /// <summary>
    /// The fleet statement. <paramref name="hourlyRelation"/> is the hourly FROM item (aliased <c>f</c>) or null when raw serves
    /// the whole window; <paramref name="ceilingClause"/> bounds the hourly side below the rollup's materialization ceiling.
    /// $1 query_hash, $2/$3 window (naive UTC, start inclusive, end exclusive for the rollup and inclusive for raw as
    /// <c>get_query_trend</c> reads them), $4 the instant raw takes over from the rollup, $5 the row cap. Rows: server_name,
    /// display_name, database_name, executions, cpu_us, elapsed_us, first_hour, last_hour, pairs (the count of server and
    /// database pairs in the window, before the cap).
    /// </summary>
    public static string BuildSql(string? hourlyRelation, string ceilingClause)
    {
        var hourly = hourlyRelation is null
            ? ""
            : $"""
            hourly AS
            (
                SELECT f.server_id, f.database_name,
                       SUM(f.execution_count_sum) AS executions, SUM(f.worker_time_sum) AS cpu_us, SUM(f.elapsed_time_sum) AS elapsed_us,
                       MIN(f.bucket) AS first_hour, MAX(f.bucket) AS last_hour
                FROM {hourlyRelation}
                WHERE f.query_hash = $1
                AND   f.bucket >= $2
                AND   f.bucket < $3{ceilingClause}
                GROUP BY f.server_id, f.database_name
            ),
            """;
        var union = hourlyRelation is null ? "" : "SELECT * FROM hourly UNION ALL ";

        return $"""
        WITH {hourly}
        tail AS
        (
            SELECT q.server_id, q.database_name,
                   SUM(q.delta_execution_count) AS executions, SUM(q.delta_worker_time) AS cpu_us, SUM(q.delta_elapsed_time) AS elapsed_us,
                   MIN(date_trunc('hour', q.collection_time)) AS first_hour, MAX(date_trunc('hour', q.collection_time)) AS last_hour
            FROM query_stats AS q
            WHERE q.query_hash = $1
            AND   q.collection_time >= $4
            AND   q.collection_time <= $3
            AND   q.collection_time >= $2
            GROUP BY q.server_id, q.database_name
        ),
        pairs AS
        (
            SELECT server_id, database_name,
                   COALESCE(SUM(executions), 0)::bigint AS executions, COALESCE(SUM(cpu_us), 0)::bigint AS cpu_us,
                   COALESCE(SUM(elapsed_us), 0)::bigint AS elapsed_us, MIN(first_hour) AS first_hour, MAX(last_hour) AS last_hour
            FROM ({union}SELECT * FROM tail) AS u
            GROUP BY server_id, database_name
        )
        SELECT s.server_name, s.display_name, p.database_name, p.executions, p.cpu_us, p.elapsed_us, p.first_hour, p.last_hour,
               COUNT(*) OVER () AS pairs
        FROM pairs AS p
        JOIN servers AS s ON s.server_id = p.server_id
        WHERE ($6::integer[] IS NULL OR p.server_id = ANY($6))
        ORDER BY p.cpu_us DESC, s.server_name, p.database_name
        LIMIT $5
        """;
    }

    /// <summary>#8570: the fewest whole closed days the daily tier must hold before the mid route is worth its probes. Below it the
    /// two partial edge days are most of the window and the old statement is the same read.</summary>
    public const int MinMidDays = 2;

    /// <summary>
    /// #8570: where the daily tier can stand in for the hourly rows of whole closed days. <see cref="DailyView"/> is the daily
    /// relation (aliased <c>d</c> in <see cref="DailyRelation"/>), <see cref="DStart"/> the first midnight at or after the window
    /// start and <see cref="DEnd"/> the last midnight at or before the window end, the hourly ceiling and the daily ceiling.
    /// </summary>
    internal sealed record DailyMidPlan(string DailyRelation, string DailyView, DateTime DStart, DateTime DEnd);

    /// <summary>
    /// #8570: the coverage guards of the mid route. Null (read the hourly rollup alone, as before) unless every one holds:
    /// <list type="number">
    /// <item>The hourly relation is ONE relation for this window and the daily relation is the SAME family (both successor or both
    /// legacy), also one relation. A stitched hourly relation, or a daily one that reads a different family than the hourly one, has a
    /// day the two tiers disagree about (the day holding the stitch floor takes its hours from both sides).</item>
    /// <item>The daily tier has a measured ceiling, and so does the hourly one. A day is read from the daily only when it ends at or
    /// before both ceilings, so a daily that trails the hourly (the daily refresh runs a day behind) leaves those days to the hourly edge.</item>
    /// <item>A day is read from the daily only when it ended before <c>now - DailyRefreshStartSpan</c> (<paramref name="now"/> cut to its day): the daily
    /// policy re-reads the newer days at its next runs, and a late hourly change to one of them (a repair refreshing the hourly over a late hour) is not in
    /// the daily until then. The hourly edge reads those days, so the reply is the old statement's between the repair and the daily's next run.</item>
    /// <item>At least <see cref="MinMidDays"/> whole days remain.</item>
    /// </list>
    /// The remaining guard, no whole-day hole in the daily across <c>[DStart, DEnd)</c>, needs the store and is
    /// <see cref="DailyHasNoHoleAsync"/>. The daily policy re-reads only <see cref="TimescaleSupport.DailyRefreshStartSpan"/> of days, so
    /// a day the daily never wrote is never written later: that hole check is what stops a day with no daily row being read as an empty day.
    /// A late change older than that window is the repair pass's to chase into the daily (it does so right after the hourly repair).
    /// </summary>
    internal static DailyMidPlan? PlanDailyMid(RollupCoverage coverage, string hourlyRelation, DateTime start, DateTime windowEnd, DateTime? hourlyCeiling, DateTime now)
    {
        if (hourlyCeiling is not DateTime hourlyEnd
            || coverage.StitchFloor(TimescaleSupport.QueryStatsHourlyView, RollupCoverage.StitchTier.Hourly, start) is not null)
        {
            return null;
        }

        var dStart = start == start.Date ? start : start.Date.AddDays(1);
        var dailyRelation = coverage.StitchedRelationSql(TimescaleSupport.QueryStatsDailyView, "d", dStart, RollupCoverage.StitchTier.Daily);
        if (dailyRelation.Contains("UNION", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        var hourlyIsSuccessor = hourlyRelation.Contains("collect." + TimescaleSupport.QueryStatsIntervalHourlyView + " ", StringComparison.Ordinal);
        var dailyIsSuccessor = dailyRelation.Contains("collect." + TimescaleSupport.QueryStatsIntervalDailyView + " ", StringComparison.Ordinal);
        if (hourlyIsSuccessor != dailyIsSuccessor)
        {
            return null;
        }

        var dailyView = dailyIsSuccessor ? TimescaleSupport.QueryStatsIntervalDailyView : TimescaleSupport.QueryStatsDailyView;
        if (coverage.CeilingOf(dailyView) is not DateTime dailyEnd)
        {
            return null;
        }

        /* A day inside the daily policy's re-read window can still change: the policy re-reads it at its next runs, and a repair that refreshes
           the hourly over a late hour of that day leaves the daily stale until then. Those days stay on the hourly edge. */
        var settled = (now - TimescaleSupport.DailyRefreshStartSpanFor(dailyView)).Date;
        var limit = new[] { windowEnd, hourlyEnd, dailyEnd, settled }.Min();
        var dEnd = DateTime.SpecifyKind(limit.Date, DateTimeKind.Unspecified);
        return (dEnd - dStart).TotalDays >= MinMidDays ? new DailyMidPlan(dailyRelation, dailyView, dStart, dEnd) : null;
    }

    /// <summary>
    /// #8570: true when every whole day in <c>[plan.DStart, plan.DEnd)</c> has a daily bucket, or the hourly rollup holds nothing for it
    /// either: the repair pass's own hole scan (<see cref="TimescaleSupport.HoleFreeThroughAsync"/>), one range probe per day.
    /// </summary>
    internal static async Task<bool> DailyHasNoHoleAsync(NpgsqlDataSource postgres, DailyMidPlan plan, CancellationToken cancellationToken)
    {
        var target = TimescaleSupport.RollupCoverageProbeTargets.Single(t => string.Equals(t.View, plan.DailyView, StringComparison.Ordinal));
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        return await TimescaleSupport.HoleFreeThroughAsync(
            connection, target, ("collect", plan.DailyView), plan.DStart, plan.DEnd, Array.Empty<(DateTime Start, DateTime End)>(), cancellationToken);
    }

    /// <summary>
    /// #8570: the fleet statement with the closed whole days read from the daily rollup. Same parameters and rows as
    /// <see cref="BuildSql"/> plus $7 (<see cref="DailyMidPlan.DStart"/>) and $8 (<see cref="DailyMidPlan.DEnd"/>):
    /// <c>edge_start</c> is the hourly rows in <c>[$2, $7)</c>, <c>mid</c> the daily rows in <c>[$7, $8)</c>, <c>edge_end</c> the hourly
    /// rows in <c>[$8, $3)</c> below the ceiling $4. A daily row carries no hour, so first_hour and last_hour of a pair that is only in
    /// the mid are two ordered LIMIT 1 probes of the hourly rollup inside its first and last daily day (one index step each). The pair's
    /// first_hour is then the least of its parts and last_hour the greatest, as the one-relation statement computes it.
    /// </summary>
    public static string BuildMidSql(string hourlyRelation, string dailyRelation)
        => $"""
        WITH edge_start AS
        (
            SELECT f.server_id, f.database_name,
                   SUM(f.execution_count_sum) AS executions, SUM(f.worker_time_sum) AS cpu_us, SUM(f.elapsed_time_sum) AS elapsed_us,
                   MIN(f.bucket) AS first_hour, MAX(f.bucket) AS last_hour
            FROM {hourlyRelation}
            WHERE f.query_hash = $1
            AND   f.bucket >= $2
            AND   f.bucket < $7
            GROUP BY f.server_id, f.database_name
        ),
        edge_end AS
        (
            SELECT f.server_id, f.database_name,
                   SUM(f.execution_count_sum) AS executions, SUM(f.worker_time_sum) AS cpu_us, SUM(f.elapsed_time_sum) AS elapsed_us,
                   MIN(f.bucket) AS first_hour, MAX(f.bucket) AS last_hour
            FROM {hourlyRelation}
            WHERE f.query_hash = $1
            AND   f.bucket >= $8
            AND   f.bucket < $3
            AND   f.bucket < $4
            GROUP BY f.server_id, f.database_name
        ),
        mid AS
        (
            SELECT d.server_id, d.database_name,
                   SUM(d.execution_count_sum) AS executions, SUM(d.worker_time_sum) AS cpu_us, SUM(d.elapsed_time_sum) AS elapsed_us,
                   MIN(d.bucket) AS first_day, MAX(d.bucket) AS last_day
            FROM {dailyRelation}
            WHERE d.query_hash = $1
            AND   d.bucket >= $7
            AND   d.bucket < $8
            GROUP BY d.server_id, d.database_name
        ),
        mid_edges AS
        (
            SELECT m.server_id, m.database_name, m.executions, m.cpu_us, m.elapsed_us, m.first_day, m.last_day,
                   EXISTS (SELECT 1 FROM edge_start AS es WHERE es.server_id = m.server_id AND es.database_name IS NOT DISTINCT FROM m.database_name) AS in_start,
                   EXISTS (SELECT 1 FROM edge_end AS ee WHERE ee.server_id = m.server_id AND ee.database_name IS NOT DISTINCT FROM m.database_name) AS in_end
            FROM mid AS m
        ),
        probe AS
        (
            SELECT m.server_id, m.database_name, m.executions, m.cpu_us, m.elapsed_us, fh.bucket AS first_hour, lh.bucket AS last_hour
            FROM mid_edges AS m
            LEFT JOIN LATERAL
            (
                SELECT f.bucket FROM {hourlyRelation}
                WHERE NOT m.in_start
                AND   f.query_hash = $1 AND f.server_id = m.server_id AND f.database_name IS NOT DISTINCT FROM m.database_name
                AND   f.bucket >= m.first_day AND f.bucket < m.first_day + interval '1 day'
                ORDER BY f.bucket LIMIT 1
            ) AS fh ON true
            LEFT JOIN LATERAL
            (
                SELECT f.bucket FROM {hourlyRelation}
                WHERE NOT m.in_end
                AND   f.query_hash = $1 AND f.server_id = m.server_id AND f.database_name IS NOT DISTINCT FROM m.database_name
                AND   f.bucket >= m.last_day AND f.bucket < m.last_day + interval '1 day'
                ORDER BY f.bucket DESC LIMIT 1
            ) AS lh ON true
        ),
        tail AS
        (
            SELECT q.server_id, q.database_name,
                   SUM(q.delta_execution_count) AS executions, SUM(q.delta_worker_time) AS cpu_us, SUM(q.delta_elapsed_time) AS elapsed_us,
                   MIN(date_trunc('hour', q.collection_time)) AS first_hour, MAX(date_trunc('hour', q.collection_time)) AS last_hour
            FROM query_stats AS q
            WHERE q.query_hash = $1
            AND   q.collection_time >= $4
            AND   q.collection_time <= $3
            AND   q.collection_time >= $2
            GROUP BY q.server_id, q.database_name
        ),
        pairs AS
        (
            SELECT server_id, database_name,
                   COALESCE(SUM(executions), 0)::bigint AS executions, COALESCE(SUM(cpu_us), 0)::bigint AS cpu_us,
                   COALESCE(SUM(elapsed_us), 0)::bigint AS elapsed_us, MIN(first_hour) AS first_hour, MAX(last_hour) AS last_hour
            FROM (SELECT * FROM edge_start UNION ALL SELECT * FROM probe UNION ALL SELECT * FROM edge_end UNION ALL SELECT * FROM tail) AS u
            GROUP BY server_id, database_name
        )
        SELECT s.server_name, s.display_name, p.database_name, p.executions, p.cpu_us, p.elapsed_us, p.first_hour, p.last_hour,
               COUNT(*) OVER () AS pairs
        FROM pairs AS p
        JOIN servers AS s ON s.server_id = p.server_id
        WHERE ($6::integer[] IS NULL OR p.server_id = ANY($6))
        ORDER BY p.cpu_us DESC, s.server_name, p.database_name
        LIMIT $5
        """;

    /// <summary>The label every row of the Query Store read carries.</summary>
    public const string QueryStoreSource = "query_store";

    /// <summary>#7609: the most hours the Query Store source reads. A longer window answers the plan cache in full and says the Query Store part was cut.</summary>
    public const int QueryStoreMaxHours = 168;

    /// <summary>
    /// The Query Store fleet statement (#7609), same row shape as <see cref="BuildSql"/>: server_name, display_name, database_name, executions,
    /// cpu_us, elapsed_us, first_hour, last_hour, pairs. It reads <c>query_store_stats_corrected_hourly</c> (never <c>query_store_stats_hourly</c>,
    /// which counts one interval's work once per collection) for the buckets below the rollup's materialization boundary $4, and the
    /// interval-deduped raw rows at or above it, the split <see cref="QueryStoreTrendRouting.BuildRollupTrendSql"/> makes, so an hour is never
    /// counted twice. With <paramref name="useRollup"/> false the whole window is raw (a store with no rollup keeps its raw rows).
    /// $1 query_hash, $2/$3 window (naive UTC), $4 the instant raw takes over, $5 the row cap.
    /// <para>The deduped raw arm is a copy of the <c>placed</c> arm in <see cref="QueryStoreTrendRouting.BuildRollupTrendSql"/>, with <c>server_id</c> added
    /// to the dedup partition because this read has no server filter. That arm is welded to its own <c>server_id = $1</c> filter, optional database
    /// filter and column list, so sharing it would mean reshaping the statement <c>get_query_store_duration_trend</c> runs (#7667).</para>
    /// </summary>
    public static string BuildQueryStoreSql(bool useRollup)
    {
        var hourly = useRollup
            ? $"""
            hourly AS
            (
                SELECT f.server_id, f.database_name,
                       SUM(f.execution_count_sum)::double precision AS executions, SUM(f.cpu_us_weighted_sum) AS cpu_us, SUM(f.duration_us_weighted_sum) AS elapsed_us,
                       MIN(f.bucket) AS first_hour, MAX(f.bucket) AS last_hour
                FROM {TimescaleSupport.QueryStoreStatsCorrectedHourlyView} AS f
                WHERE f.query_hash = $1
                AND   f.bucket >= $2
                AND   f.bucket < $3
                AND   f.bucket < $4
                GROUP BY f.server_id, f.database_name
            ),
            """
            : "";
        var union = useRollup ? "SELECT * FROM hourly UNION ALL " : "";

        return $"""
        WITH {hourly}
        placed AS
        (
            SELECT server_id, database_name, interval_start_time_utc AS point_time, execution_count, avg_duration_us, avg_cpu_time_us
            FROM
            (
                SELECT server_id, database_name, interval_start_time_utc, execution_count, avg_duration_us, avg_cpu_time_us,
                       ROW_NUMBER() OVER
                       (
                           PARTITION BY server_id, database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                           ORDER BY collection_time DESC, execution_count DESC
                       ) AS rn
                FROM query_store_stats
                WHERE query_hash = $1
                AND   interval_start_time_utc >= $2
                AND   interval_start_time_utc >= $4
                AND   interval_start_time_utc <= $3
                AND   collection_time >= GREATEST($2, $4) - interval '1 hour'
                AND   collection_time <= $3 + interval '1 day'
                AND   interval_start_time_utc IS NOT NULL
            ) AS identified
            WHERE rn = 1

            UNION ALL

            SELECT server_id, database_name, collection_time AS point_time, execution_count, avg_duration_us, avg_cpu_time_us
            FROM query_store_stats
            WHERE query_hash = $1
            AND   collection_time >= $2
            AND   collection_time >= $4
            AND   collection_time <= $3
            AND   interval_start_time_utc IS NULL
        ),
        tail AS
        (
            SELECT server_id, database_name,
                   SUM(execution_count)::double precision AS executions,
                   SUM(execution_count * avg_cpu_time_us::double precision) AS cpu_us,
                   SUM(execution_count * avg_duration_us::double precision) AS elapsed_us,
                   MIN(date_trunc('hour', point_time)) AS first_hour, MAX(date_trunc('hour', point_time)) AS last_hour
            FROM placed
            GROUP BY server_id, database_name
        ),
        pairs AS
        (
            SELECT server_id, database_name,
                   COALESCE(SUM(executions), 0)::bigint AS executions, COALESCE(SUM(cpu_us), 0)::bigint AS cpu_us,
                   COALESCE(SUM(elapsed_us), 0)::bigint AS elapsed_us, MIN(first_hour) AS first_hour, MAX(last_hour) AS last_hour
            FROM ({union}SELECT * FROM tail) AS u
            GROUP BY server_id, database_name
        )
        SELECT s.server_name, s.display_name, p.database_name, p.executions, p.cpu_us, p.elapsed_us, p.first_hour, p.last_hour,
               COUNT(*) OVER () AS pairs
        FROM pairs AS p
        JOIN servers AS s ON s.server_id = p.server_id
        WHERE ($6::integer[] IS NULL OR p.server_id = ANY($6))
        ORDER BY p.cpu_us DESC, s.server_name, p.database_name
        LIMIT $5
        """;
    }

    private sealed record FleetRow(string Server, string? Display, string Database, string Source, long Executions, long CpuUs, long ElapsedUs, DateTime First, DateTime Last);

    private static async Task<(List<FleetRow> Rows, int Pairs)> ReadFleetAsync(
        NpgsqlDataSource postgres, string sql, string source, string hash, DateTime start, DateTime windowEnd, DateTime rawFrom, int limit,
        CancellationToken cancellationToken, DailyMidPlan? mid = null)
    {
        var rows = new List<FleetRow>();
        var pairs = 0;
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddText(command, hash);
        DarlingMcpReadParameters.AddTimestamp(command, start);
        DarlingMcpReadParameters.AddTimestamp(command, windowEnd);
        DarlingMcpReadParameters.AddTimestamp(command, rawFrom);
        DarlingMcpReadParameters.AddInt(command, limit);
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, (object?)DarlingServerResolver.WebAllowedServerIds?.ToArray() ?? DBNull.Value);
        if (mid is not null)
        {
            DarlingMcpReadParameters.AddTimestamp(command, mid.DStart);
            DarlingMcpReadParameters.AddTimestamp(command, mid.DEnd);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new FleetRow(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.IsDBNull(2) ? "" : reader.GetString(2), source,
                reader.GetInt64(3), reader.GetInt64(4), reader.GetInt64(5),
                DateTime.SpecifyKind(reader.GetDateTime(6), DateTimeKind.Utc), DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc)));
            pairs = (int)reader.GetInt64(8);
        }

        return (rows, pairs);
    }

    [McpServerTool(Name = "get_query_fleet_summary"), Description("Where one statement runs and what it costs on every monitored server, in one read: one entry per server, database and source (plan_cache or query_store) for a query_hash, ranked by CPU. Use it instead of calling get_query_trend or get_top_queries_by_cpu once per server. There is no server_name: it reads the whole fleet. <<GUIDE>> Where one statement runs and what it costs on every monitored server, in one read: one entry per server, database and source (plan_cache or query_store) for a query_hash, ranked by CPU. Use it instead of calling get_query_trend or get_top_queries_by_cpu once per server. There is no server_name: it reads the whole fleet. Give a query_hash (from get_top_queries_by_cpu, get_query_store_top or a plan). Each entry has source, execution_count, cpu_ms, elapsed_ms, avg_cpu_ms, avg_elapsed_ms and the first and last hour the statement was seen. hours_back reaches 2160 (90 days), default 168; a 42-target store answered in 4.3 s (7 days), 5.8 s (28 days) and 9.0 s (90 days) with a cold cache. Reads the plan cache's hourly tier, the tier get_query_trend reads, with the newest hours the rollup has not built yet summed from the raw rows, so no hour is counted twice and none is left out. It also reads Query Store (the corrected hourly rollup plus the raw tail) for up to 7 days: each row has a source of plan_cache or query_store, and a longer window answers the plan cache in full and says the Query Store part was cut to 7 days. limit defaults to 50 and reaches 200: server_database_pairs counts every server, database and source entry in the window (a server and database present in both sources counts once per source), and truncated is true when limit cut entries. A statement that ran nowhere in the window answers status empty.")]
    public static async Task<string> GetQueryFleetSummary(
        NpgsqlDataSource postgres,
        [Description("The query_hash value from get_top_queries_by_cpu, get_query_store_top or a plan.")] string query_hash,
        [Description("Hours of history. Default 168.")] int hours_back = DefaultHoursBack,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Maximum server and database rows, ranked by CPU. Default 50, most 200.")] int limit = DefaultLimit,
        CancellationToken cancellationToken = default)
    {
        var hash = (query_hash ?? string.Empty).Trim();
        if (hash.Length == 0)
        {
            return McpHelpers.Refusal("query_hash", "query_hash is required: the hash of the statement to look for.");
        }

        if (limit < 1 || limit > MaxLimit)
        {
            return McpHelpers.Refusal("limit", $"Invalid limit value '{limit}'. Must be 1-{MaxLimit}.");
        }

        var validation = DarlingReadWindow.Validate(hours_back, as_of, WebReadReach.MaxHoursFor("get_query_fleet_summary"), out var windowEnd);
        if (validation != null) return validation;

        /* Observe the token before the first store call, as the other converted reads do (WebReadCancellationPinTests). */
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            var start = windowEnd.AddHours(-hours_back);
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, cancellationToken);

            /* The tier decision get_query_trend makes, over the same legacy pair. */
            var tier = DarlingTrendReader.ResolveTierAtWallClock(
                start, rollups.QueryGrainHourly,
                coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView));

            string? hourlyRelation = null;
            var ceilingClause = "";
            DailyMidPlan? mid = null;
            var rawFrom = start;
            if (tier != RetentionTier.Raw)
            {
                hourlyRelation = coverage.StitchedRelationSql(TimescaleSupport.QueryStatsHourlyView, "f", start, RollupCoverage.StitchTier.Hourly);
                if (coverage.HourlyEndCeiling(TimescaleSupport.QueryStatsHourlyView, start) is DateTime ceiling)
                {
                    ceilingClause = RollupCoverage.HourlyCeilingClause(4);
                    rawFrom = ceiling > start ? ceiling : start;

                    /* #8570: whole closed days come from the daily tier. The hourly rollup reads one heap page per row on a cold cache
                       (and decompresses a day's batches on a compressed chunk), the daily rollup 24 times fewer rows. Any guard that
                       fails leaves the hourly read alone. The re-read guard measures against the real clock, not the as_of window end:
                       the daily refresh policy runs on the real clock, so a day is settled by when the policy last ran. */
                    mid = PlanDailyMid(coverage, hourlyRelation, start, windowEnd, ceiling, TimeProvider.System.GetUtcNow().UtcDateTime);
                    if (mid is not null && !await DailyHasNoHoleAsync(postgres, mid, cancellationToken))
                    {
                        mid = null;
                    }
                }
                else
                {
                    /* No measured ceiling means no bound on the rollup, so raw adds nothing. */
                    rawFrom = windowEnd.AddHours(1);
                }
            }

            var (planRows, planPairs) = await ReadFleetAsync(
                postgres, mid is null ? BuildSql(hourlyRelation, ceilingClause) : BuildMidSql(hourlyRelation!, mid.DailyRelation),
                PlanCacheSource, hash, start, windowEnd, rawFrom, limit, cancellationToken, mid);

            /* #7609: the Query Store source, capped at seven days. It reads the corrected hourly rollup below the rollup's boundary and the
               deduped raw rows from it up, the split get_query_store_duration_trend makes. */
            var qsHours = hours_back > QueryStoreMaxHours ? QueryStoreMaxHours : hours_back;
            var qsStart = windowEnd.AddHours(-qsHours);
            var qsRoute = await QueryStoreTrendRouting.ResolveAsync(coverage, postgres, cancellationToken);
            var (qsRows, qsPairs) = await ReadFleetAsync(
                postgres, BuildQueryStoreSql(qsRoute.UseRollup), QueryStoreSource, hash, qsStart, windowEnd,
                qsRoute.UseRollup ? qsRoute.RawStartUtc : qsStart, limit, cancellationToken);
            var qsCut = hours_back > QueryStoreMaxHours;
            var qsUnservedBefore = QueryStoreTrendRouting.UnservedBefore(qsRoute, qsStart);

            var rows = planRows.Concat(qsRows).OrderByDescending(r => r.CpuUs).ThenBy(r => r.Server, StringComparer.Ordinal)
                .ThenBy(r => r.Database, StringComparer.Ordinal).ThenBy(r => r.Source, StringComparer.Ordinal).Take(limit).ToList();
            var pairs = planPairs + qsPairs;

            var sourceWord = tier == RetentionTier.Raw ? "raw" : "hourly";
            var qsNote = qsCut
                ? $"The Query Store source was cut to {QueryStoreMaxHours} hours (7 days), not the {hours_back} asked for. The plan-cache source covers the whole window."
                : null;
            if (rows.Count == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    $"query_hash '{hash}' has no plan-cache or Query Store history on any monitored server in the {hours_back} hours ending {windowEnd:yyyy-MM-ddTHH:mm:ssZ} " +
                    $"(the plan-cache {sourceWord} tier was read; Query Store was read for {qsHours} hours). " +
                    "Confirm the hash with get_top_queries_by_cpu before concluding the statement did not run." + (qsNote is null ? "" : " " + qsNote));
            }

            return JsonSerializer.Serialize(new
            {
                query_hash = hash,
                hours_back,
                as_of = windowEnd.ToString("o", CultureInfo.InvariantCulture),
                /* The asked-for start, not the first row served: this read sends no window notice (tracked in #7010), so it must not claim coverage with an effective_start. */
                requested_start = start.ToString("o", CultureInfo.InvariantCulture),
                source = sourceWord,
                sources = new[] { PlanCacheSource, QueryStoreSource },
                query_store = new
                {
                    hours_read = qsHours,
                    cut_to_7_days = qsCut,
                    note = qsNote,
                    unserved_before = qsUnservedBefore?.ToString("o", CultureInfo.InvariantCulture),
                    rows = qsRows.Count,
                },
                server_database_pairs = pairs,
                servers_returned = rows.Select(r => r.Server).Distinct(StringComparer.Ordinal).Count(),
                rows_returned = rows.Count,
                limit,
                truncated = pairs > rows.Count,
                ordering = "cpu_ms descending",
                note = pairs > rows.Count
                    ? $"limit {limit} cut the list: {rows.Count} of {pairs} server, database and source rows are shown, the heaviest by CPU. Raise limit (most {MaxLimit}) to see more."
                    : null,
                servers = rows.Select(r => new
                {
                    server_name = r.Server,
                    display_name = r.Display,
                    database_name = r.Database,
                    source = r.Source,
                    execution_count = r.Executions,
                    cpu_ms = Math.Round(r.CpuUs / 1000.0, 2),
                    elapsed_ms = Math.Round(r.ElapsedUs / 1000.0, 2),
                    avg_cpu_ms = Math.Round(r.Executions > 0 ? r.CpuUs / 1000.0 / r.Executions : 0, 2),
                    avg_elapsed_ms = Math.Round(r.Executions > 0 ? r.ElapsedUs / 1000.0 / r.Executions : 0, 2),
                    first_hour = r.First.ToString("o", CultureInfo.InvariantCulture),
                    last_hour = r.Last.ToString("o", CultureInfo.InvariantCulture),
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_query_fleet_summary", ex);
        }
    }
}
