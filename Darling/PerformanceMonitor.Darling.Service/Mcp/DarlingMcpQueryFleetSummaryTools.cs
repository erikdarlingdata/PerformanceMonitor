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
        ORDER BY p.cpu_us DESC, s.server_name, p.database_name
        LIMIT $5
        """;
    }

    [McpServerTool(Name = "get_query_fleet_summary"), Description("Where one statement runs and what it costs on every monitored server, in one read: one row per server and database for a query_hash, ranked by CPU. Use it instead of calling get_query_trend or get_top_queries_by_cpu once per server. There is no server_name: it reads the whole fleet. <<GUIDE>> Where one statement runs and what it costs on every monitored server, in one read: one row per server and database for a query_hash, ranked by CPU. Use it instead of calling get_query_trend or get_top_queries_by_cpu once per server. There is no server_name: it reads the whole fleet. Give a query_hash (from get_top_queries_by_cpu, get_query_store_top or a plan). Each row has executions, cpu_ms, elapsed_ms, avg_cpu_ms, avg_elapsed_ms and the first and last hour the statement was seen. hours_back reaches 2160 (90 days), default 168; a 42-target store answered in 4.3 s (7 days), 5.8 s (28 days) and 9.0 s (90 days) with a cold cache. Reads the plan cache's hourly tier, the tier get_query_trend reads, with the newest hours the rollup has not built yet summed from the raw rows, so no hour is counted twice and none is left out. limit defaults to 50 and reaches 200: server_database_pairs counts every pair in the window, and truncated is true when limit cut rows. A statement that ran nowhere in the window answers status empty.")]
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

        try
        {
            var start = windowEnd.AddHours(-hours_back);
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, cancellationToken);

            /* The tier decision get_query_trend makes, over the same legacy pair. */
            var tier = DarlingTrendReader.ResolveTier(
                start, DateTime.UtcNow, rollups.QueryGrainHourly,
                coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView));

            string? hourlyRelation = null;
            var ceilingClause = "";
            var rawFrom = start;
            if (tier != RetentionTier.Raw)
            {
                hourlyRelation = coverage.StitchedRelationSql(TimescaleSupport.QueryStatsHourlyView, "f", start, RollupCoverage.StitchTier.Hourly);
                if (coverage.HourlyEndCeiling(TimescaleSupport.QueryStatsHourlyView, start) is DateTime ceiling)
                {
                    ceilingClause = RollupCoverage.HourlyCeilingClause(4);
                    rawFrom = ceiling > start ? ceiling : start;
                }
                else
                {
                    /* No measured ceiling means no bound on the rollup, so raw adds nothing. */
                    rawFrom = windowEnd.AddHours(1);
                }
            }

            var rows = new List<(string Server, string? Display, string Database, long Executions, long CpuUs, long ElapsedUs, DateTime First, DateTime Last)>();
            var pairs = 0;
            await using (var command = postgres.CreateCommand(BuildSql(hourlyRelation, ceilingClause)))
            {
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                DarlingMcpReadParameters.AddText(command, hash);
                DarlingMcpReadParameters.AddTimestamp(command, start);
                DarlingMcpReadParameters.AddTimestamp(command, windowEnd);
                DarlingMcpReadParameters.AddTimestamp(command, rawFrom);
                DarlingMcpReadParameters.AddInt(command, limit);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    rows.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.IsDBNull(2) ? "" : reader.GetString(2),
                        reader.GetInt64(3), reader.GetInt64(4), reader.GetInt64(5),
                        DateTime.SpecifyKind(reader.GetDateTime(6), DateTimeKind.Utc), DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc)));
                    pairs = (int)reader.GetInt64(8);
                }
            }

            var sourceWord = tier == RetentionTier.Raw ? "raw" : "hourly";
            if (rows.Count == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    $"query_hash '{hash}' has no plan-cache history on any monitored server in the {hours_back} hours ending {windowEnd:yyyy-MM-ddTHH:mm:ssZ} " +
                    $"(the {sourceWord} tier was read). Confirm the hash with get_top_queries_by_cpu before concluding the statement did not run.");
            }

            return JsonSerializer.Serialize(new
            {
                query_hash = hash,
                hours_back,
                as_of = windowEnd.ToString("o", CultureInfo.InvariantCulture),
                effective_start = start.ToString("o", CultureInfo.InvariantCulture),
                source = sourceWord,
                server_database_pairs = pairs,
                servers_returned = rows.Select(r => r.Server).Distinct(StringComparer.Ordinal).Count(),
                rows_returned = rows.Count,
                limit,
                truncated = pairs > rows.Count,
                ordering = "cpu_ms descending",
                note = pairs > rows.Count
                    ? $"limit {limit} cut the list: {rows.Count} of {pairs} server and database pairs are shown, the heaviest by CPU. Raise limit (most {MaxLimit}) to see more."
                    : null,
                servers = rows.Select(r => new
                {
                    server_name = r.Server,
                    display_name = r.Display,
                    database_name = r.Database,
                    source = PlanCacheSource,
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
