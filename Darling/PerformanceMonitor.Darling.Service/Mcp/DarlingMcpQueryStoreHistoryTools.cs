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
/// #5234: one Query Store query's history, plan by plan -- the read behind the web grid's History button, twin of the
/// desktop <c>QueryStoreHistoryWindow</c>. Store-only: it reads the collector's <c>query_store_stats</c> snapshots and
/// never connects to the monitored server. It reads the raw tier only (the corrected rollups carry no plan_id), so a
/// window longer than raw retention is cut short and <c>window_truncated</c> says so.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpQueryStoreHistoryTools
{
    /// <summary>
    /// The most per-interval points one answer carries; <c>points_truncated</c> marks a cut, and the NEWEST points are kept.
    /// A point serializes to 130-160 bytes (157 with every field at full width), so 150 points is about 23 KB: with <see cref="MaxPlans"/>
    /// plans of about 310 bytes each the answer stays under <see cref="McpResponseBudget.DefaultBytes"/>. <see cref="PointByteBudget"/>
    /// is the guard behind it for unusually wide values.
    /// </summary>
    internal const int MaxPoints = 150;

    /// <summary>The most plans one answer lists, by total duration; <c>plans_truncated</c> marks a cut.</summary>
    internal const int MaxPlans = 20;

    /// <summary>Bytes the points may take: the response budget less room for the envelope and a full plans list.</summary>
    internal const int PointByteBudget = McpResponseBudget.DefaultBytes - 8 * 1024;

    /// <summary>
    /// The newest points that fit <see cref="MaxPoints"/> and <paramref name="byteBudget"/>, oldest first. <paramref name="rows"/>
    /// is ordered oldest first; the cut drops the oldest.
    /// </summary>
    internal static List<T> NewestWithinBudget<T>(IReadOnlyList<T> rows, Func<T, int> byteLength, int maxPoints, int byteBudget)
    {
        var kept = new List<T>();
        var used = 0;
        for (var i = rows.Count - 1; i >= 0 && kept.Count < maxPoints; i--)
        {
            var size = byteLength(rows[i]) + 1;
            if (used + size > byteBudget) break;
            used += size;
            kept.Add(rows[i]);
        }
        kept.Reverse();
        return kept;
    }

    /// <summary>
    /// Deduped per interval, as #1841 requires: the collector re-fetches the open interval every cycle, so the same
    /// interval is stored repeatedly with a growing execution_count. The partition is the one
    /// <c>DarlingDataReader.QueryStoreTopRawPrefix</c> uses.
    /// </summary>
    internal const string HistorySql = """
        WITH deduped AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                                         ORDER BY collection_time DESC, execution_count DESC) AS rn
            FROM query_store_stats
            WHERE server_id = $1 AND database_name = $2 AND query_id = $3
            AND collection_time >= $4 AND collection_time <= $5)
        SELECT plan_id, collection_time, execution_count,
               CAST(avg_duration_us AS double precision) / 1000.0,
               CAST(avg_cpu_time_us AS double precision) / 1000.0,
               first_execution_time, last_execution_time, is_forced_plan
        FROM deduped WHERE rn = 1 ORDER BY collection_time
        """;

    internal readonly record struct HistoryRow(
        long PlanId, DateTime CollectionTime, long ExecutionCount, double? AvgDurationMs, double? AvgCpuMs,
        DateTime? FirstExecutionTime, DateTime? LastExecutionTime, bool IsForcedPlan);

    internal sealed record PlanTotals(
        long PlanId, long ExecutionCount, double? AvgDurationMs, double TotalDurationMs, double? AvgCpuMs, double TotalCpuMs,
        DateTime? FirstExecutionTime, DateTime? LastExecutionTime, bool IsForcedPlan);

    /// <summary>
    /// Folds snapshot rows into one line per plan: totals are the sum of execution_count x average, and the average is
    /// that total over the summed executions (execution-weighted, which can differ from the grid's average of averages).
    /// </summary>
    internal static List<PlanTotals> FoldPlans(IEnumerable<HistoryRow> rows) =>
        rows.GroupBy(r => r.PlanId).OrderBy(g => g.Key).Select(g =>
        {
            var execs = g.Sum(r => r.ExecutionCount);
            var totalDuration = g.Sum(r => r.ExecutionCount * (r.AvgDurationMs ?? 0));
            var totalCpu = g.Sum(r => r.ExecutionCount * (r.AvgCpuMs ?? 0));
            return new PlanTotals(
                g.Key, execs,
                execs > 0 ? totalDuration / execs : null, totalDuration,
                execs > 0 ? totalCpu / execs : null, totalCpu,
                g.Min(r => r.FirstExecutionTime), g.Max(r => r.LastExecutionTime), g.Any(r => r.IsForcedPlan));
        }).ToList();

    [McpServerTool(Name = "get_query_store_query_history"), Description(
        "One Query Store query's history, plan by plan: per-plan executions, duration and CPU, plus a per-interval series. window_truncated marks a raw-tier floor. <<GUIDE>> " +
        "Per-plan averages are execution-weighted (total over executions), so they can differ from get_query_store_top, which averages the interval averages. " +
        "Reads the raw tier only; effective_start is where complete history begins. points keeps the newest 150 (points_truncated) and plans the 20 longest by total duration (plans_truncated).")]
    public static async Task<string> GetQueryStoreQueryHistory(
        NpgsqlDataSource postgres,
        [Description("The database_name from get_query_store_top.")] string database_name,
        [Description("The query_id from get_query_store_top.")] long query_id,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history ending at as_of (default 24).")] int hours_back = 24,
        [Description("Window end, ISO 8601 UTC. Default: now.")] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        var start = windowEnd.AddHours(-hours_back);

        try
        {
            var rows = new List<HistoryRow>();
            await using (var command = postgres.CreateCommand(HistorySql))
            {
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                command.Parameters.AddWithValue(resolved.ServerId);
                command.Parameters.AddWithValue(database_name);
                command.Parameters.AddWithValue(query_id);
                /* #1969: the columns are timestamp without time zone; a Utc-kinded bind is refused or shifted. */
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified));
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    rows.Add(new HistoryRow(
                        reader.GetInt64(0), reader.GetDateTime(1), reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                        reader.IsDBNull(3) ? null : reader.GetDouble(3), reader.IsDBNull(4) ? null : reader.GetDouble(4),
                        reader.IsDBNull(5) ? null : reader.GetDateTime(5), reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                        !reader.IsDBNull(7) && reader.GetBoolean(7)));
                }
            }

            /* #4966: probe the floor before the empty check, so an empty answer over a cut window says the window was cut. */
            var floor = await DarlingDataReader.GetQueryStoreWindowFloorAsync(postgres, resolved.ServerId, start, windowEnd, cancellationToken);

            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_store", cancellationToken)
                    ?? McpHelpers.Status("empty",
                        $"No Query Store history for query_id {query_id} in database '{database_name}' in the {hours_back}-hour window searched.",
                        DarlingMcpWindowNotice.Build(floor, start, "query_store_stats", emptyAnswer: true).AsHints());

            var effectiveStart = RawWindowFloor.EffectiveStart(floor, start);
            var truncated = RawWindowFloor.IsTruncated(floor, start);

            var allPlans = FoldPlans(rows);
            var plansTruncated = allPlans.Count > MaxPlans;
            var plans = plansTruncated ? allPlans.OrderByDescending(p => p.TotalDurationMs).Take(MaxPlans).OrderBy(p => p.PlanId).ToList() : allPlans;
            var allPoints = rows.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o", CultureInfo.InvariantCulture),
                plan_id = r.PlanId,
                execution_count = r.ExecutionCount,
                avg_duration_ms = r.AvgDurationMs,
                avg_cpu_ms = r.AvgCpuMs
            }).ToList();
            var points = NewestWithinBudget(allPoints, p => JsonSerializer.SerializeToUtf8Bytes(p, McpHelpers.JsonOptions).Length, MaxPoints, PointByteBudget);
            var pointsTruncated = points.Count < allPoints.Count;
            return JsonSerializer.Serialize(new
            {
                database_name,
                query_id,
                effective_start = McpHelpers.FormatEffectiveStart(effectiveStart),
                window_truncated = truncated,
                plans = plans.Select(p => new
                {
                    plan_id = p.PlanId,
                    execution_count = p.ExecutionCount,
                    avg_duration_ms = p.AvgDurationMs,
                    total_duration_ms = p.TotalDurationMs,
                    avg_cpu_ms = p.AvgCpuMs,
                    total_cpu_ms = p.TotalCpuMs,
                    first_execution_time = p.FirstExecutionTime?.ToString("o", CultureInfo.InvariantCulture),
                    last_execution_time = p.LastExecutionTime?.ToString("o", CultureInfo.InvariantCulture),
                    is_forced_plan = p.IsForcedPlan
                }),
                plans_truncated = plansTruncated,
                points,
                points_truncated = pointsTruncated
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_query_store_query_history", ex);
        }
    }
}
