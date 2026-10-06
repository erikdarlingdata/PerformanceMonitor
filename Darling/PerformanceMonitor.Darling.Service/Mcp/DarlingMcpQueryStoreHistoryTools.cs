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
using System.Text.Json.Serialization;
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
/// never connects to the monitored server. It follows the grid's own tier choice (<c>get_query_store_top</c>): the raw
/// tier, or <c>query_store_interval_wide</c> when the window is long enough and the grid's gate says that table holds
/// it, so History never covers less than the grid does. <c>window_truncated</c> says where stored history starts.
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
    /// The read's order, a total one over the deduped rows (every column of the dedupe's partition plus the collection
    /// time), so rows at one instant always come back in the same order and a cut keeps the same ones every time (#5300).
    /// </summary>
    internal const string HistoryOrder =
        "ORDER BY collection_time, plan_id, runtime_stats_interval_id, execution_type_desc, replica_role, first_execution_time";

    /// <summary>
    /// Deduped per interval, as #1841 requires: the collector re-fetches the open interval every cycle, so the same
    /// interval is stored repeatedly with a growing execution_count. Only the latest snapshot of an interval is kept
    /// (the newest collection_time, then the largest execution_count), so no total adds one interval twice. The
    /// partition is the one <c>DarlingDataReader.QueryStoreTopRawPrefix</c> uses; PostgreSQL matches NULL to NULL in it.
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
        FROM deduped WHERE rn = 1
        """ + " " + HistoryOrder;

    /// <summary>
    /// The interval table's twin of <see cref="HistorySql"/> (#5300), the tier the grid reads below raw retention:
    /// <c>query_store_interval_wide</c> already holds ONE row per interval identity, its latest snapshot (the dedupe above,
    /// kept as the table is written), so this reads it directly. $4 is the gate's <c>ReadStart</c>. The
    /// <c>first_execution_time</c> floor is the grid's own (#4605): it filters index entries before the heap and drops
    /// no row. A static readonly because the interval literal is derived from a TimeSpan; <c>$$"""</c> keeps <c>$1</c> literal.
    /// </summary>
    internal static readonly string HistoryTableSql = $$"""
        SELECT plan_id, collection_time, execution_count,
               CAST(avg_duration_us AS double precision) / 1000.0,
               CAST(avg_cpu_time_us AS double precision) / 1000.0,
               first_execution_time, last_execution_time, is_forced_plan
        FROM query_store_interval_wide
        WHERE server_id = $1 AND database_name = $2 AND query_id = $3
        AND collection_time >= $4 AND collection_time <= $5
        AND first_execution_time >= $4 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
        """ + " " + HistoryOrder;

    private const string SetTransactionReadOnlySql = "SET TRANSACTION READ ONLY";

    internal readonly record struct HistoryRow(
        long PlanId, DateTime CollectionTime, long ExecutionCount, double? AvgDurationMs, double? AvgCpuMs,
        DateTime? FirstExecutionTime, DateTime? LastExecutionTime, bool IsForcedPlan);

    internal sealed record PlanTotals(
        long PlanId, long ExecutionCount, double? AvgDurationMs, double TotalDurationMs, double? AvgCpuMs, double TotalCpuMs,
        DateTime? FirstExecutionTime, DateTime? LastExecutionTime, bool IsForcedPlan);

    /// <summary>One point of the per-interval series, as the answer writes it.</summary>
    internal sealed record HistoryPoint(
        [property: JsonPropertyName("collection_time")] string CollectionTime,
        [property: JsonPropertyName("plan_id")] long PlanId,
        [property: JsonPropertyName("execution_count")] long ExecutionCount,
        [property: JsonPropertyName("avg_duration_ms")] double? AvgDurationMs,
        [property: JsonPropertyName("avg_cpu_ms")] double? AvgCpuMs);

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

    /// <summary>
    /// One row per (plan_id, collection_time), oldest first and by plan within an instant (#5300): outcomes (Regular,
    /// Aborted, Exception) and replica roles that Query Store keeps in separate rows, and intervals one collection cycle
    /// stores at the same instant, are folded the way <see cref="FoldPlans"/> folds a plan (execution-weighted, so one
    /// instant draws one value per plan and a line never runs vertically). A lone row passes through unchanged.
    /// The order does not depend on the order of <paramref name="rows"/>.
    /// </summary>
    internal static List<HistoryRow> FoldPoints(IEnumerable<HistoryRow> rows) =>
        rows.GroupBy(r => (r.PlanId, r.CollectionTime))
            .OrderBy(g => g.Key.CollectionTime).ThenBy(g => g.Key.PlanId)
            .Select(g =>
            {
                var members = g.ToList();
                if (members.Count == 1) return members[0];
                var p = FoldPlans(members)[0];
                return new HistoryRow(p.PlanId, g.Key.CollectionTime, p.ExecutionCount, p.AvgDurationMs, p.AvgCpuMs, p.FirstExecutionTime, p.LastExecutionTime, p.IsForcedPlan);
            }).ToList();

    /// <summary>
    /// The series for the plans the answer lists (#5300): rows of any other plan are dropped BEFORE the points are built,
    /// so a plan the answer cuts never uses up the point cap, and the points come out in one fixed order.
    /// </summary>
    internal static List<HistoryPoint> BuildPoints(IEnumerable<HistoryRow> rows, IEnumerable<long> keptPlanIds)
    {
        var kept = keptPlanIds.ToHashSet();
        return FoldPoints(rows.Where(r => kept.Contains(r.PlanId)))
            .Select(r => new HistoryPoint(r.CollectionTime.ToString("o", CultureInfo.InvariantCulture), r.PlanId, r.ExecutionCount, r.AvgDurationMs, r.AvgCpuMs))
            .ToList();
    }

    private static async Task<List<HistoryRow>> ReadRowsAsync(NpgsqlDataReader reader, CancellationToken cancellationToken)
    {
        var rows = new List<HistoryRow>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new HistoryRow(
                reader.GetInt64(0), reader.GetDateTime(1), reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetDouble(3), reader.IsDBNull(4) ? null : reader.GetDouble(4),
                reader.IsDBNull(5) ? null : reader.GetDateTime(5), reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                !reader.IsDBNull(7) && reader.GetBoolean(7)));
        }

        return rows;
    }

    /// <summary>
    /// The grid's tier choice (#5300), mirroring <c>DarlingDataReader.TryGetQueryStoreTopFromTableAsync</c>: the same
    /// pre-check (<see cref="DarlingDataReader.QueryStoreTopMayReadTable"/>), the same gate
    /// (<see cref="QueryStoreIntervalWide.ResolveReadAsync"/>, with <see cref="DarlingDataReader.QueryStoreTopMinWindow"/> and
    /// the window's end as the literal end), on one connection in one read-only REPEATABLE READ transaction so the gate's
    /// decision and the read it authorizes see one snapshot. Null (never an empty list) when the gate says raw or anything
    /// before the table read fails; a fault falls back to raw, as the grid's does. Cancellation propagates.
    /// </summary>
    private static async Task<(List<HistoryRow> Rows, QueryStoreIntervalWide.WideReadPlan Plan)?> TryReadFromTableAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, long queryId, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        if (!DarlingDataReader.QueryStoreTopMayReadTable(start, end)) return null;
        var gateDecided = false;
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);
            await using (var readOnly = new NpgsqlCommand(SetTransactionReadOnlySql, connection) { Transaction = transaction, CommandTimeout = McpCommandDeadlines.ReadSeconds })
            {
                await readOnly.ExecuteNonQueryAsync(cancellationToken);
            }

            var plan = await QueryStoreIntervalWide.ResolveReadAsync(
                connection, serverId, start, end, end, DarlingDataReader.QueryStoreTopMinWindow,
                McpCommandDeadlines.ReadSeconds, ReadScope.Current?.Logger, cancellationToken);
            if (!plan.UseTable)
            {
                if (plan.DecisionFailed) ReadScope.Note(ReadFallback.GateFailed);
                return null;
            }

            gateDecided = true;
            await using var command = new NpgsqlCommand(HistoryTableSql, connection) { Transaction = transaction, CommandTimeout = McpCommandDeadlines.ReadSeconds };
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(databaseName);
            command.Parameters.AddWithValue(queryId);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(plan.ReadStart, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var rows = await ReadRowsAsync(reader, cancellationToken);
            ReadScope.NoteSource(ReadScope.SourceIntervalTable);
            return (rows, plan);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ReadScope.NoteFallback(gateDecided ? ReadFallback.FallbackRaw : ReadFallback.GateFailed, "#5300 Query Store history table read", ex);
            return null;
        }
    }

    [McpServerTool(Name = "get_query_store_query_history"), Description(
        "One Query Store query's history, plan by plan: per-plan executions, duration and CPU, plus a per-interval series. window_truncated marks a history floor. <<GUIDE>>" +
        "Per-plan averages are execution-weighted (total over executions), so they can differ from get_query_store_top, which averages the interval averages. " +
        "Outcomes (Regular, Aborted, Exception) and replica roles are combined, execution-weighted, into one value per plan and collection time. " +
        "Reads the raw tier, or the per-interval table (kept 9 days) past raw retention, by get_query_store_top's own rule, so it covers what that grid does; effective_start is where complete history begins. " +
        "points keeps the newest 150 of the listed plans (points_truncated) and plans the 20 longest by total duration (plans_truncated).")]
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
            /* #5300: the grid's tier first. A window the grid reads from the interval table is read from it here, from the
               same bound, so a query the grid lists always has its history; anything else reads the raw tier as before. */
            var table = await TryReadFromTableAsync(postgres, resolved.ServerId, database_name, query_id, start, windowEnd, cancellationToken);
            List<HistoryRow> rows;
            if (table is { } served)
            {
                rows = served.Rows;
            }
            else
            {
                await using var command = postgres.CreateCommand(HistorySql);
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                command.Parameters.AddWithValue(resolved.ServerId);
                command.Parameters.AddWithValue(database_name);
                command.Parameters.AddWithValue(query_id);
                /* #1969: the columns are timestamp without time zone; a Utc-kinded bind is refused or shifted. */
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified));
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                rows = await ReadRowsAsync(reader, cancellationToken);
                ReadScope.NoteSource(ReadScope.SourceRaw);
            }

            /* #4966: say where the window starts before the empty check, so an empty answer over a cut window says the window
               was cut. When the interval table served, its own plan says how far back the answer reaches and the raw floor
               probe is not asked: raw's floor describes a tier that did not answer (the grid does the same). */
            DateTime effectiveStart;
            bool truncated;
            object? emptyHints;
            if (table is { } tableServed)
            {
                effectiveStart = tableServed.Plan.ReadStart;
                truncated = RawWindowFloor.IsTruncated(effectiveStart, start);
                emptyHints = new McpWindowNotice(
                    McpHelpers.FormatEffectiveStart(effectiveStart), truncated,
                    truncated ? DarlingMcpDataTools.QueryStoreTableNote(effectiveStart, tableServed.Plan.StartBound) : null).AsHints();
            }
            else
            {
                var floor = await DarlingDataReader.GetQueryStoreWindowFloorAsync(postgres, resolved.ServerId, start, windowEnd, cancellationToken);
                effectiveStart = RawWindowFloor.EffectiveStart(floor, start);
                truncated = RawWindowFloor.IsTruncated(floor, start);
                emptyHints = DarlingMcpWindowNotice.Build(floor, start, "query_store_stats", emptyAnswer: true).AsHints();
            }

            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_store", cancellationToken)
                    ?? McpHelpers.Status("empty",
                        $"No Query Store history for query_id {query_id} in database '{database_name}' in the {hours_back}-hour window searched.",
                        emptyHints);

            var allPlans = FoldPlans(rows);
            var plansTruncated = allPlans.Count > MaxPlans;
            var plans = plansTruncated ? allPlans.OrderByDescending(p => p.TotalDurationMs).Take(MaxPlans).OrderBy(p => p.PlanId).ToList() : allPlans;
            var allPoints = BuildPoints(rows, plans.Select(p => p.PlanId));
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
