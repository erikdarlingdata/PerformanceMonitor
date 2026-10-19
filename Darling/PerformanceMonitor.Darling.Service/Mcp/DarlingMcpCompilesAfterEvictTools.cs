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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>get_compiles_after_evict</c> (#5632, step 10): after an agent evicted one plan handle (<c>evict_cached_plan</c>), did the next compile of each
/// statement use the same plan shape? Store only: it reads the journal and <c>collect.query_stats</c> and never connects to a monitored server, so it
/// needs no gate. It names no remediation type.
/// <para><b>A compile is a later <c>creation_time</c>,</b> not a new <c>plan_handle</c>: a batch can compile again under the same handle. The
/// <c>creation_time</c> of the before image and of a collected row are both on the monitored server's own clock, so they are compared with each other;
/// the store's <c>collection_time</c> is compared only with the eviction's journal time.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpCompilesAfterEvictTools
{
    internal const string ToolName = "get_compiles_after_evict";
    internal const string NotAnEviction = "not_an_eviction";
    internal const string NotCollectedYet = "not_collected_yet";
    internal const string OldPlanStillSeen = "old_plan_still_seen";
    internal const string Recompiled = "recompiled";

    /// <summary>The eviction's own journal rows: the <c>action_called</c> row of kind <c>plan_evict</c> that has an <c>action_done</c>, and that row's time. $1 is the journal id.</summary>
    internal const string EvictionSql = @"
SELECT c.server_id, c.detail::text, d.event_time
FROM config.investigation_journal AS c
JOIN LATERAL (
    SELECT x.event_time FROM config.investigation_journal AS x
    WHERE x.request_id = c.request_id AND x.event = 'action_done' AND x.detail->>'called_id' = c.journal_id::text
    ORDER BY x.journal_id DESC LIMIT 1) AS d ON true
WHERE c.journal_id = $1 AND c.event = 'action_called' AND c.detail->>'kind' = '" + Storage.AgentChangeKinds.PlanEvict + "'";

    [McpServerTool(Name = "get_compiles_after_evict"), Description(
        "Reads what happened after an evict_cached_plan: for each statement Darling collected for that handle, whether it compiled again and whether the new plan has the same shape. Reads the store only; changes nothing. <<GUIDE>> " +
        "Call it with the journal_id evict_cached_plan returned, after the next query_stats collection (about a minute), and again later: a statement that has not run yet is not_collected_yet. " +
        "Per statement: not_collected_yet (no row after the eviction; the collector keeps only the costliest statements, so a cheap one may never appear), old_plan_still_seen (rows after the eviction, none with a later creation_time), or recompiled with same_plan_shape (query_plan_hash equal), the new plan_handle and creation_time, executions since, and per-execution CPU, duration and reads beside the before image's. " +
        "A compile is a later creation_time, not a new plan_handle: the same batch can compile again under the same handle. Reads up to 7 days after the eviction. An id that is not a done eviction is refused (not_an_eviction).")]
    public static Task<string> GetCompilesAfterEvict(
        NpgsqlDataSource postgres,
        [Description("The journal_id evict_cached_plan returned.")] long journal_id,
        CancellationToken cancellationToken = default) =>
        RunAsync(postgres, journal_id, cancellationToken);

    internal static async Task<string> RunAsync(NpgsqlDataSource postgres, long journalId, CancellationToken ct)
    {
        try
        {
            int serverId;
            string detailJson;
            DateTime evictedAt;
            await using (var connection = await postgres.OpenConnectionAsync(ct))
            await using (var command = DarlingPlanEvictStore.Command(connection, EvictionSql))
            {
                command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = journalId, NpgsqlDbType = NpgsqlDbType.Bigint });
                await using var reader = await command.ExecuteReaderAsync(ct);
                if (!await reader.ReadAsync(ct) || reader.IsDBNull(0))
                {
                    return JsonSerializer.Serialize(new
                    {
                        status = "refused",
                        reason = NotAnEviction,
                        message = "That id is not an evict_cached_plan that was done. Use the journal_id its done answer returned.",
                    }, McpHelpers.JsonOptions);
                }

                serverId = reader.GetInt32(0);
                detailJson = reader.GetString(1);
                evictedAt = reader.GetDateTime(2);
            }

            var before = (JsonNode.Parse(detailJson) as JsonObject)?["before"] as JsonObject;
            var handle = (string?)before?["plan_handle"];
            var images = before?["statements"] is JsonArray array
                ? array.Select(n => n is null ? null : JsonSerializer.Deserialize<PlanEvictStatementImage>(n.ToJsonString())).OfType<PlanEvictStatementImage>().ToList()
                : new List<PlanEvictStatementImage>();
            if (images.Count == 0)
            {
                return JsonSerializer.Serialize(new
                {
                    status = "empty",
                    journal_id = journalId,
                    plan_handle = handle,
                    evicted_at = DarlingPlanEvictStore.Time(evictedAt),
                    message = "Darling had not collected this handle when it was evicted, so there is no statement to follow. Nothing in the store can say what compiled next.",
                }, McpHelpers.JsonOptions);
            }

            var rows = await DarlingPlanEvictStore.ReadAfterAsync(postgres, serverId,
                images.Select(i => i.QueryHash).OfType<string>().Distinct(StringComparer.Ordinal).ToList(),
                images.Select(i => i.SqlHandle).OfType<string>().Distinct(StringComparer.Ordinal).ToList(), evictedAt, ct);

            var statements = images.Select(image => Judge(image, rows)).ToList();
            return JsonSerializer.Serialize(new
            {
                status = "ok",
                journal_id = journalId,
                server_id = serverId,
                plan_handle = handle,
                evicted_at = DarlingPlanEvictStore.Time(evictedAt),
                window_days = DarlingPlanEvictStore.AfterWindow.TotalDays,
                statement_count = statements.Count,
                statements,
                note = "A compile is a later creation_time than the before image's, not a new plan_handle. Both creation times are on the monitored server's own clock. "
                       + "The collector keeps only the costliest statements, so a cheap statement may never appear. A recompiled entry's totals all come from after the eviction.",
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(ToolName, ex);
        }
    }

    private static DateTime? ParseTime(string? text) =>
        DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.None, out var value) ? value : null;

    /// <summary>The state of one statement of the before image, given every collected row after the eviction. Pure, so a test needs no store.</summary>
    internal static object Judge(PlanEvictStatementImage before, IReadOnlyList<DarlingPlanEvictStore.Row> rows)
    {
        var mine = rows.Where(r => string.Equals(r.Image.QueryHash, before.QueryHash, StringComparison.Ordinal)
                && string.Equals(r.Image.SqlHandle, before.SqlHandle, StringComparison.Ordinal)
                && r.Image.StatementStartOffset == before.StatementStartOffset
                && r.Image.StatementEndOffset == before.StatementEndOffset)
            .ToList();
        var beforeCreated = ParseTime(before.CreationTime);
        var beforeNode = new
        {
            plan_handle = before.PlanHandle,
            query_plan_hash = before.QueryPlanHash,
            creation_time = before.CreationTime,
            plan_generation_num = before.PlanGenerationNum,
            execution_count = before.ExecutionCount,
            avg_cpu_ms = before.AvgCpuMs,
            avg_elapsed_ms = before.AvgElapsedMs,
            avg_logical_reads = before.AvgLogicalReads,
        };
        var identity = new { query_hash = before.QueryHash, sql_handle = before.SqlHandle, statement_start_offset = before.StatementStartOffset, statement_end_offset = before.StatementEndOffset };

        if (mine.Count == 0)
        {
            return new
            {
                identity,
                state = NotCollectedYet,
                message = "No row for this statement has been collected since the eviction. The collector takes only the costliest statements, so a cheap statement may never appear. Call again after the next query_stats collection.",
                before = beforeNode,
            };
        }

        var compiled = beforeCreated is DateTime was
            ? mine.Where(r => r.CreationTime is DateTime created && created > was).OrderBy(r => r.CreationTime).ThenBy(r => r.Image.PlanHandle, StringComparer.Ordinal).ToList()
            : new List<DarlingPlanEvictStore.Row>();
        if (compiled.Count == 0)
        {
            var seen = mine.OrderByDescending(r => r.Image.CollectionTime, StringComparer.Ordinal).First().Image;
            return new
            {
                identity,
                state = OldPlanStillSeen,
                message = "Rows were collected after the eviction, but none has a later creation_time, so the statement has not compiled again yet.",
                last_seen = new { collection_time = seen.CollectionTime, last_execution_time = seen.LastExecutionTime, execution_count = seen.ExecutionCount },
                before = beforeNode,
            };
        }

        var next = compiled[0].Image;
        var same = string.Equals(next.QueryPlanHash, before.QueryPlanHash, StringComparison.OrdinalIgnoreCase);
        return new
        {
            identity,
            state = Recompiled,
            same_plan_shape = same,
            plan_handle_changed = !string.Equals(next.PlanHandle, before.PlanHandle, StringComparison.OrdinalIgnoreCase),
            message = same
                ? "The statement compiled again and the new plan has the same shape (query_plan_hash is equal)."
                : "The statement compiled again and the new plan has a different shape (query_plan_hash differs).",
            compiles_since = compiled.Count,
            after = new
            {
                plan_handle = next.PlanHandle,
                query_plan_hash = next.QueryPlanHash,
                creation_time = next.CreationTime,
                plan_generation_num = next.PlanGenerationNum,
                executions_since = next.ExecutionCount,
                last_execution_time = next.LastExecutionTime,
                collection_time = next.CollectionTime,
                avg_cpu_ms = next.AvgCpuMs,
                avg_elapsed_ms = next.AvgElapsedMs,
                avg_logical_reads = next.AvgLogicalReads,
            },
            before = beforeNode,
        };
    }
}
