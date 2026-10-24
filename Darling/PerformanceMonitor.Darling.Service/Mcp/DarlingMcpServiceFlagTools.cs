/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The service-flags write (#6393): <c>update_service_flags</c> sets plan capture, Query Store backfill, the Query Store text budget,
/// the concurrent server sweeps and the Query Store fetch top N on the single <c>config.config_service</c> row, the five flags the
/// Viewer's Settings window saves through <c>ViewerDataService.UpdateServiceFlagsAsync</c>. Served under <c>mcp.allowWrites</c> and
/// never offered to an investigation agent (<see cref="DarlingMcpWritePolicy.NeverAgentTools"/>).
///
/// <para><b>What is deliberately not settable.</b> The MCP on/off flag and port, the web on/off flag and port, and <c>paused</c>. The
/// tool has no argument for them and the <c>mcp</c> role's column grant covers none of them: turning MCP off from MCP would lock the
/// admin out of the only channel that could turn it back on, a port change would drop the connection the call arrived on, and
/// pausing is already a queued command (<c>pause_collection</c>).</para>
///
/// <para><b>One validation.</b> The three numeric ranges are <see cref="ServiceFlagLimits"/>, the same class the Settings window
/// asks, so the two callers refuse the same values. A refusal names the argument and the range and writes nothing. The write is one
/// <c>UPDATE</c> with <c>COALESCE(argument, stored value)</c> per column, so an omitted argument keeps what is stored, and the
/// <c>config_service</c> self-bump trigger moves <c>config_version</c> so the running service reloads on its next sweep.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpServiceFlagTools
{
    /// <summary>The old and the new value of each of the five flags, in one statement: the CTE reads the row the UPDATE then changes.
    /// $1 capture_plans, $2 query_store_backfill_enabled, $3 query_store_text_budget_mb, $4 max_concurrent_sweeps,
    /// $5 query_store_fetch_top_n; a NULL parameter keeps the stored value. Zero rows when the store has not seeded the row.</summary>
    internal const string UpdateSql = @"
WITH old AS (
    SELECT capture_plans, query_store_backfill_enabled, query_store_text_budget_mb, max_concurrent_sweeps, query_store_fetch_top_n
    FROM config.config_service WHERE id = 1)
UPDATE config.config_service AS c
SET capture_plans = COALESCE($1::boolean, c.capture_plans),
    query_store_backfill_enabled = COALESCE($2::boolean, c.query_store_backfill_enabled),
    query_store_text_budget_mb = COALESCE($3::integer, c.query_store_text_budget_mb),
    max_concurrent_sweeps = COALESCE($4::integer, c.max_concurrent_sweeps),
    query_store_fetch_top_n = COALESCE($5::integer, c.query_store_fetch_top_n)
FROM old
WHERE c.id = 1
RETURNING old.capture_plans, old.query_store_backfill_enabled, old.query_store_text_budget_mb, old.max_concurrent_sweeps, old.query_store_fetch_top_n,
          c.capture_plans, c.query_store_backfill_enabled, c.query_store_text_budget_mb, c.max_concurrent_sweeps, c.query_store_fetch_top_n";

    [McpServerTool(Name = "update_service_flags"), Description(
        "PARTIAL update of the five service-wide collection flags, the same ones the Viewer's Settings window saves: capture_plans (global plan capture), " +
        "query_store_backfill_enabled, query_store_text_budget_mb (4-256 MB), max_concurrent_sweeps (1-16) and query_store_fetch_top_n " +
        "(0-100000; 0 fetches every new plan). Send only the flags to change; an omitted flag keeps its stored value. An out-of-range value " +
        "returns status invalid, names the argument and its range, and writes NOTHING. Cannot change the MCP or web on/off flag or port, or " +
        "pause (turning MCP off from MCP would lock the admin out; use the Viewer). Returns {status:\"updated\", previous:{...}, settings:{...}} " +
        "with all five values before and after, or {status:\"unavailable\"} when the store has not been seeded by the service yet. The running " +
        "service picks the change up on its next sweep; no restart. Needs mcp.allowWrites; never offered to an investigation agent.")]
    public static async Task<string> UpdateServiceFlags(
        NpgsqlDataSource postgres,
        [Description("True to capture query plans on every server, false to stop. Omit to keep the stored value.")] bool? capture_plans = null,
        [Description("True to let the Query Store collector backfill history, false to stop. Omit to keep the stored value.")] bool? query_store_backfill_enabled = null,
        [Description("Query Store text budget in MB, 4 to 256. Omit to keep the stored value.")] int? query_store_text_budget_mb = null,
        [Description("Server sweeps that run at once, 1 to 16. Omit to keep the stored value.")] int? max_concurrent_sweeps = null,
        [Description("Plans per database-hour and metric the Query Store collector fetches, 0 to 100000 (0 = every new plan). Omit to keep the stored value.")] int? query_store_fetch_top_n = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        const string Tool = "update_service_flags";
        try
        {
            if (capture_plans is null && query_store_backfill_enabled is null && query_store_text_budget_mb is null
                && max_concurrent_sweeps is null && query_store_fetch_top_n is null)
            {
                return McpHelpers.Refusal("capture_plans", "No flag was given. Send at least one of capture_plans, query_store_backfill_enabled, query_store_text_budget_mb, "
                    + "max_concurrent_sweeps, query_store_fetch_top_n. Nothing was changed.");
            }

            (string Parameter, string Message)? refusal =
                query_store_text_budget_mb is int budget && ServiceFlagLimits.TextBudgetMbError(budget) is { } budgetError
                    ? ("query_store_text_budget_mb", $"query_store_text_budget_mb {budget}: {budgetError}")
                : max_concurrent_sweeps is int sweeps && ServiceFlagLimits.ConcurrentSweepsError(sweeps) is { } sweepsError
                    ? ("max_concurrent_sweeps", $"max_concurrent_sweeps {sweeps}: {sweepsError}")
                : query_store_fetch_top_n is int topN && ServiceFlagLimits.FetchTopNError(topN) is { } topNError
                    ? ("query_store_fetch_top_n", $"query_store_fetch_top_n {topN}: {topNError}")
                : null;
            if (refusal is { } refused)
            {
                return McpHelpers.Refusal(refused.Parameter, refused.Message + " Nothing was changed.");
            }

            await using var command = postgres.CreateCommand(UpdateSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = (object?)capture_plans ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = (object?)query_store_backfill_enabled ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = (object?)query_store_text_budget_mb ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = (object?)max_concurrent_sweeps ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = (object?)query_store_fetch_top_n ?? DBNull.Value });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return McpHelpers.Status("unavailable",
                    "The service has not seeded config.config_service yet, so there are no flags to change. Start the service once, then call again. Nothing was changed.");
            }

            var previous = Flags(reader, 0);
            var settings = Flags(reader, 5);
            logger?.LogInformation(
                "MCP service flags write {Tool}: capture_plans {Before1} -> {After1}, query_store_backfill_enabled {Before2} -> {After2}, "
                + "query_store_text_budget_mb {Before3} -> {After3}, max_concurrent_sweeps {Before4} -> {After4}, query_store_fetch_top_n {Before5} -> {After5}, caller {Caller}",
                Tool, reader.GetBoolean(0), reader.GetBoolean(5), reader.GetBoolean(1), reader.GetBoolean(6), reader.GetInt32(2), reader.GetInt32(7),
                reader.GetInt32(3), reader.GetInt32(8), reader.GetInt32(4), reader.GetInt32(9), ReadCaller.WriteIdentity());

            return new JsonObject
            {
                ["status"] = "updated",
                ["previous"] = previous,
                ["settings"] = settings,
                ["message"] = "The running service reloads these on its next sweep; no restart is needed.",
            }.ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(Tool, ex);
        }
    }

    private static JsonObject Flags(NpgsqlDataReader reader, int offset) => new()
    {
        ["capture_plans"] = reader.GetBoolean(offset),
        ["query_store_backfill_enabled"] = reader.GetBoolean(offset + 1),
        ["query_store_text_budget_mb"] = reader.GetInt32(offset + 2),
        ["max_concurrent_sweeps"] = reader.GetInt32(offset + 3),
        ["query_store_fetch_top_n"] = reader.GetInt32(offset + 4),
    };
}
