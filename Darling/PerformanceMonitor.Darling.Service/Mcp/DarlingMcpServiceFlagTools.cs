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
using System.Linq;
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
/// Viewer's Settings window saves through <c>ViewerDataService.UpdateServiceFlagsAsync</c>, and (#8978) the two trigram text index
/// options. Served under <c>mcp.allowWrites</c> and never offered to an investigation agent
/// (<see cref="DarlingMcpWritePolicy.NeverAgentTools"/>).
///
/// <para><b>The trigram index options are tri-state.</b> <c>query_text_trigram_index_enabled</c> and
/// <c>query_store_text_trigram_index_enabled</c> are NULL until written, and NULL means darling.json's
/// <c>queryTextTrigramIndexEnabled</c> / <c>queryStoreTextTrigramIndexEnabled</c> governs. A value written here wins over the file, and
/// <c>reset_to_file</c> clears it back to NULL. The index pass reads the resolved value at every pass, so a change needs no restart.</para>
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
    /// <summary>#8978: the option names <c>reset_to_file</c> accepts, the two tri-state trigram index columns.</summary>
    internal static readonly IReadOnlyList<string> ResettableOptions = ["query_text_trigram_index_enabled", "query_store_text_trigram_index_enabled"];

    /// <summary>The number of values <see cref="Flags"/> reads from each half of the RETURNING list.</summary>
    private const int FlagCount = 7;

    /// <summary>The old and the new value of each of the seven flags, in one statement: the CTE reads the row the UPDATE then changes.
    /// $1 capture_plans, $2 query_store_backfill_enabled, $3 query_store_text_budget_mb, $4 max_concurrent_sweeps,
    /// $5 query_store_fetch_top_n, $6 query_text_trigram_index_enabled, $7 query_store_text_trigram_index_enabled; a NULL parameter
    /// keeps the stored value. $8 and $9 (#8978) clear the two trigram index options to NULL, so darling.json governs them again.
    /// Zero rows when the store has not seeded the row.</summary>
    internal const string UpdateSql = @"
WITH old AS (
    SELECT capture_plans, query_store_backfill_enabled, query_store_text_budget_mb, max_concurrent_sweeps, query_store_fetch_top_n,
           query_text_trigram_index_enabled, query_store_text_trigram_index_enabled
    FROM config.config_service WHERE id = 1)
UPDATE config.config_service AS c
SET capture_plans = COALESCE($1::boolean, c.capture_plans),
    query_store_backfill_enabled = COALESCE($2::boolean, c.query_store_backfill_enabled),
    query_store_text_budget_mb = COALESCE($3::integer, c.query_store_text_budget_mb),
    max_concurrent_sweeps = COALESCE($4::integer, c.max_concurrent_sweeps),
    query_store_fetch_top_n = COALESCE($5::integer, c.query_store_fetch_top_n),
    query_text_trigram_index_enabled = CASE WHEN $8::boolean THEN NULL ELSE COALESCE($6::boolean, c.query_text_trigram_index_enabled) END,
    query_store_text_trigram_index_enabled = CASE WHEN $9::boolean THEN NULL ELSE COALESCE($7::boolean, c.query_store_text_trigram_index_enabled) END
FROM old
WHERE c.id = 1
RETURNING old.capture_plans, old.query_store_backfill_enabled, old.query_store_text_budget_mb, old.max_concurrent_sweeps, old.query_store_fetch_top_n,
          old.query_text_trigram_index_enabled, old.query_store_text_trigram_index_enabled,
          c.capture_plans, c.query_store_backfill_enabled, c.query_store_text_budget_mb, c.max_concurrent_sweeps, c.query_store_fetch_top_n,
          c.query_text_trigram_index_enabled, c.query_store_text_trigram_index_enabled";

    [McpServerTool(Name = "update_service_flags"), Description(
        "PARTIAL update of the seven service-wide collection flags: the five the Viewer's Settings window saves, capture_plans (global plan capture), " +
        "query_store_backfill_enabled, query_store_text_budget_mb (4-256 MB), max_concurrent_sweeps (1-16) and query_store_fetch_top_n " +
        "(0-100000; 0 fetches every new plan), and the two trigram text index options, query_text_trigram_index_enabled (plan-cache text) and " +
        "query_store_text_trigram_index_enabled (Query Store text), which speed up find_queries_by_text. The two index options are null until set, " +
        "and null means darling.json's queryTextTrigramIndexEnabled / queryStoreTextTrigramIndexEnabled governs; a value set here wins over the file, " +
        "and reset_to_file clears it back to null. Send only the flags to change; an omitted flag keeps its stored value. An out-of-range value " +
        "returns status invalid, names the argument and its range, and writes NOTHING. Cannot change the MCP or web on/off flag or port, or " +
        "pause (turning MCP off from MCP would lock the admin out; use the Viewer). Returns {status:\"updated\", previous:{...}, settings:{...}} " +
        "with all seven values before and after, or {status:\"unavailable\"} when the store has not been seeded by the service yet. The running " +
        "service picks the change up on its next sweep, and the index pass at its next pass (get_store_metrics shows the index state); no restart. " +
        "Needs mcp.allowWrites or a named operator token; never offered to an investigation agent.")]
    public static async Task<string> UpdateServiceFlags(
        NpgsqlDataSource postgres,
        [Description("True to capture query plans on every server, false to stop. Omit to keep the stored value.")] bool? capture_plans = null,
        [Description("True to let the Query Store collector backfill history, false to stop. Omit to keep the stored value.")] bool? query_store_backfill_enabled = null,
        [Description("Query Store text budget in MB, 4 to 256. Omit to keep the stored value.")] int? query_store_text_budget_mb = null,
        [Description("Server sweeps that run at once, 1 to 16. Omit to keep the stored value.")] int? max_concurrent_sweeps = null,
        [Description("Plans per database-hour and metric the Query Store collector fetches, 0 to 100000 (0 = every new plan). Omit to keep the stored value.")] int? query_store_fetch_top_n = null,
        [Description("True to build the trigram index on the plan-cache query text in the background, false to drop it. Overrides darling.json's queryTextTrigramIndexEnabled. Omit to keep the stored value.")] bool? query_text_trigram_index_enabled = null,
        [Description("True to build the trigram index on the Query Store query text in the background, false to drop it. Overrides darling.json's queryStoreTextTrigramIndexEnabled. Omit to keep the stored value.")] bool? query_store_text_trigram_index_enabled = null,
        [Description("Trigram index options to clear back to null so darling.json governs them again: query_text_trigram_index_enabled, query_store_text_trigram_index_enabled. Do not also send a value for an option named here.")] string[]? reset_to_file = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        const string Tool = "update_service_flags";
        try
        {
            var resets = (reset_to_file ?? []).Where(name => !string.IsNullOrWhiteSpace(name)).Select(name => name.Trim()).ToList();
            if (capture_plans is null && query_store_backfill_enabled is null && query_store_text_budget_mb is null
                && max_concurrent_sweeps is null && query_store_fetch_top_n is null
                && query_text_trigram_index_enabled is null && query_store_text_trigram_index_enabled is null && resets.Count == 0)
            {
                return McpHelpers.Refusal("capture_plans", "No flag was given. Send at least one of capture_plans, query_store_backfill_enabled, query_store_text_budget_mb, "
                    + "max_concurrent_sweeps, query_store_fetch_top_n, query_text_trigram_index_enabled, query_store_text_trigram_index_enabled, reset_to_file. "
                    + "Nothing was changed.");
            }

            var resetQueryText = resets.Contains(ResettableOptions[0], StringComparer.OrdinalIgnoreCase);
            var resetQueryStoreText = resets.Contains(ResettableOptions[1], StringComparer.OrdinalIgnoreCase);
            (string Parameter, string Message)? refusal =
                resets.FirstOrDefault(name => !ResettableOptions.Contains(name, StringComparer.OrdinalIgnoreCase)) is { } unknown
                    ? ("reset_to_file", $"reset_to_file names '{unknown}', which is not a resettable option. Use {string.Join(" or ", ResettableOptions)}.")
                : resetQueryText && query_text_trigram_index_enabled is not null
                    ? ("reset_to_file", "reset_to_file names query_text_trigram_index_enabled and a value for it was sent too. Send one or the other.")
                : resetQueryStoreText && query_store_text_trigram_index_enabled is not null
                    ? ("reset_to_file", "reset_to_file names query_store_text_trigram_index_enabled and a value for it was sent too. Send one or the other.")
                : query_store_text_budget_mb is int budget && ServiceFlagLimits.TextBudgetMbError(budget) is { } budgetError
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
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = (object?)query_text_trigram_index_enabled ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = (object?)query_store_text_trigram_index_enabled ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = resetQueryText });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = resetQueryStoreText });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return McpHelpers.Status("unavailable",
                    "The service has not seeded config.config_service yet, so there are no flags to change. Start the service once, then call again. Nothing was changed.");
            }

            var previous = Flags(reader, 0);
            var settings = Flags(reader, FlagCount);
            logger?.LogInformation(
                "MCP service flags write {Tool}: capture_plans {Before1} -> {After1}, query_store_backfill_enabled {Before2} -> {After2}, "
                + "query_store_text_budget_mb {Before3} -> {After3}, max_concurrent_sweeps {Before4} -> {After4}, query_store_fetch_top_n {Before5} -> {After5}, "
                + "query_text_trigram_index_enabled {Before6} -> {After6}, query_store_text_trigram_index_enabled {Before7} -> {After7}, caller {Caller}",
                Tool, reader.GetBoolean(0), reader.GetBoolean(7), reader.GetBoolean(1), reader.GetBoolean(8), reader.GetInt32(2), reader.GetInt32(9),
                reader.GetInt32(3), reader.GetInt32(10), reader.GetInt32(4), reader.GetInt32(11),
                OptionText(reader, 5), OptionText(reader, 12), OptionText(reader, 6), OptionText(reader, 13), ReadCaller.WriteIdentity());

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

    /// <summary>One half of the RETURNING list. The two trigram index options read null while darling.json governs them.</summary>
    private static JsonObject Flags(NpgsqlDataReader reader, int offset) => new()
    {
        ["capture_plans"] = reader.GetBoolean(offset),
        ["query_store_backfill_enabled"] = reader.GetBoolean(offset + 1),
        ["query_store_text_budget_mb"] = reader.GetInt32(offset + 2),
        ["max_concurrent_sweeps"] = reader.GetInt32(offset + 3),
        ["query_store_fetch_top_n"] = reader.GetInt32(offset + 4),
        ["query_text_trigram_index_enabled"] = reader.IsDBNull(offset + 5) ? null : reader.GetBoolean(offset + 5),
        ["query_store_text_trigram_index_enabled"] = reader.IsDBNull(offset + 6) ? null : reader.GetBoolean(offset + 6),
    };

    /// <summary>A trigram index option for the log line: true, false, or "file" while the column is NULL.</summary>
    private static string OptionText(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? "file" : reader.GetBoolean(ordinal) ? "true" : "false";
}
