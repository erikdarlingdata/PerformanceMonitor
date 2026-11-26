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
/// Viewer's Settings window saves through <c>ViewerDataService.UpdateServiceFlagsAsync</c>, (#8978) the two trigram text index
/// options, and (#9638) the three darling.json seeds that the Viewer's Settings window also saves: the plan content retention days, the
/// compose statement timeout and the plan XML compression mode. Served under <c>mcp.allowWrites</c> and never offered to an investigation agent
/// (<see cref="DarlingMcpWritePolicy.NeverAgentTools"/>).
///
/// <para><b>The trigram index options are tri-state.</b> <c>query_text_trigram_index_enabled</c> and
/// <c>query_store_text_trigram_index_enabled</c> are NULL until written, and NULL means darling.json's
/// <c>queryTextTrigramIndexEnabled</c> / <c>queryStoreTextTrigramIndexEnabled</c> governs. A value written here wins over the file, and
/// <c>reset_to_file</c> clears it back to NULL. The index pass reads the resolved value at every pass, so a change needs no restart.</para>
///
/// <para><b>The three #9638 values.</b> The retention days are read from the held config at each daily or manual purge, so the
/// next purge honours a change. The compression mode is a live provider on the plan writer, so the next plan written uses it:
/// plans already stored keep the format they were written in, and every plan reader takes text first and gzip second per row, so
/// a change never strands an old plan. The statement timeout lives on the database roles, so it needs the service to re-apply it
/// (it does, on the next sweep, where it provisioned the roles) and a role's new value reaches that role's next session. The
/// answer says so when the timeout changed.</para>
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
    private const int FlagCount = 10;

    /// <summary>The old and the new value of each of the ten flags, in one statement: the CTE reads the row the UPDATE then changes.
    /// $1 capture_plans, $2 query_store_backfill_enabled, $3 query_store_text_budget_mb, $4 max_concurrent_sweeps,
    /// $5 query_store_fetch_top_n, $6 query_text_trigram_index_enabled, $7 query_store_text_trigram_index_enabled; a NULL parameter
    /// keeps the stored value. $8 and $9 (#8978) clear the two trigram index options to NULL, so darling.json governs them again.
    /// $10 plan_content_retention_days, $11 compose_statement_timeout_seconds, $12 plan_xml_compression (#9638), a NULL parameter keeps the stored value.
    /// Zero rows when the store has not seeded the row.</summary>
    internal const string UpdateSql = @"
WITH old AS (
    SELECT capture_plans, query_store_backfill_enabled, query_store_text_budget_mb, max_concurrent_sweeps, query_store_fetch_top_n,
           query_text_trigram_index_enabled, query_store_text_trigram_index_enabled,
           plan_content_retention_days, compose_statement_timeout_seconds, plan_xml_compression
    FROM config.config_service WHERE id = 1)
UPDATE config.config_service AS c
SET capture_plans = COALESCE($1::boolean, c.capture_plans),
    query_store_backfill_enabled = COALESCE($2::boolean, c.query_store_backfill_enabled),
    query_store_text_budget_mb = COALESCE($3::integer, c.query_store_text_budget_mb),
    max_concurrent_sweeps = COALESCE($4::integer, c.max_concurrent_sweeps),
    query_store_fetch_top_n = COALESCE($5::integer, c.query_store_fetch_top_n),
    query_text_trigram_index_enabled = CASE WHEN $8::boolean THEN NULL ELSE COALESCE($6::boolean, c.query_text_trigram_index_enabled) END,
    query_store_text_trigram_index_enabled = CASE WHEN $9::boolean THEN NULL ELSE COALESCE($7::boolean, c.query_store_text_trigram_index_enabled) END,
    plan_content_retention_days = COALESCE($10::integer, c.plan_content_retention_days),
    compose_statement_timeout_seconds = COALESCE($11::integer, c.compose_statement_timeout_seconds),
    plan_xml_compression = COALESCE($12::text, c.plan_xml_compression)
FROM old
WHERE c.id = 1
RETURNING old.capture_plans, old.query_store_backfill_enabled, old.query_store_text_budget_mb, old.max_concurrent_sweeps, old.query_store_fetch_top_n,
          old.query_text_trigram_index_enabled, old.query_store_text_trigram_index_enabled,
          old.plan_content_retention_days, old.compose_statement_timeout_seconds, old.plan_xml_compression,
          c.capture_plans, c.query_store_backfill_enabled, c.query_store_text_budget_mb, c.max_concurrent_sweeps, c.query_store_fetch_top_n,
          c.query_text_trigram_index_enabled, c.query_store_text_trigram_index_enabled,
          c.plan_content_retention_days, c.compose_statement_timeout_seconds, c.plan_xml_compression";

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "update_service_flags"), Description(
        "PARTIAL update of the ten service-wide settings. The Viewer's Settings window saves eight of them: the five collection flags, capture_plans " +
        "(global plan capture), query_store_backfill_enabled, query_store_text_budget_mb (4-256 MB), max_concurrent_sweeps (1-16) and " +
        "query_store_fetch_top_n (0-100000; 0 fetches every new plan), and three that darling.json only seeds: " +
        "plan_content_retention_days (0 turns it off, else 7-365; a stored plan is dropped that many days after its last sighting, at the next purge), " +
        "compose_statement_timeout_seconds (5-600; the statement_timeout of the viewer and mcp roles, applied to each role's next session, and the " +
        "answer says when the service does not set it for you) and plan_xml_compression (gzip or none; applies to plans written from now on, " +
        "plans already stored keep their format and still read). The other two are the trigram text index options, " +
        "query_text_trigram_index_enabled (plan-cache text) and query_store_text_trigram_index_enabled (Query Store text), which speed up " +
        "find_queries_by_text and are set only here. The two index options are null until set, " +
        "and null means darling.json's queryTextTrigramIndexEnabled / queryStoreTextTrigramIndexEnabled governs; a value set here wins over the file, " +
        "and reset_to_file clears it back to null. Send only the flags to change; an omitted flag keeps its stored value. An out-of-range value " +
        "returns status invalid, names the argument and its range, and writes NOTHING. Cannot change the MCP or web on/off flag or port, or " +
        "pause (turning MCP off from MCP would lock the admin out; use the Viewer). Returns {status:\"updated\", previous:{...}, settings:{...}} " +
        "with all ten values before and after, or {status:\"unavailable\"} when the store has not been seeded by the service yet. The running " +
        "service picks the change up on its next sweep, and the index pass at its next pass (get_store_metrics shows the index state); no restart. " +
        "A note in the answer says what a changed retention, timeout or compression mode does next. " +
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
        [Description("Days a stored plan XML outlives its last sighting before the next purge may drop it: 0 to turn this horizon off, else 7 to 365. Omit to keep the stored value.")] int? plan_content_retention_days = null,
        [Description("Seconds of statement_timeout on the viewer and mcp roles, 5 to 600. A role's new value reaches its next session. Omit to keep the stored value.")] int? compose_statement_timeout_seconds = null,
        [Description("How new plans are stored: gzip (bytes, smaller, read through the apps and MCP) or none (plain text, readable by direct SQL). Plans already stored keep their format and still read. Omit to keep the stored value.")] string? plan_xml_compression = null,
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
                && query_text_trigram_index_enabled is null && query_store_text_trigram_index_enabled is null && resets.Count == 0
                && plan_content_retention_days is null && compose_statement_timeout_seconds is null && plan_xml_compression is null)
            {
                return McpHelpers.Refusal("capture_plans", "No flag was given. Send at least one of capture_plans, query_store_backfill_enabled, query_store_text_budget_mb, "
                    + "max_concurrent_sweeps, query_store_fetch_top_n, query_text_trigram_index_enabled, query_store_text_trigram_index_enabled, reset_to_file, "
                    + "plan_content_retention_days, compose_statement_timeout_seconds, plan_xml_compression. Nothing was changed.");
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
                : plan_content_retention_days is int retention && ServiceFlagLimits.PlanContentRetentionDaysError(retention) is { } retentionError
                    ? ("plan_content_retention_days", $"plan_content_retention_days {retention}: {retentionError}")
                : compose_statement_timeout_seconds is int timeout && ServiceFlagLimits.ComposeStatementTimeoutSecondsError(timeout) is { } timeoutError
                    ? ("compose_statement_timeout_seconds", $"compose_statement_timeout_seconds {timeout}: {timeoutError}")
                : plan_xml_compression is not null && ServiceFlagLimits.PlanXmlCompressionError(plan_xml_compression) is { } compressionError
                    ? ("plan_xml_compression", $"plan_xml_compression '{plan_xml_compression}': {compressionError}")
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
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = (object?)plan_content_retention_days ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = (object?)compose_statement_timeout_seconds ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)ServiceFlagLimits.NormalizePlanXmlCompression(plan_xml_compression) ?? DBNull.Value });

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
                + "query_text_trigram_index_enabled {Before6} -> {After6}, query_store_text_trigram_index_enabled {Before7} -> {After7}, "
                + "plan_content_retention_days {Before8} -> {After8}, compose_statement_timeout_seconds {Before9} -> {After9}, "
                + "plan_xml_compression {Before10} -> {After10}, caller {Caller}",
                Tool, reader.GetBoolean(0), reader.GetBoolean(FlagCount), reader.GetBoolean(1), reader.GetBoolean(FlagCount + 1),
                reader.GetInt32(2), reader.GetInt32(FlagCount + 2), reader.GetInt32(3), reader.GetInt32(FlagCount + 3),
                reader.GetInt32(4), reader.GetInt32(FlagCount + 4),
                OptionText(reader, 5), OptionText(reader, FlagCount + 5), OptionText(reader, 6), OptionText(reader, FlagCount + 6),
                reader.GetInt32(7), reader.GetInt32(FlagCount + 7), reader.GetInt32(8), reader.GetInt32(FlagCount + 8),
                reader.GetString(9), reader.GetString(FlagCount + 9), ReadCaller.WriteIdentity());

            return new JsonObject
            {
                ["status"] = "updated",
                ["previous"] = previous,
                ["settings"] = settings,
                ["message"] = Message(previous, settings),
            }.ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(Tool, ex);
        }
    }

    /// <summary>The answer's message: the reload sentence, plus one sentence for each #9638 value that this call changed, saying when it
    /// takes effect. A value sent equal to the stored value reads as unchanged and adds nothing.</summary>
    internal static string Message(JsonObject previous, JsonObject settings)
    {
        var message = "The running service reloads these on its next sweep; no restart is needed.";
        if ((int)previous["plan_content_retention_days"]! != (int)settings["plan_content_retention_days"]!)
        {
            message += " plan_content_retention_days applies at the next purge, the daily one or purge_now.";
        }

        if ((int)previous["compose_statement_timeout_seconds"]! != (int)settings["compose_statement_timeout_seconds"]!)
        {
            message += " compose_statement_timeout_seconds is set on the viewer and mcp database roles, not read per query: the service sets it on its next sweep or its next start"
                + " where it provisioned those roles (a managed store or the compose store), and a role uses it from its next session."
                + " On a store whose roles you provisioned yourself the service does not touch them, so run ALTER ROLE ... SET statement_timeout there by hand.";
        }

        if ((string)previous["plan_xml_compression"]! != (string)settings["plan_xml_compression"]!)
        {
            message += " plan_xml_compression applies to plans written from now on. Plans already stored keep their format and still read.";
        }

        return message;
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
        ["plan_content_retention_days"] = reader.GetInt32(offset + 7),
        ["compose_statement_timeout_seconds"] = reader.GetInt32(offset + 8),
        ["plan_xml_compression"] = reader.GetString(offset + 9),
    };

    /// <summary>A trigram index option for the log line: true, false, or "file" while the column is NULL.</summary>
    private static string OptionText(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? "file" : reader.GetBoolean(ordinal) ? "true" : "false";
}
