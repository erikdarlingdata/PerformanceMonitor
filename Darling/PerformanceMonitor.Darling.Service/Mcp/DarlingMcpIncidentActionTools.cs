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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The incident actions that run inside the service (#6230, part B): <c>test_connection</c>, <c>analyze_now</c> and <c>purge_now</c>,
/// and the reads that show their result (<c>get_command_result</c>) and the plan changes an operator can still undo
/// (<c>get_agent_changes</c>). The writes are served under <c>mcp.allowWrites</c> and are never offered to an investigation agent
/// (<see cref="DarlingMcpWritePolicy.NeverAgentTools"/>).
///
/// <para><b>One path.</b> Each tool queues the same <c>config_command</c> row the Viewer and the CLI queue, through
/// <c>config.request_mcp_command</c>: the <c>mcp</c> login holds no privilege on the command queue, and that function names
/// four command types, builds a connection test from the stored server row (no credential or connection string is an argument),
/// and refuses anything else. The command executor then runs the row on its next poll, with the plan
/// <see cref="DarlingCommandExecutor.ResolvePlan"/> resolves; this class asks that plan first, so the argument checks live once.
/// Nothing here runs an action a second way.</para>
///
/// <para>An answer says what was queued, gives the command id and the read that shows the result, and says what undoes it
/// (for these three: nothing to undo, or, for a purge, that deleted rows are gone).</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpIncidentActionTools
{
    private const string RequestedBy = "mcp";

    /// <summary>The longest retention a purge may be given, in days (the same bound the store function holds).</summary>
    internal const int MaxPurgeRetentionDays = 3650;

    [McpServerTool(Name = "test_connection"), Description(
        "Tests the connection to ONE registered server now: the service connects with the server's stored settings and password and reports the engine, version and what it can see. Read only on the monitored server; it changes nothing. It is queued as a command and runs on the service's next command poll (a few seconds); the answer gives the command_id, and get_command_result shows connected or the connection error. A second test of the same server within 15 seconds is refused. Takes only a server name: it cannot test an address or a password you give it. Needs mcp.allowWrites; never offered to an investigation agent.")]
    public static Task<string> TestConnection(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string server_name,
        CancellationToken cancellationToken = default) =>
        QueueServerCommandAsync(postgres, "test_connection", "test_connect", server_name,
            "A connection test is queued. The service runs it on its next command poll.", cancellationToken);

    [McpServerTool(Name = "analyze_now"), Description(
        "Runs one analysis pass for ONE registered server now, instead of waiting for the analysis schedule. The service derives findings from data already collected and stores them; it does not query the monitored server. It is queued as a command and runs on the service's next command poll; the answer gives the command_id, and get_command_result shows the outcome. A second request for the same server within 15 seconds is refused. Needs mcp.allowWrites; never offered to an investigation agent.")]
    public static Task<string> AnalyzeNow(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string server_name,
        CancellationToken cancellationToken = default) =>
        QueueServerCommandAsync(postgres, "analyze_now", "analyze_now", server_name,
            "An analysis pass is queued. The service runs it on its next command poll.", cancellationToken);

    [McpServerTool(Name = "purge_now"), Description(
        "Starts the retention purge across the WHOLE store now, for example when the store disk fills. With no retention_days it purges to each collector's configured retention; with retention_days it purges every collector to that many days. PURGED DATA CANNOT COME BACK: there is no undo, and a short retention_days deletes every row older than it. Refused unless confirm is true. Fleet-wide, not per server. It is queued as a command and the service starts it on its next command poll; the answer gives the command_id, and get_command_result shows that it started or that an earlier purge still holds the slot. Needs mcp.allowWrites; never offered to an investigation agent.")]
    public static async Task<string> PurgeNow(
        NpgsqlDataSource postgres,
        [Description("Must be true. The purge deletes rows for good; this says the caller means it.")] bool confirm = false,
        [Description("Optional. Purge every collector to this many days (1 to 3650). Omit to use each collector's configured retention.")] int? retention_days = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (!confirm)
            {
                return Refuse("purge_now", "confirm_required",
                    "purge_now deletes collected data and the data cannot be brought back. Nothing was queued. Call it again with confirm=true if that is what you want"
                    + (retention_days is null ? "." : $", and retention_days={retention_days.Value.ToString(CultureInfo.InvariantCulture)} would delete every row older than that."));
            }

            if (retention_days is int days && (days < 1 || days > MaxPurgeRetentionDays))
            {
                return Refuse("purge_now", "invalid",
                    $"retention_days must be a whole number from 1 to {MaxPurgeRetentionDays.ToString(CultureInfo.InvariantCulture)}. Nothing was queued.");
            }

            var args = retention_days is int d ? $"{{\"retention_days\":{d.ToString(CultureInfo.InvariantCulture)}}}" : null;
            var plan = DarlingCommandExecutor.ResolvePlan(new ClaimedCommand(0, "purge_now", null, args, RequestedBy));
            if (plan.Kind == CommandKind.Fail)
            {
                return Refuse("purge_now", "invalid", (plan.FailReason ?? "The request was refused.") + " Nothing was queued.");
            }

            var queued = await QueueAsync(postgres, "purge_now", null, retention_days, cancellationToken);
            if (queued.Reason is not null)
            {
                return Refuse("purge_now", queued.Reason, queued.Message!);
            }

            return new JsonObject
            {
                ["status"] = "queued",
                ["tool"] = "purge_now",
                ["command_id"] = queued.CommandId,
                ["retention_days"] = retention_days,
                ["message"] = retention_days is int r
                    ? $"A purge to {r.ToString(CultureInfo.InvariantCulture)} days is queued. The service starts it on its next command poll and it runs in the background."
                    : "A purge to each collector's configured retention is queued. The service starts it on its next command poll and it runs in the background.",
                ["previous_value"] = "not applicable: a purge changes no setting; it deletes rows older than the retention",
                ["undo"] = "None. Rows a purge deletes cannot be brought back. get_command_result shows whether it started.",
                ["check_with"] = new JsonObject { ["tool"] = "get_command_result", ["command_id"] = queued.CommandId },
            }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("purge_now", ex);
        }
    }

    [McpServerTool(Name = "get_command_result"), Description(
        "Reads one queued command's state and result: status (pending, in_progress, succeeded or failed), result_status and the result the service wrote. Use it after test_connection, analyze_now or purge_now with the command_id they answered, and after run_collectors_now answers running. Command rows older than the store's retention are gone and answer not_found. Read only; the command's arguments are never returned.")]
    public static async Task<string> GetCommandResult(
        NpgsqlDataSource postgres,
        [Description("The command_id a write tool answered.")] long command_id,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (command_id <= 0)
            {
                return Refuse("get_command_result", "invalid", "command_id is a positive number the write tool answered.");
            }

            await using var command = postgres.CreateCommand(
                "SELECT command_type, target_server_id, status, requested_by, created_at, claimed_at, completed_at, result_status, result_json " +
                "FROM config.config_command WHERE command_id = $1");
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = command_id });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return new JsonObject
                {
                    ["status"] = "not_found",
                    ["command_id"] = command_id,
                    ["message"] = "No command has that id. A finished command is removed after the store's retention.",
                }.ToJsonString();
            }

            var node = new JsonObject
            {
                ["status"] = "ok",
                ["command_id"] = command_id,
                ["command_type"] = reader.GetString(0),
                ["server_id"] = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                ["command_status"] = reader.GetString(2),
                ["requested_by"] = reader.IsDBNull(3) ? null : reader.GetString(3),
                ["queued_utc"] = Utc(reader, 4),
                ["claimed_utc"] = Utc(reader, 5),
                ["completed_utc"] = Utc(reader, 6),
                ["result_status"] = reader.IsDBNull(7) ? null : reader.GetString(7),
            };
            if (!reader.IsDBNull(8))
            {
                try
                {
                    node["result"] = JsonNode.Parse(reader.GetFieldValue<string>(8));
                }
                catch (System.Text.Json.JsonException)
                {
                    /* The service wrote it; if it is not JSON the status alone is the answer. */
                }
            }

            if (node["command_status"]!.GetValue<string>() is "pending" or "in_progress")
            {
                node["message"] = "Not finished yet. The service polls its command queue every few seconds; read again shortly.";
            }

            return node.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_command_result", ex);
        }
    }

    [McpServerTool(Name = "get_agent_changes"), Description(
        "Lists the changes an agent made to a monitored server (a Query Store plan force or unforce, a mute, a threshold override, an evicted plan, a killed session), newest first, with the called_id undo_plan_change takes, the result (done, failed or unknown), who undid it and when, and can_undo. Use it before undo_plan_change to find the change and after it to confirm the change reads undone. Read only.")]
    public static async Task<string> GetAgentChanges(
        NpgsqlDataSource postgres,
        [Description("Optional: only changes an operator can still undo (can_undo true).")] bool undoable_only = false,
        [Description("Most changes returned (at most the newest 200 are read). Default 50.")] int limit = 50,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (McpHelpers.ValidateTop(limit) is { } badLimit)
            {
                return badLimit;
            }

            var changes = new AgentChangeUndo(postgres, McpCommandDeadlines.ReadSeconds);
            var read = await changes.ReadAsync(DarlingWebEndpoints.AgentChangesLimit, cancellationToken);
            var body = DarlingWebEndpoints.AgentChangesBody(read, canDecide: false, DateTime.UtcNow);
            var all = body["changes"]!.AsArray().Select(c => c!.AsObject()).ToList();
            var shown = all.Where(c => !undoable_only || c["can_undo"]?.GetValue<bool>() == true).Take(limit).ToList();
            var node = new JsonObject
            {
                ["status"] = shown.Count == 0 ? "empty" : "ok",
                ["agent_changes_enabled"] = read.Enabled,
                ["count"] = shown.Count,
                ["changes"] = new JsonArray(shown.Select(c => (JsonNode?)c.DeepClone()).ToArray()),
                ["note"] = "A plan force or unforce with can_undo true is undone with undo_plan_change(called_id), which shows the statement first and writes only after a confirm.",
            };
            if (shown.Count == 0)
            {
                node["message"] = undoable_only ? "No agent change can be undone now." : "No agent change is recorded.";
            }

            return node.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_agent_changes", ex);
        }
    }

    private static string? Utc(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);

    private static string Refuse(string tool, string reason, string message) =>
        new JsonObject
        {
            ["status"] = "refused",
            ["tool"] = tool,
            ["reason"] = reason,
            ["message"] = message,
            ["changed"] = false,
        }.ToJsonString();

    private static async Task<string> QueueServerCommandAsync(
        NpgsqlDataSource postgres, string tool, string commandType, string? serverName, string message, CancellationToken cancellationToken)
    {
        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, serverName, cancellationToken);
            if (error is not null)
            {
                return error;
            }

            /* The executor's own plan first, so a request it would refuse is refused here in its words. test_connect needs an
               args_json to resolve; the real one is built by the store function from the stored row. */
            var plan = DarlingCommandExecutor.ResolvePlan(new ClaimedCommand(0, commandType, resolved.ServerId, commandType == "test_connect" ? "{}" : null, RequestedBy));
            if (plan.Kind == CommandKind.Fail)
            {
                return Refuse(tool, "invalid", (plan.FailReason ?? "The request was refused.") + " Nothing was queued.");
            }

            var queued = await QueueAsync(postgres, commandType, resolved.ServerId, null, cancellationToken);
            if (queued.Reason is not null)
            {
                return Refuse(tool, queued.Reason, queued.Message!);
            }

            return new JsonObject
            {
                ["status"] = "queued",
                ["tool"] = tool,
                ["server"] = resolved.DisplayName,
                ["command_id"] = queued.CommandId,
                ["message"] = message,
                ["previous_value"] = "not applicable: this changes no setting",
                ["undo"] = "Nothing to undo: it changes no setting and no collected data.",
                ["check_with"] = new JsonObject { ["tool"] = "get_command_result", ["command_id"] = queued.CommandId },
            }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(tool, ex);
        }
    }

    /// <summary>What the store function answered: the new command id, or the refusal in plain words.</summary>
    private readonly record struct Queued(long CommandId, string? Reason, string? Message);

    /// <summary>Queues one command through <c>config.request_mcp_command</c>. Its codes map to plain refusals.</summary>
    private static async Task<Queued> QueueAsync(
        NpgsqlDataSource postgres, string commandType, int? serverId, int? retentionDays, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = postgres.CreateCommand("SELECT config.request_mcp_command($1, $2, $3, $4)");
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = commandType });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId.HasValue ? serverId.Value : DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = retentionDays.HasValue ? retentionDays.Value : DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = RequestedBy });
            var id = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
            return new Queued(id, null, null);
        }
        catch (PostgresException ex) when (ex.SqlState is "DL400" or "DL404" or "DL409" or "DL429")
        {
            var reason = ex.SqlState switch
            {
                "DL404" => "unknown_server",
                "DL409" => "already_queued",
                "DL429" => "too_soon",
                _ => "invalid",
            };
            return new Queued(0, reason, ex.MessageText + " Nothing was queued.");
        }
        catch (PostgresException ex) when (ex.SqlState is "42883" or "42501")
        {
            return new Queued(0, "store_not_ready",
                "The store has no queue function for this tool yet, or the mcp login may not call it. Restart the Darling service so it re-provisions the store roles, or re-run tools/provision-roles.sql on a self-managed store. Nothing was queued.");
        }
    }
}
