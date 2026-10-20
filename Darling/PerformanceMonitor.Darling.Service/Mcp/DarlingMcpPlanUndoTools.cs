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
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>undo_plan_change</c> (#6230, part B): undoes an agent's Query Store plan force or unforce, by the same code the web route
/// <c>PUT /api/agent-changes/{id}/undo</c> and the Viewer's <c>undo_plan_change</c> command run
/// (<see cref="DarlingMcpPlanForceTools.UndoPlanChangeAsync"/>). It owns no undo logic: the server's own state decides what is put back.
///
/// <para><b>The gate is the force tools' gate.</b> The undo reads and writes the monitored server with that server's remediation login, so it
/// needs that server's <c>force_plan</c> switch (the per-server opt-in of #5632) and an <c>env:</c> or <c>file:</c> remediation login, the two
/// server-side conditions the undo checks, and it is served only under <c>mcp.allowWrites</c> with a token, as the force tools are. Unlike the
/// four Level 2 and Level 3 tools it is not in <see cref="DarlingMcpWritePolicy.NeverAgentTools"/>: it is the way back from a change, not a new one.
/// A refusal of the gate is journaled by the undo with the caller as <c>person:mcp</c>.</para>
///
/// <para><b>Show, then confirm.</b> The first call (confirm false) writes nothing: it answers the exact statement and the state it read. A
/// write needs <c>confirm=true</c> with the <c>statement</c> that was shown, so no caller writes a statement nobody was shown.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPlanUndoTools
{
    internal const string ToolName = "undo_plan_change";

    /// <summary>The login the undo records as the person who undid the change: <c>person:mcp</c>.</summary>
    internal const string Login = "mcp";

    [McpServerTool(Name = "undo_plan_change"), Description(
        "Undoes ONE agent change to a Query Store plan (a plan force or unforce) by its called_id from get_agent_changes (can_undo true): puts the query back to the state before the change. The server's own state decides what is put back: undoing a force puts the plan that was forced before back, or unforces the plan when none was forced; undoing an unforce forces that plan again. Anything else is refused (changed_since when another change was made since, nothing_to_undo when the server shows it never took effect). Two steps: call with confirm=false (the default) to get the exact statement and the state it read, which writes nothing; then call again with confirm=true and that statement to write. Needs that server's force_plan switch and remediation login (the same gate as force_query_store_plan and unforce_query_store_plan; ships off) and mcp.allowWrites. The undo is journaled in Agent changes as person:mcp, and the answer says how to put the change back (force or unforce again). Check with get_agent_changes and get_forced_plans.")]
    public static Task<string> UndoPlanChange(
        NpgsqlDataSource postgres,
        [Description("The called_id of the change, from get_agent_changes.")] long called_id,
        [Description("Default false: show the statement and the state, write nothing. True, with statement, writes.")] bool confirm = false,
        [Description("With confirm=true: the statement the previous call showed, unchanged.")] string? statement = null,
        StoreOwnerSource? ownerSource = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        RunAsync(postgres, ownerSource?.Read(), logger, called_id, confirm, statement, cancellationToken);

    /// <summary>The tool body, so a test drives it with its own data sources. <paramref name="owner"/> is the store owner's data source.</summary>
    internal static async Task<string> RunAsync(NpgsqlDataSource postgres, NpgsqlDataSource? owner, ILogger? logger, long calledId, bool confirm, string? shown, CancellationToken ct)
    {
        try
        {
            if (calledId <= 0)
            {
                return Refuse("invalid", "called_id is a positive number from get_agent_changes. Nothing was written.");
            }

            var changes = new AgentChangeUndo(postgres, McpCommandDeadlines.ReadSeconds);
            var answer = await DarlingMcpPlanForceTools.UndoPlanChangeAsync(changes, owner, logger ?? NullLogger.Instance, calledId, Login, confirm, shown, ct);
            var node = answer.ToJson();
            if (answer.Status == PlanUndoAnswer.Failed)
            {
                /* The failure carries the monitored server's own text; the web route answers the words below and the service log has the rest. */
                node["message"] = DarlingWebEndpoints.UndoFailedWords;
            }

            if (answer.Status == PlanUndoAnswer.Undone)
            {
                node["undo"] = "To put the change back: force or unforce the plan again with force_query_store_plan or unforce_query_store_plan (the pre-check get_plan_force_precheck first). "
                    + (answer.RestoredAs is null ? string.Empty : $"It was restored as {answer.RestoredAs}.");
                node["check_with"] = new JsonObject { ["tool"] = "get_agent_changes", ["note"] = "the change now reads undone" };
            }
            else if (answer.Status == PlanUndoAnswer.Confirm)
            {
                node["next"] = "Call undo_plan_change again with confirm=true and this statement to write it.";
            }

            return node.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(ToolName, ex);
        }
    }

    private static string Refuse(string code, string message) =>
        new JsonObject { ["status"] = "refused", ["code"] = code, ["message"] = message }.ToJsonString();
}
