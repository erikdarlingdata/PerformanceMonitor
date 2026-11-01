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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The two agent-change admin writes the Viewer and the web already had (#6401): <c>set_agent_actions_enabled</c> turns the service-wide
/// agent switch (<c>config_service.agent_actions_enabled</c>) OFF (turning it on is a person's act, below), and <c>undo_agent_change</c> undoes ONE non-plan agent change (a mute
/// rule, a custom alert rule, a threshold override or a deadlock signature merge). A plan force or unforce is undone with
/// <c>undo_plan_change</c>, which reads the monitored server first.
///
/// <para><b>One copy of the logic.</b> Both call <see cref="AgentChangeUndo"/> exactly as <c>PUT /api/agent-changes/switch</c> and
/// <c>PUT /api/agent-changes/{id}/undo</c> do, so the refusals are the web's: <c>not_found</c>, <c>already_undone</c>, <c>expired</c>,
/// <c>not_undoable</c>, <c>changed_since</c>, <c>name_taken</c>. The tool adds one: a plan change is pointed to <c>undo_plan_change</c>.</para>
///
/// <para><b>Off only.</b> MCP turns agent actions off and never on, the direction the route tools keep for every change that would let agents
/// act on more alerts or servers (#6281): a person turned agent actions off in the Viewer or on the web, and an MCP client must not be able to
/// give agents back the actions that person took away. <c>enabled: true</c> is refused and writes nothing; a person turns them on in the
/// Viewer (Agent changes) or on the web dashboard.</para>
///
/// <para><b>Admin writes.</b> Served only under <c>mcp.allowWrites</c> with a token, and in
/// <see cref="DarlingMcpWritePolicy.NeverAgentTools"/>, so an investigation agent and a Level 2 or 3 agent call never get either: an agent must
/// not turn its own brake off or take back a change it was refused. The journal actor is <c>mcp:</c> plus the token and client the request
/// came in on (<see cref="ReadCaller.WriteIdentity"/>), never <c>person:</c> and never <c>agent:</c>.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpAgentChangeAdminTools
{
    internal const string SwitchToolName = "set_agent_actions_enabled";
    internal const string UndoToolName = "undo_agent_change";

    /// <summary>The refusal for <c>enabled: true</c> (#6401): a person turns agent actions on, and nothing is written.</summary>
    internal const string TurnOnRefusalMessage =
        "Over MCP agent actions can only be turned off. A person turns them on in the Viewer (Agent changes) or on the web dashboard, so an MCP client cannot give agents back the actions a person took away. Nothing was written.";

    /// <summary>The journal actor of these two writes: the MCP actor <see cref="DarlingMcpServerActionTools.ActorOf"/> spells (#3684: alias, do not copy).</summary>
    internal static string ActorOf() => DarlingMcpServerActionTools.ActorOf();

    [McpServerTool(Name = "set_agent_actions_enabled"), Description(
        "Turns the service-wide agent-actions switch OFF (over MCP it never turns it on): the one brake over every change an investigation agent can make (mute rules, custom rules, threshold overrides, signature merges, plan force, plan evict, session kill). Off refuses every agent change with actions_off; nothing already done is undone. enabled true is refused and writes nothing, because an MCP client must not give agents back actions a person took away; a person turns them on in the Viewer (Agent changes) or the web dashboard. Per-server switches (set_server_action) are separate and still apply. Answers the state before and after, and journals agent_actions_switched. Refused, writing nothing, when the store has no service row yet. Admin only: never offered to an investigation agent.")]
    public static Task<string> SetAgentActionsEnabled(
        [Description("False to refuse every agent change. True is refused over MCP: a person turns agent actions on in the Viewer or the web dashboard.")] bool enabled,
        NpgsqlDataSource postgres,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        SwitchAsync(postgres, logger, enabled, ActorOf(), cancellationToken);

    /// <summary>The switch tool's body, so a test drives it with its own data source and caller.</summary>
    internal static async Task<string> SwitchAsync(NpgsqlDataSource postgres, ILogger? logger, bool enabled, string actor, CancellationToken ct)
    {
        /* MCP turns agent actions off and never on (#6401, as #6281 keeps for routes): a person turned them off, and an MCP client must not give
           agents the actions back. Refused before anything is read or written. */
        if (enabled)
        {
            return Refuse("on_is_a_persons_act", TurnOnRefusalMessage);
        }

        string refusal;
        try
        {
            var changes = new AgentChangeUndo(postgres, McpCommandDeadlines.ReadSeconds);
            var before = await changes.ReadEnabledAsync(ct);
            if (!await changes.SetEnabledAsActorAsync(enabled, actor, ct))
            {
                return Refuse("store_not_set_up", AgentChangeUndo.SwitchStoreNotSetUpMessage);
            }

            var after = await changes.ReadEnabledAsync(ct);
            logger?.LogWarning("MCP agent actions switch {Before} -> {After}, caller {Caller}", before, after, actor);
            return new JsonObject
            {
                ["status"] = after ? "on" : "off",
                ["changed"] = before != after,
                ["before"] = new JsonObject { ["enabled"] = before },
                ["after"] = new JsonObject { ["enabled"] = after },
                ["message"] = before == after
                    ? "The switch was already off; it is written again and journaled."
                    : "Agent actions are now off. Every agent change is refused with actions_off until a person turns them on in the Viewer (Agent changes) or the web dashboard; nothing already done was undone.",
            }.ToJsonString();
        }
        catch (TimeoutException ex) when (ex.Message == AgentChangeUndo.SwitchWaitTimedOutMessage)
        {
            refusal = Refuse("busy", AgentChangeUndo.SwitchWaitTimedOutMessage);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            refusal = Refuse("no_privilege", "The MCP store login may not write the agent switch. Re-run the role provisioning (" + DarlingManagedRoles.ProvisionRolesScriptPath + "). Nothing was changed.");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(SwitchToolName, ex);
        }

        return refusal;
    }

    [McpServerTool(Name = "undo_agent_change"), Description(
        "Undoes ONE agent change that is not a plan force or unforce: a mute rule, a custom alert rule, a threshold override or a deadlock signature merge, by its called_id from get_agent_changes (can_undo true). Puts the rule or override back to what it was before the agent's change, in one transaction, and journals action_undone. Refuses, writing nothing, when the change was already undone (already_undone), is not in the journal (not_found), is older than 365 days (expired), changed since the agent made it (changed_since: a person's later edit is kept), or has no undo (not_undoable). A plan force or unforce is refused with a pointer to undo_plan_change. Answers the state before and after. Admin only: never offered to an investigation agent.")]
    public static Task<string> UndoAgentChange(
        [Description("The called_id of the change, from get_agent_changes.")] long called_id,
        NpgsqlDataSource postgres,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        UndoAsync(postgres, logger, called_id, ActorOf(), cancellationToken);

    /// <summary>The undo tool's body, so a test drives it with its own data source and caller.</summary>
    internal static async Task<string> UndoAsync(NpgsqlDataSource postgres, ILogger? logger, long calledId, string actor, CancellationToken ct)
    {
        string refusal;
        try
        {
            if (calledId <= 0)
            {
                return McpHelpers.Refusal("called_id", "called_id is a positive number from get_agent_changes. Nothing was written.");
            }

            var changes = new AgentChangeUndo(postgres, McpCommandDeadlines.ReadSeconds);
            var row = await changes.GetAsync(calledId, ct);
            if (row?.IsPlanChange == true)
            {
                return Refuse("plan_change",
                    $"Change {calledId} is a Query Store plan {(row.Kind == AgentChangeKinds.PlanForce ? "force" : "unforce")}, which this tool does not undo. Use undo_plan_change with this called_id: it reads the server first and shows the statement before it writes. Nothing was changed.");
            }

            var result = await changes.UndoAsActorAsync(calledId, actor, ct);
            var node = new JsonObject
            {
                ["called_id"] = calledId,
                ["kind"] = row?.Kind,
                ["before"] = new JsonObject { ["undone"] = row?.UndoneAtUtc is not null },
            };
            if (result.Undone)
            {
                logger?.LogWarning("MCP undid agent change {CalledId} ({Kind}), caller {Caller}", calledId, row?.Kind, actor);
                node["status"] = "undone";
                node["changed"] = true;
                node["after"] = new JsonObject { ["undone"] = true, ["undone_by"] = actor };
                node["message"] = result.Words;
                node["check_with"] = new JsonObject { ["tool"] = "get_agent_changes", ["note"] = "the change now reads undone" };
            }
            else
            {
                node["status"] = "refused";
                node["code"] = result.Code;
                node["changed"] = false;
                node["after"] = new JsonObject { ["undone"] = row?.UndoneAtUtc is not null };
                node["message"] = result.Words + " Nothing was changed.";
            }

            return node.ToJsonString();
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            refusal = Refuse("no_privilege", "The MCP store login may not write what this undo restores. Re-run the role provisioning (" + DarlingManagedRoles.ProvisionRolesScriptPath + "). Nothing was changed.");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(UndoToolName, ex);
        }

        return refusal;
    }

    private static string Refuse(string code, string message) =>
        new JsonObject { ["status"] = "refused", ["code"] = code, ["changed"] = false, ["message"] = message }.ToJsonString();
}
