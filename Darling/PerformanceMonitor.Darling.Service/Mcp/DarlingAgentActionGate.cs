/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The one entry point every Level 2 action tool calls (#5632 Phase B, #5912), so no tool can skip a check. It reads the
/// <see cref="DarlingMcpWritePolicy.Effective"/> policy the service started with. Under the writes arm the gate is not
/// asked: the tool is an ordinary write tool and <c>agent_action</c> is ignored (<see cref="AgentActionGateOutcome.Ungated"/>).
/// Under the investigations arm it validates <c>agent_action</c> (check 1) and hands the change to
/// <see cref="InvestigationRequestStore.RunAgentActionAsync"/> (checks 2 to 8, the journal, the change and its re-check),
/// re-resolving the request's metrics over the routes as the store holds them now. The tool supplies only its kind rule
/// (check 7), its before image and its write.
/// </summary>
internal static class DarlingAgentActionGate
{
    /// <summary>The refusal a tool answers when it is reached with neither arm served (it is not offered then).</summary>
    internal const string NotServed = "not_served";

    /// <summary>
    /// Runs one Level 2 change under <paramref name="policy"/>. <paramref name="kindRule"/> is check 7 (null when the kind
    /// has none); <paramref name="readBefore"/> and <paramref name="write"/> are the tool's own, run in the gate's and the
    /// write's transactions.
    /// </summary>
    internal static async Task<AgentActionGateOutcome> RunAsync(
        DarlingMcpWritePolicy.Effective policy,
        NpgsqlDataSource postgres,
        InvestigationRequestStore store,
        ILogger logger,
        string tool,
        string kind,
        string? agentAction,
        AgentActionTarget target,
        Func<NpgsqlConnection, NpgsqlTransaction, AgentActionContext, CancellationToken, Task<AgentActionRefusal?>>? kindRule,
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<string?>> readBefore,
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<string?>> write,
        CancellationToken cancellationToken,
        IReadOnlySet<string>? namedAgentNames = null)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(target);

        if (policy.Writes)
            return AgentActionGateOutcome.Ungated;
        if (!policy.Investigations)
            return AgentActionGateOutcome.Refused(new AgentActionRefusal(NotServed,
                $"{tool} is not available on this service. An operator must set mcp.allowWrites (or mcp.allowInvestigations with mcp.allowWrites off) in darling.json, set an MCP token, and restart the service. Do not retry."));

        var problem = InvestigationToolCore.ParseAgentAction(agentAction, out var action);
        if (problem is not null)
        {
            /* #6542: a named agent token journals under its own name, never the agent_id the bad argument carried. */
            await store.RecordAgentActionRefusalAsync(LooseRequestId(agentAction), JournalActor(policy.AgentTokenName, namedAgentNames, LooseAgentId(agentAction)), tool, problem.Reason,
                policy.AgentTokenName, cancellationToken);
            return AgentActionGateOutcome.Refused(problem);
        }

        /* #6542: a named agent token is the agent's identity, so its agent_action acts only as that name. */
        if (policy.AgentTokenName is { } tokenName && !string.Equals(action!.AgentId, tokenName, StringComparison.Ordinal))
        {
            await store.RecordAgentActionRefusalAsync(action.RequestId, tokenName, tool, DarlingMcpInvestigationTools.AgentIdNotToken,
                tokenName, cancellationToken);
            return AgentActionGateOutcome.Refused(new AgentActionRefusal(DarlingMcpInvestigationTools.AgentIdNotToken,
                DarlingMcpInvestigationTools.AgentIdNotTokenMessage(tokenName)));
        }

        /* The legacy token's agent_id is free text. One that is a served named agent's name would put the change in that agent's journal, and
           one that is a reserved placeholder (invalid, unclaimed) would make its rows read as a refusal's, so it is refused and journaled
           under no agent's name. */
        if (policy.AgentTokenName is null && IsRefusedLegacyAgentId(namedAgentNames, action!.AgentId))
        {
            await store.RecordAgentActionRefusalAsync(action.RequestId, InvestigationRules.InvalidAgentId, tool,
                DarlingMcpInvestigationTools.AgentIdIsNamedToken, null, cancellationToken);
            return AgentActionGateOutcome.Refused(new AgentActionRefusal(DarlingMcpInvestigationTools.AgentIdIsNamedToken,
                DarlingMcpInvestigationTools.AgentIdIsNamedTokenMessage(action.AgentId)));
        }

        var hooks = new AgentActionHooks
        {
            LoadLevels = (connection, _, ct) => LoadLevelsAsync(connection, logger, ct),
            KindRule = kindRule,
            ReadBefore = readBefore,
            Write = write,
        };
        var result = await store.RunAgentActionAsync(new AgentActionCall(tool, kind, action!, target, policy.AgentTokenName), hooks, cancellationToken);
        return result.Refusal is { } refusal
            ? AgentActionGateOutcome.Refused(refusal)
            : new AgentActionGateOutcome(false, null, result.CalledId, result.AfterJson);
    }

    /// <summary>The <c>{status: refused, reason, message}</c> answer a tool returns for a refusal (#5912).</summary>
    internal static string RefusalAnswer(AgentActionRefusal refusal) =>
        JsonSerializer.Serialize(new { status = "refused", reason = refusal.Reason, message = refusal.Message }, McpHelpers.JsonOptions);

    /// <summary>
    /// Reads the routes as stored now, once, on the gate's own connection (inside its transaction), and answers the resolver
    /// of the level they send a metric on a server at (#5912, check 5): <see cref="NotificationRouter.ResolveAgent"/> over
    /// that one list, so a route an operator lowered refuses the next change, not the next reload. A failed tag read logs
    /// through <paramref name="logger"/> and leaves a tag-scoped route unexpanded, so it matches nothing: fail closed.
    /// </summary>
    internal static async Task<Func<string, int, AlertSeverityLevel?, int?>> LoadLevelsAsync(NpgsqlConnection connection, ILogger logger,
        CancellationToken cancellationToken)
    {
        var routes = await StoreConfigProvider.ReadNotificationRoutesAsync(connection, logger, cancellationToken);
        return (metric, serverId, severity) => NotificationRouter.ResolveAgent(metric, routes, serverId, severity)?.AgentLevel;
    }

    /// <summary>The request id of a refused argument, when one parses, so its refusal is journaled with its request.</summary>
    internal static Guid? LooseRequestId(string? agentAction) =>
        LooseText(agentAction, "request_id") is { } text && Guid.TryParse(text, out var id) ? id : null;

    internal static string? LooseAgentId(string? agentAction) => LooseText(agentAction, "agent_id");

    /// <summary>
    /// True when the legacy token passed an agent_id the gates and the claim tool refuse: the name of a served named agent token (its rows
    /// would land in that agent's journal) or a reserved placeholder, <c>invalid</c> or <c>unclaimed</c>, compared case-insensitively (its rows
    /// would read as a refusal's). The placeholder check holds whether or not named tokens are served; the name check needs at least one.
    /// </summary>
    internal static bool IsRefusedLegacyAgentId(IReadOnlySet<string>? namedAgentNames, string? agentId) =>
        agentId is not null
        && (DarlingMcpTokenSet.IsReservedName(agentId) || (namedAgentNames is { Count: > 0 } && namedAgentNames.Contains(agentId)));

    /// <summary>
    /// The agent id a refusal made before the gate (or on a bad argument) is journaled under: the named token's own name when there is one;
    /// <see cref="InvestigationRules.InvalidAgentId"/> when the caller is the legacy token and the id it passed is one the gates refuse
    /// (<see cref="IsRefusedLegacyAgentId"/>), so a refusal row never lands in a named agent's journal; otherwise the id it passed.
    /// </summary>
    internal static string? JournalActor(string? agentTokenName, IReadOnlySet<string>? namedAgentNames, string? passedAgentId) =>
        agentTokenName
        ?? (IsRefusedLegacyAgentId(namedAgentNames, passedAgentId)
            ? InvestigationRules.InvalidAgentId
            : passedAgentId);

    private static string? LooseText(string? json, string key)
    {
        try
        {
            return !string.IsNullOrWhiteSpace(json)
                && System.Text.Json.Nodes.JsonNode.Parse(json) is System.Text.Json.Nodes.JsonObject o
                && o[key] is System.Text.Json.Nodes.JsonValue v && v.TryGetValue(out string? s) ? s : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}

/// <summary>
/// What <see cref="DarlingAgentActionGate.RunAsync"/> did (#5912): <see cref="Ungated"/> under the writes arm (the tool
/// writes as it always has), a refusal, or the change made with its <c>action_called</c> row id and after image.
/// </summary>
internal sealed record AgentActionGateOutcome(bool IsUngated, AgentActionRefusal? Refusal, long? CalledId, string? AfterJson)
{
    /// <summary>The writes arm: the gate was not asked.</summary>
    internal static readonly AgentActionGateOutcome Ungated = new(true, null, null, null);

    internal static AgentActionGateOutcome Refused(AgentActionRefusal refusal) => new(false, refusal, null, null);
}
