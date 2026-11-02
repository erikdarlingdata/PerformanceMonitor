/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The one entry point every SERVER action will call (#5632, step 5), beside <see cref="DarlingAgentActionGate"/>, which answers for
/// Level 2 changes to the monitoring store. An action may run only when all four conditions hold: the service's
/// <c>agent_actions_enabled</c>, a Level 3 request, that server's switch for that action, and that server's remediation login given
/// as an <c>env:</c> or <c>file:</c> reference. A call that lacks one is refused with its own reason, and the refusal is journaled.
/// <para><b>It takes no <c>DarlingMcpWritePolicy</c>.</b> <c>mcp.allowWrites</c> never bypasses it, and there is no flag that skips a
/// check. Force, unforce, evict and kill call it. Every switch in <c>config.config_server_actions</c> reads off until it is turned on: by a
/// person in the Viewer, or over MCP by <c>set_server_action</c> for an operator-class token when <c>mcp.allowWrites</c> and
/// <c>mcp.allowServerAdmin</c> are both on (it is in NeverAgentTools, so no agent token reaches it). The remediation login is the
/// person-only condition: it must be an <c>env:</c> or <c>file:</c> reference, which only the configuration file sets (the store's
/// trigger refuses a reference from any role but the owner, and <c>edit_server</c> refuses every <c>remediation_*</c> key).</para>
/// </summary>
internal static class ServerActionGate
{
    /// <summary>
    /// Checks one action on one server and answers with the <see cref="ServerActionPermit"/> when every condition holds (#5632, step 7):
    /// the one value a server write takes as its first argument. <paramref name="ownerSource"/> is the store owner's data source: the
    /// remediation secret column is not readable by the <c>mcp</c> role, and the gate only classifies the slot by its prefix, never
    /// resolving it. A refusal is already journaled.
    /// </summary>
    internal static async Task<ServerActionDecision> AuthorizeAsync(
        InvestigationRequestStore store,
        NpgsqlDataSource ownerSource,
        ILogger logger,
        string tool,
        string serverAction,
        string? agentAction,
        AgentActionTarget target,
        CancellationToken cancellationToken,
        string? agentTokenName = null,
        IReadOnlySet<string>? namedAgentNames = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(ownerSource);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(target);

        var problem = InvestigationToolCore.ParseAgentAction(agentAction, out var action);
        if (problem is not null)
        {
            /* A refused argument still journals under the named token, or the agent_id it carried when it names one. */
            await store.RecordAgentActionRefusalAsync(DarlingAgentActionGate.LooseRequestId(agentAction),
                DarlingAgentActionGate.JournalActor(agentTokenName, namedAgentNames, DarlingAgentActionGate.LooseAgentId(agentAction)), tool, problem.Reason, agentTokenName, cancellationToken);
            return new ServerActionDecision(null, problem);
        }

        /* #6542: a named agent token is the agent's identity, so a server action acts only as that name. */
        if (agentTokenName is not null && !string.Equals(action!.AgentId, agentTokenName, StringComparison.Ordinal))
        {
            await store.RecordAgentActionRefusalAsync(action.RequestId, agentTokenName, tool, DarlingMcpInvestigationTools.AgentIdNotToken,
                agentTokenName, cancellationToken);
            return new ServerActionDecision(null, new AgentActionRefusal(DarlingMcpInvestigationTools.AgentIdNotToken,
                DarlingMcpInvestigationTools.AgentIdNotTokenMessage(agentTokenName)));
        }

        /* The legacy token's agent_id is free text. One that is a served named agent's name would put the action in that agent's journal, and
           one that is a reserved placeholder (invalid, unclaimed) would make its rows read as a refusal's, so it is refused and journaled
           under no agent's name. */
        if (agentTokenName is null && DarlingAgentActionGate.IsRefusedLegacyAgentId(namedAgentNames, action!.AgentId))
        {
            await store.RecordAgentActionRefusalAsync(action.RequestId, InvestigationRules.InvalidAgentId, tool,
                DarlingMcpInvestigationTools.AgentIdIsNamedToken, null, cancellationToken);
            return new ServerActionDecision(null, new AgentActionRefusal(DarlingMcpInvestigationTools.AgentIdIsNamedToken,
                DarlingMcpInvestigationTools.AgentIdIsNamedTokenMessage(action.AgentId)));
        }

        var hooks = new ServerActionHooks
        {
            LoadRoutes = (connection, _, ct) => DarlingAgentActionGate.LoadRoutesAsync(connection, logger, ct),
            ReadCredential = (serverId, ct) => store.ReadServerCredentialStateAsync(ownerSource, serverId, ct),
        };
        return await store.AuthorizeServerActionAsync(new ServerActionCall(tool, serverAction, action!, target, agentTokenName), hooks, cancellationToken);
    }
}
