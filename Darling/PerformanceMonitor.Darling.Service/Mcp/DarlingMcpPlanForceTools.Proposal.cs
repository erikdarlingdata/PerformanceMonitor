/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The <c>proposed</c> answer of a Level 3 tool (#7109). When the gate passed and reports that the action needs a person's approval, a call with
/// <c>dry_run</c> false runs the prepare step and a dry run with the claim, stores a proposal with the binding facts the prepare step read, and
/// writes nothing to the server. The proposal is stored on the owner's connection: only the owner writes the table, as it writes the journal.
/// </summary>
public sealed partial class DarlingMcpPlanForceTools
{
    /// <summary>The status of an answer that stored a proposal instead of running the action.</summary>
    internal const string ProposedStatus = "proposed";

    /// <summary>
    /// Stores the proposal (or finds the live one for the same request, tool and target, so a retry gets the same id) and answers
    /// <c>proposed</c> with the kind, the server and the proposal id. <paramref name="shown"/> is what the tool's dry run shows (the statement,
    /// or a kill's session), under the same keys as the tool's other answers, so <c>mcp.dataDetail</c> masks it the same way. When nobody can
    /// approve the action, the message says so.
    /// </summary>
    internal static async Task<string> ProposeAsync(NpgsqlDataSource owner, Func<DateTime>? utcNow, ServerActionDecision decision, AgentAction action,
        string tool, string serverAction, int serverId, string serverDisplayName, object target, object binding, object? shown, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(decision);
        ArgumentNullException.ThrowIfNull(action);
        var approval = decision.Approval!;
        var ownerStore = new InvestigationRequestStore(owner, ServiceCommandDeadlines.SerialLoopSeconds, utcNow);
        var created = await ownerStore.CreateProposalAsync(new ActionProposalDraft(action.RequestId, approval.ClaimNo, action.AgentId, tool, serverId,
            ToObject(target), ToObject(binding), action.Reason), ct);
        var nobody = await ownerStore.NobodyCanApproveAsync(serverId, serverAction, approval.SinglePerson, ct);

        var message = $"{ServerActionNames.Label(serverAction)} on {serverDisplayName} needs a person's approval, so nothing was run on the server. "
            + (created.Created
                ? $"It is stored as proposal {created.ProposalId} for a person to approve."
                : $"Proposal {created.ProposalId} for this request and target is already waiting for a person.")
            + " Do not call this tool again for it."
            + (nobody ? " " + ActionApprovalRules.NobodyCanApproveMessage : string.Empty);

        var node = new JsonObject
        {
            ["status"] = ProposedStatus,
            ["message"] = message,
            ["dry_run"] = false,
            ["kind"] = serverAction,
            ["server"] = serverDisplayName,
            ["proposal_id"] = created.ProposalId.ToString(),
            ["nobody_can_approve"] = nobody,
        };
        if (shown is not null)
            foreach (var property in ToObject(shown))
                node[property.Key] = property.Value?.DeepClone();
        return node.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>A plan guide's definition as stored in a proposal's target or binding.</summary>
    private static object GuideImage(PerformanceMonitor.Darling.Remediation.PlanGuideDefinition guide) =>
        new { name = guide.Name, type = guide.Type, statement = guide.Statement, module_or_batch = guide.ModuleOrBatch, @params = guide.Params, hints = guide.Hints };

    private static JsonObject ToObject(object value) => JsonSerializer.SerializeToNode(value, McpHelpers.JsonOptions)!.AsObject();
}
