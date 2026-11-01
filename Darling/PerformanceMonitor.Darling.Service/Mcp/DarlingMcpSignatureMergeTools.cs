/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>merge_deadlock_signatures</c> (#6006, D14): merges one deadlock signature into another on one server, so the daily
/// rollup, the agent gate and the digest count them as one. Offered under both arms. Under the writes arm it is an ordinary
/// write tool. Under the investigations arm it is a Level 2 action: <c>agent_action</c> is required and the change goes through
/// <see cref="DarlingAgentActionGate"/> for the request's own server only, journaled with the agent's reason and listed in
/// Agent changes with one-click undo (the undo stamps the merge's <c>undone_at</c> and keeps the row).
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpSignatureMergeTools
{
    internal const string ToolName = "merge_deadlock_signatures";

    internal const string PreconditionMessage =
        "The service's mcp store role may not add rows to config.investigation_signature_merges. A bring-your-own store needs " +
        DarlingManagedRoles.ProvisionRolesScriptPath + " re-run (it is idempotent) as a superuser. Managed and compose stores grant it themselves on start.";

    [McpServerTool(Name = "merge_deadlock_signatures"), Description(
        "Agent arm: needs agent_action, request's server only. Merges one deadlock signature into another, so the rollup, the agent gate and the digest count them as one. Changes the monitoring store only. " +
        "<<GUIDE>>" +
        "signature and merged_into are signatures from the deadlock_digest event's signatures list (dl1-...); signature is counted as " +
        "merged_into from then on, with its past days. Merges are one level deep: refused as merge_chain when merged_into is itself merged away " +
        "or signature has signatures merged into it, as already_merged when signature is already merged, and as unknown_signature when " +
        "either was not counted in the last 30 days. The reason is the agent_action reason (writes arm: the reason argument, required). " +
        "A merge is journaled and undone from Agent changes, which keeps the row and splits the counts again. Answers status merged " +
        "with merge_id, previous (the signature was counted as itself), undo null and an undo_note naming where the undo is (no tool call undoes a merge), refused with a reason, or precondition (the store role lacks the grant).")]
    public static Task<string> MergeDeadlockSignatures(
        NpgsqlDataSource postgres,
        [Description("Server name, or the start of it.")] string server_name,
        [Description("The deadlock signature to merge away (dl1-..., from the deadlock_digest event).")] string signature,
        [Description("The deadlock signature it is counted as from now on.")] string merged_into,
        [Description("Why the two are the same pattern (writes arm; 1 to 2000 characters). On the agent arm the agent_action reason is used.")] string? reason = null,
        [Description("Level 2 only: under mcp.allowInvestigations this change needs agent_action (request_id, claim_id, agent_id, reason, evidence); see the tool guide. Ignored when mcp.allowWrites is on.")] string? agent_action = null,
        DarlingAgentActionSeat? agentSeat = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        MergeAsync(postgres, agentSeat, logger, server_name, signature, merged_into, reason, agent_action, cancellationToken);

    private static string Refused(string reason, string message) =>
        JsonSerializer.Serialize(new { status = "refused", reason, message }, McpHelpers.JsonOptions);

    /// <summary>The tool body, so a test drives either arm. A null <paramref name="seat"/> is the writes arm.</summary>
    internal static async Task<string> MergeAsync(NpgsqlDataSource postgres, DarlingAgentActionSeat? seat, ILogger? logger,
        string server_name, string signature, string merged_into, string? reason, string? agent_action, CancellationToken ct)
    {
        try
        {
            signature = (signature ?? "").Trim();
            merged_into = (merged_into ?? "").Trim();
            var gated = DarlingAgentActionSeat.IsGated(seat);
            if (!gated && (string.IsNullOrWhiteSpace(reason) || reason.Trim().Length > InvestigationToolCore.MaxActionReasonChars))
                return Refused("invalid", $"reason is required: 1 to {InvestigationToolCore.MaxActionReasonChars} characters saying why the two are one pattern.");

            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, ct, NameClash.Refuse);
            if (error is not null)
                return error;

            if (!gated)
            {
                await using var connection = await postgres.OpenConnectionAsync(ct);
                await using var transaction = await connection.BeginTransactionAsync(ct);
                await DeadlockSignatureStore.LockMergesAsync(connection, transaction, resolved.ServerId, ct);
                if (await DeadlockSignatureStore.CheckMergeAsync(connection, transaction, resolved.ServerId, signature, merged_into, ct) is { } problem)
                    return Refused(problem.Code, problem.Message);
                var id = await DeadlockSignatureStore.InsertMergeAsync(connection, transaction, resolved.ServerId, signature, merged_into, reason!.Trim(), "mcp", ct);
                await transaction.CommitAsync(ct);
                return Answer(resolved.DisplayName, resolved.ServerId, signature, merged_into, id);
            }

            _ = InvestigationToolCore.ParseAgentAction(agent_action, out var action);
            var store = new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds, agentActionSource: seat!.Owner?.Read());
            var target = new AgentActionTarget(resolved.ServerId, resolved.ServerName, signature, null);
            long mergeId = 0;
            try
            {
                var outcome = await DarlingAgentActionGate.RunAsync(seat!.Policy, postgres, store,
                    logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ToolName, SignatureMergeImage.Kind, agent_action, target,
                    async (connection, transaction, _, token) =>
                        await DeadlockSignatureStore.CheckMergeAsync(connection, transaction, resolved.ServerId, signature, merged_into, token) is { } problem
                            ? new AgentActionRefusal(problem.Code, problem.Message)
                            : null,
                    (_, _, _) => Task.FromResult<string?>(null),
                    async (connection, transaction, token) =>
                    {
                        await DeadlockSignatureStore.LockMergesAsync(connection, transaction, resolved.ServerId, token);
                        if (await DeadlockSignatureStore.CheckMergeAsync(connection, transaction, resolved.ServerId, signature, merged_into, token) is { } problem)
                            throw new AgentToolOutcomeException(Refused(problem.Code, problem.Message));
                        var by = "agent:" + action!.AgentId;
                        mergeId = await DeadlockSignatureStore.InsertMergeAsync(connection, transaction, resolved.ServerId, signature, merged_into,
                            action.Reason, by.Length > 64 ? by[..64] : by, token);
                        return JsonSerializer.Serialize(new SignatureMergeImage(mergeId, signature, merged_into));
                    }, ct, seat.NamedAgentNames);
                if (outcome.Refusal is { } refusal)
                    return DarlingAgentActionGate.RefusalAnswer(refusal);
                return Answer(resolved.DisplayName, resolved.ServerId, signature, merged_into, mergeId);
            }
            catch (AgentToolOutcomeException ex)
            {
                return ex.Outcome;
            }
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return McpHelpers.Status("precondition", PreconditionMessage);
        }
        catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.UniqueViolation or PostgresErrorCodes.CheckViolation)
        {
            return Refused("invalid", "The merge was refused by the store: " + ex.MessageText);
        }
    }

    private static string Answer(string server, int serverId, string signature, string mergedInto, long mergeId) =>
        JsonSerializer.Serialize(new
        {
            status = "merged",
            server,
            server_id = serverId,
            signature,
            merged_into = mergedInto,
            merge_id = mergeId,
            previous = new { signature, counted_as = signature, merged_into = (string?)null },
            undo = (string?)null,
            undo_note = "No MCP tool call undoes a merge: undo it from Agent changes (the merge is journaled there), which keeps the row and splits the counts again.",
            message = "The signature is counted as merged_into from now on. Undo it from Agent changes to split the counts again.",
        }, McpHelpers.JsonOptions);
}
