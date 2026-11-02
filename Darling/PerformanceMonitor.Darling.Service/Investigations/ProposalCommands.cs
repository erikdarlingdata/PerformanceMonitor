/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// The command plane's part of approving an action proposal from the Viewer (#7109). The Viewer is an administrator path: it names no
/// person, so <see cref="ProposalApprover.Administrator"/> approves only on a route that allows single-person approval, and the press API
/// refuses it on a two-person route. Three commands, all run in the service as the store owner:
/// <c>read_action_proposals</c> (the list, with whether this path may approve each one and whether nobody can), <c>proposal_first_press</c>
/// and <c>proposal_confirm</c> (the two presses, each calling <see cref="ActionProposalPress"/>).
/// </summary>
internal static class ProposalCommands
{
    internal const string ReadType = "read_action_proposals";
    internal const string FirstPressType = "proposal_first_press";
    internal const string ConfirmType = "proposal_confirm";

    /// <summary>The surface the press is journaled with.</summary>
    internal const string Surface = ServerActionSurfaces.Viewer;

    internal static bool IsProposalCommand(string commandType) => commandType is ReadType or FirstPressType or ConfirmType;

    /// <summary>The press commands carry <c>{proposal_id}</c>.</summary>
    internal static bool TryParsePressArgs(string? json, out Guid proposalId)
    {
        proposalId = Guid.Empty;
        return TryRead(json, "proposal_id", out proposalId, required: true);
    }

    /// <summary>The read carries an optional <c>{request_id}</c>.</summary>
    internal static bool TryParseReadArgs(string? json, out Guid? requestId)
    {
        requestId = null;
        if (string.IsNullOrWhiteSpace(json))
            return true;
        if (!TryRead(json, "request_id", out var id, required: false))
            return false;
        requestId = id == Guid.Empty ? null : id;
        return true;
    }

    private static bool TryRead(string? json, string name, out Guid id, bool required)
    {
        id = Guid.Empty;
        if (string.IsNullOrWhiteSpace(json))
            return !required;
        try
        {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return false;
            if (!root.TryGetProperty(name, out var value) || value.ValueKind == JsonValueKind.Null)
                return !required;
            return value.ValueKind == JsonValueKind.String && Guid.TryParse(value.GetString(), out id) && id != Guid.Empty;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>A press: the administrator path, the first press or the confirm, and the press API's answer as the result.</summary>
    internal static async Task<CommandOutcome> PressAsync(NpgsqlDataSource owner, ILogger? logger, string commandType, Guid proposalId,
        McpDataDetail dataDetail, CancellationToken cancellationToken)
    {
        var press = new ActionProposalPress(owner, logger ?? NullLogger.Instance);
        var approver = ProposalApprover.Administrator(Surface);
        var result = commandType == FirstPressType
            ? await press.FirstPressAsync(proposalId, approver, cancellationToken)
            : await press.ConfirmAsync(proposalId, approver, cancellationToken);
        logger?.LogInformation("Proposal command {Type} on {ProposalId} => {Status} {Code}", commandType, proposalId, result.Status, result.Code);
        var answer = new JsonObject
        {
            ["status"] = result.Status,
            ["code"] = result.Code,
            ["message"] = result.Message,
            ["facts"] = MaskedJson(result.Facts, McpDataDetailMasker.ForAnswer(dataDetail)),
            ["changed"] = result.Changed is null ? null : new JsonArray(result.Changed.Select(c => (JsonNode?)JsonValue.Create(c)).ToArray()),
        };
        return new CommandOutcome(result.Status is "first_pressed" or ProposalStates.Run, result.Code ?? result.Status, answer.ToJsonString());
    }

    /// <summary>
    /// The proposals, newest first (of one request, or of every request), each with what this path may do: <c>can_approve_here</c> is true
    /// for a pending proposal on a route that allows single-person approval, and <c>nobody_can_approve</c> is the store's check. A
    /// two-person route's entry carries the web link when a base URL is set. A <c>statement</c> member follows <paramref name="dataDetail"/>.
    /// </summary>
    internal static async Task<CommandOutcome> ReadAsync(NpgsqlDataSource owner, ILogger? logger, Guid? requestId, int? serverId,
        McpDataDetail dataDetail, string? webBaseUrl, CancellationToken cancellationToken)
    {
        var effectiveLogger = logger ?? NullLogger.Instance;
        var store = new InvestigationRequestStore(owner, ServiceCommandDeadlines.SerialLoopSeconds);
        var rows = await store.ListProposalsAsync(requestId, null, 200, serverId, cancellationToken);
        var masker = McpDataDetailMasker.ForAnswer(dataDetail);
        var names = await ServerNamesAsync(owner, rows.Select(r => r.ServerId).Distinct().ToArray(), cancellationToken);
        var people = await ProposalDisplay.PeopleAsync(owner, rows.Select(r => r.FirstPressBy), cancellationToken);
        var approvals = new Dictionary<Guid, ActionApprovalState?>();
        var list = new JsonArray();
        foreach (var row in rows)
        {
            if (!approvals.TryGetValue(row.RequestId, out var approval))
            {
                approval = await store.RequestApprovalAsync(row.RequestId,
                    (connection, _, token) => DarlingAgentActionGate.LoadRoutesAsync(connection, effectiveLogger, token), cancellationToken);
                approvals[row.RequestId] = approval;
            }

            var singlePerson = approval?.SinglePerson ?? false;
            var pending = row.State == ProposalStates.Pending;
            var nobody = pending && approval is not null
                && await store.NobodyCanApproveAsync(row.ServerId, InvestigationRequestStore.ServerActionForTool(row.Tool), singlePerson, cancellationToken);
            var target = MaskedJson(JsonNode.Parse(row.TargetJson), masker);
            list.Add(new JsonObject
            {
                ["proposal_id"] = row.ProposalId.ToString("D"),
                ["request_id"] = row.RequestId.ToString("D"),
                ["tool"] = row.Tool,
                ["kind"] = ProposalNotice.KindOf(row.Tool),
                ["server_id"] = row.ServerId,
                ["server_name"] = names.TryGetValue(row.ServerId, out var name) ? name : row.ServerId.ToString(CultureInfo.InvariantCulture),
                ["target"] = target,
                ["target_text"] = ProposalDisplay.TargetLine(row.Tool, target),
                ["state"] = row.State,
                ["agent"] = row.Agent,
                ["reason"] = masker.FreeText(row.Reason),
                ["proposed_at"] = DateTime.SpecifyKind(row.ProposedAt, DateTimeKind.Utc).ToString("O", CultureInfo.InvariantCulture),
                ["expires_at"] = DateTime.SpecifyKind(row.ExpiresAt, DateTimeKind.Utc).ToString("O", CultureInfo.InvariantCulture),
                ["first_press_by"] = row.FirstPressBy,
                ["first_press_name"] = ProposalDisplay.PressName(row.FirstPressBy, people),
                ["single_person"] = singlePerson,
                ["can_approve_here"] = pending && singlePerson,
                ["nobody_can_approve"] = nobody,
                ["link"] = singlePerson ? null : ProposalNotice.LinkFor(webBaseUrl, row.ProposalId),
            });
        }

        return new CommandOutcome(true, "ok", new JsonObject { ["proposals"] = list }.ToJsonString());
    }

    private static async Task<Dictionary<int, string>> ServerNamesAsync(NpgsqlDataSource owner, int[] ids, CancellationToken cancellationToken)
    {
        var names = new Dictionary<int, string>();
        if (ids.Length == 0)
            return names;
        await using var command = owner.CreateCommand("SELECT server_id, COALESCE(display_name, server_name) FROM collect.servers WHERE server_id = ANY($1)");
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = ids });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            names[reader.GetInt32(0)] = reader.GetString(1);
        return names;
    }

    /// <summary>
    /// Applies <c>mcp.dataDetail</c> to a proposal's target or facts through the same masker the web page uses: every statement-class value
    /// (<see cref="McpDataDetailKeys"/>: <c>statement</c>, <c>module_or_batch</c>, <c>hints</c>, <c>definition</c> and the rest) is shown at
    /// <c>full</c>, has its literals masked at <c>maskLiterals</c>, and is left out at <c>metadataOnly</c>.
    /// </summary>
    internal static JsonNode? MaskedJson(JsonNode? node, McpDataDetailMasker masker) =>
        node is null ? null : JsonNode.Parse(masker.Json(node.ToJsonString()));
}
