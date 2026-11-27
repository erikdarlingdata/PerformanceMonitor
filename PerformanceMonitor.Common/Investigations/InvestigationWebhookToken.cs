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

namespace PerformanceMonitor.Common;

/// <summary>
/// The generic webhook's <c>{{investigation_json}}</c> value (#5821): the investigation request an alert opened or joined,
/// as one JSON object a POST runner can act on. Its request fields are <c>list_investigation_requests</c>' own, with the
/// same names; <c>brief</c> is the stored brief as <c>get_investigation_reports</c> serves it, masked at
/// <c>mcp.dataDetail</c> before it leaves the service (null until a claim composes it); <c>outcome</c> says whether this
/// alert opened the request or joined it; <c>held</c> (#9485) is true when the request opened inside a planned change window, with the
/// window's reason in <c>held_reason</c>, and false otherwise; <c>claim_tool</c> and <c>report_tool</c> name the MCP tools the runner calls
/// next, since the claim and the report still go through MCP.
/// </summary>
public static class InvestigationWebhookToken
{
    /// <summary>The MCP tool a runner claims the request with.</summary>
    public const string ClaimTool = "claim_investigation_request";

    /// <summary>The MCP tool a runner returns its report with.</summary>
    public const string ReportTool = "submit_investigation_report";

    /// <summary>
    /// The token's JSON for <paramref name="requestId"/>. <paramref name="row"/> is null when the request could not be read
    /// back: the object then carries the id, the outcome and the tool names only, which is still enough to claim it.
    /// </summary>
    public static string Render(Guid requestId, bool opened, InvestigationRequestRow? row, string? brief, McpDataDetail mode,
        PseudonymKey? key = null)
    {
        var node = row is not null
            ? JsonSerializer.SerializeToNode(InvestigationToolCore.RequestPayload(row), McpHelpers.JsonOptions) as JsonObject
              ?? new JsonObject()
            : new JsonObject { ["request_id"] = requestId.ToString() };

        node["outcome"] = opened ? "opened" : "joined";
        /* #9485: a request that opened inside a planned change window is held, and the page says so with the window's reason. */
        var heldBy = row?.HeldBy is { IsHeld: true } hold ? hold : null;
        node["held"] = heldBy is not null;
        if (heldBy is not null)
            node["held_reason"] = heldBy.Reason;
        node["brief"] = brief is null
            ? null
            : InvestigationFieldMasking.StoredBrief(brief, mode, McpDataDetailMasker.ForAnswer(mode, key));
        node["claim_tool"] = ClaimTool;
        node["report_tool"] = ReportTool;
        return node.ToJsonString(McpHelpers.JsonOptions);
    }
}
