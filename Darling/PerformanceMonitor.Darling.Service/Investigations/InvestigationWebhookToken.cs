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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// The generic webhook's <c>{{investigation_json}}</c> value (#5821): the investigation request an alert opened or joined,
/// as one JSON object a POST runner can act on. Its request fields are <c>list_investigation_requests</c>' own, with the
/// same names; <c>brief</c> is the stored brief as <c>get_investigation_reports</c> serves it, masked at
/// <c>mcp.dataDetail</c> before it leaves the service (null until a claim composes it); <c>outcome</c> says whether this
/// alert opened the request or joined it; <c>claim_tool</c> and <c>report_tool</c> name the MCP tools the runner calls
/// next, since the claim and the report still go through MCP.
/// </summary>
internal static class InvestigationWebhookToken
{
    /// <summary>The MCP tool a runner claims the request with.</summary>
    internal const string ClaimTool = "claim_investigation_request";

    /// <summary>The MCP tool a runner returns its report with.</summary>
    internal const string ReportTool = "submit_investigation_report";

    /// <summary>
    /// The token's JSON for <paramref name="requestId"/>. <paramref name="row"/> is null when the request could not be read
    /// back: the object then carries the id, the outcome and the tool names only, which is still enough to claim it.
    /// </summary>
    internal static string Render(Guid requestId, bool opened, InvestigationRequestRow? row, string? brief, McpDataDetail mode)
    {
        var node = row is not null
            ? JsonSerializer.SerializeToNode(InvestigationToolCore.RequestPayload(row), McpHelpers.JsonOptions) as JsonObject
              ?? new JsonObject()
            : new JsonObject { ["request_id"] = requestId.ToString() };

        node["outcome"] = opened ? "opened" : "joined";
        node["brief"] = brief is null
            ? null
            : InvestigationFieldMasking.StoredBrief(brief, mode, McpDataDetailMasker.ForAnswer(mode));
        node["claim_tool"] = ClaimTool;
        node["report_tool"] = ReportTool;
        return node.ToJsonString(McpHelpers.JsonOptions);
    }
}
