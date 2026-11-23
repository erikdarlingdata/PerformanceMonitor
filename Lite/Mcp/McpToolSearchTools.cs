/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// <c>find_tools</c> (#9388): searches the tools this request can call. Darling's <c>DarlingMcpToolSearchTools</c> is the
/// twin, same name, same description, same parameters (D6 lockstep). It searches the collection the request's own
/// <c>tools/list</c> serves and <c>tools/call</c> dispatches over: <c>McpServer.ServerOptions.ToolCollection</c>, which holds
/// the tools after the write policy and the investigation policy narrowed it. It never reads the registration catalog, which
/// would name tools this caller cannot call. It reads no collected data and no monitored server.
/// </summary>
[McpServerToolType]
public sealed class McpToolSearchTools
{
    [McpServerTool(Name = "find_tools"), Description(McpToolSearchToolText.Description)]
    public static string FindTools(
        McpServer server,
        [Description(McpToolSearchToolText.QueryParameter)] string query,
        [Description(McpToolSearchToolText.DetailParameter)] string detail = "summaries",
        [Description(McpToolSearchToolText.LimitParameter)] int? limit = null)
    {
        /* A limit out of range is refused with the shared envelope, like every other bounded parameter (the payload contract census). */
        if (limit is { } asked
            && McpHelpers.ValidateTop(asked, string.Equals(detail?.Trim(), "full", StringComparison.OrdinalIgnoreCase) ? McpToolSearch.MaxFullLimit : McpToolSearch.MaxLimit) is { } refusal)
        {
            return refusal;
        }

        return McpToolSearch.Render(server.ServerOptions.ToolCollection, query, detail, limit);
    }
}
