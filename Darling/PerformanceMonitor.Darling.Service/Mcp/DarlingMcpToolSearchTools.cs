/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>find_tools</c> (#9388): searches the tools this request can call. Lite's <c>McpToolSearchTools</c> is the twin,
/// same name, same description, same parameters (D6 lockstep). It searches the collection the request's own
/// <c>tools/list</c> serves and <c>tools/call</c> dispatches over: <c>McpServer.ServerOptions.ToolCollection</c>, which the
/// stateless transport builds per request and <c>ConfigureSessionOptions</c> narrows (a named token's policy, the write
/// switches, the <c>/core</c> profile). It never reads the registration catalog, which would name tools this caller cannot
/// call. It reads no store data and no monitored server.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpToolSearchTools
{
    [McpServerTool(Name = "find_tools"), Description(McpToolSearchToolText.Description)]
    public static string FindTools(
        McpServer server,
        [Description(McpToolSearchToolText.QueryParameter)] string query,
        [Description(McpToolSearchToolText.DetailParameter)] string detail = "summaries",
        [Description(McpToolSearchToolText.LimitParameter)] int? limit = null)
        => McpToolSearch.Render(Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions.GetServices<McpServerTool>(server.Services!), query, detail, limit);
}
