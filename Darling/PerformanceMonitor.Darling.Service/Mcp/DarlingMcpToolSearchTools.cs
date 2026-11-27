/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.FrontDoor;

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
    [FrontDoorClass(FrontDoorToolClass.Local)]
    [DarlingToolAccess(ToolAccess.Read)]
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

        /* call_tool is in this request's collection on /lean only (every other path serves a copy without it), so the sentence about it
           goes only where a client can use it without connecting somewhere else. */
        var served = server.ServerOptions.ToolCollection;
        var servesCallTool = served?.Any(t => string.Equals(t.ProtocolTool.Name, DarlingLeanToolProfile.CallToolName, StringComparison.Ordinal)) == true;
        return McpToolSearch.Render(served, query, detail, limit, servesCallTool ? LeanReconnectSentence : null);
    }

    /// <summary>Darling's addition to the shared reconnect note (#9584), sent on <c>/lean</c>, the one path that serves <c>call_tool</c>:
    /// it runs a read tool the client's own list lacks by its name, so a stale list needs no reconnect there for a read. <c>call_tool</c>
    /// refuses a tool that writes (<see cref="DarlingLeanToolProfile.CallToolFilter"/>), so a write still needs the reconnect.</summary>
    internal const string LeanReconnectSentence =
        "On the /lean path, call_tool runs a read tool by its name without a reconnect. A tool that writes is not run there and still needs the client to reconnect.";
}
