/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The one copy of the text a person copies or reads to reach the service's web dashboard and MCP endpoint (#6474): the
/// address to open, and the <c>claude mcp add</c> command. The Viewer's Settings "Copy Setup Command" button and the
/// service's <c>--enable-mcp</c> / <c>--enable-web</c> verbs both call it, so what the button copies and what the verb
/// prints cannot differ.
/// </summary>
public static class DarlingEndpointClientText
{
    /// <summary>The stand-in for the MCP token in <see cref="ClaudeMcpAddCommand"/>. The token itself never goes in this text.</summary>
    public const string TokenPlaceholder = "<token>";

    /// <summary>
    /// The <c>claude mcp add</c> command for the MCP endpoint on loopback at <paramref name="port"/>. The service needs the
    /// MCP token on loopback too unless <c>mcp.allowTokenlessLoopback</c> is on, so the command carries the header with
    /// <see cref="TokenPlaceholder"/> in place of the token.
    /// </summary>
    public static string ClaudeMcpAddCommand(string port, string scheme = "http") =>
        $"claude mcp add --transport http --scope user sql-monitor-darling {scheme}://localhost:{port}/ "
        + $"--header \"Authorization: Bearer {TokenPlaceholder}\"";

    /// <summary>The address to open for the web dashboard on loopback at <paramref name="port"/>, which the service always binds. <paramref name="scheme"/> is <c>https</c> only when the network listener is TLS on a wildcard or loopback listen (one listener, HTTPS for loopback too).</summary>
    public static string WebDashboardAddress(string port, string scheme = "http") => $"{scheme}://localhost:{port}/";
}
