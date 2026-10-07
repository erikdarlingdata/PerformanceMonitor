/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP endpoint's live TLS-certificate facts (#5288): published by <see cref="DarlingMcpHostService"/> when
/// it loads the certificate for its LAN listener (<c>mcp.network.tls</c>) and observed by the worker's alert
/// sweep, which raises the "MCP TLS Certificate Expiring" self-alert from it. The twin of
/// <see cref="WebTlsCertificateState"/>; every member lives on <see cref="ListenerTlsCertificateState"/>.
///
/// <para>A separate singleton rather than one shared state, so each listener publishes and clears only its
/// own certificate and a web restart can never clear or resolve an MCP alert (or the other way round). A
/// certificate that both listeners serve is published to both states, and the two alerts are independent.</para>
/// </summary>
public sealed class McpTlsCertificateState : ListenerTlsCertificateState
{
}
