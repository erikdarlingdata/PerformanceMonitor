/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>What a call to an MCP tool does to the monitoring store, the decision every tool declares with <see cref="DarlingToolAccessAttribute"/>.</summary>
internal enum ToolAccess
{
    /// <summary>The tool reads collected data or runs a computation over it. It is served under every write policy and carries <c>readOnlyHint</c>.</summary>
    Read,

    /// <summary>The tool changes monitoring configuration. It is in <see cref="DarlingMcpWritePolicy.WriteTools"/> and served only when <c>mcp.allowWrites</c> is on.</summary>
    Write,

    /// <summary>The tool changes investigation state only. It is in <see cref="DarlingMcpWritePolicy.InvestigationTools"/> and served only under <c>mcp.allowInvestigations</c>.</summary>
    Investigation,
}

/// <summary>
/// The access class of the MCP tool this method implements, set on its own line above its <c>[McpServerTool]</c> attribute. Every tool method
/// carries exactly one, a read as much as a write, so adding a tool forces the read-or-write decision where the tool is written and needs no
/// line in a shared list. <c>DarlingToolAccessTests</c> fails a tool method with none and a tool whose access disagrees with the write policy's sets.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
internal sealed class DarlingToolAccessAttribute : Attribute
{
    internal DarlingToolAccessAttribute(ToolAccess access) => Access = access;

    /// <summary>What a call to the tool does to the monitoring store.</summary>
    internal ToolAccess Access { get; }
}
