/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service.Mcp;

namespace Darling.Tests;

/// <summary>
/// The Darling MCP tools by access class. Every tool method carries a <see cref="DarlingToolAccessAttribute"/> naming the decision
/// (<see cref="ToolAccess"/>), and <see cref="ReadOnlyTools"/> is the set of registered tools marked <see cref="ToolAccess.Read"/>. A tool
/// with no attribute is in no set here, so <c>DarlingMcpWritePolicyTests</c> fails it as unclassified, and <c>DarlingToolAccessTests</c>
/// fails a tool whose attribute disagrees with <see cref="DarlingMcpWritePolicy.WriteTools"/> or
/// <see cref="DarlingMcpWritePolicy.InvestigationTools"/>. <c>analyze_server</c> persists the findings it derives but is a read.
/// </summary>
internal static class DarlingMcpToolClassification
{
    internal const BindingFlags AnyMethod = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance | BindingFlags.DeclaredOnly;

    /// <summary>The access a method declares, or null when it carries none.</summary>
    internal static ToolAccess? AccessOf(MethodInfo method) => method.GetCustomAttribute<DarlingToolAccessAttribute>()?.Access;

    /// <summary>Every registered tool by its served name with the access its method declares, null for a method that declares none.</summary>
    internal static readonly IReadOnlyDictionary<string, ToolAccess?> RegisteredAccess = DarlingMcpToolTypes.Tools
        .ToDictionary(t => t.Name, t => AccessOf(t.Method), StringComparer.Ordinal);

    /// <summary>The registered tools whose method is marked <see cref="ToolAccess.Read"/>.</summary>
    internal static readonly IReadOnlySet<string> ReadOnlyTools = RegisteredAccess
        .Where(a => a.Value == ToolAccess.Read)
        .Select(a => a.Key)
        .ToHashSet(StringComparer.Ordinal);
}
