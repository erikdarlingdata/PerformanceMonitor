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
using PerformanceMonitor.Darling.Service.Mcp;

namespace Darling.Tests;

/// <summary>
/// The read-only Darling MCP tools: every registered tool (<see cref="DarlingMcpToolTypes.Tools"/>) that is not in
/// <see cref="DarlingMcpWritePolicy.WriteTools"/> or <see cref="DarlingMcpWritePolicy.InvestigationTools"/>. The decision a new tool needs is
/// made where it is read: a write goes in the write policy's set, and the tool's row in <c>docs/darling/mcp-tool-tables.md</c> sits in the
/// table that says so. <c>DarlingMcpToolTablesDocTests</c> fails when the "Tools that change something" table and the write policy's sets
/// disagree, so a write that is missing from <see cref="DarlingMcpWritePolicy.WriteTools"/> fails there instead of shipping unswitched.
/// <c>analyze_server</c> persists the findings it derives but is a read.
/// </summary>
internal static class DarlingMcpToolClassification
{
    internal static readonly IReadOnlySet<string> ReadOnlyTools = DarlingMcpToolTypes.Tools
        .Select(t => t.Name)
        .Where(n => !DarlingMcpWritePolicy.WriteTools.Contains(n) && !DarlingMcpWritePolicy.InvestigationTools.Contains(n))
        .ToHashSet(StringComparer.Ordinal);
}
