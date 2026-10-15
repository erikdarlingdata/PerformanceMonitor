/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>
/// The product's own tool names a brief may carry in <c>next_calls</c> (#5820): the first calls per investigation family
/// for each engine, and the only tools a brief may name at all. Each product passes its own, since its MCP server
/// registers its own tools; <see cref="InvestigationBriefComposer"/> reads nothing else product-specific.
/// </summary>
/// <param name="SqlServerFamilyCalls">The first calls per family on a SQL Server target, keyed by family.</param>
/// <param name="PostgresFamilyCalls">The first calls per family on a PostgreSQL target, keyed by family; empty where the product has no PostgreSQL target.</param>
/// <param name="NameableTools">The only tools a brief may name in <c>next_calls</c>; any other name is dropped.</param>
public sealed record InvestigationBriefTools(
    IReadOnlyDictionary<string, string[]> SqlServerFamilyCalls,
    IReadOnlyDictionary<string, string[]> PostgresFamilyCalls,
    IReadOnlyCollection<string> NameableTools)
{
    /// <summary>The family-call table for a target of that engine.</summary>
    public IReadOnlyDictionary<string, string[]> FamilyCalls(bool isPostgres) => isPostgres ? PostgresFamilyCalls : SqlServerFamilyCalls;
}
