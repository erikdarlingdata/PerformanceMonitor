/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>Parameter types a collector query can bind — grown as the sweep demands.</summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1720:Identifier contains type name",
    Justification = "SQL parameter-binding vocabulary by design. Int32 names the binding type the sweep requests, not the .NET type.")]
public enum CollectorParameterType
{
    DateTime2,
    NVarChar128,

    /// <summary>
    /// Matches <c>sys.traces.path</c> exactly (#1962). Binding a trace file path as NVarChar128 would
    /// TRUNCATE a deep install path, and a truncated path never equals the live one — the collector
    /// would take its expensive fallback every cycle on precisely the servers with long paths, silently.
    /// </summary>
    NVarChar260,
    Int32,
    BigInt,
}

/// <summary>One bound parameter of a collector query.</summary>
public sealed record CollectorParameter(string Name, object? Value, CollectorParameterType Type);

/// <summary>
/// The query a definition builds for one collection cycle: T-SQL text plus bound parameters.
/// Most definitions return a constant text with no parameters; target-aware definitions
/// (e.g. cpu_utilization's Azure-vs-ring-buffer fork) select text and bind values per cycle.
/// </summary>
public sealed class CollectorQuery
{
    public CollectorQuery(string text, IReadOnlyList<CollectorParameter>? parameters = null)
    {
        Text = text;
        Parameters = parameters ?? Array.Empty<CollectorParameter>();
    }

    public string Text { get; }

    public IReadOnlyList<CollectorParameter> Parameters { get; }
}
