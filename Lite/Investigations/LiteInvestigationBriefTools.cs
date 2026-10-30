/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Mcp;

namespace PerformanceMonitorLite.Investigations;

/// <summary>
/// Lite's tool names for the investigation brief (#5820): the family-call table and the nameable tools, which
/// <see cref="InvestigationBriefComposer"/> takes as <see cref="Tools"/>. They name Lite's MCP tools, so they stay in Lite
/// while the composer is shared. Lite differs from Darling's table in two places: blocking is
/// <c>get_blocked_process_reports</c> (Darling's <c>get_blocking</c>), and availability has no <c>get_ag_health</c> (Lite
/// serves no Availability Group read), so it points at the wait statistics. Lite monitors SQL Server only, so the
/// PostgreSQL table is empty.
/// </summary>
internal static class LiteInvestigationBriefTools
{
    /// <summary>The first calls per family. Every name is a tool Lite serves.</summary>
    internal static readonly IReadOnlyDictionary<string, string[]> SqlServerFamilyCalls = new Dictionary<string, string[]>(StringComparer.Ordinal)
    {
        [InvestigationFamily.Blocking] = new[] { "get_blocked_process_reports", "get_blocking_trend", "get_deadlocks" },
        [InvestigationFamily.QueryPerformance] = new[] { "get_active_queries", "get_top_queries_by_cpu", "get_query_store_top" },
        [InvestigationFamily.Resources] = new[] { "get_cpu_utilization", "get_memory_stats", "get_wait_stats", "get_file_io_stats" },
        [InvestigationFamily.Availability] = new[] { "get_wait_stats" },
        [InvestigationFamily.Jobs] = new[] { "get_running_jobs" },
        [InvestigationFamily.SelfMonitor] = new[] { "get_wait_stats" },
        [InvestigationFamily.Other] = new[] { "get_wait_stats", "get_active_queries" },
    };

    /// <summary>Lite serves no PostgreSQL tools, and every Lite target is SQL Server.</summary>
    internal static readonly IReadOnlyDictionary<string, string[]> PostgresFamilyCalls =
        new Dictionary<string, string[]>(StringComparer.Ordinal);

    /// <summary>The only tools a brief may name in <c>next_calls</c>: the analysis recommendation table and the family calls.</summary>
    internal static IReadOnlyCollection<string> NameableTools { get; } =
        new HashSet<string>(
            ToolRecommendations.AllToolNames.Concat(SqlServerFamilyCalls.Values.SelectMany(calls => calls)).Append("get_client_timeouts"),
            StringComparer.Ordinal);

    /// <summary>The tables as the composer takes them.</summary>
    internal static InvestigationBriefTools Tools { get; } = new(SqlServerFamilyCalls, PostgresFamilyCalls, NameableTools);
}
