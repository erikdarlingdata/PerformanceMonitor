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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// Darling's tool names for the investigation brief (#5820): the family-call tables for both engines and the nameable
/// tools, which <see cref="InvestigationBriefComposer"/> takes as <see cref="Tools"/>. They name Darling's MCP tools, so they
/// stay in Darling while the composer is shared.
/// </summary>
internal static class DarlingInvestigationBriefTools
{
    /// <summary>The first calls per family on a SQL Server target. Every name is in <see cref="ToolRecommendations.AllToolNames"/>.</summary>
    internal static readonly IReadOnlyDictionary<string, string[]> SqlServerFamilyCalls = new Dictionary<string, string[]>(StringComparer.Ordinal)
    {
        [InvestigationFamily.Blocking] = new[] { "get_blocking", "get_blocking_trend", "get_deadlocks" },
        [InvestigationFamily.QueryPerformance] = new[] { "get_active_queries", "get_top_queries_by_cpu", "get_query_store_top" },
        [InvestigationFamily.Resources] = new[] { "get_cpu_utilization", "get_memory_stats", "get_wait_stats", "get_file_io_stats" },
        [InvestigationFamily.Availability] = new[] { "get_ag_health", "get_wait_stats" },
        [InvestigationFamily.Jobs] = new[] { "get_running_jobs" },
        [InvestigationFamily.SelfMonitor] = new[] { "get_wait_stats" },
        [InvestigationFamily.Other] = new[] { "get_wait_stats", "get_active_queries" },
    };

    /// <summary>The first calls per family on a PostgreSQL target. Every name is in <see cref="PgTargetToolRecommendations.AllToolNames"/>.</summary>
    internal static readonly IReadOnlyDictionary<string, string[]> PostgresFamilyCalls = new Dictionary<string, string[]>(StringComparer.Ordinal)
    {
        [InvestigationFamily.Blocking] = new[] { "get_pg_blocking", "get_pg_lock_stats" },
        [InvestigationFamily.QueryPerformance] = new[] { "get_pg_top_queries", "get_pg_query_duration_trend" },
        [InvestigationFamily.Resources] = new[] { "get_pg_cpu_utilization", "get_pg_wait_stats", "get_pg_io_stats" },
        [InvestigationFamily.Availability] = new[] { "get_pg_replication_stats", "get_pg_replication_slots" },
        [InvestigationFamily.Jobs] = new[] { "get_pg_wait_stats" },
        [InvestigationFamily.SelfMonitor] = new[] { "get_pg_wait_stats" },
        [InvestigationFamily.Other] = new[] { "get_pg_wait_stats", "get_pg_top_queries" },
    };

    /// <summary>The only tools a brief may name in <c>next_calls</c>: the analysis recommendation tables, both engines, and
    /// <c>get_query_store_regressions</c>, which the plan-change hint (#6007) names, and <c>get_client_timeouts</c>, which a Client Timeouts request names first (#6553), and <c>get_job_history</c>, which an I/O anomaly story names (#6614).</summary>
    internal static IReadOnlyCollection<string> NameableTools { get; } =
        new HashSet<string>(ToolRecommendations.AllToolNames.Concat(PgTargetToolRecommendations.AllToolNames).Append("get_query_store_regressions").Append("get_client_timeouts").Append("get_job_history"), StringComparer.Ordinal);

    /// <summary>The three tables as the composer takes them.</summary>
    internal static InvestigationBriefTools Tools { get; } = new(SqlServerFamilyCalls, PostgresFamilyCalls, NameableTools);
}
