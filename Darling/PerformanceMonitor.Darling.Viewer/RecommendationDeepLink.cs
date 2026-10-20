/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>Where a recommendation card's deep link opens (#6134).</summary>
public enum RecommendationLinkTarget
{
    /// <summary>No link: this server's engine has no view for the finding.</summary>
    None,

    /// <summary>A SQL Server server's Queries tab, Active Queries sub-tab.</summary>
    SqlServerActiveQueries,

    /// <summary>A PostgreSQL server's Activity tab, Blocking sub-tab (blocking and deadlock findings).</summary>
    PgBlocking,

    /// <summary>A PostgreSQL server's Activity tab, Query Shapes sub-tab (every other finding it can show).</summary>
    PgQueryShapes
}

/// <summary>
/// Picks the tab a recommendation's deep link opens, by the server's engine (#6134). The link used to open the
/// SQL Server Queries tab for every server, which a PostgreSQL server's tab set hides.
/// </summary>
public static class RecommendationDeepLink
{
    /// <summary>
    /// The target for a finding on a server of the given engine. SQL Server is always Active Queries. PostgreSQL
    /// goes to its Activity tab (Blocking for blocking and deadlock findings, Query Shapes otherwise), except for the
    /// findings about SQL Server only features (Agent jobs, trace flags, the plan cache, memory grants), which get no link.
    /// </summary>
    public static RecommendationLinkTarget Resolve(bool isPostgres, string? rootFactKey)
    {
        if (!isPostgres)
        {
            return RecommendationLinkTarget.SqlServerActiveQueries;
        }

        var key = rootFactKey ?? string.Empty;
        if (key.Contains("BLOCKING", StringComparison.OrdinalIgnoreCase)
            || key.Contains("DEADLOCK", StringComparison.OrdinalIgnoreCase))
        {
            return RecommendationLinkTarget.PgBlocking;
        }

        return key.ToUpperInvariant() switch
        {
            "RUNNING_JOBS" or "TRACE_FLAGS" or "PLAN_CACHE_BLOAT" or "MEMORY_GRANT_PENDING" => RecommendationLinkTarget.None,
            _ => RecommendationLinkTarget.PgQueryShapes
        };
    }

    /// <summary>The button text for a target.</summary>
    public static string Label(RecommendationLinkTarget target) =>
        target is RecommendationLinkTarget.PgBlocking or RecommendationLinkTarget.PgQueryShapes
            ? "Open in Activity"
            : "Open in Active Queries";

    /// <summary>The button tooltip for a target.</summary>
    public static string ToolTip(RecommendationLinkTarget target) => target switch
    {
        RecommendationLinkTarget.PgBlocking => "Open this server's Activity tab (Blocking) scoped to the finding's time window",
        RecommendationLinkTarget.PgQueryShapes => "Open this server's Activity tab (Query Shapes) scoped to the finding's time window",
        _ => "Open this server's Active Queries scoped to the finding's time window"
    };
}
