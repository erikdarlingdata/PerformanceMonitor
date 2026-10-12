/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The store lookups that decide which query text an actual-plan request runs (#5639). The service reads the text
/// with this SQL when it executes the request, and the viewer reads it with the SAME constant before it shows the
/// confirm prompt, so the text the operator confirms and the text the service digests come from the same row. One
/// constant, referenced from both ends and never copied (a copy could drift and then refuse every request).
/// </summary>
public static class ActualPlanStoredQuery
{
    /// <summary>
    /// The <c>query_stats</c> resolver: a (server_id, query_hash, database_name) identifier to the latest captured
    /// query text + estimated plan. The row a grid shows can hold a different (older, or sample) text; this is the
    /// one the service runs. $1 server_id, $2 query_hash, $3 database_name. The third column (isolation level) is
    /// NULL here (query_stats does not capture it); the fourth is the gzip form of the plan (#2069).
    /// </summary>
    public const string QueryStatsResolveSql = @"
SELECT query_text, query_plan_xml, NULL::text AS transaction_isolation_level, query_plan_gz
FROM v_query_stats
WHERE server_id = $1
AND   query_hash = $2
AND   database_name = $3
AND   query_text IS NOT NULL
ORDER BY collection_time DESC
LIMIT 1";
}
