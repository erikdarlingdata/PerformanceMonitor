/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The restart arm of a long-window read that takes its rows from the hourly interval rollups of
/// <c>collect.query_stats</c> and <c>collect.procedure_stats</c> (#4605). A restart row is a row in which no
/// counter was knowable (<c>sample_interval_seconds = 0</c>): a first sighting, an uncredited counter reset (a
/// restart, a plan-cache eviction) or a gap past the policy. Its delta cannot be rated, so the
/// rollups exclude it, and a read that wants those rows back takes them from the raw table. The reads carry
/// presence, not CPU: every counter delta of such a row is 0, so a group that exists only as a restart row
/// reappears with no value. These two reads are that arm. The routing that runs them beside the rollups is the Custom Views runner (<c>DarlingWebEndpoints.RunComposedPanelCoreAsync</c>), which runs the count guard first.
/// <para>Each read carries the predicate of the partial index built for it in <see cref="PgTableTuning"/>
/// (<see cref="PgTableTuning.QueryStatsRestartRowIndexName"/>,
/// <see cref="PgTableTuning.ProcedureStatsRestartRowIndexName"/>), because the planner uses a partial index
/// only when the read implies its predicate. The window is half-open: <c>$1</c> is the start, inclusive, and
/// <c>$2</c> the end, exclusive.</para>
/// </summary>
public static class IntervalRollupRestartRows
{
    /// <summary>The raw restart rows of <c>collect.query_stats</c> in <c>[$1, $2)</c>, read through the partial index on <c>collection_time</c>.</summary>
    public const string QueryStatsRestartRowsSql = @"
SELECT r.server_name, r.database_name, r.sql_handle, r.collection_time, r.sample_interval_seconds
FROM collect.query_stats AS r
WHERE r.collection_time >= $1
  AND r.collection_time < $2
  AND r.sample_interval_seconds = 0;";

    /// <summary>The raw restart rows of <c>collect.procedure_stats</c> in <c>[$1, $2)</c>, read through the partial index on <c>collection_time</c>.</summary>
    public const string ProcedureStatsRestartRowsSql = @"
SELECT r.server_name, r.object_name, r.collection_time, r.sample_interval_seconds
FROM collect.procedure_stats AS r
WHERE r.collection_time >= $1
  AND r.collection_time < $2
  AND r.sample_interval_seconds = 0;";
}
